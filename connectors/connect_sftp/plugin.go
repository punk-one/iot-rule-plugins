package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/crypto/ssh"
)

type config struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	PrivateKey  string `json:"private_key_pem"`
	Fingerprint string `json:"host_key_sha256"`
	Root        string `json:"root"`
	TimeoutMS   int    `json:"timeout_ms"`
	MaxBytes    int64  `json:"max_file_bytes"`
	MaxFiles    int    `json:"max_files"`
	AllowWrite  bool   `json:"allow_write"`
}
type resource struct {
	cfg    config
	gate   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	signer ssh.Signer
}
type connector struct {
	mu        sync.Mutex
	resources map[string]*resource
	closed    bool
}

var packageSlots = make(chan struct{}, 8)

func NewConnector() sdk.ConnectorPlugin { return &connector{resources: map[string]*resource{}} }
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-sftp", Name: "SFTP Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Pinned SSH host key, bounded directory/file operations, one finite session per operation", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "sftp", Label: "SFTP", Capabilities: []string{"read", "write", "session:per_operation"}, DisplayFields: []string{"host", "port", "root"}}}}
}
func decode(raw []byte, v any) error {
	if len(raw) > 3*1048576 {
		return errors.New("SFTP input too large")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return errors.New("invalid SFTP input")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing SFTP input")
	}
	return nil
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Port: 22, TimeoutMS: 5000, MaxBytes: 1048576, MaxFiles: 1024}
	if len(spec.Connection) > 65536 || decode(spec.Connection, &cfg) != nil || spec.ID == "" || cfg.Host == "" || len(cfg.Host) > 253 || strings.ContainsAny(cfg.Host, "/\\\x00\r\n") || cfg.Port < 1 || cfg.Port > 65535 || cfg.Username == "" || len(cfg.Username) > 128 || len(cfg.Password) > 4096 || len(cfg.PrivateKey) > 32768 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxBytes < 1 || cfg.MaxBytes > 1048576 || cfg.MaxFiles < 1 || cfg.MaxFiles > 1024 || !path.IsAbs(cfg.Root) || path.Clean(cfg.Root) != cfg.Root || len(cfg.Root) > 1024 || strings.ContainsAny(cfg.Root, "\x00\\") {
		return "", errors.New("invalid SFTP configuration")
	}
	fingerprint, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(cfg.Fingerprint, "SHA256:"))
	if err != nil || len(fingerprint) != 32 || !strings.HasPrefix(cfg.Fingerprint, "SHA256:") {
		return "", errors.New("a SHA256 SSH host fingerprint is required")
	}
	if (cfg.Password == "") == (cfg.PrivateKey == "") {
		return "", errors.New("configure exactly one SSH password or unencrypted private key")
	}
	var signer ssh.Signer
	if cfg.PrivateKey != "" {
		signer, err = ssh.ParsePrivateKey([]byte(cfg.PrivateKey))
		if err != nil {
			return "", errors.New("invalid SSH private key")
		}
		cfg.PrivateKey = ""
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &resource{cfg: cfg, gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel, signer: signer}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.resources) >= 32 || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("connector stopped, duplicate resource or resource limit")
	}
	c.resources[spec.ID] = r
	return spec.ID, nil
}
func (c *connector) get(h string) (*resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.resources[h]
	if r == nil {
		return nil, errors.New("unknown SFTP resource")
	}
	return r, nil
}
func (r *resource) close() { r.cancel(); r.gate <- struct{}{}; <-r.gate }
func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	r := c.resources[h]
	delete(c.resources, h)
	c.mu.Unlock()
	if r != nil {
		r.close()
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.closed = true
	resources := c.resources
	c.resources = map[string]*resource{}
	c.mu.Unlock()
	for _, r := range resources {
		r.close()
	}
	return nil
}

// Inspect each packet length before the SFTP library allocates its response.
// A total wire budget also bounds ReadDir, whose public API collects entries.
type boundedPackets struct {
	source  io.Reader
	pending []byte
	budget  int64
}

func (r *boundedPackets) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) == 0 {
		header := make([]byte, 4)
		if _, e := io.ReadFull(r.source, header); e != nil {
			return 0, e
		}
		n := int64(binary.BigEndian.Uint32(header))
		if n < 1 || n > 262144 || n+4 > r.budget {
			return 0, errors.New("SFTP packet or total response budget exceeded")
		}
		r.budget -= n + 4
		r.pending = make([]byte, 4+int(n))
		copy(r.pending, header)
		if _, e := io.ReadFull(r.source, r.pending[4:]); e != nil {
			r.pending = nil
			return 0, e
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
func (r *resource) withSession(ctx context.Context, budget int64, fn func(*sftp.Client) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case packageSlots <- struct{}{}:
	}
	defer func() { <-packageSlots }()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(r.cfg.Host, strconv.Itoa(r.cfg.Port)))
	if err != nil {
		return errors.New("SFTP connection failed")
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	auth := ssh.Password(r.cfg.Password)
	if r.signer != nil {
		auth = ssh.PublicKeys(r.signer)
	}
	sshcfg := &ssh.ClientConfig{User: r.cfg.Username, Auth: []ssh.AuthMethod{auth}, HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
		if ssh.FingerprintSHA256(key) != r.cfg.Fingerprint {
			return errors.New("SSH host key mismatch")
		}
		return nil
	}, Timeout: time.Until(deadline)}
	raw, chans, reqs, err := ssh.NewClientConn(conn, r.cfg.Host, sshcfg)
	if err != nil {
		return errors.New("SSH host verification or authentication failed")
	}
	client := ssh.NewClient(raw, chans, reqs)
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return errors.New("SSH subsystem session unavailable")
	}
	defer session.Close()
	stdout, err := session.StdoutPipe()
	if err != nil {
		return err
	}
	stdin, err := session.StdinPipe()
	if err != nil {
		return err
	}
	if err = session.RequestSubsystem("sftp"); err != nil {
		return errors.New("SFTP subsystem unavailable")
	}
	fs, err := sftp.NewClientPipe(&boundedPackets{source: stdout, budget: budget}, stdin, sftp.MaxPacket(32768), sftp.UseConcurrentReads(false), sftp.UseConcurrentWrites(false))
	if err != nil {
		return errors.New("SFTP negotiation failed")
	}
	defer fs.Close()
	if err = checkRoot(fs, r.cfg.Root); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fn(fs)
}
func cleanRelative(name string) bool {
	return name != "" && len(name) <= 1024 && name != "." && path.Clean(name) == name && !path.IsAbs(name) && name != ".." && !strings.HasPrefix(name, "../") && !strings.ContainsAny(name, "\\\x00\r\n")
}
func checkRoot(fs *sftp.Client, root string) error {
	real, err := fs.RealPath(root)
	if err != nil || path.Clean(real) != root {
		return errors.New("SFTP root is unavailable or resolves through a symlink")
	}
	return checkParents(fs, root, true)
}
func checkParents(fs *sftp.Client, full string, includeLast bool) error {
	target := full
	if !includeLast {
		target = path.Dir(full)
	}
	current := "/"
	for _, part := range strings.Split(strings.TrimPrefix(target, "/"), "/") {
		if part == "" {
			continue
		}
		current = path.Join(current, part)
		info, err := fs.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("SFTP directory must exist and contain no symlinks")
		}
	}
	return nil
}

type fileEntry struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	ModifiedMS int64  `json:"modified_ms"`
}

func readFile(fs *sftp.Client, full string, max int64) ([]byte, os.FileInfo, error) {
	if err := checkParents(fs, full, false); err != nil {
		return nil, nil, err
	}
	before, err := fs.Lstat(full)
	if err != nil || !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > max {
		return nil, nil, errors.New("SFTP file is unsafe or oversized")
	}
	f, err := fs.Open(full)
	if err != nil {
		return nil, nil, errors.New("SFTP file open failed")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !sameFile(before, opened) {
		return nil, nil, errors.New("SFTP file changed before open")
	}
	raw, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil || int64(len(raw)) > max {
		return nil, nil, errors.New("SFTP read failed or exceeded limit")
	}
	after, err := f.Stat()
	if err != nil || !sameFile(before, after) || int64(len(raw)) != after.Size() {
		return nil, nil, errors.New("SFTP file changed during reading")
	}
	last, err := fs.Lstat(full)
	if err != nil || !sameFile(after, last) {
		return nil, nil, errors.New("SFTP path changed during reading")
	}
	return raw, after, nil
}
func sameFile(a, b os.FileInfo) bool {
	return a.Mode().IsRegular() && b.Mode().IsRegular() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	var spec struct {
		Path        string `json:"path"`
		Data        []byte `json:"data"`
		SHA256      string `json:"sha256"`
		RequireSync bool   `json:"require_sync"`
	}
	if decode(req.Payload, &spec) != nil || (!cleanRelative(spec.Path) && !(req.Target == "list" && spec.Path == ".")) {
		return sdk.ConnectorResponse{}, errors.New("invalid bounded SFTP request")
	}
	if (req.Kind != sdk.ConnectorRequestRead || req.Target != "list" && req.Target != "read") && (req.Kind != sdk.ConnectorRequestWrite || req.Target != "put") {
		return sdk.ConnectorResponse{}, errors.New("unsupported SFTP operation")
	}
	if req.Target == "put" && (!r.cfg.AllowWrite || int64(len(spec.Data)) > r.cfg.MaxBytes || spec.SHA256 == "") {
		return sdk.ConnectorResponse{}, errors.New("SFTP write disabled, missing digest or oversized")
	}
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	case r.gate <- struct{}{}:
	}
	defer func() { <-r.gate }()
	if ctx.Err() != nil {
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	full := path.Join(r.cfg.Root, spec.Path)
	var result any
	err = r.withSession(ctx, r.cfg.MaxBytes+2*1048576, func(fs *sftp.Client) error {
		switch req.Target {
		case "list":
			if err := checkParents(fs, full, true); err != nil {
				return err
			}
			files, e := fs.ReadDirContext(ctx, full)
			if e != nil {
				return errors.New("SFTP directory read failed or exceeded wire budget")
			}
			if len(files) > r.cfg.MaxFiles {
				return errors.New("SFTP directory entry limit exceeded")
			}
			out := []fileEntry{}
			for _, f := range files {
				if !cleanRelative(f.Name()) || strings.Contains(f.Name(), "/") || !f.Mode().IsRegular() {
					continue
				}
				out = append(out, fileEntry{f.Name(), f.Size(), f.ModTime().UnixMilli()})
			}
			result = map[string]any{"files": out}
		case "read":
			raw, info, e := readFile(fs, full, r.cfg.MaxBytes)
			if e != nil {
				return e
			}
			hash := sha256.Sum256(raw)
			result = map[string]any{"data": raw, "sha256": hex.EncodeToString(hash[:]), "size": len(raw), "modified_ms": info.ModTime().UnixMilli()}
		case "put":
			if e := checkParents(fs, full, false); e != nil {
				return e
			}
			hash := sha256.Sum256(spec.Data)
			if hex.EncodeToString(hash[:]) != spec.SHA256 {
				return errors.New("SFTP content digest mismatch")
			}
			if _, e := fs.Lstat(full); e == nil {
				raw, _, e := readFile(fs, full, r.cfg.MaxBytes)
				if e != nil || !bytes.Equal(raw, spec.Data) {
					return errors.New("SFTP target already exists with different or unsafe content")
				}
				if spec.RequireSync {
					f, e := fs.OpenFile(full, os.O_WRONLY)
					if e != nil {
						return errors.New("SFTP existing file cannot be synced")
					}
					e = f.Sync()
					f.Close()
					if e != nil {
						return errors.New("SFTP server fsync unsupported or failed")
					}
				}
				result = map[string]any{"received": true, "synced": spec.RequireSync, "duplicate": true}
				return nil
			} else if !os.IsNotExist(e) {
				return errors.New("SFTP target existence unknown")
			}
			temp := path.Join(path.Dir(full), ".iot-"+sdk.NewEvent(nil, nil).ID+".tmp")
			f, e := fs.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
			if e != nil {
				return errors.New("SFTP temporary upload open failed")
			}
			defer fs.Remove(temp)
			if e = f.Chmod(0600); e == nil {
				_, e = f.Write(spec.Data)
			}
			if e == nil && spec.RequireSync {
				e = f.Sync()
			}
			ce := f.Close()
			if e != nil || ce != nil {
				return errors.New("SFTP upload or file sync failed; outcome unknown")
			}
			// Publish with an atomic no-replace hard link. Plain SFTP RENAME
			// is implemented as replacement by some servers, so it is unsafe
			// as an idempotent event commit under competing resources.
			if _, ok := fs.HasExtension("hardlink@openssh.com"); !ok {
				return errors.New("SFTP atomic output requires OpenSSH hardlink extension")
			}
			if e = fs.Link(temp, full); e != nil {
				return errors.New("SFTP no-replace publish failed; outcome unknown, reconcile stable filename and digest")
			}

			result = map[string]any{"received": true, "synced": spec.RequireSync, "duplicate": false}
		}
		return nil
	})
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	raw, err := json.Marshal(result)
	return sdk.ConnectorResponse{Payload: raw}, err
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	_, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusDisconnected, ReasonCode: "IDLE_DEMAND_SESSION", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}

const configSchema = `{"type":"object","required":["host","username","host_key_sha256","root"],"properties":{"host":{"type":"string"},"port":{"type":"integer","minimum":1,"maximum":65535,"default":22},"username":{"type":"string"},"password":{"type":"string","format":"password","writeOnly":true},"private_key_pem":{"type":"string","format":"password","writeOnly":true},"host_key_sha256":{"type":"string","pattern":"^SHA256:"},"root":{"type":"string"},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"max_file_bytes":{"type":"integer","minimum":1,"maximum":1048576,"default":1048576},"max_files":{"type":"integer","minimum":1,"maximum":1024,"default":1024},"allow_write":{"type":"boolean","default":false}},"additionalProperties":false}`

func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("SFTP is a finite file-operation connector")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("SFTP streaming is not supported")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error {
	return errors.New("SFTP streaming is not supported")
}
func (c *connector) NackStream(string, sdk.StreamNackRequest) error {
	return errors.New("SFTP streaming is not supported")
}
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("SFTP streaming is not supported")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }
