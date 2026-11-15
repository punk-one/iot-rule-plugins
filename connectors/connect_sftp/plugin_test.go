package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/pkg/sftp"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	binary      string
	listener    net.Listener
	cfg         *ssh.ServerConfig
	mu          sync.Mutex
	connections map[net.Conn]bool
	wg          sync.WaitGroup
	accepted    atomic.Int32
	active      atomic.Int32
	fingerprint string
}

func startFixture(t *testing.T) *fixture {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{cfg: &ssh.ServerConfig{PasswordCallback: func(metadata ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
		if metadata.User() == "test" && string(password) == "fixture-only" {
			return nil, nil
		}
		return nil, errors.New("denied")
	}}, connections: map[net.Conn]bool{}, fingerprint: ssh.FingerprintSHA256(signer.PublicKey())}
	f.cfg.AddHostKey(signer)
	f.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			conn, err := f.listener.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.connections[conn] = true
			f.mu.Unlock()
			f.accepted.Add(1)
			f.active.Add(1)
			f.wg.Add(1)
			go f.serve(conn)
		}
	}()
	t.Cleanup(func() {
		f.listener.Close()
		f.mu.Lock()
		for conn := range f.connections {
			conn.Close()
		}
		f.mu.Unlock()
		f.wg.Wait()
	})
	return f
}
func (f *fixture) serve(conn net.Conn) {
	defer f.wg.Done()
	defer conn.Close()
	defer func() { f.active.Add(-1); f.mu.Lock(); delete(f.connections, conn); f.mu.Unlock() }()
	server, chans, reqs, err := ssh.NewServerConn(conn, f.cfg)
	if err != nil {
		return
	}
	defer server.Close()
	go ssh.DiscardRequests(reqs)
	for channel := range chans {
		if channel.ChannelType() != "session" {
			channel.Reject(ssh.UnknownChannelType, "session only")
			continue
		}
		ch, requests, err := channel.Accept()
		if err != nil {
			return
		}
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			defer ch.Close()
			for req := range requests {
				if req.Type != "subsystem" {
					req.Reply(false, nil)
					continue
				}
				var sub struct{ Name string }
				if ssh.Unmarshal(req.Payload, &sub) != nil || sub.Name != "sftp" {
					req.Reply(false, nil)
					continue
				}
				req.Reply(true, nil)
				if f.binary != "" {
					cmd := exec.Command(f.binary)
					cmd.Stdin = ch
					cmd.Stdout = ch
					cmd.Stderr = io.Discard
					_ = cmd.Run()
					return
				}
				fs, err := sftp.NewServer(ch)
				if err == nil {
					fs.Serve()
					fs.Close()
				}
				return
			}
		}()
	}
}
func fixtureResource(t *testing.T, f *fixture, root string, changes map[string]any) (*connector, string) {
	t.Helper()
	host, port, _ := net.SplitHostPort(f.listener.Addr().String())
	portNumber, _ := strconv.Atoi(port)
	cfg := map[string]any{"host": host, "port": portNumber, "username": "test", "password": "fixture-only", "host_key_sha256": f.fingerprint, "root": root, "allow_write": true}
	for key, value := range changes {
		cfg[key] = value
	}
	raw, _ := json.Marshal(cfg)
	c := NewConnector().(*connector)
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "test", Connection: raw})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop() })
	return c, h
}
func execute(t *testing.T, c *connector, h, op string, payload any) (sdk.ConnectorResponse, error) {
	t.Helper()
	raw, _ := json.Marshal(payload)
	kind := sdk.ConnectorRequestRead
	if op == "put" {
		kind = sdk.ConnectorRequestWrite
	}
	return c.Execute(h, sdk.ConnectorRequest{Kind: kind, Target: op, Payload: raw})
}
func TestActualSSHFileReadAndIdempotentNoReplaceUpload(t *testing.T) {
	f := nativeFixture(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "input.xml"), []byte("<x>42</x>"), 0600); err != nil {
		t.Fatal(err)
	}
	c, h := fixtureResource(t, f, root, nil)
	response, err := execute(t, c, h, "list", map[string]any{"path": "."})
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Files []fileEntry `json:"files"`
	}
	json.Unmarshal(response.Payload, &listed)
	if len(listed.Files) != 1 || listed.Files[0].Name != "input.xml" {
		t.Fatal(string(response.Payload))
	}
	response, err = execute(t, c, h, "read", map[string]any{"path": "input.xml"})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Data []byte `json:"data"`
	}
	json.Unmarshal(response.Payload, &got)
	if string(got.Data) != "<x>42</x>" {
		t.Fatal(string(got.Data))
	}
	raw := []byte(`{"event":"one"}`)
	hash := sha256.Sum256(raw)
	request := map[string]any{"path": "out.json", "data": raw, "sha256": hex.EncodeToString(hash[:]), "require_sync": false}
	for i := 0; i < 2; i++ {
		response, err = execute(t, c, h, "put", request)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]bool
		json.Unmarshal(response.Payload, &got)
		if !got["received"] || got["synced"] || got["duplicate"] != (i == 1) {
			t.Fatal(string(response.Payload))
		}
	}
	request["data"] = []byte("changed")
	hash = sha256.Sum256([]byte("changed"))
	request["sha256"] = hex.EncodeToString(hash[:])
	if _, err = execute(t, c, h, "put", request); err == nil {
		t.Fatal("overwrote existing event")
	}
	contents, _ := os.ReadFile(filepath.Join(root, "out.json"))
	if !bytes.Equal(contents, raw) {
		t.Fatal(string(contents))
	}
	deadline := time.Now().Add(time.Second)
	for f.active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.active.Load() != 0 {
		t.Fatal("session not released")
	}
	if f.accepted.Load() != 5 {
		t.Fatal(f.accepted.Load())
	}
}
func TestSSHWrongPinReadOnlyTraversalAndSymlink(t *testing.T) {
	f := startFixture(t)
	root := t.TempDir()
	c, h := fixtureResource(t, f, root, map[string]any{"allow_write": false})
	for _, p := range []string{"../secret", "/etc/passwd", "a/../../b", "a\\b"} {
		if _, err := execute(t, c, h, "read", map[string]any{"path": p}); err == nil {
			t.Fatal(p)
		}
	}
	hash := sha256.Sum256([]byte("x"))
	if _, err := execute(t, c, h, "put", map[string]any{"path": "x", "data": []byte("x"), "sha256": hex.EncodeToString(hash[:])}); err == nil {
		t.Fatal("write allowed")
	}
	before := f.accepted.Load()
	if before != 0 {
		t.Fatal("rejected requests dialed")
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(root, "unsafe")); err != nil {
		t.Fatal(err)
	}
	if _, err := execute(t, c, h, "read", map[string]any{"path": "unsafe"}); err == nil {
		t.Fatal("symlink followed")
	}
	wrong := sha256.Sum256([]byte("wrong key"))
	c2, h2 := fixtureResource(t, f, root, map[string]any{"host_key_sha256": "SHA256:" + rawBase64(wrong[:])})
	if _, err := execute(t, c2, h2, "list", map[string]any{"path": "."}); err == nil {
		t.Fatal("wrong pin accepted")
	}
}
func rawBase64(raw []byte) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var out []byte
	var value uint32
	var bits uint
	for _, b := range raw {
		value = value<<8 | uint32(b)
		bits += 8
		for bits >= 6 {
			bits -= 6
			out = append(out, alphabet[(value>>bits)&63])
		}
	}
	if bits > 0 {
		out = append(out, alphabet[(value<<(6-bits))&63])
	}
	return string(out)
}
func TestPacketBoundsBeforeAllocation(t *testing.T) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 1<<30)
	r := &boundedPackets{source: bytes.NewReader(header), budget: 1048576}
	if _, err := r.Read(make([]byte, 1)); err == nil {
		t.Fatal("giant packet accepted")
	}
	binary.BigEndian.PutUint32(header, 10)
	r = &boundedPackets{source: io.MultiReader(bytes.NewReader(header), bytes.NewReader(make([]byte, 10))), budget: 12}
	if _, err := r.Read(make([]byte, 1)); err == nil {
		t.Fatal("total budget ignored")
	}
}
func TestHungHandshakeCancelledByResourceStop(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() { conn, _ := listener.Accept(); accepted <- conn }()
	fake := &fixture{listener: listener, fingerprint: "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
	c, h := fixtureResource(t, fake, t.TempDir(), nil)
	done := make(chan error, 1)
	go func() { _, err := execute(t, c, h, "list", map[string]any{"path": "."}); done <- err }()
	conn := <-accepted
	defer conn.Close()
	started := time.Now()
	if err := c.DestroyResource(h); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("hung SSH accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("resource stop did not interrupt SSH")
	}
	if time.Since(started) > time.Second {
		t.Fatal("slow cancellation")
	}
}

func TestOpenSSHFileSyncAndNoReplace(t *testing.T) {
	binary := "/usr/lib/openssh/sftp-server"
	if _, err := os.Stat(binary); err != nil {
		t.Skip("independent OpenSSH sftp-server not installed")
	}
	f := startFixture(t)
	f.binary = binary
	root := t.TempDir()
	c, h := fixtureResource(t, f, root, nil)
	data := []byte("synced")
	digest := sha256.Sum256(data)
	request := map[string]any{"path": "synced.json", "data": data, "sha256": hex.EncodeToString(digest[:]), "require_sync": true}
	for i := 0; i < 2; i++ {
		response, err := execute(t, c, h, "put", request)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]bool
		json.Unmarshal(response.Payload, &got)
		if !got["received"] || !got["synced"] || got["duplicate"] != (i == 1) {
			t.Fatal(string(response.Payload))
		}
	}
}
func TestServerWithoutSyncCannotProduceSyncReceipt(t *testing.T) {
	f := startFixture(t)
	c, h := fixtureResource(t, f, t.TempDir(), nil)
	data := []byte("x")
	digest := sha256.Sum256(data)
	if _, err := execute(t, c, h, "put", map[string]any{"path": "x", "data": data, "sha256": hex.EncodeToString(digest[:]), "require_sync": true}); err == nil {
		t.Fatal("unsupported fsync claimed successful")
	}
}

func nativeFixture(t *testing.T) *fixture {
	t.Helper()
	binary := "/usr/lib/openssh/sftp-server"
	if _, err := os.Stat(binary); err != nil {
		t.Skip("OpenSSH sftp-server not installed")
	}
	f := startFixture(t)
	f.binary = binary
	return f
}
func TestRemoteDirectoryEntryLimit(t *testing.T) {
	f := startFixture(t)
	root := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	c, h := fixtureResource(t, f, root, map[string]any{"max_files": 1})
	if _, err := execute(t, c, h, "list", map[string]any{"path": "."}); err == nil {
		t.Fatal("directory limit ignored")
	}
}
func TestConcurrentResourcesCannotReplaceEvent(t *testing.T) {
	f := nativeFixture(t)
	root := t.TempDir()
	a, ha := fixtureResource(t, f, root, nil)
	b, hb := fixtureResource(t, f, root, nil)
	var wg sync.WaitGroup
	for i, c := range []*connector{a, b} {
		wg.Add(1)
		go func(i int, c *connector) {
			defer wg.Done()
			data := []byte(strconv.Itoa(i))
			hash := sha256.Sum256(data)
			handle := ha
			if i == 1 {
				handle = hb
			}
			_, _ = execute(t, c, handle, "put", map[string]any{"path": "event.json", "data": data, "sha256": hex.EncodeToString(hash[:])})
		}(i, c)
	}
	wg.Wait()
	raw, err := os.ReadFile(filepath.Join(root, "event.json"))
	if err != nil || (string(raw) != "0" && string(raw) != "1") {
		t.Fatal(string(raw), err)
	}
	original := string(raw)
	other := []byte("other")
	hash := sha256.Sum256(other)
	if _, err := execute(t, a, ha, "put", map[string]any{"path": "event.json", "data": other, "sha256": hex.EncodeToString(hash[:])}); err == nil {
		t.Fatal("replacement allowed")
	}
	raw, _ = os.ReadFile(filepath.Join(root, "event.json"))
	if string(raw) != original {
		t.Fatal("event overwritten")
	}
}
