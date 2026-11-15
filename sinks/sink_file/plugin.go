package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type config struct {
	Directory string `json:"directory"`
	Mode      string `json:"mode"`
	File      string `json:"file"`
	MaxEvent  int    `json:"max_event_bytes"`
	MaxFile   int64  `json:"max_file_bytes"`
	MaxFiles  int    `json:"max_files"`
	Ack       bool   `json:"ack_after_sync"`
}
type plugin struct {
	sdk.BasePlugin
	cfg    config
	ctx    sdk.RuntimeContext
	mu     sync.RWMutex
	dir    *os.File
	closed bool
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-file", Name: "File Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Bounded local Linux JSONL append or atomic per-event files; optional ACK after fsync", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Directory: "./data/file-output", Mode: "event_file", File: "events.jsonl", MaxEvent: 1048576, MaxFile: 104857600, MaxFiles: 10000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.Directory == "" || (p.cfg.Mode != "event_file" && p.cfg.Mode != "jsonl") || p.cfg.File == "" || filepath.Base(p.cfg.File) != p.cfg.File || p.cfg.File == "." || p.cfg.File == ".." || p.cfg.MaxEvent < 1 || p.cfg.MaxEvent > 1048576 || p.cfg.MaxFile < int64(p.cfg.MaxEvent) || p.cfg.MaxFile > 1073741824 || p.cfg.MaxFiles < 1 || p.cfg.MaxFiles > 100000 {
		return sdk.FatalError(errors.New("invalid file sink bounds"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	if err := os.MkdirAll(p.cfg.Directory, 0700); err != nil {
		return err
	}
	fd, err := unix.Open(p.cfg.Directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return errors.New("cannot open configured output directory without symlinks")
	}
	p.dir = os.NewFile(uintptr(fd), p.cfg.Directory)
	p.ctx = ctx
	return nil
}
func writeAll(w io.Writer, raw []byte) error {
	for len(raw) > 0 {
		n, err := w.Write(raw)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}
func regular(fd int) bool {
	var stat unix.Stat_t
	return unix.Fstat(fd, &stat) == nil && stat.Mode&unix.S_IFMT == unix.S_IFREG
}
func (p *plugin) existing(name string, raw []byte) (bool, error) {
	fd, err := unix.Openat(int(p.dir.Fd()), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err == unix.ENOENT {
		return false, nil
	}
	if err != nil {
		return false, errors.New("existing event file cannot be opened safely")
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	if !regular(fd) {
		return false, errors.New("event target is not a regular file")
	}
	stored, err := io.ReadAll(io.LimitReader(file, int64(p.cfg.MaxEvent)+1))
	if err != nil || !bytes.Equal(stored, raw) {
		return false, errors.New("event ID already exists with different content")
	}
	if err = file.Sync(); err != nil {
		return false, err
	}
	if err = p.dir.Sync(); err != nil {
		return false, err
	}
	return true, nil
}
func (p *plugin) countFiles() error {
	fd, err := unix.Openat(int(p.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "output-directory")
	defer file.Close()
	entries, err := file.ReadDir(p.cfg.MaxFiles + 1)
	if err != nil && err != io.EOF {
		return err
	}
	if len(entries) >= p.cfg.MaxFiles {
		return errors.New("file count capacity reached; retention required")
	}
	return nil
}
func (p *plugin) atomicFile(eventID string, raw []byte) error {
	hash := sha256.Sum256([]byte(eventID))
	name := hex.EncodeToString(hash[:]) + ".json"
	if exists, err := p.existing(name, raw); err != nil || exists {
		return err
	}
	if err := p.countFiles(); err != nil {
		return err
	}
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	temporary := "." + hex.EncodeToString(hash[:8]) + "-" + hex.EncodeToString(nonce) + ".tmp"
	dirfd := int(p.dir.Fd())
	fd, err := unix.Openat(dirfd, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	defer unix.Unlinkat(dirfd, temporary, 0)
	file := os.NewFile(uintptr(fd), temporary)
	if err = writeAll(file, raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = unix.Linkat(dirfd, temporary, dirfd, name, 0); err == unix.EEXIST {
		_, err = p.existing(name, raw)
		return err
	}
	if err != nil {
		return err
	}
	return p.dir.Sync()
}
func (p *plugin) appendJSONL(raw []byte) error {
	fd, err := unix.Openat(int(p.dir.Fd()), p.cfg.File, unix.O_WRONLY|unix.O_APPEND|unix.O_CREAT|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), p.cfg.File)
	defer file.Close()
	if !regular(fd) {
		return errors.New("JSONL target must be a regular file")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if err != unix.EAGAIN && err != unix.EWOULDBLOCK {
			return err
		}
		if !time.Now().Before(deadline) {
			return errors.New("JSONL append lock deadline exceeded")
		}
		time.Sleep(5 * time.Millisecond)
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size()+int64(len(raw))+1 > p.cfg.MaxFile {
		return errors.New("JSONL file size cap reached; rotate while stopped")
	}
	raw = append(append([]byte(nil), raw...), '\n')
	if err = writeAll(file, raw); err != nil {
		return errors.New("partial append possible; reconcile trailing record before replay")
	}
	if err = file.Sync(); err != nil {
		return err
	}
	return p.dir.Sync()
}
func (p *plugin) persist(e sdk.Event) error {
	if e.ID == "" || len(e.ID) > 1024 {
		return errors.New("bounded event ID required")
	}
	raw, err := json.Marshal(map[string]any{"event_id": e.ID, "payload": e.Payload})
	if err != nil || len(raw) > p.cfg.MaxEvent {
		return errors.New("event exceeds output limit")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed || p.dir == nil {
		return errors.New("file sink stopped")
	}
	if p.cfg.Mode == "event_file" {
		return p.atomicFile(e.ID, raw)
	}
	return p.appendJSONL(raw)
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	if err := p.persist(e); err != nil {
		return sdk.SkippableError(err)
	}
	if p.cfg.Ack && e.Metadata["buffer_record_id"] != "" && e.Metadata["buffer_node_id"] != "" {
		if p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], EventID: e.ID, Status: sdk.AckStatusSuccess}) != nil {
			return sdk.SkippableError(errors.New("file synced but buffer ACK failed; replay needs stable ID"))
		}
	}
	return nil
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
func (p *plugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.dir != nil {
		err := p.dir.Close()
		p.dir = nil
		return err
	}
	return nil
}
