package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type config struct {
	Directory  string `json:"directory"`
	Pattern    string `json:"pattern"`
	DeviceCode string `json:"device_code"`
	StateDir   string `json:"state_dir"`
	IntervalMS int    `json:"interval_ms"`
	StableMS   int    `json:"stable_ms"`
	MaxBytes   int64  `json:"max_bytes"`
	MaxFiles   int    `json:"max_files"`
	Encoding   string `json:"encoding"`
}
type observation struct {
	size     int64
	modified time.Time
	since    time.Time
}
type plugin struct {
	sdk.BasePlugin
	cfg  config
	ctx  sdk.RuntimeContext
	root *os.File
	db   *bolt.DB
	seen map[string]observation
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-file-watch", Name: "File Watch Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Nonrecursive stable-file polling with no-follow access, bounded content and persistent digest checkpoints", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Pattern: "*.xml", StateDir: "./state/file-watch", IntervalMS: 500, StableMS: 1000, MaxBytes: 1048576, MaxFiles: 1024, Encoding: "utf8"}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.Directory == "" || p.cfg.DeviceCode == "" || p.cfg.StateDir == "" || p.cfg.Pattern == "" || strings.ContainsAny(p.cfg.Pattern, "/\\") || p.cfg.IntervalMS < 100 || p.cfg.IntervalMS > 60000 || p.cfg.StableMS < 100 || p.cfg.StableMS > 60000 || p.cfg.MaxBytes < 1 || p.cfg.MaxBytes > 8388608 || p.cfg.MaxFiles < 1 || p.cfg.MaxFiles > 10000 || (p.cfg.Encoding != "utf8" && p.cfg.Encoding != "base64") {
		return sdk.FatalError(errors.New("invalid file source settings"))
	}
	if _, err := filepath.Match(p.cfg.Pattern, "test"); err != nil {
		return sdk.FatalError(errors.New("invalid file pattern"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	root, err := filepath.EvalSymlinks(p.cfg.Directory)
	if err != nil {
		return errors.New("input directory unavailable")
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return errors.New("cannot open input directory")
	}
	p.root = os.NewFile(uintptr(fd), root)
	if err = os.MkdirAll(p.cfg.StateDir, 0700); err != nil {
		p.root.Close()
		return err
	}
	scope := sha256.Sum256([]byte(ctx.RuleID() + "\x00" + ctx.NodeID() + "\x00" + root))
	p.db, err = bolt.Open(filepath.Join(p.cfg.StateDir, hex.EncodeToString(scope[:16])+".db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		p.root.Close()
		return errors.New("cannot open file checkpoint store")
	}
	if err = p.db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists([]byte("delivered")); return err }); err != nil {
		p.db.Close()
		p.root.Close()
		return err
	}
	p.ctx = ctx
	p.seen = map[string]observation{}
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.wg.Add(1)
	go p.loop()
	return nil
}
func (p *plugin) Stop() error {
	var result error
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
		p.wg.Wait()
		if p.db != nil {
			result = p.db.Close()
		}
		if p.root != nil {
			if err := p.root.Close(); err != nil && result == nil {
				result = err
			}
		}
	})
	return result
}
func (p *plugin) loop() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Duration(p.cfg.IntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := p.scan(time.Now()); err != nil {
			p.ctx.Metrics().IncCounter("file_scan_failures", nil)
		}
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		}
	}
}
func (p *plugin) scan(now time.Time) error {
	if _, err := p.root.Seek(0, io.SeekStart); err != nil {
		return err
	}
	entries, err := p.root.ReadDir(p.cfg.MaxFiles + 1)
	if err != nil && err != io.EOF {
		return err
	}
	if len(entries) > p.cfg.MaxFiles {
		return errors.New("directory entry limit exceeded; use a dedicated bounded input directory")
	}
	present := map[string]bool{}
	var result error
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") || entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		matches, _ := filepath.Match(p.cfg.Pattern, name)
		if !matches {
			continue
		}
		present[name] = true
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		old, ok := p.seen[name]
		if !ok || old.size != info.Size() || !old.modified.Equal(info.ModTime()) {
			p.seen[name] = observation{size: info.Size(), modified: info.ModTime(), since: now}
			continue
		}
		if now.Sub(old.since) < time.Duration(p.cfg.StableMS)*time.Millisecond {
			continue
		}
		if err = p.readAndPublish(name); err != nil {
			result = err
		}
	}
	for name := range p.seen {
		if !present[name] {
			delete(p.seen, name)
		}
	}
	if err = p.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("delivered"))
		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if !present[string(k)] {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return result
}
func (p *plugin) readAndPublish(name string) error {
	fd, err := unix.Openat(int(p.root.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return errors.New("file unavailable or unsafe")
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() > p.cfg.MaxBytes {
		return errors.New("file is not regular or exceeds limit")
	}
	raw, err := io.ReadAll(io.LimitReader(file, p.cfg.MaxBytes+1))
	if err != nil || int64(len(raw)) > p.cfg.MaxBytes {
		return errors.New("file read failed or exceeded limit")
	}
	after, err := file.Stat()
	if err != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || int64(len(raw)) != after.Size() {
		return errors.New("file changed during reading")
	}
	observed, ok := p.seen[name]
	if !ok || observed.size != before.Size() || !observed.modified.Equal(before.ModTime()) {
		return errors.New("file changed after stable observation")
	}
	digest := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(digest[:])
	var duplicate bool
	if err = p.db.View(func(tx *bolt.Tx) error {
		duplicate = string(tx.Bucket([]byte("delivered")).Get([]byte(name))) == fingerprint
		return nil
	}); err != nil {
		return err
	}
	if duplicate {
		return nil
	}
	var value string
	field := "raw"
	if p.cfg.Encoding == "base64" {
		field = "raw_base64"
		value = base64.StdEncoding.EncodeToString(raw)
	} else {
		if !utf8.Valid(raw) {
			return errors.New("file is not UTF-8; configure base64 for binary input")
		}
		value = string(raw)
	}
	id := sha256.Sum256([]byte(p.ctx.RuleID() + "\x00" + p.ctx.NodeID() + "\x00" + name + "\x00" + fingerprint))
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, field: value, "file": map[string]any{"name": name, "sha256": fingerprint, "size": len(raw), "modified_at": after.ModTime().UnixMilli()}, "time": time.Now().UnixMilli()}, nil)
	event.ID = "evt_file_" + hex.EncodeToString(id[:])
	if err = p.ctx.Emitter().Publish(event); err != nil {
		return fmt.Errorf("file handoff failed: %w", err)
	}
	return p.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("delivered")).Put([]byte(name), []byte(fingerprint)) })
}
