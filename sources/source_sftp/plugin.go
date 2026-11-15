package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	Directory    string `json:"directory"`
	Pattern      string `json:"pattern"`
	DeviceCode   string `json:"device_code"`
	StateDir     string `json:"state_dir"`
	IntervalMS   int    `json:"interval_ms"`
	StableMS     int    `json:"stable_ms"`
	TimeoutMS    int    `json:"timeout_ms"`
	MaxBytes     int64  `json:"max_bytes"`
	MaxFiles     int    `json:"max_files"`
	Encoding     string `json:"encoding"`
}
type fileEntry struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	ModifiedMS int64  `json:"modified_ms"`
}
type observation struct {
	fileEntry
	since time.Time
}
type plugin struct {
	sdk.BasePlugin
	cfg  config
	ctx  sdk.RuntimeContext
	db   *bolt.DB
	seen map[string]observation
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-sftp", Name: "SFTP File Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded stable remote-file polling and durable digest checkpoint after handoff", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"sftp"}, RequiredCapabilities: []string{"read"}}}}
}
func cleanDirectory(s string) bool {
	return s == "." || s != "" && len(s) <= 1024 && path.Clean(s) == s && !path.IsAbs(s) && s != ".." && !strings.HasPrefix(s, "../") && !strings.ContainsAny(s, "\\\x00\r\n")
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Directory: ".", Pattern: "*.xml", StateDir: "./state/sftp", IntervalMS: 5000, StableMS: 1000, TimeoutMS: 5000, MaxBytes: 1048576, MaxFiles: 1024, Encoding: "utf8"}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 65536 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || !cleanDirectory(p.cfg.Directory) || p.cfg.Pattern == "" || len(p.cfg.Pattern) > 256 || strings.ContainsAny(p.cfg.Pattern, "/\\") || p.cfg.StateDir == "" || p.cfg.IntervalMS < 100 || p.cfg.IntervalMS > 86400000 || p.cfg.StableMS < 100 || p.cfg.StableMS > 60000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.MaxBytes < 1 || p.cfg.MaxBytes > 1048576 || p.cfg.MaxFiles < 1 || p.cfg.MaxFiles > 1024 || (p.cfg.Encoding != "utf8" && p.cfg.Encoding != "base64") {
		return sdk.FatalError(errors.New("invalid SFTP source configuration"))
	}
	if _, err := path.Match(p.cfg.Pattern, "test"); err != nil {
		return sdk.FatalError(errors.New("invalid SFTP file pattern"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("bound SFTP connector and emitter required")
	}
	if err := os.MkdirAll(p.cfg.StateDir, 0700); err != nil {
		return errors.New("SFTP checkpoint directory unavailable")
	}
	scope := sha256.Sum256([]byte(ctx.RuleID() + "\x00" + ctx.NodeID() + "\x00" + p.cfg.ConnectorRef + "\x00" + p.cfg.Directory))
	db, err := bolt.Open(filepath.Join(p.cfg.StateDir, hex.EncodeToString(scope[:16])+".db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return errors.New("SFTP checkpoint store unavailable")
	}
	if err = db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists([]byte("delivered")); return err }); err != nil {
		db.Close()
		return err
	}
	p.ctx = ctx
	p.db = db
	p.seen = map[string]observation{}
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.wg.Add(1)
	go p.loop()
	return nil
}
func (p *plugin) Stop() error {
	var err error
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
		p.wg.Wait()
		if p.db != nil {
			err = p.db.Close()
		}
	})
	return err
}
func (p *plugin) loop() {
	defer p.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	next := time.Now()
	interval := time.Duration(p.cfg.IntervalMS) * time.Millisecond
	failures := 0
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		err := p.scan(time.Now())
		if err != nil {
			failures++
			if m := p.ctx.Metrics(); m != nil {
				m.IncCounter("sftp_scan_failures", nil)
			}
		} else {
			failures = 0
		}
		next = next.Add(interval)
		now := time.Now()
		if !next.After(now) {
			skipped := now.Sub(next)/interval + 1
			next = next.Add(skipped * interval)
			if m := p.ctx.Metrics(); m != nil {
				m.Observe("dropped_scans", float64(skipped), nil)
			}
		}
		if failures > 0 {
			retry := now.Add(time.Duration(1<<min(failures-1, 5)) * time.Second)
			if next.Before(retry) {
				next = retry
			}
		}
		timer.Reset(time.Until(next))
	}
}
func (p *plugin) read(target string, payload any, result any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: target, Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return errors.New("SFTP operation failed")
	}
	if len(response.Payload) > 2*1048576 {
		return errors.New("SFTP response oversized")
	}
	d := json.NewDecoder(bytes.NewReader(response.Payload))
	d.DisallowUnknownFields()
	if d.Decode(result) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid SFTP response")
	}
	return nil
}
func (p *plugin) scan(now time.Time) error {
	var listing struct {
		Files []fileEntry `json:"files"`
	}
	if err := p.read("list", map[string]any{"path": p.cfg.Directory}, &listing); err != nil {
		return err
	}
	if listing.Files == nil || len(listing.Files) > p.cfg.MaxFiles {
		return errors.New("invalid SFTP listing or entry limit exceeded")
	}
	present := map[string]bool{}
	for _, entry := range listing.Files {
		if entry.Name == "" || len(entry.Name) > 255 || entry.Name == "." || entry.Name == ".." || strings.ContainsAny(entry.Name, "/\\\x00") || entry.Size < 0 || entry.ModifiedMS < 0 || present[entry.Name] {
			return errors.New("invalid or duplicate SFTP listing entry")
		}
		present[entry.Name] = true
	}
	var result error
	for _, entry := range listing.Files {
		select {
		case <-p.stop:
			return nil
		default:
		}
		matches, _ := path.Match(p.cfg.Pattern, entry.Name)
		if !matches || strings.HasPrefix(entry.Name, ".") {
			continue
		}
		old, ok := p.seen[entry.Name]
		if !ok || old.Size != entry.Size || old.ModifiedMS != entry.ModifiedMS {
			p.seen[entry.Name] = observation{entry, now}
			continue
		}
		if now.Sub(old.since) < time.Duration(p.cfg.StableMS)*time.Millisecond {
			continue
		}
		if err := p.deliver(entry); err != nil {
			result = err
		}
	}
	for name := range p.seen {
		if !present[name] {
			delete(p.seen, name)
		}
	}
	if err := p.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte("delivered")).Cursor()
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
func (p *plugin) deliver(entry fileEntry) error {
	if entry.Size > p.cfg.MaxBytes {
		return errors.New("SFTP file exceeds source size limit")
	}
	var response struct {
		Data       []byte `json:"data"`
		SHA256     string `json:"sha256"`
		Size       int64  `json:"size"`
		ModifiedMS int64  `json:"modified_ms"`
	}
	if err := p.read("read", map[string]any{"path": path.Join(p.cfg.Directory, entry.Name)}, &response); err != nil {
		return err
	}
	hash := sha256.Sum256(response.Data)
	digest := hex.EncodeToString(hash[:])
	if int64(len(response.Data)) != response.Size || response.Size != entry.Size || response.Size > p.cfg.MaxBytes || response.ModifiedMS != entry.ModifiedMS || digest != response.SHA256 {
		return errors.New("SFTP file changed after stable observation or digest invalid")
	}
	var duplicate bool
	if err := p.db.View(func(tx *bolt.Tx) error {
		duplicate = string(tx.Bucket([]byte("delivered")).Get([]byte(entry.Name))) == digest
		return nil
	}); err != nil {
		return err
	}
	if duplicate {
		return nil
	}
	field, value := "raw", string(response.Data)
	if p.cfg.Encoding == "base64" {
		field = "raw_base64"
		value = base64.StdEncoding.EncodeToString(response.Data)
	} else if !utf8.Valid(response.Data) {
		return errors.New("SFTP file is not UTF-8; use base64 for binary files")
	}
	id := sha256.Sum256([]byte(p.ctx.RuleID() + "\x00" + p.ctx.NodeID() + "\x00" + p.cfg.ConnectorRef + "\x00" + p.cfg.Directory + "\x00" + entry.Name + "\x00" + digest))
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, field: value, "file": map[string]any{"name": entry.Name, "size": entry.Size, "sha256": digest, "modified_at": entry.ModifiedMS}, "time": time.Now().UnixMilli()}, nil)
	event.ID = "evt_sftp_" + hex.EncodeToString(id[:])
	sdk.SetTraceID(&event, event.ID)
	select {
	case <-p.stop:
		return nil
	default:
	}
	if err := p.ctx.Emitter().Publish(event); err != nil {
		return err
	}
	return p.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("delivered")).Put([]byte(entry.Name), []byte(digest)) })
}

const configSchema = `{"type":"object","required":["connector_ref","device_code"],"properties":{"connector_ref":{"type":"string"},"device_code":{"type":"string"},"directory":{"type":"string","default":"."},"pattern":{"type":"string","default":"*.xml"},"state_dir":{"type":"string","default":"./state/sftp"},"interval_ms":{"type":"integer","minimum":100,"maximum":86400000,"default":5000},"stable_ms":{"type":"integer","minimum":100,"maximum":60000,"default":1000},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"max_bytes":{"type":"integer","minimum":1,"maximum":1048576,"default":1048576},"max_files":{"type":"integer","minimum":1,"maximum":1024,"default":1024},"encoding":{"type":"string","enum":["utf8","base64"],"default":"utf8"}},"additionalProperties":false}`
