package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type config struct {
	DeviceCode  string   `json:"device_code"`
	Operations  []string `json:"allowed_operations"`
	StateDir    string   `json:"state_dir"`
	MaxTTLMS    int64    `json:"max_ttl_ms"`
	RetentionMS int64    `json:"retention_ms"`
	ClockSkewMS int64    `json:"clock_skew_ms"`
	MaxCommands int      `json:"max_commands"`
}
type command struct {
	ID        string
	Device    string
	Operation string
	Issued    int64
	Expires   int64
	Digest    string
}
type record struct {
	Digest  string `json:"digest"`
	Expires int64  `json:"expires"`
	Status  string `json:"status"`
}
type plugin struct {
	sdk.BasePlugin
	cfg    config
	ctx    sdk.RuntimeContext
	mu     sync.Mutex
	db     *bolt.DB
	closed bool
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "command-guard", Name: "Command Guard", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Persistent per-rule/node/device command claim with target, operation and expiry checks", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 2, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{StateDir: "./state/command-guard", MaxTTLMS: 300000, RetentionMS: 86400000, ClockSkewMS: 5000, MaxCommands: 10000}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.DeviceCode == "" || p.cfg.StateDir == "" || len(p.cfg.Operations) == 0 || len(p.cfg.Operations) > 32 || p.cfg.MaxTTLMS < 100 || p.cfg.MaxTTLMS > 86400000 || p.cfg.RetentionMS < p.cfg.MaxTTLMS || p.cfg.RetentionMS > 604800000 || p.cfg.ClockSkewMS < 0 || p.cfg.ClockSkewMS > 60000 || p.cfg.MaxCommands < 1 || p.cfg.MaxCommands > 100000 {
		return sdk.FatalError(errors.New("invalid command guard config"))
	}
	seen := map[string]bool{}
	for _, op := range p.cfg.Operations {
		if op == "" || seen[op] {
			return sdk.FatalError(errors.New("operation list contains empty or duplicate values"))
		}
		seen[op] = true
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	if err := os.MkdirAll(p.cfg.StateDir, 0700); err != nil {
		return err
	}
	scope := sha256.Sum256([]byte(ctx.RuleID() + "\x00" + ctx.NodeID() + "\x00" + p.cfg.DeviceCode))
	db, err := bolt.Open(filepath.Join(p.cfg.StateDir, hex.EncodeToString(scope[:16])+".db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return errors.New("cannot open command deduplication state")
	}
	if err = db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists([]byte("commands")); return err }); err != nil {
		db.Close()
		return err
	}
	p.mu.Lock()
	p.ctx = ctx
	p.db = db
	p.closed = false
	p.mu.Unlock()
	return nil
}
func (p *plugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.db != nil {
		err := p.db.Close()
		p.db = nil
		return err
	}
	return nil
}
func integer(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 9007199254740991 || math.Trunc(v) != v {
			return 0, false
		}
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	}
	return 0, false
}
func (p *plugin) validate(e sdk.Event, now int64) (command, string) {
	cmd := command{}
	cmd.ID, _ = e.Payload["command_id"].(string)
	cmd.Device, _ = e.Payload["device_code"].(string)
	cmd.Operation, _ = e.Payload["operation"].(string)
	var ok bool
	if cmd.ID == "" || len(cmd.ID) > 128 {
		return cmd, "invalid_command_id"
	}
	if cmd.Device != p.cfg.DeviceCode {
		return cmd, "target_mismatch"
	}
	allowed := false
	for _, op := range p.cfg.Operations {
		if cmd.Operation == op {
			allowed = true
			break
		}
	}
	if !allowed {
		return cmd, "operation_not_allowed"
	}
	if cmd.Issued, ok = integer(e.Payload["issued_at"]); !ok {
		return cmd, "invalid_issued_at"
	}
	if cmd.Expires, ok = integer(e.Payload["expires_at"]); !ok {
		return cmd, "invalid_expires_at"
	}
	if cmd.Issued < 0 || cmd.Issued > now+p.cfg.ClockSkewMS || cmd.Issued < now-p.cfg.MaxTTLMS-p.cfg.ClockSkewMS {
		return cmd, "issued_at_outside_window"
	}
	if cmd.Expires <= now || cmd.Expires <= cmd.Issued || cmd.Expires-cmd.Issued > p.cfg.MaxTTLMS {
		return cmd, "expired_or_excessive_ttl"
	}
	if _, ok = e.Payload["arguments"].(map[string]any); !ok {
		return cmd, "arguments_object_required"
	}
	raw, err := json.Marshal(e.Payload)
	if err != nil || len(raw) > 65536 {
		return cmd, "invalid_or_oversized_command"
	}
	digest := sha256.Sum256(raw)
	cmd.Digest = hex.EncodeToString(digest[:])
	return cmd, ""
}
func (p *plugin) claim(cmd command, now int64) string {
	reason := ""
	err := p.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("commands"))
		if raw := bucket.Get([]byte(cmd.ID)); raw != nil {
			var old record
			if json.Unmarshal(raw, &old) != nil {
				return errors.New("invalid stored command state")
			}
			reason = "duplicate_command"
			if old.Digest != cmd.Digest {
				reason = "command_id_content_mismatch"
			}
			return nil
		}
		if bucket.Stats().KeyN >= p.cfg.MaxCommands {
			cursor := bucket.Cursor()
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				var old record
				if json.Unmarshal(v, &old) != nil {
					return errors.New("invalid stored command state")
				}
				if old.Expires+p.cfg.RetentionMS < now {
					if err := cursor.Delete(); err != nil {
						return err
					}
				}
			}
			if bucket.Stats().KeyN >= p.cfg.MaxCommands {
				reason = "command_state_capacity_exceeded"
				return nil
			}
		}
		raw, _ := json.Marshal(record{Digest: cmd.Digest, Expires: cmd.Expires, Status: "claimed"})
		return bucket.Put([]byte(cmd.ID), raw)
	})
	if err != nil {
		return "command_state_unavailable"
	}
	return reason
}
func (p *plugin) OnEvent(e sdk.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx == nil || p.db == nil || p.closed {
		return sdk.FatalError(errors.New("command guard not running"))
	}
	now := time.Now().UnixMilli()
	cmd, reason := p.validate(e, now)
	if reason == "" {
		reason = p.claim(cmd, now)
	}
	if reason != "" {
		p.ctx.Metrics().IncCounter("commands_rejected", map[string]string{"reason": reason})
		out := e.Clone()
		out.Payload = map[string]any{"command_id": cmd.ID, "device_code": cmd.Device, "status": "rejected", "reason": reason}
		return p.ctx.Emitter().EmitTo("rejected", out)
	}
	if err := p.ctx.Emitter().EmitTo("accepted", e.Clone()); err != nil {
		return sdk.SkippableError(errors.New("command claimed but forwarding outcome unknown; reconcile before any retry"))
	}
	if err := p.db.Update(func(tx *bolt.Tx) error {
		raw, _ := json.Marshal(record{Digest: cmd.Digest, Expires: cmd.Expires, Status: "forwarded"})
		return tx.Bucket([]byte("commands")).Put([]byte(cmd.ID), raw)
	}); err != nil {
		return sdk.SkippableError(errors.New("command forwarded but status update failed; reconcile command state"))
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
