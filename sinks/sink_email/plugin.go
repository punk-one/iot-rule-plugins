package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"sync"
	"time"
)

type config struct {
	ConnectorRef string   `json:"connector_ref"`
	To           []string `json:"to"`
	Subject      string   `json:"subject"`
	BodyPath     string   `json:"body_path"`
	DedupPath    string   `json:"dedup_path"`
	DedupMS      int      `json:"dedup_ms"`
	MaxEntries   int      `json:"max_entries"`
	MaxPerMinute int      `json:"max_per_minute"`
	TimeoutMS    int      `json:"timeout_ms"`
	Ack          bool     `json:"ack_smtp_receipt"`
}
type record struct {
	expires  time.Time
	accepted bool
}
type plugin struct {
	sdk.BasePlugin
	cfg     config
	ctx     sdk.RuntimeContext
	mu      sync.Mutex
	records map[string]record
	window  time.Time
	sent    int
	closed  bool
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-email", Name: "Email Notification Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Bounded alert submission with throttling, duplicate claims and explicit SMTP receipt goal", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"smtp"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Subject: "Rule Engine notification", BodyPath: "rendered", DedupPath: "event.id", DedupMS: 3600000, MaxEntries: 10000, MaxPerMinute: 20, TimeoutMS: 5000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.ConnectorRef == "" || len(p.cfg.To) < 1 || len(p.cfg.To) > 10 || p.cfg.BodyPath == "" || p.cfg.DedupPath == "" || len(p.cfg.Subject) > 200 || strings.ContainsAny(p.cfg.Subject, "\r\n\x00") || p.cfg.DedupMS < 60000 || p.cfg.DedupMS > 86400000 || p.cfg.MaxEntries < 1 || p.cfg.MaxEntries > 100000 || p.cfg.MaxPerMinute < 1 || p.cfg.MaxPerMinute > 1000 || p.cfg.TimeoutMS < 100 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid email notification settings"))
	}
	p.records = map[string]record{}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("SMTP resource required")
	}
	p.ctx = ctx
	return nil
}
func lookup(e sdk.Event, path string) (any, bool) {
	if path == "event.id" {
		return e.ID, e.ID != ""
	}
	var v any = e.Payload
	for _, part := range strings.Split(strings.TrimPrefix(path, "payload."), ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return v, true
}
func (p *plugin) claim(key string, now time.Time) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, errors.New("email sink stopped")
	}
	for id, r := range p.records {
		if !r.expires.After(now) {
			delete(p.records, id)
		}
	}
	if r, ok := p.records[key]; ok {
		if r.accepted {
			return true, nil
		}
		return false, errors.New("notification already attempted; outcome unknown/in-flight, reconcile before retry")
	}
	if p.window.IsZero() || now.Sub(p.window) >= time.Minute {
		p.window = now
		p.sent = 0
	}
	if p.sent >= p.cfg.MaxPerMinute || len(p.records) >= p.cfg.MaxEntries {
		return false, errors.New("email quota or dedup capacity reached")
	}
	p.sent++
	p.records[key] = record{expires: now.Add(time.Duration(p.cfg.DedupMS) * time.Millisecond)}
	return false, nil
}
func (p *plugin) ack(e sdk.Event) error {
	if p.cfg.Ack && e.Metadata["buffer_record_id"] != "" && e.Metadata["buffer_node_id"] != "" {
		if p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], EventID: e.ID, Status: sdk.AckStatusSuccess}) != nil {
			return sdk.SkippableError(errors.New("SMTP accepted but buffer ACK failed; stable replay ID required"))
		}
	}
	return nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	if e.Metadata["buffer_record_id"] != "" && !p.cfg.Ack {
		return sdk.SkippableError(errors.New("SMTP receipt goal must be explicitly enabled for buffered delivery"))
	}
	v, ok := lookup(e, p.cfg.BodyPath)
	body, isString := v.(string)
	if !ok || !isString || len(body) > 65536 {
		return sdk.SkippableError(errors.New("bounded email body string required"))
	}
	v, ok = lookup(e, p.cfg.DedupPath)
	key, isString := v.(string)
	if !ok || !isString || key == "" || len(key) > 1024 {
		return sdk.SkippableError(errors.New("bounded notification dedup key required"))
	}
	accepted, err := p.claim(key, time.Now())
	if err != nil {
		return sdk.SkippableError(err)
	}
	if accepted {
		return p.ack(e)
	}
	hash := sha256.Sum256([]byte(p.ctx.RuleID() + "\x00" + p.ctx.NodeID() + "\x00" + key))
	raw, _ := json.Marshal(map[string]any{"to": p.cfg.To, "subject": p.cfg.Subject, "body": body, "message_id": "iot-rule-" + hex.EncodeToString(hash[:16]) + "@iot-rule-engine.invalid"})
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "submit", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("SMTP attempt rejected/unknown; claim retained until expiry, no auto-retry"))
	}
	var receipt struct {
		Accepted bool `json:"accepted"`
	}
	if json.Unmarshal(response.Payload, &receipt) != nil || !receipt.Accepted {
		return sdk.SkippableError(errors.New("SMTP receipt invalid; claim retained"))
	}
	p.mu.Lock()
	record := p.records[key]
	record.accepted = true
	p.records[key] = record
	p.mu.Unlock()
	return p.ack(e)
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
func (p *plugin) Stop() error { p.mu.Lock(); p.closed = true; p.mu.Unlock(); return nil }
