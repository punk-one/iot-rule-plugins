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
	KeyPath  string `json:"key_path"`
	WindowMS int    `json:"window_ms"`
	MaxCount int    `json:"max_count"`
	MaxKeys  int    `json:"max_keys"`
	MaxBytes int    `json:"max_bytes"`
}
type group struct {
	key    string
	start  time.Time
	events []sdk.Event
	bytes  int
}
type plugin struct {
	sdk.BasePlugin
	cfg    config
	ctx    sdk.RuntimeContext
	mu     sync.Mutex
	groups map[string]*group
	bytes  int
	closed bool
	stop   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "batch-events", Name: "Batch Events", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded volatile per-key count/time batches with stable member IDs", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyPath: "device_code", WindowMS: 1000, MaxCount: 100, MaxKeys: 1024, MaxBytes: 8388608}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.KeyPath == "" || p.cfg.WindowMS < 10 || p.cfg.WindowMS > 86400000 || p.cfg.MaxCount < 1 || p.cfg.MaxCount > 4096 || p.cfg.MaxKeys < 1 || p.cfg.MaxKeys > 10000 || p.cfg.MaxBytes < 1024 || p.cfg.MaxBytes > 67108864 {
		return sdk.FatalError(errors.New("invalid batch bounds"))
	}
	p.groups = map[string]*group{}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.wg.Add(1)
	go p.loop()
	return nil
}
func lookup(root map[string]any, path string) (any, bool) {
	var v any = root
	for _, part := range strings.Split(path, ".") {
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
func output(g *group, end time.Time) sdk.Event {
	records := make([]any, 0, len(g.events))
	ids := make([]any, 0, len(g.events))
	identity := sha256.New()
	identity.Write([]byte(g.key))
	for _, e := range g.events {
		records = append(records, e.Payload)
		ids = append(ids, e.ID)
		identity.Write([]byte{0})
		identity.Write([]byte(e.ID))
	}
	event := sdk.NewEvent(map[string]any{"device_code": g.key, "records": records, "batch": map[string]any{"event_ids": ids, "count": len(records), "start": g.start.UnixMilli(), "end": end.UnixMilli()}, "time": end.UnixMilli()}, nil)
	event.ID = "evt_batch_" + hex.EncodeToString(identity.Sum(nil)[:16])
	sdk.SetTraceID(&event, event.ID)
	return event
}
func (p *plugin) add(e sdk.Event, now time.Time) ([]sdk.Event, error) {
	if e.Metadata["buffer_record_id"] != "" {
		return nil, errors.New("batch must precede durable buffer; aggregate input ACK unsupported")
	}
	v, ok := lookup(e.Payload, p.cfg.KeyPath)
	key, isString := v.(string)
	if !ok || !isString || key == "" || len(key) > 1024 || e.ID == "" {
		return nil, errors.New("bounded key and event ID required")
	}
	raw, err := json.Marshal(e)
	if err != nil || len(raw) > 1047552 {
		return nil, errors.New("batch input exceeds 1 MiB")
	}
	owned := e.Clone()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("batch stopped")
	}
	out := []sdk.Event{}
	g := p.groups[key]
	expired := g != nil && (now.Sub(g.start) >= time.Duration(p.cfg.WindowMS)*time.Millisecond || g.bytes+len(raw) > 1047552)
	oldBytes := 0
	if expired {
		oldBytes = g.bytes
	}
	if p.bytes-oldBytes+len(raw) > p.cfg.MaxBytes || g == nil && len(p.groups) >= p.cfg.MaxKeys {
		return nil, errors.New("batch capacity exceeded")
	}
	if expired {
		out = append(out, output(g, now))
		p.bytes -= g.bytes
		delete(p.groups, key)
		g = nil
	}
	if g == nil {
		g = &group{key: key, start: now}
		p.groups[key] = g
	}
	g.events = append(g.events, owned)
	g.bytes += len(raw)
	p.bytes += len(raw)
	if len(g.events) >= p.cfg.MaxCount {
		out = append(out, output(g, now))
		p.bytes -= g.bytes
		delete(p.groups, key)
	}
	return out, nil
}
func (p *plugin) drain(now time.Time) []sdk.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []sdk.Event{}
	for key, g := range p.groups {
		if now.Sub(g.start) >= time.Duration(p.cfg.WindowMS)*time.Millisecond {
			out = append(out, output(g, now))
			p.bytes -= g.bytes
			delete(p.groups, key)
		}
	}
	return out
}
func (p *plugin) loop() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Duration(min(p.cfg.WindowMS, 20)) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case now := <-ticker.C:
			for _, e := range p.drain(now) {
				if p.ctx.Emitter().Publish(e) != nil {
					p.ctx.Metrics().IncCounter("batch_emit_failures", nil)
				}
			}
		}
	}
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	out, err := p.add(e, time.Now())
	if err != nil {
		return sdk.SkippableError(err)
	}
	for _, event := range out {
		if err = p.ctx.Emitter().Publish(event); err != nil {
			return sdk.SkippableError(errors.New("volatile batch output handoff failed"))
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
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.groups = map[string]*group{}
		p.bytes = 0
		p.mu.Unlock()
		if p.stop != nil {
			close(p.stop)
		}
	})
	p.wg.Wait()
	return nil
}
