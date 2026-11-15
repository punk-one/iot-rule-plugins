package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"sync"
	"time"
)

type config struct {
	InputField  string `json:"input_field"`
	OutputField string `json:"output_field"`
	TimeoutMS   int    `json:"timeout_ms"`
	MaxGroups   int    `json:"max_groups"`
	MaxParts    int    `json:"max_parts"`
	MaxBytes    int    `json:"max_bytes"`
	EmitPartial bool   `json:"emit_partial"`
}
type group struct {
	base    sdk.Event
	items   map[int]any
	digests map[int][32]byte
	count   int
	bytes   int
	due     time.Time
	done    bool
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
	return sdk.PluginInfo{ID: "join-records", Name: "Join Records", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded indexed join with duplicate conflict checks, timeouts and volatile completion tombstones", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{InputField: "record", OutputField: "records", TimeoutMS: 5000, MaxGroups: 1024, MaxParts: 1024, MaxBytes: 8388608}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.InputField == "" || p.cfg.OutputField == "" || p.cfg.OutputField == "parts" || p.cfg.TimeoutMS < 10 || p.cfg.TimeoutMS > 86400000 || p.cfg.MaxGroups < 1 || p.cfg.MaxGroups > 10000 || p.cfg.MaxParts < 1 || p.cfg.MaxParts > 4096 || p.cfg.MaxBytes < 1024 || p.cfg.MaxBytes > 67108864 {
		return sdk.FatalError(errors.New("invalid join settings"))
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
func integer(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) || x < 0 || x > 4096 || math.Trunc(x) != x {
			return 0, false
		}
		return int(x), true
	case int:
		return x, x >= 0 && x <= 4096
	default:
		return 0, false
	}
}
func (p *plugin) add(e sdk.Event, now time.Time) (*sdk.Event, error) {
	if e.Metadata["buffer_record_id"] != "" {
		return nil, errors.New("join must precede durable buffer; input ACK aggregation unsupported")
	}
	parts, ok := e.Payload["parts"].(map[string]any)
	if !ok {
		return nil, errors.New("parts object required")
	}
	id, ok := parts["id"].(string)
	count, valid := integer(parts["count"])
	index, validIndex := integer(parts["index"])
	value, present := e.Payload[p.cfg.InputField]
	if !ok || id == "" || len(id) > 1024 || !valid || count < 1 || count > p.cfg.MaxParts || !validIndex || index >= count || !present {
		return nil, errors.New("invalid part correlation")
	}
	raw, err := json.Marshal(e)
	if err != nil || len(raw) > 1048576 {
		return nil, errors.New("part event exceeds 1 MiB")
	}
	valueRaw, _ := json.Marshal(value)
	digest := sha256.Sum256(valueRaw)
	owned := e.Clone()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("join stopped")
	}
	g := p.groups[id]
	if g == nil {
		if len(p.groups) >= p.cfg.MaxGroups || p.bytes+len(raw)+len(valueRaw) > p.cfg.MaxBytes {
			return nil, errors.New("join capacity exceeded")
		}
		g = &group{base: owned, items: map[int]any{}, digests: map[int][32]byte{}, count: count, bytes: len(raw), due: now.Add(time.Duration(p.cfg.TimeoutMS) * time.Millisecond)}
		p.groups[id] = g
		p.bytes += len(raw)
	}
	if g.done {
		return nil, errors.New("group already closed; replay ignored within retention window")
	}
	if !now.Before(g.due) {
		return nil, errors.New("late part beyond join deadline")
	}
	if g.count != count {
		return nil, errors.New("conflicting part count")
	}
	if previous, exists := g.digests[index]; exists {
		if previous != digest {
			return nil, errors.New("duplicate index with conflicting value")
		}
		return nil, nil
	}
	if p.bytes+len(valueRaw) > p.cfg.MaxBytes {
		return nil, errors.New("join byte capacity exceeded")
	}
	g.items[index] = owned.Payload[p.cfg.InputField]
	g.digests[index] = digest
	g.bytes += len(valueRaw)
	p.bytes += len(valueRaw)
	if len(g.items) == g.count {
		out := p.output(id, g, false)
		p.closeGroup(g)
		return &out, nil
	}
	return nil, nil
}
func (p *plugin) output(id string, g *group, partial bool) sdk.Event {
	out := g.base.Clone()
	records := []any{}
	missing := []any{}
	for i := 0; i < g.count; i++ {
		if value, ok := g.items[i]; ok {
			records = append(records, value)
		} else {
			records = append(records, nil)
			missing = append(missing, float64(i))
		}
	}
	delete(out.Payload, "parts")
	out.Payload[p.cfg.OutputField] = records
	out.Payload["join"] = map[string]any{"id": id, "partial": partial, "missing": missing}
	hash := sha256.Sum256([]byte(id))
	out.ID = "evt_join_" + hex.EncodeToString(hash[:16])
	return out
}
func (p *plugin) closeGroup(g *group) {
	p.bytes -= g.bytes
	g.bytes = 0
	g.items = nil
	g.digests = nil
	g.base = sdk.Event{}
	g.done = true
}
func (p *plugin) expire(now time.Time) []sdk.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []sdk.Event{}
	for id, g := range p.groups {
		if !g.due.After(now) {
			if !g.done && p.cfg.EmitPartial {
				out = append(out, p.output(id, g, true))
			}
			if !g.done {
				p.closeGroup(g)
			}
			delete(p.groups, id)
		}
	}
	return out
}
func (p *plugin) loop() {
	defer p.wg.Done()
	timer := time.NewTicker(time.Duration(min(p.cfg.TimeoutMS, 20)) * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-p.stop:
			return
		case now := <-timer.C:
			for _, e := range p.expire(now) {
				if p.ctx.Emitter().Publish(e) != nil {
					p.ctx.Metrics().IncCounter("join_emit_failures", nil)
				}
			}
		}
	}
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
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	out, err := p.add(e, time.Now())
	if err != nil {
		return sdk.SkippableError(err)
	}
	if out != nil {
		return p.ctx.Emitter().Publish(*out)
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
