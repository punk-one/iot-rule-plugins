package main

import (
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"strings"
	"sync"
	"time"
)

type config struct {
	KeyPath   string  `json:"key_path"`
	Rate      float64 `json:"rate_per_second"`
	Burst     int     `json:"burst"`
	MaxKeys   int     `json:"max_keys"`
	IdleTTLMS int     `json:"idle_ttl_ms"`
}
type bucket struct {
	tokens float64
	last   time.Time
	seen   time.Time
}
type plugin struct {
	sdk.BasePlugin
	cfg     config
	ctx     sdk.RuntimeContext
	mu      sync.Mutex
	buckets map[string]*bucket
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "rate-limit", Name: "Rate Limit", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded per-key token buckets with explicit drop semantics and metrics", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: `{"type":"object","required":["rate_per_second"],"properties":{"key_path":{"type":"string","default":"device_code"},"rate_per_second":{"type":"number","exclusiveMinimum":0,"maximum":100000},"burst":{"type":"integer","minimum":1,"maximum":10000,"default":1},"max_keys":{"type":"integer","minimum":1,"maximum":10000,"default":1024},"idle_ttl_ms":{"type":"integer","minimum":1000,"maximum":86400000,"default":300000}},"additionalProperties":false}`}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyPath: "device_code", Burst: 1, MaxKeys: 1024, IdleTTLMS: 300000}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.KeyPath == "" || p.cfg.Rate <= 0 || p.cfg.Rate > 100000 || math.IsNaN(p.cfg.Rate) || math.IsInf(p.cfg.Rate, 0) || p.cfg.Burst < 1 || p.cfg.Burst > 10000 || p.cfg.MaxKeys < 1 || p.cfg.MaxKeys > 10000 || p.cfg.IdleTTLMS < 1000 || p.cfg.IdleTTLMS > 86400000 {
		return sdk.FatalError(errors.New("invalid token bucket settings"))
	}
	p.buckets = map[string]*bucket{}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	p.ctx = ctx
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
func (p *plugin) allow(key string, now time.Time) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.buckets[key]
	if b == nil {
		if len(p.buckets) >= p.cfg.MaxKeys {
			ttl := time.Duration(p.cfg.IdleTTLMS) * time.Millisecond
			for k, old := range p.buckets {
				if now.Sub(old.seen) >= ttl {
					delete(p.buckets, k)
				}
			}
		}
		if len(p.buckets) >= p.cfg.MaxKeys {
			return false, errors.New("rate-limit key capacity exceeded")
		}
		b = &bucket{tokens: float64(p.cfg.Burst), last: now, seen: now}
		p.buckets[key] = b
	}
	delta := now.Sub(b.last).Seconds()
	if delta > 0 {
		b.tokens = math.Min(float64(p.cfg.Burst), b.tokens+delta*p.cfg.Rate)
		b.last = now
	}
	b.seen = now
	if b.tokens < 1 {
		return false, nil
	}
	b.tokens--
	return true, nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if e.Metadata["buffer_record_id"] != "" {
		return sdk.SkippableError(errors.New("place this lossy/analytical node before the persistent buffer; input ACK aggregation is unsupported"))
	}
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	v, ok := lookup(e.Payload, p.cfg.KeyPath)
	if !ok || v == nil {
		return sdk.SkippableError(errors.New("rate-limit key missing"))
	}
	switch v.(type) {
	case string, bool, float64, int, int64, uint64, json.Number:
	default:
		return sdk.SkippableError(errors.New("rate-limit key must be scalar"))
	}
	key, err := scalarKey(v)
	if err != nil {
		return sdk.SkippableError(err)
	}
	if len(key) > 1024 {
		return sdk.SkippableError(errors.New("rate-limit key too long"))
	}
	allowed, err := p.allow(key, time.Now())
	if err != nil {
		return sdk.SkippableError(err)
	}
	if !allowed {
		if m := p.ctx.Metrics(); m != nil {
			m.IncCounter("rate_limited_events", nil)
		}
		return nil
	}
	return p.ctx.Emitter().Publish(e.Clone())
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}

// Prefix the representation with its type so 1, "1", and true are separate keys.
func scalarKey(v any) (string, error) {
	switch n := v.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return "", errors.New("non-finite key")
		}
	case json.Number:
		if _, err := n.Float64(); err != nil {
			return "", errors.New("invalid numeric key")
		}
	}
	return fmt.Sprintf("%T:%v", v, v), nil
}
