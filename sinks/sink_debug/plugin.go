package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type config struct {
	SampleEvery   int      `json:"sample_every"`
	MaxPerSecond  int      `json:"max_per_second"`
	Fields        []string `json:"fields"`
	IncludeValues bool     `json:"include_values"`
}
type plugin struct {
	sdk.BasePlugin
	cfg     config
	ctx     sdk.RuntimeContext
	mu      sync.Mutex
	counter uint64
	window  time.Time
	emitted int
	closed  bool
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-debug", Name: "Sampled Debug Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Sampled bounded debug metadata with explicit scalar-field opt-in and secret-name redaction", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{SampleEvery: 100, MaxPerSecond: 10}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.SampleEvery < 1 || p.cfg.SampleEvery > 1000000 || p.cfg.MaxPerSecond < 1 || p.cfg.MaxPerSecond > 100 || len(p.cfg.Fields) > 16 {
		return sdk.FatalError(errors.New("invalid debug quota"))
	}
	for _, path := range p.cfg.Fields {
		if path == "" || len(path) > 256 {
			return sdk.FatalError(errors.New("invalid debug field path"))
		}
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Logger() == nil {
		return errors.New("runtime logger required")
	}
	p.ctx = ctx
	return nil
}
func clipped(v string, n int) string {
	if len(v) <= n {
		return v
	}
	v = v[:n]
	for !utf8.ValidString(v) {
		v = v[:len(v)-1]
	}
	return v + "…"
}
func sensitive(path string) bool {
	path = strings.ToLower(path)
	for _, word := range []string{"password", "passwd", "secret", "credential", "token", "authorization", "cookie", "private_key", "apikey", "api_key"} {
		if strings.Contains(path, word) {
			return true
		}
	}
	return false
}
func lookup(root map[string]any, path string) (any, bool) {
	var v any = root
	for _, part := range strings.Split(strings.TrimPrefix(path, "payload."), ".") {
		object, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return v, true
}
func (p *plugin) summary(e sdk.Event) map[string]any {
	out := map[string]any{"event_id": clipped(e.ID, 128), "trace_id": clipped(sdk.GetTraceID(e), 128), "payload_field_count": len(e.Payload)}
	if !p.cfg.IncludeValues {
		return out
	}
	fields := map[string]any{}
	paths := append([]string(nil), p.cfg.Fields...)
	sort.Strings(paths)
	for _, path := range paths {
		if sensitive(path) {
			fields[path] = "[REDACTED]"
			continue
		}
		v, ok := lookup(e.Payload, path)
		if !ok {
			fields[path] = "[MISSING]"
			continue
		}
		switch value := v.(type) {
		case nil, bool, float64, int, int64, uint64, json.Number:
			raw, err := json.Marshal(value)
			if err == nil && len(raw) <= 256 {
				fields[path] = value
			} else {
				fields[path] = "[INVALID]"
			}
		case string:
			fields[path] = clipped(value, 256)
		default:
			fields[path] = "[CONTAINER OMITTED]"
		}
	}
	out["fields"] = fields
	return out
}
func (p *plugin) allow(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.counter++
	if p.counter%uint64(p.cfg.SampleEvery) != 0 {
		return false
	}
	if p.window.IsZero() || now.Sub(p.window) >= time.Second {
		p.window = now
		p.emitted = 0
	}
	if p.emitted >= p.cfg.MaxPerSecond {
		return false
	}
	p.emitted++
	return true
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	if e.Metadata["buffer_record_id"] != "" {
		return sdk.SkippableError(errors.New("debug logs are not a durable buffer delivery target"))
	}
	if p.allow(time.Now()) {
		raw, err := json.Marshal(p.summary(e))
		if err != nil {
			return sdk.SkippableError(errors.New("debug summary invalid"))
		}
		p.ctx.Logger().Info("sampled rule event", sdk.Field{Key: "summary", Value: string(raw)})
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
func (p *plugin) Stop() error { p.mu.Lock(); p.closed = true; p.mu.Unlock(); return nil }
