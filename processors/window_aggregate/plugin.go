package main

import (
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type config struct {
	KeyPath   string `json:"key_path"`
	ValuePath string `json:"value_path"`
	WindowMS  int    `json:"window_ms"`
	MaxCount  int    `json:"max_count"`
	MaxKeys   int    `json:"max_keys"`
}
type aggregate struct {
	key           string
	start         time.Time
	count         int
	sum, min, max float64
}
type plugin struct {
	sdk.BasePlugin
	cfg    config
	ctx    sdk.RuntimeContext
	mu     sync.Mutex
	groups map[string]*aggregate
	closed bool
	stop   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "window-aggregate", Name: "Window Aggregate", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded processing-time windows with count/min/max/sum/average; volatile analytical state", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: `{"type":"object","required":["value_path"],"properties":{"key_path":{"type":"string","default":"device_code"},"value_path":{"type":"string","minLength":1},"window_ms":{"type":"integer","minimum":50,"maximum":86400000,"default":1000},"max_count":{"type":"integer","minimum":1,"maximum":1000000,"default":1000},"max_keys":{"type":"integer","minimum":1,"maximum":10000,"default":1024}},"additionalProperties":false}`}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyPath: "device_code", WindowMS: 1000, MaxCount: 1000, MaxKeys: 1024}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.KeyPath == "" || p.cfg.ValuePath == "" || p.cfg.WindowMS < 50 || p.cfg.WindowMS > 86400000 || p.cfg.MaxCount < 1 || p.cfg.MaxCount > 1000000 || p.cfg.MaxKeys < 1 || p.cfg.MaxKeys > 10000 {
		return sdk.FatalError(errors.New("invalid aggregation limits"))
	}
	p.groups = map[string]*aggregate{}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.mu.Lock()
	p.closed = false
	p.mu.Unlock()
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
func (p *plugin) add(key string, value float64, now time.Time) ([]aggregate, error) {
	return p.addDisplay(key, key, value, now)
}
func (p *plugin) addDisplay(key, display string, value float64, now time.Time) ([]aggregate, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("aggregator stopped")
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, errors.New("non-finite input")
	}
	var flushed []aggregate
	g := p.groups[key]
	if g != nil && now.Sub(g.start) >= time.Duration(p.cfg.WindowMS)*time.Millisecond {
		flushed = append(flushed, *g)
		delete(p.groups, key)
		g = nil
	}

	if g == nil {
		if len(p.groups) >= p.cfg.MaxKeys {
			return nil, errors.New("aggregation key capacity exceeded")
		}
		g = &aggregate{key: display, start: now, min: value, max: value}
		p.groups[key] = g
	}
	sum := g.sum + value
	if math.IsNaN(sum) || math.IsInf(sum, 0) {
		return nil, errors.New("aggregate overflow")
	}
	g.count++
	g.sum = sum
	g.min = math.Min(g.min, value)
	g.max = math.Max(g.max, value)
	if g.count >= p.cfg.MaxCount {
		copy := *g
		delete(p.groups, key)
		return append(flushed, copy), nil
	}
	return flushed, nil
}
func (p *plugin) drain(now time.Time, all bool) []aggregate {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []aggregate
	for key, g := range p.groups {
		if all || now.Sub(g.start) >= time.Duration(p.cfg.WindowMS)*time.Millisecond {
			out = append(out, *g)
			delete(p.groups, key)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}
func (p *plugin) emit(g aggregate, now time.Time) error {
	event := sdk.NewEvent(map[string]any{"device_code": g.key, "data": map[string]any{"count": g.count, "min": g.min, "max": g.max, "sum": g.sum, "average": g.sum / float64(g.count)}, "window": map[string]any{"start": g.start.UnixMilli(), "end": now.UnixMilli(), "value_path": p.cfg.ValuePath}, "time": now.UnixMilli()}, nil)
	if err := p.ctx.Emitter().Publish(event); err != nil {
		if m := p.ctx.Metrics(); m != nil {
			m.IncCounter("aggregate_emit_failures", nil)
		}
		return err
	}
	return nil
}
func (p *plugin) loop() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Duration(min(p.cfg.WindowMS, 100)) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case now := <-ticker.C:
			for _, g := range p.drain(now, false) {
				_ = p.emit(g, now)
			}
		}
	}
}
func (p *plugin) Stop() error {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		if p.stop != nil {
			close(p.stop)
		}
	})
	p.wg.Wait()
	groups := p.drain(time.Now(), true)
	var result error
	if p.ctx != nil {
		for _, g := range groups {
			if err := p.emit(g, time.Now()); err != nil {
				result = err
			}
		}
	}
	return result
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if e.Metadata["buffer_record_id"] != "" {
		return sdk.SkippableError(errors.New("place this lossy/analytical node before the persistent buffer; input ACK aggregation is unsupported"))
	}
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	keyValue, ok := lookup(e.Payload, p.cfg.KeyPath)
	if !ok || keyValue == nil {
		return sdk.SkippableError(errors.New("aggregation key missing"))
	}
	switch keyValue.(type) {
	case string, float64, int, int64, uint64, json.Number:
	default:
		return sdk.SkippableError(errors.New("aggregation key must be scalar"))
	}
	key := fmt.Sprint(keyValue)
	if len(key) > 1024 {
		return sdk.SkippableError(errors.New("aggregation key too long"))
	}
	v, ok := lookup(e.Payload, p.cfg.ValuePath)
	value, isNumber := v.(float64)
	if !ok || !isNumber {
		return sdk.SkippableError(errors.New("aggregation value must be JSON number"))
	}
	g, err := p.addDisplay(fmt.Sprintf("%T:%v", keyValue, keyValue), key, value, time.Now())
	if err != nil {
		return sdk.SkippableError(err)
	}
	for _, item := range g {
		if err = p.emit(item, time.Now()); err != nil {
			return sdk.SkippableError(errors.New("aggregate output failed; analytical window state is not durable"))
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
