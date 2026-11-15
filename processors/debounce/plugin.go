package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"sync"
	"time"
)

const triggerMode = false

type config struct {
	KeyPath  string `json:"key_path"`
	DelayMS  int    `json:"delay_ms"`
	MaxKeys  int    `json:"max_keys"`
	MaxBytes int    `json:"max_bytes"`
}
type pending struct {
	event sdk.Event
	due   time.Time
	size  int
}
type plugin struct {
	sdk.BasePlugin
	cfg     config
	ctx     sdk.RuntimeContext
	mu      sync.Mutex
	pending map[string]pending
	bytes   int
	closed  bool
	stop    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "debounce", Name: "Debounce", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded volatile per-key latest event timer", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyPath: "device_code", DelayMS: 1000, MaxKeys: 1024, MaxBytes: 8388608}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.KeyPath == "" || p.cfg.DelayMS < 10 || p.cfg.DelayMS > 86400000 || p.cfg.MaxKeys < 1 || p.cfg.MaxKeys > 10000 || p.cfg.MaxBytes < 1024 || p.cfg.MaxBytes > 67108864 {
		return sdk.FatalError(errors.New("invalid bounded timer settings"))
	}
	p.pending = map[string]pending{}
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
func (p *plugin) store(event sdk.Event, now time.Time) error {
	if event.Metadata["buffer_record_id"] != "" {
		return errors.New("timer must precede durable buffer; input ACK aggregation is unsupported")
	}
	value, ok := lookup(event.Payload, p.cfg.KeyPath)
	key, isString := value.(string)
	if !ok || !isString || key == "" || len(key) > 1024 {
		return errors.New("timer key must be a bounded string")
	}
	raw, err := json.Marshal(event)
	if err != nil || len(raw) > 1048576 {
		return errors.New("timer event exceeds 1 MiB")
	}
	owned := event.Clone()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("timer stopped")
	}
	old, exists := p.pending[key]
	if !exists && len(p.pending) >= p.cfg.MaxKeys || p.bytes-old.size+len(raw) > p.cfg.MaxBytes {
		return errors.New("timer capacity exceeded")
	}
	p.pending[key] = pending{event: owned, due: now.Add(time.Duration(p.cfg.DelayMS) * time.Millisecond), size: len(raw)}
	p.bytes += len(raw) - old.size
	return nil
}
func (p *plugin) take(now time.Time) []sdk.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []sdk.Event{}
	for key, item := range p.pending {
		if !item.due.After(now) {
			out = append(out, item.event)
			p.bytes -= item.size
			delete(p.pending, key)
		}
	}
	return out
}
func (p *plugin) loop() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Duration(min(p.cfg.DelayMS, 20)) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case now := <-ticker.C:
			for _, event := range p.take(now) {
				var err error
				if triggerMode {
					event.Payload["trigger_timeout"] = true
					err = p.ctx.Emitter().EmitTo("timeout", event)
				} else {
					err = p.ctx.Emitter().Publish(event)
				}
				if err != nil {
					p.ctx.Metrics().IncCounter("timer_emit_failures", nil)
				}
			}
		}
	}
}
func (p *plugin) Stop() error {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.pending = map[string]pending{}
		p.bytes = 0
		p.mu.Unlock()
		if p.stop != nil {
			close(p.stop)
		}
	})
	p.wg.Wait()
	return nil
}
func (p *plugin) OnEvent(event sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	if err := p.store(event, time.Now()); err != nil {
		return sdk.SkippableError(err)
	}
	if triggerMode {
		return p.ctx.Emitter().EmitTo("received", event.Clone())
	}
	return nil
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}
