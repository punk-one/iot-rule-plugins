package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"github.com/robfig/cron/v3"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
)

type config struct {
	IntervalMS int            `json:"interval_ms,omitempty"`
	Cron       string         `json:"cron,omitempty"`
	Timezone   string         `json:"timezone"`
	OnStart    bool           `json:"on_start"`
	Payload    map[string]any `json:"payload"`
}
type plugin struct {
	sdk.BasePlugin
	cfg      config
	ctx      sdk.RuntimeContext
	schedule cron.Schedule
	location *time.Location
	stop     chan struct{}
	once     sync.Once
	wg       sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-schedule", Name: "Schedule Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Single-flight interval or five-field cron trigger with explicit timezone and skipped missed ticks", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: `{"type":"object","properties":{"interval_ms":{"type":"integer","minimum":50,"maximum":86400000},"cron":{"type":"string","minLength":1},"timezone":{"type":"string","default":"UTC"},"on_start":{"type":"boolean","default":false},"payload":{"type":"object"}},"oneOf":[{"required":["interval_ms"],"not":{"required":["cron"]}},{"required":["cron"],"not":{"required":["interval_ms"]}}],"additionalProperties":false}`}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Timezone: "UTC"}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if (p.cfg.Cron == "") == (p.cfg.IntervalMS == 0) {
		return sdk.FatalError(errors.New("configure exactly one of interval_ms and cron"))
	}
	loc, err := time.LoadLocation(p.cfg.Timezone)
	if err != nil {
		return sdk.FatalError(errors.New("invalid timezone"))
	}
	p.location = loc
	if p.cfg.Cron != "" {
		if len(strings.Fields(p.cfg.Cron)) != 5 {
			return sdk.FatalError(errors.New("cron must have five fields without embedded timezone"))
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		p.schedule, err = parser.Parse(p.cfg.Cron)
		if err != nil || p.schedule.Next(time.Now().In(loc)).IsZero() {
			return sdk.FatalError(errors.New("invalid or unschedulable five-field cron"))
		}
	} else if p.cfg.IntervalMS < 50 || p.cfg.IntervalMS > 86400000 {
		return sdk.FatalError(errors.New("interval_ms must be 50..86400000"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.wg.Add(1)
	go p.loop()
	return nil
}
func (p *plugin) Stop() error {
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
	})
	p.wg.Wait()
	return nil
}
func (p *plugin) next(after time.Time) time.Time {
	if p.schedule != nil {
		return p.schedule.Next(after.In(p.location))
	}
	return after.Add(time.Duration(p.cfg.IntervalMS) * time.Millisecond)
}
func (p *plugin) loop() {
	defer p.wg.Done()
	next := p.next(time.Now())
	if p.cfg.OnStart {
		next = time.Now()
	}
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		event := sdk.NewEvent(p.cfg.Payload, nil).Clone()
		if event.Payload == nil {
			event.Payload = map[string]any{}
		}
		if err := p.ctx.Emitter().Publish(event); err != nil {
			p.ctx.Metrics().IncCounter("schedule_emit_failures", nil)
		}
		now := time.Now()
		if p.schedule != nil {
			next = p.next(now)
		} else {
			interval := time.Duration(p.cfg.IntervalMS) * time.Millisecond
			next = next.Add(interval)
			if !next.After(now) {
				next = next.Add((now.Sub(next)/interval + 1) * interval)
			}
		}
		if next.IsZero() {
			return
		}
		timer.Reset(time.Until(next))
	}
}
