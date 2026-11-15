package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync"
	"time"
)

type point struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}
type config struct {
	ConnectorRef string  `json:"connector_ref"`
	DeviceCode   string  `json:"device_code"`
	Points       []point `json:"points"`
	IntervalMS   int     `json:"interval_ms"`
	TimeoutMS    int     `json:"timeout_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg  config
	ctx  sdk.RuntimeContext
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-lsv2", Name: "LSV2 Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Latest-only bounded named LSV2 read queries", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"lsv2"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{IntervalMS: 5000, TimeoutMS: 5000}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if len(p.cfg.Points) < 1 || len(p.cfg.Points) > 64 || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.IntervalMS < 50 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid HTTP polling config"))
	}
	seen := map[string]bool{}
	allowed := map[string]bool{}
	for _, query := range []string{"version", "control_id", "release", "splc_version", "execution_state", "program_state", "nc_uptime", "machine_uptime", "machine_runtime", "spindle_runtime_0", "spindle_runtime_1", "overrides", "current_tool", "axis_locations", "selected_program"} {
		allowed[query] = true
	}
	for _, point := range p.cfg.Points {
		if point.Name == "" || seen[point.Name] || !allowed[point.Query] {
			return sdk.FatalError(errors.New("invalid or duplicate LSV2 query"))
		}
		seen[point.Name] = true
	}

	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound connector required")
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
func (p *plugin) loop() {
	defer p.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	interval := time.Duration(p.cfg.IntervalMS) * time.Millisecond
	next := time.Now()
	failures := 0
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		if err := p.poll(); err != nil {
			failures++
			p.ctx.Metrics().IncCounter("poll_failures", nil)
		} else {
			failures = 0
		}
		next = next.Add(interval)
		now := time.Now()
		if !next.After(now) {
			skipped := now.Sub(next)/interval + 1
			next = next.Add(skipped * interval)
			p.ctx.Metrics().Observe("dropped_scans", float64(skipped), nil)
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
func (p *plugin) poll() error {
	raw, _ := json.Marshal(map[string]any{"points": p.cfg.Points})
	response, readErr := p.ctx.Connector().BatchRead(sdk.ConnectorRequest{Target: "queries", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	var result struct {
		Values  map[string]any `json:"values"`
		Quality map[string]any `json:"quality"`
	}
	if readErr == nil {
		readErr = json.Unmarshal(response.Payload, &result)
	}
	data := map[string]any{}
	quality := map[string]any{}
	for _, point := range p.cfg.Points {
		if readErr != nil {
			data[point.Name] = nil
			quality[point.Name] = "bad:read"
		} else {
			data[point.Name] = result.Values[point.Name]
			quality[point.Name] = result.Quality[point.Name]
		}
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": data, "quality": quality, "time": time.Now().UnixMilli()}, map[string]string{"protocol": "lsv2"})
	sdk.SetTraceID(&event, event.ID)
	if err := p.ctx.Emitter().Publish(event); err != nil {
		return err
	}
	return readErr
}
