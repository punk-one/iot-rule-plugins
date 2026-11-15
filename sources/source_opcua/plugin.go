package main

import (
	"encoding/json"
	"errors"
	"github.com/gopcua/opcua/ua"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync"
	"time"
)

type point struct {
	Name         string `json:"name"`
	NodeID       string `json:"node_id"`
	NamespaceURI string `json:"namespace_uri,omitempty"`
}
type config struct {
	ConnectorRef      string  `json:"connector_ref"`
	Mode              string  `json:"mode"`
	EventNodeID       string  `json:"event_node_id"`
	EventNamespaceURI string  `json:"event_namespace_uri"`
	Buffer            int     `json:"buffer_messages"`
	DeviceCode        string  `json:"device_code"`
	IntervalMS        int     `json:"interval_ms"`
	TimeoutMS         int     `json:"timeout_ms"`
	Points            []point `json:"points"`
}
type plugin struct {
	sdk.BasePlugin
	cfg    config
	ctx    sdk.RuntimeContext
	stop   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
	mu     sync.Mutex
	stream sdk.StreamConsumer
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-opcua", Name: "OPC UA Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Batched polling or bounded data/event subscriptions with quality and source timestamps", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"opcua"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{IntervalMS: 5000, TimeoutMS: 5000, Mode: "poll", Buffer: 16, EventNodeID: "i=2253"}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.IntervalMS < 50 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || (p.cfg.Mode != "events" && len(p.cfg.Points) == 0) || len(p.cfg.Points) > 1024 || (p.cfg.Mode != "poll" && p.cfg.Mode != "subscription" && p.cfg.Mode != "events") || p.cfg.Buffer < 1 || p.cfg.Buffer > 32 || (p.cfg.Mode != "poll" && p.cfg.IntervalMS > 60000) || (p.cfg.Mode == "subscription" && len(p.cfg.Points) > 256) || (p.cfg.Mode == "events" && len(p.cfg.Points) != 0) {
		return sdk.FatalError(errors.New("invalid OPC UA source settings"))
	}
	if p.cfg.Mode == "events" {
		if _, err := ua.ParseNodeID(p.cfg.EventNodeID); err != nil {
			return sdk.FatalError(errors.New("invalid event node ID"))
		}
	}
	seen := map[string]bool{}
	for _, v := range p.cfg.Points {
		if v.Name == "" || seen[v.Name] {
			return sdk.FatalError(errors.New("point name missing or duplicated"))
		}
		if _, err := ua.ParseNodeID(v.NodeID); err != nil {
			return sdk.FatalError(errors.New("invalid NodeId"))
		}
		seen[v.Name] = true
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("bound connector required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.wg.Add(1)
	if p.cfg.Mode == "poll" {
		go p.loop()
	} else {
		go p.subscriptionLoop()
	}
	return nil
}
func (p *plugin) Stop() error {
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
		p.mu.Lock()
		stream := p.stream
		p.stream = nil
		p.mu.Unlock()
		if stream != nil {
			stream.Close()
		}
	})
	p.wg.Wait()
	return nil
}
func (p *plugin) loop() {
	defer p.wg.Done()
	interval := time.Duration(p.cfg.IntervalMS) * time.Millisecond
	next := time.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()
	failures := 0
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		if err := p.poll(); err != nil {
			failures++
			if m := p.ctx.Metrics(); m != nil {
				m.IncCounter("scan_failures", nil)
			}
		} else {
			failures = 0
		}
		next = next.Add(interval)
		now := time.Now()
		if !next.After(now) {
			skipped := now.Sub(next)/interval + 1
			next = next.Add(skipped * interval)
			if m := p.ctx.Metrics(); m != nil {
				m.Observe("dropped_scans", float64(skipped), nil)
			}
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
	response, readErr := p.ctx.Connector().BatchRead(sdk.ConnectorRequest{Target: "points", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	decoded := struct {
		Values  map[string]any    `json:"values"`
		Quality map[string]string `json:"quality"`
		Times   map[string]int64  `json:"source_time"`
	}{}
	if readErr == nil {
		readErr = json.Unmarshal(response.Payload, &decoded)
	}
	if readErr == nil {
		if len(decoded.Values) != len(p.cfg.Points) || len(decoded.Quality) != len(p.cfg.Points) {
			readErr = errors.New("incomplete OPC UA point result")
		} else {
			for _, point := range p.cfg.Points {
				if _, ok := decoded.Values[point.Name]; !ok || decoded.Quality[point.Name] == "" {
					readErr = errors.New("OPC UA point quality/value missing")
					break
				}
			}
		}
	}
	if readErr != nil {
		decoded.Values = map[string]any{}
		decoded.Quality = map[string]string{}
		for _, point := range p.cfg.Points {
			decoded.Values[point.Name] = nil
			decoded.Quality[point.Name] = "bad:read"
		}
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": decoded.Values, "quality": decoded.Quality, "source_time": decoded.Times, "time": time.Now().UnixMilli()}, nil)
	sdk.SetTraceID(&event, event.ID)
	if p.stopped() {
		return nil
	}
	if err := p.ctx.Emitter().Publish(event); err != nil {
		return err
	}
	return readErr
}
