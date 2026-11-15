package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"regexp"
	"sync"
	"time"
)

type config struct {
	ConnectorRef string          `json:"connector_ref"`
	Collection   string          `json:"collection"`
	DeviceCode   string          `json:"device_code"`
	Filter       json.RawMessage `json:"filter"`
	Sort         string          `json:"sort"`
	Descending   bool            `json:"descending"`
	Limit        int             `json:"limit"`
	TimeoutMS    int             `json:"timeout_ms"`
	IntervalMS   int             `json:"interval_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg     config
	ctx     sdk.RuntimeContext
	request []byte
	stop    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-mongodb", Name: "MongoDB Poll Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded majority-read snapshots with latest-only scheduling; not change streams", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"mongodb"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Filter: json.RawMessage(`{}`), Limit: 1000, TimeoutMS: 5000, IntervalMS: 5000}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 131072 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`).MatchString(p.cfg.Collection) || p.cfg.Sort != "" && !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,7}$`).MatchString(p.cfg.Sort) || p.cfg.Limit < 1 || p.cfg.Limit > 1000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.IntervalMS < 100 || p.cfg.IntervalMS > 86400000 || len(p.cfg.Filter) > 65536 {
		return sdk.FatalError(errors.New("invalid MongoDB source config"))
	}
	var filter map[string]json.RawMessage
	if json.Unmarshal(p.cfg.Filter, &filter) != nil || filter == nil {
		return sdk.FatalError(errors.New("MongoDB filter must be an object"))
	}
	var err error
	p.request, err = json.Marshal(map[string]any{"collection": p.cfg.Collection, "filter": p.cfg.Filter, "sort": p.cfg.Sort, "descending": p.cfg.Descending, "limit": p.cfg.Limit})
	return err
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("bound MongoDB connector and emitter required")
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
	next := time.Now()
	interval := time.Duration(p.cfg.IntervalMS) * time.Millisecond
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
				m.IncCounter("mongodb_poll_failures", nil)
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
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "find", Payload: append([]byte(nil), p.request...), TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return errors.New("MongoDB snapshot failed")
	}
	var result struct {
		Documents []map[string]any `json:"documents"`
		Count     int              `json:"count"`
	}
	d := json.NewDecoder(bytes.NewReader(response.Payload))
	d.UseNumber()
	d.DisallowUnknownFields()
	if len(response.Payload) > 5*1048576 || d.Decode(&result) != nil || d.Decode(new(any)) != io.EOF || result.Documents == nil || result.Count != len(result.Documents) || len(result.Documents) > p.cfg.Limit {
		return errors.New("invalid or oversized MongoDB snapshot")
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": map[string]any{"documents": result.Documents, "count": result.Count}, "time": time.Now().UnixMilli()}, nil)
	sdk.SetTraceID(&event, event.ID)
	select {
	case <-p.stop:
		return nil
	default:
	}
	return p.ctx.Emitter().Publish(event)
}

const configSchema = `{"type":"object","required":["connector_ref","collection","device_code"],"properties":{"connector_ref":{"type":"string"},"collection":{"type":"string"},"device_code":{"type":"string"},"filter":{"type":"object","default":{},"additionalProperties":true},"sort":{"type":"string"},"descending":{"type":"boolean","default":false},"limit":{"type":"integer","minimum":1,"maximum":1000,"default":1000},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"interval_ms":{"type":"integer","minimum":100,"maximum":86400000,"default":5000}},"additionalProperties":false}`
