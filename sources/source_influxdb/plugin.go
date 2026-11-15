package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"strings"
	"sync"
	"time"
)

type config struct {
	ConnectorRef string         `json:"connector_ref"`
	DeviceCode   string         `json:"device_code"`
	Query        string         `json:"query"`
	Params       map[string]any `json:"params"`
	IntervalMS   int            `json:"interval_ms"`
	TimeoutMS    int            `json:"timeout_ms"`
	MaxRows      int            `json:"max_rows"`
}
type plugin struct {
	sdk.BasePlugin
	cfg     config
	ctx     sdk.RuntimeContext
	stop    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
	request []byte
}

const configSchema = `{"type":"object","required":["connector_ref","device_code","query"],"properties":{"connector_ref":{"type":"string","minLength":1},"device_code":{"type":"string","minLength":1,"maxLength":128},"query":{"type":"string","minLength":1,"maxLength":65536},"params":{"type":"object","maxProperties":128,"additionalProperties":{"type":["string","number","boolean","null"]}},"interval_ms":{"type":"integer","minimum":100,"maximum":86400000,"default":5000},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"max_rows":{"type":"integer","minimum":1,"maximum":10000,"default":1000}},"additionalProperties":false}`

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-timeseries-influxdb", Name: "InfluxDB Flux Poll Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded periodic Flux snapshots with latest-only scheduling", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"influxdb"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{IntervalMS: 5000, TimeoutMS: 5000, MaxRows: 1000}
	if len(raw.Raw) > 1048576 {
		return sdk.FatalError(errors.New("configuration exceeds 1 MiB"))
	}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p.cfg); err != nil {
		return sdk.FatalError(errors.New("invalid Flux polling config"))
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return sdk.FatalError(errors.New("trailing configuration data"))
	}
	if p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || strings.TrimSpace(p.cfg.Query) == "" || len(p.cfg.Query) > 65536 || len(p.cfg.Params) > 128 || p.cfg.IntervalMS < 100 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.MaxRows < 1 || p.cfg.MaxRows > 10000 {
		return sdk.FatalError(errors.New("invalid Flux polling limits"))
	}
	for name, value := range p.cfg.Params {
		if name == "" || len(name) > 128 {
			return sdk.FatalError(errors.New("invalid parameter name"))
		}
		switch v := value.(type) {
		case nil, bool:
		case string:
			if len(v) > 65536 {
				return sdk.FatalError(errors.New("parameter string too large"))
			}
		case float64:
			if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 9007199254740991 {
				return sdk.FatalError(errors.New("unsafe numeric parameter; use a decimal string with explicit Flux conversion"))
			}
		default:
			return sdk.FatalError(errors.New("Flux parameters must be scalar"))
		}
	}
	var err error
	p.request, err = json.Marshal(map[string]any{"query": p.cfg.Query, "params": p.cfg.Params})
	return err
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("bound connector and emitter required")
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
				m.IncCounter("poll_failures", nil)
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
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "query", Payload: append([]byte(nil), p.request...), TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return errors.New("Flux snapshot query failed")
	}
	rows, err := decodeRows(response.Payload, p.cfg.MaxRows)
	if err != nil {
		return err
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": map[string]any{"rows": rows, "row_count": len(rows)}, "time": time.Now().UnixMilli()}, nil)
	sdk.SetTraceID(&event, event.ID)
	select {
	case <-p.stop:
		return nil
	default:
	}
	return p.ctx.Emitter().Publish(event)
}
func decodeRows(raw []byte, limit int) ([]map[string]any, error) {
	if len(raw) > 8388608 {
		return nil, errors.New("Flux response exceeds 8 MiB")
	}
	var result struct {
		Status   int              `json:"status"`
		Rows     []map[string]any `json:"rows"`
		RowCount int              `json:"row_count"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&result); err != nil {
		return nil, errors.New("invalid Flux result")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing Flux result")
	}
	if result.Status < 200 || result.Status >= 300 || result.Rows == nil || len(result.Rows) > limit || result.RowCount != len(result.Rows) {
		return nil, errors.New("Flux result status, row count or limit invalid")
	}
	for _, row := range result.Rows {
		if row == nil || len(row) > 256 {
			return nil, errors.New("invalid Flux columns")
		}
		for _, v := range row {
			if _, ok := v.(string); !ok {
				return nil, errors.New("Flux cells must remain strings")
			}
		}
	}
	return result.Rows, nil
}
