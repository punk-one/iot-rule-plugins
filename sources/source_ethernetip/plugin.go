package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"sync"
	"time"
)

type point struct {
	Name      string `json:"name"`
	Tag       string `json:"tag"`
	ValueType string `json:"valueType"`

	Scale      *float64 `json:"scale,omitempty"`
	Offset     float64  `json:"offset,omitempty"`
	Precision  *int     `json:"precision,omitempty"`
	Min        *float64 `json:"min,omitempty"`
	Max        *float64 `json:"max,omitempty"`
	OutOfRange string   `json:"outOfRangePolicy,omitempty"`
	Default    any      `json:"defaultValue,omitempty"`
}
type config struct {
	ConnectorRef string  `json:"connector_ref"`
	DeviceCode   string  `json:"device_code"`
	IntervalMS   int     `json:"interval_ms"`
	TimeoutMS    int     `json:"timeout_ms"`
	Points       []point `json:"points"`
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
	return sdk.PluginInfo{ID: "source-ethernetip", Name: "EtherNet/IP Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Batch poll EtherNet/IP TCP points with engineering values and explicit quality", Capabilities: sdk.PluginCapabilities{InputPorts: 0, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"ethernetip"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{IntervalMS: 5000, TimeoutMS: 3000}
	dec := json.NewDecoder(bytes.NewReader(raw.Raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.IntervalMS < 50 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || len(p.cfg.Points) == 0 || len(p.cfg.Points) > 128 {
		return sdk.FatalError(errors.New("invalid source settings"))
	}
	names := map[string]bool{}
	for _, v := range p.cfg.Points {
		if names[v.Name] {
			return sdk.FatalError(errors.New("duplicate point name"))
		}
		names[v.Name] = true
		if err := validatePoint(v); err != nil {
			return sdk.FatalError(err)
		}
	}
	return nil
}
func validatePoint(v point) error {
	if v.Name == "" || v.Tag == "" || len(v.Tag) > 200 {
		return errors.New("point requires name and bounded tag")
	}
	switch v.ValueType {
	case "Bool", "Int64", "Uint64":
		if v.Scale != nil || v.Offset != 0 || v.Precision != nil || v.Min != nil || v.Max != nil {
			return errors.New("exact/bool type does not accept numeric engineering policies")
		}
	case "Int16", "Uint16", "Int32", "Uint32", "Float32", "Float64":
	default:
		return errors.New("unsupported atomic type")
	}
	if v.Precision != nil && (*v.Precision < 0 || *v.Precision > 9) || v.Min != nil && v.Max != nil && *v.Min > *v.Max {
		return errors.New("invalid engineering bounds")
	}
	if v.OutOfRange != "" && v.OutOfRange != "null" && v.OutOfRange != "default" || v.OutOfRange == "default" && v.Default == nil {
		return errors.New("invalid range policy")
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
		err := p.poll()
		if err != nil {
			failures++
			p.ctx.Metrics().IncCounter("scan_failures", nil)
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
			backoff := time.Duration(1<<min(failures-1, 5)) * time.Second
			if next.Before(now.Add(backoff)) {
				next = now.Add(backoff)
			}
		}
		timer.Reset(time.Until(next))
	}
}
func engineering(v any, p point) (any, string) {
	if p.ValueType == "Int64" || p.ValueType == "Uint64" {
		if exact, ok := v.(string); ok {
			return exact, "good"
		}
		return nil, "bad:type"
	}

	if v == nil {
		return nil, "bad:missing"
	}
	if p.ValueType == "Bool" {
		if b, ok := v.(bool); ok {
			return b, "good"
		}
		return nil, "bad:type"
	}
	var n float64
	switch value := v.(type) {
	case float64:
		n = value
	case json.Number:
		var err error
		n, err = value.Float64()
		if err != nil {
			return nil, "bad:type"
		}
	default:
		return nil, "bad:type"
	}
	if p.Scale != nil {
		n *= *p.Scale
	}
	n += p.Offset
	quality := "good"
	if math.IsNaN(n) || math.IsInf(n, 0) {
		quality = "bad:non_finite"
	} else if (p.Min != nil && n < *p.Min) || (p.Max != nil && n > *p.Max) {
		quality = "bad:range"
	}
	if quality != "good" {
		if p.OutOfRange == "default" {
			return p.Default, quality
		}
		return nil, quality
	}
	if p.Precision != nil {
		factor := math.Pow10(*p.Precision)
		if math.Abs(n) > math.MaxFloat64/factor {
			return nil, "bad:non_finite"
		}
		n = math.Round(n*factor) / factor
	}
	return n, "good"
}
func (p *plugin) poll() error {
	start := time.Now()
	points := []map[string]any{}
	for _, p := range p.cfg.Points {
		points = append(points, map[string]any{"name": p.Name, "tag": p.Tag, "valueType": p.ValueType})
	}
	raw, _ := json.Marshal(map[string]any{"points": points})
	response, readErr := p.ctx.Connector().BatchRead(sdk.ConnectorRequest{Target: "points", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	decoded := struct {
		Values   map[string]any `json:"values"`
		Segments int            `json:"segments"`
	}{}
	if readErr == nil {
		readErr = json.Unmarshal(response.Payload, &decoded)
	}
	data := map[string]any{}
	quality := map[string]any{}
	for _, point := range p.cfg.Points {
		if readErr != nil {
			data[point.Name] = nil
			quality[point.Name] = "bad:read"
			continue
		}
		data[point.Name], quality[point.Name] = engineering(decoded.Values[point.Name], point)
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": data, "quality": quality, "time": time.Now().UnixMilli()}, nil)
	sdk.SetTraceID(&event, event.ID)
	p.ctx.Metrics().Observe("scan_duration_ms", float64(time.Since(start).Microseconds())/1000, nil)
	if err := p.ctx.Emitter().Publish(event); err != nil {
		return fmt.Errorf("emit scan: %w", err)
	}
	return readErr
}
