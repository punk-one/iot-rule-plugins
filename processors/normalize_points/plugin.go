package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"reflect"
	"strconv"
)

type pointPolicy struct {
	Name       string   `json:"name"`
	Scale      *float64 `json:"scale,omitempty"`
	Offset     float64  `json:"offset,omitempty"`
	Precision  *int     `json:"precision,omitempty"`
	Min        *float64 `json:"min,omitempty"`
	Max        *float64 `json:"max,omitempty"`
	Default    any      `json:"defaultValue,omitempty"`
	OutOfRange string   `json:"outOfRangePolicy,omitempty"`
}
type config struct {
	Points []pointPolicy `json:"points"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "normalize-points", Name: "Normalize Points", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Engineering value policies with preserved per-point bad quality", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if len(p.cfg.Points) == 0 || len(p.cfg.Points) > 1024 {
		return sdk.FatalError(errors.New("points count must be 1..1024"))
	}
	seen := map[string]bool{}
	for _, v := range p.cfg.Points {
		if v.Name == "" || seen[v.Name] || (v.Precision != nil && (*v.Precision < 0 || *v.Precision > 9)) || (v.Min != nil && v.Max != nil && *v.Min > *v.Max) || (v.OutOfRange != "" && v.OutOfRange != "null" && v.OutOfRange != "default") || (v.OutOfRange == "default" && v.Default == nil) {
			return sdk.FatalError(errors.New("invalid or duplicate point policy"))
		}
		seen[v.Name] = true
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	p.ctx = ctx
	return nil
}
func numeric(value any) (float64, bool) {
	var n float64
	switch v := value.(type) {
	case json.Number:
		parsed, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, false
		}
		n = parsed
	default:
		if value == nil {
			return 0, false
		}
		r := reflect.ValueOf(value)
		switch r.Kind() {
		case reflect.Float32, reflect.Float64:
			n = r.Float()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if r.Int() > 9007199254740991 || r.Int() < -9007199254740991 {
				return 0, false
			}
			n = float64(r.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if r.Uint() > 9007199254740991 {
				return 0, false
			}
			n = float64(r.Uint())
		default:
			return 0, false
		}
	}
	if math.IsNaN(n) || math.IsInf(n, 0) || math.Abs(n) > 9007199254740991 {
		return 0, false
	}
	return n, true
}
func normalize(value any, quality string, p pointPolicy) (any, string) {
	fallback := func(q string) (any, string) {
		if p.OutOfRange == "default" {
			return p.Default, q
		}
		return nil, q
	}
	if quality != "" && quality != "good" {
		return fallback(quality)
	}
	n, ok := numeric(value)
	if !ok {
		return fallback("bad:number")
	}
	if p.Scale != nil {
		n *= *p.Scale
	}
	n += p.Offset
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return fallback("bad:non_finite")
	}
	if (p.Min != nil && n < *p.Min) || (p.Max != nil && n > *p.Max) {
		return fallback("bad:range")
	}
	if p.Precision != nil {
		factor := math.Pow10(*p.Precision)
		if math.Abs(n) > math.MaxFloat64/factor {
			return fallback("bad:non_finite")
		}
		n = math.Round(n*factor) / factor
	}
	return n, "good"
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	out := e.Clone()
	data, ok := out.Payload["data"].(map[string]any)
	if !ok {
		return sdk.SkippableError(errors.New("payload.data object required"))
	}
	qualities, ok := out.Payload["quality"].(map[string]any)
	if !ok {
		qualities = map[string]any{}
		out.Payload["quality"] = qualities
	}
	for _, policy := range p.cfg.Points {
		q := ""
		if raw, exists := qualities[policy.Name]; exists {
			if text, ok := raw.(string); ok {
				q = text
			} else {
				q = "bad:quality"
			}
		}
		data[policy.Name], qualities[policy.Name] = normalize(data[policy.Name], q, policy)
	}
	return p.ctx.Emitter().Publish(out)
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
