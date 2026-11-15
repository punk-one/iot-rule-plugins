package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"time"
)

type point struct {
	Name     string `json:"name"`
	Number   int    `json:"number"`
	Decimals int    `json:"decimals"`
}
type config struct {
	ConnectorRef string  `json:"connector_ref"`
	DeviceCode   string  `json:"device_code"`
	TimeoutMS    int     `json:"timeout_ms"`
	Points       []point `json:"points"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-fanuc", Name: "FANUC Controlled Macro Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Allowlisted macro writes with target, command ID and absolute expiry; no transparent retries", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"fanuc"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 3000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.TimeoutMS < 1000 || p.cfg.TimeoutMS > 10000 || len(p.cfg.Points) < 1 || len(p.cfg.Points) > 64 {
		return sdk.FatalError(errors.New("invalid FANUC macro sink settings"))
	}
	names := map[string]bool{}
	numbers := map[int]bool{}
	for _, point := range p.cfg.Points {
		if point.Name == "" || names[point.Name] || numbers[point.Number] || point.Number < 1 || point.Number > 9999 || point.Decimals < 0 || point.Decimals > 9 {
			return sdk.FatalError(errors.New("invalid/duplicate macro mapping"))
		}
		names[point.Name] = true
		numbers[point.Number] = true
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("native bound connector required")
	}
	p.ctx = ctx
	return nil
}
func expires(value any) (int64, error) {
	switch n := value.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || math.Trunc(n) != n || n <= float64(time.Now().UnixMilli()) || n > 9007199254740991 {
			return 0, errors.New("invalid/expired command")
		}
		return int64(n), nil
	case int64:
		if n > time.Now().UnixMilli() {
			return n, nil
		}
	case int:
		if int64(n) > time.Now().UnixMilli() {
			return int64(n), nil
		}
	}
	return 0, errors.New("absolute unexpired integer expires_at required")
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	if e.Payload["device_code"] != p.cfg.DeviceCode {
		return sdk.SkippableError(errors.New("FANUC target mismatch"))
	}
	id, ok := e.Payload["command_id"].(string)
	if !ok || id == "" || len(id) > 256 {
		return sdk.SkippableError(errors.New("bounded command_id required; use persistent command-guard upstream"))
	}
	expiry, err := expires(e.Payload["expires_at"])
	if err != nil {
		return sdk.SkippableError(err)
	}
	data, ok := e.Payload["data"].(map[string]any)
	if !ok {
		return sdk.SkippableError(errors.New("macro data object required"))
	}
	points := []map[string]any{}
	for _, point := range p.cfg.Points {
		value, exists := data[point.Name]
		if !exists {
			return sdk.SkippableError(errors.New("configured macro value missing"))
		}
		switch value.(type) {
		case string, float64, int, int64, json.Number:
		default:
			return sdk.SkippableError(errors.New("macro scalar must be decimal text/number"))
		}
		points = append(points, map[string]any{"name": point.Name, "query": "macro", "number": point.Number, "decimals": point.Decimals, "value": value})
	}
	raw, err := json.Marshal(map[string]any{"points": points, "expires_at": expiry})
	if err != nil {
		return sdk.SkippableError(err)
	}
	if _, err = p.ctx.Connector().BatchWrite(sdk.ConnectorRequest{Target: "macro", Payload: raw, TimeoutMS: p.cfg.TimeoutMS}); err != nil {
		return sdk.SkippableError(errors.New("FANUC macro outcome unknown or rejected; do not automatically retry"))
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
