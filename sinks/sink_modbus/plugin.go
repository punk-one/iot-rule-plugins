package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"time"
)

type writePoint struct {
	Count     int    `json:"count,omitempty"`
	Name      string `json:"name"`
	Area      string `json:"area"`
	Address   int    `json:"address"`
	ValueType string `json:"valueType"`
	ByteOrder string `json:"byteOrder,omitempty"`
	UnitID    *int   `json:"unit_id,omitempty"`
	Value     any    `json:"value,omitempty"`
}
type config struct {
	ConnectorRef string       `json:"connector_ref"`
	DeviceCode   string       `json:"device_code"`
	TimeoutMS    int          `json:"timeout_ms"`
	Points       []writePoint `json:"points"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-modbus", Name: "Modbus Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Validate target and write configured Modbus points without transparent retry", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 0, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"modbus"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 3000}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || len(p.cfg.Points) == 0 || len(p.cfg.Points) > 128 {
		return sdk.FatalError(errors.New("invalid sink settings"))
	}
	seen := map[string]bool{}
	for _, point := range p.cfg.Points {
		if point.Name == "" || seen[point.Name] || point.Address < 0 || point.Address > 65535 || (point.Area != "holding" && point.Area != "coil") {
			return sdk.FatalError(errors.New("invalid or duplicate writable point"))
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
	return nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("sink not started"))
	}
	if e.Payload["device_code"] != p.cfg.DeviceCode {
		return sdk.SkippableError(errors.New("write target mismatch"))
	}
	var expiresAt *int64
	if raw, exists := e.Payload["expires_at"]; exists {
		parsed, err := expiry(raw)
		if err != nil {
			return sdk.SkippableError(err)
		}
		expiresAt = parsed
	}
	values, ok := e.Payload["data"].(map[string]any)
	if !ok {
		return sdk.SkippableError(errors.New("data object required"))
	}
	points := make([]writePoint, len(p.cfg.Points))
	for i, point := range p.cfg.Points {
		v, exists := values[point.Name]
		if !exists {
			return sdk.SkippableError(errors.New("configured write value missing"))
		}
		points[i] = point
		points[i].Value = v
	}
	raw, err := json.Marshal(map[string]any{"points": points, "expires_at": expiresAt})
	if err != nil {
		return sdk.SkippableError(err)
	}
	if _, err = p.ctx.Connector().BatchWrite(sdk.ConnectorRequest{Target: "points", Payload: raw, TimeoutMS: p.cfg.TimeoutMS}); err != nil {
		return sdk.SkippableError(errors.New("Modbus write failed or outcome unknown; do not retry automatically"))
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

func expiry(value any) (*int64, error) {
	var n int64
	switch v := value.(type) {
	case int64:
		n = v
	case int:
		n = int64(v)
	case json.Number:
		parsed, err := v.Int64()
		if err != nil {
			return nil, errors.New("expires_at must be integer milliseconds")
		}
		n = parsed
	case float64:
		if v < 0 || v > 9007199254740991 || v != float64(int64(v)) {
			return nil, errors.New("expires_at must be integer milliseconds")
		}
		n = int64(v)
	default:
		return nil, errors.New("expires_at must be integer milliseconds")
	}
	if n <= time.Now().UnixMilli() {
		return nil, errors.New("command expired before device operation")
	}
	return &n, nil
}
