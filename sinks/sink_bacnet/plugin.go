package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"time"
)

type point struct {
	Name       string  `json:"name"`
	ObjectType uint32  `json:"object_type"`
	Instance   uint32  `json:"instance"`
	Property   uint32  `json:"property"`
	ArrayIndex *uint32 `json:"array_index,omitempty"`
	ValueType  string  `json:"valueType"`
	Priority   int     `json:"priority,omitempty"`
	Value      any     `json:"value,omitempty"`
}
type config struct {
	ConnectorRef string  `json:"connector_ref"`
	DeviceCode   string  `json:"device_code"`
	Points       []point `json:"points"`
	TimeoutMS    int     `json:"timeout_ms"`
	MaxHorizonMS int     `json:"max_command_horizon_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-bacnet", Name: "BACnet/IP Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Allowlisted expiring scalar property writes; property ACK is not physical completion", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"bacnet"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 3000, MaxHorizonMS: 10000}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if len(raw.Raw) > 262144 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.MaxHorizonMS < 100 || p.cfg.MaxHorizonMS > 60000 || len(p.cfg.Points) < 1 || len(p.cfg.Points) > 128 {
		return sdk.FatalError(errors.New("invalid BACnet sink configuration"))
	}
	seen := map[string]bool{}
	for _, v := range p.cfg.Points {
		if v.Name == "" || len(v.Name) > 128 || seen[v.Name] || v.ObjectType > 1023 || v.Instance >= 4194303 || v.Property > 4194303 || v.Priority < 0 || v.Priority > 16 || v.Value != nil {
			return sdk.FatalError(errors.New("invalid/duplicate fixed BACnet write address"))
		}
		seen[v.Name] = true
		switch v.ValueType {
		case "Null", "Bool", "Uint32", "Int32", "Float32", "Float64", "Enum", "String", "Bytes", "Bits", "ObjectID":
		default:
			return sdk.FatalError(errors.New("unsupported BACnet write type"))
		}
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound BACnet connector required")
	}
	p.ctx = ctx
	return nil
}
func millis(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) && math.Trunc(v) == v && math.Abs(v) <= 9007199254740991 {
			return int64(v), true
		}
	case json.Number:
		n, e := v.Int64()
		return n, e == nil
	}
	return 0, false
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("BACnet sink not started"))
	}
	if e.Metadata["buffer_record_id"] != "" {
		return sdk.SkippableError(errors.New("BACnet physical writes reject automatic buffered replay; use command-guard and explicit reconciliation"))
	}
	id, ok := e.Payload["command_id"].(string)
	expires, valid := millis(e.Payload["expires_at"])
	now := time.Now().UnixMilli()
	if e.Payload["device_code"] != p.cfg.DeviceCode || !ok || id == "" || len(id) > 256 || !valid || expires <= now || expires > now+int64(p.cfg.MaxHorizonMS) {
		return sdk.SkippableError(errors.New("BACnet command requires matching device_code, command_id and live expires_at"))
	}
	values, ok := e.Payload["data"].(map[string]any)
	if !ok {
		return sdk.SkippableError(errors.New("BACnet command data object required"))
	}
	points := make([]point, len(p.cfg.Points))
	for i, v := range p.cfg.Points {
		value, exists := values[v.Name]
		if !exists {
			return sdk.SkippableError(errors.New("configured BACnet write value missing"))
		}
		points[i] = v
		points[i].Value = value
	}
	raw, err := json.Marshal(map[string]any{"points": points, "expires_at": expires})
	if err != nil || len(raw) > 262144 {
		return sdk.SkippableError(errors.New("invalid/oversized BACnet write values"))
	}
	response, err := p.ctx.Connector().BatchWrite(sdk.ConnectorRequest{Target: "points", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("BACnet write failed; may be partial/unknown, do not transparently retry"))
	}
	var receipt struct {
		Written  int    `json:"written"`
		Scope    string `json:"scope"`
		Physical bool   `json:"physical_completion"`
	}
	if len(response.Payload) > 1024 || json.Unmarshal(response.Payload, &receipt) != nil || receipt.Written != len(points) || receipt.Scope != "property_ack" || receipt.Physical {
		return sdk.SkippableError(errors.New("invalid BACnet property acknowledgement"))
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
