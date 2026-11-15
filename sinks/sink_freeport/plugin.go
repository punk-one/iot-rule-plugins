package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"strings"
	"time"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	DeviceCode   string `json:"device_code"`
	InputPath    string `json:"input_path"`
	TimeoutMS    int    `json:"timeout_ms"`
	MaxBytes     int    `json:"max_bytes"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-freeport", Name: "Freeport Raw Write Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Explicit target and expiring raw writes over bound TCP/serial/UDP; no delivery ACK", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"tcp", "serial", "udp"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{InputPath: "data.raw_base64", TimeoutMS: 5000, MaxBytes: 65507}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.InputPath == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.MaxBytes < 1 || p.cfg.MaxBytes > 1048576 {
		return sdk.FatalError(errors.New("invalid raw write sink config"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound transport required")
	}
	p.ctx = ctx
	return nil
}
func (p *plugin) payload(e sdk.Event) ([]byte, int64, error) {
	if e.Metadata["buffer_record_id"] != "" {
		return nil, 0, errors.New("raw socket write has no durable delivery ACK")
	}
	if e.Payload["device_code"] != p.cfg.DeviceCode {
		return nil, 0, errors.New("raw write target mismatch")
	}
	expiry, ok := e.Payload["expires_at"].(float64)
	if !ok || math.IsNaN(expiry) || math.IsInf(expiry, 0) || math.Trunc(expiry) != expiry || expiry <= float64(time.Now().UnixMilli()) || expiry > 9007199254740991 {
		return nil, 0, errors.New("absolute unexpired integer expires_at required")
	}
	var value any = e.Payload
	for _, part := range strings.Split(p.cfg.InputPath, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return nil, 0, errors.New("raw frame path missing")
		}
		value = object[part]
	}
	encoded, ok := value.(string)
	if !ok || len(encoded) > p.cfg.MaxBytes*2 {
		return nil, 0, errors.New("bounded base64 raw frame required")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) < 1 || len(raw) > p.cfg.MaxBytes {
		return nil, 0, errors.New("raw frame exceeds limit or base64 invalid")
	}
	return raw, int64(expiry), nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	body, expiry, err := p.payload(e)
	if err != nil {
		return sdk.SkippableError(err)
	}
	raw, _ := json.Marshal(map[string]any{"data": body, "expires_at": expiry})
	if _, err = p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "send", Payload: raw, TimeoutMS: p.cfg.TimeoutMS}); err != nil {
		return sdk.SkippableError(errors.New("raw write rejected or outcome unknown; no transparent retry"))
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
