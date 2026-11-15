package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	DeviceCode   string `json:"device_code"`
	ID           string `json:"id"`
	Extended     bool   `json:"extended"`
	FD           bool   `json:"can_fd"`
	Remote       bool   `json:"remote"`
	RTRLength    int    `json:"rtr_length"`
	BRS          bool   `json:"bitrate_switch"`
	DataPath     string `json:"data_path"`
	TimeoutMS    int    `json:"timeout_ms"`
	MaxHorizonMS int    `json:"max_command_horizon_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-can", Name: "CAN Frame Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Fixed-ID expiring CAN commands with local-socket receipt; no device-completion ACK", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"can"}, RequiredCapabilities: []string{"write"}}}}
}
func validID(raw string, extended bool) bool {
	if raw == "" || len(raw) > 10 || strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, "+-") {
		return false
	}
	base := 10
	value := raw
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		base = 16
		value = raw[2:]
	} else if len(raw) > 1 && raw[0] == '0' {
		return false
	}
	n, err := strconv.ParseUint(value, base, 32)
	limit := uint64(0x7ff)
	if extended {
		limit = 0x1fffffff
	}
	return err == nil && n <= limit
}
func fdLength(n int) bool {
	return n >= 0 && n <= 8 || n == 12 || n == 16 || n == 20 || n == 24 || n == 32 || n == 48 || n == 64
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{DataPath: "data.raw_base64", TimeoutMS: 1000, MaxHorizonMS: 10000}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 65536 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || !validID(p.cfg.ID, p.cfg.Extended) || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 5000 || p.cfg.MaxHorizonMS < 100 || p.cfg.MaxHorizonMS > 60000 || p.cfg.DataPath == "" || len(p.cfg.DataPath) > 256 || p.cfg.Remote && p.cfg.FD || p.cfg.RTRLength < 0 || p.cfg.RTRLength > 8 || !p.cfg.Remote && p.cfg.RTRLength != 0 || !p.cfg.FD && p.cfg.BRS {
		return sdk.FatalError(errors.New("invalid CAN sink configuration"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound CAN connector required")
	}
	p.ctx = ctx
	return nil
}
func resolve(payload map[string]any, path string) (any, bool) {
	value := any(payload)
	for _, part := range strings.Split(strings.TrimPrefix(path, "payload."), ".") {
		m, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return value, true
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
		return sdk.FatalError(errors.New("CAN sink not started"))
	}
	if e.Metadata["buffer_record_id"] != "" {
		return sdk.SkippableError(errors.New("CAN commands cannot use automatic buffered replay; use command-guard and explicit outcome reconciliation"))
	}
	device, ok := e.Payload["device_code"].(string)
	id, idOK := e.Payload["command_id"].(string)
	expiry, expiryOK := millis(e.Payload["expires_at"])
	now := time.Now().UnixMilli()
	if !ok || device != p.cfg.DeviceCode || !idOK || id == "" || len(id) > 256 || !expiryOK || expiry <= now || expiry > now+int64(p.cfg.MaxHorizonMS) {
		return sdk.SkippableError(errors.New("CAN command needs matching device_code, bounded command_id and live expires_at"))
	}
	data := []byte{}
	if !p.cfg.Remote {
		value, ok := resolve(e.Payload, p.cfg.DataPath)
		text, isText := value.(string)
		if !ok || !isText || len(text) > 88 {
			return sdk.SkippableError(errors.New("CAN payload must be bounded base64 at configured data_path"))
		}
		var err error
		data, err = base64.StdEncoding.Strict().DecodeString(text)
		if err != nil || p.cfg.FD && !fdLength(len(data)) || !p.cfg.FD && len(data) > 8 {
			return sdk.SkippableError(errors.New("CAN payload length is not valid for frame format"))
		}
	}
	raw, _ := json.Marshal(map[string]any{"frame": map[string]any{"id": p.cfg.ID, "extended": p.cfg.Extended, "fd": p.cfg.FD, "remote": p.cfg.Remote, "rtr_length": p.cfg.RTRLength, "bitrate_switch": p.cfg.BRS, "data": data}, "expires_at": expiry})
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "send", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("CAN write failed; outcome unknown, do not transparently retry physical commands"))
	}
	var receipt struct {
		Queued bool   `json:"queued"`
		Scope  string `json:"scope"`
	}
	if len(response.Payload) > 1024 || json.Unmarshal(response.Payload, &receipt) != nil || !receipt.Queued || receipt.Scope != "local_socket" {
		return sdk.SkippableError(errors.New("invalid CAN local-socket receipt"))
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

const configSchema = `{"type":"object","required":["connector_ref","device_code","id"],"properties":{"connector_ref":{"type":"string"},"device_code":{"type":"string"},"id":{"type":"string","description":"Fixed standard11-bit or extended29-bit ID, decimal or 0x hex"},"extended":{"type":"boolean","default":false},"can_fd":{"type":"boolean","default":false},"remote":{"type":"boolean","default":false},"rtr_length":{"type":"integer","minimum":0,"maximum":8,"default":0},"bitrate_switch":{"type":"boolean","default":false},"data_path":{"type":"string","default":"data.raw_base64"},"timeout_ms":{"type":"integer","minimum":50,"maximum":5000,"default":1000},"max_command_horizon_ms":{"type":"integer","minimum":100,"maximum":60000,"default":10000}},"additionalProperties":false}`
