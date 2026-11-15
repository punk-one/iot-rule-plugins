package main

import (
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

type config struct {
	ConnectorRef string            `json:"connector_ref"`
	Measurement  string            `json:"measurement"`
	TimeoutMS    int               `json:"timeout_ms"`
	AckOnSuccess bool              `json:"ack_on_success"`
	FieldTypes   map[string]string `json:"field_types"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-timeseries-influxdb", Name: "InfluxDB 2 Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Deterministic bounded line protocol using device_code, data, and millisecond time", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"influxdb"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 5000}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || !validText(p.cfg.Measurement) || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || len(p.cfg.FieldTypes) > 256 {
		return sdk.FatalError(errors.New("invalid InfluxDB sink settings"))
	}
	for name, kind := range p.cfg.FieldTypes {
		if !validText(name) || (kind != "int64" && kind != "uint64") {
			return sdk.FatalError(errors.New("explicit field types are int64 or uint64"))
		}
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
		return sdk.FatalError(errors.New("not started"))
	}
	line, err := encodeLine(p.cfg, e.Payload)
	if err != nil {
		return sdk.SkippableError(err)
	}
	raw, err := json.Marshal(map[string]any{"lines": line})
	if err != nil {
		return sdk.SkippableError(err)
	}
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "write", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("InfluxDB write failed or outcome unknown; no transparent retry"))
	}
	var result struct {
		Status int `json:"status"`
	}
	if json.Unmarshal(response.Payload, &result) != nil || result.Status < 200 || result.Status >= 300 {
		return sdk.SkippableError(errors.New("InfluxDB receipt outside 2xx"))
	}
	if p.cfg.AckOnSuccess && e.Metadata["buffer_record_id"] != "" && e.Metadata["buffer_node_id"] != "" {
		if err = p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], EventID: e.ID, Status: sdk.AckStatusSuccess}); err != nil {
			return sdk.SkippableError(errors.New("InfluxDB receipt succeeded but buffer ack failed; endpoint idempotency required"))
		}
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

func validText(v string) bool {
	return v != "" && len(v) <= 1024 && utf8.ValidString(v) && !strings.ContainsAny(v, "\r\n\x00")
}
func escape(v string, equals bool) string {
	v = strings.NewReplacer("\\", "\\\\", ",", "\\,", " ", "\\ ").Replace(v)
	if equals {
		v = strings.ReplaceAll(v, "=", "\\=")
	}
	return v
}
func encodeField(v any, kind string) (string, error) {
	if kind != "" {
		text, ok := v.(string)
		if !ok {
			return "", errors.New("exact integer field must be decimal string")
		}
		if kind == "int64" {
			n, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(n, 10) + "i", nil
		}
		n, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return "", err
		}
		return strconv.FormatUint(n, 10) + "u", nil
	}
	switch x := v.(type) {
	case bool:
		return strconv.FormatBool(x), nil
	case string:
		if !validText(x) {
			return "", errors.New("invalid string field")
		}
		return "\"" + strings.NewReplacer("\\", "\\\\", "\"", "\\\"").Replace(x) + "\"", nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) > 9007199254740991 {
			return "", errors.New("unsafe float field")
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case int:
		return strconv.FormatInt(int64(x), 10) + "i", nil
	case int64:
		return strconv.FormatInt(x, 10) + "i", nil
	case uint64:
		return strconv.FormatUint(x, 10) + "u", nil
	default:
		return "", errors.New("unsupported field value")
	}
}
func encodeLine(cfg config, payload map[string]any) (string, error) {
	device, ok := payload["device_code"].(string)
	if !ok || !validText(device) {
		return "", errors.New("device_code required")
	}
	data, ok := payload["data"].(map[string]any)
	if !ok || len(data) == 0 || len(data) > 256 {
		return "", errors.New("data must contain 1..256 scalar fields")
	}
	var timestamp int64
	switch v := payload["time"].(type) {
	case float64:
		if math.IsNaN(v) || v < 0 || v > 9223372036854 || math.Trunc(v) != v {
			return "", errors.New("invalid ms timestamp")
		}
		timestamp = int64(v)
	case int64:
		timestamp = v
	case int:
		timestamp = int64(v)
	default:
		return "", errors.New("integer ms time required")
	}
	if timestamp < 0 || timestamp > 9223372036854 {
		return "", errors.New("time outside InfluxDB nanosecond range")
	}
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := []string{}
	for _, key := range keys {
		if !validText(key) {
			return "", errors.New("invalid field key")
		}
		v, err := encodeField(data[key], cfg.FieldTypes[key])
		if err != nil {
			return "", fmt.Errorf("field %s: %w", key, err)
		}
		fields = append(fields, escape(key, true)+"="+v)
	}
	line := escape(cfg.Measurement, false) + ",device_code=" + escape(device, true) + " " + strings.Join(fields, ",") + " " + strconv.FormatInt(timestamp, 10)
	if len(line) > 1048576 {
		return "", errors.New("line too large")
	}
	return line, nil
}
