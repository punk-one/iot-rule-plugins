package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
)

type config struct {
	InputPath   string `json:"input_path"`
	OutputField string `json:"output_field"`
	MaxParts    int    `json:"max_parts"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "split-records", Name: "Split Records", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded array fanout with stable part correlation and independent owned events", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{InputPath: "records", OutputField: "record", MaxParts: 1024}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.InputPath == "" || p.cfg.OutputField == "" || p.cfg.OutputField == "parts" || p.cfg.MaxParts < 1 || p.cfg.MaxParts > 4096 {
		return sdk.FatalError(errors.New("invalid split settings"))
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
func lookup(root map[string]any, path string) (any, bool) {
	var value any = root
	for _, part := range strings.Split(path, ".") {
		object, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return value, true
}
func (p *plugin) split(e sdk.Event) ([]sdk.Event, error) {
	if e.Metadata["buffer_record_id"] != "" {
		return nil, errors.New("split must precede durable buffer; input ACK aggregation unsupported")
	}
	value, ok := lookup(e.Payload, p.cfg.InputPath)
	records, isArray := value.([]any)
	if !ok || !isArray || len(records) > p.cfg.MaxParts || e.ID == "" {
		return nil, errors.New("bounded records array and event ID required")
	}
	raw, err := json.Marshal(e)
	if err != nil || len(raw) > 1048576 {
		return nil, errors.New("split event exceeds 1 MiB")
	}
	if len(records) > 0 && len(raw) > 8388608/len(records) {
		return nil, errors.New("total split event copies exceed 8 MiB")
	}
	out := make([]sdk.Event, 0, len(records))
	for i := range records {
		child := e.Clone()
		copied, _ := lookup(child.Payload, p.cfg.InputPath)
		child.Payload[p.cfg.OutputField] = copied.([]any)[i]
		child.Payload["parts"] = map[string]any{"id": e.ID, "index": float64(i), "count": float64(len(records))}
		fingerprint := sha256.Sum256([]byte(e.ID + "/" + string(mustJSON(i))))
		child.ID = "evt_part_" + hex.EncodeToString(fingerprint[:16])
		out = append(out, child)
	}
	return out, nil
}
func mustJSON(v any) []byte { raw, _ := json.Marshal(v); return raw }
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	out, err := p.split(e)
	if err != nil {
		return sdk.SkippableError(err)
	}
	for _, child := range out {
		if err = p.ctx.Emitter().Publish(child); err != nil {
			return sdk.SkippableError(errors.New("split partially handed off; deterministic child IDs require downstream deduplication"))
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
