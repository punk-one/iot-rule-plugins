package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"regexp"
)

type config struct {
	WatchNodes []string `json:"watch_nodes"`
	Kinds      []string `json:"kinds"`
	DeviceCode string   `json:"device_code"`
	QPS        int      `json:"max_events_per_second"`
	Burst      int      `json:"burst"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-runtime-events", Name: "Runtime Events Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Engine-managed bounded node error, health, status and task-done value events", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Kinds: []string{"node_error", "node_health", "node_status"}, DeviceCode: "rule-runtime", QPS: 10, Burst: 20}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 32768 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || len(p.cfg.WatchNodes) < 1 || len(p.cfg.WatchNodes) > 128 || len(p.cfg.Kinds) < 1 || len(p.cfg.Kinds) > 4 || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || p.cfg.QPS < 1 || p.cfg.QPS > 100 || p.cfg.Burst < 1 || p.cfg.Burst > 200 {
		return sdk.FatalError(errors.New("invalid runtime event source configuration"))
	}
	seen := map[string]bool{}
	for _, id := range p.cfg.WatchNodes {
		if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`).MatchString(id) || seen[id] {
			return sdk.FatalError(errors.New("invalid/duplicate watched node"))
		}
		seen[id] = true
	}
	seen = map[string]bool{}
	for _, kind := range p.cfg.Kinds {
		if seen[kind] || kind != "node_error" && kind != "node_health" && kind != "node_status" && kind != "node_done" {
			return sdk.FatalError(errors.New("invalid/duplicate runtime event kind"))
		}
		seen[kind] = true
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime context required")
	}
	return nil
}
func (p *plugin) OnEvent(sdk.Event) error {
	return sdk.SkippableError(errors.New("runtime event Source only accepts Engine-managed observations"))
}

const configSchema = `{"type":"object","required":["watch_nodes"],"properties":{"watch_nodes":{"type":"array","minItems":1,"maxItems":128,"uniqueItems":true,"items":{"type":"string","pattern":"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"},"description":"Explicit same-rule node IDs; cannot watch event Sources or this branch's descendants"},"kinds":{"type":"array","minItems":1,"maxItems":4,"uniqueItems":true,"items":{"type":"string","enum":["node_error","node_health","node_status","node_done"]},"default":["node_error","node_health","node_status"]},"device_code":{"type":"string","minLength":1,"maxLength":128,"default":"rule-runtime"},"max_events_per_second":{"type":"integer","minimum":1,"maximum":100,"default":10},"burst":{"type":"integer","minimum":1,"maximum":200,"default":20}},"additionalProperties":false}`
