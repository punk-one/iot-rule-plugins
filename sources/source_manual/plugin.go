package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
)

type config struct {
	AllowLive bool  `json:"allow_live_trigger"`
	MaxBody   int64 `json:"max_body_bytes"`
	QPS       int   `json:"qps"`
	Burst     int   `json:"burst"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-manual", Name: "Manual Trigger Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Explicitly enabled administrator live-rule injection; no clock or listening port", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{MaxBody: 262144, QPS: 2, Burst: 4}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 4096 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.MaxBody < 1024 || p.cfg.MaxBody > 1048576 || p.cfg.QPS < 1 || p.cfg.QPS > 10 || p.cfg.Burst < 1 || p.cfg.Burst > 20 {
		return sdk.FatalError(errors.New("invalid manual source configuration"))
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
	return sdk.SkippableError(errors.New("source-manual accepts only administrator Engine-managed injection"))
}

const configSchema = `{"type":"object","properties":{"allow_live_trigger":{"type":"boolean","default":false,"description":"Opt in to administrator triggers that execute the deployed rule, including its Sinks"},"max_body_bytes":{"type":"integer","minimum":1024,"maximum":1048576,"default":262144},"qps":{"type":"integer","minimum":1,"maximum":10,"default":2},"burst":{"type":"integer","minimum":1,"maximum":20,"default":4}},"additionalProperties":false}`
