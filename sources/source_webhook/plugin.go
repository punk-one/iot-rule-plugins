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
	RouteKey  string `json:"route_key"`
	SecretEnv string `json:"secret_env"`
	MaxBody   int64  `json:"max_body_bytes"`
	QPS       int    `json:"qps"`
	Burst     int    `json:"burst"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-webhook", Name: "Webhook Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Engine-hosted signed HTTP ingress; no per-rule listening port", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{MaxBody: 262144, QPS: 10, Burst: 20}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 4096 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`).MatchString(p.cfg.RouteKey) || !regexp.MustCompile(`^IOT_RULE_WEBHOOK_[A-Z0-9_]{1,64}$`).MatchString(p.cfg.SecretEnv) || p.cfg.MaxBody < 1024 || p.cfg.MaxBody > 1048576 || p.cfg.QPS < 1 || p.cfg.QPS > 100 || p.cfg.Burst < 1 || p.cfg.Burst > 200 {
		return sdk.FatalError(errors.New("invalid webhook source configuration"))
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
	return sdk.SkippableError(errors.New("source-webhook accepts only Engine-managed signed ingress"))
}

const configSchema = `{"type":"object","required":["route_key","secret_env"],"properties":{"route_key":{"type":"string","pattern":"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"},"secret_env":{"type":"string","pattern":"^IOT_RULE_WEBHOOK_[A-Z0-9_]{1,64}$","description":"Engine environment variable name, never a literal signing key"},"max_body_bytes":{"type":"integer","minimum":1024,"maximum":1048576,"default":262144},"qps":{"type":"integer","minimum":1,"maximum":100,"default":10},"burst":{"type":"integer","minimum":1,"maximum":200,"default":20}},"additionalProperties":false}`
