package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
)

type config struct {
	ConnectorRef string            `json:"connector_ref"`
	Query        string            `json:"query"`
	Params       map[string]string `json:"params"`

	ResultField string `json:"result_field"`
	TimeoutMS   int    `json:"timeout_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "request-influxdb", Name: "InfluxDB Flux Query", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded parameterized Flux query returning rows to downstream nodes", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"influxdb"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 5000, ResultField: "query_result"}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || strings.TrimSpace(p.cfg.Query) == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.ResultField == "" || len(p.cfg.Params) > 128 {
		return sdk.FatalError(errors.New("invalid Flux query config"))
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
func resolve(e sdk.Event, path string) (any, bool) {
	if path == "event.id" {
		return e.ID, e.ID != ""
	}
	var v any = e.Payload
	for _, part := range strings.Split(strings.TrimPrefix(path, "payload."), ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return v, true
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	params := map[string]any{}
	for name, path := range p.cfg.Params {
		value, ok := resolve(e, path)
		if !ok {
			return sdk.SkippableError(errors.New("Flux parameter path missing"))
		}
		params[name] = value
	}
	raw, err := json.Marshal(map[string]any{"query": p.cfg.Query, "params": params})
	if err != nil {
		return sdk.SkippableError(err)
	}
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "query", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.RetryableError(errors.New("Flux query failed"))
	}
	var result map[string]any
	if err = json.Unmarshal(response.Payload, &result); err != nil {
		return sdk.SkippableError(errors.New("invalid Flux query response"))
	}
	out := e.Clone()
	if out.Payload == nil {
		out.Payload = map[string]any{}
	}
	out.Payload[p.cfg.ResultField] = result
	return p.ctx.Emitter().Publish(out)
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
