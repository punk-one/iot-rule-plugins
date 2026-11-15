package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	TimeoutMS    int    `json:"timeout_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-websocket", Name: "WebSocket Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Send event payload JSON without claiming peer acknowledgement", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"websocket"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 5000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.ConnectorRef == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid WebSocket sink config"))
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
	body, err := json.Marshal(e.Payload)
	if err != nil {
		return sdk.SkippableError(err)
	}
	raw, err := json.Marshal(map[string]any{"type": "text", "data": body})
	if err != nil {
		return sdk.SkippableError(err)
	}
	_, err = p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "send", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("WebSocket write failed or outcome unknown; no transparent retry"))
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
