package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net/url"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	Method       string `json:"method"`
	Path         string `json:"path"`
	TimeoutMS    int    `json:"timeout_ms"`
	AckOnSuccess bool   `json:"ack_on_success"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-http", Name: "HTTP Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Send event JSON to a bound HTTP endpoint; optional explicit receipt acknowledgement", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 0, ConfigSchema: `{"type":"object","required":["connector_ref","path"],"properties":{"connector_ref":{"type":"string","minLength":1},"path":{"type":"string"},"method":{"type":"string","enum":["POST","PUT","PATCH","DELETE"],"default":"POST"},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"ack_on_success":{"type":"boolean","default":false,"description":"Enable only when the endpoint's 2xx receipt meets the configured delivery goal"}},"additionalProperties":false}`, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"http"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Method: "POST", TimeoutMS: 5000}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	ref, err := url.Parse(p.cfg.Path)
	if err != nil || ref.IsAbs() || ref.Host != "" || ref.Fragment != "" || p.cfg.ConnectorRef == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid HTTP sink settings"))
	}
	switch p.cfg.Method {
	case "POST", "PUT", "PATCH", "DELETE":
	default:
		return sdk.FatalError(errors.New("unsupported write method"))
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
	raw, err := json.Marshal(map[string]any{"method": p.cfg.Method, "path": p.cfg.Path, "body": e.Payload})
	if err != nil {
		return sdk.SkippableError(err)
	}
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "request", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("HTTP write failed or outcome unknown; no transparent retry"))
	}
	var result struct {
		Status int `json:"status"`
	}
	if json.Unmarshal(response.Payload, &result) != nil || result.Status < 200 || result.Status >= 300 {
		return sdk.SkippableError(errors.New("HTTP receipt outside 2xx"))
	}
	if p.cfg.AckOnSuccess && e.Metadata["buffer_record_id"] != "" && e.Metadata["buffer_node_id"] != "" {
		if err = p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], EventID: e.ID, Status: sdk.AckStatusSuccess}); err != nil {
			return sdk.SkippableError(errors.New("HTTP receipt succeeded but buffer ack failed; endpoint idempotency required"))
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
