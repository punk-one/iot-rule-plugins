package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
)

type config struct {
	ConnectorRef string            `json:"connector_ref"`
	Statement    string            `json:"statement"`
	Params       map[string]string `json:"params"`
	TimeoutMS    int               `json:"timeout_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-relational-mysql", Name: "MySQL Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Parameterized transactional SQL with buffer confirmation only after commit", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 0, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"mysql"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg.TimeoutMS = 5000
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || strings.TrimSpace(p.cfg.Statement) == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || len(p.cfg.Params) > 128 {
		return sdk.FatalError(errors.New("invalid SQL sink config"))
	}
	for _, path := range p.cfg.Params {
		if path == "" {
			return sdk.FatalError(errors.New("empty parameter path"))
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
func resolve(e sdk.Event, path string) (any, bool) {
	if path == "event.id" {
		return e.ID, e.ID != ""
	}
	if path == "event.trace_id" {
		return sdk.GetTraceID(e), sdk.GetTraceID(e) != ""
	}
	var value any = e.Payload
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
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	params := map[string]any{}
	for name, path := range p.cfg.Params {
		value, ok := resolve(e, path)
		if !ok {
			return sdk.SkippableError(errors.New("SQL parameter path missing"))
		}
		params[name] = value
	}
	raw, err := json.Marshal(map[string]any{"query": p.cfg.Statement, "params": params})
	if err != nil {
		return sdk.SkippableError(err)
	}
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "statement", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("SQL write failed or commit unknown; automatic retry is unsafe without idempotent SQL"))
	}
	var result struct {
		Committed bool `json:"committed"`
	}
	if json.Unmarshal(response.Payload, &result) != nil || !result.Committed {
		return sdk.SkippableError(errors.New("SQL commit confirmation missing"))
	}
	if e.Metadata["buffer_record_id"] != "" && e.Metadata["buffer_node_id"] != "" {
		if err = p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], EventID: e.ID, Status: sdk.AckStatusSuccess}); err != nil {
			return sdk.SkippableError(errors.New("SQL committed but buffer ack failed; replay requires idempotent SQL"))
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
