package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"time"
)

type config struct {
	Origin       string            `json:"origin_instance"`
	KeyID        string            `json:"key_id"`
	Secret       string            `json:"secret"`
	Committer    string            `json:"committer"`
	ConnectorRef string            `json:"connector_ref"`
	Statement    string            `json:"statement"`
	Params       map[string]string `json:"params"`
	TimeoutMS    int               `json:"timeout_ms"`
	MaxAgeMS     int64             `json:"max_age_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "transaction-receipt", Name: "Transactional Business Receipt", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Verify an edge envelope, commit an idempotent SQL transaction, then emit a signed application receipt", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"mssql", "mysql", "sqlite"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyID: "v1", TimeoutMS: 5000, MaxAgeMS: 604800000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || !boundedID(p.cfg.Origin) || !boundedID(p.cfg.KeyID) || !boundedID(p.cfg.Committer) || !secretValid(p.cfg.Secret) || p.cfg.ConnectorRef == "" || strings.TrimSpace(p.cfg.Statement) == "" || len(p.cfg.Statement) > 65536 || len(p.cfg.Params) > 128 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || p.cfg.MaxAgeMS < 60000 || p.cfg.MaxAgeMS > 31622400000 {
		return sdk.FatalError(errors.New("invalid transaction receipt scope/query limits"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("transactional SQL resource required")
	}
	p.ctx = ctx
	return nil
}
func resolve(root map[string]any, path string) (any, bool) {
	var value any = root
	for _, part := range strings.Split(path, ".") {
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
	var input envelope
	if err := decodeObject(e.Payload, &input); err != nil {
		return sdk.SkippableError(err)
	}
	if err := verifyEnvelope(input, p.cfg.Secret, p.cfg.KeyID, p.cfg.Origin, p.cfg.MaxAgeMS); err != nil {
		return sdk.SkippableError(err)
	}
	headerRaw, _ := json.Marshal(input.Delivery)
	header := map[string]any{}
	json.Unmarshal(headerRaw, &header)
	root := map[string]any{"delivery": header, "payload": input.Payload}
	params := map[string]any{}
	for name, path := range p.cfg.Params {
		value, ok := resolve(root, path)
		if !ok {
			return sdk.SkippableError(errors.New("transaction parameter path missing"))
		}
		params[name] = value
	}
	raw, err := json.Marshal(map[string]any{"query": p.cfg.Statement, "params": params})
	if err != nil {
		return sdk.SkippableError(err)
	}
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "statement", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("SQL transaction rejected/unknown; no receipt emitted; replay requires idempotent SQL"))
	}
	var committed struct {
		Committed bool `json:"committed"`
	}
	if json.Unmarshal(response.Payload, &committed) != nil || !committed.Committed {
		return sdk.SkippableError(errors.New("connector did not explicitly confirm transaction commit"))
	}
	out := receipt{Delivery: input.Delivery, Committer: p.cfg.Committer, CommittedAt: time.Now().UnixMilli(), Status: "committed"}
	out.Signature = sign(p.cfg.Secret, "commit-receipt-v1", out)
	raw, _ = json.Marshal(out)
	payload := map[string]any{}
	json.Unmarshal(raw, &payload)
	event := e.Clone()
	event.Payload = payload
	if p.ctx.Emitter().Publish(event) != nil {
		return sdk.SkippableError(errors.New("SQL committed but receipt handoff failed; idempotent replay required"))
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
