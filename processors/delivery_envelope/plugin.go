package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type config struct {
	Origin string `json:"origin_instance"`
	KeyID  string `json:"key_id"`
	Secret string `json:"secret"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "delivery-envelope", Name: "Business Delivery Envelope", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Stable authenticated envelope for a persisted edge buffer record; does not ACK broker receipt", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyID: "v1"}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || !boundedID(p.cfg.Origin) || !boundedID(p.cfg.KeyID) || !secretValid(p.cfg.Secret) {
		return sdk.FatalError(errors.New("origin/key/32-byte minimum secret required"))
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
func (p *plugin) prepare(e sdk.Event) (envelope, error) {
	if !boundedID(e.Metadata["buffer_record_id"]) || !boundedID(e.Metadata["buffer_node_id"]) || !boundedID(e.ID) || e.Timestamp.IsZero() || p.ctx == nil || !boundedID(p.ctx.RuleID()) {
		return envelope{}, errors.New("delivery-envelope must follow a persistent buffer with stable IDs")
	}
	body, hash, err := canonicalPayload(e.Payload)
	if err != nil {
		return envelope{}, err
	}
	header := deliveryHeader{Version: 1, Origin: p.cfg.Origin, Rule: p.ctx.RuleID(), Buffer: e.Metadata["buffer_node_id"], Record: e.Metadata["buffer_record_id"], Event: e.ID, KeyID: p.cfg.KeyID, PayloadHash: hash, IssuedAt: e.Timestamp.UnixMilli()}
	header.ID = identity(header)
	header.Signature = sign(p.cfg.Secret, "delivery-v1", header)
	return envelope{Delivery: header, Payload: body}, nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	out, err := p.prepare(e)
	if err != nil {
		return sdk.SkippableError(err)
	}
	raw, _ := json.Marshal(out)
	payload := map[string]any{}
	json.Unmarshal(raw, &payload)
	event := e.Clone()
	event.Payload = payload
	return p.ctx.Emitter().Publish(event)
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
