package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type config struct {
	Origin    string `json:"origin_instance"`
	KeyID     string `json:"key_id"`
	Secret    string `json:"secret"`
	Committer string `json:"committer"`
	Buffer    string `json:"buffer_node_id"`
	MaxAgeMS  int64  `json:"max_age_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-business-ack", Name: "Business Commit ACK", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Validate scoped authenticated SQL commit receipts before acknowledging the original local buffer", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyID: "v1", MaxAgeMS: 604800000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || !boundedID(p.cfg.Origin) || !boundedID(p.cfg.KeyID) || !boundedID(p.cfg.Committer) || !boundedID(p.cfg.Buffer) || !secretValid(p.cfg.Secret) || p.cfg.MaxAgeMS < 60000 || p.cfg.MaxAgeMS > 31622400000 {
		return sdk.FatalError(errors.New("invalid business ACK scope"))
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
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	var signed receipt
	if err := decodeObject(e.Payload, &signed); err != nil {
		return sdk.SkippableError(err)
	}
	if err := verifyReceipt(signed, p.cfg.Secret, p.cfg.KeyID, p.cfg.Origin, p.cfg.Committer, p.cfg.MaxAgeMS); err != nil {
		return sdk.SkippableError(err)
	}
	if signed.Delivery.Rule != p.ctx.RuleID() || signed.Delivery.Buffer != p.cfg.Buffer {
		return sdk.SkippableError(errors.New("receipt belongs to another rule/buffer"))
	}
	return p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: signed.Delivery.Record, RuleID: p.ctx.RuleID(), BufferNodeID: p.cfg.Buffer, EventID: signed.Delivery.Event, Status: sdk.AckStatusSuccess})
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
