package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"time"
)

type config struct {
	Origin   string `json:"origin_instance"`
	KeyID    string `json:"key_id"`
	Secret   string `json:"secret"`
	MaxAgeMS int64  `json:"max_age_ms"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "verify-delivery", Name: "Verify Business Delivery", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Authenticate telemetry before server persistence and adopt stable delivery identity for bounded pending deduplication", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{KeyID: "v1", MaxAgeMS: 604800000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || !boundedID(p.cfg.Origin) || !boundedID(p.cfg.KeyID) || !secretValid(p.cfg.Secret) || p.cfg.MaxAgeMS < 60000 || p.cfg.MaxAgeMS > 31622400000 {
		return sdk.FatalError(errors.New("invalid delivery verification scope"))
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
func (p *plugin) verify(e sdk.Event) (sdk.Event, error) {
	if e.Metadata["buffer_record_id"] != "" {
		return sdk.Event{}, errors.New("verify-delivery must precede server buffer because it adopts an authenticated stable Event ID")
	}
	var input envelope
	if err := decodeObject(e.Payload, &input); err != nil {
		return sdk.Event{}, err
	}
	if err := verifyEnvelope(input, p.cfg.Secret, p.cfg.KeyID, p.cfg.Origin, p.cfg.MaxAgeMS); err != nil {
		return sdk.Event{}, err
	}
	out := e.Clone()
	out.ID = "evt_delivery_" + input.Delivery.ID
	out.Timestamp = time.UnixMilli(input.Delivery.IssuedAt).UTC()
	delete(out.Payload, "event_id")
	return out, nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	out, err := p.verify(e)
	if err != nil {
		return sdk.SkippableError(err)
	}
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
