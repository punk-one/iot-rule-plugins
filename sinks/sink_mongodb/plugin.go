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
	ConnectorRef string `json:"connector_ref"`
	Collection   string `json:"collection"`
	TimeoutMS    int    `json:"timeout_ms"`
	Ack          bool   `json:"ack_after_commit"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-mongodb", Name: "MongoDB Event Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Immutable majority-confirmed single-document event insert with stable-ID duplicate validation", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"mongodb"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 5000}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 65536 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`).MatchString(p.cfg.Collection) || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid MongoDB sink config"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("MongoDB connector and emitter required")
	}
	p.ctx = ctx
	return nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("MongoDB sink not started"))
	}
	if e.ID == "" || len(e.ID) > 1024 {
		return sdk.SkippableError(errors.New("stable bounded event ID required"))
	}
	if e.Metadata["buffer_record_id"] != "" && (!p.cfg.Ack || e.Metadata["buffer_node_id"] == "") {
		return sdk.SkippableError(errors.New("buffered input requires explicit commit ACK and original buffer IDs"))
	}
	payload, err := json.Marshal(e.Payload)
	if err != nil || len(payload) > 1048576 || e.Payload == nil {
		return sdk.SkippableError(errors.New("MongoDB payload must be a bounded JSON document"))
	}
	raw, _ := json.Marshal(map[string]any{"collection": p.cfg.Collection, "event_id": e.ID, "payload": json.RawMessage(payload)})
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "insert_event", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("MongoDB majority write failed; outcome unknown, replay only with same ID and bytes"))
	}
	var result struct {
		Committed bool   `json:"committed"`
		EventID   string `json:"event_id"`
		Duplicate bool   `json:"duplicate"`
	}
	if len(response.Payload) > 4096 || json.Unmarshal(response.Payload, &result) != nil || !result.Committed || result.EventID != e.ID {
		return sdk.SkippableError(errors.New("invalid MongoDB commit receipt"))
	}
	if p.cfg.Ack && e.Metadata["buffer_record_id"] != "" {
		return p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], EventID: e.ID, Status: sdk.AckStatusSuccess})
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

const configSchema = `{"type":"object","required":["connector_ref","collection"],"properties":{"connector_ref":{"type":"string"},"collection":{"type":"string"},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"ack_after_commit":{"type":"boolean","default":false}},"additionalProperties":false}`
