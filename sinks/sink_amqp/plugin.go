package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"time"
)

type config struct {
	ConnectorRef     string `json:"connector_ref"`
	Exchange         string `json:"exchange"`
	RoutingKey       string `json:"routing_key"`
	TimeoutMS        int    `json:"timeout_ms"`
	AckBrokerReceipt bool   `json:"ack_broker_receipt"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

const configSchema = `{"type":"object","required":["connector_ref","exchange","routing_key"],"properties":{"connector_ref":{"type":"string","minLength":1},"exchange":{"type":"string","maxLength":255},"routing_key":{"type":"string","maxLength":255},"timeout_ms":{"type":"integer","minimum":100,"maximum":30000,"default":5000},"ack_broker_receipt":{"type":"boolean","default":false}},"additionalProperties":false}`

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-amqp", Name: "AMQP Confirmed Publish", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "Mandatory persistent AMQP publish with broker confirms; no business-commit claim", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"amqp"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 5000}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.ConnectorRef == "" || len(p.cfg.Exchange) > 255 || len(p.cfg.RoutingKey) > 255 || p.cfg.TimeoutMS < 100 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid AMQP sink settings"))
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
	if e.Metadata["buffer_record_id"] != "" && !p.cfg.AckBrokerReceipt {
		return sdk.SkippableError(errors.New("buffered input requires explicit broker-receipt goal; use signed business receipts for downstream commits"))
	}
	if e.ID == "" || len(e.ID) > 255 {
		return sdk.SkippableError(errors.New("bounded event ID required"))
	}
	body, err := json.Marshal(e.Payload)
	if err != nil || len(body) > 1048576 {
		return sdk.SkippableError(errors.New("invalid or oversized AMQP payload"))
	}
	raw, _ := json.Marshal(map[string]any{"exchange": p.cfg.Exchange, "routing_key": p.cfg.RoutingKey, "body": body, "message_id": e.ID})
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "publish", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("AMQP publish failed or outcome unknown; not transparently retried"))
	}
	var receipt struct {
		Confirmed bool `json:"broker_confirmed"`
	}
	if json.Unmarshal(response.Payload, &receipt) != nil || !receipt.Confirmed {
		return sdk.SkippableError(errors.New("AMQP broker confirmation missing"))
	}
	if p.cfg.AckBrokerReceipt && e.Metadata["buffer_record_id"] != "" {
		return p.ctx.Emitter().PublishAck(sdk.AckMessage{RecordID: e.Metadata["buffer_record_id"], BufferNodeID: e.Metadata["buffer_node_id"], RuleID: p.ctx.RuleID(), FromNodeID: p.ctx.NodeID(), EventID: e.ID, Status: sdk.AckStatusSuccess, AckedAt: time.Now().UTC()})
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
