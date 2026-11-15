package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"regexp"
	"strconv"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	Topic        string `json:"topic"`
	TimeoutMS    int    `json:"timeout_ms"`
	Ack          bool   `json:"ack_broker_receipt"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "sink-kafka", Name: "Kafka Publish Sink", Version: "1.0.0", Type: sdk.PluginTypeSink, Description: "All-ISR Kafka reception with stable event key and explicit broker-receipt ACK goal", Capabilities: sdk.PluginCapabilities{InputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"kafka"}, RequiredCapabilities: []string{"write"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{TimeoutMS: 5000}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 65536 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}$`).MatchString(p.cfg.Topic) || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid Kafka sink config"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("Kafka connector and emitter required")
	}
	p.ctx = ctx
	return nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("Kafka sink not started"))
	}
	if e.ID == "" || len(e.ID) > 1024 {
		return sdk.SkippableError(errors.New("bounded stable event ID required"))
	}
	if e.Metadata["buffer_record_id"] != "" && (!p.cfg.Ack || e.Metadata["buffer_node_id"] == "") {
		return sdk.SkippableError(errors.New("buffered input requires explicit broker receipt goal and original buffer IDs"))
	}
	value, err := json.Marshal(e.Payload)
	if err != nil || len(value) > 1048576 {
		return sdk.SkippableError(errors.New("Kafka JSON payload exceeds 1 MiB"))
	}
	raw, _ := json.Marshal(map[string]any{"topic": p.cfg.Topic, "key": e.ID, "value": value})
	response, err := p.ctx.Connector().Write(sdk.ConnectorRequest{Target: "publish", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.SkippableError(errors.New("Kafka publish failed; outcome unknown, no automatic sink retry"))
	}
	var result struct {
		Received  bool   `json:"received"`
		Partition *int32 `json:"partition"`
		Offset    string `json:"offset"`
	}
	if len(response.Payload) > 4096 || json.Unmarshal(response.Payload, &result) != nil || !result.Received || result.Partition == nil || *result.Partition < 0 {
		return sdk.SkippableError(errors.New("invalid Kafka reception receipt"))
	}
	offset, err := strconv.ParseInt(result.Offset, 10, 64)
	if err != nil || offset < 0 {
		return sdk.SkippableError(errors.New("invalid Kafka offset receipt"))
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

const configSchema = `{"type":"object","required":["connector_ref","topic"],"properties":{"connector_ref":{"type":"string"},"topic":{"type":"string"},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"ack_broker_receipt":{"type":"boolean","default":false}},"additionalProperties":false}`
