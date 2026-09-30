package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sinkMQTTConnectorStub struct {
	writes []sdk.ConnectorRequest
	status sdk.ResourceStatusEvent
}

func (c *sinkMQTTConnectorStub) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sinkMQTTConnectorStub) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.writes = append(c.writes, req)
	return sdk.ConnectorResponse{}, nil
}
func (c *sinkMQTTConnectorStub) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sinkMQTTConnectorStub) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sinkMQTTConnectorStub) OpenStream(req sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, nil
}
func (c *sinkMQTTConnectorStub) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return c.status, true
}

type sinkMQTTLogger struct{}

func (sinkMQTTLogger) Debug(string, ...sdk.Field)                                {}
func (sinkMQTTLogger) Info(string, ...sdk.Field)                                 {}
func (sinkMQTTLogger) Warn(string, ...sdk.Field)                                 {}
func (sinkMQTTLogger) Error(string, ...sdk.Field)                                {}
func (sinkMQTTLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {}

type sinkMQTTMetrics struct{}

func (sinkMQTTMetrics) IncCounter(string, map[string]string)       {}
func (sinkMQTTMetrics) Observe(string, float64, map[string]string) {}

type sinkMQTTHealth struct{}

func (sinkMQTTHealth) Report(sdk.HealthReport) error                      { return nil }
func (sinkMQTTHealth) Healthy(string, map[string]string) error            { return nil }
func (sinkMQTTHealth) Degraded(string, string, map[string]string) error   { return nil }
func (sinkMQTTHealth) Recovering(string, string, map[string]string) error { return nil }
func (sinkMQTTHealth) Unhealthy(string, string, map[string]string) error  { return nil }
func (sinkMQTTHealth) Heartbeat(string, map[string]string) error          { return nil }
func (sinkMQTTHealth) Stopped(string, map[string]string) error            { return nil }

type sinkMQTTEmitter struct{}

func (sinkMQTTEmitter) Publish(sdk.Event) error         { return nil }
func (sinkMQTTEmitter) Ack(string) error                { return nil }
func (sinkMQTTEmitter) PublishAck(sdk.AckMessage) error { return nil }
func (sinkMQTTEmitter) EmitTo(string, sdk.Event) error  { return nil }
func (sinkMQTTEmitter) EmitBatch([]sdk.Event) error     { return nil }

type sinkMQTTAckEmitter struct{ acks []sdk.AckMessage }

func (e *sinkMQTTAckEmitter) Publish(sdk.Event) error { return nil }
func (e *sinkMQTTAckEmitter) Ack(string) error        { return nil }
func (e *sinkMQTTAckEmitter) PublishAck(msg sdk.AckMessage) error {
	e.acks = append(e.acks, msg)
	return nil
}
func (e *sinkMQTTAckEmitter) EmitTo(string, sdk.Event) error { return nil }
func (e *sinkMQTTAckEmitter) EmitBatch([]sdk.Event) error    { return nil }

type sinkMQTTRuntimeContext struct{ connector sdk.ConnectorClient }

func (c sinkMQTTRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c sinkMQTTRuntimeContext) NodeID() string                                 { return "sink-node" }
func (c sinkMQTTRuntimeContext) Logger() sdk.Logger                             { return sinkMQTTLogger{} }
func (c sinkMQTTRuntimeContext) Emitter() sdk.Emitter                           { return sinkMQTTEmitter{} }
func (c sinkMQTTRuntimeContext) Metrics() sdk.Metrics                           { return sinkMQTTMetrics{} }
func (c sinkMQTTRuntimeContext) Health() sdk.HealthReporter                     { return sinkMQTTHealth{} }
func (c sinkMQTTRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c sinkMQTTRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

type sinkMQTTRuntimeContextWithEmitter struct {
	connector sdk.ConnectorClient
	emitter   sdk.Emitter
}

func (c sinkMQTTRuntimeContextWithEmitter) RuleID() string                 { return "rule-1" }
func (c sinkMQTTRuntimeContextWithEmitter) NodeID() string                 { return "sink-node" }
func (c sinkMQTTRuntimeContextWithEmitter) Logger() sdk.Logger             { return sinkMQTTLogger{} }
func (c sinkMQTTRuntimeContextWithEmitter) Emitter() sdk.Emitter           { return c.emitter }
func (c sinkMQTTRuntimeContextWithEmitter) Metrics() sdk.Metrics           { return sinkMQTTMetrics{} }
func (c sinkMQTTRuntimeContextWithEmitter) Health() sdk.HealthReporter     { return sinkMQTTHealth{} }
func (c sinkMQTTRuntimeContextWithEmitter) Connector() sdk.ConnectorClient { return c.connector }
func (c sinkMQTTRuntimeContextWithEmitter) ResourceEvents() <-chan sdk.ResourceStatusEvent {
	return nil
}

func TestMQTTSinkConfigRequiresConnectorRef(t *testing.T) {
	var sink MQTTSink
	err := sink.validateConfig([]byte(`{"target":{"topic":"v1/test"}}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestMQTTSinkPublishesWithSimplifiedTopicTemplate(t *testing.T) {
	connector := &sinkMQTTConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	sink := &MQTTSink{ctx: sinkMQTTRuntimeContext{connector: connector}}
	if err := sink.validateConfig([]byte(`{
		"connector_ref": "mqtt-1",
		"target": {
			"topic": "v1/gateway/${payload.device_code}/status/${event.source}",
			"qos": 1
		},
		"options": {
			"write_timeout_sec": 5
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)

	event := sdk.NewEvent(map[string]interface{}{
		"device_code": "ZH_DYZZ_0005",
		"online":      true,
	}, map[string]string{"product_type": "gateway"})
	event.Source = "status-node"
	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one connector write, got %d", len(connector.writes))
	}
	if connector.writes[0].Target != "publish" {
		t.Fatalf("unexpected connector target: %s", connector.writes[0].Target)
	}

	var payload mqttWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Topic != "v1/gateway/ZH_DYZZ_0005/status/status-node" {
		t.Fatalf("unexpected topic: %s", payload.Topic)
	}
	raw, err := base64.StdEncoding.DecodeString(payload.PayloadBase64)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if !strings.Contains(string(raw), `"device_code":"ZH_DYZZ_0005"`) {
		t.Fatalf("unexpected payload body: %s", string(raw))
	}
}

func TestMQTTSinkPublishesBufferAckAfterSuccessfulWrite(t *testing.T) {
	connector := &sinkMQTTConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	emitter := &sinkMQTTAckEmitter{}
	sink := &MQTTSink{ctx: sinkMQTTRuntimeContextWithEmitter{connector: connector, emitter: emitter}}
	if err := sink.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"target":{"topic":"rules/out"},
		"options":{"write_timeout_sec":5}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)

	event := sdk.NewEvent(map[string]any{"value": 42}, map[string]string{
		bufferRecordIDMetadataKey: "record-1",
		bufferNodeIDMetadataKey:   "buffer-node",
	})
	event.ID = "event-1"

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.acks) != 1 {
		t.Fatalf("expected one ack, got %d", len(emitter.acks))
	}
	if emitter.acks[0].RecordID != "record-1" || emitter.acks[0].BufferNodeID != "buffer-node" {
		t.Fatalf("unexpected ack payload: %#v", emitter.acks[0])
	}
}
