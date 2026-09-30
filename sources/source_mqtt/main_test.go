package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sourceTestConnector struct {
	resp       sdk.ConnectorResponse
	status     sdk.ResourceStatusEvent
	reqs       []sdk.ConnectorRequest
	streamReqs []sdk.StreamOpenRequest
	stream     sdk.StreamConsumer
}

func (c *sourceTestConnector) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.reqs = append(c.reqs, req)
	return c.resp, nil
}
func (c *sourceTestConnector) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sourceTestConnector) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sourceTestConnector) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sourceTestConnector) OpenStream(req sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	c.streamReqs = append(c.streamReqs, req)
	return c.stream, nil
}
func (c *sourceTestConnector) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return c.status, true
}

type sourceTestStream struct {
	mu        sync.Mutex
	message   sdk.StreamMessage
	delivered bool
	acked     []sdk.StreamAckRequest
	nacked    []sdk.StreamNackRequest
	closed    bool
}

func (s *sourceTestStream) ID() string { return "stream-1" }

func (s *sourceTestStream) Recv(timeout time.Duration) (sdk.StreamMessage, bool, error) {
	if s.delivered {
		time.Sleep(10 * time.Millisecond)
		return sdk.StreamMessage{}, false, nil
	}
	s.delivered = true
	return s.message, true, nil
}

func (s *sourceTestStream) Ack(deliveryID string, credits int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked = append(s.acked, sdk.StreamAckRequest{StreamID: s.ID(), DeliveryID: deliveryID, Credits: credits})
	return nil
}

func (s *sourceTestStream) Nack(deliveryID string, requeue bool, reason string, credits int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nacked = append(s.nacked, sdk.StreamNackRequest{StreamID: s.ID(), DeliveryID: deliveryID, Requeue: requeue, Reason: reason, Credits: credits})
	return nil
}

func (s *sourceTestStream) GrantCredits(int) error { return nil }

func (s *sourceTestStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *sourceTestStream) ackedSnapshot() []sdk.StreamAckRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sdk.StreamAckRequest(nil), s.acked...)
}

type sourceTestEmitter struct {
	mu     sync.Mutex
	events []sdk.Event
}

func (e *sourceTestEmitter) Publish(event sdk.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
	return nil
}

func (e *sourceTestEmitter) snapshot() []sdk.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdk.Event(nil), e.events...)
}
func (e *sourceTestEmitter) Ack(string) error                { return nil }
func (e *sourceTestEmitter) PublishAck(sdk.AckMessage) error { return nil }
func (e *sourceTestEmitter) EmitTo(string, sdk.Event) error  { return nil }
func (e *sourceTestEmitter) EmitBatch([]sdk.Event) error     { return nil }

type noopLogger struct{}

func (noopLogger) Debug(string, ...sdk.Field) {}
func (noopLogger) Info(string, ...sdk.Field)  {}
func (noopLogger) Warn(string, ...sdk.Field)  {}
func (noopLogger) Error(string, ...sdk.Field) {}
func (noopLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {
}

type noopMetrics struct{}

func (noopMetrics) IncCounter(string, map[string]string)       {}
func (noopMetrics) Observe(string, float64, map[string]string) {}

type noopHealth struct{}

func (noopHealth) Report(sdk.HealthReport) error                      { return nil }
func (noopHealth) Healthy(string, map[string]string) error            { return nil }
func (noopHealth) Degraded(string, string, map[string]string) error   { return nil }
func (noopHealth) Recovering(string, string, map[string]string) error { return nil }
func (noopHealth) Unhealthy(string, string, map[string]string) error  { return nil }
func (noopHealth) Heartbeat(string, map[string]string) error          { return nil }
func (noopHealth) Stopped(string, map[string]string) error            { return nil }

type sourceRuntimeContext struct {
	connector sdk.ConnectorClient
	emitter   sdk.Emitter
}

func (c sourceRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c sourceRuntimeContext) NodeID() string                                 { return "source-node" }
func (c sourceRuntimeContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c sourceRuntimeContext) Emitter() sdk.Emitter                           { return c.emitter }
func (c sourceRuntimeContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (c sourceRuntimeContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (c sourceRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c sourceRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

func TestMQTTSourceConfigRequiresConnectorRef(t *testing.T) {
	var source MQTTSource
	err := source.validateConfig([]byte(`{"process":{"subscribe":{"topics":["a/b"]}}}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestMQTTSourceConfigAcceptsUsageWeight(t *testing.T) {
	var source MQTTSource
	err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"usage_weight":1,
		"process":{"subscribe":{"topics":["a/b"]}}
	}`))
	if err != nil {
		t.Fatalf("expected usage_weight to be accepted, got %v", err)
	}
}

func TestMQTTSourceConfigRejectsInvalidExtractTarget(t *testing.T) {
	var source MQTTSource
	err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"process":{
			"subscribe":{"topics":["a/b"]},
			"extract":{"fields":[{"from":"topic[1]","to":"headers.device_code"}]}
		}
	}`))
	if err == nil || !strings.Contains(err.Error(), "process.extract.fields[0].to") {
		t.Fatalf("expected invalid extract target error, got %v", err)
	}
}

func TestMQTTSourceConfigRejectsLegacyExtractShape(t *testing.T) {
	var source MQTTSource
	err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"process":{
			"subscribe":{"topics":["a/b"]},
			"extract":{"device_code":"topic[1]"}
		}
	}`))
	if err == nil {
		t.Fatal("expected legacy extract shape to be rejected")
	}
}

func TestMQTTSourcePollPublishesExtractedFields(t *testing.T) {
	rawPayload := map[string]any{"value": 42, "time": "2026-04-05T10:00:00Z"}
	raw, _ := json.Marshal(rawPayload)
	respPayload, _ := json.Marshal(mqttReadResponse{
		Messages: []mqttInboundMessage{{
			Topic:         "factory/boiler/dev-1",
			PayloadBase64: base64.StdEncoding.EncodeToString(raw),
		}},
	})
	connector := &sourceTestConnector{
		resp: sdk.ConnectorResponse{Payload: respPayload},
		status: sdk.ResourceStatusEvent{
			ResourceID: "mqtt-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &MQTTSource{ctx: sourceRuntimeContext{connector: connector, emitter: emitter}, stopCh: make(chan struct{})}
	if err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"process":{
			"subscribe":{"topics":["factory/#"]},
			"decode":{"format":"json"},
			"extract":{"fields":[
				{"from":"topic[2]","to":"payload.device_code"},
				{"from":"topic[1]","to":"metadata.product_type"}
			]}
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	source.setStatus(sdk.ResourceStatusHealthy)

	congested, err := source.poll()
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if congested {
		t.Fatal("did not expect congested")
	}
	if len(connector.reqs) != 1 {
		t.Fatalf("expected one connector request, got %d", len(connector.reqs))
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected one emitted event, got %d", len(emitter.events))
	}
	if emitter.events[0].Metadata["product_type"] != "boiler" {
		t.Fatalf("unexpected metadata: %+v", emitter.events[0].Metadata)
	}
	if emitter.events[0].Payload["device_code"] != "dev-1" {
		t.Fatalf("unexpected payload: %+v", emitter.events[0].Payload)
	}
}

func TestMQTTSourcePollDoesNotOverwriteExistingPayloadField(t *testing.T) {
	rawPayload := map[string]any{"device_code": "payload-dev", "value": 42}
	raw, _ := json.Marshal(rawPayload)
	respPayload, _ := json.Marshal(mqttReadResponse{
		Messages: []mqttInboundMessage{{
			Topic:         "factory/boiler/dev-1",
			PayloadBase64: base64.StdEncoding.EncodeToString(raw),
		}},
	})
	connector := &sourceTestConnector{
		resp: sdk.ConnectorResponse{Payload: respPayload},
		status: sdk.ResourceStatusEvent{
			ResourceID: "mqtt-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &MQTTSource{ctx: sourceRuntimeContext{connector: connector, emitter: emitter}, stopCh: make(chan struct{})}
	if err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"process":{
			"subscribe":{"topics":["factory/#"]},
			"decode":{"format":"json"},
			"extract":{"fields":[{"from":"topic[2]","to":"payload.device_code"}]}
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	source.setStatus(sdk.ResourceStatusHealthy)

	if _, err := source.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected one emitted event, got %d", len(emitter.events))
	}
	if emitter.events[0].Payload["device_code"] != "payload-dev" {
		t.Fatalf("expected existing payload field to be preserved, got %+v", emitter.events[0].Payload)
	}
}

func TestMQTTSourcePollWritesNestedPayloadField(t *testing.T) {
	rawPayload := map[string]any{"value": 42}
	raw, _ := json.Marshal(rawPayload)
	respPayload, _ := json.Marshal(mqttReadResponse{
		Messages: []mqttInboundMessage{{
			Topic:         "factory/boiler/dev-1",
			PayloadBase64: base64.StdEncoding.EncodeToString(raw),
		}},
	})
	connector := &sourceTestConnector{
		resp: sdk.ConnectorResponse{Payload: respPayload},
		status: sdk.ResourceStatusEvent{
			ResourceID: "mqtt-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &MQTTSource{ctx: sourceRuntimeContext{connector: connector, emitter: emitter}, stopCh: make(chan struct{})}
	if err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"process":{
			"subscribe":{"topics":["factory/#"]},
			"decode":{"format":"json"},
			"extract":{"fields":[{"from":"topic[1]","to":"payload.tags.product_type"}]}
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	source.setStatus(sdk.ResourceStatusHealthy)

	if _, err := source.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected one emitted event, got %d", len(emitter.events))
	}
	tags, ok := emitter.events[0].Payload["tags"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested tags map, got %+v", emitter.events[0].Payload)
	}
	if tags["product_type"] != "boiler" {
		t.Fatalf("expected nested extracted field, got %+v", tags)
	}
}

func TestMQTTSourcePollDoesNotSyncTimestampFromPayloadTimeString(t *testing.T) {
	rawPayload := map[string]any{"value": 42, "time": "2026-04-05T10:00:00Z"}
	raw, _ := json.Marshal(rawPayload)
	respPayload, _ := json.Marshal(mqttReadResponse{
		Messages: []mqttInboundMessage{{
			Topic:         "factory/line1/dev-1",
			PayloadBase64: base64.StdEncoding.EncodeToString(raw),
		}},
	})
	connector := &sourceTestConnector{
		resp: sdk.ConnectorResponse{Payload: respPayload},
		status: sdk.ResourceStatusEvent{
			ResourceID: "mqtt-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &MQTTSource{
		cfg: MQTTSourceConfig{
			ConnectorRef: "mqtt-1",
		},
		ctx:    sourceRuntimeContext{connector: connector, emitter: emitter},
		stopCh: make(chan struct{}),
	}
	source.cfg.Process.Subscribe.Topics = []string{"factory/#"}
	source.cfg.Process.Decode.Format = "json"
	source.cfg.Polling.WaitTimeoutMS = 1000
	source.cfg.Polling.MaxBackoffMS = 10000
	source.setStatus(sdk.ResourceStatusHealthy)

	if _, err := source.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected one emitted event, got %d", len(emitter.events))
	}

	payloadTime, err := time.Parse(time.RFC3339, rawPayload["time"].(string))
	if err != nil {
		t.Fatalf("parse payload time: %v", err)
	}
	if emitter.events[0].Timestamp.Equal(payloadTime) {
		t.Fatalf("expected source-mqtt to leave event timestamp untouched, got payload time %s", emitter.events[0].Timestamp)
	}
}

func TestMQTTSourceOpenStreamUsesDeterministicSessionKey(t *testing.T) {
	stream := &sourceTestStream{}
	connector := &sourceTestConnector{
		stream: stream,
		status: sdk.ResourceStatusEvent{
			ResourceID: "mqtt-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &MQTTSource{}
	if err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"stream":{
			"initial_credits":32,
			"max_buffered_messages":2048,
			"overflow_policy":"drop_oldest",
			"ack_timeout_ms":90000,
			"max_deliver_batch":16
		},
		"polling":{"wait_timeout_ms":1000,"max_backoff_ms":10000},
		"process":{
			"subscribe":{"topics":["factory/#"],"qos":0},
			"decode":{"format":"json"}
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	if err := source.Start(sourceRuntimeContext{connector: connector, emitter: emitter}); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = source.Stop() }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(connector.streamReqs) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(connector.streamReqs) != 1 {
		t.Fatalf("expected one stream open request, got %d", len(connector.streamReqs))
	}
	if connector.streamReqs[0].SessionKey != "rule-1:source-node:mqtt-1" {
		t.Fatalf("unexpected session key: %+v", connector.streamReqs[0])
	}
	if connector.streamReqs[0].InitialCredits != 32 ||
		connector.streamReqs[0].MaxBufferedMessages != 2048 ||
		connector.streamReqs[0].OverflowPolicy != "drop_oldest" ||
		connector.streamReqs[0].AckTimeoutMS != 90000 ||
		connector.streamReqs[0].MaxDeliverBatch != 16 {
		t.Fatalf("unexpected stream options: %+v", connector.streamReqs[0])
	}
}

func TestMQTTSourceNextDelayBacksOffOnDisconnected(t *testing.T) {
	source := &MQTTSource{}
	source.cfg.Polling.MaxBackoffMS = 10000
	source.setStatus(sdk.ResourceStatusDisconnected)
	delay := source.nextDelay(1)
	if delay < 1600*time.Millisecond {
		t.Fatalf("expected disconnected backoff, got %s", delay)
	}
}

func TestMQTTSourceStartConsumesStreamMessages(t *testing.T) {
	rawPayload := map[string]any{"value": 42}
	raw, _ := json.Marshal(rawPayload)
	stream := &sourceTestStream{
		message: sdk.StreamMessage{
			StreamID:   "stream-1",
			DeliveryID: "delivery-1",
			Topic:      "factory/line1/dev-1",
			RawPayload: raw,
		},
	}
	connector := &sourceTestConnector{
		stream: stream,
		status: sdk.ResourceStatusEvent{
			ResourceID: "mqtt-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &MQTTSource{}
	if err := source.validateConfig([]byte(`{
		"connector_ref":"mqtt-1",
		"process":{
			"subscribe":{"topics":["factory/#"],"qos":0},
			"decode":{"format":"json"},
			"extract":{"fields":[{"from":"topic[2]","to":"metadata.device_code"}]}
		},
		"polling":{"wait_timeout_ms":50,"max_backoff_ms":200}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	if err := source.Start(sourceRuntimeContext{connector: connector, emitter: emitter}); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = source.Stop() })

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(emitter.snapshot()) == 1 && len(stream.ackedSnapshot()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := source.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	events := emitter.snapshot()
	acks := stream.ackedSnapshot()
	if len(events) != 1 {
		t.Fatalf("expected one emitted event, got %d", len(events))
	}
	if events[0].Metadata["device_code"] != "dev-1" {
		t.Fatalf("unexpected event metadata: %#v", events[0].Metadata)
	}
	if len(acks) != 1 || acks[0].DeliveryID != "delivery-1" {
		t.Fatalf("expected message ack, got %#v", acks)
	}
}
