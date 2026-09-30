package main

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sourceRedisConnectorStub struct {
	stream sdk.StreamConsumer
	status sdk.ResourceStatusEvent
	reqs   []sdk.StreamOpenRequest
}

func (c *sourceRedisConnectorStub) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *sourceRedisConnectorStub) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *sourceRedisConnectorStub) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *sourceRedisConnectorStub) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *sourceRedisConnectorStub) OpenStream(req sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	c.reqs = append(c.reqs, req)
	return c.stream, nil
}

func (c *sourceRedisConnectorStub) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return c.status, true
}

type sourceRedisStreamStub struct {
	mu        sync.Mutex
	message   sdk.StreamMessage
	delivered bool
	acked     []sdk.StreamAckRequest
	nacked    []sdk.StreamNackRequest
	closed    bool
}

func (s *sourceRedisStreamStub) ID() string { return "redis-stream-1" }

func (s *sourceRedisStreamStub) Recv(timeout time.Duration) (sdk.StreamMessage, bool, error) {
	if s.delivered {
		time.Sleep(10 * time.Millisecond)
		return sdk.StreamMessage{}, false, nil
	}
	s.delivered = true
	return s.message, true, nil
}

func (s *sourceRedisStreamStub) Ack(deliveryID string, credits int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked = append(s.acked, sdk.StreamAckRequest{StreamID: s.ID(), DeliveryID: deliveryID, Credits: credits})
	return nil
}

func (s *sourceRedisStreamStub) Nack(deliveryID string, requeue bool, reason string, credits int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nacked = append(s.nacked, sdk.StreamNackRequest{StreamID: s.ID(), DeliveryID: deliveryID, Requeue: requeue, Reason: reason, Credits: credits})
	return nil
}

func (s *sourceRedisStreamStub) GrantCredits(credits int) error { return nil }

func (s *sourceRedisStreamStub) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *sourceRedisStreamStub) ackedSnapshot() []sdk.StreamAckRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sdk.StreamAckRequest(nil), s.acked...)
}

type sourceRedisEmitter struct {
	mu     sync.Mutex
	events []sdk.Event
}

func (e *sourceRedisEmitter) Publish(event sdk.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
	return nil
}

func (e *sourceRedisEmitter) snapshot() []sdk.Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdk.Event(nil), e.events...)
}
func (e *sourceRedisEmitter) Ack(string) error                { return nil }
func (e *sourceRedisEmitter) PublishAck(sdk.AckMessage) error { return nil }
func (e *sourceRedisEmitter) EmitTo(string, sdk.Event) error  { return nil }
func (e *sourceRedisEmitter) EmitBatch([]sdk.Event) error     { return nil }

type sourceRedisLogger struct{}

func (sourceRedisLogger) Debug(string, ...sdk.Field)                                {}
func (sourceRedisLogger) Info(string, ...sdk.Field)                                 {}
func (sourceRedisLogger) Warn(string, ...sdk.Field)                                 {}
func (sourceRedisLogger) Error(string, ...sdk.Field)                                {}
func (sourceRedisLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {}

type sourceRedisMetrics struct{}

func (sourceRedisMetrics) IncCounter(string, map[string]string)       {}
func (sourceRedisMetrics) Observe(string, float64, map[string]string) {}

type sourceRedisHealth struct{}

func (sourceRedisHealth) Report(sdk.HealthReport) error                      { return nil }
func (sourceRedisHealth) Healthy(string, map[string]string) error            { return nil }
func (sourceRedisHealth) Degraded(string, string, map[string]string) error   { return nil }
func (sourceRedisHealth) Recovering(string, string, map[string]string) error { return nil }
func (sourceRedisHealth) Unhealthy(string, string, map[string]string) error  { return nil }
func (sourceRedisHealth) Heartbeat(string, map[string]string) error          { return nil }
func (sourceRedisHealth) Stopped(string, map[string]string) error            { return nil }

type sourceRedisRuntimeContext struct {
	connector sdk.ConnectorClient
	emitter   sdk.Emitter
}

func (c sourceRedisRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c sourceRedisRuntimeContext) NodeID() string                                 { return "source-redis-node" }
func (c sourceRedisRuntimeContext) Logger() sdk.Logger                             { return sourceRedisLogger{} }
func (c sourceRedisRuntimeContext) Emitter() sdk.Emitter                           { return c.emitter }
func (c sourceRedisRuntimeContext) Metrics() sdk.Metrics                           { return sourceRedisMetrics{} }
func (c sourceRedisRuntimeContext) Health() sdk.HealthReporter                     { return sourceRedisHealth{} }
func (c sourceRedisRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c sourceRedisRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

func TestRedisSourceConfigRequiresConnectorRef(t *testing.T) {
	var source RedisSource
	err := source.validateConfig([]byte(`{"stream":{"name":"rules:events"}}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestRedisSourceConsumesStreamAndPublishesDecodedPayload(t *testing.T) {
	rawMessage, _ := json.Marshal(redisStreamEnvelope{
		Stream: "rules:events",
		ID:     "1740000000000-0",
		Values: map[string]any{
			"payload":     `{"value":42,"device_code":"dev-1"}`,
			"source_type": "redis",
		},
		ReceivedAt: "2026-04-18T22:00:00Z",
	})
	stream := &sourceRedisStreamStub{
		message: sdk.StreamMessage{
			DeliveryID: "rules:events|1740000000000-0",
			Topic:      "rules:events",
			RawPayload: rawMessage,
			Metadata: map[string]string{
				"redis_stream":     "rules:events",
				"redis_message_id": "1740000000000-0",
			},
		},
	}
	connector := &sourceRedisConnectorStub{
		stream: stream,
		status: sdk.ResourceStatusEvent{ResourceID: "redis-1", Status: sdk.ResourceStatusHealthy},
	}
	emitter := &sourceRedisEmitter{}

	source := &RedisSource{}
	if err := source.validateConfig([]byte(`{
		"connector_ref":"redis-1",
		"streaming":{
			"initial_credits":24,
			"max_buffered_messages":4096,
			"overflow_policy":"pause_fetch",
			"ack_timeout_ms":120000,
			"max_deliver_batch":32
		},
		"stream":{"name":"rules:events","start_id":"0-0","block_timeout_ms":50},
		"decode":{"mode":"payload_json","payload_field":"payload"},
		"output":{"include_meta":true}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	if err := source.Start(sourceRedisRuntimeContext{connector: connector, emitter: emitter}); err != nil {
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
	if events[0].Payload["device_code"] != "dev-1" {
		t.Fatalf("unexpected payload: %#v", events[0].Payload)
	}
	if events[0].Metadata["redis_stream"] != "rules:events" {
		t.Fatalf("unexpected metadata: %#v", events[0].Metadata)
	}
	if len(acks) != 1 || acks[0].DeliveryID != "rules:events|1740000000000-0" {
		t.Fatalf("expected one ack, got %#v", acks)
	}
	if len(connector.reqs) != 1 {
		t.Fatalf("expected one stream open request, got %d", len(connector.reqs))
	}
	if connector.reqs[0].SessionKey != "rule-1:source-redis-node:redis-1" {
		t.Fatalf("unexpected session key: %+v", connector.reqs[0])
	}
	if connector.reqs[0].InitialCredits != 24 ||
		connector.reqs[0].MaxBufferedMessages != 4096 ||
		connector.reqs[0].OverflowPolicy != "pause_fetch" ||
		connector.reqs[0].AckTimeoutMS != 120000 ||
		connector.reqs[0].MaxDeliverBatch != 32 {
		t.Fatalf("unexpected stream options: %+v", connector.reqs[0])
	}
}
