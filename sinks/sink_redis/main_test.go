package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sinkRedisConnectorStub struct {
	writes []sdk.ConnectorRequest
	status sdk.ResourceStatusEvent
}

func (c *sinkRedisConnectorStub) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *sinkRedisConnectorStub) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.writes = append(c.writes, req)
	return sdk.ConnectorResponse{}, nil
}

func (c *sinkRedisConnectorStub) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *sinkRedisConnectorStub) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *sinkRedisConnectorStub) OpenStream(req sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, nil
}

func (c *sinkRedisConnectorStub) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return c.status, true
}

type sinkRedisLogger struct{}

func (sinkRedisLogger) Debug(string, ...sdk.Field)                                {}
func (sinkRedisLogger) Info(string, ...sdk.Field)                                 {}
func (sinkRedisLogger) Warn(string, ...sdk.Field)                                 {}
func (sinkRedisLogger) Error(string, ...sdk.Field)                                {}
func (sinkRedisLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {}

type sinkRedisMetrics struct{}

func (sinkRedisMetrics) IncCounter(string, map[string]string)       {}
func (sinkRedisMetrics) Observe(string, float64, map[string]string) {}

type sinkRedisHealth struct{}

func (sinkRedisHealth) Report(sdk.HealthReport) error                      { return nil }
func (sinkRedisHealth) Healthy(string, map[string]string) error            { return nil }
func (sinkRedisHealth) Degraded(string, string, map[string]string) error   { return nil }
func (sinkRedisHealth) Recovering(string, string, map[string]string) error { return nil }
func (sinkRedisHealth) Unhealthy(string, string, map[string]string) error  { return nil }
func (sinkRedisHealth) Heartbeat(string, map[string]string) error          { return nil }
func (sinkRedisHealth) Stopped(string, map[string]string) error            { return nil }

type sinkRedisEmitter struct{}

func (sinkRedisEmitter) Publish(sdk.Event) error         { return nil }
func (sinkRedisEmitter) Ack(string) error                { return nil }
func (sinkRedisEmitter) PublishAck(sdk.AckMessage) error { return nil }
func (sinkRedisEmitter) EmitTo(string, sdk.Event) error  { return nil }
func (sinkRedisEmitter) EmitBatch([]sdk.Event) error     { return nil }

type sinkRedisRuntimeContext struct{ connector sdk.ConnectorClient }

func (c sinkRedisRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c sinkRedisRuntimeContext) NodeID() string                                 { return "sink-node" }
func (c sinkRedisRuntimeContext) Logger() sdk.Logger                             { return sinkRedisLogger{} }
func (c sinkRedisRuntimeContext) Emitter() sdk.Emitter                           { return sinkRedisEmitter{} }
func (c sinkRedisRuntimeContext) Metrics() sdk.Metrics                           { return sinkRedisMetrics{} }
func (c sinkRedisRuntimeContext) Health() sdk.HealthReporter                     { return sinkRedisHealth{} }
func (c sinkRedisRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c sinkRedisRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

func TestRedisSinkConfigRequiresConnectorRef(t *testing.T) {
	var sink RedisSink
	err := sink.validateConfig([]byte(`{"target":{"stream":"rules:events"}}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestRedisSinkWritesStreamThroughConnector(t *testing.T) {
	connector := &sinkRedisConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	sink := &RedisSink{ctx: sinkRedisRuntimeContext{connector: connector}}
	if err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "stream",
			"stream": "rules:events"
		},
		"encode": {
			"include_metadata": true,
			"include_context": true
		},
		"options": {
			"write_timeout_sec": 5,
			"approx": true
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)

	event := sdk.NewEvent(map[string]interface{}{"value": 42}, map[string]string{"device_code": "dev-1"})
	event.RuleID = "rule-1"
	event.Source = "node-1"
	event.Timestamp = time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one connector write, got %d", len(connector.writes))
	}
	if connector.writes[0].Target != "stream_add" {
		t.Fatalf("expected stream_add target, got %s", connector.writes[0].Target)
	}

	var payload redisWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Stream != "rules:events" {
		t.Fatalf("unexpected stream: %+v", payload)
	}
	if raw, ok := payload.Values["payload"].(string); !ok || !strings.Contains(raw, `"value":42`) {
		t.Fatalf("unexpected payload field: %+v", payload.Values)
	}
	if payload.Values["meta_device_code"] != "dev-1" {
		t.Fatalf("unexpected metadata fields: %+v", payload.Values)
	}
	if payload.Values["rule_id"] != "rule-1" {
		t.Fatalf("unexpected context fields: %+v", payload.Values)
	}
}

func TestRedisSinkStreamModeRejectsLegacyPayloadField(t *testing.T) {
	var sink RedisSink
	err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "stream",
			"stream": "rules:events"
		},
		"encode": {
			"payload_field": "custom_payload"
		}
	}`))
	if err == nil || !strings.Contains(err.Error(), "encode.payload_field is no longer supported") {
		t.Fatalf("expected stream mode payload_field validation error, got %v", err)
	}
}

func TestRedisSinkHashModeRejectsLegacyFieldsField(t *testing.T) {
	var sink RedisSink
	err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "hash",
			"key_field": "payload.device_code"
		},
		"encode": {
			"fields_field": "payload.data"
		}
	}`))
	if err == nil || !strings.Contains(err.Error(), "hash mode does not support encode.fields_field") {
		t.Fatalf("expected hash mode fields_field validation error, got %v", err)
	}
}

func TestRedisSinkWritesHashThroughConnectorUsingFieldMappings(t *testing.T) {
	connector := &sinkRedisConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	sink := &RedisSink{ctx: sinkRedisRuntimeContext{connector: connector}}
	if err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "hash",
			"key_prefix": "iot:device:status:",
			"key_field": "payload.device_code"
		},
		"encode": {
			"field_mappings": [
				{"field": "device_code", "path": "payload.device_code"},
				{"field": "online", "path": "payload.data.online"},
				{"field": "connection_state", "path": "payload.data.connection_state"},
				{"field": "last_seen_at", "path": "payload.data.last_seen_at"},
				{"field": "updated_at", "path": "payload.time"}
			]
		},
		"options": {
			"write_timeout_sec": 5
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)

	event := sdk.NewEvent(map[string]interface{}{
		"device_code": "dev-3",
		"time":        1775564872785,
		"data": map[string]interface{}{
			"online":           true,
			"connection_state": "connected",
			"last_seen_at":     1775564872000,
		},
	}, nil)

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one connector write, got %d", len(connector.writes))
	}

	var payload redisHashWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Key != "iot:device:status:dev-3" {
		t.Fatalf("unexpected key: %+v", payload)
	}
	if payload.Fields["device_code"] != "dev-3" {
		t.Fatalf("unexpected device_code field: %+v", payload.Fields)
	}
	if payload.Fields["connection_state"] != "connected" {
		t.Fatalf("unexpected connection_state field: %+v", payload.Fields)
	}
}

func TestRedisSinkWritesHashThroughConnectorUsingNestedFieldsProjection(t *testing.T) {
	connector := &sinkRedisConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	sink := &RedisSink{ctx: sinkRedisRuntimeContext{connector: connector}}
	if err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "hash",
			"key_field": "payload.key"
		},
		"encode": {
			"field_mappings": [
				{"field": "device_code", "path": "payload.fields.device_code"},
				{"field": "active_events", "path": "payload.fields.active_events"},
				{"field": "updated_at", "path": "payload.fields.updated_at"}
			]
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)

	event := sdk.NewEvent(map[string]interface{}{
		"key": "iot:device:status:dev-2",
		"fields": map[string]interface{}{
			"device_code":   "dev-2",
			"active_events": []interface{}{map[string]interface{}{"event_key": "status.offline"}},
			"updated_at":    1775564872785,
		},
	}, nil)

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one connector write, got %d", len(connector.writes))
	}

	var payload redisHashWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Key != "iot:device:status:dev-2" {
		t.Fatalf("unexpected key: %+v", payload)
	}
	if payload.Fields["device_code"] != "dev-2" {
		t.Fatalf("unexpected nested device_code field: %+v", payload.Fields)
	}
	if _, ok := payload.Fields["active_events"]; !ok {
		t.Fatalf("expected active_events field, got %+v", payload.Fields)
	}
}

func TestRedisSinkHashModeRejectsLegacyEncodeFields(t *testing.T) {
	var sink RedisSink
	err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "hash",
			"key_field": "payload.device_code"
		},
		"encode": {
			"payload_field": "payload",
			"fields_field": "payload.data"
		}
	}`))
	if err == nil || !strings.Contains(err.Error(), "encode.payload_field is no longer supported") {
		t.Fatalf("expected hash mode field_mappings validation error, got %v", err)
	}
}

func TestRedisSinkWritesHashThroughConnectorUsingExpandObject(t *testing.T) {
	connector := &sinkRedisConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	sink := &RedisSink{ctx: sinkRedisRuntimeContext{connector: connector}}
	if err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "hash",
			"key_field": "payload.device_code"
		},
		"encode": {
			"expand_object": "payload.data"
		},
		"options": {
			"write_timeout_sec": 5
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)

	event := sdk.NewEvent(map[string]interface{}{
		"device_code": "ZJLJG004",
		"data": map[string]interface{}{
			"AlarmInfo":    float64(0),
			"AlarmStat":    float64(0),
			"AutStat":      float64(1),
			"CncID":        "3c7b5d01-be322789-00000000-00000000",
			"RunStat":      float64(3),
			"PartsCount":   float64(11057),
			"SpindleSpeed": float64(0),
		},
	}, nil)

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one connector write, got %d", len(connector.writes))
	}

	var payload redisHashWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Key != "ZJLJG004" {
		t.Fatalf("unexpected key: %+v", payload)
	}
	if v, ok := payload.Fields["AlarmInfo"]; !ok || v != float64(0) {
		t.Fatalf("unexpected AlarmInfo field: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["AutStat"]; !ok || v != float64(1) {
		t.Fatalf("unexpected AutStat field: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["CncID"]; !ok || v != "3c7b5d01-be322789-00000000-00000000" {
		t.Fatalf("unexpected CncID field: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["RunStat"]; !ok || v != float64(3) {
		t.Fatalf("unexpected RunStat field: %+v", payload.Fields)
	}
}

func TestRedisSinkWritesHashThroughConnectorUsingExpandObjectWithFieldMappingsOverride(t *testing.T) {
	connector := &sinkRedisConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	sink := &RedisSink{ctx: sinkRedisRuntimeContext{connector: connector}}
	if err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "hash",
			"key_field": "payload.device_code"
		},
		"encode": {
			"expand_object": "payload.data",
			"field_mappings": [
				{"field": "device_code", "path": "payload.device_code"},
				{"field": "send_at", "path": "payload.send_at"},
				{"field": "RunStat", "path": "payload.data.RunStatOverride"}
			],
			"include_context": true
		},
		"options": {
			"write_timeout_sec": 5
		}
	}`)); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)

	event := sdk.NewEvent(map[string]interface{}{
		"device_code": "ZJLJG004",
		"send_at":     float64(1778981925893),
		"data": map[string]interface{}{
			"AlarmInfo":       float64(0),
			"AutStat":         float64(1),
			"PartsCount":      float64(11057),
			"RunStatOverride": float64(5),
		},
	}, nil)
	event.RuleID = "rule-telemetry"
	event.Source = "normalize-node"

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one connector write, got %d", len(connector.writes))
	}

	var payload redisHashWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Key != "ZJLJG004" {
		t.Fatalf("unexpected key: %+v", payload)
	}
	if v, ok := payload.Fields["AlarmInfo"]; !ok || v != float64(0) {
		t.Fatalf("expected AlarmInfo from expand_object: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["AutStat"]; !ok || v != float64(1) {
		t.Fatalf("expected AutStat from expand_object: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["PartsCount"]; !ok || v != float64(11057) {
		t.Fatalf("expected PartsCount from expand_object: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["RunStat"]; !ok || v != float64(5) {
		t.Fatalf("expected RunStat overridden by field_mappings: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["device_code"]; !ok || v != "ZJLJG004" {
		t.Fatalf("expected device_code from field_mappings: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["send_at"]; !ok || v != float64(1778981925893) {
		t.Fatalf("expected send_at from field_mappings: %+v", payload.Fields)
	}
	if v, ok := payload.Fields["rule_id"]; !ok || v != "rule-telemetry" {
		t.Fatalf("expected rule_id from context: %+v", payload.Fields)
	}
}

func TestRedisSinkHashModeRequiresFieldMappingsOrExpandObject(t *testing.T) {
	var sink RedisSink
	err := sink.validateConfig([]byte(`{
		"connector_ref": "redis-1",
		"target": {
			"mode": "hash",
			"key": "fixed-key"
		}
	}`))
	if err == nil || !strings.Contains(err.Error(), "hash mode requires encode.field_mappings or encode.expand_object") {
		t.Fatalf("expected validation error about field_mappings/expand_object, got %v", err)
	}
}
