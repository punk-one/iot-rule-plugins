package main

import (
	"encoding/json"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

// test helpers

type testLogger struct{}

func (testLogger) Debug(string, ...sdk.Field)                                {}
func (testLogger) Info(string, ...sdk.Field)                                 {}
func (testLogger) Warn(string, ...sdk.Field)                                 {}
func (testLogger) Error(string, ...sdk.Field)                                {}
func (testLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {}

type testMetrics struct{}

func (testMetrics) IncCounter(string, map[string]string)       {}
func (testMetrics) Observe(string, float64, map[string]string) {}

type testHealth struct{}

func (testHealth) Report(sdk.HealthReport) error                      { return nil }
func (testHealth) Healthy(string, map[string]string) error            { return nil }
func (testHealth) Degraded(string, string, map[string]string) error   { return nil }
func (testHealth) Recovering(string, string, map[string]string) error { return nil }
func (testHealth) Unhealthy(string, string, map[string]string) error  { return nil }
func (testHealth) Heartbeat(string, map[string]string) error          { return nil }
func (testHealth) Stopped(string, map[string]string) error            { return nil }

type testEmitter struct {
	events []sdk.Event
}

func (e *testEmitter) Publish(event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *testEmitter) Ack(string) error                { return nil }
func (e *testEmitter) PublishAck(sdk.AckMessage) error { return nil }
func (e *testEmitter) EmitTo(_ string, event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *testEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type testRuntimeContext struct {
	emitter *testEmitter
}

func (c testRuntimeContext) RuleID() string                                 { return "rule-transform" }
func (c testRuntimeContext) NodeID() string                                 { return "transform-node" }
func (c testRuntimeContext) Logger() sdk.Logger                             { return testLogger{} }
func (c testRuntimeContext) Emitter() sdk.Emitter                           { return c.emitter }
func (c testRuntimeContext) Metrics() sdk.Metrics                           { return testMetrics{} }
func (c testRuntimeContext) Health() sdk.HealthReporter                     { return testHealth{} }
func (c testRuntimeContext) Connector() sdk.ConnectorClient                 { return nil }
func (c testRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

func initPlugin(t *testing.T, rawJSON string) (*TransformPlugin, *testEmitter) {
	t.Helper()
	plugin := &TransformPlugin{}
	if err := plugin.Init(sdk.PluginConfig{Raw: []byte(rawJSON)}); err != nil {
		t.Fatalf("init plugin: %v", err)
	}
	emitter := &testEmitter{}
	if err := plugin.Start(testRuntimeContext{emitter: emitter}); err != nil {
		t.Fatalf("start plugin: %v", err)
	}
	return plugin, emitter
}

// --- Tests ---

func TestTransformRuleToCompact(t *testing.T) {
	raw := `{
		"process": {
			"name": "rule_to_compact",
			"metadata": {
				"copy": ["trace_id", "time", "send_at"]
			},
			"mappings": [
				{
					"name": "device_payload",
					"type": "object",
					"key": {
						"template": "{{ .payload.device_code }}"
					},
					"value": {
						"jsonpath": "$.data",
						"exclude": ["temperature"]
					}
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		ID:        "evt-1",
		Timestamp: time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC),
		Payload: map[string]interface{}{
			"trace_id":    "acm006-1710000000000000000",
			"data":        map[string]interface{}{"total_out": 100.0, "alarm": true, "temperature": 36.5},
			"time":        1710000000000.0,
			"send_at":     1710000000500.0,
			"device_code": "acm006",
		},
		Metadata: map[string]string{"protocol": "mqtt"},
		RuleID:   "incoming-rule",
		Source:   "source-mqtt",
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	got := emitter.events[0]
	payload := got.Payload

	// Check metadata fields copied
	if payload["trace_id"] != "acm006-1710000000000000000" {
		t.Fatalf("expected trace_id, got %#v", payload["trace_id"])
	}
	if payload["time"] != 1710000000000.0 {
		t.Fatalf("expected time, got %#v", payload["time"])
	}
	if payload["send_at"] != 1710000000500.0 {
		t.Fatalf("expected send_at, got %#v", payload["send_at"])
	}

	// Check dynamic key
	devicePayload, ok := payload["acm006"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected acm006 key in payload, got %#v", payload)
	}
	if devicePayload["total_out"] != 100.0 {
		t.Fatalf("expected total_out=100, got %#v", devicePayload["total_out"])
	}
	if devicePayload["alarm"] != true {
		t.Fatalf("expected alarm=true, got %#v", devicePayload["alarm"])
	}

	// Check exclude
	if _, exists := devicePayload["temperature"]; exists {
		t.Fatalf("expected temperature to be excluded, but it exists")
	}

	// Check routing fields
	if got.RuleID != "rule-transform" {
		t.Fatalf("expected rule_id=rule-transform, got %q", got.RuleID)
	}
	if got.Source != "transform-node" {
		t.Fatalf("expected source=transform-node, got %q", got.Source)
	}
}

func TestTransformCompactToRule(t *testing.T) {
	raw := `{
		"process": {
			"name": "compact_to_rule",
			"metadata": {
				"copy": ["trace_id", "time", "send_at"]
			},
			"mappings": [
				{
					"type": "copy",
					"from": "$keys()",
					"to": "device_code"
				},
				{
					"type": "object",
					"key": {
						"template": "data"
					},
					"value": {
						"jsonpath": "$.*"
					}
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		ID:        "evt-2",
		Timestamp: time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC),
		Payload: map[string]interface{}{
			"trace_id": "acm006-1710000000000000000",
			"time":     1710000000000.0,
			"send_at":  1710000000500.0,
			"acm006":   map[string]interface{}{"total_out": 100.0, "alarm": true},
		},
		Metadata: map[string]string{"protocol": "mqtt"},
		RuleID:   "incoming-rule",
		Source:   "source-mqtt",
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	got := emitter.events[0]
	payload := got.Payload

	// Check metadata fields copied
	if payload["trace_id"] != "acm006-1710000000000000000" {
		t.Fatalf("expected trace_id, got %#v", payload["trace_id"])
	}
	if payload["time"] != 1710000000000.0 {
		t.Fatalf("expected time, got %#v", payload["time"])
	}
	if payload["send_at"] != 1710000000500.0 {
		t.Fatalf("expected send_at, got %#v", payload["send_at"])
	}

	// Check device_code from $keys()
	if payload["device_code"] != "acm006" {
		t.Fatalf("expected device_code=acm006, got %#v", payload["device_code"])
	}

	// Check data from $.*
	data, ok := payload["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data key in payload, got %#v", payload)
	}
	if data["total_out"] != 100.0 {
		t.Fatalf("expected total_out=100, got %#v", data["total_out"])
	}
	if data["alarm"] != true {
		t.Fatalf("expected alarm=true, got %#v", data["alarm"])
	}
}

func TestTransformCopyMapping(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "copy",
					"from": "$.device_code",
					"to": "device"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"device_code": "acm006",
			"data":        map[string]interface{}{"value": 42.0},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	if payload["device"] != "acm006" {
		t.Fatalf("expected device=acm006, got %#v", payload["device"])
	}
}

func TestTransformTemplateMapping(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "template",
					"to": "message",
					"template": "device={{ .payload.device_code }},temp={{ .payload.data.temperature }}"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"device_code": "acm006",
			"data":        map[string]interface{}{"temperature": 36.5},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	expected := "device=acm006,temp=36.5"
	if payload["message"] != expected {
		t.Fatalf("expected message=%q, got %#v", expected, payload["message"])
	}
}

func TestTransformObjectMappingWithExclude(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "object",
					"key": {
						"template": "device_{{ .payload.id }}"
					},
					"value": {
						"jsonpath": "$.data",
						"exclude": ["secret"]
					}
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"id":   "001",
			"data": map[string]interface{}{"value": 100.0, "secret": "hidden", "visible": true},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	device, ok := payload["device_001"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected device_001 key, got %#v", payload)
	}
	if device["value"] != 100.0 {
		t.Fatalf("expected value=100, got %#v", device["value"])
	}
	if device["visible"] != true {
		t.Fatalf("expected visible=true, got %#v", device["visible"])
	}

	// Check exclude
	if _, exists := device["secret"]; exists {
		t.Fatalf("expected secret to be excluded")
	}
}

func TestTransformNoMetadataCopy(t *testing.T) {
	// Test that without metadata.copy, only mappings are applied
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "copy",
					"from": "$.value",
					"to": "result"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"trace_id": "should-not-appear",
			"value":    42.0,
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	if payload["result"] != 42.0 {
		t.Fatalf("expected result=42, got %#v", payload["result"])
	}
	if _, exists := payload["trace_id"]; exists {
		t.Fatalf("expected trace_id to NOT be in output (no metadata.copy)")
	}
	if _, exists := payload["value"]; exists {
		t.Fatalf("expected value to NOT be in output (no copy mapping for it)")
	}
}

func TestTransformEventEnvelopePreserved(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "copy",
					"from": "$.device",
					"to": "d"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		ID:        "evt-preserve",
		Timestamp: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		Payload: map[string]interface{}{
			"device": "abc",
		},
		Metadata: map[string]string{"trace_id": "trace-123"},
		RuleID:   "incoming",
		Source:   "source-mqtt",
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	got := emitter.events[0]
	if got.ID != event.ID {
		t.Fatalf("expected event id %q, got %q", event.ID, got.ID)
	}
	if !got.Timestamp.Equal(event.Timestamp) {
		t.Fatalf("expected timestamp %s, got %s", event.Timestamp, got.Timestamp)
	}
	if got.Metadata["trace_id"] != "trace-123" {
		t.Fatalf("expected metadata trace_id preserved, got %#v", got.Metadata)
	}
}

func TestTransformInvalidConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name:    "empty mappings",
			config:  `{"process": {"mappings": []}}`,
			wantErr: "process.mappings is required",
		},
		{
			name:    "missing mappings",
			config:  `{"process": {"name": "test"}}`,
			wantErr: "process.mappings is required",
		},
		{
			name:    "copy missing from",
			config:  `{"process": {"mappings": [{"type": "copy", "to": "x"}]}}`,
			wantErr: "from is required for copy",
		},
		{
			name:    "copy missing to",
			config:  `{"process": {"mappings": [{"type": "copy", "from": "$.x"}]}}`,
			wantErr: "to is required for copy",
		},
		{
			name:    "object missing key",
			config:  `{"process": {"mappings": [{"type": "object"}]}}`,
			wantErr: "key.template is required for object",
		},
		{
			name:    "template missing to",
			config:  `{"process": {"mappings": [{"type": "template", "template": "hello"}]}}`,
			wantErr: "to is required for template",
		},
		{
			name:    "template missing template",
			config:  `{"process": {"mappings": [{"type": "template", "to": "x"}]}}`,
			wantErr: "template is required for template",
		},
		{
			name:    "invalid type",
			config:  `{"process": {"mappings": [{"type": "unknown"}]}}`,
			wantErr: "type must be one of: copy, object, template",
		},
		{
			name:    "invalid JSON",
			config:  `{invalid`,
			wantErr: "failed to parse config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &TransformPlugin{}
			err := plugin.Init(sdk.PluginConfig{Raw: []byte(tt.config)})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestTransformNestedFieldAccess(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "copy",
					"from": "$.data.nested.value",
					"to": "result"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"data": map[string]interface{}{
				"nested": map[string]interface{}{
					"value": 99.0,
				},
			},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	if payload["result"] != 99.0 {
		t.Fatalf("expected result=99, got %#v", payload["result"])
	}
}

func TestTransformNilPayload(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "copy",
					"from": "$.x",
					"to": "y"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: nil,
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}
}

func TestTransformMetadataCopyFromPayload(t *testing.T) {
	// Test that metadata.copy correctly copies fields from the input payload
	raw := `{
		"process": {
			"metadata": {
				"copy": ["trace_id", "timestamp"]
			},
			"mappings": [
				{
					"type": "copy",
					"from": "$.value",
					"to": "v"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"trace_id":  "trace-abc",
			"timestamp": 1710000000000.0,
			"value":     42.0,
			"extra":     "should-be-ignored",
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	if payload["trace_id"] != "trace-abc" {
		t.Fatalf("expected trace_id, got %#v", payload["trace_id"])
	}
	if payload["timestamp"] != 1710000000000.0 {
		t.Fatalf("expected timestamp, got %#v", payload["timestamp"])
	}
	if payload["v"] != 42.0 {
		t.Fatalf("expected v=42, got %#v", payload["v"])
	}
	if _, exists := payload["extra"]; exists {
		t.Fatalf("expected extra to NOT be in output")
	}
}

func TestTransformMultipleMappings(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "copy",
					"from": "$.device_code",
					"to": "device"
				},
				{
					"type": "template",
					"to": "summary",
					"template": "Device {{ .payload.device_code }} has value {{ .payload.value }}"
				},
				{
					"type": "object",
					"key": {
						"template": "meta_{{ .payload.device_code }}"
					},
					"value": {
						"jsonpath": "$.data"
					}
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"device_code": "acm006",
			"value":       42.5,
			"data":        map[string]interface{}{"temp": 36.5, "humidity": 60.0},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	if payload["device"] != "acm006" {
		t.Fatalf("expected device=acm006, got %#v", payload["device"])
	}
	if payload["summary"] != "Device acm006 has value 42.5" {
		t.Fatalf("unexpected summary: %#v", payload["summary"])
	}
	meta, ok := payload["meta_acm006"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected meta_acm006 key, got %#v", payload)
	}
	if meta["temp"] != 36.5 {
		t.Fatalf("expected temp=36.5, got %#v", meta["temp"])
	}
}

func TestTransformConfigSchema(t *testing.T) {
	info := (&TransformPlugin{}).Info()

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(info.Capabilities.ConfigSchema), &schema); err != nil {
		t.Fatalf("parse config schema: %v", err)
	}

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing")
	}
	process, ok := properties["process"].(map[string]interface{})
	if !ok {
		t.Fatalf("process schema missing")
	}
	processProps, ok := process["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.properties missing")
	}

	// Check mappings is defined
	if _, ok := processProps["mappings"]; !ok {
		t.Fatalf("process.mappings schema missing")
	}

	// Check metadata is defined
	if _, ok := processProps["metadata"]; !ok {
		t.Fatalf("process.metadata schema missing")
	}
}

func TestTransformTemplateWithFunctions(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "template",
					"to": "upper_device",
					"template": "{{ upper .payload.device_code }}"
				},
				{
					"type": "template",
					"to": "json_value",
					"template": "{{ json .payload.data }}"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"device_code": "acm006",
			"data":        map[string]interface{}{"temp": 36.5},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	if payload["upper_device"] != "ACM006" {
		t.Fatalf("expected upper_device=ACM006, got %#v", payload["upper_device"])
	}

	// json function returns a decoded value (not a string)
	jsonVal, ok := payload["json_value"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected json_value to be decoded map, got %T %#v", payload["json_value"], payload["json_value"])
	}
	if jsonVal["temp"] != 36.5 {
		t.Fatalf("expected temp=36.5 in json_value, got %#v", jsonVal)
	}
}

func TestTransformPluginInfo(t *testing.T) {
	info := (&TransformPlugin{}).Info()
	if info.ID != "transform" {
		t.Fatalf("expected ID transform, got %q", info.ID)
	}
	if info.Type != sdk.PluginTypeProcessor {
		t.Fatalf("expected Type processor, got %v", info.Type)
	}
	if info.Capabilities.InputPorts != 1 {
		t.Fatalf("expected InputPorts=1, got %d", info.Capabilities.InputPorts)
	}
	if info.Capabilities.OutputPorts != 1 {
		t.Fatalf("expected OutputPorts=1, got %d", info.Capabilities.OutputPorts)
	}
}

func TestTransformOnEvents(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "copy",
					"from": "$.x",
					"to": "y"
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	events := []sdk.Event{
		{Payload: map[string]interface{}{"x": 1.0}},
		{Payload: map[string]interface{}{"x": 2.0}},
		{Payload: map[string]interface{}{"x": 3.0}},
	}

	if err := plugin.OnEvents(events); err != nil {
		t.Fatalf("on events: %v", err)
	}
	if len(emitter.events) != 3 {
		t.Fatalf("expected 3 emitted events, got %d", len(emitter.events))
	}
	for i, e := range emitter.events {
		if e.Payload["y"] != float64(i+1) {
			t.Fatalf("event %d: expected y=%d, got %#v", i, i+1, e.Payload["y"])
		}
	}
}

func TestTransformObjectMappingWithTopLevelExclude(t *testing.T) {
	raw := `{
		"process": {
			"mappings": [
				{
					"type": "object",
					"key": {
						"template": "filtered"
					},
					"value": {
						"jsonpath": "$.data"
					},
					"exclude": ["a", "c"]
				}
			]
		}
	}`

	plugin, emitter := initPlugin(t, raw)

	event := sdk.Event{
		Payload: map[string]interface{}{
			"data": map[string]interface{}{"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	payload := emitter.events[0].Payload
	filtered, ok := payload["filtered"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected filtered key, got %#v", payload)
	}
	if len(filtered) != 2 {
		t.Fatalf("expected 2 keys in filtered, got %d: %#v", len(filtered), filtered)
	}
	if filtered["b"] != 2.0 || filtered["d"] != 4.0 {
		t.Fatalf("unexpected filtered content: %#v", filtered)
	}
	if _, exists := filtered["a"]; exists {
		t.Fatalf("expected a to be excluded")
	}
	if _, exists := filtered["c"]; exists {
		t.Fatalf("expected c to be excluded")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsStr(s, substr)
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
