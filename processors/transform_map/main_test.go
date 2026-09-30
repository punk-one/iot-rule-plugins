package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type mapTestLogger struct{}

func (mapTestLogger) Debug(string, ...sdk.Field) {}
func (mapTestLogger) Info(string, ...sdk.Field)  {}
func (mapTestLogger) Warn(string, ...sdk.Field)  {}
func (mapTestLogger) Error(string, ...sdk.Field) {}
func (mapTestLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {
}

type mapTestMetrics struct{}

func (mapTestMetrics) IncCounter(string, map[string]string)       {}
func (mapTestMetrics) Observe(string, float64, map[string]string) {}

type mapTestHealth struct{}

func (mapTestHealth) Report(sdk.HealthReport) error                      { return nil }
func (mapTestHealth) Healthy(string, map[string]string) error            { return nil }
func (mapTestHealth) Degraded(string, string, map[string]string) error   { return nil }
func (mapTestHealth) Recovering(string, string, map[string]string) error { return nil }
func (mapTestHealth) Unhealthy(string, string, map[string]string) error  { return nil }
func (mapTestHealth) Heartbeat(string, map[string]string) error          { return nil }
func (mapTestHealth) Stopped(string, map[string]string) error            { return nil }

type mapTestEmitter struct {
	events []sdk.Event
}

func (e *mapTestEmitter) Publish(event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}

func (e *mapTestEmitter) Ack(string) error { return nil }

func (e *mapTestEmitter) PublishAck(sdk.AckMessage) error { return nil }

func (e *mapTestEmitter) EmitTo(_ string, event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}

func (e *mapTestEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type mapTestRuntimeContext struct {
	emitter *mapTestEmitter
}

func (c mapTestRuntimeContext) RuleID() string                                 { return "rule-transform" }
func (c mapTestRuntimeContext) NodeID() string                                 { return "transform-map-node" }
func (c mapTestRuntimeContext) Logger() sdk.Logger                             { return mapTestLogger{} }
func (c mapTestRuntimeContext) Emitter() sdk.Emitter                           { return c.emitter }
func (c mapTestRuntimeContext) Metrics() sdk.Metrics                           { return mapTestMetrics{} }
func (c mapTestRuntimeContext) Health() sdk.HealthReporter                     { return mapTestHealth{} }
func (c mapTestRuntimeContext) Connector() sdk.ConnectorClient                 { return nil }
func (c mapTestRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

func TestTransformMapPreservesEnvelopeAndExpandsConfiguredMap(t *testing.T) {
	raw := []byte(`{
		"process": {
			"template": {
				"time": "{{ unixMilli .payload.time }}",
				"device_code": "{{ .payload.device_code }}",
				"event_type": "report",
				"trace_snapshot": "{{ .metadata.trace_id }}"
			},
			"expand": [
				{"from": "payload.data"}
			]
		}
	}`)

	plugin := &TransformMapPlugin{}
	if err := plugin.Init(sdk.PluginConfig{Raw: raw}); err != nil {
		t.Fatalf("init plugin: %v", err)
	}

	emitter := &mapTestEmitter{}
	if err := plugin.Start(mapTestRuntimeContext{emitter: emitter}); err != nil {
		t.Fatalf("start plugin: %v", err)
	}

	event := sdk.Event{
		ID:        "evt-1",
		Timestamp: time.Date(2026, 4, 14, 8, 0, 0, 123000000, time.UTC),
		Payload: map[string]interface{}{
			"device_code": "ZH_LDRHL_0001",
			"time":        int64(1776130663187),
			"data": map[string]interface{}{
				"bath_tmp_set":  730.0,
				"hold_metal_wt": 6.97446,
			},
		},
		Metadata: map[string]string{
			"trace_id": "trace-1",
			"protocol": "mqtt",
		},
		RuleID: "incoming-rule",
		Source: "source-mqtt",
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
	if got.Metadata["trace_id"] != "trace-1" {
		t.Fatalf("expected trace_id to be preserved, got %+v", got.Metadata)
	}
	if got.RuleID != "rule-transform" || got.Source != "transform-map-node" {
		t.Fatalf("unexpected routing fields: rule_id=%q source=%q", got.RuleID, got.Source)
	}

	if got.Payload["time"] != "2026-04-14T01:37:43.187Z" {
		t.Fatalf("unexpected time value: %#v", got.Payload["time"])
	}
	if got.Payload["device_code"] != "ZH_LDRHL_0001" {
		t.Fatalf("unexpected device_code: %#v", got.Payload["device_code"])
	}
	if got.Payload["event_type"] != "report" {
		t.Fatalf("unexpected event_type: %#v", got.Payload["event_type"])
	}
	if got.Payload["trace_snapshot"] != "trace-1" {
		t.Fatalf("unexpected trace_snapshot: %#v", got.Payload["trace_snapshot"])
	}
	if got.Payload["bath_tmp_set"] != 730.0 {
		t.Fatalf("expected expanded bath_tmp_set, got %#v", got.Payload["bath_tmp_set"])
	}
	if got.Payload["hold_metal_wt"] != 6.97446 {
		t.Fatalf("expected expanded hold_metal_wt, got %#v", got.Payload["hold_metal_wt"])
	}
}

func TestTransformMapIteratesArrayAndRendersTypedTemplateValues(t *testing.T) {
	raw := []byte(`{
		"process": {
			"iterate": "payload.data.metrics",
			"template": {
				"device_code": "{{ .payload.device_code }}",
				"metric_code": "{{ .item.name }}",
				"value": "{{ json .item.value }}",
				"trace_id": "{{ default .metadata.trace_id \"\" }}"
			}
		}
	}`)

	plugin := &TransformMapPlugin{}
	if err := plugin.Init(sdk.PluginConfig{Raw: raw}); err != nil {
		t.Fatalf("init plugin: %v", err)
	}

	emitter := &mapTestEmitter{}
	if err := plugin.Start(mapTestRuntimeContext{emitter: emitter}); err != nil {
		t.Fatalf("start plugin: %v", err)
	}

	event := sdk.Event{
		ID:        "evt-2",
		Timestamp: time.Date(2026, 4, 14, 9, 0, 0, 0, time.UTC),
		Payload: map[string]interface{}{
			"device_code": "ZH_DYZZ_0002",
			"data": map[string]interface{}{
				"metrics": []interface{}{
					map[string]interface{}{"name": "flow_01", "value": 12.5},
					map[string]interface{}{"name": "flow_02", "value": 18.0},
				},
			},
		},
		Metadata: map[string]string{"trace_id": "trace-2"},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(emitter.events) != 2 {
		t.Fatalf("expected 2 emitted events, got %d", len(emitter.events))
	}

	assertMetricEvent(t, emitter.events[0], "flow_01", 12.5)
	assertMetricEvent(t, emitter.events[1], "flow_02", 18.0)
}

func TestTransformMapSchemaDeclaresTemplateAsJSONStringEditor(t *testing.T) {
	info := (&TransformMapPlugin{}).Info()

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(info.Capabilities.ConfigSchema), &schema); err != nil {
		t.Fatalf("parse config schema: %v", err)
	}

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing or invalid: %#v", schema["properties"])
	}
	process, ok := properties["process"].(map[string]interface{})
	if !ok {
		t.Fatalf("process schema missing or invalid: %#v", properties["process"])
	}
	processProps, ok := process["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.properties missing or invalid: %#v", process["properties"])
	}
	templateSchema, ok := processProps["template"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.template schema missing or invalid: %#v", processProps["template"])
	}
	if templateSchema["type"] != "string" {
		t.Fatalf("expected process.template type=string, got %#v", templateSchema["type"])
	}
	if templateSchema["format"] != "json" {
		t.Fatalf("expected process.template format=json, got %#v", templateSchema["format"])
	}
}

func TestTransformMapSchemaDeclaresIterateAndExpandDescriptions(t *testing.T) {
	info := (&TransformMapPlugin{}).Info()

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(info.Capabilities.ConfigSchema), &schema); err != nil {
		t.Fatalf("parse config schema: %v", err)
	}

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing or invalid: %#v", schema["properties"])
	}
	process, ok := properties["process"].(map[string]interface{})
	if !ok {
		t.Fatalf("process schema missing or invalid: %#v", properties["process"])
	}
	processProps, ok := process["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.properties missing or invalid: %#v", process["properties"])
	}

	iterateSchema, ok := processProps["iterate"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.iterate schema missing or invalid: %#v", processProps["iterate"])
	}
	if !hasNonEmptyString(iterateSchema["description"]) {
		t.Fatalf("expected process.iterate description, got %#v", iterateSchema["description"])
	}

	expandSchema, ok := processProps["expand"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.expand schema missing or invalid: %#v", processProps["expand"])
	}
	if !hasNonEmptyString(expandSchema["description"]) {
		t.Fatalf("expected process.expand description, got %#v", expandSchema["description"])
	}

	itemsSchema, ok := expandSchema["items"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.expand.items schema missing or invalid: %#v", expandSchema["items"])
	}
	itemProps, ok := itemsSchema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("process.expand.items.properties missing or invalid: %#v", itemsSchema["properties"])
	}

	for _, key := range []string{"from", "to", "overwrite"} {
		propSchema, ok := itemProps[key].(map[string]interface{})
		if !ok {
			t.Fatalf("process.expand.items.%s schema missing or invalid: %#v", key, itemProps[key])
		}
		if !hasNonEmptyString(propSchema["description"]) {
			t.Fatalf("expected process.expand.items.%s description, got %#v", key, propSchema["description"])
		}
	}
}

func hasNonEmptyString(v interface{}) bool {
	text, ok := v.(string)
	return ok && strings.TrimSpace(text) != ""
}

func TestTransformMapRejectsInvalidTemplateJSONString(t *testing.T) {
	plugin := &TransformMapPlugin{}
	err := plugin.Init(sdk.PluginConfig{Raw: []byte(`{
		"process": {
			"template": "{invalid"
		}
	}`)})
	if err == nil {
		t.Fatal("expected invalid template JSON to fail")
	}
	if !strings.Contains(err.Error(), "failed to parse process.template JSON") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertMetricEvent(t *testing.T, event sdk.Event, wantMetric string, wantValue float64) {
	t.Helper()

	if event.Payload["device_code"] != "ZH_DYZZ_0002" {
		t.Fatalf("unexpected device_code: %#v", event.Payload["device_code"])
	}
	if event.Payload["metric_code"] != wantMetric {
		t.Fatalf("unexpected metric_code: %#v", event.Payload["metric_code"])
	}

	typedValue, ok := event.Payload["value"].(float64)
	if !ok {
		raw, _ := json.Marshal(event.Payload["value"])
		t.Fatalf("expected float64 value, got %T %s", event.Payload["value"], raw)
	}
	if typedValue != wantValue {
		t.Fatalf("unexpected metric value: want %v got %v", wantValue, typedValue)
	}
	if event.Payload["trace_id"] != "trace-2" {
		t.Fatalf("unexpected trace_id field: %#v", event.Payload["trace_id"])
	}
}
