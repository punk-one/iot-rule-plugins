package main

import (
	"encoding/json"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type testEmitter struct{ events []sdk.Event }

func (e *testEmitter) Publish(event sdk.Event) error          { e.events = append(e.events, event); return nil }
func (*testEmitter) Ack(string) error                         { return nil }
func (*testEmitter) PublishAck(sdk.AckMessage) error          { return nil }
func (e *testEmitter) EmitTo(_ string, event sdk.Event) error { return e.Publish(event) }
func (e *testEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type testConnector struct{ request sdk.ConnectorRequest }

func (c *testConnector) Read(request sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.request = request
	return sdk.ConnectorResponse{Payload: []byte(`{"fields":{"42":"Widget"}}`)}, nil
}
func (c *testConnector) Write(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *testConnector) BatchRead(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *testConnector) BatchWrite(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (*testConnector) OpenStream(sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, sdk.ErrNotImplemented
}
func (*testConnector) CurrentStatus(string) (sdk.ResourceStatusEvent, bool) {
	return sdk.ResourceStatusEvent{}, false
}

type testContext struct {
	emitter   *testEmitter
	connector *testConnector
}

func (testContext) RuleID() string                                 { return "rule-1" }
func (testContext) NodeID() string                                 { return "mapping-1" }
func (testContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c testContext) Emitter() sdk.Emitter                         { return c.emitter }
func (testContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (testContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (c testContext) Connector() sdk.ConnectorClient               { return c.connector }
func (testContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

type noopLogger struct{}

func (noopLogger) Debug(string, ...sdk.Field)                                {}
func (noopLogger) Info(string, ...sdk.Field)                                 {}
func (noopLogger) Warn(string, ...sdk.Field)                                 {}
func (noopLogger) Error(string, ...sdk.Field)                                {}
func (noopLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {}

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

func TestMapsRelationshipWithHashGetWithoutMutatingInput(t *testing.T) {
	p := &RedisRelationshipMappingProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{
		"connector_ref":"redis-main","default_hash_key":"products",
		"mappings":[{"source_path":"data.product_id","target_path":"data.product_name"}]
	}`)}); err != nil {
		t.Fatal(err)
	}
	emitter, connector := &testEmitter{}, &testConnector{}
	if err := p.Start(testContext{emitter: emitter, connector: connector}); err != nil {
		t.Fatal(err)
	}
	data := map[string]interface{}{"product_id": "42"}
	if err := p.OnEvent(sdk.NewEvent(map[string]interface{}{"data": data}, nil)); err != nil {
		t.Fatal(err)
	}
	if _, exists := data["product_name"]; exists {
		t.Fatal("input event was mutated")
	}
	if connector.request.Target != "hash_get" {
		t.Fatalf("target=%q", connector.request.Target)
	}
	var request map[string]string
	if err := json.Unmarshal(connector.request.Payload, &request); err != nil {
		t.Fatal(err)
	}
	if request["key"] != "products" || request["field"] != "42" {
		t.Fatalf("request=%v", request)
	}
	got := emitter.events[0].Payload["data"].(map[string]interface{})["product_name"]
	if got != "Widget" {
		t.Fatalf("mapped value=%v", got)
	}
}

func TestDefaultPolicyWritesConfiguredValue(t *testing.T) {
	p := &RedisRelationshipMappingProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{
		"connector_ref":"redis-main","default_hash_key":"products",
		"mappings":[{"source_path":"missing","target_path":"status","on_missing":"default","default_value":"unknown"}]
	}`)}); err != nil {
		t.Fatal(err)
	}
	emitter := &testEmitter{}
	if err := p.Start(testContext{emitter: emitter, connector: &testConnector{}}); err != nil {
		t.Fatal(err)
	}
	if err := p.OnEvent(sdk.NewEvent(map[string]interface{}{}, nil)); err != nil {
		t.Fatal(err)
	}
	if emitter.events[0].Payload["status"] != "unknown" {
		t.Fatalf("payload=%v", emitter.events[0].Payload)
	}
}
