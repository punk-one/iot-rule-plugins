package main

import (
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestConfigSchemaPublishesLargeEditorAndMainExample(t *testing.T) {
	var schemaDoc map[string]interface{}
	if err := json.Unmarshal([]byte(configSchema), &schemaDoc); err != nil {
		t.Fatalf("decode config schema: %v", err)
	}
	properties, ok := schemaDoc["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("config schema properties missing")
	}
	code, ok := properties["code"].(map[string]interface{})
	if !ok {
		t.Fatal("code schema missing")
	}
	if code["format"] != "code" || code["x-ui-height"] != "520px" {
		t.Fatalf("unexpected code editor schema: %+v", code)
	}
	example, _ := code["default"].(string)
	if !strings.Contains(example, "function main(input)") || !strings.Contains(example, "console.log") {
		t.Fatalf("standard main example is incomplete: %q", example)
	}
}

type testEmitter struct{ events []sdk.Event }

func (e *testEmitter) Publish(event sdk.Event) error          { e.events = append(e.events, event); return nil }
func (*testEmitter) Ack(string) error                         { return nil }
func (*testEmitter) PublishAck(sdk.AckMessage) error          { return nil }
func (e *testEmitter) EmitTo(_ string, event sdk.Event) error { return e.Publish(event) }
func (e *testEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type testContext struct{ emitter *testEmitter }

func (testContext) RuleID() string                                 { return "rule-js" }
func (testContext) NodeID() string                                 { return "js-1" }
func (testContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c testContext) Emitter() sdk.Emitter                         { return c.emitter }
func (testContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (testContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (testContext) Connector() sdk.ConnectorClient                 { return nil }
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

func TestTransformsEventWithDocumentedContract(t *testing.T) {
	p := &JavaScriptTransformProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{
		"timeout_ms":100,
		"code":"function main(input) { console.log('input', JSON.stringify(input)); return { payload: { value: input.payload.value * 2 }, metadata: { origin: input.metadata.origin } }; }"
	}`)}); err != nil {
		t.Fatal(err)
	}
	emitter := &testEmitter{}
	if err := p.Start(testContext{emitter: emitter}); err != nil {
		t.Fatal(err)
	}
	input := sdk.NewEvent(map[string]interface{}{"value": 21}, map[string]string{"origin": "plc"})
	if err := p.OnEvent(input); err != nil {
		t.Fatal(err)
	}
	if len(emitter.events) != 1 || emitter.events[0].Payload["value"] != float64(42) {
		t.Fatalf("events=%+v", emitter.events)
	}
	if emitter.events[0].RuleID != "rule-js" || emitter.events[0].Source != "js-1" {
		t.Fatalf("routing=%+v", emitter.events[0])
	}
}

func TestNullDropsEvent(t *testing.T) {
	p := &JavaScriptTransformProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"code":"function main(input) { return null; }"}`)}); err != nil {
		t.Fatal(err)
	}
	emitter := &testEmitter{}
	if err := p.Start(testContext{emitter: emitter}); err != nil {
		t.Fatal(err)
	}
	if err := p.OnEvent(sdk.NewEvent(map[string]interface{}{}, nil)); err != nil {
		t.Fatal(err)
	}
	if len(emitter.events) != 0 {
		t.Fatalf("events=%d", len(emitter.events))
	}
}

func TestInfiniteLoopTimesOut(t *testing.T) {
	p := &JavaScriptTransformProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"timeout_ms":5,"code":"function main(input) { for (;;) {} }"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(testContext{emitter: &testEmitter{}}); err != nil {
		t.Fatal(err)
	}
	err := p.OnEvent(sdk.NewEvent(map[string]interface{}{}, nil))
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("err=%v", err)
	}
}

func TestRejectsUndocumentedResultFields(t *testing.T) {
	p := &JavaScriptTransformProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"code":"function main(input) { return { payload: {}, unexpected: true }; }"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(testContext{emitter: &testEmitter{}}); err != nil {
		t.Fatal(err)
	}
	err := p.OnEvent(sdk.NewEvent(map[string]interface{}{}, nil))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("err=%v", err)
	}
}

func TestGetterExportIsCoveredByTimeout(t *testing.T) {
	p := &JavaScriptTransformProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"timeout_ms":5,"code":"function main(input) { return { get payload() { for (;;) {} } }; }"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(testContext{emitter: &testEmitter{}}); err != nil {
		t.Fatal(err)
	}
	err := p.OnEvent(sdk.NewEvent(map[string]interface{}{}, nil))
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("expected getter export timeout, got %v", err)
	}
}

func TestThrowingGetterReturnsErrorWithoutPanic(t *testing.T) {
	p := &JavaScriptTransformProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"timeout_ms":100,"code":"function main(input) { return { get payload() { throw new Error('getter failed'); } }; }"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(testContext{emitter: &testEmitter{}}); err != nil {
		t.Fatal(err)
	}
	if err := p.OnEvent(sdk.NewEvent(map[string]interface{}{}, nil)); err == nil || !strings.Contains(err.Error(), "getter failed") {
		t.Fatalf("expected safe getter error, got %v", err)
	}
}

func TestOversizedJavaScriptOutputIsRejected(t *testing.T) {
	p := &JavaScriptTransformProcessor{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"timeout_ms":100,"code":"function main(input) { return { payload: { value: 'x'.repeat(1100000) } }; }"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(testContext{emitter: &testEmitter{}}); err != nil {
		t.Fatal(err)
	}
	err := p.OnEvent(sdk.NewEvent(map[string]interface{}{}, nil))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected output limit error, got %v", err)
	}
}
