package main

import (
	"fmt"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type testLogger struct{}

func (l *testLogger) Debug(msg string, fields ...sdk.Field) {}
func (l *testLogger) Info(msg string, fields ...sdk.Field)  {}
func (l *testLogger) Warn(msg string, fields ...sdk.Field)  {}
func (l *testLogger) Error(msg string, fields ...sdk.Field) {}
func (l *testLogger) LogBatch(level sdk.LogLevel, messages []string, fields []map[string]interface{}) {
}

type testEmitter struct {
	events []sdk.Event
}

func (e *testEmitter) Publish(event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}

func (e *testEmitter) Ack(eventID string) error            { return nil }
func (e *testEmitter) PublishAck(ack sdk.AckMessage) error { return nil }
func (e *testEmitter) EmitTo(label string, event sdk.Event) error {
	return e.Publish(event)
}
func (e *testEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type testMetrics struct{}

func (m *testMetrics) IncCounter(name string, labels map[string]string)             {}
func (m *testMetrics) Observe(name string, value float64, labels map[string]string) {}

type testHealthReporter struct{}

func (r *testHealthReporter) Report(report sdk.HealthReport) error { return nil }
func (r *testHealthReporter) Healthy(msg string, details map[string]string) error {
	return nil
}
func (r *testHealthReporter) Degraded(code, msg string, details map[string]string) error {
	return nil
}
func (r *testHealthReporter) Recovering(code, msg string, details map[string]string) error {
	return nil
}
func (r *testHealthReporter) Unhealthy(code, msg string, details map[string]string) error {
	return nil
}
func (r *testHealthReporter) Heartbeat(msg string, details map[string]string) error {
	return nil
}

type testConnectorClient struct{}

func (c *testConnectorClient) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *testConnectorClient) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *testConnectorClient) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *testConnectorClient) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *testConnectorClient) OpenStream(req sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, nil
}
func (c *testConnectorClient) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return sdk.ResourceStatusEvent{}, false
}

type testStateManager struct {
	store   map[string]interface{}
	lastTTL time.Duration
	usedTTL bool
}

func newTestStateManager() *testStateManager {
	return &testStateManager{store: make(map[string]interface{})}
}

func (m *testStateManager) GetState(key string, state interface{}) error {
	value, ok := m.store[key]
	if !ok {
		return fmt.Errorf("state not found: %s", key)
	}

	ptr, ok := state.(*ChangeState)
	if !ok {
		return nil
	}
	*ptr = value.(ChangeState)
	return nil
}

func (m *testStateManager) SetState(key string, state interface{}) error {
	if value, ok := state.(*ChangeState); ok {
		m.store[key] = *value
	}
	return nil
}

func (m *testStateManager) SetStateWithTTL(key string, state interface{}, ttl time.Duration) error {
	m.usedTTL = true
	m.lastTTL = ttl
	return m.SetState(key, state)
}

func (m *testStateManager) DeleteState(key string) error {
	delete(m.store, key)
	return nil
}

func (m *testStateManager) GenerateKey(keys []string) string {
	return "state-key"
}

type testRuntimeContext struct {
	ruleID  string
	nodeID  string
	logger  sdk.Logger
	emitter sdk.Emitter
	metrics sdk.Metrics
	health  sdk.HealthReporter
	state   sdk.StateManager
	conn    sdk.ConnectorClient
}

func (c *testRuntimeContext) RuleID() string       { return c.ruleID }
func (c *testRuntimeContext) NodeID() string       { return c.nodeID }
func (c *testRuntimeContext) Logger() sdk.Logger   { return c.logger }
func (c *testRuntimeContext) Emitter() sdk.Emitter { return c.emitter }
func (c *testRuntimeContext) Metrics() sdk.Metrics { return c.metrics }
func (c *testRuntimeContext) Health() sdk.HealthReporter {
	return c.health
}
func (c *testRuntimeContext) Connector() sdk.ConnectorClient {
	if c.conn != nil {
		return c.conn
	}
	return &testConnectorClient{}
}
func (c *testRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }
func (c *testRuntimeContext) State() sdk.StateManager                        { return c.state }

func TestGetPathValueSupportsDotPaths(t *testing.T) {
	payload := map[string]interface{}{
		"payload": map[string]interface{}{
			"device_code": "acm006",
			"data": map[string]interface{}{
				"total_out": 12.5,
			},
		},
	}

	value, ok := getPathValue(payload, "payload.device_code")
	if !ok || value != "acm006" {
		t.Fatalf("expected nested device_code, got value=%v ok=%v", value, ok)
	}

	value, ok = getPathValue(payload, "payload.data.total_out")
	if !ok || value != 12.5 {
		t.Fatalf("expected nested total_out, got value=%v ok=%v", value, ok)
	}

	if _, ok := getPathValue(payload, "payload.data.missing"); ok {
		t.Fatalf("expected missing nested path to fail")
	}
}

func TestOnEventUsesStateTTLWhenConfigured(t *testing.T) {
	stateMgr := newTestStateManager()
	plugin := &ChangeDetectPlugin{
		cfg: Config{},
		ctx: &testRuntimeContext{
			ruleID:  "rule-test",
			nodeID:  "detect-node",
			logger:  &testLogger{},
			emitter: &testEmitter{},
			metrics: &testMetrics{},
			health:  &testHealthReporter{},
			state:   stateMgr,
		},
		stateMgr: stateMgr,
		stateTTL: time.Minute,
	}

	if err := plugin.saveState("state-key", &ChangeState{LastValue: 1}); err != nil {
		t.Fatalf("saveState failed: %v", err)
	}

	if !stateMgr.usedTTL {
		t.Fatal("expected SetStateWithTTL to be used")
	}
	if stateMgr.lastTTL != time.Minute {
		t.Fatalf("expected ttl 1m, got %v", stateMgr.lastTTL)
	}
}

func TestOnEventUsesDotPathsForStateAndField(t *testing.T) {
	emitter := &testEmitter{}
	stateMgr := newTestStateManager()
	plugin := &ChangeDetectPlugin{
		cfg: Config{},
		ctx: &testRuntimeContext{
			ruleID:  "rule-test",
			nodeID:  "detect-node",
			logger:  &testLogger{},
			emitter: emitter,
			metrics: &testMetrics{},
			health:  &testHealthReporter{},
			state:   stateMgr,
		},
		stateMgr: stateMgr,
	}
	plugin.cfg.State.KeyBy = []string{"payload.device_code"}
	plugin.cfg.Process.Detect.Field = "payload.data.total_out"
	plugin.cfg.Process.Detect.Type = string(ModeDelta)
	plugin.cfg.Process.Detect.Value = 1
	plugin.cfg.Process.Detect.Direction = string(DirectionAny)
	plugin.cfg.Output.Emit = string(EmitPolicyOnChange)

	stateMgr.store["state-key"] = ChangeState{
		LastValue: 10,
		LastTs:    time.Now().Add(-time.Minute),
	}

	event := sdk.Event{
		ID:        "evt-1",
		Timestamp: time.Now(),
		Payload: map[string]interface{}{
			"payload": map[string]interface{}{
				"device_code": "acm006",
				"data": map[string]interface{}{
					"total_out": 13.5,
				},
			},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("OnEvent failed: %v", err)
	}

	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}

	if emitter.events[0].Payload["_change_detected"] != true {
		t.Fatalf("expected change detected flag to be true, got %v", emitter.events[0].Payload["_change_detected"])
	}

	if got := emitter.events[0].Payload["_change_value"]; got != 3.5 {
		t.Fatalf("expected change value 3.5, got %v", got)
	}
}

func TestOnEventChangedModeDetectsBooleanAndStringFields(t *testing.T) {
	emitter := &testEmitter{}
	stateMgr := newTestStateManager()
	plugin := &ChangeDetectPlugin{
		cfg: Config{},
		ctx: &testRuntimeContext{
			ruleID:  "rule-test",
			nodeID:  "detect-node",
			logger:  &testLogger{},
			emitter: emitter,
			metrics: &testMetrics{},
			health:  &testHealthReporter{},
			state:   stateMgr,
		},
		stateMgr: stateMgr,
	}
	plugin.cfg.State.KeyBy = []string{"deviceCode"}
	plugin.cfg.Process.Detect.Type = string(ModeChanged)
	plugin.cfg.Process.Detect.Field = "data.online"
	plugin.cfg.Process.Detect.Fields = []string{"data.online", "data.connectionState"}
	plugin.cfg.Output.Emit = string(EmitPolicyOnChange)

	stateMgr.store["state-key"] = ChangeState{
		LastValues: map[string]interface{}{
			"data.online":          true,
			"data.connectionState": "connected",
		},
		LastTs: time.Now().Add(-time.Minute),
	}

	event := sdk.Event{
		ID:        "evt-2",
		Timestamp: time.Now(),
		Payload: map[string]interface{}{
			"deviceCode": "qhl0001",
			"data": map[string]interface{}{
				"online":          false,
				"connectionState": "disconnected",
			},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("OnEvent failed: %v", err)
	}

	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}
	if emitter.events[0].Payload["_change_detected"] != true {
		t.Fatalf("expected changed mode to detect change, got %v", emitter.events[0].Payload["_change_detected"])
	}
	if emitter.events[0].Payload["_change_value"] != 2 {
		t.Fatalf("expected 2 changed fields, got %v", emitter.events[0].Payload["_change_value"])
	}
	fields, ok := emitter.events[0].Payload["_change_fields"].([]string)
	if !ok {
		t.Fatalf("expected []string _change_fields, got %#v", emitter.events[0].Payload["_change_fields"])
	}
	if len(fields) != 2 || fields[0] != "data.online" || fields[1] != "data.connectionState" {
		t.Fatalf("unexpected changed fields: %#v", fields)
	}
}

func TestOnEventChangedModeFirstValueDoesNotEmitOnOnChange(t *testing.T) {
	emitter := &testEmitter{}
	stateMgr := newTestStateManager()
	plugin := &ChangeDetectPlugin{
		cfg: Config{},
		ctx: &testRuntimeContext{
			ruleID:  "rule-test",
			nodeID:  "detect-node",
			logger:  &testLogger{},
			emitter: emitter,
			metrics: &testMetrics{},
			health:  &testHealthReporter{},
			state:   stateMgr,
		},
		stateMgr: stateMgr,
	}
	plugin.cfg.State.KeyBy = []string{"deviceCode"}
	plugin.cfg.Process.Detect.Type = string(ModeChanged)
	plugin.cfg.Process.Detect.Fields = []string{"data.online", "data.connectionState"}
	plugin.cfg.Output.Emit = string(EmitPolicyOnChange)

	event := sdk.Event{
		ID:        "evt-3",
		Timestamp: time.Now(),
		Payload: map[string]interface{}{
			"deviceCode": "qhl0001",
			"data": map[string]interface{}{
				"online":          false,
				"connectionState": "disconnected",
			},
		},
	}

	if err := plugin.OnEvent(event); err != nil {
		t.Fatalf("OnEvent failed: %v", err)
	}
	if len(emitter.events) != 0 {
		t.Fatalf("expected first changed-mode event not to emit, got %d", len(emitter.events))
	}

	saved, ok := stateMgr.store["state-key"].(ChangeState)
	if !ok {
		t.Fatalf("expected saved state, got %#v", stateMgr.store["state-key"])
	}
	if saved.LastValues["data.online"] != false || saved.LastValues["data.connectionState"] != "disconnected" {
		t.Fatalf("unexpected stored state: %#v", saved.LastValues)
	}
}

func TestValidateConfigChangedModeAllowsMultipleFieldsWithoutThreshold(t *testing.T) {
	raw := []byte(`{
		"state": {"key_by": ["deviceCode"]},
		"process": {
			"detect": {
				"type": "changed",
				"fields": ["data.online", "data.connectionState"]
			}
		},
		"output": {"emit": "on_change"}
	}`)

	var plugin ChangeDetectPlugin
	if err := plugin.validateConfig(sdk.PluginConfig{Raw: raw}); err != nil {
		t.Fatalf("validateConfig failed: %v", err)
	}
	if len(plugin.cfg.Process.Detect.Fields) != 2 {
		t.Fatalf("expected normalized fields, got %#v", plugin.cfg.Process.Detect.Fields)
	}
}

func TestDetectChangeInitInvalidConfigReturnsFatal(t *testing.T) {
	err := (&ChangeDetectPlugin{}).Init(sdk.PluginConfig{Raw: []byte(`{}`)})
	if err == nil {
		t.Fatal("expected init error, got nil")
	}
	if !sdk.IsFatal(err) {
		t.Fatalf("expected fatal init error, got %v", err)
	}
}

func TestDetectChangeStartWithoutStateManagerReturnsFatal(t *testing.T) {
	err := (&ChangeDetectPlugin{}).Start(&struct {
		testRuntimeContext
	}{
		testRuntimeContext: testRuntimeContext{
			ruleID:  "rule-test",
			nodeID:  "detect-node",
			logger:  &testLogger{},
			emitter: &testEmitter{},
			metrics: &testMetrics{},
			health:  &testHealthReporter{},
		},
	})
	if err == nil {
		t.Fatal("expected start error, got nil")
	}
	if !sdk.IsFatal(err) {
		t.Fatalf("expected fatal start error, got %v", err)
	}
}

func TestDetectChangeOnEventWithoutStartReturnsFatal(t *testing.T) {
	err := (&ChangeDetectPlugin{}).OnEvent(sdk.Event{})
	if err == nil {
		t.Fatal("expected runtime error, got nil")
	}
	if !sdk.IsFatal(err) {
		t.Fatalf("expected fatal runtime error, got %v", err)
	}
}

func TestDetectChangePublishFailureReturnsRetryable(t *testing.T) {
	emitter := &publishErrorEmitter{err: fmt.Errorf("downstream busy")}
	stateMgr := newTestStateManager()
	plugin := &ChangeDetectPlugin{
		cfg: Config{},
		ctx: &testRuntimeContext{
			ruleID:  "rule-test",
			nodeID:  "detect-node",
			logger:  &testLogger{},
			emitter: emitter,
			metrics: &testMetrics{},
			health:  &testHealthReporter{},
			state:   stateMgr,
		},
		stateMgr: stateMgr,
	}
	plugin.cfg.State.KeyBy = []string{"device_code"}
	plugin.cfg.Process.Detect.Field = "value"
	plugin.cfg.Process.Detect.Type = string(ModeDelta)
	plugin.cfg.Process.Detect.Value = 1
	plugin.cfg.Process.Detect.Direction = string(DirectionAny)
	plugin.cfg.Output.Emit = string(EmitPolicyAlways)

	event := sdk.Event{
		ID:        "evt-1",
		Timestamp: time.Now(),
		Payload: map[string]interface{}{
			"device_code": "acm006",
			"value":       13.5,
		},
	}

	err := plugin.OnEvent(event)
	if err == nil {
		t.Fatal("expected publish error, got nil")
	}
	if !sdk.IsRetryable(err) {
		t.Fatalf("expected retryable publish error, got %v", err)
	}
}

type publishErrorEmitter struct {
	err error
}

func (e *publishErrorEmitter) Publish(event sdk.Event) error              { return e.err }
func (e *publishErrorEmitter) Ack(eventID string) error                   { return nil }
func (e *publishErrorEmitter) PublishAck(ack sdk.AckMessage) error        { return nil }
func (e *publishErrorEmitter) EmitTo(label string, event sdk.Event) error { return e.err }
func (e *publishErrorEmitter) EmitBatch(events []sdk.Event) error         { return e.err }
