package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type statusTestLogger struct{}

func (l *statusTestLogger) Debug(string, ...sdk.Field)                                {}
func (l *statusTestLogger) Info(string, ...sdk.Field)                                 {}
func (l *statusTestLogger) Warn(string, ...sdk.Field)                                 {}
func (l *statusTestLogger) Error(string, ...sdk.Field)                                {}
func (l *statusTestLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {}

type statusTestEmitter struct {
	byLabel map[string][]sdk.Event
}

func newStatusTestEmitter() *statusTestEmitter {
	return &statusTestEmitter{byLabel: map[string][]sdk.Event{}}
}

func (e *statusTestEmitter) Publish(event sdk.Event) error {
	e.byLabel[""] = append(e.byLabel[""], event)
	return nil
}

func (e *statusTestEmitter) Ack(string) error                { return nil }
func (e *statusTestEmitter) PublishAck(sdk.AckMessage) error { return nil }
func (e *statusTestEmitter) EmitBatch(events []sdk.Event) error {
	e.byLabel[""] = append(e.byLabel[""], events...)
	return nil
}
func (e *statusTestEmitter) EmitTo(label string, event sdk.Event) error {
	e.byLabel[label] = append(e.byLabel[label], event)
	return nil
}

type statusTestMetrics struct{}

func (m *statusTestMetrics) IncCounter(string, map[string]string)       {}
func (m *statusTestMetrics) Observe(string, float64, map[string]string) {}

type statusTestHealth struct{}

func (r *statusTestHealth) Report(sdk.HealthReport) error                      { return nil }
func (r *statusTestHealth) Healthy(string, map[string]string) error            { return nil }
func (r *statusTestHealth) Degraded(string, string, map[string]string) error   { return nil }
func (r *statusTestHealth) Recovering(string, string, map[string]string) error { return nil }
func (r *statusTestHealth) Unhealthy(string, string, map[string]string) error  { return nil }
func (r *statusTestHealth) Heartbeat(string, map[string]string) error          { return nil }
func (r *statusTestHealth) Stopped(string, map[string]string) error            { return nil }

type statusTestConnector struct{}

func (c *statusTestConnector) Read(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *statusTestConnector) Write(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *statusTestConnector) BatchRead(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *statusTestConnector) BatchWrite(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *statusTestConnector) OpenStream(sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, nil
}
func (c *statusTestConnector) CurrentStatus(string) (sdk.ResourceStatusEvent, bool) {
	return sdk.ResourceStatusEvent{}, false
}

type statusTestStateManager struct {
	store map[string][]byte
}

func newStatusTestStateManager() *statusTestStateManager {
	return &statusTestStateManager{store: map[string][]byte{}}
}

func (m *statusTestStateManager) GetState(key string, state interface{}) error {
	raw, ok := m.store[key]
	if !ok {
		return fmt.Errorf("state not found: %s", key)
	}
	return json.Unmarshal(raw, state)
}

func (m *statusTestStateManager) SetState(key string, state interface{}) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	m.store[key] = raw
	return nil
}

func (m *statusTestStateManager) SetStateWithTTL(key string, state interface{}, ttl time.Duration) error {
	return m.SetState(key, state)
}

func (m *statusTestStateManager) DeleteState(key string) error {
	delete(m.store, key)
	return nil
}

func (m *statusTestStateManager) GenerateKey(keys []string) string {
	return statusTestStringsJoin(keys, ":")
}

type statusTestRuntimeContext struct {
	ruleID  string
	nodeID  string
	emitter sdk.Emitter
	state   sdk.StateManager
}

func (c *statusTestRuntimeContext) RuleID() string                                 { return c.ruleID }
func (c *statusTestRuntimeContext) NodeID() string                                 { return c.nodeID }
func (c *statusTestRuntimeContext) Logger() sdk.Logger                             { return &statusTestLogger{} }
func (c *statusTestRuntimeContext) Emitter() sdk.Emitter                           { return c.emitter }
func (c *statusTestRuntimeContext) Metrics() sdk.Metrics                           { return &statusTestMetrics{} }
func (c *statusTestRuntimeContext) Health() sdk.HealthReporter                     { return &statusTestHealth{} }
func (c *statusTestRuntimeContext) Connector() sdk.ConnectorClient                 { return &statusTestConnector{} }
func (c *statusTestRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }
func (c *statusTestRuntimeContext) State() sdk.StateManager                        { return c.state }

func TestStatusPluginEmitsLifecycleRows(t *testing.T) {
	plugin := &StatusEventPlugin{}
	if err := plugin.Init(sdk.PluginConfig{Raw: []byte(`{}`)}); err != nil {
		t.Fatalf("init plugin: %v", err)
	}

	emitter := newStatusTestEmitter()
	stateMgr := newStatusTestStateManager()
	ctx := &statusTestRuntimeContext{
		ruleID:  "rule-state",
		nodeID:  "process-state",
		emitter: emitter,
		state:   stateMgr,
	}
	if err := plugin.Start(ctx); err != nil {
		t.Fatalf("start plugin: %v", err)
	}

	offline := sdk.NewEvent(map[string]interface{}{
		"device_code": "DEV_1",
		"time":        int64(1775564872785),
		"data": map[string]interface{}{
			"online":           false,
			"connection_state": "disconnected",
		},
	}, nil)
	sdk.SetTraceID(&offline, "trace-state-1")
	if err := plugin.OnEvent(offline); err != nil {
		t.Fatalf("offline event: %v", err)
	}
	if len(emitter.byLabel["redis"]) != 1 {
		t.Fatalf("expected one redis projection, got %d", len(emitter.byLabel["redis"]))
	}
	if len(emitter.byLabel["events"]) != 1 {
		t.Fatalf("expected one event row, got %d", len(emitter.byLabel["events"]))
	}

	redisFields := emitter.byLabel["redis"][0].Payload["fields"].(map[string]interface{})
	if _, ok := redisFields["device_code"]; !ok {
		t.Fatalf("expected snake_case redis field keys, got %+v", redisFields)
	}
	activeEvents := redisFields["active_events"].([]map[string]interface{})
	if len(activeEvents) != 1 {
		t.Fatalf("expected one active event projection, got %+v", activeEvents)
	}
	if activeEvents[0]["event_key"] != "status.offline" {
		t.Fatalf("unexpected active event projection: %+v", activeEvents[0])
	}

	row := emitter.byLabel["events"][0].Payload["data"].(map[string]interface{})
	if row["phase"] != "start" || row["event_key"] != "status.offline" {
		t.Fatalf("unexpected state event row: %+v", row)
	}
	if row["trace_id"] != "trace-state-1" {
		t.Fatalf("expected trace_id to propagate, got %+v", row)
	}

	online := sdk.NewEvent(map[string]interface{}{
		"device_code": "DEV_1",
		"time":        int64(1775564876785),
		"data": map[string]interface{}{
			"online":           true,
			"connection_state": "connected",
		},
	}, nil)
	sdk.SetTraceID(&online, "trace-state-2")
	if err := plugin.OnEvent(online); err != nil {
		t.Fatalf("online event: %v", err)
	}
	if len(emitter.byLabel["events"]) != 2 {
		t.Fatalf("expected two event rows, got %d", len(emitter.byLabel["events"]))
	}

	endRow := emitter.byLabel["events"][1].Payload["data"].(map[string]interface{})
	if endRow["phase"] != "end" || endRow["status"] != "closed" {
		t.Fatalf("unexpected end row: %+v", endRow)
	}
	if endRow["message"] != "device back online" {
		t.Fatalf("expected recovery message, got %+v", endRow)
	}
}

func statusTestStringsJoin(values []string, sep string) string {
	if len(values) == 0 {
		return ""
	}
	result := values[0]
	for _, value := range values[1:] {
		result += sep + value
	}
	return result
}
