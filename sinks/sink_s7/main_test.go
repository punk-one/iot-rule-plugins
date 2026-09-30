package main

import (
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type noopLogger struct{}

func (noopLogger) Debug(msg string, fields ...sdk.Field) {}
func (noopLogger) Info(msg string, fields ...sdk.Field)  {}
func (noopLogger) Warn(msg string, fields ...sdk.Field)  {}
func (noopLogger) Error(msg string, fields ...sdk.Field) {}
func (noopLogger) LogBatch(level sdk.LogLevel, messages []string, fields []map[string]interface{}) {
}

type noopMetrics struct{}

func (noopMetrics) IncCounter(name string, labels map[string]string)             {}
func (noopMetrics) Observe(name string, value float64, labels map[string]string) {}

type noopHealth struct{}

func (noopHealth) Report(report sdk.HealthReport) error                         { return nil }
func (noopHealth) Healthy(msg string, details map[string]string) error          { return nil }
func (noopHealth) Degraded(code, msg string, details map[string]string) error   { return nil }
func (noopHealth) Recovering(code, msg string, details map[string]string) error { return nil }
func (noopHealth) Unhealthy(code, msg string, details map[string]string) error  { return nil }
func (noopHealth) Heartbeat(msg string, details map[string]string) error        { return nil }
func (noopHealth) Stopped(msg string, details map[string]string) error          { return nil }

type sinkTestEmitter struct{ events []sdk.Event }

func (e *sinkTestEmitter) Publish(event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sinkTestEmitter) Ack(eventID string) error            { return nil }
func (e *sinkTestEmitter) PublishAck(ack sdk.AckMessage) error { return nil }
func (e *sinkTestEmitter) EmitTo(label string, event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sinkTestEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type sinkTestConnector struct {
	resp   sdk.ConnectorResponse
	status sdk.ResourceStatusEvent
	reqs   []sdk.ConnectorRequest
}

func (c *sinkTestConnector) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sinkTestConnector) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sinkTestConnector) BatchRead(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *sinkTestConnector) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return c.status, true
}
func (c *sinkTestConnector) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.reqs = append(c.reqs, req)
	return c.resp, nil
}
func (c *sinkTestConnector) OpenStream(req sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, sdk.ErrNotImplemented
}

type sinkRuntimeContext struct {
	connector sdk.ConnectorClient
	events    <-chan sdk.ResourceStatusEvent
}

func (c sinkRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c sinkRuntimeContext) NodeID() string                                 { return "sink-node" }
func (c sinkRuntimeContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c sinkRuntimeContext) Emitter() sdk.Emitter                           { return &sinkTestEmitter{} }
func (c sinkRuntimeContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (c sinkRuntimeContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (c sinkRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c sinkRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return c.events }

func TestSinkConfigRequiresConnectorRef(t *testing.T) {
	var sink S7Sink
	err := sink.validateConfig([]byte(`{"points":[{"address":"DB1.DBX0.0","name":"x","value_type":"Bool"}]}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestSinkOnEventUsesConnectorBatchWrite(t *testing.T) {
	connector := &sinkTestConnector{
		status: sdk.ResourceStatusEvent{ResourceID: "s7-1", Status: sdk.ResourceStatusHealthy},
	}
	sink := &S7Sink{
		cfg: S7SinkConfig{
			ConnectorRef: "s7-1",
			Points: []SinkPointDef{
				{Address: "DB1.DBX0.0", Name: "running", ValueType: "Bool"},
			},
		},
		ctx:    sinkRuntimeContext{connector: connector},
		stopCh: make(chan struct{}),
		pointIndex: map[string]SinkPointDef{
			"running": {Address: "DB1.DBX0.0", Name: "running", ValueType: "Bool"},
		},
	}
	sink.cfg.Input.DataField = "data"
	sink.setStatus(sdk.ResourceStatusHealthy)

	err := sink.OnEvent(sdk.NewEvent(map[string]interface{}{
		"data": map[string]interface{}{
			"running": true,
		},
	}, nil))
	if err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.reqs) != 1 {
		t.Fatalf("expected one connector request, got %d", len(connector.reqs))
	}
	var payload sinkWritePayload
	if err := json.Unmarshal(connector.reqs[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(payload.Writes) != 1 || payload.Writes[0].Address != "DB1.DBX0.0" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestSinkBuffersWhenDisconnected(t *testing.T) {
	sink := &S7Sink{
		cfg: S7SinkConfig{
			ConnectorRef: "s7-1",
			Buffer: struct {
				Enabled         bool `json:"enabled"`
				MaxEvents       int  `json:"max_events"`
				FlushIntervalMs int  `json:"flush_interval_ms"`
			}{Enabled: true, MaxEvents: 4, FlushIntervalMs: 1000},
		},
		stopCh: make(chan struct{}),
	}
	sink.setStatus(sdk.ResourceStatusDisconnected)

	err := sink.bufferEvent(sdk.NewEvent(map[string]interface{}{"data": map[string]interface{}{"x": 1}}, nil))
	if err != nil {
		t.Fatalf("buffer event: %v", err)
	}
	if len(sink.buffer) != 1 {
		t.Fatalf("expected one buffered event, got %d", len(sink.buffer))
	}
}
