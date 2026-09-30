package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sourceTestConnector struct {
	resp   sdk.ConnectorResponse
	status sdk.ResourceStatusEvent
	reqs   []sdk.ConnectorRequest
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
	return nil, nil
}
func (c *sourceTestConnector) CurrentStatus(resourceRef string) (sdk.ResourceStatusEvent, bool) {
	return c.status, true
}

type sourceTestEmitter struct{ events []sdk.Event }

func (e *sourceTestEmitter) Publish(event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sourceTestEmitter) Ack(eventID string) error            { return nil }
func (e *sourceTestEmitter) PublishAck(ack sdk.AckMessage) error { return nil }
func (e *sourceTestEmitter) EmitTo(label string, event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sourceTestEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

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

type sourceRuntimeContext struct {
	connector sdk.ConnectorClient
	emitter   sdk.Emitter
	events    <-chan sdk.ResourceStatusEvent
}

func (c sourceRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c sourceRuntimeContext) NodeID() string                                 { return "source-node" }
func (c sourceRuntimeContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c sourceRuntimeContext) Emitter() sdk.Emitter                           { return c.emitter }
func (c sourceRuntimeContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (c sourceRuntimeContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (c sourceRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c sourceRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return c.events }

func TestSourceConfigRequiresConnectorRef(t *testing.T) {
	var source TimescaleSource
	err := source.validateConfig([]byte(`{"query":{"sql":"select 1"}}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestSourcePollUsesConnectorAndPublishesEvents(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"rows": []map[string]any{
			{
				"time":        "2026-04-05T10:00:00Z",
				"device_code": "dev-1",
				"temp":        23.5,
				"running":     true,
			},
		},
	})
	connector := &sourceTestConnector{
		resp: sdk.ConnectorResponse{Payload: payload},
		status: sdk.ResourceStatusEvent{
			ResourceID: "pg-1",
			Status:     sdk.ResourceStatusHealthy,
		},
	}
	emitter := &sourceTestEmitter{}
	source := &TimescaleSource{
		cfg: TimescaleSourceConfig{
			ConnectorRef: "pg-1",
			Query: struct {
				SQL     string        `json:"sql"`
				Args    []interface{} `json:"args,omitempty"`
				MaxRows int           `json:"max_rows,omitempty"`
			}{
				SQL: "select * from telemetry",
			},
		},
		ctx:    sourceRuntimeContext{connector: connector, emitter: emitter},
		stopCh: make(chan struct{}),
	}
	source.cfg.Output.IncludeMeta = true
	source.cfg.Output.DeviceCodeColumn = "device_code"
	source.cfg.Output.TimeColumn = "time"
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
	got := emitter.events[0].Payload["data"].(map[string]interface{})
	if got["temp"] != 23.5 || got["running"] != true {
		t.Fatalf("unexpected payload: %+v", emitter.events[0].Payload)
	}
	if emitter.events[0].Payload["device_code"] != "dev-1" {
		t.Fatalf("unexpected device_code: %+v", emitter.events[0].Payload)
	}
}

func TestSourceNextDelayBacksOffOnDisconnected(t *testing.T) {
	source := &TimescaleSource{}
	source.cfg.Polling.IntervalMs = 1000
	source.cfg.Polling.MaxBackoffMs = 10000
	source.setStatus(sdk.ResourceStatusDisconnected)

	delay := source.nextDelay(1)
	if delay < 8*time.Second {
		t.Fatalf("expected disconnected backoff, got %s", delay)
	}
}
