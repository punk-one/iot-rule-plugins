package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

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

type sinkTestEmitter struct{ events []sdk.Event }

func (e *sinkTestEmitter) Publish(event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sinkTestEmitter) Ack(string) error                { return nil }
func (e *sinkTestEmitter) PublishAck(sdk.AckMessage) error { return nil }
func (e *sinkTestEmitter) EmitTo(_ string, event sdk.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (e *sinkTestEmitter) EmitBatch(events []sdk.Event) error {
	e.events = append(e.events, events...)
	return nil
}

type pgConnector struct {
	readResp  sdk.ConnectorResponse
	writeResp sdk.ConnectorResponse
	reads     []sdk.ConnectorRequest
	writes    []sdk.ConnectorRequest
	writeFn   func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c *pgConnector) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.reads = append(c.reads, req)
	return c.readResp, nil
}
func (c *pgConnector) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.writes = append(c.writes, req)
	if c.writeFn != nil {
		return c.writeFn(req)
	}
	return c.writeResp, nil
}
func (c *pgConnector) BatchRead(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *pgConnector) BatchWrite(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}
func (c *pgConnector) OpenStream(sdk.StreamOpenRequest) (sdk.StreamConsumer, error) { return nil, nil }
func (c *pgConnector) CurrentStatus(string) (sdk.ResourceStatusEvent, bool) {
	return sdk.ResourceStatusEvent{ResourceID: "pg", Status: sdk.ResourceStatusHealthy}, true
}

type pgRuntimeContext struct{ connector sdk.ConnectorClient }

func (c pgRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c pgRuntimeContext) NodeID() string                                 { return "sink-node" }
func (c pgRuntimeContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c pgRuntimeContext) Emitter() sdk.Emitter                           { return &sinkTestEmitter{} }
func (c pgRuntimeContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (c pgRuntimeContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (c pgRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c pgRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

func TestPostgresConfigRequiresConnectorRef(t *testing.T) {
	var sink PostgresSink
	err := sink.validateConfig([]byte(`{"output":{"table":"metrics"}}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestPostgresConfigSchemaHidesMaxBytes(t *testing.T) {
	info := (&PostgresSink{}).Info()

	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(info.Capabilities.ConfigSchema), &schema); err != nil {
		t.Fatalf("parse config schema: %v", err)
	}

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing or invalid: %#v", schema["properties"])
	}
	outputSchema, ok := properties["output"].(map[string]interface{})
	if !ok {
		t.Fatalf("output schema missing or invalid: %#v", properties["output"])
	}
	outputProps, ok := outputSchema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("output.properties missing or invalid: %#v", outputSchema["properties"])
	}
	batchSchema, ok := outputProps["batch"].(map[string]interface{})
	if !ok {
		t.Fatalf("output.batch schema missing or invalid: %#v", outputProps["batch"])
	}
	batchProps, ok := batchSchema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("output.batch.properties missing or invalid: %#v", batchSchema["properties"])
	}
	if _, ok := batchProps["max_rows"]; !ok {
		t.Fatalf("expected output.batch.max_rows in schema")
	}
	if _, ok := batchProps["flush_interval_ms"]; !ok {
		t.Fatalf("expected output.batch.flush_interval_ms in schema")
	}
	if _, ok := batchProps["max_bytes"]; ok {
		t.Fatalf("did not expect output.batch.max_bytes in schema")
	}
}

func TestPostgresSinkSingleModeWritesTopLevelPayloadAsColumns(t *testing.T) {
	connector := &pgConnector{}
	sink := &PostgresSink{cfg: Config{ConnectorRef: "postgres"}, tableTemplate: "metrics", writeMode: writeModeSingle, ctx: pgRuntimeContext{connector: connector}, tableColumns: make(map[string]map[string]bool)}

	event := sdk.Event{Timestamp: time.Date(2026, 4, 14, 8, 0, 0, 0, time.UTC), Payload: map[string]interface{}{
		"observed_at":  "2026-04-14T01:37:43.187Z",
		"device_code":  "ZH_LDRHL_0001",
		"bath_tmp_set": 730.0,
	}, Metadata: map[string]string{"table_suffix": "ignored"}}

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}
	if len(connector.reads) != 0 {
		t.Fatalf("expected no schema reads, got %d", len(connector.reads))
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one connector write, got %d", len(connector.writes))
	}

	var payload sqlWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal write payload: %v", err)
	}
	if !strings.Contains(payload.SQL, "INSERT INTO metrics") {
		t.Fatalf("unexpected sql: %s", payload.SQL)
	}
	if len(payload.Args) != 3 {
		t.Fatalf("expected 3 args, got %d (%+v)", len(payload.Args), payload.Args)
	}
	assertArgPresent(t, payload.Args, "2026-04-14T01:37:43.187Z")
	assertArgPresent(t, payload.Args, "ZH_LDRHL_0001")
	assertArgPresent(t, payload.Args, 730.0)
	if containsArg(payload.Args, event.Timestamp) {
		t.Fatalf("sink should not inject event.Timestamp into payload args: %+v", payload.Args)
	}
}

func TestPostgresSinkBatchModeFlushesWhenMaxRowsReached(t *testing.T) {
	connector := &pgConnector{}
	sink := &PostgresSink{cfg: Config{ConnectorRef: "postgres"}, tableTemplate: "metrics", writeMode: writeModeBatch, batchConfig: BatchConfig{MaxRows: 2, FlushIntervalMS: 60000, MaxBytes: 512 * 1024}, ctx: pgRuntimeContext{connector: connector}, tableColumns: make(map[string]map[string]bool)}

	first := sdk.Event{Payload: map[string]interface{}{"observed_at": "2026-04-14T01:37:43.187Z", "device_code": "dev-1", "value": 1.0}}
	second := sdk.Event{Payload: map[string]interface{}{"observed_at": "2026-04-14T01:37:44.187Z", "device_code": "dev-2", "value": 2.0}}

	if err := sink.OnEvent(first); err != nil {
		t.Fatalf("buffer first event: %v", err)
	}
	if len(connector.writes) != 0 {
		t.Fatalf("expected no write before reaching max_rows, got %d", len(connector.writes))
	}
	if err := sink.OnEvent(second); err != nil {
		t.Fatalf("buffer second event: %v", err)
	}
	if len(connector.writes) != 1 {
		t.Fatalf("expected one batched write, got %d", len(connector.writes))
	}

	var payload sqlWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal write payload: %v", err)
	}
	if !strings.Contains(payload.SQL, "INSERT INTO metrics") {
		t.Fatalf("unexpected sql: %s", payload.SQL)
	}
	if !strings.Contains(payload.SQL, "), (") {
		t.Fatalf("expected multi-row insert sql, got %s", payload.SQL)
	}
	if len(payload.Args) != 6 {
		t.Fatalf("expected 6 args for two rows, got %d (%+v)", len(payload.Args), payload.Args)
	}
	assertArgPresent(t, payload.Args, "dev-1")
	assertArgPresent(t, payload.Args, "dev-2")
	assertArgPresent(t, payload.Args, 1.0)
	assertArgPresent(t, payload.Args, 2.0)
}

func assertArgPresent(t *testing.T, args []interface{}, want interface{}) {
	t.Helper()
	if !containsArg(args, want) {
		t.Fatalf("expected arg %#v in %+v", want, args)
	}
}

func containsArg(args []interface{}, want interface{}) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}
