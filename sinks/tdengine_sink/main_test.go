package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type noopLogger struct{}

func (noopLogger) Debug(string, ...sdk.Field) {}
func (noopLogger) Info(string, ...sdk.Field)  {}
func (noopLogger) Warn(string, ...sdk.Field)  {}
func (noopLogger) Error(string, ...sdk.Field) {}
func (noopLogger) LogBatch(sdk.LogLevel, []string, []map[string]interface{}) {
}

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

type tdConnector struct {
	readResp  sdk.ConnectorResponse
	writeResp sdk.ConnectorResponse
	reads     []sdk.ConnectorRequest
	writes    []sdk.ConnectorRequest
	writeFn   func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c *tdConnector) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.reads = append(c.reads, req)
	return c.readResp, nil
}

func (c *tdConnector) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.writes = append(c.writes, req)
	if c.writeFn != nil {
		return c.writeFn(req)
	}
	return c.writeResp, nil
}

func (c *tdConnector) BatchRead(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *tdConnector) BatchWrite(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return sdk.ConnectorResponse{}, nil
}

func (c *tdConnector) OpenStream(sdk.StreamOpenRequest) (sdk.StreamConsumer, error) {
	return nil, nil
}

func (c *tdConnector) CurrentStatus(string) (sdk.ResourceStatusEvent, bool) {
	return sdk.ResourceStatusEvent{ResourceID: "td", Status: sdk.ResourceStatusHealthy}, true
}

type tdRuntimeContext struct{ connector sdk.ConnectorClient }

func (c tdRuntimeContext) RuleID() string                                 { return "rule-1" }
func (c tdRuntimeContext) NodeID() string                                 { return "sink-node" }
func (c tdRuntimeContext) Logger() sdk.Logger                             { return noopLogger{} }
func (c tdRuntimeContext) Emitter() sdk.Emitter                           { return &sinkTestEmitter{} }
func (c tdRuntimeContext) Metrics() sdk.Metrics                           { return noopMetrics{} }
func (c tdRuntimeContext) Health() sdk.HealthReporter                     { return noopHealth{} }
func (c tdRuntimeContext) Connector() sdk.ConnectorClient                 { return c.connector }
func (c tdRuntimeContext) ResourceEvents() <-chan sdk.ResourceStatusEvent { return nil }

func TestTDengineConfigRequiresConnectorRef(t *testing.T) {
	var sink TDengineSink
	err := sink.validateConfig([]byte(`{"output":{"table":"metrics"}}`))
	if err == nil || !strings.Contains(err.Error(), "connector_ref is required") {
		t.Fatalf("expected connector_ref validation error, got %v", err)
	}
}

func TestTDengineConfigSingleModeIgnoresBatchConfig(t *testing.T) {
	var sink TDengineSink
	err := sink.validateConfig([]byte(`{
		"connector_ref": "tdengine",
		"output": {
			"table": "metrics",
			"write_mode": "single",
			"batch": {
				"max_rows": 100,
				"flush_interval_ms": 500,
				"max_bytes": 2048
			}
		}
	}`))
	if err != nil {
		t.Fatalf("expected single mode to ignore batch config, got %v", err)
	}
	if sink.writeMode != writeModeSingle {
		t.Fatalf("expected write mode single, got %q", sink.writeMode)
	}
	if sink.batchConfig != (BatchConfig{}) {
		t.Fatalf("expected batch config to be ignored in single mode, got %+v", sink.batchConfig)
	}
}

func TestTDengineConfigSchemaDescribesOutputFields(t *testing.T) {
	info := (&TDengineSink{}).Info()

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

	for _, key := range []string{"table", "column_mapping", "auto_create_columns", "write_mode", "batch"} {
		propSchema, ok := outputProps[key].(map[string]interface{})
		if !ok {
			t.Fatalf("output.%s schema missing or invalid: %#v", key, outputProps[key])
		}
		// column_mapping doesn't require title/description
		if key == "column_mapping" {
			continue
		}
		if !hasNonEmptyString(propSchema["title"]) {
			t.Fatalf("expected output.%s title, got %#v", key, propSchema["title"])
		}
		if !hasNonEmptyString(propSchema["description"]) {
			t.Fatalf("expected output.%s description, got %#v", key, propSchema["description"])
		}
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
	if _, ok := batchProps["max_bytes"]; !ok {
		t.Fatalf("expected output.batch.max_bytes in schema")
	}
}

func TestTDengineSinkSingleModeWritesTopLevelPayloadAsColumns(t *testing.T) {
	connector := &tdConnector{}
	sink := &TDengineSink{
		cfg:           Config{ConnectorRef: "tdengine"},
		tableTemplate: "metrics",
		writeMode:     writeModeSingle,
		ctx:           tdRuntimeContext{connector: connector},
		tableColumns:  make(map[string]map[string]bool),
	}

	event := sdk.Event{
		Timestamp: time.Date(2026, 4, 14, 8, 0, 0, 0, time.UTC),
		Payload: map[string]interface{}{
			"ts":           "2026-04-14T01:37:43.187Z",
			"device_code":  "ZH_LDRHL_0001",
			"bath_tmp_set": 730.0,
		},
		Metadata: map[string]string{"table_suffix": "ignored"},
	}

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
	// Values are embedded directly (taosRestful driver doesn't support ? placeholders properly)
	if !strings.Contains(payload.SQL, "'ZH_LDRHL_0001'") {
		t.Fatalf("expected quoted string value in sql: %s", payload.SQL)
	}
	if !strings.Contains(payload.SQL, "730") {
		t.Fatalf("expected numeric value 730 in sql: %s", payload.SQL)
	}
	if len(payload.Args) != 0 {
		t.Fatalf("expected 0 args, got %d (%+v)", len(payload.Args), payload.Args)
	}
}

func TestTDengineSinkColumnMapping(t *testing.T) {
	connector := &tdConnector{}
	sink := &TDengineSink{
		cfg: Config{
			ConnectorRef: "tdengine",
			Output: struct {
				Table             string              `json:"table"`
				ChildTable        string              `json:"child_table,omitempty"`
				TagFields         []string            `json:"tag_fields,omitempty"`
				ColumnMapping     []ColumnMappingItem `json:"column_mapping,omitempty"`
				AutoCreateColumns bool                `json:"auto_create_columns,omitempty"`
				WriteMode         string              `json:"write_mode,omitempty"`
				Batch             BatchConfig         `json:"batch,omitempty"`
			}{
				ColumnMapping: []ColumnMappingItem{
					{Field: "time", Column: "ts"},
					{Field: "device_code", Column: "device_id"},
				},
			},
		},
		tableTemplate: "metrics",
		writeMode:     writeModeSingle,
		ctx:           tdRuntimeContext{connector: connector},
		tableColumns:  make(map[string]map[string]bool),
	}

	event := sdk.Event{
		Payload: map[string]interface{}{
			"time":        "2026-04-14T01:37:43.187Z",
			"device_code": "ZH_LDRHL_0001",
			"value":       730.0,
		},
	}

	if err := sink.OnEvent(event); err != nil {
		t.Fatalf("on event: %v", err)
	}

	var payload sqlWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal write payload: %v", err)
	}
	// Should contain mapped column names, not original
	if !strings.Contains(payload.SQL, "ts") {
		t.Fatalf("expected mapped column 'ts' in sql: %s", payload.SQL)
	}
	if !strings.Contains(payload.SQL, "device_id") {
		t.Fatalf("expected mapped column 'device_id' in sql: %s", payload.SQL)
	}
	if strings.Contains(payload.SQL, "\"time\"") || strings.Contains(payload.SQL, "time,") {
		t.Fatalf("original column 'time' should not appear after mapping: %s", payload.SQL)
	}
}

func TestTDengineSinkBatchModeFlushesWhenMaxRowsReached(t *testing.T) {
	connector := &tdConnector{}
	sink := &TDengineSink{
		cfg:           Config{ConnectorRef: "tdengine"},
		tableTemplate: "metrics",
		writeMode:     writeModeBatch,
		batchConfig: BatchConfig{
			MaxRows:         2,
			FlushIntervalMS: 60_000,
			MaxBytes:        512 * 1024,
		},
		ctx:          tdRuntimeContext{connector: connector},
		tableColumns: make(map[string]map[string]bool),
	}

	first := sdk.Event{Payload: map[string]interface{}{
		"ts":          "2026-04-14T01:37:43.187Z",
		"device_code": "dev-1",
		"value":       1.0,
	}}
	second := sdk.Event{Payload: map[string]interface{}{
		"ts":          "2026-04-14T01:37:44.187Z",
		"device_code": "dev-2",
		"value":       2.0,
	}}

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
	if !strings.Contains(payload.SQL, "'dev-1'") || !strings.Contains(payload.SQL, "'dev-2'") {
		t.Fatalf("expected quoted string values in sql: %s", payload.SQL)
	}
	if len(payload.Args) != 0 {
		t.Fatalf("expected 0 args, got %d (%+v)", len(payload.Args), payload.Args)
	}
}

func hasNonEmptyString(v interface{}) bool {
	text, ok := v.(string)
	return ok && strings.TrimSpace(text) != ""
}
