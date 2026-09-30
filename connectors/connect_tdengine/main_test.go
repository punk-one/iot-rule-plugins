package main

import (
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestSQLRowLimitIsBounded(t *testing.T) {
	if got, err := normalizeSQLRowLimit(0); err != nil || got != defaultSQLMaxRows {
		t.Fatalf("default limit=%d err=%v", got, err)
	}
	if _, err := normalizeSQLRowLimit(maximumSQLMaxRows + 1); err == nil {
		t.Fatal("expected oversized max_rows to fail")
	}
}

func TestConnectorInfoContainsSchema(t *testing.T) {
	info := NewConnector().Info()
	if info.Type != sdk.PluginTypeConnector {
		t.Fatalf("expected connector type, got %s", info.Type)
	}
	if strings.TrimSpace(info.Capabilities.ConfigSchema) == "" {
		t.Fatal("expected config schema")
	}
}

func TestCreateResourceAndMetadataInfo(t *testing.T) {
	connector := NewConnector()
	resource := sdk.ConnectorResource{
		ID:       "td-1",
		PluginID: "connect-tdengine",
		Connection: mustJSON(t, map[string]any{
			"dsn": "root:taosdata@tcp(127.0.0.1:6030)/test",
		}),
		Enabled: true,
	}
	handle, err := connector.CreateResource(resource)
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}
	defer func() { _ = connector.DestroyResource(handle) }()

	resp, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestMetadata,
		Target: "info",
	})
	if err != nil {
		t.Fatalf("metadata info: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["plugin_id"] != "connect-tdengine" {
		t.Fatalf("unexpected plugin_id: %+v", payload)
	}
	if payload["resource_id"] != "td-1" {
		t.Fatalf("unexpected resource_id: %+v", payload)
	}
}

func TestCreateResourceValidatesDSN(t *testing.T) {
	connector := NewConnector()
	_, err := connector.CreateResource(sdk.ConnectorResource{
		ID:         "td-invalid",
		PluginID:   "connect-tdengine",
		Connection: mustJSON(t, map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "connection.dsn is required") {
		t.Fatalf("expected dsn validation error, got %v", err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
}
