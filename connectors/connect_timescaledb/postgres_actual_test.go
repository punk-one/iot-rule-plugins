package main

import (
	"encoding/json"
	"os"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestPostgresParameterRoundTrip(t *testing.T) {
	dsn := os.Getenv("IOT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set IOT_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
	}
	connector := NewConnector()
	t.Cleanup(func() { _ = connector.Stop() })
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "temporary-postgres", Enabled: true,
		Connection: mustJSON(t, map[string]any{"dsn": dsn, "max_open_conns": 1, "max_idle_conns": 1}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{
		`{"sql":"CREATE TEMP TABLE sample (id bigint PRIMARY KEY, reading numeric)"}`,
		`{"statements":[{"sql":"INSERT INTO sample VALUES ($1, $2)","args":[9007199254740993,1.234567890123456789]}]}`,
	} {
		if _, err := connector.Execute(handle, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:    sdk.ConnectorRequestRead,
		Payload: json.RawMessage(`{"sql":"SELECT id::text, reading::text FROM sample WHERE id=$1","args":[9007199254740993]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Rows []map[string]string `json:"rows"`
	}
	if err := json.Unmarshal(resp.Payload, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != "9007199254740993" || result.Rows[0]["reading"] != "1.234567890123456789" {
		t.Fatalf("database changed parameter precision: %s", resp.Payload)
	}
}
