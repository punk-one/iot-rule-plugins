package main

import (
	"bytes"
	"encoding/json"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestPostgresPoolDiagnosticsDoNotCreateOrConnectPool(t *testing.T) {
	c := NewConnector().(*postgresConnector)
	defer c.Stop()
	handle, err := c.CreateResource(sdk.ConnectorResource{
		ID: "pg", Connection: mustJSON(t, map[string]any{
			"dsn":            "host=192.0.2.10 user=example password=diagnostic-secret dbname=telemetry",
			"max_open_conns": 3,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, err := c.getResource(handle)
	if err != nil {
		t.Fatal(err)
	}
	readPool := func() postgresPoolDiagnostics {
		t.Helper()
		reply, err := c.Execute(handle, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestMetadata, Target: "info"})
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(reply.Payload, []byte("diagnostic-secret")) {
			t.Fatal("pool diagnostics exposed credentials")
		}
		var data struct {
			Pool postgresPoolDiagnostics `json:"pool"`
		}
		if err := json.Unmarshal(reply.Payload, &data); err != nil {
			t.Fatal(err)
		}
		return data.Pool
	}
	if stats := readPool(); stats.Initialized || stats.MaxOpenConnections != 3 || resource.db != nil {
		t.Fatalf("metadata/info created a pool or reported the wrong limit: %+v", stats)
	}
	// database/sql initializes the local handle without opening a connection.
	if _, err := c.openDB(resource); err != nil {
		t.Fatal(err)
	}
	if stats := readPool(); !stats.Initialized || stats.MaxOpenConnections != 3 || stats.OpenConnections != 0 {
		t.Fatalf("metadata/info unexpectedly connected or misreported the live pool: %+v", stats)
	}
}
