package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestSQLiteTransactionsBoundsAndReadOnly(t *testing.T) {
	dir := t.TempDir()
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"directory": dir, "file": "test.db", "allow_write": true})
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "rw", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	execute := func(query string, params map[string]any, kind sdk.ConnectorRequestKind) (sdk.ConnectorResponse, error) {
		raw, _ := json.Marshal(map[string]any{"query": query, "params": params})
		return c.Execute("rw", sdk.ConnectorRequest{Kind: kind, Payload: raw})
	}
	if _, err := execute("CREATE TABLE events(id INTEGER PRIMARY KEY,value TEXT)", nil, sdk.ConnectorRequestWrite); err != nil {
		t.Fatal(err)
	}
	if _, err := execute("INSERT INTO events(id,value) VALUES(@id,@value)", map[string]any{"id": 1, "value": "synthetic"}, sdk.ConnectorRequestWrite); err != nil {
		t.Fatal(err)
	}
	response, err := execute("SELECT id,value FROM events", nil, sdk.ConnectorRequestRead)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Rows []map[string]any `json:"rows"`
	}
	if json.Unmarshal(response.Payload, &result) != nil || len(result.Rows) != 1 {
		t.Fatal("query failed")
	}
	raw, _ = json.Marshal(map[string]any{"directory": dir, "file": "test.db"})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "ro", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	statement, _ := json.Marshal(map[string]any{"query": "DELETE FROM events"})
	if _, err = c.Execute("ro", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: statement}); err == nil {
		t.Fatal("read-only SQLite file modified")
	}
}
