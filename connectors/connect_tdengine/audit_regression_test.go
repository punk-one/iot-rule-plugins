package main

import (
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestRetiredResourceCannotLazilyOpenPool(t *testing.T) {
	c := NewConnector().(*tdengineConnector)
	config := sdk.ConnectorResource{ID: "db", Connection: mustJSON(t, map[string]any{"dsn": "test:test@http(127.0.0.1:6041)/example"})}
	defer c.Stop()
	handle, err := c.CreateResource(config)
	if err != nil {
		t.Fatal(err)
	}
	old, err := c.getResource(handle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateResource(config); err != nil {
		t.Fatal(err)
	}
	if db, err := c.openDB(old); db != nil || err == nil {
		t.Fatal("previous generation opened a pool")
	}
	current, err := c.getResource(handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DestroyResource(handle); err != nil {
		t.Fatal(err)
	}
	if db, err := c.openDB(current); db != nil || err == nil {
		t.Fatal("removed generation opened a pool")
	}
}
