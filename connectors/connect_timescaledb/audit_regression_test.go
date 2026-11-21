package main

import (
	"database/sql/driver"
	"encoding/json"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestRetiredResourceCannotLazilyOpenPool(t *testing.T) {
	c := NewConnector().(*timescaledbConnector)
	config := sdk.ConnectorResource{ID: "db", Connection: mustJSON(t, map[string]any{"dsn": "host=127.0.0.1 user=test dbname=test"})}
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

func TestSQLParametersPreserveNumericText(t *testing.T) {
	var write sqlWritePayload
	if err := json.Unmarshal([]byte(`{"args":[9007199254740993],"statements":[{"args":[1.234567890123456789]}]}`), &write); err != nil {
		t.Fatal(err)
	}
	for _, sample := range []struct {
		arg  any
		want string
	}{
		{write.Args[0], "9007199254740993"},
		{write.Statements[0].Args[0], "1.234567890123456789"},
	} {
		value, err := driver.DefaultParameterConverter.ConvertValue(sample.arg)
		if value != sample.want || err != nil {
			t.Fatalf("binding changed precision: got %v (%T), want %s, err=%v", value, value, sample.want, err)
		}
	}
}
