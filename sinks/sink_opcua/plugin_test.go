package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func TestDuplicateNodeNamesRejected(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"x","device_code":"d","points":[{"name":"x","node_id":"ns=2;s=x","valueType":"Uint16"},{"name":"x","node_id":"ns=2;s=y","valueType":"Uint16"}]}`)}); err == nil {
		t.Fatal("duplicate name accepted")
	}
}

func TestExpiredWriteIsRejectedBeforeConnector(t *testing.T) {
	if _, err := expiry(time.Now().UnixMilli() - 1); err == nil {
		t.Fatal("expired command accepted")
	}
	if _, err := expiry(time.Now().UnixMilli() + 1000); err != nil {
		t.Fatal(err)
	}
}
