package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestDuplicateNodeNamesRejected(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"x","device_code":"d","points":[{"name":"x","node_id":"ns=2;s=x"},{"name":"x","node_id":"ns=2;s=y"}]}`)}); err == nil {
		t.Fatal("duplicate name accepted")
	}
}
