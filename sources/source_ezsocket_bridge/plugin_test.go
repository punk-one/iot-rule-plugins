package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestBridgeBindingAndDeviceRequired(t *testing.T) {
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"http","device_code":"machine-001"}`)}) != nil {
		t.Fatal("valid bridge config rejected")
	}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"http"}`)}) == nil {
		t.Fatal("device identity missing")
	}
}
