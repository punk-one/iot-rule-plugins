package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestFINSConfigBindingAndAddress(t *testing.T) {
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"value","area":"DM","address":0,"valueType":"Uint16"}]}`)}) != nil {
		t.Fatal("valid config failed")
	}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"value","area":"DM","address":65536,"valueType":"Uint16"}]}`)}) == nil {
		t.Fatal("invalid address")
	}
}
