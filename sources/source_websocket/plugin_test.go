package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestSourceBounds(t *testing.T) {
	for _, raw := range []string{`{"connector_ref":"r","device_code":"demo","buffer_messages":65}`, `{"connector_ref":"r"}`} {
		if (&plugin{}).Init(sdk.PluginConfig{Raw: []byte(raw)}) == nil {
			t.Fatal("invalid config accepted")
		}
	}
	if (&plugin{}).Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo"}`)}) != nil {
		t.Fatal("valid config rejected")
	}
}
