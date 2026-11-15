package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestSourceRequiresExplicitFraming(t *testing.T) {
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo"}`)}) == nil {
		t.Fatal("missing framing")
	}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","frame":{"mode":"fixed","length":3}}`)}) != nil {
		t.Fatal("valid framing rejected")
	}
}
