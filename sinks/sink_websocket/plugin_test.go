package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestSinkRequiresBinding(t *testing.T) {
	if (&plugin{}).Init(sdk.PluginConfig{Raw: []byte(`{}`)}) == nil {
		t.Fatal("binding omitted")
	}
	if (&plugin{}).Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","timeout_ms":49}`)}) == nil {
		t.Fatal("unbounded timeout")
	}
}
