package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestTagConfigAndBinding(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"value","tag":"Demo","valueType":"Int32"}]}`)}); err != nil {
		t.Fatal(err)
	}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"value","valueType":"Int32"}]}`)}) == nil {
		t.Fatal("missing tag")
	}
}
