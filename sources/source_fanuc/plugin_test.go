package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestMacroPointTable(t *testing.T) {
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"macro","query":"macro"}]}`)}) == nil {
		t.Fatal("macro number omitted")
	}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"state","query":"status"},{"name":"macro","query":"macro","number":100}]}`)}); err != nil {
		t.Fatal(err)
	}
}
