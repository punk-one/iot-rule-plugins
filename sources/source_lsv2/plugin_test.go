package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestRequiredQueries(t *testing.T) {
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo"}`)}) == nil {
		t.Fatal("empty query set")
	}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"version","query":"version"}]}`)}) != nil {
		t.Fatal("valid query rejected")
	}
}

func TestDuplicateAndInvalidPointsRejected(t *testing.T) {
	p := &plugin{}
	for _, raw := range []string{`{"connector_ref":"r","device_code":"demo","points":[{"name":"a","query":"unknown"}]}`, `{"connector_ref":"r","device_code":"demo","points":[{"name":"a","query":"version"},{"name":"a","query":"version"}]}`} {
		if p.Init(sdk.PluginConfig{Raw: []byte(raw)}) == nil {
			t.Fatal("invalid point table accepted")
		}
	}
}
