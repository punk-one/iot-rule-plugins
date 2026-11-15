package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestInterrogationIntervalBound(t *testing.T) {
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","interval_ms":100,"points":[{"name":"a","ioa":1}]}`)}) == nil {
		t.Fatal("high frequency GI accepted")
	}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"a","ioa":1}]}`)}) != nil {
		t.Fatal("valid points rejected")
	}
}

func TestDuplicateAndInvalidPointsRejected(t *testing.T) {
	p := &plugin{}
	for _, raw := range []string{`{"connector_ref":"r","device_code":"demo","points":[{"name":"a","ioa":16777216}]}`, `{"connector_ref":"r","device_code":"demo","points":[{"name":"a","ioa":1},{"name":"b","ioa":1}]}`} {
		if p.Init(sdk.PluginConfig{Raw: []byte(raw)}) == nil {
			t.Fatal("invalid point table accepted")
		}
	}
}
