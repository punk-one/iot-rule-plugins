package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func TestExpiryAndDuplicateMacroMapping(t *testing.T) {
	if _, err := expires(float64(1)); err == nil {
		t.Fatal("expired accepted")
	}
	if _, err := expires(float64(time.Now().Add(time.Minute).UnixMilli())); err != nil {
		t.Fatal(err)
	}
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo","points":[{"name":"a","number":100,"decimals":1},{"name":"b","number":100,"decimals":1}]}`)}) == nil {
		t.Fatal("duplicate macro address accepted")
	}
}
