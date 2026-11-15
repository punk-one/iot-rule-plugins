package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func TestRawWriteTargetExpiryAndBufferBoundary(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"demo"}`)})
	e := sdk.NewEvent(map[string]any{"device_code": "demo", "expires_at": float64(time.Now().Add(time.Minute).UnixMilli()), "data": map[string]any{"raw_base64": "AQI="}}, nil)
	raw, _, err := p.payload(e)
	if err != nil || len(raw) != 2 {
		t.Fatal(err)
	}
	e.Metadata = map[string]string{"buffer_record_id": "x"}
	if _, _, err = p.payload(e); err == nil {
		t.Fatal("unsafe delivery goal accepted")
	}
	e.Metadata = nil
	e.Payload["expires_at"] = float64(1)
	if _, _, err = p.payload(e); err == nil {
		t.Fatal("stale command accepted")
	}
}
