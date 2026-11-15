package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"testing"
	"time"
)

func TestDebugQuotaAndRedaction(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: []byte(`{"sample_every":1,"max_per_second":2,"include_values":true,"fields":["password","data","value"]}`)})
	e := sdk.NewEvent(map[string]any{"password": "super-secret-value", "data": map[string]any{"token": "nested-secret"}, "value": 12.5}, nil)
	raw, _ := json.Marshal(p.summary(e))
	if strings.Contains(string(raw), "super-secret-value") || strings.Contains(string(raw), "nested-secret") {
		t.Fatal("secret leaked")
	}
	now := time.Now()
	if !p.allow(now) || !p.allow(now) || p.allow(now) {
		t.Fatal("quota ignored")
	}
	if !p.allow(now.Add(time.Second)) {
		t.Fatal("quota not renewed")
	}
	p.Stop()
	if p.allow(now.Add(2 * time.Second)) {
		t.Fatal("log after stop")
	}
}
