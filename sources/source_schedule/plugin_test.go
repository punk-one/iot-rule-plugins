package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func TestCronUsesTimezoneAndIntervalIsExclusive(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"cron":"0 8 * * *","timezone":"Asia/Shanghai"}`)}); err != nil {
		t.Fatal(err)
	}
	next := p.next(time.Date(2026, 11, 14, 23, 0, 0, 0, time.UTC))
	if !next.Equal(time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("next=%s", next)
	}
	for _, cfg := range []string{`{"cron":"* * * * *","interval_ms":100}`, `{"cron":"0 0 0 * * *"}`, `{"cron":"* * * * *","timezone":"unknown"}`, `{}`} {
		if err := (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(cfg)}); err == nil {
			t.Fatalf("invalid schedule accepted: %s", cfg)
		}
	}
}
