package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestWebhookConfigRejectsSecretsAndBadLimits(t *testing.T) {
	p := &plugin{}
	for _, raw := range []string{`{}`, `{"route_key":"telemetry","secret_env":"IOT_RULE_ADMIN_PASSWORD"}`, `{"route_key":"../x","secret_env":"IOT_RULE_WEBHOOK_TEST"}`, `{"route_key":"telemetry","secret_env":"IOT_RULE_WEBHOOK_TEST","qps":101}`, `{"route_key":"telemetry","secret_env":"IOT_RULE_WEBHOOK_TEST","secret":"plaintext"}`} {
		if p.Init(sdk.PluginConfig{Raw: []byte(raw)}) == nil {
			t.Fatal(raw)
		}
	}
	if e := p.Init(sdk.PluginConfig{Raw: []byte(`{"route_key":"telemetry","secret_env":"IOT_RULE_WEBHOOK_TEST"}`)}); e != nil {
		t.Fatal(e)
	}
	if p.OnEvent(sdk.NewEvent(nil, nil)) == nil {
		t.Fatal("direct source input accepted")
	}
}
