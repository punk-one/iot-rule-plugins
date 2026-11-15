package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func TestNotificationClaimQuotaAndAcceptedReplay(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","to":["operator@example.invalid"],"max_per_minute":1}`)}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := p.claim("a", now); err != nil {
		t.Fatal(err)
	}
	if _, err := p.claim("a", now); err == nil {
		t.Fatal("inflight duplicate accepted")
	}
	if _, err := p.claim("b", now); err == nil {
		t.Fatal("quota ignored")
	}
	r := p.records["a"]
	r.accepted = true
	p.records["a"] = r
	if accepted, err := p.claim("a", now); err != nil || !accepted {
		t.Fatal("accepted replay cannot ACK without resend")
	}
	p.Stop()
	if _, err := p.claim("c", now.Add(time.Minute)); err == nil {
		t.Fatal("attempt after stop")
	}
}
