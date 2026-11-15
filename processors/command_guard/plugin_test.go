package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type runtime struct{ sdk.RuntimeContext }

func (runtime) RuleID() string { return "rule" }
func (runtime) NodeID() string { return "guard" }
func TestCommandTargetExpiryAndPersistentClaim(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"device_code": "demo", "allowed_operations": []string{"write"}, "state_dir": t.TempDir()})
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: raw}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(runtime{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	event := sdk.NewEvent(map[string]any{"command_id": "id", "device_code": "demo", "operation": "write", "issued_at": now, "expires_at": now + 10000, "arguments": map[string]any{"value": 1}}, nil)
	cmd, reason := p.validate(event, now)
	if reason != "" {
		t.Fatal(reason)
	}
	if reason = p.claim(cmd, now); reason != "" {
		t.Fatal(reason)
	}
	p.Stop()
	p = &plugin{}
	p.Init(sdk.PluginConfig{Raw: raw})
	p.Start(runtime{})
	defer p.Stop()
	if reason = p.claim(cmd, now); reason != "duplicate_command" {
		t.Fatal("claim not persisted")
	}
	event.Payload["device_code"] = "wrong"
	if _, reason = p.validate(event, now); reason != "target_mismatch" {
		t.Fatal(reason)
	}
	event.Payload["device_code"] = "demo"
	event.Payload["expires_at"] = now - 1
	if _, reason = p.validate(event, now); reason != "expired_or_excessive_ttl" {
		t.Fatal(reason)
	}
}
