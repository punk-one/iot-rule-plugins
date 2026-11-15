package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func part(index int, value float64) sdk.Event {
	return sdk.NewEvent(map[string]any{"record": map[string]any{"v": value}, "parts": map[string]any{"id": "group", "index": float64(index), "count": 2.0}}, nil)
}
func TestJoinOutOfOrderDuplicateConflictAndTimeout(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: []byte(`{"timeout_ms":100,"emit_partial":true}`)})
	now := time.Now()
	e := part(1, 2)
	if out, err := p.add(e, now); err != nil || out != nil {
		t.Fatal(err)
	}
	e.Payload["record"].(map[string]any)["v"] = 99.0
	if _, err := p.add(part(1, 3), now); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	if out, err := p.add(part(1, 2), now); err != nil || out != nil {
		t.Fatal("same duplicate failed")
	}
	out, err := p.add(part(0, 1), now)
	if err != nil || out == nil {
		t.Fatal(err)
	}
	if out.Payload["records"].([]any)[1].(map[string]any)["v"] != 2.0 || p.bytes != 0 {
		t.Fatal("order, ownership or accounting")
	}
	if _, err = p.add(part(0, 1), now); err == nil {
		t.Fatal("closed group reopened")
	}
	p.expire(now.Add(100 * time.Millisecond))
	p.add(part(0, 1), now)
	expired := p.expire(now.Add(100 * time.Millisecond))
	if len(expired) != 1 || !expired[0].Payload["join"].(map[string]any)["partial"].(bool) {
		t.Fatal("partial timeout missing")
	}
	if p.bytes != 0 {
		t.Fatal("leaked byte accounting")
	}
}
