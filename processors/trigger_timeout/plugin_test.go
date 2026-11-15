package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func TestLatestOnlyCloneDeadlineCapacity(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"delay_ms":100,"max_keys":1}`)}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	e := sdk.NewEvent(map[string]any{"device_code": "a", "data": map[string]any{"v": 1.0}}, nil)
	if err := p.store(e, now); err != nil {
		t.Fatal(err)
	}
	e.Payload["data"].(map[string]any)["v"] = 2.0
	if err := p.store(e, now.Add(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	e.Payload["data"].(map[string]any)["v"] = 3.0
	if len(p.take(now.Add(100*time.Millisecond))) != 0 {
		t.Fatal("not debounced")
	}
	out := p.take(now.Add(150 * time.Millisecond))
	if len(out) != 1 || out[0].Payload["data"].(map[string]any)["v"] != 2.0 || p.bytes != 0 {
		t.Fatal("latest clone or accounting failed")
	}
	if err := p.store(e, now); err != nil {
		t.Fatal(err)
	}
	e.Payload["device_code"] = "b"
	if p.store(e, now) == nil {
		t.Fatal("unbounded keys")
	}
	p.Stop()
	if p.store(e, now) == nil {
		t.Fatal("accepted after stop")
	}
}
