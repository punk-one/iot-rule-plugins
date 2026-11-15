package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func TestCountTimeOwnershipAndCapacity(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: []byte(`{"window_ms":100,"max_count":2,"max_keys":1}`)})
	now := time.Now()
	e := sdk.NewEvent(map[string]any{"device_code": "a", "data": map[string]any{"v": 1.0}}, nil)
	if out, err := p.add(e, now); err != nil || len(out) != 0 {
		t.Fatal(err)
	}
	e.Payload["data"].(map[string]any)["v"] = 2.0
	out, err := p.add(e, now)
	if err != nil || len(out) != 1 || p.bytes != 0 {
		t.Fatal("count flush failed")
	}
	if out[0].Payload["records"].([]any)[0].(map[string]any)["data"].(map[string]any)["v"] != 1.0 {
		t.Fatal("shared event")
	}
	p.add(e, now)
	out, err = p.add(e, now.Add(100*time.Millisecond))
	if err != nil || len(out) != 1 || len(p.groups["a"].events) != 1 {
		t.Fatal("time boundary crossed")
	}
	e.Payload["device_code"] = "b"
	if _, err = p.add(e, now); err == nil {
		t.Fatal("key limit ignored")
	}
	e.Metadata = map[string]string{"buffer_record_id": "x"}
	if _, err = p.add(e, now); err == nil {
		t.Fatal("unsafe input ACK")
	}
	p.Stop()
	if p.bytes != 0 {
		t.Fatal("retained after stop")
	}
}
