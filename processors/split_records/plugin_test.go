package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestSplitIsolationStableIDsAndACKBoundary(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: []byte(`{}`)})
	e := sdk.NewEvent(map[string]any{"records": []any{map[string]any{"v": 1.0}, map[string]any{"v": 2.0}}}, nil)
	a, err := p.split(e)
	if err != nil || len(a) != 2 {
		t.Fatal(err)
	}
	b, _ := p.split(e)
	if a[0].ID != b[0].ID || a[0].ID == a[1].ID {
		t.Fatal("unstable parts")
	}
	a[0].Payload["record"].(map[string]any)["v"] = 99.0
	if e.Payload["records"].([]any)[0].(map[string]any)["v"] != 1.0 {
		t.Fatal("shared mutation")
	}
	e.Metadata = map[string]string{"buffer_record_id": "x"}
	if _, err = p.split(e); err == nil {
		t.Fatal("unsafe ACK fanout")
	}
}
