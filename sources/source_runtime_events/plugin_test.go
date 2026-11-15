package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestRuntimeSourceConfigRequiresExplicitBoundedSelection(t *testing.T) {
	p := &plugin{}
	for _, raw := range []string{`{}`, `{"watch_nodes":["work","work"]}`, `{"watch_nodes":["work"],"kinds":["all"]}`, `{"watch_nodes":["work"],"max_events_per_second":101}`, `{"watch_nodes":["work"],"burst":201}`, `{"watch_nodes":["work"],"include_messages":true}`} {
		if p.Init(sdk.PluginConfig{Raw: []byte(raw)}) == nil {
			t.Fatal(raw)
		}
	}
	if e := p.Init(sdk.PluginConfig{Raw: []byte(`{"watch_nodes":["work"],"kinds":["node_error","node_done"]}`)}); e != nil {
		t.Fatal(e)
	}
	if p.OnEvent(sdk.NewEvent(nil, nil)) == nil {
		t.Fatal("direct business event accepted")
	}
}
