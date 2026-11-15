package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestManualDefaultDisabledAndConfigBounds(t *testing.T) {
	p := &plugin{}
	if e := p.Init(sdk.PluginConfig{Raw: []byte(`{}`)}); e != nil || p.cfg.AllowLive {
		t.Fatal("manual must default disabled", e)
	}
	if e := p.Init(sdk.PluginConfig{Raw: []byte(`{"allow_live_trigger":true}`)}); e != nil || !p.cfg.AllowLive {
		t.Fatal(e)
	}
	for _, raw := range []string{`{"qps":11}`, `{"burst":21}`, `{"unknown":true}`, `{} {}`} {
		if p.Init(sdk.PluginConfig{Raw: []byte(raw)}) == nil {
			t.Fatal(raw)
		}
	}
	if p.OnEvent(sdk.NewEvent(nil, nil)) == nil {
		t.Fatal("direct source input accepted")
	}
}
