package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestEscapingMissingPathsAndBounds(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: []byte(`{"template":"Hi {{payload.name}}","escape":"html"}`)}); err != nil {
		t.Fatal(err)
	}
	e := sdk.NewEvent(map[string]any{"name": "<script>"}, nil)
	text, err := p.render(e)
	if err != nil || text != "Hi &lt;script&gt;" {
		t.Fatal(text, err)
	}
	p.cfg.Escape = "json"
	text, err = p.render(e)
	if err != nil || text != `Hi "\u003cscript\u003e"` {
		t.Fatal(text, err)
	}
	p.cfg.MaxBytes = 3
	if _, err = p.render(e); err == nil {
		t.Fatal("unbounded output")
	}
	p.cfg.MaxBytes = 100
	delete(e.Payload, "name")
	if _, err = p.render(e); err == nil {
		t.Fatal("missing path accepted")
	}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"template":"{{range x}}"}`)}) == nil {
		t.Fatal("unsafe control syntax")
	}
}
