package main

import (
	"encoding/json"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestOperationsApplyInOrderOnOwnedInputAndLiterals(t *testing.T) {
	p := &TransformMapPlugin{}
	raw := []byte(`{"process":{"operations":[
       {"op":"move","from":"data.temp","path":"data.temperature"},
       {"op":"delete","path":"debug"},
       {"op":"set","path":"data.config","value":{"counter":9007199254740993,"flag":null}}
    ]}}`)
	if err := p.Init(sdk.PluginConfig{Raw: raw}); err != nil {
		t.Fatal(err)
	}
	emit := &mapTestEmitter{}
	if err := p.Start(mapTestRuntimeContext{emitter: emit}); err != nil {
		t.Fatal(err)
	}
	input := sdk.NewEvent(map[string]any{"data": map[string]any{"temp": 23}, "debug": true}, map[string]string{"region": "A"})
	for i := 0; i < 2; i++ {
		if err := p.OnEvent(input); err != nil {
			t.Fatal(err)
		}
	}
	first := emit.events[0]
	data := first.Payload["data"].(map[string]any)
	cfg := data["config"].(map[string]any)
	if data["temperature"] != 23 || data["temp"] != nil || first.Payload["debug"] != nil || cfg["counter"] != json.Number("9007199254740993") {
		t.Fatalf("unexpected output: %v", first.Payload)
	}
	if value, ok := cfg["flag"]; !ok || value != nil {
		t.Fatalf("explicit null lost: %v", cfg)
	}
	cfg["counter"] = false
	first.Metadata["region"] = "changed"
	if emit.events[1].Payload["data"].(map[string]any)["config"].(map[string]any)["counter"] != json.Number("9007199254740993") ||
		input.Payload["data"].(map[string]any)["temp"] != 23 || input.Payload["debug"] != true || input.Metadata["region"] != "A" ||
		emit.events[1].Metadata["region"] != "A" {
		t.Fatal("edit mutated the input, literal, or another event")
	}
}

func TestOperationsValidateAndEditMappedResult(t *testing.T) {
	p := &TransformMapPlugin{}
	for _, invalid := range []string{
		`{"op":"move","from":"a","path":"a.b"}`,
		`{"op":"set","path":"a"}`,
		`{"op":"delete","path":"a..b"}`,
	} {
		cfg := sdk.PluginConfig{Raw: []byte(`{"process":{"operations":[` + invalid + `]}}`)}
		if err := p.Init(cfg); err == nil {
			t.Fatalf("accepted %s", invalid)
		}
	}
	cfg := sdk.PluginConfig{Raw: []byte(`{"process":{"template":{"temporary":"configured"},"operations":[{"op":"move","from":"temporary","path":"kept"},{"op":"delete","path":"missing.key"}]}}`)}
	if err := p.Init(cfg); err != nil {
		t.Fatal(err)
	}
	emit := &mapTestEmitter{}
	if err := p.Start(mapTestRuntimeContext{emitter: emit}); err != nil {
		t.Fatal(err)
	}
	if err := p.OnEvent(sdk.NewEvent(map[string]any{"unmapped": true}, nil)); err != nil {
		t.Fatal(err)
	}
	if out := emit.events[0].Payload; len(out) != 1 || out["kept"] != "configured" {
		t.Fatalf("output=%v", out)
	}
}
