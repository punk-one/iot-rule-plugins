package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"os"
	"testing"
)

func TestExternalTelemetryExampleExcludesInternalQuality(t *testing.T) {
	raw, err := os.ReadFile("../../examples/development/telemetry-three-fields.transform-map.json")
	if err != nil {
		t.Fatal(err)
	}
	p := &TransformMapPlugin{}
	if err = p.Init(sdk.PluginConfig{Raw: json.RawMessage(raw)}); err != nil {
		t.Fatal(err)
	}
	out := &mapTestEmitter{}
	if err = p.Start(mapTestRuntimeContext{emitter: out}); err != nil {
		t.Fatal(err)
	}
	in := sdk.NewEvent(map[string]any{"device_code": "machine-001", "data": map[string]any{"temperature": 0}, "quality": map[string]any{"temperature": "bad:range_default"}, "time": int64(1794614400000)}, nil)
	if err = p.OnEvent(in); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 1 || len(out.events[0].Payload) != 3 || out.events[0].Payload["quality"] != nil || in.Payload["quality"] == nil {
		t.Fatal(out.events)
	}
	out.events[0].Payload["data"].(map[string]any)["temperature"] = 99
	if in.Payload["data"].(map[string]any)["temperature"] != 0 {
		t.Fatal("mapping aliases input")
	}
}
