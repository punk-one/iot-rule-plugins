package main

import (
	"errors"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestDeadbandBaselinePreservesUnsentDriftAndRetry(t *testing.T) {
	for _, mode := range []string{baselinePreviousSample, baselineLastChange} {
		t.Run(mode, func(t *testing.T) {
			p := new(ChangeDetectPlugin)
			raw := []byte(`{"state":{"key_by":["device"]},"process":{"detect":{"type":"delta","field":"value","value":5,"baseline":"` + mode + `"}},"output":{"emit":"on_change"}}`)
			if err := p.Init(sdk.PluginConfig{Raw: raw}); err != nil {
				t.Fatal(err)
			}
			emit := new(testEmitter)
			ctx := &testRuntimeContext{logger: &testLogger{}, emitter: emit, metrics: &testMetrics{}, health: &testHealthReporter{}, state: newTestStateManager()}
			if err := p.Start(ctx); err != nil {
				t.Fatal(err)
			}
			event := func(value any) sdk.Event {
				return sdk.NewEvent(map[string]any{"device": "demo", "value": value}, map[string]string{"region": "west"})
			}
			for _, sample := range []any{20, 22, "invalid", 24} {
				if err := p.OnEvent(event(sample)); err != nil {
					t.Fatal(err)
				}
			}
			if len(emit.events) != 0 {
				t.Fatal("sub-threshold or invalid reading emitted")
			}
			crossing := event(25)
			if mode == baselineLastChange {
				ctx.emitter = &publishErrorEmitter{err: errors.New("temporary unavailable")}
				if err := p.OnEvent(crossing); err == nil {
					t.Fatal("failed publication was hidden")
				}
				ctx.emitter = emit
			}
			if err := p.OnEvent(crossing); err != nil {
				t.Fatal(err)
			}
			if mode == baselinePreviousSample && len(emit.events) != 0 {
				t.Fatal("legacy comparison changed")
			}
			if mode == baselineLastChange {
				if len(emit.events) != 1 || emit.events[0].Payload["_change_value"] != float64(5) {
					t.Fatalf("missing accumulated drift or retry result: %v", emit.events)
				}
				emit.events[0].Metadata["region"] = "changed"
				if crossing.Metadata["region"] != "west" {
					t.Fatal("output aliases input")
				}
			}
		})
	}
}
