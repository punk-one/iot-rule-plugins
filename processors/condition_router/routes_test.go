package main

import (
	"encoding/json"
	"reflect"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type routeCapture struct {
	sdk.Emitter
	labels []string
	events []sdk.Event
}

func (c *routeCapture) EmitTo(label string, event sdk.Event) error {
	c.labels = append(c.labels, label)
	// Simulate metadata mutation by the published SDK EmitTo and a downstream
	// consumer. Adjacent branches and the input must be independent.
	event.Metadata["route_label"] = label
	event.Payload["nested"].(map[string]any)["owner"] = label
	c.events = append(c.events, event)
	return nil
}

type routeContext struct {
	sdk.RuntimeContext
	target *routeCapture
}

func (r routeContext) Emitter() sdk.Emitter { return r.target }

func TestOrderedRoutesModesTypesAndOwnership(t *testing.T) {
	setup := func(match string) (*plugin, *routeCapture) {
		t.Helper()
		cfg := sdk.PluginConfig{Raw: []byte(`{"match":"` + match + `","default_label":"none","routes":[
          {"label":"threshold","conditions":[{"path":"value","op":"gt","value":9007199254740992}]},
          {"label":"same","conditions":[{"path":"value","op":"eq","value":9007199254740993}]}
        ]}`)}
		p := new(plugin)
		if err := p.Init(cfg); err != nil {
			t.Fatal(err)
		}
		capture := new(routeCapture)
		if err := p.Start(routeContext{target: capture}); err != nil {
			t.Fatal(err)
		}
		return p, capture
	}
	input := sdk.NewEvent(map[string]any{"value": json.Number("9007199254740993"), "nested": map[string]any{}}, map[string]string{"region": "west"})
	for _, test := range []struct {
		mode   string
		labels []string
	}{
		{"first", []string{"threshold"}},
		{"all", []string{"threshold", "same"}},
	} {
		p, capture := setup(test.mode)
		if err := p.OnEvent(input); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(capture.labels, test.labels) {
			t.Fatalf("mode %s: %v", test.mode, capture.labels)
		}
		for i, event := range capture.events {
			if event.Payload["nested"].(map[string]any)["owner"] != test.labels[i] || event.Metadata["route_label"] != test.labels[i] {
				t.Fatalf("branch shares mutable state: %#v", event)
			}
		}
		if input.Payload["nested"].(map[string]any)["owner"] != nil || input.Metadata["route_label"] != "" {
			t.Fatal("input was mutated")
		}
	}
	p, capture := setup("all")
	buffered := input.Clone()
	buffered.Metadata["buffer_record_id"] = "record-1"
	if err := p.OnEvent(buffered); err == nil || len(capture.events) > 0 {
		t.Fatal("buffered fanout must fail before emitting")
	}
	stringInput := input.Clone()
	stringInput.Payload["value"] = "9007199254740993"
	if err := p.OnEvent(stringInput); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(capture.labels, []string{"none"}) {
		t.Fatalf("string was coerced to a number: %v", capture.labels)
	}
}

func TestDuplicateRouteLabelsRejected(t *testing.T) {
	check := `{"label":"a","conditions":[{"path":"x","op":"exists"}]}`
	cfg := sdk.PluginConfig{Raw: []byte(`{"routes":[` + check + "," + check + `]}`)}
	if err := new(plugin).Init(cfg); err == nil {
		t.Fatal("ambiguous labels allowed")
	}
}
