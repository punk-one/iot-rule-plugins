package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

type emitter struct {
	sdk.Emitter
	labels []string
}

func (e *emitter) EmitTo(label string, event sdk.Event) error {
	e.labels = append(e.labels, label)
	event.Payload["changed"] = true
	return nil
}

type runtime struct {
	sdk.RuntimeContext
	emit *emitter
}

func (r runtime) Emitter() sdk.Emitter { return r.emit }
func TestRoutingAndOwnership(t *testing.T) {
	p := &plugin{cfg: config{Mode: "all", Conditions: []condition{{Path: "data.x", Op: "gt", Value: float64(10)}}}}
	e := &emitter{}
	p.Start(runtime{emit: e})
	input := sdk.NewEvent(map[string]any{"data": map[string]any{"x": float64(11)}}, nil)
	if err := p.OnEvent(input); err != nil {
		t.Fatal(err)
	}
	if e.labels[0] != "matched" {
		t.Fatal("wrong route")
	}
	if _, exists := input.Payload["changed"]; exists {
		t.Fatal("input mutated")
	}
	input.Payload["data"] = map[string]any{"x": "11"}
	p.OnEvent(input)
	if e.labels[1] != "otherwise" {
		t.Fatal("string was numerically coerced")
	}
}
