package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"testing"
)

type runtime struct{ sdk.RuntimeContext }

func (runtime) RuleID() string { return "edge-rule" }
func TestStableEnvelopeAndTamper(t *testing.T) {
	p := &plugin{cfg: config{Origin: "edge-1", KeyID: "v1", Secret: strings.Repeat("x", 32)}, ctx: runtime{}}
	e := sdk.NewEvent(map[string]any{"value": 1.0}, map[string]string{"buffer_record_id": "record", "buffer_node_id": "buffer"})
	first, err := p.prepare(e)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := p.prepare(e)
	if first.Delivery.ID != second.Delivery.ID || first.Delivery.Signature != second.Delivery.Signature {
		t.Fatal("replay envelope changed")
	}
	if err = verifyEnvelope(first, p.cfg.Secret, "v1", "edge-1", 86400000); err != nil {
		t.Fatal(err)
	}
	first.Payload["value"] = 2.0
	if verifyEnvelope(first, p.cfg.Secret, "v1", "edge-1", 86400000) == nil {
		t.Fatal("payload tamper accepted")
	}
	if e.Payload["value"] != 1.0 {
		t.Fatal("caller mutated")
	}
	delete(e.Metadata, "buffer_record_id")
	if _, err = p.prepare(e); err == nil {
		t.Fatal("unpersisted delivery accepted")
	}
}
