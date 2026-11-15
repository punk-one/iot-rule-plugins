package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"testing"
	"time"
)

type emitter struct {
	sdk.Emitter
	acks []sdk.AckMessage
}

func (e *emitter) PublishAck(ack sdk.AckMessage) error { e.acks = append(e.acks, ack); return nil }

type runtime struct {
	sdk.RuntimeContext
	e *emitter
}

func (runtime) RuleID() string         { return "edge-rule" }
func (r runtime) Emitter() sdk.Emitter { return r.e }
func TestReceiptScopeAuthenticationAndNoAckOnForgery(t *testing.T) {
	secret := strings.Repeat("s", 32)
	out := &emitter{}
	p := &plugin{cfg: config{Origin: "edge", KeyID: "v1", Secret: secret, Committer: "server", Buffer: "buffer", MaxAgeMS: 86400000}, ctx: runtime{e: out}}
	h := deliveryHeader{Version: 1, Origin: "edge", Rule: "edge-rule", Buffer: "buffer", Record: "record", Event: "event", KeyID: "v1", PayloadHash: strings.Repeat("a", 64), IssuedAt: time.Now().UnixMilli()}
	h.ID = identity(h)
	h.Signature = sign(secret, "delivery-v1", h)
	signed := receipt{Delivery: h, Committer: "server", CommittedAt: time.Now().UnixMilli(), Status: "committed"}
	signed.Signature = sign(secret, "commit-receipt-v1", signed)
	event := sdk.NewEvent(nil, nil)
	raw, _ := json.Marshal(signed)
	json.Unmarshal(raw, &event.Payload)
	if err := p.OnEvent(event); err != nil || len(out.acks) != 1 || out.acks[0].EventID != "event" {
		t.Fatal(err)
	}
	event.Payload["signature"] = strings.Repeat("0", 64)
	if p.OnEvent(event) == nil || len(out.acks) != 1 {
		t.Fatal("forged ACK applied")
	}
	signed.Signature = ""
	signed.Delivery.Rule = "another-rule"
	signed.Delivery.ID = identity(signed.Delivery)
	signed.Delivery.Signature = ""
	signed.Delivery.Signature = sign(secret, "delivery-v1", signed.Delivery)
	signed.Signature = sign(secret, "commit-receipt-v1", signed)
	raw, _ = json.Marshal(signed)
	json.Unmarshal(raw, &event.Payload)
	if p.OnEvent(event) == nil || len(out.acks) != 1 {
		t.Fatal("cross-rule receipt applied")
	}
}
