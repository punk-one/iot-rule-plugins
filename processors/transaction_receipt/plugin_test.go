package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"testing"
	"time"
)

type client struct {
	sdk.ConnectorClient
	committed bool
	writes    int
}

func (c *client) Write(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.writes++
	raw, _ := json.Marshal(map[string]bool{"committed": c.committed})
	return sdk.ConnectorResponse{Payload: raw}, nil
}

type emitter struct {
	sdk.Emitter
	events []sdk.Event
}

func (e *emitter) Publish(event sdk.Event) error { e.events = append(e.events, event); return nil }

type runtime struct {
	sdk.RuntimeContext
	c *client
	e *emitter
}

func (r runtime) Connector() sdk.ConnectorClient { return r.c }
func (r runtime) Emitter() sdk.Emitter           { return r.e }
func TestReceiptOnlyAfterVerifiedPayloadAndConfirmedCommit(t *testing.T) {
	secret := strings.Repeat("s", 32)
	payload, hash, _ := canonicalPayload(map[string]any{"value": 1.0})
	h := deliveryHeader{Version: 1, Origin: "edge", Rule: "r", Buffer: "b", Record: "record", Event: "event", KeyID: "v1", PayloadHash: hash, IssuedAt: time.Now().UnixMilli()}
	h.ID = identity(h)
	h.Signature = sign(secret, "delivery-v1", h)
	raw, _ := json.Marshal(envelope{Delivery: h, Payload: payload})
	event := sdk.NewEvent(nil, nil)
	json.Unmarshal(raw, &event.Payload)
	client := &client{}
	out := &emitter{}
	p := &plugin{cfg: config{Origin: "edge", KeyID: "v1", Secret: secret, Committer: "server", Statement: "INSERT", TimeoutMS: 1000, MaxAgeMS: 86400000}, ctx: runtime{c: client, e: out}}
	if p.OnEvent(event) == nil || len(out.events) != 0 {
		t.Fatal("uncommitted receipt emitted")
	}
	event.Payload["event_id"] = "server-local-transport-id"
	client.committed = true
	if err := p.OnEvent(event); err != nil || len(out.events) != 1 {
		t.Fatal(err)
	}
	var receipt receipt
	decodeObject(out.events[0].Payload, &receipt)
	if verifyReceipt(receipt, secret, "v1", "edge", "server", 86400000) != nil {
		t.Fatal("invalid committed receipt")
	}
	event.Payload["payload"].(map[string]any)["value"] = 2.0
	before := client.writes
	if p.OnEvent(event) == nil || client.writes != before {
		t.Fatal("tampered payload reached SQL")
	}
}
