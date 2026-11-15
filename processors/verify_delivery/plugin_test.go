package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"testing"
	"time"
)

func TestAuthenticatedIdentityStableAcrossMQTTReceives(t *testing.T) {
	secret := strings.Repeat("s", 32)
	payload, hash, _ := canonicalPayload(map[string]any{"value": 1.0})
	header := deliveryHeader{Version: 1, Origin: "edge", Rule: "r", Buffer: "b", Record: "record", Event: "event", KeyID: "v1", PayloadHash: hash, IssuedAt: time.Now().UnixMilli()}
	header.ID = identity(header)
	header.Signature = sign(secret, "delivery-v1", header)
	raw, _ := json.Marshal(envelope{Delivery: header, Payload: payload})
	first := sdk.NewEvent(nil, nil)
	json.Unmarshal(raw, &first.Payload)
	second := first.Clone()
	second.ID = "another-mqtt-arrival"
	p := &plugin{cfg: config{Origin: "edge", KeyID: "v1", Secret: secret, MaxAgeMS: 86400000}}
	a, err := p.verify(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.verify(second)
	if err != nil || a.ID != b.ID {
		t.Fatal("server replay identity changed")
	}
	first.Metadata = map[string]string{"buffer_record_id": "server-record"}
	if _, err = p.verify(first); err == nil {
		t.Fatal("changed identity after persistence")
	}
	second.Payload["payload"].(map[string]any)["value"] = 2.0
	if _, err = p.verify(second); err == nil {
		t.Fatal("tampered data acquired stable ID")
	}
}
