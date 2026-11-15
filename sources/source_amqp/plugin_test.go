package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type emitter struct {
	sdk.Emitter
	events []sdk.Event
	err    error
}

func (e *emitter) Publish(v sdk.Event) error { e.events = append(e.events, v); return e.err }

type runtimeContext struct {
	sdk.RuntimeContext
	out sdk.Emitter
}

func (r runtimeContext) Emitter() sdk.Emitter { return r.out }

type consumerFixture struct {
	sdk.StreamConsumer
	message sdk.StreamMessage
	ack     func(string, int) error
}

func (s consumerFixture) Recv(time.Duration) (sdk.StreamMessage, bool, error) {
	return s.message, true, nil
}
func (s consumerFixture) Ack(id string, credits int) error { return s.ack(id, credits) }
func TestHandoffFailureDoesNotAcknowledge(t *testing.T) {
	p := &plugin{cfg: config{Decode: "json", DeviceCode: "test", Queue: "q"}, stop: make(chan struct{})}
	out := &emitter{err: errors.New("backpressure")}
	p.ctx = runtimeContext{out: out}
	s := consumerFixture{message: sdk.StreamMessage{DeliveryID: "1", RawPayload: []byte(`{"counter":9007199254740993}`)}, ack: func(string, int) error { t.Fatal("ACK before successful handoff"); return nil }}
	if err := p.consume(s); err == nil {
		t.Fatal("handoff failure ignored")
	}
	if out.events[0].Payload["data"].(map[string]any)["counter"] != "9007199254740993" {
		t.Fatal("large integer rounded")
	}
	out.err = nil
	s.ack = func(id string, credits int) error {
		if id != "1" || credits != 1 {
			t.Fatal("invalid ACK")
		}
		close(p.stop)
		return nil
	}
	if err := p.consume(s); err != nil {
		t.Fatal(err)
	}
}
func TestStableIdentityRequiresProducerMessageID(t *testing.T) {
	p := &plugin{cfg: config{Decode: "json", ConnectorRef: "broker", Queue: "q"}}
	m := sdk.StreamMessage{RawPayload: []byte(`{}`), Metadata: map[string]string{"message_id": "producer-01"}}
	a, _ := p.event(m)
	b, _ := p.event(m)
	if a.ID != b.ID {
		t.Fatal("message ID not stable")
	}
	delete(m.Metadata, "message_id")
	a, _ = p.event(m)
	b, _ = p.event(m)
	if a.ID == b.ID {
		t.Fatal("body hash collapsed distinct unidentified messages")
	}
}
func TestPoisonJSONFailsWithoutInventedData(t *testing.T) {
	p := &plugin{cfg: config{Decode: "json"}}
	for _, raw := range []string{`{} {}`, `{"x":`, string(make([]byte, 1048577))} {
		if _, err := p.event(sdk.StreamMessage{RawPayload: []byte(raw)}); err == nil {
			t.Fatal("malformed body accepted")
		}
	}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"r","device_code":"test","queue":"q","decode":"xml"}`)}); err == nil {
		t.Fatal("invalid decoding accepted")
	}
}
