package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type sourceEmitter struct {
	sdk.Emitter
	err    error
	events []sdk.Event
}

func (e *sourceEmitter) Publish(v sdk.Event) error { e.events = append(e.events, v); return e.err }

type sourceRuntime struct {
	sdk.RuntimeContext
	out sdk.Emitter
}

func (c sourceRuntime) Emitter() sdk.Emitter { return c.out }

type sourceStream struct {
	sdk.StreamConsumer
	m   sdk.StreamMessage
	ack func(string, int) error
}

func (s sourceStream) Recv(time.Duration) (sdk.StreamMessage, bool, error) { return s.m, true, nil }
func (s sourceStream) Ack(id string, c int) error                          { return s.ack(id, c) }
func TestSourceSubscriptionModesAndHandoff(t *testing.T) {
	for _, raw := range []string{`{"connector_ref":"r","device_code":"test","mode":"events"}`, `{"connector_ref":"r","device_code":"test","mode":"subscription","interval_ms":100,"points":[{"name":"temp","node_id":"s=temp","namespace_uri":"urn:test"}]}`} {
		if err := (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(raw)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{"connector_ref":"r","device_code":"test","mode":"subscription"}`, `{"connector_ref":"r","device_code":"test","mode":"unknown"}`, `{"connector_ref":"r","device_code":"test","mode":"events","event_node_id":"ns=invalid;i=1"}`} {
		if err := (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(raw)}); err == nil {
			t.Fatal("invalid mode accepted", raw)
		}
	}
	p := &plugin{cfg: config{Mode: "subscription", DeviceCode: "test", Points: []point{{Name: "temp"}}}, stop: make(chan struct{})}
	out := &sourceEmitter{err: errors.New("backpressure")}
	p.ctx = sourceRuntime{out: out}
	stream := sourceStream{m: sdk.StreamMessage{DeliveryID: "1", RawPayload: []byte(`{"values":{"temp":12.5},"quality":{"temp":"good"},"source_time":{"temp":1794614400000},"partial":true}`)}, ack: func(string, int) error { t.Fatal("ACK before handoff"); return nil }}
	if healthy, err := p.consumeSubscription(stream); err == nil || healthy {
		t.Fatal("failed handoff accepted")
	}
	out.err = nil
	stream.ack = func(string, int) error { close(p.stop); return nil }
	if healthy, err := p.consumeSubscription(stream); err != nil || !healthy {
		t.Fatal("successful handoff lost", err)
	}
	if out.events[0].Payload["partial"] != true {
		t.Fatal("partial notification misrepresented as snapshot")
	}
}
