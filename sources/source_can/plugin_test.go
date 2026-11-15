package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type testEmitter struct {
	sdk.Emitter
	publish func(sdk.Event) error
}

func (e testEmitter) Publish(v sdk.Event) error { return e.publish(v) }

type testContext struct {
	sdk.RuntimeContext
	emitter sdk.Emitter
}

func (c testContext) Health() sdk.HealthReporter { return nil }
func (c testContext) Emitter() sdk.Emitter       { return c.emitter }

type testStream struct {
	sdk.StreamConsumer
	m     sdk.StreamMessage
	acks  int
	order *[]string
}

func (s *testStream) Recv(time.Duration) (sdk.StreamMessage, bool, error) { return s.m, true, nil }
func (s *testStream) Ack(string, int) error {
	s.acks++
	*s.order = append(*s.order, "credit")
	return nil
}
func newPlugin(t *testing.T) *plugin {
	t.Helper()
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"bus","device_code":"test","ids":[{"id":"0x123"}]}`)}); err != nil {
		t.Fatal(err)
	}
	p.stop = make(chan struct{})
	return p
}
func message() sdk.StreamMessage {
	return sdk.StreamMessage{RawPayload: []byte{1, 2}, DeliveryID: "1", Metadata: map[string]string{"can_id": "0x123", "extended": "false", "fd": "false", "remote": "false", "bitrate_switch": "false", "error_state": "false", "bus_error": "false", "rtr_length": "0", "raw_dlc": "0", "rx_dropped_total": "0", "rx_dropped_delta": "0"}}
}
func TestCANFramePayloadAndQuality(t *testing.T) {
	p := newPlugin(t)
	m := message()
	e, err := p.event(m)
	if err != nil {
		t.Fatal(err)
	}
	data := e.Payload["data"].(map[string]any)
	if data["raw_base64"] != "AQI=" || data["can_id"] != "0x123" || e.Payload["quality"].(map[string]any)["frame"] != "good" {
		t.Fatal(e)
	}
	m.Metadata["rx_dropped_total"] = "5"
	m.Metadata["rx_dropped_delta"] = "5"
	e, err = p.event(m)
	if err != nil || e.Payload["quality"].(map[string]any)["frame"] != "uncertain:rx_overflow" {
		t.Fatal(e, err)
	}
	m.Metadata["bus_error"] = "true"
	m.RawPayload = make([]byte, 8)
	e, err = p.event(m)
	if err != nil || e.Payload["quality"].(map[string]any)["frame"] != "bad:can_bus_error" {
		t.Fatal(e, err)
	}
	e.Metadata["protocol"] = "changed"
	if m.Metadata["protocol"] != "" {
		t.Fatal("shared metadata")
	}
}
func TestCANMalformedFramesCannotBecomeGoodQuality(t *testing.T) {
	p := newPlugin(t)
	for _, modify := range []func(*sdk.StreamMessage){func(m *sdk.StreamMessage) { m.RawPayload = make([]byte, 9) }, func(m *sdk.StreamMessage) { m.Metadata["fd"] = "true"; m.RawPayload = make([]byte, 9) }, func(m *sdk.StreamMessage) { m.Metadata["remote"] = "true" }, func(m *sdk.StreamMessage) { m.Metadata["extended"] = "invalid" }, func(m *sdk.StreamMessage) { m.Metadata["can_id"] = "0x800" }, func(m *sdk.StreamMessage) { m.Metadata["rx_dropped_total"] = "-1" }} {
		m := message()
		modify(&m)
		if _, err := p.event(m); err == nil {
			t.Fatal("invalid CAN frame became good", m)
		}
	}
}
func TestCANReleasesCreditOnlyAfterHandoff(t *testing.T) {
	for _, failure := range []bool{false, true} {
		p := newPlugin(t)
		order := []string{}
		s := &testStream{m: message(), order: &order}
		p.ctx = testContext{emitter: testEmitter{publish: func(sdk.Event) error {
			order = append(order, "publish")
			if failure {
				return errors.New("full")
			}
			close(p.stop)
			return nil
		}}}
		err := p.consume(s)
		if failure {
			if err == nil || s.acks != 0 {
				t.Fatal("failed handoff credited")
			}
		} else if err != nil || len(order) != 2 || order[0] != "publish" || order[1] != "credit" {
			t.Fatal(order, err)
		}
	}
}
func TestCANIdentifierAliasesAreRejected(t *testing.T) {
	if (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"x","device_code":"d","ids":[{"id":"16"},{"id":"0x10"}]}`)}) == nil {
		t.Fatal("duplicate decimal/hex CAN ID accepted")
	}
}
