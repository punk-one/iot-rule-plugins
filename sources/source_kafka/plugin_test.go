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

func (c testContext) Emitter() sdk.Emitter { return c.emitter }

type testStream struct {
	sdk.StreamConsumer
	message sdk.StreamMessage
	acks    int
	order   *[]string
}

func (s *testStream) Recv(time.Duration) (sdk.StreamMessage, bool, error) {
	return s.message, true, nil
}
func (s *testStream) Ack(id string, credits int) error {
	s.acks++
	*s.order = append(*s.order, "commit")
	if id != s.message.DeliveryID || credits != 1 {
		return errors.New("wrong ack")
	}
	return nil
}
func newPlugin(t *testing.T) *plugin {
	t.Helper()
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"broker","device_code":"demo","topic":"telemetry","group_id":"iot-rule.edge-a"}`)}); err != nil {
		t.Fatal(err)
	}
	p.stop = make(chan struct{})
	return p
}
func message() sdk.StreamMessage {
	return sdk.StreamMessage{Topic: "telemetry", DeliveryID: "telemetry:0:3", RawPayload: []byte(`{"wide":9007199254740993}`), Metadata: map[string]string{"partition": "0", "offset": "3"}}
}
func TestKafkaStableIdentityAndLargeIntegers(t *testing.T) {
	p := newPlugin(t)
	a, err := p.event(message())
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.event(message())
	if err != nil || a.ID != b.ID {
		t.Fatal(a, b, err)
	}
	if a.Payload["data"].(map[string]any)["wide"] != "9007199254740993" {
		t.Fatal(a.Payload)
	}
	other := message()
	other.Metadata["offset"] = "4"
	c, err := p.event(other)
	if err != nil || a.ID == c.ID {
		t.Fatal("offset identity collision")
	}
	p.cfg.GroupID = "iot-rule.other"
	d, _ := p.event(message())
	if d.ID == a.ID {
		t.Fatal("different projection identity collision")
	}
	a.Metadata["partition"] = "changed"
	if message().Metadata["partition"] != "0" {
		t.Fatal("shared metadata")
	}
}
func TestKafkaCommitsOnlyAfterSuccessfulHandoff(t *testing.T) {
	for _, failure := range []bool{false, true} {
		p := newPlugin(t)
		order := []string{}
		s := &testStream{message: message(), order: &order}
		p.ctx = testContext{emitter: testEmitter{publish: func(sdk.Event) error {
			order = append(order, "publish")
			if failure {
				return errors.New("handoff failed")
			}
			close(p.stop)
			return nil
		}}}
		err := p.consume(s)
		if failure {
			if err == nil || s.acks != 0 {
				t.Fatal("failed handoff committed")
			}
		} else if err != nil || len(order) != 2 || order[0] != "publish" || order[1] != "commit" || s.acks != 1 {
			t.Fatal(order, err)
		}
	}
}
func TestKafkaPoisonBodyDoesNotCommit(t *testing.T) {
	p := newPlugin(t)
	order := []string{}
	bad := message()
	bad.RawPayload = []byte("invalid")
	s := &testStream{message: bad, order: &order}
	if p.consume(s) == nil || s.acks != 0 {
		t.Fatal("poison committed")
	}
	for _, raw := range []string{`{"connector_ref":"x","device_code":"d","topic":"t"}`, `{"connector_ref":"x","device_code":"d","topic":"t","group_id":"g","reset":"unknown"}`} {
		if (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(raw)}) == nil {
			t.Fatal(raw)
		}
	}
}
