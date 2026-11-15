package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type testStream struct {
	sdk.StreamConsumer
	message sdk.StreamMessage
	acks    int
	stop    func()
}

func (s *testStream) Recv(time.Duration) (sdk.StreamMessage, bool, error) {
	return s.message, true, nil
}
func (s *testStream) Ack(string, int) error {
	s.acks++
	if s.stop != nil {
		s.stop()
	}
	return nil
}

type testEmitter struct {
	sdk.Emitter
	publish func(sdk.Event) error
}

func (e testEmitter) Publish(v sdk.Event) error { return e.publish(v) }

type testContext struct {
	sdk.RuntimeContext
	emitter sdk.Emitter
}

func (c testContext) Emitter() sdk.Emitter       { return c.emitter }
func (c testContext) Health() sdk.HealthReporter { return nil }
func TestIEC104ContinuousDeltasAndHandoff(t *testing.T) {
	for _, fail := range []bool{false, true} {
		p := &plugin{cfg: config{DeviceCode: "station", Points: []point{{Name: "value", IOA: 100}, {Name: "unchanged", IOA: 101}}}, stop: make(chan struct{})}
		raw, _ := json.Marshal(map[string]any{"values": map[string]any{"value": 22.75}, "quality": map[string]any{"value": "good"}, "details": map[string]any{}, "cause": 3, "received_at": time.Now().UnixMilli()})
		s := &testStream{message: sdk.StreamMessage{RawPayload: raw, Metadata: map[string]string{"delivery": "best_effort"}, DeliveryID: "1", Sequence: 1}, stop: func() { close(p.stop) }}
		p.ctx = testContext{emitter: testEmitter{publish: func(e sdk.Event) error {
			data := e.Payload["data"].(map[string]any)
			if data["value"] != 22.75 || len(data) != 1 || e.Payload["protocol_details"].(map[string]any)["snapshot"] != false {
				t.Fatal(e)
			}
			if fail {
				return errors.New("handoff full")
			}
			return nil
		}}}
		err := p.consume(s)
		if fail && (err == nil || s.acks != 0) || !fail && (err != nil || s.acks != 1) {
			t.Fatal(err, s.acks)
		}
	}
}
func TestIEC104MalformedStreamCannotInventGoodQuality(t *testing.T) {
	p := &plugin{cfg: config{Points: []point{{Name: "value", IOA: 100}}}}
	for _, raw := range []string{`{}`, `{"values":{"other":1},"quality":{"other":"good"},"received_at":1}`, `{"values":{"value":1},"quality":{},"received_at":1}`, `{"values":{"value":1},"quality":{"value":0},"received_at":1}`} {
		if _, err := p.event(sdk.StreamMessage{RawPayload: []byte(raw), Metadata: map[string]string{"delivery": "best_effort"}}); err == nil {
			t.Fatal(raw)
		}
	}
}
