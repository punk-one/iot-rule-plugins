package main

import (
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type testStream struct {
	sdk.StreamConsumer
	message sdk.StreamMessage
	acks    int
	close   func()
}

func (s *testStream) Recv(time.Duration) (sdk.StreamMessage, bool, error) {
	return s.message, true, nil
}
func (s *testStream) Ack(string, int) error {
	s.acks++
	if s.close != nil {
		s.close()
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
func TestRawSourceStreamHandoffAndQuality(t *testing.T) {
	for _, failed := range []bool{false, true} {
		p := &plugin{cfg: config{DeviceCode: "test"}, stop: make(chan struct{})}
		s := &testStream{message: sdk.StreamMessage{RawPayload: []byte{0xff, 0}, PublishedAtUnixNano: time.Now().UnixNano(), DeliveryID: "1", Sequence: 1, Metadata: map[string]string{"delivery": "best_effort"}}, close: func() { close(p.stop) }}
		p.ctx = testContext{emitter: testEmitter{publish: func(e sdk.Event) error {
			values := e.Payload["data"].(map[string]any)
			if values["raw_base64"] != "/wA=" || values["raw"] != nil || e.Payload["quality"].(map[string]any)["frame"] != "good" {
				t.Fatal(e)
			}
			if failed {
				return errors.New("full")
			}
			return nil
		}}}
		err := p.consume(s)
		if failed && (err == nil || s.acks != 0) || !failed && (err != nil || s.acks != 1) {
			t.Fatal(err, s.acks)
		}
	}
}
func TestStreamSourceCannotRepeatPollingRequest(t *testing.T) {
	p := &plugin{}
	if p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"d","mode":"stream","frame":{"mode":"fixed","length":1},"request_base64":"AQ=="}`)}) == nil {
		t.Fatal("Source startup command accepted for shared stream")
	}
	if e := p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"r","device_code":"d","mode":"stream","frame":{"mode":"fixed","length":1}}`)}); e != nil {
		t.Fatal(e)
	}
}
