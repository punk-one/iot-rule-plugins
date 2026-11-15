package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type testClient struct {
	sdk.ConnectorClient
	read func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c testClient) Read(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) { return c.read(r) }

type testEmitter struct {
	sdk.Emitter
	events []sdk.Event
	err    error
}

func (e *testEmitter) Publish(v sdk.Event) error { e.events = append(e.events, v); return e.err }

type testContext struct {
	sdk.RuntimeContext
	client  sdk.ConnectorClient
	emitter sdk.Emitter
}

func (c testContext) Connector() sdk.ConnectorClient { return c.client }
func (c testContext) Emitter() sdk.Emitter           { return c.emitter }
func TestSnapshotPreservesTextAndParameters(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"db","device_code":"test","query":"from(bucket: params.bucket)","params":{"bucket":"demo"}}`)}); err != nil {
		t.Fatal(err)
	}
	emitter := &testEmitter{}
	p.ctx = testContext{emitter: emitter, client: testClient{read: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		if r.Target != "query" || r.TimeoutMS != 5000 {
			t.Fatalf("request=%+v", r)
		}
		var q map[string]any
		if err := json.Unmarshal(r.Payload, &q); err != nil {
			t.Fatal(err)
		}
		if q["params"].(map[string]any)["bucket"] != "demo" {
			t.Fatalf("params=%v", q)
		}
		return sdk.ConnectorResponse{Payload: json.RawMessage(`{"status":200,"rows":[{"_value":"18446744073709551615","_time":"2026-11-14T00:00:00Z"}],"row_count":1}`)}, nil
	}}}
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	rows := emitter.events[0].Payload["data"].(map[string]any)["rows"].([]map[string]any)
	if rows[0]["_value"] != "18446744073709551615" {
		t.Fatal(rows)
	}
	emitter.err = errors.New("handoff failed")
	if err := p.poll(); err == nil {
		t.Fatal("handoff failure lost")
	}
}
func TestRejectMalformedAndTruncatedResults(t *testing.T) {
	for _, raw := range []string{`{"status":500,"rows":[],"row_count":0}`, `{"status":200,"rows":[{}],"row_count":0}`, `{"status":200,"rows":[{"x":9007199254740993}],"row_count":1}`, `{"status":200,"rows":null,"row_count":0}`, `{"status":200,"rows":[{},{}],"row_count":2}`, `{} {}`} {
		if _, err := decodeRows([]byte(raw), 1); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
func TestConfigRejectsUnsafeNumbersAndContainers(t *testing.T) {
	for _, params := range []string{`{"x":9007199254740993}`, `{"x":[]}`, `{"x":{}}`} {
		raw := `{"connector_ref":"db","device_code":"test","query":"q","params":` + params + `}`
		if err := (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(raw)}); err == nil {
			t.Fatal(raw)
		}
	}
}
func TestStopPreventsLateEmission(t *testing.T) {
	p := &plugin{stop: make(chan struct{})}
	close(p.stop)
	out := &testEmitter{}
	p.cfg.MaxRows = 1
	p.ctx = testContext{emitter: out, client: testClient{read: func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		return sdk.ConnectorResponse{Payload: json.RawMessage(`{"status":200,"rows":[],"row_count":0}`)}, nil
	}}}
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 0 {
		t.Fatal("emitted after stop")
	}
	// Stop is idempotent for a source that was never started.
	q := &plugin{}
	done := make(chan struct{})
	go func() { q.Stop(); q.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop blocked")
	}
}
