package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

type testClient struct {
	sdk.ConnectorClient
	read func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c testClient) Read(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) { return c.read(r) }

type testEmitter struct {
	sdk.Emitter
	events []sdk.Event
}

func (e *testEmitter) Publish(v sdk.Event) error { e.events = append(e.events, v); return nil }

type testContext struct {
	sdk.RuntimeContext
	client  sdk.ConnectorClient
	emitter sdk.Emitter
}

func (c testContext) Connector() sdk.ConnectorClient { return c.client }
func (c testContext) Emitter() sdk.Emitter           { return c.emitter }
func TestScalarEqualityAndEventOwnership(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"db","collection":"events","filter":{"enabled":true},"bindings":{"device":"payload.device"}}`)}); err != nil {
		t.Fatal(err)
	}
	out := &testEmitter{}
	calls := 0
	p.Start(testContext{emitter: out, client: testClient{read: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		calls++
		var q map[string]any
		json.Unmarshal(r.Payload, &q)
		and := q["filter"].(map[string]any)["$and"].([]any)
		if and[1].(map[string]any)["device"].(map[string]any)["$eq"] != "demo" {
			t.Fatal(q)
		}
		return sdk.ConnectorResponse{Payload: json.RawMessage(`{"documents":[{"value":{"$numberLong":"9007199254740993"}}],"count":1}`)}, nil
	}}})
	e := sdk.NewEvent(map[string]any{"device": "demo"}, map[string]string{"tag": "input"})
	if err := p.OnEvent(e); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(out.events) != 1 || out.events[0].ID != e.ID || e.Payload["mongodb"] != nil {
		t.Fatal(e, out.events)
	}
	out.events[0].Metadata["tag"] = "changed"
	if e.Metadata["tag"] != "input" {
		t.Fatal("shared metadata")
	}
	e.Payload["device"] = map[string]any{"$ne": nil}
	if p.OnEvent(e) == nil || calls != 1 {
		t.Fatal("operator injection reached connector")
	}
}
func TestRejectMalformedQueryResponses(t *testing.T) {
	for _, raw := range []string{`{"documents":null,"count":0}`, `{"documents":[{}],"count":0}`, `{"documents":[{},{}],"count":2}`, `{} {}`} {
		if _, err := decodeResult([]byte(raw), 1); err == nil {
			t.Fatal(raw)
		}
	}
	if scalar(int64(9007199254740993)) || scalar(uint64(18446744073709551615)) {
		t.Fatal("unsafe numeric binding")
	}
}
