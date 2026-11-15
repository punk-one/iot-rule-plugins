package main

import (
	"encoding/json"
	"errors"
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
func TestSnapshotPreservesCanonicalLargeIntegerAndStop(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"db","collection":"events","device_code":"demo"}`)}); err != nil {
		t.Fatal(err)
	}
	out := &testEmitter{}
	reply := `{"documents":[{"n":{"$numberLong":"9007199254740993"}}],"count":1}`
	p.ctx = testContext{emitter: out, client: testClient{read: func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		return sdk.ConnectorResponse{Payload: json.RawMessage(reply)}, nil
	}}}
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	docs := out.events[0].Payload["data"].(map[string]any)["documents"].([]map[string]any)
	if docs[0]["n"].(map[string]any)["$numberLong"] != "9007199254740993" {
		t.Fatal(docs)
	}
	reply = `{"documents":[{}],"count":0}`
	if p.poll() == nil {
		t.Fatal("partial invalid snapshot accepted")
	}
	reply = `{"documents":[],"count":0}`
	out.err = errors.New("handoff failed")
	if p.poll() == nil {
		t.Fatal("handoff error lost")
	}
	p.stop = make(chan struct{})
	close(p.stop)
	out.err = nil
	before := len(out.events)
	if err := p.poll(); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != before {
		t.Fatal("late result emitted")
	}
	(&plugin{}).Stop()
}
