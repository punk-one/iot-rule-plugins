package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

type testClient struct {
	sdk.ConnectorClient
	write func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c testClient) Write(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) { return c.write(r) }

type testEmitter struct {
	sdk.Emitter
	acks []sdk.AckMessage
}

func (e *testEmitter) PublishAck(a sdk.AckMessage) error { e.acks = append(e.acks, a); return nil }

type testContext struct {
	sdk.RuntimeContext
	client  sdk.ConnectorClient
	emitter sdk.Emitter
}

func (c testContext) Connector() sdk.ConnectorClient { return c.client }
func (c testContext) Emitter() sdk.Emitter           { return c.emitter }
func TestStableFilenameAndExplicitRemoteReceipt(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"files","ack_remote_receipt":true}`)}); err != nil {
		t.Fatal(err)
	}
	out := &testEmitter{}
	writes := []string{}
	reply := `{"received":true,"synced":true,"duplicate":false}`
	failure := false
	p.Start(testContext{emitter: out, client: testClient{write: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		if failure {
			return sdk.ConnectorResponse{}, errors.New("unknown")
		}
		var q struct {
			Path    string `json:"path"`
			Data    []byte `json:"data"`
			Require bool   `json:"require_sync"`
		}
		json.Unmarshal(r.Payload, &q)
		if !q.Require || r.Target != "put" || len(q.Data) == 0 {
			t.Fatal(string(r.Payload))
		}
		writes = append(writes, q.Path)
		return sdk.ConnectorResponse{Payload: json.RawMessage(reply)}, nil
	}}})
	e := sdk.NewEvent(map[string]any{"x": 1}, map[string]string{"buffer_record_id": "record", "buffer_node_id": "buffer"})
	for i := 0; i < 2; i++ {
		if err := p.OnEvent(e); err != nil {
			t.Fatal(err)
		}
	}
	if len(writes) != 2 || writes[0] != writes[1] || len(out.acks) != 2 || out.acks[0].EventID != e.ID || out.acks[0].RecordID != "record" {
		t.Fatal(writes, out.acks)
	}
	reply = `{"received":true,"synced":false}`
	if p.OnEvent(e) == nil || len(out.acks) != 2 {
		t.Fatal("acked unsynced")
	}
	failure = true
	if p.OnEvent(e) == nil || len(out.acks) != 2 {
		t.Fatal("acked unknown")
	}
}
func TestBufferedInputRequiresExplicitReceipt(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"files"}`)})
	p.ctx = testContext{}
	if p.OnEvent(sdk.NewEvent(nil, map[string]string{"buffer_record_id": "r"})) == nil {
		t.Fatal("implicit ACK policy")
	}
	if (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"files","ack_remote_receipt":true,"require_sync":false}`)}) == nil {
		t.Fatal("ACK without sync allowed")
	}
}
