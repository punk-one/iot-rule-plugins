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
func TestAckOnlyForOriginalMajorityCommitReceipt(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"db","collection":"events","ack_after_commit":true}`)}); err != nil {
		t.Fatal(err)
	}
	out := &testEmitter{}
	failure := false
	wrong := false
	writes := 0
	p.Start(testContext{emitter: out, client: testClient{write: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		writes++
		if failure {
			return sdk.ConnectorResponse{}, errors.New("unknown")
		}
		var q struct {
			EventID string `json:"event_id"`
		}
		json.Unmarshal(r.Payload, &q)
		if wrong {
			q.EventID = "other"
		}
		raw, _ := json.Marshal(map[string]any{"committed": true, "event_id": q.EventID})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}}})
	e := sdk.NewEvent(map[string]any{"x": 1}, map[string]string{"buffer_record_id": "record", "buffer_node_id": "buffer"})
	if err := p.OnEvent(e); err != nil {
		t.Fatal(err)
	}
	if len(out.acks) != 1 || out.acks[0].EventID != e.ID || out.acks[0].RecordID != "record" {
		t.Fatal(out.acks)
	}
	wrong = true
	if p.OnEvent(e) == nil || len(out.acks) != 1 {
		t.Fatal("wrong receipt acked")
	}
	failure = true
	if p.OnEvent(e) == nil || len(out.acks) != 1 {
		t.Fatal("unknown outcome acked")
	}
	delete(e.Metadata, "buffer_node_id")
	before := writes
	if p.OnEvent(e) == nil || writes != before {
		t.Fatal("invalid IDs wrote")
	}
}
