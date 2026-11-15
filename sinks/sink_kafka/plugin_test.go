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
func TestKafkaExplicitBrokerReceiptAndStableKey(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"broker","topic":"telemetry","ack_broker_receipt":true}`)}); err != nil {
		t.Fatal(err)
	}
	out := &testEmitter{}
	reply := `{"received":true,"partition":0,"offset":"9007199254740993"}`
	failure := false
	writes := 0
	e := sdk.NewEvent(map[string]any{"x": 1}, map[string]string{"buffer_record_id": "record", "buffer_node_id": "buffer"})
	p.Start(testContext{emitter: out, client: testClient{write: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		writes++
		if failure {
			return sdk.ConnectorResponse{}, errors.New("unknown")
		}
		var q struct {
			Key   string `json:"key"`
			Value []byte `json:"value"`
		}
		json.Unmarshal(r.Payload, &q)
		if q.Key != e.ID || string(q.Value) != `{"x":1}` {
			t.Fatal(string(r.Payload))
		}
		return sdk.ConnectorResponse{Payload: json.RawMessage(reply)}, nil
	}}})
	if err := p.OnEvent(e); err != nil {
		t.Fatal(err)
	}
	if len(out.acks) != 1 || out.acks[0].EventID != e.ID {
		t.Fatal(out.acks)
	}
	reply = `{"received":true,"offset":"0"}`
	if p.OnEvent(e) == nil || len(out.acks) != 1 {
		t.Fatal("missing partition ACK")
	}
	failure = true
	if p.OnEvent(e) == nil || len(out.acks) != 1 {
		t.Fatal("unknown ACK")
	}
	delete(e.Metadata, "buffer_node_id")
	before := writes
	if p.OnEvent(e) == nil || writes != before {
		t.Fatal("missing identity wrote")
	}
}
func TestKafkaBufferedDataRequiresExplicitGoal(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"broker","topic":"telemetry"}`)})
	p.ctx = testContext{}
	if p.OnEvent(sdk.NewEvent(nil, map[string]string{"buffer_record_id": "record"})) == nil {
		t.Fatal("implicit buffer receipt goal")
	}
}
