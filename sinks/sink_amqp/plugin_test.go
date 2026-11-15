package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

type clientFixture struct {
	sdk.ConnectorClient
	write func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c clientFixture) Write(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return c.write(r)
}

type emitterFixture struct {
	sdk.Emitter
	acks []sdk.AckMessage
}

func (e *emitterFixture) PublishAck(a sdk.AckMessage) error { e.acks = append(e.acks, a); return nil }

type runtimeFixture struct {
	sdk.RuntimeContext
	client sdk.ConnectorClient
	out    sdk.Emitter
}

func (r runtimeFixture) Connector() sdk.ConnectorClient { return r.client }
func (r runtimeFixture) Emitter() sdk.Emitter           { return r.out }
func (r runtimeFixture) RuleID() string                 { return "rule" }
func (r runtimeFixture) NodeID() string                 { return "amqp-out" }
func TestNoBufferAckWithoutExplicitGoalAndBrokerConfirmation(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"r","exchange":"events","routing_key":"telemetry"}`)}); err != nil {
		t.Fatal(err)
	}
	out := &emitterFixture{}
	calls := 0
	result := `{}`
	failure := false
	p.ctx = runtimeFixture{out: out, client: clientFixture{write: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		calls++
		if failure {
			return sdk.ConnectorResponse{}, errors.New("lost confirmation")
		}
		var spec struct {
			MessageID string `json:"message_id"`
			Body      []byte `json:"body"`
		}
		if err := json.Unmarshal(r.Payload, &spec); err != nil {
			t.Fatal(err)
		}
		if spec.MessageID != "original-event" {
			t.Fatal("identity changed")
		}
		return sdk.ConnectorResponse{Payload: json.RawMessage(result)}, nil
	}}}
	e := sdk.Event{ID: "original-event", Payload: map[string]any{"value": 1}, Metadata: map[string]string{"buffer_record_id": "record", "buffer_node_id": "buffer"}}
	if err := p.OnEvent(e); err == nil || calls != 0 {
		t.Fatal("buffered input accepted without explicit receipt goal")
	}
	p.cfg.AckBrokerReceipt = true
	if err := p.OnEvent(e); err == nil || len(out.acks) != 0 {
		t.Fatal("ACK without confirmation")
	}
	failure = true
	if err := p.OnEvent(e); err == nil || len(out.acks) != 0 {
		t.Fatal("ACK after unknown publish outcome")
	}
	failure = false
	result = `{"broker_confirmed":true}`
	if err := p.OnEvent(e); err != nil {
		t.Fatal(err)
	}
	if len(out.acks) != 1 || out.acks[0].Status != sdk.AckStatusSuccess || out.acks[0].EventID != e.ID || out.acks[0].FromNodeID != "amqp-out" {
		t.Fatalf("ACK=%+v", out.acks)
	}
}
