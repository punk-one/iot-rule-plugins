package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

type client struct {
	sdk.ConnectorClient
	status int
}

func (c client) Write(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	raw, _ := json.Marshal(map[string]int{"status": c.status})
	return sdk.ConnectorResponse{Payload: raw}, nil
}

type emitter struct {
	sdk.Emitter
	acks int
}

func (e *emitter) PublishAck(sdk.AckMessage) error { e.acks++; return nil }

type runtime struct {
	sdk.RuntimeContext
	c client
	e *emitter
}

func (r runtime) Connector() sdk.ConnectorClient { return r.c }
func (r runtime) Emitter() sdk.Emitter           { return r.e }
func TestHTTPReceiptAcknowledgementIsExplicitAndAfterSuccess(t *testing.T) {
	for _, tc := range []struct {
		status int
		ack    bool
		want   int
	}{{200, false, 0}, {500, true, 0}, {200, true, 1}} {
		p := &plugin{cfg: config{Method: "POST", Path: "/events", TimeoutMS: 100, AckOnSuccess: tc.ack}}
		em := &emitter{}
		p.Start(runtime{c: client{status: tc.status}, e: em})
		event := sdk.NewEvent(map[string]any{"value": 1}, map[string]string{"buffer_record_id": "r", "buffer_node_id": "b"})
		err := p.OnEvent(event)
		if tc.status == 500 && err == nil {
			t.Fatal("failed HTTP acknowledged")
		}
		if em.acks != tc.want {
			t.Fatalf("acks=%d want=%d", em.acks, tc.want)
		}
	}
}
