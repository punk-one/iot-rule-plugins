package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type testClient struct {
	sdk.ConnectorClient
	write func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c testClient) BatchWrite(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return c.write(r)
}

type testContext struct {
	sdk.RuntimeContext
	client sdk.ConnectorClient
}

func (c testContext) Connector() sdk.ConnectorClient { return c.client }
func TestFixedAddressAndPropertyReceipt(t *testing.T) {
	p := &plugin{}
	if e := p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"hvac","device_code":"demo","points":[{"name":"setpoint","object_type":2,"instance":1,"property":85,"valueType":"Float32","priority":16}]}`)}); e != nil {
		t.Fatal(e)
	}
	calls := 0
	physical := false
	p.Start(testContext{client: testClient{write: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		calls++
		var b struct {
			Points  []point
			Expires int64 `json:"expires_at"`
		}
		if json.Unmarshal(r.Payload, &b) != nil || len(b.Points) != 1 || b.Points[0].Instance != 1 || b.Points[0].Priority != 16 || b.Points[0].Value != 23.5 || b.Expires <= time.Now().UnixMilli() {
			t.Fatal(string(r.Payload))
		}
		raw, _ := json.Marshal(map[string]any{"written": 1, "scope": "property_ack", "physical_completion": physical})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}}})
	event := sdk.NewEvent(map[string]any{"device_code": "demo", "command_id": "one", "expires_at": time.Now().Add(time.Second).UnixMilli(), "instance": 999, "data": map[string]any{"setpoint": 23.5}}, nil)
	if e := p.OnEvent(event); e != nil || calls != 1 {
		t.Fatal(e, calls)
	}
	physical = true
	if p.OnEvent(event) == nil {
		t.Fatal("false physical completion accepted")
	}
	event.Metadata["buffer_record_id"] = "r"
	if p.OnEvent(event) == nil || calls != 2 {
		t.Fatal("buffered physical command sent")
	}
	delete(event.Metadata, "buffer_record_id")
	event.Payload["expires_at"] = 1
	if p.OnEvent(event) == nil || calls != 2 {
		t.Fatal("expired command sent")
	}
	event.Payload["expires_at"] = time.Now().Add(time.Hour).UnixMilli()
	if p.OnEvent(event) == nil || calls != 2 {
		t.Fatal("unbounded future command sent")
	}
}
