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

func (c testClient) Write(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) { return c.write(r) }

type testContext struct {
	sdk.RuntimeContext
	client sdk.ConnectorClient
}

func (c testContext) Connector() sdk.ConnectorClient { return c.client }
func TestCANCommandsUseFixedIDAndLocalReceipt(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"bus","device_code":"test","id":"0x123"}`)}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	scope := "local_socket"
	p.Start(testContext{client: testClient{write: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		calls++
		var request struct {
			Frame struct {
				ID   string `json:"id"`
				Data []byte `json:"data"`
			} `json:"frame"`
			Expires int64 `json:"expires_at"`
		}
		json.Unmarshal(r.Payload, &request)
		if request.Frame.ID != "0x123" || string(request.Frame.Data) != "\x01\x02" || request.Expires <= time.Now().UnixMilli() {
			t.Fatal(string(r.Payload))
		}
		raw, _ := json.Marshal(map[string]any{"queued": true, "scope": scope})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}}})
	e := sdk.NewEvent(map[string]any{"device_code": "test", "command_id": "one", "expires_at": time.Now().Add(time.Second).UnixMilli(), "id": "0x456", "data": map[string]any{"raw_base64": "AQI="}}, nil)
	if err := p.OnEvent(e); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || e.Payload["id"] != "0x456" {
		t.Fatal("mutated command")
	}
	scope = "device_completed"
	if p.OnEvent(e) == nil {
		t.Fatal("invented device completion accepted")
	}
}
func TestUnsafeCANCommandsNeverReachConnector(t *testing.T) {
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"bus","device_code":"test","id":"0x123"}`)})
	calls := 0
	p.ctx = testContext{client: testClient{write: func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		calls++
		return sdk.ConnectorResponse{}, nil
	}}}
	for _, modify := range []func(*sdk.Event){func(e *sdk.Event) { e.Payload["device_code"] = "other" }, func(e *sdk.Event) { e.Payload["expires_at"] = int64(1) }, func(e *sdk.Event) { delete(e.Payload, "command_id") }, func(e *sdk.Event) { e.Metadata = map[string]string{"buffer_record_id": "r"} }, func(e *sdk.Event) { e.Payload["data"] = map[string]any{"raw_base64": "AAAAAAAAAAAA"} }} {
		e := sdk.NewEvent(map[string]any{"device_code": "test", "command_id": "one", "expires_at": time.Now().Add(time.Second).UnixMilli(), "data": map[string]any{"raw_base64": "AQI="}}, nil)
		modify(&e)
		if p.OnEvent(e) == nil {
			t.Fatal("unsafe command accepted")
		}
	}
	if calls != 0 {
		t.Fatal("rejected command sent", calls)
	}
}
