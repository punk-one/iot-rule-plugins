package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type clientStub struct {
	sdk.ConnectorClient
	calls int
}

func (c *clientStub) BatchWrite(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.calls++
	return sdk.ConnectorResponse{}, nil
}

type runtimeStub struct {
	sdk.RuntimeContext
	client *clientStub
}

func (r runtimeStub) Connector() sdk.ConnectorClient { return r.client }
func TestSinkRequiresExactDeviceAndCompleteValues(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"x","device_code":"d","points":[{"name":"x","area":"D","address":0,"valueType":"Uint16"}]}`)}); err != nil {
		t.Fatal(err)
	}
	c := &clientStub{}
	p.Start(runtimeStub{client: c})
	for _, payload := range []map[string]any{{"device_code": "wrong", "data": map[string]any{"x": 1}}, {"device_code": "d", "data": map[string]any{}}} {
		if err := p.OnEvent(sdk.NewEvent(payload, nil)); err == nil {
			t.Fatal("invalid write accepted")
		}
	}
	if c.calls != 0 {
		t.Fatal("invalid write reached connector")
	}
	if err := p.OnEvent(sdk.NewEvent(map[string]any{"device_code": "d", "data": map[string]any{"x": 1}}, nil)); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 {
		t.Fatal("write missing")
	}
}

func TestExpiredWriteIsRejectedBeforeConnector(t *testing.T) {
	if _, err := expiry(time.Now().UnixMilli() - 1); err == nil {
		t.Fatal("expired command accepted")
	}
	if _, err := expiry(time.Now().UnixMilli() + 1000); err != nil {
		t.Fatal(err)
	}
}
