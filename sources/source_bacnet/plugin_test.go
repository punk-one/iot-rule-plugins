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

func (c testClient) BatchRead(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	return c.read(r)
}

type testEmitter struct {
	sdk.Emitter
	publish func(sdk.Event) error
}

func (e testEmitter) Publish(v sdk.Event) error { return e.publish(v) }

type testMetrics struct{ sdk.Metrics }

func (testMetrics) Observe(string, float64, map[string]string) {}

type testContext struct {
	sdk.RuntimeContext
	client  sdk.ConnectorClient
	emitter sdk.Emitter
}

func (c testContext) Connector() sdk.ConnectorClient { return c.client }
func (c testContext) Emitter() sdk.Emitter           { return c.emitter }
func (c testContext) Metrics() sdk.Metrics           { return testMetrics{} }
func TestBACnetSourceQualityAndCanonicalOutput(t *testing.T) {
	p := &plugin{}
	if e := p.Init(sdk.PluginConfig{Raw: []byte(`{"connector_ref":"hvac","device_code":"demo","points":[{"name":"temperature","object_type":2,"instance":1,"property":85,"valueType":"Float32","max":22.74,"precision":1,"outOfRangePolicy":"default","defaultValue":-1},{"name":"failed","object_type":2,"instance":99,"property":85,"valueType":"Float32"},{"name":"name","object_type":8,"instance":1,"property":77,"valueType":"String"}]}`)}); e != nil {
		t.Fatal(e)
	}
	var events []sdk.Event
	p.ctx = testContext{client: testClient{read: func(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		var body struct{ Points []map[string]any }
		if json.Unmarshal(req.Payload, &body) != nil || len(body.Points) != 3 || body.Points[0]["object_type"] != float64(2) {
			t.Fatal(string(req.Payload))
		}
		return sdk.ConnectorResponse{Payload: []byte(`{"values":{"temperature":22.75,"failed":0,"name":"demo unit"},"quality":{"temperature":"good","failed":"bad:device_2_32","name":"good"}}`)}, nil
	}}, emitter: testEmitter{publish: func(e sdk.Event) error { events = append(events, e); return nil }}}
	if e := p.poll(); e != nil {
		t.Fatal(e)
	}
	e := events[0]
	data := e.Payload["data"].(map[string]any)
	quality := e.Payload["quality"].(map[string]any)
	if e.Payload["device_code"] != "demo" || data["temperature"] != json.Number("-1") || quality["temperature"] != "bad:range" || data["failed"] != nil || quality["failed"] != "bad:device_2_32" || data["name"] != "demo unit" {
		t.Fatal(e)
	}
	if data["temperature"] == 22.7 {
		t.Fatal("rounding before range")
	}
}
func TestBACnetSourceNetworkFailureCannotFabricateZero(t *testing.T) {
	p := &plugin{cfg: config{DeviceCode: "demo", Points: []point{{Name: "value", ValueType: "Float32"}}}}
	var got sdk.Event
	p.ctx = testContext{client: testClient{read: func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		return sdk.ConnectorResponse{}, errors.New("offline")
	}}, emitter: testEmitter{publish: func(e sdk.Event) error { got = e; return nil }}}
	if p.poll() == nil {
		t.Fatal("network failure hidden")
	}
	if got.Payload["data"].(map[string]any)["value"] != nil || got.Payload["quality"].(map[string]any)["value"] != "bad:read" {
		t.Fatal(got)
	}
}
func TestBACnetSourceAddressAndPolicyValidation(t *testing.T) {
	p := &plugin{}
	for _, raw := range []string{`{"connector_ref":"r","device_code":"d","points":[{"name":"x","object_type":1024,"instance":0,"property":85,"valueType":"Float32"}]}`, `{"connector_ref":"r","device_code":"d","points":[{"name":"x","object_type":2,"instance":1,"property":85,"valueType":"String","scale":2}]}`, `{"connector_ref":"r","device_code":"d","points":[{"name":"x","object_type":2,"instance":1,"property":85,"valueType":"Float32"}]} {}`} {
		if p.Init(sdk.PluginConfig{Raw: []byte(raw)}) == nil {
			t.Fatal("invalid config accepted", raw)
		}
	}
}
