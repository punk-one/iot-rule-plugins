package main

import (
	"context"
	"encoding/json"
	"github.com/gopcua/opcua/ua"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync/atomic"
	"testing"
)

type fakeSession struct{ closed atomic.Int32 }

func (f *fakeSession) Close(context.Context) error { f.closed.Add(1); return nil }
func (f *fakeSession) Read(ctx context.Context, req *ua.ReadRequest) (*ua.ReadResponse, error) {
	v, _ := ua.NewVariant(int64(9007199254740993))
	results := make([]*ua.DataValue, len(req.NodesToRead))
	for i := range results {
		results[i] = &ua.DataValue{Value: v, Status: ua.StatusOK}
	}
	return &ua.ReadResponse{Results: results}, nil
}
func (f *fakeSession) Write(ctx context.Context, req *ua.WriteRequest) (*ua.WriteResponse, error) {
	return &ua.WriteResponse{Results: make([]ua.StatusCode, len(req.NodesToWrite))}, nil
}
func (f *fakeSession) Browse(context.Context, *ua.BrowseRequest) (*ua.BrowseResponse, error) {
	return &ua.BrowseResponse{}, nil
}
func (f *fakeSession) BrowseNext(context.Context, *ua.BrowseNextRequest) (*ua.BrowseNextResponse, error) {
	return &ua.BrowseNextResponse{}, nil
}
func TestDemandSessionReuseAndLargeIntegerPreservation(t *testing.T) {
	c := NewConnector().(*connector)
	f := &fakeSession{}
	opens := 0
	c.open = func(context.Context, config) (uaSession, error) { opens++; return f, nil }
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: json.RawMessage(`{"endpoint":"opc.tcp://127.0.0.1:4840","security_mode":"None"}`)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"counter","node_id":"ns=2;s=counter"}]}`)})
		if err != nil {
			t.Fatal(err)
		}
		var data struct {
			Values map[string]any `json:"values"`
		}
		if err = json.Unmarshal(response.Payload, &data); err != nil {
			t.Fatal(err)
		}
		if data.Values["counter"] != "9007199254740993" {
			t.Fatalf("integer rounded: %v", data.Values)
		}
	}
	if opens != 1 {
		t.Fatalf("session opens=%d", opens)
	}
	c.Stop()
	if f.closed.Load() != 1 {
		t.Fatal("session not released")
	}
}
func TestOPCUASecurityAndWriteValidation(t *testing.T) {
	if err := validateConfig(config{Endpoint: "opc.tcp://localhost:4840", SecurityMode: "None", Username: "user", TimeoutMS: 100}); err == nil {
		t.Fatal("insecure credential auth accepted")
	}
	if err := validateConfig(config{Endpoint: "opc.tcp://localhost:4840", SecurityMode: "SignAndEncrypt", TimeoutMS: 100}); err == nil {
		t.Fatal("secure mode without pinned certificate accepted")
	}
	if err := validatePoints([]point{{Name: "x", NodeID: "ns=2;s=x", ValueType: "Uint16", Value: json.Number("65536")}}, true); err == nil {
		t.Fatal("out-of-range write accepted")
	}
}

func TestUAArrayIntegerPreservation(t *testing.T) {
	value, err := serializable([]int64{9007199254740993, 2}, 0)
	if err != nil {
		t.Fatal(err)
	}
	array := value.([]any)
	if array[0] != "9007199254740993" || array[1] != int64(2) {
		t.Fatal("array numbers changed")
	}
}
