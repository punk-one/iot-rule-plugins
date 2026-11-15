package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBridgeAuthenticationReadOnlyAndContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config{Listen: "127.0.0.1:19080", TimeoutMS: 1000, Devices: map[string]device{"demo": {ProgID: "Synthetic.Test", OpenMethod: "Open", OpenArguments: []any{"127.0.0.1", float64(0), float64(1)}, Points: map[string]readPoint{"value": {Method: "GetValue"}}}}}
	token := strings.Repeat("a", 32)
	bridge, err := newBridge(cfg, token, func(device) result {
		return result{data: map[string]any{"value": 12.5}, quality: map[string]any{"value": "good"}}
	}, ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/v1/devices/demo/telemetry", nil)
	response := httptest.NewRecorder()
	bridge.ServeHTTP(response, request)
	if response.Code != 401 {
		t.Fatal("missing authentication accepted")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	bridge.ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"device_code":"demo"`) {
		t.Fatal(response.Code, response.Body.String())
	}
	request.Method = "POST"
	response = httptest.NewRecorder()
	bridge.ServeHTTP(response, request)
	if response.Code != 405 {
		t.Fatal("write endpoint accepted")
	}
	d := cfg.Devices["demo"]
	d.Points["value"] = readPoint{Method: "DeleteProgram"}
	cfg.Devices["demo"] = d
	if validate(cfg) == nil {
		t.Fatal("mutating method accepted")
	}
}

func TestCOMArgsAreBoundedScalars(t *testing.T) {
	for _, value := range []any{map[string]any{"method": "Delete"}, []any{1}, float64(2147483648)} {
		if validateArgs([]any{value}) == nil {
			t.Fatal("unsafe COM argument accepted")
		}
	}
}
