package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestResponseDecodeJSONAndStatus(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"status": 200, "body_base64": base64.StdEncoding.EncodeToString([]byte(`{"value":12.5}`)), "content_type": "application/json"})
	value, err := decodeResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if value["body"].(map[string]any)["value"] != 12.5 {
		t.Fatal("JSON response lost")
	}
	if _, err = decodeResponse([]byte(`{"status":200,"body_base64":"!"}`)); err == nil {
		t.Fatal("invalid base64 accepted")
	}
}

func TestLargeJSONIntegerIsNotRounded(t *testing.T) {
	value, err := decodeSafeJSON([]byte(`{"counter":9007199254740993,"nested":[18446744073709551615],"temperature":12.5}`))
	if err != nil {
		t.Fatal(err)
	}
	object := value.(map[string]any)
	if object["counter"] != "9007199254740993" || object["nested"].([]any)[0] != "18446744073709551615" || object["temperature"] != 12.5 {
		t.Fatalf("value changed: %v", value)
	}
	if _, err := decodeSafeJSON([]byte(`{} {}`)); err == nil {
		t.Fatal("trailing data accepted")
	}
}
