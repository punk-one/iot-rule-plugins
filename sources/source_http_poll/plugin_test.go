package main

import (
	"encoding/base64"
	"testing"
)

func TestPollBodyDecode(t *testing.T) {
	data, err := decodeBody(base64.StdEncoding.EncodeToString([]byte(`{"temperature":12.5}`)), "application/json")
	if err != nil || data["temperature"] != 12.5 {
		t.Fatalf("%v %v", data, err)
	}
	if _, err := decodeBody("!", "application/json"); err == nil {
		t.Fatal("invalid encoding accepted")
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
