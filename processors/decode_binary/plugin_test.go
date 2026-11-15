package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestBinaryEndiannessBoundsAndLargeInteger(t *testing.T) {
	p := &plugin{}
	cfg := `{"encoding":"hex","fields":[{"name":"number","offset":0,"type":"Uint16","byte_order":"little"},{"name":"counter","offset":2,"type":"Uint64"}]}`
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(cfg)}); err != nil {
		t.Fatal(err)
	}
	value, err := p.decode("34120020000000000001")
	if err != nil || value["number"] != uint16(0x1234) || value["counter"] != "9007199254740993" {
		t.Fatalf("%v %v", value, err)
	}
	if _, err = p.decode("34"); err == nil {
		t.Fatal("short frame accepted")
	}
	if _, err = decodeValue([]byte{0x7f, 0x80, 0, 0}, field{Type: "Float32"}); err == nil {
		t.Fatal("infinite field accepted")
	}
}

func TestBCDSpanAndTextEncodingVectors(t *testing.T) {
	value, err := decodeValue([]byte{0x12, 0x34}, field{Type: "BCD", Length: 2})
	if err != nil || value != "1234" {
		t.Fatal(value, err)
	}
	value, err = decodeValue([]byte{0x12, 0x3d}, field{Type: "SignedBCD", Length: 2})
	if err != nil || value != "-123" {
		t.Fatal(value, err)
	}
	if _, err = decodeValue([]byte{0xa1}, field{Type: "BCD", Length: 1}); err == nil {
		t.Fatal("invalid BCD digit")
	}
	value, err = decodeValue([]byte{0xf0, 0x01}, field{Type: "Bits", Length: 2, BitOffset: 4, BitLength: 8})
	if err != nil || value != uint64(0) {
		t.Fatal(value, err)
	}
	value, err = decodeValue([]byte{0x2d, 0x4e}, field{Type: "String", TextEncoding: "utf16le", Length: 2})
	if err != nil || value != "中" {
		t.Fatal(value, err)
	}
	if _, err = decodeValue([]byte{0x01}, field{Type: "String", TextEncoding: "utf16le", Length: 1}); err == nil {
		t.Fatal("truncated UTF-16")
	}
}
