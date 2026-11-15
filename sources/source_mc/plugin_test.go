package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestEngineeringPreservesBadQualityWithDefault(t *testing.T) {
	lo, hi, scale := float64(0), float64(100), float64(0.1)
	precision := 2
	p := point{ValueType: "Float32", Scale: &scale, Precision: &precision, Min: &lo, Max: &hi, OutOfRange: "default", Default: 22.5}
	v, q := engineering(float64(1234), p)
	if v != 22.5 || q != "bad:range" {
		t.Fatalf("%v %s", v, q)
	}
	v, q = engineering(float64(123), p)
	if v != 12.3 || q != "good" {
		t.Fatalf("%v %s", v, q)
	}
}
func TestSourceRejectsDuplicateAndInvalidPoints(t *testing.T) {
	for _, raw := range []string{`{"connector_ref":"x","device_code":"d","points":[{"name":"x","area":"D","address":0,"valueType":"Uint16"},{"name":"x","area":"D","address":1,"valueType":"Uint16"}]}`, `{"connector_ref":"x","device_code":"d","points":[{"name":"x","area":"D","address":-1,"valueType":"Uint16"}]}`} {
		p := &plugin{}
		if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(raw)}); err == nil {
			t.Fatal("invalid source config accepted")
		}
	}
}
