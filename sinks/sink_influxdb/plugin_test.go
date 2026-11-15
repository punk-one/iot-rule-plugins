package main

import (
	"math"
	"strings"
	"testing"
)

func TestLineEscapingAndExactIntegers(t *testing.T) {
	cfg := config{Measurement: "test space", FieldTypes: map[string]string{"counter": "uint64"}}
	p := map[string]any{"device_code": "demo,=1", "data": map[string]any{"value": 12.5, "counter": "18446744073709551615", "text": "a\"b"}, "time": float64(1000)}
	line, err := encodeLine(cfg, p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, `test\ space,device_code=demo\,\=1 `) || !strings.Contains(line, "counter=18446744073709551615u") {
		t.Fatal(line)
	}
	p["data"] = map[string]any{"bad": math.Inf(1)}
	if _, err = encodeLine(cfg, p); err == nil {
		t.Fatal("nonfinite accepted")
	}
	p["data"] = map[string]any{"v": 1.0}
	p["time"] = 1.5
	if _, err = encodeLine(cfg, p); err == nil {
		t.Fatal("fractional timestamp")
	}
}
