package main

import (
	"encoding/json"
	"testing"
)

func TestDefaultEngineeringPolicyStaysBadAndRoundsAfterReplacement(t *testing.T) {
	lo, hi := 0.0, 10.0
	precision := 1
	p := point{Name: "x", Area: "D", Address: 0, ValueType: "Float32", Min: &lo, Max: &hi, Precision: &precision, OutOfRange: "default", Default: json.Number("4.567")}
	if err := validatePoint(p); err != nil {
		t.Fatal(err)
	}
	value, quality := engineering(99.0, p)
	if value != 4.6 || quality != "bad:range" {
		t.Fatal(value, quality)
	}
	for _, invalid := range []any{map[string]any{"x": 1}, "4.5", 11.0} {
		p.Default = invalid
		if validatePoint(p) == nil {
			t.Fatal("invalid default accepted", invalid)
		}
	}
}
