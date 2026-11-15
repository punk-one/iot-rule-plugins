package main

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestComplexRuntimeFixedVectors(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want string
	}{
		{"overrides", []byte{0, 0, 0x27, 0x10, 0, 0, 0x13, 0x88, 0, 0, 0x9, 0xc4}, `"feed_percent":100`},
		{"current_tool", []byte{0, 0, 0, 42, 0, 3, 0, 2}, `"axis":"Z"`},
		{"axis_locations", append([]byte{0, 2}, []byte("123.456\x00-45.678\x00X\x00Y\x00")...), `"positions":{"X":123.456,"Y":-45.678}`},
		{"selected_program", append([]byte{0, 0, 0, 27}, []byte("TNC:\\main.h\x00TNC:\\sub.h\x00")...), `"line_number":27`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, e := decode(queries[tc.name], tc.raw)
			b, _ := json.Marshal(v)
			if e != nil || !strings.Contains(string(b), tc.want) || !strings.Contains(string(b), "raw_base64") {
				t.Fatal(string(b), e)
			}
		})
	}
	raw := make([]byte, 24)
	binary.BigEndian.PutUint32(raw, 99)
	binary.LittleEndian.PutUint64(raw[8:], math.Float64bits(123.25))
	binary.LittleEndian.PutUint64(raw[16:], math.Float64bits(4.5))
	v, e := decode(queries["current_tool"], raw)
	if e != nil || v.(map[string]any)["length"] != 123.25 {
		t.Fatal(v, e)
	}
	binary.LittleEndian.PutUint64(raw[8:], math.Float64bits(math.NaN()))
	if _, e = decode(queries["current_tool"], raw); e == nil {
		t.Fatal("NaN tool accepted")
	}
	text, _ := textValue([]byte{'M', 0xfc, 'l', 'l', 0})
	if text != "Müll" {
		t.Fatal(text)
	}
}

func TestComplexRuntimeRejectsInvalidVariants(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"overrides", make([]byte, 13)}, {"current_tool", make([]byte, 16)},
		{"axis_locations", []byte{0, 33}}, {"axis_locations", append([]byte{0, 2}, []byte("1\x002\x00X\x00X\x00")...)},
		{"axis_locations", append([]byte{0, 1}, []byte("NaN\x00X\x00")...)},
		{"axis_locations", append([]byte{0, 1}, []byte("1\x00X\x00junk")...)},
		{"selected_program", []byte{0, 0, 0, 1, 'A', 0, 'B'}},
	}
	for _, tc := range cases {
		if _, e := decode(queries[tc.name], tc.raw); e == nil {
			t.Fatalf("accepted %s: %x", tc.name, tc.raw)
		}
	}
}

func FuzzComplexRuntime(f *testing.F) {
	f.Add([]byte{0, 1, '1', 0, 'X', 0})
	f.Add(make([]byte, 24))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 65536 {
			t.Skip()
		}
		for _, name := range []string{"overrides", "current_tool", "axis_locations", "selected_program"} {
			v, e := decode(queries[name], b)
			if e == nil {
				if _, e = json.Marshal(v); e != nil {
					t.Fatal(e)
				}
			}
		}
	})
}
