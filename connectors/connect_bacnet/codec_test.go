package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func fromHex(s string) []byte {
	v, e := hex.DecodeString(s)
	if e != nil {
		panic(e)
	}
	return v
}
func TestReadPropertyFixedVector(t *testing.T) {
	p := point{Name: "temperature", ObjectType: 2, Instance: 1, Property: 85, ValueType: "Float32"}
	want := fromHex("810a001101040004010c0c008000011955")
	if got := requestPacket(1, 12, p, nil); !bytes.Equal(got, want) {
		t.Fatalf("request %x", got)
	}
	reply := fromHex("810a0017010030010c0c0080000119553e4441b600003f")
	v, e := responsePacket(reply, 1, 12, p)
	if e != nil || v != 22.75 {
		t.Fatalf("%v %v", v, e)
	}
	reply[14]++
	if _, e = responsePacket(reply, 1, 12, p); e == nil {
		t.Fatal("address mismatch accepted")
	}
}
func TestScalarRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		kind  string
		value any
		want  any
	}{{"Null", nil, nil}, {"Bool", true, true}, {"Uint32", json.Number("4294967295"), uint32(4294967295)}, {"Int32", json.Number("-2147483648"), int32(-2147483648)}, {"Int32", 127, int32(127)}, {"Int32", -129, int32(-129)}, {"Float32", 22.75, 22.75}, {"Float64", -1e200, -1e200}, {"Enum", 255, uint32(255)}, {"String", "温度\x00value", "温度\x00value"}, {"Bytes", "AAEC/w==", "AAEC/w=="}, {"Bits", "101010101", "101010101"}, {"Bits", "", ""}, {"ObjectID", map[string]any{"object_type": 8, "instance": 1234}, map[string]any{"object_type": uint32(8), "instance": uint32(1234)}}} {
		t.Run(tc.kind, func(t *testing.T) {
			b, e := encodeValue(point{ValueType: tc.kind, Value: tc.value})
			if e != nil {
				t.Fatal(e)
			}
			got, e := decodeValue(b, tc.kind)
			if e != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%v %v want %v bytes %x", got, e, tc.want, b)
			}
		})
	}
}
func TestRejectUnsupportedAndMalformed(t *testing.T) {
	for _, tc := range []struct {
		kind string
		b    []byte
	}{{"Float32", fromHex("447f800000")}, {"Float64", fromHex("55807ff0000000000000")}, {"String", []byte{0x72, 5, 'a'}}, {"Bits", []byte{0x82, 7, 1}}, {"Time", []byte{0xb4, 24, 0, 0, 0}}, {"Uint32", []byte{0x20}}, {"Null", []byte{0, 0}}, {"Bool", []byte{0x12}}} {
		if _, e := decodeValue(tc.b, tc.kind); e == nil {
			t.Fatalf("accepted %s %x", tc.kind, tc.b)
		}
	}
	for _, p := range []point{{ValueType: "Uint32", Value: -1}, {ValueType: "Int32", Value: 2147483648.0}, {ValueType: "Float32", Value: math.Inf(1)}, {ValueType: "Float32", Value: math.MaxFloat64}, {ValueType: "Bool", Value: 1}, {ValueType: "Bytes", Value: "!"}} {
		if _, e := encodeValue(p); e == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
}
func TestProtocolEnvelopes(t *testing.T) {
	p := point{Name: "x", ValueType: "Float32"}
	for _, raw := range [][]byte{{}, {0x81}, fromHex("810a0009010039010c"), fromHex("810a0009010030020c"), fromHex("810a0009010030010d"), fromHex("810b0009010020010f")} {
		if _, e := responsePacket(raw, 1, 12, p); e == nil {
			t.Fatalf("accepted %x", raw)
		}
	}
	v, e := responsePacket(fromHex("810a0009010020010f"), 1, 15, p)
	if e != nil || v != nil {
		t.Fatal(v, e)
	}
	_, e = responsePacket(fromHex("810a000d010050010c91029120"), 1, 12, p)
	if _, ok := e.(*deviceError); !ok {
		t.Fatalf("not a device error %v", e)
	}
}
func FuzzScalarDecoder(f *testing.F) {
	f.Add([]byte{0x44, 0x41, 0xb6, 0, 0})
	f.Add([]byte{0x75, 254, 255, 255})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		for _, kind := range []string{"Null", "Bool", "Uint32", "Int32", "Float32", "Float64", "Bytes", "String", "Bits", "Enum", "Date", "Time", "ObjectID"} {
			v, e := decodeValue(b, kind)
			if e == nil {
				if _, e = json.Marshal(v); e != nil {
					t.Fatal(e)
				}
			}
		}
		p := point{ValueType: "Float32"}
		_, _ = responsePacket(b, 1, 12, p)
	})
}
func readReply(invoke byte, p point, value []byte) []byte {
	b := append([]byte{0x81, 0x0a, 0, 0, 1, 0, 0x30, invoke, 12}, address(p)...)
	b = append(b, 0x3e)
	b = append(b, value...)
	b = append(b, 0x3f)
	binary.BigEndian.PutUint16(b[2:], uint16(len(b)))
	return b
}
