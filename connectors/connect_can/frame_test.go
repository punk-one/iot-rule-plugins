package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSocketCANFixedVectorsAndFormats(t *testing.T) {
	f, err := fromSpec(frameSpec{ID: "0x123", Data: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]byte, 16)
	binary.NativeEndian.PutUint32(expected, 0x123)
	expected[4] = 3
	copy(expected[8:], []byte{1, 2, 3})
	if !bytes.Equal(raw, expected) {
		t.Fatal(raw, expected)
	}
	for _, spec := range []frameSpec{{ID: "0x1fffffff", Extended: true, Data: []byte{1, 2}}, {ID: "0x123", Remote: true, RTRLength: 8}, {ID: "0x123", FD: true, BitrateSwitch: true, ErrorState: true, Data: bytes.Repeat([]byte{9}, 64)}} {
		frame, err := fromSpec(spec)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := encodeFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := decodeFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.id != frame.id || parsed.extended != frame.extended || parsed.fd != frame.fd || parsed.remote != frame.remote || parsed.brs != frame.brs || parsed.esi != frame.esi || parsed.rtrLength != frame.rtrLength || !bytes.Equal(parsed.data, frame.data) {
			t.Fatal(parsed, frame)
		}
	}
}
func TestRejectAmbiguousIDsAndCANFDLengths(t *testing.T) {
	for _, id := range []string{"0123", "0x", "-1", "+1", " 1", "0x800"} {
		if _, err := parseID(id, false); err == nil {
			t.Fatal(id)
		}
	}
	for _, spec := range []frameSpec{{ID: "1", FD: true, Data: make([]byte, 9)}, {ID: "1", FD: true, Remote: true}, {ID: "1", Data: make([]byte, 9)}, {ID: "1", Remote: true, Data: []byte{1}}, {ID: "1", BitrateSwitch: true}} {
		if _, err := fromSpec(spec); err == nil {
			t.Fatal(spec)
		}
	}
	if _, err := decodeFrame(make([]byte, 15)); err == nil {
		t.Fatal("short frame accepted")
	}
	if _, err := encodeFrame(canFrame{busError: true}); err == nil {
		t.Fatal("error frame transmission accepted")
	}
}
