package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"testing"
)

func TestTagPathVectorsAndBoundedValues(t *testing.T) {
	raw, err := tagPath("Tag[300].Value")
	expected, _ := hex.DecodeString("91035461670029002c01910556616c756500")
	if err != nil || !bytes.Equal(raw, expected) {
		t.Fatal(hex.EncodeToString(raw), err)
	}
	for _, name := range []string{"A[1,2]", "A[-1]", "A;DROP", "A[99999999999]"} {
		if _, err = tagPath(name); err == nil {
			t.Fatal("invalid tag accepted")
		}
	}
	value, err := decodeValue([]byte{0xc5, 0, 1, 0, 0, 0, 0, 0, 0x20, 0}, "Int64")
	if err != nil || value != "9007199254740993" {
		t.Fatal(value, err)
	}
	if _, err = encodeValue(point{ValueType: "Int64", Value: float64(9007199254740992)}); err == nil {
		t.Fatal("rounded int64 accepted")
	}
}
func TestExplicitSessionUsesOneConnectionForBatch(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		for i := 0; i < 3; i++ {
			header := make([]byte, 24)
			if _, err = io.ReadFull(conn, header); err != nil {
				done <- err
				return
			}
			raw := make([]byte, int(binary.LittleEndian.Uint16(header[2:4])))
			if _, err = io.ReadFull(conn, raw); err != nil {
				done <- err
				return
			}
			if i == 0 {
				binary.LittleEndian.PutUint32(header[4:8], 123)
				raw = []byte{1, 0, 0, 0}
			} else {
				raw = cpfRequest([]byte{0xcc, 0, 0, 0, 0xc4, 0, 42, 0, 0, 0})
			}
			binary.LittleEndian.PutUint16(header[2:4], uint16(len(raw)))
			if _, err = conn.Write(append(header, raw...)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(config{Host: host, Port: n, TimeoutMS: 1000})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"a","tag":"A","valueType":"Int32"},{"name":"b","tag":"B","valueType":"Int32"}]}`)})
	if err != nil || !bytes.Contains(response.Payload, []byte(`"a":42`)) {
		t.Fatal(string(response.Payload), err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
