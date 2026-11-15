package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func Test3EProtocolGoldenFrames(t *testing.T) {
	got := mcRequest(0x0401, "D", 100, 2, nil)
	want := []byte{16, 0, 1, 4, 0, 0, 100, 0, 0, 0xa8, 2, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("request %x want %x", got, want)
	}
	p := point{Name: "p", Area: "D", Address: 100, ValueType: "Uint32", Value: json.Number("305419896")}
	w, err := encodeWrite(p, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w[12:], []byte{0x78, 0x56, 0x34, 0x12}) {
		t.Fatalf("MC word encoding %x", w)
	}
	if decodeValue(w[12:], p) != uint32(305419896) {
		t.Fatal("word decode mismatch")
	}
}
func TestMCBatchReadAndWriteWithSimulatedPLC(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2; i++ {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			h := make([]byte, 9)
			if _, err = io.ReadFull(conn, h); err != nil {
				conn.Close()
				return
			}
			body := make([]byte, int(binary.LittleEndian.Uint16(h[7:9])))
			if _, err = io.ReadFull(conn, body); err != nil {
				conn.Close()
				return
			}
			response := []byte{0, 0}
			if binary.LittleEndian.Uint16(body[2:4]) == 0x0401 {
				response = append(response, 0x34, 0x12, 0x78, 0x56)
			}
			h[0] = 0xd0
			binary.LittleEndian.PutUint16(h[7:9], uint16(len(response)))
			conn.Write(append(h, response...))
			conn.Close()
		}
	}()
	host, p, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(p)
	c := NewConnector().(*connector)
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"host": host, "port": port})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"a","area":"D","address":100,"valueType":"Uint16"},{"name":"b","area":"D","address":101,"valueType":"Uint16"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Values map[string]int `json:"values"`
	}
	if err = json.Unmarshal(resp.Payload, &result); err != nil {
		t.Fatal(err)
	}
	if result.Values["a"] != 0x1234 || result.Values["b"] != 0x5678 {
		t.Fatalf("values %+v", result)
	}
	if _, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(`{"points":[{"name":"a","area":"M","address":0,"valueType":"Bool","value":true}]}`)}); err != nil {
		t.Fatal(err)
	}
	<-done
}
func TestMCRejectsUnsupportedModesAndInputWrites(t *testing.T) {
	c := NewConnector()
	raw := json.RawMessage(`{"host":"localhost","mode":"4e"}`)
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err == nil {
		t.Fatal("4E accepted")
	}
	if _, err := encodeWrite(point{Name: "x", Area: "X", ValueType: "Bool", Value: true}, 0); err == nil {
		t.Fatal("X write accepted")
	}
}

func TestStopCancelsBlockedNetworkRead(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, e := l.Accept()
		if e == nil {
			accepted <- conn
		}
	}()
	host, ps, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(ps)
	c := NewConnector()
	raw, _ := json.Marshal(map[string]any{"host": host, "port": port, "timeout_ms": 30000})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "cancel", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, e := c.Execute("cancel", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"x","area":"D","address":0,"valueType":"Uint16"}]}`)})
		done <- e
	}()
	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("connection not opened")
	}
	defer conn.Close()
	c.Stop()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("canceled read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel network read")
	}
}
