package main

import (
	"encoding/binary"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func server(t *testing.T, badTransaction bool) (string, *atomic.Int32) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	count := &atomic.Int32{}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			count.Add(1)
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				for {
					h := make([]byte, 7)
					if _, err := io.ReadFull(conn, h); err != nil {
						return
					}
					n := int(binary.BigEndian.Uint16(h[4:6]))
					pdu := make([]byte, n-1)
					if _, err := io.ReadFull(conn, pdu); err != nil {
						return
					}
					var response []byte
					switch pdu[0] {
					case 1, 2:
						response = []byte{pdu[0], 1, 5}
					case 3, 4:
						q := int(binary.BigEndian.Uint16(pdu[3:5]))
						response = make([]byte, 2+q*2)
						response[0] = pdu[0]
						response[1] = byte(q * 2)
						for i := 0; i < q; i++ {
							binary.BigEndian.PutUint16(response[2+i*2:], uint16(100+i))
						}
					case 5, 6:
						response = pdu
					case 15, 16:
						response = pdu[:5]
					default:
						response = []byte{pdu[0] | 128, 1}
					}
					if badTransaction {
						h[0] ^= 1
					}
					binary.BigEndian.PutUint16(h[4:6], uint16(len(response)+1))
					if _, err := conn.Write(append(h, response...)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().String(), count
}
func configured(t *testing.T, addr string) *connector {
	t.Helper()
	h, p, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(p)
	c := NewConnector().(*connector)
	raw, _ := json.Marshal(connectionConfig{Host: h, Port: port, UnitID: 1, TimeoutMS: 500, Mode: "tcp"})
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "test", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop() })
	return c
}
func TestBatchMergesAdjacentPointsAndUsesOneSession(t *testing.T) {
	addr, count := server(t, false)
	c := configured(t, addr)
	raw := json.RawMessage(`{"points":[{"name":"a","area":"holding","address":0,"valueType":"Uint16"},{"name":"b","area":"holding","address":1,"valueType":"Uint16"}]}`)
	response, err := c.Execute("test", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestBatchRead, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Values   map[string]int `json:"values"`
		Segments int            `json:"segments"`
	}
	if err = json.Unmarshal(response.Payload, &out); err != nil {
		t.Fatal(err)
	}
	if out.Values["a"] != 100 || out.Values["b"] != 101 || out.Segments != 1 || count.Load() != 1 {
		t.Fatalf("unexpected batch: %+v count=%d", out, count.Load())
	}
}
func TestWriteReadOnlyValidationAndAcknowledgement(t *testing.T) {
	addr, count := server(t, false)
	c := configured(t, addr)
	for _, payload := range []string{`{"points":[{"name":"a","area":"input","address":0,"valueType":"Uint16","value":1}]}`, `{"points":[{"name":"a","area":"holding","address":0,"valueType":"Uint16","value":65536}]}`} {
		if _, err := c.Execute("test", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(payload)}); err == nil {
			t.Fatal("invalid write accepted")
		}
	}
	if count.Load() != 0 {
		t.Fatal("invalid request reached device")
	}
	for _, payload := range []string{`{"points":[{"name":"a","area":"holding","address":0,"valueType":"Uint16","value":23}]}`, `{"points":[{"name":"a","area":"holding","address":0,"valueType":"Float32","value":12.5}]}`, `{"points":[{"name":"a","area":"coil","address":0,"valueType":"Bool","value":true}]}`} {
		if _, err := c.Execute("test", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(payload)}); err != nil {
			t.Fatal(err)
		}
	}
}
func TestTransactionMismatchRejected(t *testing.T) {
	addr, _ := server(t, true)
	c := configured(t, addr)
	if _, err := c.Execute("test", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"x","area":"holding","address":0,"valueType":"Int16"}]}`)}); err == nil {
		t.Fatal("transaction mismatch accepted")
	}
}
func TestByteOrderAndAddressLimits(t *testing.T) {
	for _, order := range []string{"ABCD", "BADC", "CDAB", "DCBA"} {
		p := point{Name: "x", Area: "holding", ValueType: "Uint32", ByteOrder: order}
		p.Value = json.Number("305419896")
		encoded, err := encodeWrite(p, 1)
		if err != nil {
			t.Fatal(err)
		}
		if got := decodeValue(encoded[6:], p); got != uint32(305419896) {
			t.Fatalf("order %s: %v", order, got)
		}
	}
	if _, err := plan([]point{{Name: "x", Area: "holding", Address: 65535, ValueType: "Float32"}}, 1); err == nil {
		t.Fatal("overflow address accepted")
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
		_, e := c.Execute("cancel", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"x","area":"holding","address":0,"valueType":"Uint16"}]}`)})
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

func TestMultiCoilFC15AndBounds(t *testing.T) {
	p := point{Name: "bits", Area: "coil", Address: 100, ValueType: "BoolArray", Count: 3, Value: []any{true, false, true}}
	if _, err := pointWidth(p); err != nil {
		t.Fatal(err)
	}
	body, err := encodeWrite(p, 1)
	if err != nil {
		t.Fatal(err)
	}
	if body[0] != 15 || body[4] != 3 || body[5] != 1 || body[6] != 5 {
		t.Fatalf("invalid FC15 frame: %x", body)
	}
	p.Address = 65535
	if _, err = encodeWrite(p, 1); err == nil {
		t.Fatal("coil array exceeds address range")
	}
	addr, _ := server(t, false)
	c := configured(t, addr)
	_, err = c.Execute("test", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(`{"points":[{"name":"bits","area":"coil","address":100,"valueType":"BoolArray","count":3,"value":[true,false,true]}]}`)})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExpiredBatchNeverOpensDeviceSession(t *testing.T) {
	addr, count := server(t, false)
	c := configured(t, addr)
	_, err := c.Execute("test", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(`{"expires_at":1,"points":[{"name":"x","area":"holding","address":0,"valueType":"Uint16","value":1}]}`)})
	if err == nil || count.Load() != 0 {
		t.Fatal("expired command reached device")
	}
}
