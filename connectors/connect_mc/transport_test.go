package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func localMC(mode, transport string) connectionConfig {
	return connectionConfig{Mode: mode, Transport: transport, PCNumber: 255, IONumber: 1023, MonitoringTimer: 16}
}
func TestMCFrameFixedVectors(t *testing.T) {
	body := mcRequest(0x0401, "D", 100, 2, nil)
	for _, test := range []struct {
		mode, request, response string
		ascii                   bool
	}{
		{mode: "3e_ascii", request: "500000FF03FF000018001004010000D*0001000002", response: "D00000FF03FF00000C000012345678", ascii: true},
		{mode: "4e_ascii", request: "54001234000000FF03FF000018001004010000D*0001000002", response: "D4001234000000FF03FF00000C000012345678", ascii: true},
		{mode: "3e_binary", request: "500000ffff03000c00100001040000640000a80200", response: "d00000ffff03000600000034127856"},
		{mode: "4e_binary", request: "54003412000000ffff03000c00100001040000640000a80200", response: "d4003412000000ffff03000600000034127856"},
	} {
		t.Run(test.mode, func(t *testing.T) {
			cfg := localMC(test.mode, "tcp")
			request, err := encodeMCFrame(cfg, body, 0x1234)
			if err != nil {
				t.Fatal(err)
			}
			want, response := []byte(test.request), []byte(test.response)
			if !test.ascii {
				want, _ = hex.DecodeString(test.request)
				response, _ = hex.DecodeString(test.response)
			}
			if !bytes.Equal(request, want) {
				t.Fatalf("request %s (%x), want %s", request, request, test.request)
			}
			data, err := decodeMCResponse(cfg, request, response, body)
			if err != nil || !bytes.Equal(data, []byte{0x34, 0x12, 0x78, 0x56}) {
				t.Fatalf("data %x error %v", data, err)
			}
		})
	}
}
func TestMCASCIIWordAndBitRepresentations(t *testing.T) {
	write, _ := encodeWrite(point{Name: "x", Area: "D", Address: 1, ValueType: "Uint32", Value: json.Number("305419896")}, 0)
	ascii, err := asciiRequestBody(write, 16)
	if err != nil || !strings.HasSuffix(string(ascii), "56781234") {
		t.Fatal(string(ascii), err)
	}
	boolBody := mcRequest(0x0401, "M", 0, 3, nil)
	cfg := localMC("3e_ascii", "tcp")
	request, _ := encodeMCFrame(cfg, boolBody, 0)
	response := []byte("D00000FF03FF0000070000101")
	decoded, err := decodeMCResponse(cfg, request, response, boolBody)
	if err != nil || !bytes.Equal(decoded, []byte{0x10, 0x10}) {
		t.Fatal(decoded, err)
	}
	if _, err = asciiRequestBody(mcRequest(0x0401, "D", 1000000, 1, nil), 16); err == nil {
		t.Fatal("decimal ASCII address overflow accepted")
	}
}
func fixtureMCReply(cfg connectionConfig, request []byte, write bool, stale bool) []byte {
	h := append([]byte(nil), request[:mcHeaderSize(cfg)]...)
	if mcASCII(cfg) {
		copy(h[:4], "D000")
		if mc4E(cfg) {
			copy(h[:4], "D400")
			if stale {
				value, _ := mcHex(string(h[4:8]))
				copy(h[4:8], fmt.Sprintf("%04X", uint16(value)^1))
			}
		}
		data := "000012345678"
		if write {
			data = "0000"
		}
		copy(h[len(h)-4:], fmt.Sprintf("%04X", len(data)))
		return append(h, []byte(data)...)
	}
	h[0] = 0xd0
	if mc4E(cfg) {
		h[0] = 0xd4
		if stale {
			h[2] ^= 0xff
		}
	}
	data := []byte{0, 0, 0x34, 0x12, 0x78, 0x56}
	if write {
		data = []byte{0, 0}
	}
	binary.LittleEndian.PutUint16(h[len(h)-2:], uint16(len(data)))
	return append(h, data...)
}
func TestAllMCTransportCombinations(t *testing.T) {
	for _, mode := range []string{"3e_binary", "3e_ascii", "4e_binary", "4e_ascii"} {
		for _, transport := range []string{"tcp", "udp"} {
			t.Run(mode+"/"+transport, func(t *testing.T) {
				cfg := localMC(mode, transport)
				var address string
				done := make(chan error, 1)
				if transport == "udp" {
					l, err := net.ListenPacket("udp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					address = l.LocalAddr().String()
					go func() {
						l.SetDeadline(time.Now().Add(3 * time.Second))
						for i := 0; i < 2; i++ {
							b := make([]byte, 8192)
							n, peer, err := l.ReadFrom(b)
							if err != nil {
								done <- err
								return
							}
							if mc4E(cfg) {
								if _, err = l.WriteTo(fixtureMCReply(cfg, b[:n], i == 1, true), peer); err != nil {
									done <- err
									return
								}
							}
							if _, err = l.WriteTo(fixtureMCReply(cfg, b[:n], i == 1, false), peer); err != nil {
								done <- err
								return
							}
						}
						done <- nil
					}()
				} else {
					l, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					address = l.Addr().String()
					go func() {
						for i := 0; i < 2; i++ {
							conn, err := l.Accept()
							if err != nil {
								done <- err
								return
							}
							conn.SetDeadline(time.Now().Add(3 * time.Second))
							header := make([]byte, mcHeaderSize(cfg))
							if _, err = io.ReadFull(conn, header); err != nil {
								conn.Close()
								done <- err
								return
							}
							size, err := mcResponseSize(cfg, header)
							if err != nil {
								conn.Close()
								done <- err
								return
							}
							body := make([]byte, size)
							if _, err = io.ReadFull(conn, body); err != nil {
								conn.Close()
								done <- err
								return
							}
							_, err = conn.Write(fixtureMCReply(cfg, append(header, body...), i == 1, false))
							conn.Close()
							if err != nil {
								done <- err
								return
							}
						}
						done <- nil
					}()
				}
				host, portText, _ := net.SplitHostPort(address)
				port, _ := strconv.Atoi(portText)
				c := NewConnector().(*connector)
				defer c.Stop()
				raw, _ := json.Marshal(map[string]any{"host": host, "port": port, "mode": mode, "transport": transport, "timeout_ms": 1000})
				if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
					t.Fatal(err)
				}
				response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"a","area":"D","address":100,"valueType":"Uint16"},{"name":"b","area":"D","address":101,"valueType":"Uint16"}]}`)})
				if err != nil {
					t.Fatal(err)
				}
				var result struct {
					Values map[string]int `json:"values"`
				}
				json.Unmarshal(response.Payload, &result)
				if result.Values["a"] != 0x1234 || result.Values["b"] != 0x5678 {
					t.Fatal(string(response.Payload))
				}
				if _, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(`{"points":[{"name":"output","area":"M","address":3,"valueType":"Bool","value":true}]}`)}); err != nil {
					t.Fatal(err)
				}
				if err = <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
func TestMCRejectsRouteSerialAndCorruptResponses(t *testing.T) {
	for _, mode := range []string{"3e_binary", "3e_ascii", "4e_binary", "4e_ascii"} {
		cfg := localMC(mode, "tcp")
		body := mcRequest(0x0401, "D", 100, 2, nil)
		request, _ := encodeMCFrame(cfg, body, 123)
		response := fixtureMCReply(cfg, request, false, false)
		route := 2
		if mc4E(cfg) {
			route = 6
		}
		if mcASCII(cfg) {
			route *= 2
			response[route] = '1'
		} else {
			response[route] ^= 1
		}
		if _, err := decodeMCResponse(cfg, request, response, body); err == nil {
			t.Fatal("route mismatch accepted", mode)
		}
		if mc4E(cfg) {
			bad := fixtureMCReply(cfg, request, false, true)
			if _, err := decodeMCResponse(cfg, request, bad, body); !errors.Is(err, errMCSerial) {
				t.Fatal("serial mismatch accepted", mode, err)
			}
		}
	}
	for _, bad := range []string{"+001", "00G0", "000-"} {
		if _, err := mcHex(bad); err == nil {
			t.Fatal("invalid hexadecimal accepted", bad)
		}
	}
}
func Test3EUDPDoesNotMisassociateMultiSegmentRead(t *testing.T) {
	c := NewConnector()
	defer c.Stop()
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: json.RawMessage(`{"host":"127.0.0.1","port":1,"transport":"udp"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"a","area":"D","address":1,"valueType":"Uint16"},{"name":"b","area":"D","address":900,"valueType":"Uint16"}]}`)}); err == nil || !strings.Contains(err.Error(), "one PDU") {
		t.Fatal("unsafe 3E UDP batch accepted", err)
	}
}
