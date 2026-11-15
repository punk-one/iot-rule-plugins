package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestOneEOfficialFormatGoldenVectors(t *testing.T) {
	for _, mode := range []string{"1e_binary", "1e_ascii"} {
		cfg := localMC(mode, "tcp")
		body := mcRequest(0x0401, "D", 100, 2, nil)
		frame, err := encodeMCFrame(cfg, body, 0)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := hex.DecodeString("01ff00006400000020440200")
		response, _ := hex.DecodeString("810034127856")
		if mcASCII(cfg) {
			want = []byte("01FF00004420000000640200")
			response = []byte("810012345678")
		}
		if !bytes.Equal(frame, want) {
			t.Fatalf("%s request %s (%x), want %s (%x)", mode, frame, frame, want, want)
		}
		value, err := decodeOneEResponse(cfg, body, response)
		if err != nil || !bytes.Equal(value, []byte{0x34, 0x12, 0x78, 0x56}) {
			t.Fatal(mode, value, err)
		}
		bit := mcRequest(0x0401, "M", 100, 256, nil)
		frame, err = encodeOneE(cfg, bit)
		if err != nil {
			t.Fatal(err)
		}
		if mcASCII(cfg) {
			if string(frame) != "00FF00004D20000000640000" {
				t.Fatal(string(frame))
			}
		} else if frame[10] != 0 || frame[11] != 0 {
			t.Fatal("256-bit zero count encoding wrong", frame)
		}
		write, _ := encodeWrite(point{Name: "out", Area: "M", Address: 50, ValueType: "Bool", Value: true}, 0)
		frame, err = encodeOneE(cfg, write)
		if err != nil {
			t.Fatal(err)
		}
		want, _ = hex.DecodeString("02ff000032000000204d010010")
		if mcASCII(cfg) {
			want = []byte("02FF00004D200000003201001")
		}
		if !bytes.Equal(frame, want) {
			t.Fatal("1E write format", mode, frame, want)
		}
	}
}
func TestOneELimitsNeverSplitA32BitPoint(t *testing.T) {
	points := []point{}
	for i := 0; i < 63; i++ {
		points = append(points, point{Name: fmt.Sprint(i), Area: "D", Address: i, ValueType: "Uint16"})
	}
	points = append(points, point{Name: "wide", Area: "D", Address: 63, ValueType: "Uint32"})
	ranges, err := planSized(points, 64, 256)
	if err != nil || len(ranges) != 2 || ranges[1].start != 63 || ranges[1].end != 65 {
		t.Fatal(ranges, err)
	}
	if _, err = encodeOneE(localMC("1e_binary", "tcp"), mcRequest(0x0401, "D", 0, 65, nil)); err == nil {
		t.Fatal("word count overflow accepted")
	}
}
func oneEFixtureReply(mode string, write bool) []byte {
	if mode == "1e_ascii" {
		if write {
			return []byte("8200")
		}
		return []byte("810012345678")
	}
	if write {
		return []byte{0x82, 0}
	}
	return []byte{0x81, 0, 0x34, 0x12, 0x78, 0x56}
}
func TestOneETCPAndUDPSimulatedPLCs(t *testing.T) {
	for _, mode := range []string{"1e_binary", "1e_ascii"} {
		for _, transport := range []string{"tcp", "udp"} {
			t.Run(mode+"/"+transport, func(t *testing.T) {
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
							buf := make([]byte, 1024)
							if _, peer, err := l.ReadFrom(buf); err != nil {
								done <- err
								return
							} else if _, err = l.WriteTo(oneEFixtureReply(mode, i == 1), peer); err != nil {
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
							headerSize := 12
							if mode == "1e_ascii" {
								headerSize = 24
							}
							header := make([]byte, headerSize)
							if _, err = io.ReadFull(conn, header); err != nil {
								conn.Close()
								done <- err
								return
							}
							if i == 1 {
								var value [1]byte
								if _, err = io.ReadFull(conn, value[:]); err != nil {
									conn.Close()
									done <- err
									return
								}
							}
							_, err = conn.Write(oneEFixtureReply(mode, i == 1))
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
				c := NewConnector()
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
				if _, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Payload: json.RawMessage(`{"points":[{"name":"out","area":"M","address":50,"valueType":"Bool","value":true}]}`)}); err != nil {
					t.Fatal(err)
				}
				if err = <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
func TestOneERejectsErrorOpcodeAndIncompleteRead(t *testing.T) {
	for _, mode := range []string{"1e_binary", "1e_ascii"} {
		cfg := localMC(mode, "tcp")
		body := mcRequest(0x0401, "D", 100, 2, nil)
		for _, binaryFrame := range [][]byte{{0x81, 0x5b}, {0x80, 0, 1, 2, 3, 4}, {0x81, 0, 1, 2}} {
			frame := binaryFrame
			if mcASCII(cfg) {
				frame = []byte(hex.EncodeToString(binaryFrame))
			}
			if _, err := decodeOneEResponse(cfg, body, frame); err == nil {
				t.Fatal("invalid response accepted", mode, frame)
			}
		}
	}
	c := NewConnector()
	defer c.Stop()
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "route", Connection: json.RawMessage(`{"host":"127.0.0.1","mode":"1e_binary","network_number":1}`)}); err == nil {
		t.Fatal("unrepresentable 1E route ignored")
	}
}
