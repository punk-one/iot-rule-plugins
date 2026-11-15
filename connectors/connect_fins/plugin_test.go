package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"strconv"
	"testing"
)

func TestCSCJAreaAndMergedPlan(t *testing.T) {
	for area, code := range map[string]byte{"DM": 0x82, "CIO": 0xb0, "W": 0xb1, "H": 0xb2, "A": 0xb3} {
		got, _ := areaCode(point{Area: area, ValueType: "Uint16"})
		if got != code {
			t.Fatal("area code mismatch")
		}
	}
	plan, err := plan([]point{{Name: "a", Area: "DM", Address: 0, ValueType: "Uint16"}, {Name: "b", Area: "DM", Address: 1, ValueType: "Uint16"}})
	if err != nil || len(plan) != 1 || plan[0].end != 2 {
		t.Fatal("not merged")
	}
	if _, err = validate(point{Name: "x", Area: "DM", ValueType: "Float32"}); err == nil {
		t.Fatal("implicit 32-bit word order")
	}
}
func TestFINSUDPReadWriteAndSIDValidation(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go func() {
		buf := make([]byte, 4096)
		for {
			n, addr, err := server.ReadFrom(buf)
			if err != nil {
				return
			}
			request := append([]byte(nil), buf[:n]...)
			if n < 18 {
				continue
			}
			reply := append([]byte(nil), request[:12]...)
			reply[0] = 0xc0
			copy(reply[3:6], request[6:9])
			copy(reply[6:9], request[3:6])
			reply = append(reply, 0, 0)
			if request[11] == 1 {
				count := int(binary.BigEndian.Uint16(request[16:18]))
				for i := 0; i < count; i++ {
					reply = append(reply, 0, byte(40+i))
				}
			}
			server.WriteTo(reply, addr)
		}
	}()
	host, port, _ := net.SplitHostPort(server.LocalAddr().String())
	n, _ := strconv.Atoi(port)
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(config{Host: host, Port: n, Mode: "udp", SourceNode: 10, DestNode: 20, TimeoutMS: 1000})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []sdk.ConnectorRequestKind{sdk.ConnectorRequestRead, sdk.ConnectorRequestWrite} {
		raw = []byte(`{"points":[{"name":"a","area":"DM","address":0,"valueType":"Uint16","value":3},{"name":"b","area":"DM","address":1,"valueType":"Uint16","value":4}]}`)
		res, err := c.Execute("r", sdk.ConnectorRequest{Kind: kind, Payload: raw})
		if err != nil {
			t.Fatal(err)
		}
		if kind == sdk.ConnectorRequestRead && (!bytes.Contains(res.Payload, []byte(`"a":40`)) || !bytes.Contains(res.Payload, []byte(`"b":41`))) {
			t.Fatal(string(res.Payload))
		}
	}
}
