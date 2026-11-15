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

func TestASDUQualitySequentialAndInvalidTimestamp(t *testing.T) {
	raw := []byte{1, 0x82, 20, 0, 1, 0, 100, 0, 0, 1, 0x80}
	values, _, err := parseASDU(raw, 1, time.UTC)
	if err != nil || values[100].value != true || values[101].quality != 0x80 {
		t.Fatal(values, err)
	}
	if _, _, err = parseASDU(raw[:len(raw)-1], 1, time.UTC); err == nil {
		t.Fatal("truncated ASDU")
	}
	if _, bad := cp56([]byte{0, 0, 0, 0, 31, 2, 26}, time.UTC); !bad {
		t.Fatal("invalid February timestamp")
	}
	if _, _, err = parseASDU([]byte{100, 1, 0x4a, 0, 1, 0, 0, 0, 0, 20}, 1, time.UTC); err == nil {
		t.Fatal("negative confirmation accepted")
	}
}
func TestGeneralInterrogationLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err = readAPDU(conn); err != nil {
			return
		}
		writeAPDU(conn, apdu([]byte{0x0b, 0, 0, 0}, nil))
		request, err := readAPDU(conn)
		if err != nil {
			return
		}
		if !bytes.Equal(request[4:], []byte{100, 1, 6, 0, 1, 0, 0, 0, 0, 20}) {
			t.Error("wrong general interrogation")
		}
		asdus := [][]byte{{100, 1, 7, 0, 1, 0, 0, 0, 0, 20}, {1, 1, 20, 0, 1, 0, 100, 0, 0, 1}, {100, 1, 10, 0, 1, 0, 0, 0, 0, 20}}
		for i, a := range asdus {
			control := make([]byte, 4)
			binary.LittleEndian.PutUint16(control, uint16(i<<1))
			binary.LittleEndian.PutUint16(control[2:], 2)
			if writeAPDU(conn, apdu(control, a)) != nil {
				return
			}
			if _, err = readAPDU(conn); err != nil {
				return
			}
		}
		io.Copy(io.Discard, conn)
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(config{Host: host, Port: n, TimeoutMS: 1000, CommonAddress: 1, TimeZone: "UTC"})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"status","ioa":100},{"name":"missing","ioa":101}]}`)})
	if err != nil || !bytes.Contains(response.Payload, []byte(`"status":true`)) || !bytes.Contains(response.Payload, []byte(`"missing":"bad:missing"`)) {
		t.Fatal(string(response.Payload), err)
	}
}
