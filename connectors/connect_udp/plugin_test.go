package main

import (
	"bytes"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"strconv"
	"testing"
)

func TestUDPDatagramAndBounds(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go func() {
		raw := make([]byte, 32)
		_, address, err := server.ReadFrom(raw)
		if err == nil {
			server.WriteTo([]byte("reply"), address)
		}
	}()
	host, port, _ := net.SplitHostPort(server.LocalAddr().String())
	n, _ := strconv.Atoi(port)
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"host": host, "port": n, "timeout_ms": 100})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(request{Data: []byte("poll"), Frame: framing{Mode: "datagram"}})
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "exchange", Payload: raw})
	if err != nil || !bytes.Contains(response.Payload, []byte("cmVwbHk=")) {
		t.Fatal(string(response.Payload), err)
	}
	if validateFrame(framing{Mode: "fixed", Length: 3}, 32) == nil {
		t.Fatal("stream framing accepted for UDP")
	}
	if _, err = c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "send", Payload: json.RawMessage(`{"data":"eA=="}`)}); err == nil {
		t.Fatal("unapproved write")
	}
}
