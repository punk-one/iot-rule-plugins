package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestUDPSharedContinuousDatagrams(t *testing.T) {
	server, e := net.ListenPacket("udp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	addresses := make(chan net.Addr, 1)
	go func() {
		raw := make([]byte, 16)
		_, addr, e := server.ReadFrom(raw)
		if e == nil {
			addresses <- addr
		}
	}()
	host, port, _ := net.SplitHostPort(server.LocalAddr().String())
	n, _ := strconv.Atoi(port)
	f := framing{Mode: "datagram"}
	raw, _ := json.Marshal(map[string]any{"host": host, "port": n, "mode": "stream", "stream_frame": f, "stream_request_base64": "aGVsbG8=", "timeout_ms": 500})
	c := NewConnector().(*connector)
	defer c.Stop()
	if _, e = c.CreateResource(sdk.ConnectorResource{ID: "stream", Connection: raw}); e != nil {
		t.Fatal(e)
	}
	var address net.Addr
	exerciseRaw(t, c, f, func(data []byte) {
		if address == nil {
			select {
			case address = <-addresses:
			case <-time.After(time.Second):
				t.Fatal("greeting timeout")
			}
		}
		if _, e := server.WriteTo(data, address); e != nil {
			t.Fatal(e)
		}
	})
}
