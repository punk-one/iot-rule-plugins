package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestTCPSharedContinuousFramingAndOverflow(t *testing.T) {
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, e := listener.Accept()
		if e == nil {
			accepted <- conn
		}
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	n, _ := strconv.Atoi(port)
	f := framing{Mode: "delimiter", Delimiter: []byte("!")}
	raw, _ := json.Marshal(map[string]any{"host": host, "port": n, "mode": "stream", "stream_frame": f, "timeout_ms": 500})
	c := NewConnector().(*connector)
	defer c.Stop()
	if _, e = c.CreateResource(sdk.ConnectorResource{ID: "stream", Connection: raw}); e != nil {
		t.Fatal(e)
	}
	var peer net.Conn
	defer func() {
		if peer != nil {
			peer.Close()
		}
	}()
	exerciseRaw(t, c, f, func(data []byte) {
		if peer == nil {
			select {
			case peer = <-accepted:
			case <-time.After(time.Second):
				t.Fatal("accept timeout")
			}
		}
		peer.SetWriteDeadline(time.Now().Add(time.Second))
		if _, e := peer.Write(data); e != nil {
			t.Fatal(e)
		}
	})
}
