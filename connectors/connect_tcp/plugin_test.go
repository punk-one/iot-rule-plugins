package main

import (
	"bytes"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestFramingBoundsAndPartialReads(t *testing.T) {
	for _, tc := range []struct {
		raw  []byte
		f    framing
		want string
	}{{[]byte("abc!ignored"), framing{Mode: "delimiter", Delimiter: []byte("!")}, "abc"}, {[]byte{0, 5, 'a', 'b', 'c'}, framing{Mode: "length_prefix", Width: 2, Endian: "big"}, string([]byte{0, 5, 'a', 'b', 'c'})}, {[]byte("abc"), framing{Mode: "fixed", Length: 3}, "abc"}} {
		got, err := readFrame(bytes.NewReader(tc.raw), tc.f, 32)
		if err != nil || string(got) != tc.want {
			t.Fatalf("%v %q", err, got)
		}
	}
	if _, err := readFrame(bytes.NewReader([]byte{255}), framing{Mode: "length_prefix", Width: 1, Endian: "big"}, 32); err == nil {
		t.Fatal("unbounded declared length")
	}
	if _, err := readFrame(bytes.NewReader([]byte("aaaa")), framing{Mode: "delimiter", Delimiter: []byte("!")}, 4); err == nil {
		t.Fatal("unterminated oversized frame")
	}
}
func TestTCPOperationAndCancellation(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		io.ReadFull(conn, buf)
		conn.Write([]byte("ok!"))
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	n, _ := strconv.Atoi(port)
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"host": host, "port": n, "timeout_ms": 100})
	if _, err = c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(request{Data: []byte("?"), Frame: framing{Mode: "delimiter", Delimiter: []byte("!")}})
	out, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "exchange", Payload: raw})
	if err != nil || !bytes.Contains(out.Payload, []byte("b2s=")) {
		t.Fatal(string(out.Payload), err)
	}
	<-finished
	done := make(chan error, 1)
	go func() {
		_, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "exchange", Payload: raw})
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	c.DestroyResource("r")
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled operation succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("stop blocked")
	}
}
