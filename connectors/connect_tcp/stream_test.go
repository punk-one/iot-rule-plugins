package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

func rawOpen(t *testing.T, c *connector, key string, f framing) sdk.StreamOpenResponse {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"frame": f})
	s, e := c.OpenStream("stream", sdk.StreamOpenRequest{SessionKey: key, Target: "frames", Payload: payload, InitialCredits: 1, MaxBufferedMessages: 8, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func rawReceive(t *testing.T, c *connector, s sdk.StreamOpenResponse, want []byte) {
	t.Helper()
	response, e := c.ReceiveStream("stream", sdk.StreamReceiveRequest{StreamID: s.StreamID, MaxMessages: 1, WaitTimeoutMS: 1000})
	if e != nil || len(response.Messages) != 1 || !bytes.Equal(response.Messages[0].RawPayload, want) {
		t.Fatalf("receive %v %v want %q", response, e, want)
	}
	m := response.Messages[0]
	if e = c.AckStream("stream", sdk.StreamAckRequest{StreamID: s.StreamID, DeliveryID: m.DeliveryID, Credits: 1}); e != nil {
		t.Fatal(e)
	}
	if e = c.AckStream("stream", sdk.StreamAckRequest{StreamID: s.StreamID, DeliveryID: m.DeliveryID, Credits: 1}); e == nil {
		t.Fatal("duplicate credit accepted")
	}
	m.RawPayload[0] ^= 255
}
func exerciseRaw(t *testing.T, c *connector, f framing, send func([]byte)) {
	t.Helper()
	a := rawOpen(t, c, "a", f)
	b := rawOpen(t, c, "b", f)
	send([]byte("one!two!"))
	if f.Mode == "datagram" {
		rawReceive(t, c, a, []byte("one!two!"))
		rawReceive(t, c, b, []byte("one!two!"))
	} else {
		for _, s := range []sdk.StreamOpenResponse{a, b} {
			rawReceive(t, c, s, []byte("one"))
			rawReceive(t, c, s, []byte("two"))
		}
	}
	for i := 0; i < 9; i++ {
		frame := []byte(fmt.Sprintf("%d!", i))
		send(frame)
		expected := frame
		if f.Mode != "datagram" {
			expected = frame[:len(frame)-1]
		}
		rawReceive(t, c, a, expected)
	}
	_, e := c.ReceiveStream("stream", sdk.StreamReceiveRequest{StreamID: b.StreamID, MaxMessages: 1, WaitTimeoutMS: 20})
	if e == nil {
		t.Fatal("slow consumer overflow silently accepted")
	}
	if e = c.CloseStream("stream", sdk.StreamCloseRequest{StreamID: b.StreamID}); e != nil {
		t.Fatal(e)
	}
	send([]byte("alive!"))
	expected := []byte("alive!")
	if f.Mode != "datagram" {
		expected = []byte("alive")
	}
	rawReceive(t, c, a, expected)
	r, _ := c.get("stream")
	r.streamMu.Lock()
	opens := r.opens
	queued := r.queuedBytes
	r.streamMu.Unlock()
	if opens != 1 || queued != 0 {
		t.Fatal("sharing or queue leak", opens, queued)
	}
	if _, e = c.Execute("stream", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Target: "exchange", Payload: []byte(`{}`)}); e == nil {
		t.Fatal("poll competed with continuous reader")
	}
	done := make(chan error, 1)
	go func() { done <- c.CloseStream("stream", sdk.StreamCloseRequest{StreamID: a.StreamID}) }()
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("last consumer did not close promptly")
	}
	r.streamMu.Lock()
	active := r.active
	r.streamMu.Unlock()
	if active != nil {
		t.Fatal("session survived no demand")
	}
	reopened := rawOpen(t, c, "new-generation", f)
	if err := c.CloseStream("stream", sdk.StreamCloseRequest{StreamID: a.StreamID}); err != nil {
		t.Fatal(err)
	}
	r.streamMu.Lock()
	newActive := r.active
	opens = r.opens
	r.streamMu.Unlock()
	if newActive == nil || opens != 2 {
		t.Fatal("late close retired a new session", opens)
	}
	if err := c.CloseStream("stream", sdk.StreamCloseRequest{StreamID: reopened.StreamID}); err != nil {
		t.Fatal(err)
	}
}
