package main

import (
	"context"
	"encoding/json"
	ws "github.com/gorilla/websocket"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedSocketFanoutCreditAndStop(t *testing.T) {
	var connections atomic.Int32
	upgrader := ws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connections.Add(1)
		defer conn.Close()
		for {
			kind, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if conn.WriteMessage(kind, raw) != nil {
				return
			}
		}
	}))
	defer server.Close()
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"url": "ws" + strings.TrimPrefix(server.URL, "http")})
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		if _, err := c.OpenStream("r", sdk.StreamOpenRequest{Target: "messages", SessionKey: key, InitialCredits: 1, MaxBufferedMessages: 2, OverflowPolicy: "fail_session"}); err != nil {
			t.Fatal(err)
		}
	}
	send := func() {
		raw, _ := json.Marshal(map[string]any{"type": "text", "data": []byte(`{"v":1}`)})
		if _, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "send", Payload: raw}); err != nil {
			t.Fatal(err)
		}
	}
	send()
	for _, key := range []string{"a", "b"} {
		response, err := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: key, MaxMessages: 1, WaitTimeoutMS: 1000})
		if err != nil || len(response.Messages) != 1 {
			t.Fatalf("fanout failed: %v", err)
		}
	}
	if connections.Load() != 1 {
		t.Fatal("multiple physical sockets")
	}
	send()
	response, err := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: "a", MaxMessages: 1, WaitTimeoutMS: 20})
	if err != nil || len(response.Messages) != 0 {
		t.Fatal("credits ignored")
	}
	if err = c.GrantCredits("r", sdk.StreamGrantCreditsRequest{StreamID: "a", Credits: 1}); err != nil {
		t.Fatal(err)
	}
	response, err = c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: "a", MaxMessages: 1, WaitTimeoutMS: 1000})
	if err != nil || len(response.Messages) != 1 {
		t.Fatal("queued frame unavailable")
	}
	done := make(chan struct{})
	go func() { c.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stop leaked goroutines")
	}
}

func TestWriteDeadlineIncludesWaitingGates(t *testing.T) {
	c := NewConnector().(*connector)
	defer c.Stop()
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: json.RawMessage(`{"url":"ws://127.0.0.1:9"}`)}); err != nil {
		t.Fatal(err)
	}
	r, _ := c.get("r")
	r.writeGate <- struct{}{}
	started := time.Now()
	_, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "send", TimeoutMS: 20, Payload: json.RawMessage(`{"type":"text","data":"eA=="}`)})
	<-r.writeGate
	if err == nil || time.Since(started) > time.Second {
		t.Fatal("write gate ignored deadline")
	}
	r.dialGate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started = time.Now()
	_, err = r.ensure(ctx)
	<-r.dialGate
	if err == nil || time.Since(started) > time.Second {
		t.Fatal("dial gate ignored deadline")
	}
}
