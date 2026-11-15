package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type streamPeer struct {
	listener  net.Listener
	conn      net.Conn
	mu        sync.Mutex
	ready     chan struct{}
	done      chan struct{}
	replyTest bool
	tests     atomic.Int64
	sequence  uint16
}

func newStreamPeer(t *testing.T, replyTest bool) *streamPeer {
	t.Helper()
	ln, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	p := &streamPeer{listener: ln, ready: make(chan struct{}), done: make(chan struct{}), replyTest: replyTest}
	go func() {
		defer close(p.done)
		conn, e := ln.Accept()
		if e != nil {
			return
		}
		p.mu.Lock()
		p.conn = conn
		p.mu.Unlock()
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(10 * time.Second))
		raw, e := readAPDU(conn)
		if e != nil || !bytes.Equal(raw, []byte{7, 0, 0, 0}) {
			return
		}
		writeAPDU(conn, apdu([]byte{11, 0, 0, 0}, nil))
		close(p.ready)
		for {
			raw, e = readAPDU(conn)
			if e != nil {
				return
			}
			if bytes.Equal(raw, []byte{0x43, 0, 0, 0}) {
				p.tests.Add(1)
				if p.replyTest {
					p.sendControl([]byte{0x83, 0, 0, 0})
				}
			}
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		if p.conn != nil {
			p.conn.Close()
		}
		p.mu.Unlock()
		select {
		case <-p.done:
		case <-time.After(time.Second):
			t.Error("peer did not stop")
		}
	})
	return p
}
func (p *streamPeer) sendControl(control []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		_ = writeAPDU(p.conn, apdu(control, nil))
	}
}
func (p *streamPeer) sendPoint(t *testing.T, sequence *uint16) {
	t.Helper()
	select {
	case <-p.ready:
	case <-time.After(time.Second):
		t.Fatal("peer not connected")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.sequence
	if sequence != nil {
		n = *sequence
	}
	control := make([]byte, 4)
	binary.LittleEndian.PutUint16(control, n<<1)
	if e := writeAPDU(p.conn, apdu(control, []byte{1, 1, 3, 0, 1, 0, 100, 0, 0, 1})); e != nil {
		t.Fatal(e)
	}
	p.sequence = (p.sequence + 1) & 0x7fff
}
func newStreamConnector(t *testing.T, p *streamPeer, t1, t3 int) *connector {
	t.Helper()
	host, port, _ := net.SplitHostPort(p.listener.Addr().String())
	n, _ := strconv.Atoi(port)
	raw, _ := json.Marshal(map[string]any{"host": host, "port": n, "mode": "stream", "read_ioas": []uint32{100}, "general_interrogation_on_connect": false, "timeout_ms": 1000, "t1_ms": t1, "t3_ms": t3})
	c := NewConnector().(*connector)
	if _, e := c.CreateResource(sdk.ConnectorResource{ID: "station", Connection: raw}); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Stop() })
	return c
}
func openSelection(t *testing.T, c *connector, key, name string, ioa uint32) sdk.StreamOpenResponse {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"points": []point{{Name: name, IOA: ioa}}})
	s, e := c.OpenStream("station", sdk.StreamOpenRequest{SessionKey: key, Target: "events", Payload: raw, InitialCredits: 1, MaxBufferedMessages: 8, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func receiveSelection(t *testing.T, c *connector, s sdk.StreamOpenResponse, name string) {
	t.Helper()
	response, e := c.ReceiveStream("station", sdk.StreamReceiveRequest{StreamID: s.StreamID, MaxMessages: 1, WaitTimeoutMS: 1000})
	if e != nil || len(response.Messages) != 1 {
		t.Fatal(response, e)
	}
	var frame iecFrame
	if json.Unmarshal(response.Messages[0].RawPayload, &frame) != nil || frame.Values[name] != true || frame.Quality[name] != "good" || frame.Cause != 3 || len(frame.Values) != 1 {
		t.Fatal(string(response.Messages[0].RawPayload))
	}
	response.Messages[0].RawPayload[0] ^= 255
	if e = c.AckStream("station", sdk.StreamAckRequest{StreamID: s.StreamID, DeliveryID: response.Messages[0].DeliveryID, Credits: 1}); e != nil {
		t.Fatal(e)
	}
}
func TestIEC104ContinuousSharingSelectionAndStop(t *testing.T) {
	peer := newStreamPeer(t, true)
	c := newStreamConnector(t, peer, 1000, 2000)
	a := openSelection(t, c, "a", "first", 100)
	b := openSelection(t, c, "b", "second", 100)
	peer.sendPoint(t, nil)
	receiveSelection(t, c, a, "first")
	receiveSelection(t, c, b, "second")
	if e := c.CloseStream("station", sdk.StreamCloseRequest{StreamID: a.StreamID}); e != nil {
		t.Fatal(e)
	}
	peer.sendPoint(t, nil)
	receiveSelection(t, c, b, "second")
	r, _ := c.get("station")
	r.streamMu.Lock()
	opens := r.opens
	active := r.active != nil
	r.streamMu.Unlock()
	if opens != 1 || !active {
		t.Fatal("shared station session not preserved", opens, active)
	}
	if e := c.CloseStream("station", sdk.StreamCloseRequest{StreamID: b.StreamID}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-peer.done:
	case <-time.After(time.Second):
		t.Fatal("last-demand socket stayed open")
	}
}
func TestIEC104ContinuousSlowConsumerAndWireFailure(t *testing.T) {
	peer := newStreamPeer(t, true)
	c := newStreamConnector(t, peer, 1000, 2000)
	a := openSelection(t, c, "a", "first", 100)
	b := openSelection(t, c, "b", "second", 100)
	for i := 0; i < 9; i++ {
		peer.sendPoint(t, nil)
		receiveSelection(t, c, a, "first")
	}
	if _, e := c.ReceiveStream("station", sdk.StreamReceiveRequest{StreamID: b.StreamID, MaxMessages: 1, WaitTimeoutMS: 20}); e == nil {
		t.Fatal("overflow hidden")
	}
	wrong := uint16(100)
	peer.sendPoint(t, &wrong)
	if _, e := c.ReceiveStream("station", sdk.StreamReceiveRequest{StreamID: a.StreamID, MaxMessages: 1, WaitTimeoutMS: 1000}); e == nil {
		t.Fatal("wrong APDU sequence accepted")
	}
	select {
	case <-peer.done:
	case <-time.After(time.Second):
		t.Fatal("broken socket not closed")
	}
	c.CloseStream("station", sdk.StreamCloseRequest{StreamID: a.StreamID})
	c.CloseStream("station", sdk.StreamCloseRequest{StreamID: b.StreamID})
	start := time.Now()
	raw, _ := json.Marshal(map[string]any{"points": []point{{Name: "again", IOA: 100}}})
	if _, e := c.OpenStream("station", sdk.StreamOpenRequest{SessionKey: "new", Target: "events", Payload: raw, InitialCredits: 1, MaxBufferedMessages: 8, OverflowPolicy: "fail_session"}); e == nil || time.Since(start) > 100*time.Millisecond {
		t.Fatal("failed session dial backoff missing", e)
	}
}
func TestIEC104IdleTestFramesAndTimeout(t *testing.T) {
	for _, reply := range []bool{true, false} {
		t.Run(strconv.FormatBool(reply), func(t *testing.T) {
			peer := newStreamPeer(t, reply)
			c := newStreamConnector(t, peer, 100, 200)
			s := openSelection(t, c, "a", "value", 100)
			if reply {
				time.Sleep(480 * time.Millisecond)
				response, e := c.ReceiveStream("station", sdk.StreamReceiveRequest{StreamID: s.StreamID, MaxMessages: 1, WaitTimeoutMS: 10})
				if e != nil || len(response.Messages) != 0 || peer.tests.Load() < 2 {
					t.Fatal("healthy idle session failed", e, peer.tests.Load())
				}
			} else {
				if _, e := c.ReceiveStream("station", sdk.StreamReceiveRequest{StreamID: s.StreamID, MaxMessages: 1, WaitTimeoutMS: 1000}); e == nil || peer.tests.Load() != 1 {
					t.Fatal("missing TESTFR confirmation not detected", e, peer.tests.Load())
				}
			}
			c.CloseStream("station", sdk.StreamCloseRequest{StreamID: s.StreamID})
		})
	}
}
func TestIEC104ExtraMonitoringTypesAndBoundaries(t *testing.T) {
	for _, tc := range []struct {
		raw  []byte
		want any
	}{{[]byte{5, 1, 3, 0, 1, 0, 100, 0, 0, 0x7f, 0}, -1}, {[]byte{7, 1, 3, 0, 1, 0, 100, 0, 0, 0xdd, 0xcc, 0xbb, 0xaa, 0}, uint32(0xaabbccdd)}} {
		values, _, e := parseASDU(tc.raw, 1, time.UTC)
		if e != nil || values[100].value != tc.want {
			t.Fatal(values, e)
		}
	}
	d := &iecStreamDevice{sent: 1}
	if d.applyAck([]byte{4, 0}) == nil {
		t.Fatal("future ACK accepted")
	}
	if d.applyAck([]byte{2, 0}) != nil || d.applyAck([]byte{0, 0}) == nil {
		t.Fatal("ACK regression accepted")
	}
	if _, _, e := parseASDU([]byte{70, 1, 4, 0, 1, 0}, 1, time.UTC); e == nil {
		t.Fatal("truncated initialization accepted")
	}
}
func TestIndependentLib60870Continuous(t *testing.T) {
	server := os.Getenv("TEST_IEC104_SERVER")
	if server == "" {
		t.Skip("set TEST_IEC104_SERVER to independently built lib60870 2.4.1 simple_server")
	}
	if os.Getenv("TEST_IEC104_ISOLATED") != "1" {
		t.Fatal("requires private network namespace")
	}
	logFile, e := os.Create(filepath.Join(t.TempDir(), "server.log"))
	if e != nil {
		t.Fatal(e)
	}
	defer logFile.Close()
	cmd := exec.Command(server)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	time.Sleep(150 * time.Millisecond)
	c := NewConnector().(*connector)
	defer c.Stop()
	raw := []byte(`{"host":"127.0.0.1","port":2404,"mode":"stream","read_ioas":[100,110,500],"timeout_ms":1000}`)
	if _, e = c.CreateResource(sdk.ConnectorResource{ID: "station", Connection: raw}); e != nil {
		t.Fatal(e)
	}
	payload, _ := json.Marshal(map[string]any{"points": []point{{Name: "initial", IOA: 100}, {Name: "periodic", IOA: 110}, {Name: "bits", IOA: 500}}})
	opened, e := c.OpenStream("station", sdk.StreamOpenRequest{SessionKey: "native", Target: "events", Payload: payload, InitialCredits: 8, MaxBufferedMessages: 8, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	periodic := map[string]bool{}
	end := time.Now().Add(7 * time.Second)
	for time.Now().Before(end) {
		response, e := c.ReceiveStream("station", sdk.StreamReceiveRequest{StreamID: opened.StreamID, MaxMessages: 1, WaitTimeoutMS: 1000})
		if e != nil {
			t.Fatal(e)
		}
		for _, m := range response.Messages {
			var frame iecFrame
			if json.Unmarshal(m.RawPayload, &frame) != nil {
				t.Fatal(string(m.RawPayload))
			}
			if frame.Values["initial"] == float64(-1) {
				seen["initial"] = true
			}
			if frame.Values["bits"] == float64(0xaaaa) {
				seen["bits"] = true
			}
			if value, ok := frame.Values["periodic"]; ok && frame.Cause == 1 {
				periodic[fmt.Sprint(value)] = true
			}
			if e = c.AckStream("station", sdk.StreamAckRequest{StreamID: opened.StreamID, DeliveryID: m.DeliveryID, Credits: 1}); e != nil {
				t.Fatal(e)
			}
		}
		if seen["initial"] && seen["bits"] && len(periodic) >= 2 {
			break
		}
	}
	if !seen["initial"] || !seen["bits"] || len(periodic) < 2 {
		t.Fatal("independent GI/periodic incomplete", seen, periodic)
	}
	if e = c.CloseStream("station", sdk.StreamCloseRequest{StreamID: opened.StreamID}); e != nil {
		t.Fatal(e)
	}
	t.Log("lib60870 2.4.1 initial GI, bit-string and continuing periodic deltas passed")
}
