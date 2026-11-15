package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testBroker struct {
	listener    net.Listener
	wg          sync.WaitGroup
	accepted    atomic.Int32
	acks        chan uint64
	conns       sync.Map
	hangConsume bool
}

func short(s string) []byte { return append([]byte{byte(len(s))}, []byte(s)...) }
func long(s string) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(len(s)))
	return append(out, []byte(s)...)
}
func method(class, id uint16, args []byte) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint16(b, class)
	binary.BigEndian.PutUint16(b[2:], id)
	return append(b, args...)
}
func sendFrame(c net.Conn, kind byte, ch uint16, payload []byte) error {
	b := make([]byte, 7)
	b[0] = kind
	binary.BigEndian.PutUint16(b[1:], ch)
	binary.BigEndian.PutUint32(b[3:], uint32(len(payload)))
	b = append(b, payload...)
	b = append(b, 0xce)
	_, err := c.Write(b)
	return err
}
func readFrame(c net.Conn) (byte, uint16, []byte, error) {
	var h [7]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		return 0, 0, nil, err
	}
	n := binary.BigEndian.Uint32(h[3:])
	if n > 1048576 {
		return 0, 0, nil, fmt.Errorf("test frame too large")
	}
	b := make([]byte, n+1)
	_, err := io.ReadFull(c, b)
	if err != nil {
		return 0, 0, nil, err
	}
	return h[0], binary.BigEndian.Uint16(h[1:]), b[:n], nil
}
func sendMethod(c net.Conn, ch, class, id uint16, args []byte) error {
	return sendFrame(c, 1, ch, method(class, id, args))
}
func contentHeader(size int, id string) []byte {
	b := make([]byte, 14)
	binary.BigEndian.PutUint16(b, 60)
	binary.BigEndian.PutUint64(b[4:], uint64(size))
	if id != "" {
		binary.BigEndian.PutUint16(b[12:], 0x8080)
		b = append(b, short("application/json")...)
		b = append(b, short(id)...)
	}
	return b
}
func newBroker(t *testing.T, hang bool) *testBroker {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &testBroker{listener: l, acks: make(chan uint64, 16), hangConsume: hang}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			b.accepted.Add(1)
			b.conns.Store(c, true)
			b.wg.Add(1)
			go func() { defer b.wg.Done(); defer c.Close(); defer b.conns.Delete(c); b.serve(c) }()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		b.conns.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
		b.wg.Wait()
	})
	return b
}
func (b *testBroker) serve(c net.Conn) {
	c.SetDeadline(time.Now().Add(10 * time.Second))
	var protocol [8]byte
	if _, err := io.ReadFull(c, protocol[:]); err != nil || !bytes.Equal(protocol[:], []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}) {
		return
	}
	start := []byte{0, 9, 0, 0, 0, 0}
	start = append(start, long("PLAIN")...)
	start = append(start, long("en_US")...)
	if sendMethod(c, 0, 10, 10, start) != nil {
		return
	}
	if _, _, _, err := readFrame(c); err != nil {
		return
	}
	tune := make([]byte, 8)
	binary.BigEndian.PutUint16(tune, 64)
	binary.BigEndian.PutUint32(tune[2:], 131072)
	if sendMethod(c, 0, 10, 30, tune) != nil {
		return
	}
	if _, _, _, err := readFrame(c); err != nil {
		return
	}
	if _, _, _, err := readFrame(c); err != nil {
		return
	}
	if sendMethod(c, 0, 10, 41, []byte{0}) != nil {
		return
	}
	type publication struct {
		missing   bool
		remaining uint64
		body      []byte
	}
	pubs := map[uint16]*publication{}
	for {
		kind, ch, p, err := readFrame(c)
		if err != nil {
			return
		}
		if kind == 8 {
			continue
		}
		if kind == 2 {
			if pub := pubs[ch]; pub != nil && len(p) >= 12 {
				pub.remaining = binary.BigEndian.Uint64(p[4:])
				if pub.remaining == 0 {
					b.confirm(c, ch, pub.missing)
					delete(pubs, ch)
				}
			}
			continue
		}
		if kind == 3 {
			if pub := pubs[ch]; pub != nil {
				pub.body = append(pub.body, p...)
				if uint64(len(p)) > pub.remaining {
					return
				}
				pub.remaining -= uint64(len(p))
				if pub.remaining == 0 {
					b.confirm(c, ch, pub.missing)
					delete(pubs, ch)
				}
			}
			continue
		}
		if len(p) < 4 {
			return
		}
		class, id := binary.BigEndian.Uint16(p), binary.BigEndian.Uint16(p[2:])
		args := p[4:]
		switch {
		case class == 20 && id == 10:
			sendMethod(c, ch, 20, 11, []byte{0, 0, 0, 0})
		case class == 20 && id == 40:
			sendMethod(c, ch, 20, 41, nil)
		case class == 85 && id == 10:
			sendMethod(c, ch, 85, 11, nil)
		case class == 60 && id == 10:
			sendMethod(c, ch, 60, 11, nil)
		case class == 60 && id == 20:
			if b.hangConsume {
				continue
			}
			if len(args) < 3 {
				return
			}
			offset := 3 + int(args[2])
			if offset >= len(args) {
				return
			}
			tagLen := int(args[offset])
			if offset+1+tagLen > len(args) {
				return
			}
			tag := string(args[offset+1 : offset+1+tagLen])
			sendMethod(c, ch, 60, 21, short(tag))
			deliver := short(tag)
			var number [8]byte
			binary.BigEndian.PutUint64(number[:], 1)
			deliver = append(deliver, number[:]...)
			deliver = append(deliver, 0)
			deliver = append(deliver, short("events")...)
			deliver = append(deliver, short("test")...)
			body := []byte(`{"count":9007199254740993}`)
			sendMethod(c, ch, 60, 60, deliver)
			sendFrame(c, 2, ch, contentHeader(len(body), "producer-001"))
			sendFrame(c, 3, ch, body)
		case class == 60 && id == 40:
			if len(args) < 3 {
				return
			}
			offset := 3 + int(args[2])
			if offset >= len(args) {
				return
			}
			n := int(args[offset])
			if offset+1+n > len(args) {
				return
			}
			key := string(args[offset+1 : offset+1+n])
			pubs[ch] = &publication{missing: key == "missing"}
		case class == 60 && id == 80:
			if len(args) >= 8 {
				select {
				case b.acks <- binary.BigEndian.Uint64(args):
				default:
				}
			}
		case class == 60 && id == 120:
		case class == 10 && id == 50:
			sendMethod(c, 0, 10, 51, nil)
			return
		}
	}
}
func (b *testBroker) confirm(c net.Conn, ch uint16, missing bool) {
	if missing {
		args := []byte{1, 56}
		args = append(args, short("NO_ROUTE")...)
		args = append(args, short("events")...)
		args = append(args, short("missing")...)
		sendMethod(c, ch, 60, 50, args)
		sendFrame(c, 2, ch, contentHeader(2, ""))
		sendFrame(c, 3, ch, []byte(`{}`))
	}
	var args [9]byte
	binary.BigEndian.PutUint64(args[:], 1)
	sendMethod(c, ch, 60, 80, args[:])
}
func brokerResource(t *testing.T, b *testBroker, timeout int) (*connector, string) {
	t.Helper()
	c := NewConnector().(*connector)
	cfg, _ := json.Marshal(config{Address: b.listener.Addr().String(), Username: "synthetic", Password: "synthetic-test-only", VHost: "/", TimeoutMS: timeout, MaxMessageBytes: 65536, AllowWrite: true, Exchanges: []string{"events"}})
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "broker", Connection: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop() })
	return c, h
}
func TestSharedConnectionManualAckAndPublisherConfirms(t *testing.T) {
	b := newBroker(t, false)
	c, h := brokerResource(t, b, 2000)
	for _, id := range []string{"rule-a", "rule-b"} {
		if _, err := c.OpenStream(h, sdk.StreamOpenRequest{SessionKey: id, Target: "q", InitialCredits: 1, MaxBufferedMessages: 1, OverflowPolicy: "fail_session"}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: "rule-a", MaxMessages: 1, WaitTimeoutMS: 1000})
	if err != nil || len(result.Messages) != 1 {
		t.Fatalf("%v %v", result, err)
	}
	if result.Messages[0].Metadata["message_id"] != "producer-001" {
		t.Fatal(result.Messages)
	}
	select {
	case <-b.acks:
		t.Fatal("ACK before engine handoff")
	default:
	}
	if err = c.AckStream(h, sdk.StreamAckRequest{StreamID: "rule-a", DeliveryID: "1", Credits: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.acks:
	case <-time.After(time.Second):
		t.Fatal("native ACK missing")
	}
	if err = c.AckStream(h, sdk.StreamAckRequest{StreamID: "rule-a", DeliveryID: "1", Credits: 1}); err == nil {
		t.Fatal("duplicate ACK accepted")
	}
	for _, key := range []string{"ok", "missing"} {
		raw, _ := json.Marshal(publishSpec{Exchange: "events", RoutingKey: key, Body: []byte(`{"x":1}`), MessageID: "event-001"})
		out, err := c.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "publish", Payload: raw})
		if key == "missing" {
			if err == nil {
				t.Fatal("unroutable confirmed as delivered")
			}
		} else if err != nil || !bytes.Contains(out.Payload, []byte(`"broker_confirmed":true`)) {
			t.Fatalf("publish=%s err=%v", out.Payload, err)
		}
	}
	if b.accepted.Load() != 1 {
		t.Fatalf("physical connections=%d", b.accepted.Load())
	}
	if err = c.CloseStream(h, sdk.StreamCloseRequest{StreamID: "rule-a"}); err != nil {
		t.Fatal(err)
	}
	out, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: "rule-b", MaxMessages: 1, WaitTimeoutMS: 1000})
	if err != nil || len(out.Messages) != 1 {
		t.Fatal("stopping one consumer affected another", out, err)
	}
}
func TestConsumeDeadlineInterruptsProtocolRPC(t *testing.T) {
	b := newBroker(t, true)
	c, h := brokerResource(t, b, 100)
	start := time.Now()
	if _, err := c.OpenStream(h, sdk.StreamOpenRequest{SessionKey: "blocked", Target: "q", InitialCredits: 1, MaxBufferedMessages: 1, OverflowPolicy: "fail_session"}); err == nil {
		t.Fatal("missing consume reply accepted")
	}
	if time.Since(start) > time.Second {
		t.Fatal("deadline did not interrupt AMQP RPC")
	}
	c.Stop()
}
func TestRejectInsecureRemoteCredentialsAndUnboundedConsumer(t *testing.T) {
	c := NewConnector().(*connector)
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: json.RawMessage(`{"address":"192.0.2.10:5672","tls":false,"username":"test","password":"test"}`)}); err == nil {
		t.Fatal("remote plaintext credentials accepted")
	}
	c.Stop()
}
func TestBodyBoundBeforeLibraryAllocation(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() { defer b.Close(); sendFrame(b, 2, 1, contentHeader(1048576, "")) }()
	limited := &frameConn{Conn: a, maxBody: 1024}
	if _, err := io.ReadAll(limited); err == nil {
		t.Fatal("oversized advertised message accepted")
	}
}
func TestFrameBoundWithoutReadingAdvertisedHugeBody(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go func() {
		defer b.Close()
		var h [7]byte
		h[0] = 3
		binary.BigEndian.PutUint32(h[3:], 0xffffffff)
		b.Write(h[:])
	}()
	limited := &frameConn{Conn: a, maxBody: 1024}
	var dst [1]byte
	if _, err := limited.Read(dst[:]); err == nil {
		t.Fatal("oversized frame accepted")
	}
}
