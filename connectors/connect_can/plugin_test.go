package main

import (
	"context"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockFactory struct {
	mu                      sync.Mutex
	devices                 []*mockDevice
	opened, active, written atomic.Int32
	identity                string
}
type mockDevice struct {
	factory  *mockFactory
	queue    chan canFrame
	done     chan struct{}
	once     sync.Once
	identity string
}

func (f *mockFactory) open(config) (canDevice, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := &mockDevice{factory: f, queue: make(chan canFrame, 256), done: make(chan struct{}), identity: f.identity}
	f.devices = append(f.devices, d)
	f.opened.Add(1)
	f.active.Add(1)
	return d, nil
}
func (f *mockFactory) last() *mockDevice {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.devices[len(f.devices)-1]
}
func (d *mockDevice) Identity() string { return d.identity }
func (d *mockDevice) Read(ctx context.Context) (canFrame, error) {
	select {
	case <-ctx.Done():
		return canFrame{}, ctx.Err()
	case <-d.done:
		return canFrame{}, errors.New("mock socket lost")
	case f := <-d.queue:
		return f, nil
	}
}
func (d *mockDevice) Write(ctx context.Context, f canFrame) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if f.expiresAt <= time.Now().UnixMilli() {
		return errors.New("mock expired write")
	}
	d.factory.written.Add(1)
	return nil
}
func (d *mockDevice) Close() error {
	d.once.Do(func() { close(d.done); d.factory.active.Add(-1) })
	return nil
}
func fixture(t *testing.T) (*connector, string, *mockFactory) {
	t.Helper()
	factory := &mockFactory{identity: "1"}
	c := NewConnector().(*connector)
	c.open = factory.open
	raw := json.RawMessage(`{"interface":"fixture-can","read_ids":[{"id":"0x123"},{"id":"0x123","extended":true}],"write_ids":[{"id":"0x123"}],"allow_write":true,"can_fd":true,"include_error_frames":true}`)
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "can", Connection: raw})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop() })
	return c, h, factory
}
func subscribe(t *testing.T, c *connector, h, key string, extended bool) string {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"ids": []identifier{{ID: "0x123", Extended: extended}}, "include_errors": true, "include_remote": true})
	reply, err := c.OpenStream(h, sdk.StreamOpenRequest{SessionKey: key, Target: "frames", Payload: payload, InitialCredits: 8, MaxBufferedMessages: 64, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
	if err != nil {
		t.Fatal(err)
	}
	return reply.StreamID
}
func receive(t *testing.T, c *connector, h, id string) sdk.StreamMessage {
	t.Helper()
	reply, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 1000})
	if err != nil || len(reply.Messages) != 1 {
		t.Fatal("expected one CAN message", err, reply)
	}
	return reply.Messages[0]
}
func ack(t *testing.T, c *connector, h string, m sdk.StreamMessage) {
	t.Helper()
	if err := c.AckStream(h, sdk.StreamAckRequest{StreamID: m.StreamID, DeliveryID: m.DeliveryID, Credits: 1}); err != nil {
		t.Fatal(err)
	}
}
func TestSharedCANBusFiltersOwnershipAndDemandClose(t *testing.T) {
	c, h, f := fixture(t)
	if f.opened.Load() != 0 {
		t.Fatal("resource creation opened device")
	}
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "duplicate", Connection: json.RawMessage(`{"interface":"fixture-can","read_ids":[{"id":"1"}]}`)}); err == nil {
		t.Fatal("duplicate physical interface allowed")
	}
	a := subscribe(t, c, h, "a", false)
	b := subscribe(t, c, h, "b", true)
	if f.opened.Load() != 1 || f.active.Load() != 1 {
		t.Fatal("multiple sessions", f.opened.Load())
	}
	f.last().queue <- canFrame{id: 0x123, data: []byte{1}}
	ma := receive(t, c, h, a)
	if string(ma.RawPayload) != "\x01" || ma.Metadata["extended"] != "false" {
		t.Fatal(ma)
	}
	ma.RawPayload[0] = 99
	ack(t, c, h, ma)
	if c.AckStream(h, sdk.StreamAckRequest{StreamID: a, DeliveryID: ma.DeliveryID, Credits: 1}) == nil {
		t.Fatal("duplicate ACK increased credit")
	}
	f.last().queue <- canFrame{id: 0x123, extended: true, data: []byte{2}}
	mb := receive(t, c, h, b)
	if mb.Metadata["extended"] != "true" || mb.RawPayload[0] != 2 {
		t.Fatal(mb)
	}
	ack(t, c, h, mb)
	if err := c.CloseStream(h, sdk.StreamCloseRequest{StreamID: a}); err != nil {
		t.Fatal(err)
	}
	if f.active.Load() != 1 {
		t.Fatal("stopping a closed shared socket")
	}
	f.last().queue <- canFrame{id: 0x123, extended: true, data: []byte{3}}
	mb = receive(t, c, h, b)
	ack(t, c, h, mb)
	c.CloseStream(h, sdk.StreamCloseRequest{StreamID: b})
	if f.active.Load() != 0 {
		t.Fatal("no-demand socket retained")
	}
}
func TestSlowCANConsumerCannotOverflowFastConsumer(t *testing.T) {
	c, h, f := fixture(t)
	slow := subscribe(t, c, h, "slow", false)
	fast := subscribe(t, c, h, "fast", false)
	for i := 0; i < 65; i++ {
		f.last().queue <- canFrame{id: 0x123, data: []byte{byte(i)}}
		m := receive(t, c, h, fast)
		ack(t, c, h, m)
	}
	if _, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: slow, MaxMessages: 1, WaitTimeoutMS: 1}); err == nil {
		t.Fatal("slow overflow not reported")
	}
	f.last().queue <- canFrame{id: 0x123, data: []byte{9}, dropDelta: 5}
	m := receive(t, c, h, fast)
	if m.Metadata["rx_dropped_total"] != "5" || m.Metadata["rx_dropped_delta"] != "5" {
		t.Fatal(m)
	}
	ack(t, c, h, m)
	if f.active.Load() != 1 {
		t.Fatal("fast consumer lost session")
	}
}
func TestCANDropAndErrorQualityMetadata(t *testing.T) {
	c, h, f := fixture(t)
	id := subscribe(t, c, h, "errors", false)
	f.last().queue <- canFrame{id: 4, busError: true, data: make([]byte, 8)}
	m := receive(t, c, h, id)
	if m.Metadata["bus_error"] != "true" {
		t.Fatal(m)
	}
	status, err := c.Probe(h, sdk.ConnectorRequest{})
	if err != nil || status.Status != sdk.ResourceStatusDegraded {
		t.Fatal(status, err)
	}
	f.last().queue <- canFrame{id: 0x123, data: []byte{1}}
	m = receive(t, c, h, id)
	ack(t, c, h, m)
	status, _ = c.Probe(h, sdk.ConnectorRequest{})
	if status.Status != sdk.ResourceStatusHealthy {
		t.Fatal(status)
	}
}
func TestCANCircuitHasNoBackgroundReopenAndIdentityChangeRejected(t *testing.T) {
	c, h, f := fixture(t)
	id := subscribe(t, c, h, "source", false)
	f.last().Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 10})
		if err != nil {
			break
		}
	}
	if f.opened.Load() != 1 {
		t.Fatal("background reopen")
	}
	c.CloseStream(h, sdk.StreamCloseRequest{StreamID: id})
	f.mu.Lock()
	f.identity = "2"
	f.mu.Unlock()
	payload := json.RawMessage(`{"ids":[{"id":"0x123"}]}`)
	if _, err := c.OpenStream(h, sdk.StreamOpenRequest{SessionKey: "new", Target: "frames", Payload: payload, InitialCredits: 1, MaxBufferedMessages: 64, OverflowPolicy: "fail_session"}); err == nil {
		t.Fatal("interface identity replacement accepted")
	}
	if f.active.Load() != 0 {
		t.Fatal("rejected interface leaked")
	}
}
func TestCANExpiryAndAllowlistBeforeDeviceAccess(t *testing.T) {
	c, h, f := fixture(t)
	for _, request := range []map[string]any{{"frame": frameSpec{ID: "0x124", Data: []byte{1}}, "expires_at": time.Now().Add(time.Second).UnixMilli()}, {"frame": frameSpec{ID: "0x123", Data: []byte{1}}, "expires_at": int64(1)}} {
		raw, _ := json.Marshal(request)
		if _, err := c.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "send", Payload: raw}); err == nil {
			t.Fatal("unsafe write accepted")
		}
	}
	if f.opened.Load() != 0 || f.written.Load() != 0 {
		t.Fatal("rejected command touched bus")
	}
	raw, _ := json.Marshal(map[string]any{"frame": frameSpec{ID: "0x123", Data: []byte{1}}, "expires_at": time.Now().Add(time.Second).UnixMilli()})
	if _, err := c.Execute(h, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestWrite, Target: "send", Payload: raw}); err != nil {
		t.Fatal(err)
	}
	if f.written.Load() != 1 || f.active.Load() != 0 {
		t.Fatal("standalone send retained session or retried")
	}
}
func TestResourceStopInterruptsCANReceive(t *testing.T) {
	c, h, _ := fixture(t)
	id := subscribe(t, c, h, "waiting", false)
	done := make(chan error, 1)
	go func() {
		_, err := c.ReceiveStream(h, sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 30000})
		done <- err
	}()
	c.DestroyResource(h)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stop not reported")
		}
	case <-time.After(time.Second):
		t.Fatal("receive stuck after resource destroy")
	}
}
