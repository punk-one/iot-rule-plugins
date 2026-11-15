package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync/atomic"
	"testing"
	"time"
)

type testSubscription struct{ closed atomic.Int32 }

func (s *testSubscription) Cancel(context.Context) error { s.closed.Add(1); return nil }
func (s *testSubscription) Monitor(_ context.Context, _ ua.TimestampsToReturn, items ...*ua.MonitoredItemCreateRequest) (*ua.CreateMonitoredItemsResponse, error) {
	r := &ua.CreateMonitoredItemsResponse{}
	for range items {
		r.Results = append(r.Results, &ua.MonitoredItemCreateResult{StatusCode: ua.StatusOK})
	}
	return r, nil
}
func subRequest(id string, max int) sdk.StreamOpenRequest {
	return sdk.StreamOpenRequest{SessionKey: id, Target: "data_changes", Payload: json.RawMessage(`{"points":[{"name":"counter","node_id":"ns=2;s=counter"}],"interval_ms":100}`), InitialCredits: 1, MaxBufferedMessages: max, OverflowPolicy: "fail_session"}
}
func dataNotification(v int64) *opcua.PublishNotificationData {
	return &opcua.PublishNotificationData{Value: &ua.DataChangeNotification{MonitoredItems: []*ua.MonitoredItemNotification{{ClientHandle: 1, Value: &ua.DataValue{Value: ua.MustVariant(v), SourceTimestamp: time.UnixMilli(1794614400000)}}}}}
}
func receive(t *testing.T, c *connector, id string) sdk.StreamMessage {
	t.Helper()
	r, err := c.ReceiveStream("r", sdk.StreamReceiveRequest{StreamID: id, MaxMessages: 1, WaitTimeoutMS: 1000})
	if err != nil || len(r.Messages) != 1 {
		t.Fatalf("messages=%v err=%v", r, err)
	}
	return r.Messages[0]
}
func TestSubscriptionsShareSessionAndStopIndependently(t *testing.T) {
	c := NewConnector().(*connector)
	f := &fakeSession{}
	opens := 0
	c.open = func(context.Context, config) (uaSession, error) { opens++; return f, nil }
	channels := []chan<- *opcua.PublishNotificationData{}
	subs := []*testSubscription{}
	c.subscribe = func(_ context.Context, _ uaSession, _ *opcua.SubscriptionParameters, ch chan<- *opcua.PublishNotificationData) (uaSubscription, error) {
		s := &testSubscription{}
		channels = append(channels, ch)
		subs = append(subs, s)
		return s, nil
	}
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: json.RawMessage(`{"endpoint":"opc.tcp://127.0.0.1:4840","security_mode":"None","session_reuse_ms":0}`)}); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	for _, key := range []string{"a", "b"} {
		if _, err := c.OpenStream("r", subRequest(key, 4)); err != nil {
			t.Fatal(err)
		}
	}
	channels[0] <- dataNotification(9007199254740993)
	m := receive(t, c, "a")
	var decoded struct {
		Values  map[string]any `json:"values"`
		Partial bool           `json:"partial"`
	}
	json.Unmarshal(m.RawPayload, &decoded)
	if decoded.Values["counter"] != "9007199254740993" || !decoded.Partial {
		t.Fatal(string(m.RawPayload))
	}
	if _, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"points":[{"name":"counter","node_id":"ns=2;s=counter"}]}`)}); err != nil {
		t.Fatal(err)
	}
	if f.closed.Load() != 0 {
		t.Fatal("poll retired an actively subscribed session")
	}
	c.CloseStream("r", sdk.StreamCloseRequest{StreamID: "a"})
	if subs[0].closed.Load() != 1 || f.closed.Load() != 0 {
		t.Fatal("one source closed shared session")
	}
	channels[1] <- dataNotification(10)
	receive(t, c, "b")
	if opens != 1 {
		t.Fatal("more than one session")
	}
	c.CloseStream("r", sdk.StreamCloseRequest{StreamID: "b"})
	if f.closed.Load() != 1 {
		t.Fatal("last subscriber did not close session")
	}
}
func TestSlowSubscriptionFailsAndKeepsDraining(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &uaConsumer{notify: make(chan *opcua.PublishNotificationData, 32), done: make(chan struct{}), wake: make(chan struct{}, 1), max: 1, credits: 1, kind: "data_changes", spec: subscriptionSpec{Points: []point{{Name: "counter"}}}}
	go s.loop(ctx)
	defer func() { cancel(); <-s.done }()
	finished := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			s.notify <- dataNotification(int64(i))
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("slow consumer blocked native shared publish loop")
	}
	s.mu.Lock()
	err := s.err
	size := s.bytes
	s.mu.Unlock()
	if err == nil || size != 0 {
		t.Fatal("overflow did not retire bounded consumer")
	}
}
func TestEventFilterAndExplicitValueEncoding(t *testing.T) {
	request := eventMonitor(ua.NewNumericNodeID(0, 2253))
	if request.ItemToMonitor.AttributeID != ua.AttributeIDEventNotifier || request.RequestedParameters.Filter == nil {
		t.Fatal("event filter missing")
	}
	fields := []*ua.Variant{ua.MustVariant([]byte{1, 2}), ua.MustVariant(ua.NewNumericNodeID(0, 2041)), ua.MustVariant(ua.NewStringNodeID(2, "machine")), ua.MustVariant("machine"), ua.MustVariant(time.UnixMilli(1794614400000)), ua.MustVariant(time.UnixMilli(1794614400001)), ua.MustVariant(&ua.LocalizedText{Locale: "en", Text: "alarm"}), ua.MustVariant(uint16(500))}
	s := &uaConsumer{kind: "events"}
	raw, err := s.encode(&ua.EventNotificationList{Events: []*ua.EventFieldList{{ClientHandle: 1, EventFields: fields}}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Events []map[string]any `json:"events"`
	}
	json.Unmarshal(raw, &decoded)
	if decoded.Events[0]["EventId"] != "AQI=" || decoded.Events[0]["Message"].(map[string]any)["text"] != "alarm" {
		t.Fatal(string(raw))
	}
	if _, err = s.encode(&ua.EventNotificationList{Events: []*ua.EventFieldList{{ClientHandle: 2, EventFields: fields}}}); err == nil {
		t.Fatal("foreign client handle accepted")
	}
	if _, err = s.encode(&ua.StatusChangeNotification{}); err == nil {
		t.Fatal("status change did not trigger rebuild")
	}
}
func TestQualityNeverMasksOverflowOrUncertain(t *testing.T) {
	for _, status := range []ua.StatusCode{ua.StatusCode(0x480), ua.StatusCode(0x40000000), ua.StatusBadOutOfRange} {
		_, quality, _ := qualityValue(&ua.DataValue{Status: status, Value: ua.MustVariant(float64(12))})
		if quality == "good" {
			t.Fatal("bad/overflow value labelled good")
		}
	}
}

type namespaceSession struct {
	fakeSession
	names []string
	reads int
	last  uint16
}

func (s *namespaceSession) Read(ctx context.Context, r *ua.ReadRequest) (*ua.ReadResponse, error) {
	if len(r.NodesToRead) == 1 && r.NodesToRead[0].NodeID.IntID() == 2255 {
		s.reads++
		return &ua.ReadResponse{Results: []*ua.DataValue{{Value: ua.MustVariant(s.names)}}}, nil
	}
	s.last = r.NodesToRead[0].NodeID.Namespace()
	return s.fakeSession.Read(ctx, r)
}
func TestNamespaceURIResolvedAndRebuiltPerSession(t *testing.T) {
	s := &namespaceSession{names: []string{"http://opcfoundation.org/UA/", "urn:old", "urn:machine"}}
	r := &resource{client: s}
	points := []point{{Name: "value", NodeID: "i=17", NamespaceURI: "urn:machine"}}
	out, err := r.resolvePoints(context.Background(), points)
	if err != nil || out[0].NodeID != "ns=2;i=17" {
		t.Fatal(out, err)
	}
	r.resolvePoints(context.Background(), points)
	if s.reads != 1 {
		t.Fatal("namespace cache not reused")
	}
	r.closeClient()
	r.client = s
	s.names = []string{"http://opcfoundation.org/UA/", "urn:machine"}
	out, err = r.resolvePoints(context.Background(), points)
	if err != nil || out[0].NodeID != "ns=1;i=17" {
		t.Fatal(out, err)
	}
	if points[0].NodeID != "i=17" {
		t.Fatal("caller point mutated")
	}
}
func TestFailedNamespaceArrayCannotLeavePartialCache(t *testing.T) {
	s := &namespaceSession{names: []string{"a", "a"}}
	r := &resource{client: s}
	for range 2 {
		if _, err := r.resolvePoints(context.Background(), []point{{Name: "value", NodeID: "i=17", NamespaceURI: "a"}}); err == nil {
			t.Fatal("ambiguous namespace accepted")
		}
	}
	if r.namespaces != nil {
		t.Fatal("partial namespace map retained")
	}
}
func TestNativeSubscriptionErrorIsVisible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &uaConsumer{notify: make(chan *opcua.PublishNotificationData, 1), done: make(chan struct{}), wake: make(chan struct{}, 1)}
	go s.loop(ctx)
	s.notify <- &opcua.PublishNotificationData{Error: errors.New("offline")}
	select {
	case <-s.wake:
	case <-time.After(time.Second):
		t.Fatal("failure invisible")
	}
	cancel()
	<-s.done
}
