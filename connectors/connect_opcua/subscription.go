package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"strconv"
	"sync"
	"time"
)

type uaSubscription interface {
	Cancel(context.Context) error
	Monitor(context.Context, ua.TimestampsToReturn, ...*ua.MonitoredItemCreateRequest) (*ua.CreateMonitoredItemsResponse, error)
}
type subscribeFunction func(context.Context, uaSession, *opcua.SubscriptionParameters, chan<- *opcua.PublishNotificationData) (uaSubscription, error)

func subscribeNative(ctx context.Context, session uaSession, params *opcua.SubscriptionParameters, notify chan<- *opcua.PublishNotificationData) (uaSubscription, error) {
	client, ok := session.(interface {
		Subscribe(context.Context, *opcua.SubscriptionParameters, chan<- *opcua.PublishNotificationData) (*opcua.Subscription, error)
	})
	if !ok {
		return nil, errors.New("session does not support subscriptions")
	}
	return client.Subscribe(ctx, params, notify)
}

type subscriptionSpec struct {
	Points            []point `json:"points"`
	IntervalMS        int     `json:"interval_ms"`
	EventNodeID       string  `json:"event_node_id"`
	EventNamespaceURI string  `json:"event_namespace_uri"`
}
type uaConsumer struct {
	mu                  sync.Mutex
	sub                 uaSubscription
	notify              chan *opcua.PublishNotificationData
	cancel              context.CancelFunc
	done                chan struct{}
	wake                chan struct{}
	queue               []sdk.StreamMessage
	bytes, credits, max int
	seq                 uint64
	err                 error
	spec                subscriptionSpec
	kind                string
}

var eventFields = []string{"EventId", "EventType", "SourceNode", "SourceName", "Time", "ReceiveTime", "Message", "Severity"}

func eventMonitor(node *ua.NodeID) *ua.MonitoredItemCreateRequest {
	selects := make([]*ua.SimpleAttributeOperand, len(eventFields))
	for i, name := range eventFields {
		selects[i] = &ua.SimpleAttributeOperand{TypeDefinitionID: ua.NewNumericNodeID(0, id.BaseEventType), BrowsePath: []*ua.QualifiedName{{NamespaceIndex: 0, Name: name}}, AttributeID: ua.AttributeIDValue}
	}
	filter := &ua.ExtensionObject{EncodingMask: ua.ExtensionObjectBinary, TypeID: &ua.ExpandedNodeID{NodeID: ua.NewNumericNodeID(0, id.EventFilter_Encoding_DefaultBinary)}, Value: &ua.EventFilter{SelectClauses: selects, WhereClause: &ua.ContentFilter{}}}
	request := opcua.NewMonitoredItemCreateRequestWithDefaults(node, ua.AttributeIDEventNotifier, 1)
	request.RequestedParameters.Filter = filter
	request.RequestedParameters.QueueSize = 32
	return request
}
func validateSubscription(req sdk.StreamOpenRequest) (subscriptionSpec, error) {
	var spec subscriptionSpec
	if req.SessionKey == "" || len(req.SessionKey) > 512 || (req.Target != "data_changes" && req.Target != "events") || len(req.Payload) > 262144 || req.InitialCredits < 1 || req.InitialCredits > 32 || req.MaxBufferedMessages < 1 || req.MaxBufferedMessages > 32 || req.InitialCredits > req.MaxBufferedMessages || req.OverflowPolicy != "fail_session" {
		return spec, errors.New("invalid bounded subscription options")
	}
	d := json.NewDecoder(bytes.NewReader(req.Payload))
	d.DisallowUnknownFields()
	if d.Decode(&spec) != nil {
		return spec, errors.New("invalid subscription payload")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return spec, errors.New("trailing subscription payload")
	}
	if spec.IntervalMS < 50 || spec.IntervalMS > 60000 {
		return spec, errors.New("subscription interval must be 50..60000 ms")
	}
	if req.Target == "data_changes" {
		if len(spec.Points) > 256 {
			return spec, errors.New("subscription point limit 256")
		}
		if err := validatePoints(spec.Points, false); err != nil {
			return spec, err
		}
		if spec.EventNodeID != "" || spec.EventNamespaceURI != "" {
			return spec, errors.New("event fields do not belong to data subscription")
		}
	} else {
		if len(spec.Points) != 0 {
			return spec, errors.New("events do not take point fields")
		}
		if spec.EventNodeID == "" {
			spec.EventNodeID = "i=2253"
		}
		if err := validatePoints([]point{{Name: "events", NodeID: spec.EventNodeID, NamespaceURI: spec.EventNamespaceURI}}, false); err != nil {
			return spec, err
		}
	}
	return spec, nil
}
func (c *connector) OpenStream(h string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return sdk.StreamOpenResponse{}, errors.New("unknown resource")
	}
	spec, err := validateSubscription(req)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case r.gate <- struct{}{}:
	case <-ctx.Done():
		return sdk.StreamOpenResponse{}, ctx.Err()
	}
	defer func() { <-r.gate }()
	if r.ctx.Err() != nil || len(r.streams) >= 16 || r.streams[req.SessionKey] != nil {
		return sdk.StreamOpenResponse{}, errors.New("resource stopped, duplicate stream or 16-stream limit")
	}
	if r.client == nil {
		r.client, err = c.open(ctx, r.cfg)
		if err != nil {
			return sdk.StreamOpenResponse{}, err
		}
	}
	if req.Target == "data_changes" {
		spec.Points, err = r.resolvePoints(ctx, spec.Points)
	} else {
		var points []point
		points, err = r.resolvePoints(ctx, []point{{Name: "events", NodeID: spec.EventNodeID, NamespaceURI: spec.EventNamespaceURI}})
		if err == nil {
			spec.EventNodeID = points[0].NodeID
		}
	}
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	streamCtx, streamCancel := context.WithCancel(r.ctx)
	s := &uaConsumer{notify: make(chan *opcua.PublishNotificationData, 32), cancel: streamCancel, done: make(chan struct{}), wake: make(chan struct{}, 1), credits: req.InitialCredits, max: req.MaxBufferedMessages, spec: spec, kind: req.Target}
	sub, err := c.subscribe(ctx, r.client, &opcua.SubscriptionParameters{Interval: time.Duration(spec.IntervalMS) * time.Millisecond, LifetimeCount: 300, MaxKeepAliveCount: 10, MaxNotificationsPerPublish: 256}, s.notify)
	if err != nil {
		streamCancel()
		r.closeClient()
		return sdk.StreamOpenResponse{}, errors.New("OPC UA subscription creation failed")
	}
	s.sub = sub
	// Drain native notifications before Monitor, which may start publishing immediately.
	go s.loop(streamCtx)
	requests := []*ua.MonitoredItemCreateRequest{}
	if req.Target == "events" {
		node, _ := ua.ParseNodeID(spec.EventNodeID)
		requests = append(requests, eventMonitor(node))
	} else {
		for i, p := range spec.Points {
			node, _ := ua.ParseNodeID(p.NodeID)
			request := opcua.NewMonitoredItemCreateRequestWithDefaults(node, ua.AttributeIDValue, uint32(i+1))
			request.RequestedParameters.QueueSize = 1
			request.RequestedParameters.SamplingInterval = float64(spec.IntervalMS)
			requests = append(requests, request)
		}
	}
	response, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, requests...)
	if err == nil && (response == nil || len(response.Results) != len(requests)) {
		err = errors.New("incomplete monitored item results")
	}
	if err == nil {
		for _, result := range response.Results {
			if result == nil || uint32(result.StatusCode)&0xc0000000 != 0 {
				err = errors.New("monitored item rejected")
				break
			}
		}
	}
	if err != nil {
		closeCtx, done := context.WithTimeout(context.Background(), time.Second)
		_ = sub.Cancel(closeCtx)
		done()
		streamCancel()
		<-s.done
		return sdk.StreamOpenResponse{}, errors.New("OPC UA monitored item setup failed")
	}
	if r.streams == nil {
		r.streams = map[string]*uaConsumer{}
	}
	r.streams[req.SessionKey] = s
	r.lastUse = time.Now()
	return sdk.StreamOpenResponse{StreamID: req.SessionKey, DeliverySemantics: sdk.StreamDeliveryBestEffort, InitialCredits: req.InitialCredits}, nil
}
func (s *uaConsumer) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *uaConsumer) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
		s.queue = nil
		s.bytes = 0
	}
	s.signal()
	s.mu.Unlock()
}
func (s *uaConsumer) loop(ctx context.Context) {
	defer close(s.done)
	for {
		select {
		case <-ctx.Done():
			s.fail(errors.New("subscription closed"))
			return
		case n, ok := <-s.notify:
			if !ok {
				s.fail(errors.New("native notification channel closed"))
				return
			}
			if n == nil {
				continue
			}
			if n.Error != nil {
				s.fail(errors.New("OPC UA subscription lost; recreate required"))
				continue
			}
			s.mu.Lock()
			failed := s.err != nil
			s.mu.Unlock()
			if failed {
				continue
			} // continue draining so a slow consumer cannot block other subscriptions
			raw, err := s.encode(n.Value)
			if err != nil {
				s.fail(err)
				continue
			}
			if len(raw) == 0 {
				continue
			}
			s.mu.Lock()
			if len(s.queue) >= s.max || len(raw) > 1048576 || s.bytes+len(raw) > 1048576 {
				s.err = errors.New("subscription overflow; notifications lost, recreate required")
				s.queue = nil
				s.bytes = 0
				s.signal()
				s.mu.Unlock()
				continue
			}
			s.seq++
			s.queue = append(s.queue, sdk.StreamMessage{DeliveryID: strconv.FormatUint(s.seq, 10), Sequence: s.seq, RawPayload: raw, Metadata: map[string]string{"kind": s.kind}, PublishedAtUnixNano: time.Now().UnixNano()})
			s.bytes += len(raw)
			s.signal()
			s.mu.Unlock()
		}
	}
}
func qualityValue(value *ua.DataValue) (any, string, int64) {
	if value == nil || value.Value == nil {
		return nil, "bad:missing", 0
	}
	at := int64(0)
	if !value.SourceTimestamp.IsZero() {
		at = value.SourceTimestamp.UnixMilli()
	}
	status := uint32(value.Status)
	if status&0xc0000000 != 0 {
		return nil, fmt.Sprintf("bad:0x%08x", status), at
	}
	if status&0x480 == 0x480 {
		return nil, "bad:overflow", at
	}
	normalized, err := serializable(value.Value.Value(), 0)
	if err != nil {
		return nil, "bad:unsupported_value", at
	}
	return normalized, "good", at
}
func (s *uaConsumer) encode(value any) ([]byte, error) {
	var result any
	switch n := value.(type) {
	case *ua.DataChangeNotification:
		if s.kind != "data_changes" || n == nil || len(n.MonitoredItems) > 256 {
			return nil, errors.New("invalid data notification")
		}
		values := map[string]any{}
		quality := map[string]string{}
		times := map[string]int64{}
		for _, item := range n.MonitoredItems {
			if item == nil || item.ClientHandle == 0 || int(item.ClientHandle) > len(s.spec.Points) {
				return nil, errors.New("invalid monitored client handle")
			}
			name := s.spec.Points[item.ClientHandle-1].Name

			v, q, at := qualityValue(item.Value)
			values[name] = v
			quality[name] = q
			if at != 0 {
				times[name] = at
			}
		}
		result = map[string]any{"values": values, "quality": quality, "source_time": times, "partial": true}
	case *ua.EventNotificationList:
		if s.kind != "events" || n == nil || len(n.Events) > 256 {
			return nil, errors.New("invalid event notification")
		}
		events := []map[string]any{}
		for _, item := range n.Events {
			if item == nil || item.ClientHandle != 1 || len(item.EventFields) != len(eventFields) {
				return nil, errors.New("invalid event field count")
			}
			fields := map[string]any{}
			for i, v := range item.EventFields {
				if v == nil {
					fields[eventFields[i]] = nil
					continue
				}
				converted, err := eventValue(v.Value())
				if err != nil {
					return nil, err
				}
				fields[eventFields[i]] = converted
			}
			events = append(events, fields)
		}
		result = map[string]any{"events": events}
	case *ua.StatusChangeNotification:
		return nil, errors.New("OPC UA subscription status changed; recreate required")
	default:
		return nil, errors.New("unsupported subscription notification")
	}
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > 1048576 {
		return nil, errors.New("notification result exceeds 1 MiB or is not JSON compatible")
	}
	return raw, nil
}
func eventValue(v any) (any, error) {
	switch value := v.(type) {
	case *ua.NodeID:
		if value == nil {
			return nil, nil
		}
		return value.String(), nil
	case *ua.LocalizedText:
		if value == nil {
			return nil, nil
		}
		if len(value.Text) > 65536 {
			return nil, errors.New("event message too large")
		}
		return map[string]any{"locale": value.Locale, "text": value.Text}, nil
	default:
		return serializable(v, 0)
	}
}
func (c *connector) findStream(h, id string) (*resource, *uaConsumer, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return nil, nil, errors.New("unknown resource")
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case r.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	s := r.streams[id]
	<-r.gate
	if s == nil {
		return nil, nil, errors.New("unknown subscription")
	}
	return r, s, nil
}
func (c *connector) ReceiveStream(h string, q sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	r, s, err := c.findStream(h, q.StreamID)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	if q.MaxMessages < 1 || q.MaxMessages > 32 || q.WaitTimeoutMS < 1 || q.WaitTimeoutMS > 30000 {
		return sdk.StreamReceiveResponse{}, errors.New("invalid receive limits")
	}
	timer := time.NewTimer(time.Duration(q.WaitTimeoutMS) * time.Millisecond)
	defer timer.Stop()
	for {
		s.mu.Lock()
		if s.err != nil {
			err = s.err
			s.mu.Unlock()
			return sdk.StreamReceiveResponse{}, err
		}
		out := []sdk.StreamMessage{}
		for len(out) < q.MaxMessages && len(s.queue) > 0 && s.credits > 0 {
			m := s.queue[0]
			s.queue[0] = sdk.StreamMessage{}
			s.queue = s.queue[1:]
			s.bytes -= len(m.RawPayload)
			s.credits--
			m.StreamID = q.StreamID
			out = append(out, m)
		}
		s.mu.Unlock()
		if len(out) > 0 {
			return sdk.StreamReceiveResponse{Messages: out}, nil
		}
		select {
		case <-r.ctx.Done():
			return sdk.StreamReceiveResponse{}, errors.New("resource closed")
		case <-s.done:
			return sdk.StreamReceiveResponse{}, errors.New("subscription closed")
		case <-s.wake:
		case <-timer.C:
			return sdk.StreamReceiveResponse{}, nil
		}
	}
}
func (c *connector) GrantCredits(h string, q sdk.StreamGrantCreditsRequest) error {
	_, s, err := c.findStream(h, q.StreamID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if q.Credits < 0 || q.Credits > 32 || s.credits+q.Credits > s.max {
		return errors.New("invalid credits")
	}
	s.credits += q.Credits
	s.signal()
	return nil
}
func (c *connector) AckStream(h string, q sdk.StreamAckRequest) error {
	return c.GrantCredits(h, sdk.StreamGrantCreditsRequest{StreamID: q.StreamID, Credits: q.Credits})
}
func (c *connector) NackStream(h string, q sdk.StreamNackRequest) error {
	if q.Requeue {
		return errors.New("OPC UA live notification cannot be requeued")
	}
	return c.GrantCredits(h, sdk.StreamGrantCreditsRequest{StreamID: q.StreamID, Credits: q.Credits})
}
func (c *connector) CloseStream(h string, q sdk.StreamCloseRequest) error {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case r.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-r.gate }()
	if s := r.streams[q.StreamID]; s != nil {
		delete(r.streams, q.StreamID)
		err := s.sub.Cancel(ctx)
		s.cancel()
		<-s.done
		if err != nil {
			r.closeClient()
		}
		if len(r.streams) == 0 {
			r.closeClient()
		}
	}
	return nil
}
