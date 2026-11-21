package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const (
	mqttDefaultInitialCredits      = 1
	mqttDefaultMaxBufferedMessages = 4096
	mqttDefaultAckTimeoutMS        = 120000
	mqttDefaultMaxDeliverBatch     = 64
	mqttMaxWaitMS                  = 30_000
)

type mqttMessageDeque struct {
	buf  []sdk.StreamMessage
	head int
	size int
}

func (d *mqttMessageDeque) len() int {
	return d.size
}

func (d *mqttMessageDeque) pushBack(message sdk.StreamMessage) {
	d.grow()
	idx := (d.head + d.size) % len(d.buf)
	d.buf[idx] = message
	d.size++
}

func (d *mqttMessageDeque) pushFront(message sdk.StreamMessage) {
	d.grow()
	d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
	d.buf[d.head] = message
	d.size++
}

func (d *mqttMessageDeque) popFront() (sdk.StreamMessage, bool) {
	if d.size == 0 {
		return sdk.StreamMessage{}, false
	}
	message := d.buf[d.head]
	d.buf[d.head] = sdk.StreamMessage{}
	d.head = (d.head + 1) % len(d.buf)
	d.size--
	if d.size == 0 {
		d.head = 0
	}
	return message, true
}

func (d *mqttMessageDeque) grow() {
	if d.buf == nil {
		d.buf = make([]sdk.StreamMessage, 8)
		return
	}
	if d.size < len(d.buf) {
		return
	}
	next := make([]sdk.StreamMessage, len(d.buf)*2)
	for i := 0; i < d.size; i++ {
		next[i] = d.buf[(d.head+i)%len(d.buf)]
	}
	d.buf = next
	d.head = 0
}

type mqttConsumerSession struct {
	residentBytes int
	reserve       func(int) bool
	release       func(int)
	id            string

	mu                  sync.Mutex
	topics              map[string]struct{}
	availableCredits    int
	maxBufferedMessages int
	overflowPolicy      string
	ackTimeout          time.Duration
	maxDeliverBatch     int
	ready               mqttMessageDeque
	requeue             mqttMessageDeque
	pending             map[string]sdk.StreamMessage
	nextSequence        uint64
	closed              bool
	overflowTotal       uint64
	lastOverflowAt      time.Time
	creditsStarvedSince time.Time
	notifyCh            chan struct{}
	done                chan struct{}
}

func newMQTTConsumerSession(id string, req sdk.StreamOpenRequest) *mqttConsumerSession {
	credits := req.InitialCredits
	if credits <= 0 {
		credits = mqttDefaultInitialCredits
	}
	maxBuffered := req.MaxBufferedMessages
	if maxBuffered <= 0 {
		maxBuffered = mqttDefaultMaxBufferedMessages
	}
	ackTimeout := req.AckTimeoutMS
	if ackTimeout <= 0 {
		ackTimeout = mqttDefaultAckTimeoutMS
	}
	maxDeliverBatch := req.MaxDeliverBatch
	if maxDeliverBatch <= 0 {
		maxDeliverBatch = mqttDefaultMaxDeliverBatch
	}
	return &mqttConsumerSession{
		id:                  id,
		topics:              make(map[string]struct{}),
		availableCredits:    credits,
		maxBufferedMessages: maxBuffered,
		overflowPolicy:      normalizeMQTTOverflowPolicy(req.OverflowPolicy),
		ackTimeout:          time.Duration(ackTimeout) * time.Millisecond,
		maxDeliverBatch:     maxDeliverBatch,
		pending:             make(map[string]sdk.StreamMessage),
		notifyCh:            make(chan struct{}, 1),
		done:                make(chan struct{}),
	}
}

func normalizeMQTTOverflowPolicy(policy string) string {
	switch strings.TrimSpace(policy) {
	case "", "drop_oldest":
		return "drop_oldest"
	case "drop_newest", "fail_session", "pause_fetch":
		return policy
	default:
		return "drop_oldest"
	}
}

func (s *mqttConsumerSession) enqueue(message mqttInboundMessage) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	cost := streamCost(message.RawPayload, message.Topic, message.Subscription, message.ReceivedAt)
	if len(message.RawPayload) > mqttPayloadLimit || !s.ensureRoomLocked(cost) {
		return false
	}
	for s.reserve != nil && !s.reserve(cost) {
		s.overflowTotal++
		if !s.dropOldestLocked() {
			return false
		}
	}
	s.residentBytes += cost
	s.nextSequence++
	id := fmt.Sprintf("%s:%d", s.id, s.nextSequence)
	s.ready.pushBack(sdk.StreamMessage{StreamID: s.id, DeliveryID: id, Sequence: s.nextSequence, Topic: message.Topic, RawPayload: append([]byte(nil), message.RawPayload...), Metadata: map[string]string{"mqtt_topic": message.Topic, "mqtt_subscription": message.Subscription, "received_at": message.ReceivedAt}, PublishedAtUnixNano: time.Now().UnixNano()})
	s.updateStarvationLocked()
	s.signal()
	return true
}
func (s *mqttConsumerSession) messageCost(m sdk.StreamMessage) int {
	return streamCost(m.RawPayload, m.Topic, m.Metadata["mqtt_subscription"], m.Metadata["received_at"])
}
func (s *mqttConsumerSession) freeMessage(m sdk.StreamMessage) {
	cost := s.messageCost(m)
	s.residentBytes -= cost
	if s.release != nil {
		s.release(cost)
	}
}
func (s *mqttConsumerSession) dropOldestLocked() bool {
	if s.overflowPolicy != "drop_oldest" {
		if s.overflowPolicy == "fail_session" {
			s.closeLocked()
		}
		return false
	}
	message, ok := s.ready.popFront()
	if !ok {
		message, ok = s.requeue.popFront()
	}
	if ok {
		s.freeMessage(message)
	}
	return ok
}
func (s *mqttConsumerSession) ensureRoomLocked(wanted ...int) bool {
	cost := 0
	if len(wanted) > 0 {
		cost = wanted[0]
	}
	if cost > mqttSessionByteLimit {
		s.overflowTotal++
		return false
	}
	for s.backlogLenLocked()+len(s.pending) >= s.maxBufferedMessages || s.residentBytes+cost > mqttSessionByteLimit {
		s.overflowTotal++
		s.lastOverflowAt = time.Now()
		if !s.dropOldestLocked() {
			return false
		}
	}
	return true
}

func (s *mqttConsumerSession) backlogLenLocked() int {
	return s.ready.len() + s.requeue.len()
}

func (s *mqttConsumerSession) receive(ctx context.Context, maxMessages int) ([]sdk.StreamMessage, error) {
	for {
		messages, closed := s.receiveBatch(maxMessages)
		if len(messages) > 0 {
			return messages, nil
		}
		if closed {
			return nil, fmt.Errorf("MQTT stream is closed or overflowed")
		}

		s.mu.Lock()
		notifyCh := s.notifyCh
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, nil
		case <-s.done:
			return nil, fmt.Errorf("MQTT stream is closed or overflowed")
		case <-notifyCh:
		}
	}
}

func (s *mqttConsumerSession) receiveBatch(maxMessages int) ([]sdk.StreamMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	messages := s.dispatchAvailableLocked(maxMessages)
	return messages, s.closed
}

func (s *mqttConsumerSession) dispatchAvailableLocked(maxMessages int) []sdk.StreamMessage {
	if maxMessages <= 0 {
		maxMessages = s.maxDeliverBatch
	}
	if maxMessages <= 0 {
		maxMessages = 1
	}
	available := s.availableCredits
	backlog := s.backlogLenLocked()
	if available <= 0 || backlog == 0 {
		s.updateStarvationLocked()
		return nil
	}
	count := min(maxMessages, s.maxDeliverBatch)
	if available < count {
		count = available
	}
	if backlog < count {
		count = backlog
	}
	messages := make([]sdk.StreamMessage, 0, count)
	for i := 0; i < count; i++ {
		message, ok := s.requeue.popFront()
		if !ok {
			message, ok = s.ready.popFront()
		}
		if !ok {
			break
		}
		s.availableCredits--
		message.Metadata["mqtt_overflow_total"] = fmt.Sprintf("%d", s.overflowTotal)
		s.pending[message.DeliveryID] = message
		// The caller serializes its response outside the session lock. A requeue
		// may update the pending message before that serialization completes.
		message.Metadata = maps.Clone(message.Metadata)
		message.RawPayload = slices.Clone(message.RawPayload)
		messages = append(messages, message)
	}
	s.updateStarvationLocked()
	return messages
}

func (s *mqttConsumerSession) ack(req sdk.StreamAckRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	message, ok := s.pending[req.DeliveryID]
	if !ok {
		return
	}
	delete(s.pending, req.DeliveryID)
	s.freeMessage(message)
	s.availableCredits = min(1024, s.availableCredits+max(0, min(req.Credits, 1024)))
	s.updateStarvationLocked()
	s.signal()
}
func (s *mqttConsumerSession) nack(req sdk.StreamNackRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	message, ok := s.pending[req.DeliveryID]
	if !ok {
		return
	}
	delete(s.pending, req.DeliveryID)
	// Requeue moves existing memory; it neither allocates nor re-reserves bytes.
	if req.Requeue && !s.closed {
		s.requeue.pushFront(message)
	} else {
		s.freeMessage(message)
	}
	s.availableCredits = min(1024, s.availableCredits+max(0, min(req.Credits, 1024)))
	s.updateStarvationLocked()
	s.signal()
}

func (s *mqttConsumerSession) grantCredits(credits int) {
	if credits <= 0 {
		return
	}
	s.mu.Lock()
	s.availableCredits = min(1024, s.availableCredits+min(credits, 1024))
	s.updateStarvationLocked()
	shouldSignal := s.backlogLenLocked() > 0
	s.mu.Unlock()
	if shouldSignal {
		s.signal()
	}
}

func (s *mqttConsumerSession) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
}

func (s *mqttConsumerSession) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
	if s.release != nil && s.residentBytes > 0 {
		s.release(s.residentBytes)
	}
	s.residentBytes = 0
	s.ready = mqttMessageDeque{}
	s.requeue = mqttMessageDeque{}
	s.pending = nil
}

func (s *mqttConsumerSession) updateStarvationLocked() {
	if s.backlogLenLocked() > 0 && s.availableCredits <= 0 {
		if s.creditsStarvedSince.IsZero() {
			s.creditsStarvedSince = time.Now()
		}
		return
	}
	s.creditsStarvedSince = time.Time{}
}

func (s *mqttConsumerSession) signal() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func mqttReceiveWait(waitMS int) time.Duration {
	if waitMS <= 0 {
		return time.Second
	}
	return time.Duration(min(waitMS, mqttMaxWaitMS)) * time.Millisecond
}

func (c *mqttConnector) receiveConsumerStream(resource *mqttResource, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	session, err := c.getSession(resource, req.StreamID)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), mqttReceiveWait(req.WaitTimeoutMS))
	defer cancel()
	if _, err = c.ensureConnected(resource); err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	messages, err := session.receive(ctx, req.MaxMessages)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	return sdk.StreamReceiveResponse{Messages: messages}, nil
}

func (c *mqttConnector) ackConsumerStream(resource *mqttResource, req sdk.StreamAckRequest) error {
	session, err := c.getSession(resource, req.StreamID)
	if err != nil {
		return err
	}
	session.ack(req)
	return nil
}

func (c *mqttConnector) nackConsumerStream(resource *mqttResource, req sdk.StreamNackRequest) error {
	session, err := c.getSession(resource, req.StreamID)
	if err != nil {
		return err
	}
	session.nack(req)
	return nil
}

func (c *mqttConnector) grantConsumerCredits(resource *mqttResource, req sdk.StreamGrantCreditsRequest) error {
	session, err := c.getSession(resource, req.StreamID)
	if err != nil {
		return err
	}
	session.grantCredits(req.Credits)
	return nil
}

func (c *mqttConnector) getSession(resource *mqttResource, streamID string) (*mqttConsumerSession, error) {
	resource.mu.Lock()
	defer resource.mu.Unlock()
	session, ok := resource.sessions[streamID]
	if !ok {
		return nil, fmt.Errorf("stream %s not found", streamID)
	}
	return session, nil
}
