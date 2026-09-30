package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const (
	mqttDefaultInitialCredits      = 1
	mqttDefaultMaxBufferedMessages = 4096
	mqttDefaultAckTimeoutMS        = 120000
	mqttDefaultMaxDeliverBatch     = 64
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
	id string

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

func (s *mqttConsumerSession) enqueue(message mqttInboundMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.nextSequence++
	deliveryID := fmt.Sprintf("%s:%d", s.id, s.nextSequence)
	streamMessage := sdk.StreamMessage{
		StreamID:            s.id,
		DeliveryID:          deliveryID,
		Sequence:            s.nextSequence,
		Topic:               message.Topic,
		RawPayload:          append([]byte(nil), message.RawPayload...),
		Metadata:            map[string]string{"mqtt_topic": message.Topic, "mqtt_subscription": message.Subscription, "received_at": message.ReceivedAt},
		PublishedAtUnixNano: time.Now().UnixNano(),
	}
	if !s.ensureRoomLocked() {
		s.updateStarvationLocked()
		return
	}
	s.ready.pushBack(streamMessage)
	s.updateStarvationLocked()
	s.signal()
}

func (s *mqttConsumerSession) ensureRoomLocked() bool {
	if s.maxBufferedMessages <= 0 || s.backlogLenLocked() < s.maxBufferedMessages {
		return true
	}
	now := time.Now()
	s.overflowTotal++
	s.lastOverflowAt = now

	switch s.overflowPolicy {
	case "drop_newest", "pause_fetch":
		return false
	case "fail_session":
		s.closed = true
		s.signal()
		return false
	case "drop_oldest":
		if _, ok := s.ready.popFront(); ok {
			return true
		}
		if _, ok := s.requeue.popFront(); ok {
			return true
		}
		return true
	default:
		return false
	}
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
			return nil, nil
		}

		s.mu.Lock()
		notifyCh := s.notifyCh
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, nil
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
	count := maxMessages
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
		s.pending[message.DeliveryID] = message
		messages = append(messages, message)
	}
	s.updateStarvationLocked()
	return messages
}

func (s *mqttConsumerSession) ack(req sdk.StreamAckRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, req.DeliveryID)
	if req.Credits > 0 {
		s.availableCredits += req.Credits
	}
	s.updateStarvationLocked()
	if s.backlogLenLocked() > 0 && s.availableCredits > 0 {
		s.signal()
	}
}

func (s *mqttConsumerSession) nack(req sdk.StreamNackRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	message, ok := s.pending[req.DeliveryID]
	if ok {
		delete(s.pending, req.DeliveryID)
	}
	if req.Credits > 0 {
		s.availableCredits += req.Credits
	}
	if req.Requeue && ok {
		if s.ensureRoomLocked() {
			s.requeue.pushFront(message)
		}
	}
	s.updateStarvationLocked()
	if s.backlogLenLocked() > 0 && s.availableCredits > 0 {
		s.signal()
	}
}

func (s *mqttConsumerSession) grantCredits(credits int) {
	if credits <= 0 {
		return
	}
	s.mu.Lock()
	s.availableCredits += credits
	s.updateStarvationLocked()
	shouldSignal := s.backlogLenLocked() > 0
	s.mu.Unlock()
	if shouldSignal {
		s.signal()
	}
}

func (s *mqttConsumerSession) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	s.signal()
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

func (c *mqttConnector) openConsumerStream(resource *mqttResource, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	var payload mqttReadPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.StreamOpenResponse{}, fmt.Errorf("parse stream payload: %w", err)
	}
	if len(payload.Topics) == 0 {
		return sdk.StreamOpenResponse{}, fmt.Errorf("stream.topics is required")
	}

	client, err := c.ensureConnected(resource)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}

	sessionID := strings.TrimSpace(req.SessionKey)
	if sessionID == "" {
		sessionID = req.ResourceRef
	}
	if sessionID == "" {
		return sdk.StreamOpenResponse{}, fmt.Errorf("stream session key is required")
	}

	session := newMQTTConsumerSession(sessionID, req)

	resource.mu.Lock()
	if current, ok := resource.sessions[sessionID]; ok {
		current.close()
		delete(resource.sessions, sessionID)
	}
	resource.sessions[sessionID] = session
	resource.mu.Unlock()

	if err := c.ensureSessionSubscription(resource, session, payload.Topics, byte(payload.QoS), client); err != nil {
		return sdk.StreamOpenResponse{}, err
	}

	return sdk.StreamOpenResponse{
		StreamID:          sessionID,
		DeliverySemantics: sdk.StreamDeliveryAtLeastOnce,
		InitialCredits:    session.availableCredits,
	}, nil
}

func (c *mqttConnector) receiveConsumerStream(resource *mqttResource, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	session, err := c.getSession(resource, req.StreamID)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	timeout := time.Duration(req.WaitTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
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

func (c *mqttConnector) closeConsumerStream(resource *mqttResource, req sdk.StreamCloseRequest) error {
	resource.mu.Lock()
	session, ok := resource.sessions[req.StreamID]
	if !ok {
		resource.mu.Unlock()
		return nil
	}
	delete(resource.sessions, req.StreamID)
	for topic := range session.topics {
		state := resource.subscriptions[topic]
		if state == nil {
			continue
		}
		delete(state.sessions, req.StreamID)
		if len(state.tokens) == 0 && len(state.sessions) == 0 && mqttClientReady(resource.client) {
			unsub := resource.client.Unsubscribe(resource.effectiveTopic(topic))
			if unsub.Wait() && unsub.Error() != nil {
				resource.mu.Unlock()
				return unsub.Error()
			}
			delete(resource.subscriptions, topic)
		}
	}
	resource.mu.Unlock()
	session.close()
	return nil
}

func (c *mqttConnector) ensureSessionSubscription(resource *mqttResource, session *mqttConsumerSession, topics []string, qos byte, client mqtt.Client) error {
	resource.mu.Lock()
	defer resource.mu.Unlock()

	for _, topic := range topics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}
		if _, ok := session.topics[topic]; ok {
			continue
		}

		session.topics[topic] = struct{}{}
		state, ok := resource.subscriptions[topic]
		if !ok {
			state = &mqttSubscription{
				qos:      qos,
				tokens:   make(map[string]struct{}),
				sessions: make(map[string]struct{}),
			}
			effectiveTopic := resource.effectiveTopic(topic)
			tokenResult := client.Subscribe(effectiveTopic, qos, c.makeMessageHandler(resource, topic))
			if tokenResult.Wait() && tokenResult.Error() != nil {
				delete(session.topics, topic)
				return tokenResult.Error()
			}
			resource.subscriptions[topic] = state
		} else if qos > state.qos {
			effectiveTopic := resource.effectiveTopic(topic)
			tokenResult := client.Subscribe(effectiveTopic, qos, c.makeMessageHandler(resource, topic))
			if tokenResult.Wait() && tokenResult.Error() != nil {
				delete(session.topics, topic)
				return tokenResult.Error()
			}
			state.qos = qos
		}
		if state.sessions == nil {
			state.sessions = make(map[string]struct{})
		}
		state.sessions[session.id] = struct{}{}
	}
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
