package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	redislib "github.com/redis/go-redis/v9"
)

type redisStreamSession struct {
	id string

	stream         string
	cursor         string
	count          int64
	blockTimeoutMS int

	mu               sync.Mutex
	availableCredits int
	maxBuffered      int
	overflowPolicy   string
	ackTimeout       time.Duration
	maxDeliverBatch  int
	requeue          []sdk.StreamMessage
	pending          map[string]sdk.StreamMessage
	closed           bool
}

func newRedisStreamSession(id string, payload redisReadPayload, req sdk.StreamOpenRequest) *redisStreamSession {
	if payload.StartID == "" {
		payload.StartID = "$"
	}
	if payload.Count <= 0 {
		payload.Count = 1
	}
	if payload.BlockTimeoutMS <= 0 {
		payload.BlockTimeoutMS = 1000
	}
	credits := req.InitialCredits
	if credits <= 0 {
		credits = 1
	}
	maxBuffered := req.MaxBufferedMessages
	if maxBuffered <= 0 {
		maxBuffered = 4096
	}
	ackTimeout := req.AckTimeoutMS
	if ackTimeout <= 0 {
		ackTimeout = 120000
	}
	maxDeliverBatch := req.MaxDeliverBatch
	if maxDeliverBatch <= 0 {
		maxDeliverBatch = 128
	}
	return &redisStreamSession{
		id:               id,
		stream:           payload.Stream,
		cursor:           payload.StartID,
		count:            payload.Count,
		blockTimeoutMS:   payload.BlockTimeoutMS,
		availableCredits: credits,
		maxBuffered:      maxBuffered,
		overflowPolicy:   normalizeRedisOverflowPolicy(req.OverflowPolicy),
		ackTimeout:       time.Duration(ackTimeout) * time.Millisecond,
		maxDeliverBatch:  maxDeliverBatch,
		pending:          make(map[string]sdk.StreamMessage),
	}
}

func (s *redisStreamSession) reserveCredits(limit int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.availableCredits <= 0 {
		return 0
	}
	if s.shouldPauseFetchLocked() {
		return 0
	}
	if limit <= 0 || limit > s.availableCredits {
		limit = s.availableCredits
	}
	if s.maxDeliverBatch > 0 && limit > s.maxDeliverBatch {
		limit = s.maxDeliverBatch
	}
	if s.count > 0 && int64(limit) > s.count {
		limit = int(s.count)
	}
	return limit
}

func (s *redisStreamSession) dequeueRequeued(limit int) []sdk.StreamMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.availableCredits <= 0 || len(s.requeue) == 0 {
		return nil
	}
	if limit <= 0 || limit > s.availableCredits {
		limit = s.availableCredits
	}
	if s.maxDeliverBatch > 0 && limit > s.maxDeliverBatch {
		limit = s.maxDeliverBatch
	}
	if limit > len(s.requeue) {
		limit = len(s.requeue)
	}
	out := append([]sdk.StreamMessage(nil), s.requeue[:limit]...)
	s.requeue = append([]sdk.StreamMessage(nil), s.requeue[limit:]...)
	s.availableCredits -= limit
	for _, message := range out {
		s.pending[message.DeliveryID] = message
	}
	return out
}

func (s *redisStreamSession) recordFetched(messages []sdk.StreamMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(messages) > s.availableCredits {
		messages = messages[:s.availableCredits]
	}
	s.availableCredits -= len(messages)
	for _, message := range messages {
		s.pending[message.DeliveryID] = message
		s.cursor = message.Key
	}
}

func (s *redisStreamSession) ack(deliveryID string, credits int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, deliveryID)
	if credits > 0 {
		s.availableCredits += credits
	}
}

func (s *redisStreamSession) nack(deliveryID string, requeue bool, credits int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	message, ok := s.pending[deliveryID]
	if ok {
		delete(s.pending, deliveryID)
	}
	if credits > 0 {
		s.availableCredits += credits
	}
	if requeue && ok {
		s.requeue = append([]sdk.StreamMessage{message}, s.requeue...)
	}
}

func (s *redisStreamSession) grantCredits(credits int) {
	if credits <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.availableCredits += credits
}

func (s *redisStreamSession) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *redisStreamSession) shouldPauseFetchLocked() bool {
	return s.overflowPolicy == "pause_fetch" && s.maxBuffered > 0 && len(s.requeue)+len(s.pending) >= s.maxBuffered
}

func normalizeRedisOverflowPolicy(policy string) string {
	switch policy {
	case "", "pause_fetch":
		return "pause_fetch"
	case "drop_oldest", "drop_newest", "fail_session":
		return policy
	default:
		return "pause_fetch"
	}
}

func (c *redisConnector) openReadStream(resource *redisResource, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	var payload redisReadPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.StreamOpenResponse{}, fmt.Errorf("parse stream payload: %w", err)
	}
	payload.Stream = stringsTrim(payload.Stream)
	payload.StartID = stringsTrim(payload.StartID)
	if payload.Stream == "" {
		return sdk.StreamOpenResponse{}, fmt.Errorf("stream.stream is required")
	}
	sessionID := stringsTrim(req.SessionKey)
	if sessionID == "" {
		sessionID = resource.resource.ID + ":" + payload.Stream
	}
	session := newRedisStreamSession(sessionID, payload, req)

	resource.mu.Lock()
	if current, ok := resource.sessions[sessionID]; ok {
		current.close()
	}
	resource.sessions[sessionID] = session
	resource.mu.Unlock()

	return sdk.StreamOpenResponse{
		StreamID:          sessionID,
		DeliverySemantics: sdk.StreamDeliveryAtLeastOnce,
		InitialCredits:    req.InitialCredits,
	}, nil
}

func (c *redisConnector) receiveReadStream(resource *redisResource, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	session, err := c.getReadStreamSession(resource, req.StreamID)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	if requeued := session.dequeueRequeued(req.MaxMessages); len(requeued) > 0 {
		return sdk.StreamReceiveResponse{Messages: requeued}, nil
	}

	limit := session.reserveCredits(req.MaxMessages)
	if limit <= 0 {
		return sdk.StreamReceiveResponse{}, nil
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}

	blockTimeout := time.Duration(req.WaitTimeoutMS) * time.Millisecond
	if blockTimeout <= 0 {
		blockTimeout = time.Duration(session.blockTimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(0, blockTimeout+2*time.Second))
	defer cancel()

	streams, err := client.XRead(ctx, &redislib.XReadArgs{
		Streams: []string{session.stream, session.cursor},
		Count:   int64(limit),
		Block:   blockTimeout,
	})
	if err != nil {
		if errors.Is(err, redislib.Nil) {
			return sdk.StreamReceiveResponse{}, nil
		}
		return sdk.StreamReceiveResponse{}, fmt.Errorf("xread stream %s: %w", session.stream, err)
	}

	messages := make([]sdk.StreamMessage, 0, limit)
	for _, stream := range streams {
		for _, message := range stream.Messages {
			streamMessage, err := buildRedisStreamMessage(stream.Stream, message)
			if err != nil {
				return sdk.StreamReceiveResponse{}, err
			}
			messages = append(messages, streamMessage)
		}
	}
	session.recordFetched(messages)
	return sdk.StreamReceiveResponse{Messages: messages}, nil
}

func (c *redisConnector) ackReadStream(resource *redisResource, req sdk.StreamAckRequest) error {
	session, err := c.getReadStreamSession(resource, req.StreamID)
	if err != nil {
		return err
	}
	session.ack(req.DeliveryID, req.Credits)
	return nil
}

func (c *redisConnector) nackReadStream(resource *redisResource, req sdk.StreamNackRequest) error {
	session, err := c.getReadStreamSession(resource, req.StreamID)
	if err != nil {
		return err
	}
	session.nack(req.DeliveryID, req.Requeue, req.Credits)
	return nil
}

func (c *redisConnector) grantReadStreamCredits(resource *redisResource, req sdk.StreamGrantCreditsRequest) error {
	session, err := c.getReadStreamSession(resource, req.StreamID)
	if err != nil {
		return err
	}
	session.grantCredits(req.Credits)
	return nil
}

func (c *redisConnector) closeReadStream(resource *redisResource, req sdk.StreamCloseRequest) error {
	resource.mu.Lock()
	defer resource.mu.Unlock()
	session, ok := resource.sessions[req.StreamID]
	if !ok {
		return nil
	}
	session.close()
	delete(resource.sessions, req.StreamID)
	return nil
}

func (c *redisConnector) getReadStreamSession(resource *redisResource, streamID string) (*redisStreamSession, error) {
	resource.mu.Lock()
	defer resource.mu.Unlock()
	session, ok := resource.sessions[streamID]
	if !ok {
		return nil, fmt.Errorf("stream %s not found", streamID)
	}
	return session, nil
}

func buildRedisStreamMessage(streamName string, message redislib.XMessage) (sdk.StreamMessage, error) {
	normalized := normalizeRedisValues(message.Values)
	raw, err := json.Marshal(redisStreamMessage{
		Stream:     streamName,
		ID:         message.ID,
		Values:     normalized,
		ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return sdk.StreamMessage{}, err
	}
	return sdk.StreamMessage{
		DeliveryID:          streamName + "|" + message.ID,
		Topic:               streamName,
		Key:                 message.ID,
		RawPayload:          raw,
		Metadata:            map[string]string{"redis_stream": streamName, "redis_message_id": message.ID},
		PublishedAtUnixNano: time.Now().UnixNano(),
	}, nil
}
