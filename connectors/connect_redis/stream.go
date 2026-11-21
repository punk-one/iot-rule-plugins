package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

	receiveMu        sync.Mutex
	mu               sync.Mutex
	availableCredits int
	retainedBytes    int
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
		availableCredits: min(credits, maxBuffered, maxRedisBufferedMessages),
		maxBuffered:      min(maxBuffered, maxRedisBufferedMessages),
		overflowPolicy:   normalizeRedisOverflowPolicy(req.OverflowPolicy),
		ackTimeout:       time.Duration(ackTimeout) * time.Millisecond,
		maxDeliverBatch:  min(maxDeliverBatch, maxRedisStreamReadCount),
		pending:          make(map[string]sdk.StreamMessage),
	}
}

func (s *redisStreamSession) reserveCredits(limit int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.availableCredits <= 0 {
		return 0
	}
	free := s.maxBuffered - len(s.pending) - len(s.requeue)
	if free <= 0 || s.retainedBytes >= maxRedisResultBytes {
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
	return min(limit, free)
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
	for i, message := range out {
		s.pending[message.DeliveryID] = message
		out[i] = cloneRedisStreamMessage(message)
	}
	return out
}

func (s *redisStreamSession) recordFetched(messages []sdk.StreamMessage) ([]sdk.StreamMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("Redis stream is closed")
	}
	for i, message := range messages {
		if len(message.RawPayload) > maxRedisResultBytes {
			if i == 0 {
				return nil, fmt.Errorf("Redis stream message exceeds %d bytes", maxRedisResultBytes)
			}
			return messages[:i], nil
		}
		if s.retainedBytes+len(message.RawPayload) > maxRedisResultBytes {
			return messages[:i], nil
		}
		s.pending[message.DeliveryID] = cloneRedisStreamMessage(message)
		s.retainedBytes += len(message.RawPayload)
		s.cursor = message.Key
		s.availableCredits--
	}
	return messages, nil
}

func cloneRedisStreamMessage(message sdk.StreamMessage) sdk.StreamMessage {
	message.RawPayload = append([]byte(nil), message.RawPayload...)
	message.Metadata = maps.Clone(message.Metadata)
	return message
}

func (s *redisStreamSession) ack(deliveryID string, credits int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if message, ok := s.pending[deliveryID]; ok {
		delete(s.pending, deliveryID)
		s.retainedBytes -= len(message.RawPayload)
		s.addCreditsLocked(credits)
	}
}

func (s *redisStreamSession) nack(deliveryID string, requeue bool, credits int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	message, ok := s.pending[deliveryID]
	if !ok {
		return
	}
	delete(s.pending, deliveryID)
	s.addCreditsLocked(credits)
	if requeue {
		s.requeue = append([]sdk.StreamMessage{message}, s.requeue...)
	} else {
		s.retainedBytes -= len(message.RawPayload)
	}
}

func (s *redisStreamSession) grantCredits(credits int) {
	if credits <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addCreditsLocked(credits)
}

func (s *redisStreamSession) addCreditsLocked(credits int) {
	if credits > 0 && !s.closed {
		s.availableCredits = min(s.maxBuffered, s.availableCredits+min(credits, s.maxBuffered))
	}
}

func (s *redisStreamSession) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
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
	if err := normalizeRedisReadLimits(&payload); err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	sessionID := stringsTrim(req.SessionKey)
	if sessionID == "" {
		sessionID = resource.resource.ID + ":" + payload.Stream
	}
	session := newRedisStreamSession(sessionID, payload, req)

	resource.mu.Lock()
	if resource.closed {
		resource.mu.Unlock()
		return sdk.StreamOpenResponse{}, fmt.Errorf("Redis resource is closed")
	}
	if current, ok := resource.sessions[sessionID]; ok {
		current.close()
	} else if len(resource.sessions) >= maxRedisStreamSessions {
		resource.mu.Unlock()
		return sdk.StreamOpenResponse{}, fmt.Errorf("Redis resource reached the stream session limit")
	}
	initialCredits := session.availableCredits
	resource.sessions[sessionID] = session
	resource.mu.Unlock()

	return sdk.StreamOpenResponse{
		StreamID:          sessionID,
		DeliverySemantics: sdk.StreamDeliveryAtLeastOnce,
		InitialCredits:    initialCredits,
	}, nil
}

func (c *redisConnector) receiveReadStream(resource *redisResource, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	session, err := c.getReadStreamSession(resource, req.StreamID)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	session.receiveMu.Lock()
	defer session.receiveMu.Unlock()
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

	blockTimeout := time.Duration(min(req.WaitTimeoutMS, maxRedisReadWaitMS)) * time.Millisecond
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
	bytes := 0
readBatch:
	for _, stream := range streams {
		for _, message := range stream.Messages {
			if len(messages) >= limit {
				return sdk.StreamReceiveResponse{}, fmt.Errorf("Redis stream returned more than %d messages", limit)
			}
			streamMessage, err := buildRedisStreamMessage(stream.Stream, message)
			if err != nil {
				return sdk.StreamReceiveResponse{}, err
			}
			if bytes+len(streamMessage.RawPayload) > maxRedisResultBytes {
				if len(messages) == 0 {
					return sdk.StreamReceiveResponse{}, fmt.Errorf("Redis stream message exceeds %d bytes", maxRedisResultBytes)
				}
				break readBatch
			}
			bytes += len(streamMessage.RawPayload)
			messages = append(messages, streamMessage)
		}
	}
	accepted, err := session.recordFetched(messages)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	return sdk.StreamReceiveResponse{Messages: accepted}, nil
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
