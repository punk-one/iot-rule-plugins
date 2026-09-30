package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type redisStreamEnvelope struct {
	Stream     string         `json:"stream"`
	ID         string         `json:"id"`
	Values     map[string]any `json:"values"`
	ReceivedAt string         `json:"received_at,omitempty"`
}

func (s *RedisSource) openStream() error {
	raw, err := json.Marshal(map[string]any{
		"stream":           s.cfg.Stream.Name,
		"start_id":         s.cfg.Stream.StartID,
		"count":            s.cfg.Stream.Count,
		"block_timeout_ms": s.cfg.Stream.BlockTimeoutMS,
	})
	if err != nil {
		return err
	}
	stream, err := s.ctx.Connector().OpenStream(sdk.StreamOpenRequest{
		ResourceRef:         s.cfg.ConnectorRef,
		SessionKey:          buildSourceSessionKey(s.ctx.RuleID(), s.ctx.NodeID(), s.cfg.ConnectorRef),
		Target:              "stream",
		Payload:             raw,
		InitialCredits:      s.cfg.Streaming.InitialCredits,
		MaxBufferedMessages: s.cfg.Streaming.MaxBufferedMessages,
		OverflowPolicy:      s.cfg.Streaming.OverflowPolicy,
		AckTimeoutMS:        s.cfg.Streaming.AckTimeoutMS,
		MaxDeliverBatch:     s.cfg.Streaming.MaxDeliverBatch,
	})
	if err != nil {
		return redisSourceRetryable(err)
	}
	if s.stopping() {
		_ = stream.Close()
		return fmt.Errorf("redis source is stopping")
	}
	s.setStream(stream)
	return nil
}

func (s *RedisSource) consumeLoop() {
	backoff := 0
	for {
		if s.stopping() {
			return
		}
		stream := s.currentStream()
		if stream == nil {
			if err := s.openStream(); err != nil {
				if s.stopping() {
					return
				}
				s.reportReadError(err)
				if !s.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			stream = s.currentStream()
		}

		timeout := time.Duration(s.cfg.Stream.BlockTimeoutMS) * time.Millisecond
		message, ok, err := stream.Recv(timeout)
		if err != nil {
			s.reportReadError(err)
			_ = s.closeStream(stream)
			if !s.sleepWithBackoff(backoff) {
				return
			}
			backoff++
			continue
		}
		if !ok {
			backoff = 0
			continue
		}

		err = s.publishMessage(message)
		switch {
		case err == nil:
			if ackErr := stream.Ack(message.DeliveryID, 1); ackErr != nil {
				s.handleDeliveryFailure(stream, "ack", ackErr)
				if !s.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			s.reportHeartbeat(1)
			backoff = 0
		case sdk.IsSkippable(err):
			if ackErr := stream.Ack(message.DeliveryID, 1); ackErr != nil {
				s.handleDeliveryFailure(stream, "ack skipped message", ackErr)
				if !s.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			s.reportReadError(err)
		case sdk.IsRetryable(err):
			if nackErr := stream.Nack(message.DeliveryID, true, err.Error(), 1); nackErr != nil {
				s.handleDeliveryFailure(stream, "nack retryable message", nackErr)
				if !s.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			s.reportReadError(err)
		default:
			if nackErr := stream.Nack(message.DeliveryID, false, err.Error(), 1); nackErr != nil {
				s.handleDeliveryFailure(stream, "nack failed message", nackErr)
				if !s.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			s.reportReadError(err)
		}
	}
}

func (s *RedisSource) handleDeliveryFailure(stream sdk.StreamConsumer, operation string, err error) {
	wrapped := redisSourceRetryable(fmt.Errorf("%s: %w", operation, err))
	s.ctx.Logger().Error("Redis Source: delivery confirmation failed",
		sdk.Field{Key: "operation", Value: operation},
		sdk.Field{Key: "error", Value: err.Error()},
	)
	s.reportReadError(wrapped)
	_ = s.closeStream(stream)
}

func (s *RedisSource) sleepWithBackoff(backoff int) bool {
	delay := time.Duration(100*(1<<min(backoff, 4))) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

func (s *RedisSource) publishMessage(message sdk.StreamMessage) error {
	var envelope redisStreamEnvelope
	if err := json.Unmarshal(message.RawPayload, &envelope); err != nil {
		return redisSourceSkippable(fmt.Errorf("decode redis stream payload: %w", err))
	}
	payload, err := s.decodeEnvelope(envelope)
	if err != nil {
		return err
	}

	metadata := map[string]string{
		"protocol":         "redis",
		"redis_stream":     envelope.Stream,
		"redis_message_id": envelope.ID,
		"is_source":        "true",
	}
	if s.cfg.Output.IncludeMeta {
		for key, value := range message.Metadata {
			metadata[key] = value
		}
	}

	event := sdk.NewEvent(payload, metadata)
	event.RuleID = s.ctx.RuleID()
	event.Source = s.ctx.NodeID()
	if sdk.GetTraceID(event) == "" {
		sdk.SetTraceID(&event, uuid.New().String())
	}
	if err := s.ctx.Emitter().Publish(event); err != nil {
		return redisSourceRetryable(err)
	}
	return nil
}

func (s *RedisSource) decodeEnvelope(envelope redisStreamEnvelope) (map[string]any, error) {
	switch s.cfg.Decode.Mode {
	case "values":
		return envelope.Values, nil
	case "payload_json":
		return decodePayloadField(envelope.Values, s.cfg.Decode.PayloadField)
	default:
		if payload, err := decodePayloadField(envelope.Values, s.cfg.Decode.PayloadField); err == nil {
			return payload, nil
		}
		return envelope.Values, nil
	}
}

func decodePayloadField(values map[string]any, field string) (map[string]any, error) {
	raw, ok := values[field]
	if !ok {
		return nil, fmt.Errorf("payload field %q not found", field)
	}
	text, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("payload field %q is not a string", field)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return nil, fmt.Errorf("parse payload field %q: %w", field, err)
	}
	return payload, nil
}

func (s *RedisSource) watchResourceEvents() {
	ch := s.ctx.ResourceEvents()
	if ch == nil {
		return
	}
	for {
		select {
		case <-s.stopCh:
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			s.onResourceStatus(event)
		}
	}
}

func (s *RedisSource) onResourceStatus(event sdk.ResourceStatusEvent) {
	s.setStatus(event.Status)
	s.reportStatus(event)
}

func (s *RedisSource) setStatus(status sdk.ResourceStatus) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.status = status
}

func (s *RedisSource) currentStatus() sdk.ResourceStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func buildSourceSessionKey(ruleID, nodeID, resourceRef string) string {
	parts := []string{
		strings.TrimSpace(ruleID),
		strings.TrimSpace(nodeID),
		strings.TrimSpace(resourceRef),
	}
	return strings.Join(parts, ":")
}
