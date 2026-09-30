package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type redisWritePayload struct {
	Stream string         `json:"stream"`
	MaxLen int64          `json:"max_len,omitempty"`
	Approx bool           `json:"approx,omitempty"`
	Values map[string]any `json:"values"`
}

type redisHashWritePayload struct {
	Key    string         `json:"key"`
	Fields map[string]any `json:"fields"`
}

var redisFieldSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func (s *RedisSink) OnEvent(e sdk.Event) error {
	if s.currentStatus() == sdk.ResourceStatusDisconnected || s.currentStatus() == sdk.ResourceStatusBlocked || s.currentStatus() == sdk.ResourceStatusReadonly {
		return redisSinkRetryable(fmt.Errorf("redis sink connector status is %s", s.currentStatus()))
	}

	var err error
	switch s.cfg.Target.Mode {
	case "hash":
		err = s.writeHash(e)
	default:
		err = s.writeStream(e)
	}
	if err != nil {
		return err
	}

	s.reportPublishSuccess()
	s.reportHeartbeat()
	if err := s.publishBufferAck(e); err != nil {
		return redisSinkSkippable(err)
	}
	return nil
}

func (s *RedisSink) writeStream(e sdk.Event) error {
	rawPayload, err := json.Marshal(e.Payload)
	if err != nil {
		return redisSinkSkippable(fmt.Errorf("Redis Sink: marshal payload: %w", err))
	}

	values := map[string]any{
		"payload": string(rawPayload),
	}
	if !e.Timestamp.IsZero() {
		values["timestamp"] = e.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	if e.ID != "" {
		values["event_id"] = e.ID
	}
	if s.cfg.Encode.IncludeContext {
		if e.RuleID != "" {
			values["rule_id"] = e.RuleID
		}
		if e.Source != "" {
			values["source"] = e.Source
		}
		if traceID := sdk.GetTraceID(e); traceID != "" {
			values["trace_id"] = traceID
		}
	}
	if s.cfg.Encode.IncludeMetadata {
		for key, value := range e.Metadata {
			key = redisMetadataField(key)
			if key == "" || strings.TrimSpace(value) == "" {
				continue
			}
			values[key] = value
		}
	}

	raw, err := json.Marshal(redisWritePayload{
		Stream: s.cfg.Target.Stream,
		MaxLen: s.cfg.Options.MaxLen,
		Approx: s.cfg.Options.Approx,
		Values: values,
	})
	if err != nil {
		return redisSinkSkippable(err)
	}

	if _, err := s.ctx.Connector().Write(sdk.ConnectorRequest{
		Target:    "stream_add",
		Payload:   raw,
		TimeoutMS: s.cfg.Options.WriteTimeoutSec * 1000,
	}); err != nil {
		s.reportPublishFailure(err)
		return redisSinkRetryable(err)
	}
	return nil
}

func (s *RedisSink) writeHash(e sdk.Event) error {
	key, err := s.resolveHashKey(e)
	if err != nil {
		return redisSinkSkippable(err)
	}
	fields, err := s.buildHashFields(e)
	if err != nil {
		return redisSinkSkippable(err)
	}
	if len(fields) == 0 {
		return redisSinkSkippable(fmt.Errorf("Redis Sink: no hash fields resolved"))
	}

	raw, err := json.Marshal(redisHashWritePayload{Key: key, Fields: fields})
	if err != nil {
		return redisSinkSkippable(err)
	}

	if _, err := s.ctx.Connector().Write(sdk.ConnectorRequest{
		Target:    "hash_set",
		Payload:   raw,
		TimeoutMS: s.cfg.Options.WriteTimeoutSec * 1000,
	}); err != nil {
		s.reportPublishFailure(err)
		return redisSinkRetryable(err)
	}
	return nil
}

func (s *RedisSink) resolveHashKey(e sdk.Event) (string, error) {
	if s.cfg.Target.KeyField != "" {
		if value, ok := getRedisEventPathValue(redisEventContext(e), s.cfg.Target.KeyField); ok {
			resolved := strings.TrimSpace(redisScalarString(value))
			if resolved != "" {
				return s.cfg.Target.KeyPrefix + resolved, nil
			}
		}
	}
	if s.cfg.Target.Key != "" {
		return s.cfg.Target.Key, nil
	}
	return "", fmt.Errorf("Redis Sink: hash key resolved empty")
}

func (s *RedisSink) buildHashFields(e sdk.Event) (map[string]any, error) {
	ctx := redisEventContext(e)
	fields := map[string]any{}

	if s.cfg.Encode.ExpandObject != "" {
		obj, ok := getRedisEventPathValue(ctx, s.cfg.Encode.ExpandObject)
		if !ok {
			return nil, fmt.Errorf("Redis Sink: expand_object path %q not found", s.cfg.Encode.ExpandObject)
		}
		switch typed := obj.(type) {
		case map[string]any:
			for k, v := range typed {
				fields[k] = v
			}
		case map[string]string:
			for k, v := range typed {
				fields[k] = v
			}
		default:
			return nil, fmt.Errorf("Redis Sink: expand_object path %q resolved to %T, expected object", s.cfg.Encode.ExpandObject, obj)
		}
	}

	for _, mapping := range s.cfg.Encode.FieldMappings {
		value, ok := getRedisEventPathValue(ctx, mapping.Path)
		if !ok {
			continue
		}
		fields[mapping.Field] = value
	}

	if s.cfg.Encode.IncludeContext {
		if e.RuleID != "" {
			fields["rule_id"] = e.RuleID
		}
		if e.Source != "" {
			fields["source"] = e.Source
		}
		if traceID := sdk.GetTraceID(e); traceID != "" {
			fields["trace_id"] = traceID
		}
	}
	if s.cfg.Encode.IncludeMetadata {
		for key, value := range e.Metadata {
			key = redisMetadataField(key)
			if key == "" || strings.TrimSpace(value) == "" {
				continue
			}
			fields[key] = value
		}
	}

	return fields, nil
}

func redisEventContext(e sdk.Event) map[string]any {
	meta := map[string]any{}
	for key, value := range e.Metadata {
		meta[key] = value
	}
	return map[string]any{
		"payload":  e.Payload,
		"metadata": meta,
		"meta":     meta,
	}
}

func getRedisEventPathValue(root map[string]any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, false
	}
	parts := strings.Split(path, ".")
	var current any = root
	for _, part := range parts {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		case map[string]string:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		default:
			return nil, false
		}
	}
	return current, true
}

func redisScalarString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprintf("%v", value)
	}
}

func redisMetadataField(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	key = redisFieldSanitizer.ReplaceAllString(key, "_")
	key = strings.Trim(key, "_")
	if key == "" {
		return ""
	}
	return "meta_" + key
}

func (s *RedisSink) publishBufferAck(e sdk.Event) error {
	if e.Metadata == nil {
		return nil
	}
	recordID := e.Metadata[bufferRecordIDMetadataKey]
	bufferNodeID := e.Metadata[bufferNodeIDMetadataKey]
	if recordID == "" || bufferNodeID == "" {
		return nil
	}
	if err := s.ctx.Emitter().PublishAck(sdk.AckMessage{
		RecordID:     recordID,
		BufferNodeID: bufferNodeID,
		EventID:      e.ID,
		Status:       sdk.AckStatusSuccess,
	}); err != nil {
		return fmt.Errorf("Redis Sink: publish ack: %w", err)
	}
	return nil
}

func (s *RedisSink) OnEvents(es []sdk.Event) error {
	for _, e := range es {
		if err := s.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisSink) watchResourceEvents() {
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

func (s *RedisSink) onResourceStatus(event sdk.ResourceStatusEvent) {
	s.setStatus(event.Status)
	s.reportStatus(event)
}

func (s *RedisSink) setStatus(status sdk.ResourceStatus) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.status = status
}

func (s *RedisSink) currentStatus() sdk.ResourceStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}
