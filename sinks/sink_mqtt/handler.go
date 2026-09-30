package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type mqttWritePayload struct {
	Topic         string `json:"topic"`
	QoS           int    `json:"qos,omitempty"`
	Retain        bool   `json:"retain,omitempty"`
	PayloadBase64 string `json:"payload_base64"`
}

type compiledTopicTemplate struct {
	segments []topicSegment
}

type topicSegment struct {
	literal string
	expr    string
}

func (s *MQTTSink) OnEvent(e sdk.Event) error {
	if s.currentStatus() == sdk.ResourceStatusDisconnected || s.currentStatus() == sdk.ResourceStatusBlocked || s.currentStatus() == sdk.ResourceStatusReadonly {
		return mqttSinkRetryable(fmt.Errorf("mqtt sink connector status is %s", s.currentStatus()))
	}

	topic, err := s.topicTemplate.Render(e)
	if err != nil {
		return mqttSinkSkippable(err)
	}
	payload, err := s.encodeEvent(e)
	if err != nil {
		return mqttSinkSkippable(err)
	}

	raw, err := json.Marshal(mqttWritePayload{
		Topic:         topic,
		QoS:           s.cfg.Target.QoS,
		Retain:        s.cfg.Target.Retain,
		PayloadBase64: base64.StdEncoding.EncodeToString(payload),
	})
	if err != nil {
		return mqttSinkSkippable(err)
	}

	if _, err := s.ctx.Connector().Write(sdk.ConnectorRequest{
		Target:    "publish",
		Payload:   raw,
		TimeoutMS: s.cfg.Options.WriteTimeoutSec * 1000,
	}); err != nil {
		s.reportPublishFailure(err)
		return mqttSinkRetryable(err)
	}

	s.reportPublishSuccess()
	s.reportHeartbeat()
	if err := s.publishBufferAck(e); err != nil {
		return mqttSinkSkippable(err)
	}
	return nil
}

func (s *MQTTSink) publishBufferAck(e sdk.Event) error {
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
		return fmt.Errorf("MQTT Sink: publish ack: %w", err)
	}
	return nil
}

func (s *MQTTSink) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := s.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *MQTTSink) watchResourceEvents() {
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

func (s *MQTTSink) onResourceStatus(event sdk.ResourceStatusEvent) {
	s.setStatus(event.Status)
	s.reportStatus(event)
}

func (s *MQTTSink) setStatus(status sdk.ResourceStatus) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.status = status
}

func (s *MQTTSink) currentStatus() sdk.ResourceStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

func (s *MQTTSink) encodeEvent(e sdk.Event) ([]byte, error) {
	switch s.cfg.Encode.Mode {
	case "event":
		envelope := map[string]any{
			"id":        e.ID,
			"rule_id":   e.RuleID,
			"source":    e.Source,
			"timestamp": e.Timestamp.UTC().Format(time.RFC3339Nano),
			"payload":   e.Payload,
			"metadata":  e.Metadata,
			"trace_id":  sdk.GetTraceID(e),
		}
		return json.Marshal(envelope)
	default:
		return json.Marshal(e.Payload)
	}
}

func compileTopicTemplate(template string) (compiledTopicTemplate, error) {
	segments := make([]topicSegment, 0, 4)
	for len(template) > 0 {
		start := strings.Index(template, "${")
		if start < 0 {
			segments = append(segments, topicSegment{literal: template})
			break
		}
		if start > 0 {
			segments = append(segments, topicSegment{literal: template[:start]})
		}
		end := strings.Index(template[start+2:], "}")
		if end < 0 {
			return compiledTopicTemplate{}, fmt.Errorf("topic template contains an unclosed expression")
		}
		expr := strings.TrimSpace(template[start+2 : start+2+end])
		if expr == "" {
			return compiledTopicTemplate{}, fmt.Errorf("topic template contains an empty expression")
		}
		segments = append(segments, topicSegment{expr: expr})
		template = template[start+2+end+1:]
	}
	return compiledTopicTemplate{segments: segments}, nil
}

func (t compiledTopicTemplate) Render(event sdk.Event) (string, error) {
	var builder strings.Builder
	for _, segment := range t.segments {
		if segment.expr == "" {
			builder.WriteString(segment.literal)
			continue
		}
		value, ok := resolveTopicExpression(event, segment.expr)
		if !ok {
			return "", fmt.Errorf("topic expression %q resolved empty", segment.expr)
		}
		builder.WriteString(value)
	}
	return builder.String(), nil
}

func resolveTopicExpression(event sdk.Event, expr string) (string, bool) {
	parts := strings.Split(expr, ".")
	if len(parts) == 0 {
		return "", false
	}
	switch parts[0] {
	case "payload":
		value, ok := getNestedValue(event.Payload, parts[1:])
		stringValue := scalarString(value)
		return stringValue, ok && stringValue != ""
	case "metadata":
		if len(parts) < 2 || event.Metadata == nil {
			return "", false
		}
		value := strings.TrimSpace(event.Metadata[strings.Join(parts[1:], ".")])
		return value, value != ""
	case "event":
		if len(parts) != 2 {
			return "", false
		}
		switch parts[1] {
		case "id":
			return event.ID, event.ID != ""
		case "rule_id":
			return event.RuleID, event.RuleID != ""
		case "source":
			return event.Source, event.Source != ""
		case "trace_id":
			value := sdk.GetTraceID(event)
			return value, value != ""
		default:
			return "", false
		}
	default:
		return "", false
	}
}

func getNestedValue(root map[string]any, parts []string) (any, bool) {
	if len(parts) == 0 {
		return root, root != nil
	}
	var current any = root
	for _, part := range parts {
		typed, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = typed[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func scalarString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return fmt.Sprintf("%v", typed)
	}
}
