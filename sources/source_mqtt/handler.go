package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

var topicIndexRulePattern = regexp.MustCompile(`^topic\[(\d+)\]$`)

type mqttReadPayload struct {
	Topics        []string `json:"topics"`
	QoS           int      `json:"qos,omitempty"`
	WaitTimeoutMS int      `json:"wait_timeout_ms,omitempty"`
}

type mqttInboundMessage struct {
	Topic         string `json:"topic"`
	Subscription  string `json:"subscription,omitempty"`
	PayloadBase64 string `json:"payload_base64"`
	ReceivedAt    string `json:"received_at,omitempty"`
}

type mqttReadResponse struct {
	Messages []mqttInboundMessage `json:"messages"`
}

func (m *MQTTSource) openStream() error {
	raw, err := json.Marshal(mqttReadPayload{
		Topics:        m.cfg.Process.Subscribe.Topics,
		QoS:           m.cfg.Process.Subscribe.QoS,
		WaitTimeoutMS: m.cfg.Polling.WaitTimeoutMS,
	})
	if err != nil {
		return err
	}
	stream, err := m.ctx.Connector().OpenStream(sdk.StreamOpenRequest{
		ResourceRef:         m.cfg.ConnectorRef,
		SessionKey:          buildSourceSessionKey(m.ctx.RuleID(), m.ctx.NodeID(), m.cfg.ConnectorRef),
		Target:              "messages",
		Payload:             raw,
		InitialCredits:      m.cfg.Stream.InitialCredits,
		MaxBufferedMessages: m.cfg.Stream.MaxBufferedMessages,
		OverflowPolicy:      m.cfg.Stream.OverflowPolicy,
		AckTimeoutMS:        m.cfg.Stream.AckTimeoutMS,
		MaxDeliverBatch:     m.cfg.Stream.MaxDeliverBatch,
	})
	if err != nil {
		return mqttSourceRetryable(err)
	}
	if m.stopping() {
		_ = stream.Close()
		return fmt.Errorf("mqtt source is stopping")
	}
	m.setStream(stream)
	return nil
}

func (m *MQTTSource) consumeLoop() {
	backoff := 0
	for {
		if m.stopping() {
			return
		}
		stream := m.currentStream()
		if stream == nil {
			if err := m.openStream(); err != nil {
				if m.stopping() {
					return
				}
				m.ctx.Logger().Warn("MQTT Source: open stream error", sdk.Field{Key: "error", Value: err.Error()})
				m.reportPollError(err)
				if !m.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			stream = m.currentStream()
		}

		timeout := time.Duration(m.cfg.Polling.WaitTimeoutMS) * time.Millisecond
		message, ok, err := stream.Recv(timeout)
		if err != nil {
			m.ctx.Logger().Warn("MQTT Source: stream recv error", sdk.Field{Key: "error", Value: err.Error()})
			m.reportPollError(err)
			_ = m.closeStream(stream)
			if !m.sleepWithBackoff(backoff) {
				return
			}
			backoff++
			continue
		}
		if !ok {
			backoff = 0
			continue
		}

		err = m.publishStreamMessage(message)
		switch {
		case err == nil:
			if ackErr := stream.Ack(message.DeliveryID, 1); ackErr != nil {
				m.handleDeliveryFailure(stream, "ack", ackErr)
				if !m.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			m.reportHeartbeat(1)
			backoff = 0
		case sdk.IsSkippable(err):
			if ackErr := stream.Ack(message.DeliveryID, 1); ackErr != nil {
				m.handleDeliveryFailure(stream, "ack skipped message", ackErr)
				if !m.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			m.reportPollError(err)
		case sdk.IsRetryable(err):
			if nackErr := stream.Nack(message.DeliveryID, true, err.Error(), 1); nackErr != nil {
				m.handleDeliveryFailure(stream, "nack retryable message", nackErr)
				if !m.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			m.reportPollError(err)
		default:
			if nackErr := stream.Nack(message.DeliveryID, false, err.Error(), 1); nackErr != nil {
				m.handleDeliveryFailure(stream, "nack failed message", nackErr)
				if !m.sleepWithBackoff(backoff) {
					return
				}
				backoff++
				continue
			}
			m.reportPollError(err)
		}
	}
}

func (m *MQTTSource) handleDeliveryFailure(stream sdk.StreamConsumer, operation string, err error) {
	wrapped := mqttSourceRetryable(fmt.Errorf("%s: %w", operation, err))
	m.ctx.Logger().Error("MQTT Source: delivery confirmation failed",
		sdk.Field{Key: "operation", Value: operation},
		sdk.Field{Key: "error", Value: err.Error()},
	)
	m.reportPollError(wrapped)
	_ = m.closeStream(stream)
}

func (m *MQTTSource) sleepWithBackoff(backoff int) bool {
	delay := m.nextDelay(backoff)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-m.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

func (m *MQTTSource) pollLoop() {
	backoff := 0
	for {
		delay := m.nextDelay(backoff)
		timer := time.NewTimer(delay)
		select {
		case <-m.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		congested, err := m.poll()
		if err != nil {
			m.ctx.Logger().Warn("MQTT Source: poll error", sdk.Field{Key: "error", Value: err.Error()})
			m.reportPollError(err)
		}
		if err != nil || congested {
			backoff++
			continue
		}
		backoff = 0
	}
}

func (m *MQTTSource) poll() (bool, error) {
	if m.currentStatus() == sdk.ResourceStatusDisconnected || m.currentStatus() == sdk.ResourceStatusBlocked {
		return true, fmt.Errorf("connector status is %s", m.currentStatus())
	}

	raw, err := json.Marshal(mqttReadPayload{
		Topics:        m.cfg.Process.Subscribe.Topics,
		QoS:           m.cfg.Process.Subscribe.QoS,
		WaitTimeoutMS: m.cfg.Polling.WaitTimeoutMS,
	})
	if err != nil {
		return false, err
	}
	resp, err := m.ctx.Connector().Read(sdk.ConnectorRequest{
		Target:    "messages",
		Payload:   raw,
		TimeoutMS: m.cfg.Polling.WaitTimeoutMS,
	})
	if err != nil {
		return false, mqttSourceRetryable(err)
	}

	var decoded mqttReadResponse
	if err := json.Unmarshal(resp.Payload, &decoded); err != nil {
		return resp.Congested, err
	}
	for _, message := range decoded.Messages {
		if err := m.publishMessage(message); err != nil {
			return resp.Congested, err
		}
	}
	if len(decoded.Messages) > 0 {
		m.reportHeartbeat(len(decoded.Messages))
	}
	return resp.Congested, nil
}

func (m *MQTTSource) publishMessage(message mqttInboundMessage) error {
	payload, err := base64.StdEncoding.DecodeString(message.PayloadBase64)
	if err != nil {
		return mqttSourceSkippable(fmt.Errorf("decode mqtt payload: %w", err))
	}
	return m.publishPayload(message.Topic, payload)
}

func (m *MQTTSource) publishStreamMessage(message sdk.StreamMessage) error {
	return m.publishPayload(message.Topic, message.RawPayload)
}

func (m *MQTTSource) publishPayload(topic string, payload []byte) error {
	decodedPayload, err := m.decodePayload(payload)
	if err != nil {
		return err
	}

	event := sdk.NewEvent(decodedPayload, map[string]string{
		"protocol":   "mqtt",
		"mqtt_topic": topic,
		"is_source":  "true",
	})
	event.RuleID = m.ctx.RuleID()
	event.Source = m.ctx.NodeID()
	if sdk.GetTraceID(event) == "" {
		sdk.SetTraceID(&event, uuid.New().String())
	}

	m.applyExtractRules(&event, topic)

	if err := m.ctx.Emitter().Publish(event); err != nil {
		return mqttSourceRetryable(err)
	}
	return nil
}

func (m *MQTTSource) watchResourceEvents() {
	ch := m.ctx.ResourceEvents()
	if ch == nil {
		return
	}
	for {
		select {
		case <-m.stopCh:
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			m.onResourceStatus(event)
		}
	}
}

func (m *MQTTSource) onResourceStatus(event sdk.ResourceStatusEvent) {
	m.setStatus(event.Status)
	m.reportStatus(event)
}

func (m *MQTTSource) setStatus(status sdk.ResourceStatus) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	m.status = status
}

func (m *MQTTSource) currentStatus() sdk.ResourceStatus {
	m.statusMu.RLock()
	defer m.statusMu.RUnlock()
	return m.status
}

func (m *MQTTSource) nextDelay(backoff int) time.Duration {
	base := 200 * time.Millisecond
	switch m.currentStatus() {
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		base *= 2
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusBlocked:
		base *= 4
	}
	if backoff > 0 {
		base = base * time.Duration(1<<minInt(backoff, 4))
	}
	maxBackoff := time.Duration(m.cfg.Polling.MaxBackoffMS) * time.Millisecond
	if base > maxBackoff {
		return maxBackoff
	}
	return base
}

func (m *MQTTSource) applyExtractRules(event *sdk.Event, topic string) {
	if event == nil {
		return
	}
	for _, field := range m.cfg.Process.Extract.Fields {
		value := extractValueFromTopic(topic, field.From)
		if value == "" {
			continue
		}
		applyExtractTarget(event, field.To, value)
	}
}

func applyExtractTarget(event *sdk.Event, target string, value string) {
	switch {
	case strings.HasPrefix(target, "metadata."):
		key := strings.TrimPrefix(target, "metadata.")
		if key == "" {
			return
		}
		if _, exists := event.Metadata[key]; exists {
			return
		}
		event.Metadata[key] = value
	case strings.HasPrefix(target, "payload."):
		path := strings.Split(strings.TrimPrefix(target, "payload."), ".")
		setPayloadPathIfAbsent(event.Payload, path, value)
	}
}

func setPayloadPathIfAbsent(payload map[string]interface{}, path []string, value string) {
	if len(path) == 0 {
		return
	}
	current := payload
	for i := 0; i < len(path)-1; i++ {
		key := path[i]
		next, exists := current[key]
		if !exists {
			child := map[string]interface{}{}
			current[key] = child
			current = child
			continue
		}
		child, ok := next.(map[string]interface{})
		if !ok {
			return
		}
		current = child
	}
	leaf := path[len(path)-1]
	if _, exists := current[leaf]; exists {
		return
	}
	current[leaf] = value
}

func (m *MQTTSource) decodePayload(payload []byte) (map[string]interface{}, error) {
	format := m.cfg.Process.Decode.Format
	if format == "" {
		format = "auto"
	}
	switch format {
	case "json":
		var data map[string]interface{}
		if err := json.Unmarshal(payload, &data); err != nil {
			return nil, mqttSourceSkippable(fmt.Errorf("failed to parse JSON: %w", err))
		}
		return data, nil
	case "text":
		return map[string]interface{}{"data": string(payload)}, nil
	case "binary":
		return map[string]interface{}{"raw": base64.StdEncoding.EncodeToString(payload)}, nil
	default:
		var data map[string]interface{}
		if err := json.Unmarshal(payload, &data); err == nil {
			return data, nil
		}
		return map[string]interface{}{"data": string(payload)}, nil
	}
}

func extractTopicSegment(topic string, index int) string {
	parts := strings.Split(topic, "/")
	if index >= 0 && index < len(parts) {
		return parts[index]
	}
	return ""
}

func extractValueFromTopic(topic string, rule string) string {
	if rule == "" || topic == "" {
		return ""
	}
	matches := topicIndexRulePattern.FindStringSubmatch(rule)
	if matches == nil {
		return ""
	}
	index, err := strconv.Atoi(matches[1])
	if err != nil {
		return ""
	}
	return extractTopicSegment(topic, index)
}

func minInt(a int, b int) int {
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
