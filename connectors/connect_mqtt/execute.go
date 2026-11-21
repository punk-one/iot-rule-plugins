package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type mqttReadPayload struct {
	Topics        []string `json:"topics"`
	QoS           int      `json:"qos,omitempty"`
	WaitTimeoutMS int      `json:"wait_timeout_ms,omitempty"`
}

type mqttReadResponse struct {
	Messages []mqttInboundMessage `json:"messages"`
}

type mqttWritePayload struct {
	Topic         string `json:"topic"`
	QoS           int    `json:"qos,omitempty"`
	Retain        bool   `json:"retain,omitempty"`
	PayloadBase64 string `json:"payload_base64"`
}

func (c *mqttConnector) executeMetadata(resource *mqttResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	switch req.Target {
	case "", "info":
		return buildJSONResponse(map[string]any{
			"buffered_bytes":  resource.bufferedBytes.Load(),
			"receive_dropped": resource.dropped.Load(),
			"resource_id":     resource.resource.ID,
			"plugin_id":       c.Info().ID,
			"broker":          resource.cfg.Broker,
			"supported_targets": []string{
				"metadata/info",
				"metadata/ping",
				"read/messages",
				"write/publish",
			},
		}, map[string]string{
			"resource_id": resource.resource.ID,
			"plugin_id":   c.Info().ID,
		})
	case "ping":
		if _, err := c.ensureConnected(resource); err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("connect mqtt broker: %w", err)
		}
		return buildJSONResponse(map[string]any{"ok": true}, map[string]string{"status": "healthy"})
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported metadata target %q", req.Target)
	}
}

func (c *mqttConnector) probe(resource *mqttResource, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	if _, err := c.ensureConnected(resource); err != nil {
		return sdk.ResourceStatusEvent{}, fmt.Errorf("connect mqtt broker: %w", err)
	}
	return sdk.ResourceStatusEvent{
		ResourceID:         resource.resource.ID,
		ProviderPluginID:   c.Info().ID,
		Status:             sdk.ResourceStatusHealthy,
		ReasonCode:         "READY",
		Message:            "mqtt broker reachable",
		ObservedAtUnixNano: time.Now().UnixNano(),
	}, nil
}

func (c *mqttConnector) executeRead(resource *mqttResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	if len(req.Payload) > 65536 {
		return sdk.ConnectorResponse{}, fmt.Errorf("MQTT legacy read request exceeds bound")
	}
	var payload mqttReadPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse read payload: %w", err)
	}
	if len(payload.Topics) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("read.topics is required")
	}
	if req.ResourceToken == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("resource token is required")
	}

	binding, err := c.registerLegacyBinding(resource, req.ResourceToken, payload.Topics, payload.QoS)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if _, err := c.ensureConnected(resource); err != nil {
		return sdk.ConnectorResponse{}, err
	}
	for _, topic := range payload.Topics {
		c.flushOrphans(resource, topic)
	}
	ctx, cancel := context.WithTimeout(context.Background(), mqttReceiveWait(payload.WaitTimeoutMS))
	defer cancel()

	select {
	case <-binding.done:
		return sdk.ConnectorResponse{}, fmt.Errorf("MQTT resource stopped")
	case message := <-binding.inbox:
		resource.release(inboundCost(message))
		return buildJSONResponse(mqttReadResponse{Messages: []mqttInboundMessage{message}}, nil)
	case <-ctx.Done():
		return buildJSONResponse(mqttReadResponse{Messages: []mqttInboundMessage{}}, nil)
	}
}

func (c *mqttConnector) registerLegacyBinding(resource *mqttResource, token string, topics []string, qos int) (*mqttBinding, error) {
	if token == "" || len(token) > 512 || len(topics) < 1 || len(topics) > 64 || qos < 0 || qos > 2 {
		return nil, fmt.Errorf("invalid MQTT read binding")
	}
	for _, topic := range topics {
		if !validMQTTFilter(topic) {
			return nil, fmt.Errorf("invalid MQTT topic filter")
		}
	}
	resource.mu.Lock()
	defer resource.mu.Unlock()
	if resource.closed {
		return nil, fmt.Errorf("resource closed")
	}
	binding := resource.bindings[token]
	if binding == nil {
		if len(resource.bindings) >= 32 {
			return nil, fmt.Errorf("MQTT legacy binding capacity exceeded")
		}
		binding = &mqttBinding{topics: map[string]struct{}{}, inbox: make(chan mqttInboundMessage, min(resource.cfg.QueueSize, 256)), done: make(chan struct{})}
		resource.bindings[token] = binding
	}
	newTopics := make(map[string]struct{})
	for _, topic := range topics {
		if _, exists := binding.topics[topic]; !exists {
			newTopics[topic] = struct{}{}
		}
	}
	if len(binding.topics)+len(newTopics) > 64 {
		return nil, fmt.Errorf("MQTT legacy binding topic capacity exceeded")
	}
	for _, topic := range topics {
		binding.topics[topic] = struct{}{}
		state := resource.subscriptions[topic]
		if state == nil {
			state = &mqttSubscription{tokens: map[string]struct{}{}, sessions: map[string]struct{}{}}
			resource.subscriptions[topic] = state
		}
		state.qos = max(state.qos, byte(qos))
		state.tokens[token] = struct{}{}
	}
	return binding, nil
}

func (c *mqttConnector) executeWrite(resource *mqttResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	if len(req.Payload) > 1<<20 {
		return sdk.ConnectorResponse{}, fmt.Errorf("MQTT publish request exceeds bound")
	}
	var payload mqttWritePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse write payload: %w", err)
	}
	if stringsTrim(payload.Topic) == "" || len(payload.Topic) > 1024 || strings.ContainsAny(payload.Topic, "+#\x00") || payload.QoS < 0 || payload.QoS > 2 {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.topic is required")
	}

	data, err := base64.StdEncoding.DecodeString(payload.PayloadBase64)
	if err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("decode payload_base64: %w", err)
	}

	if len(data) > mqttPayloadLimit {
		return sdk.ConnectorResponse{}, fmt.Errorf("MQTT publish payload exceeds 256 KiB")
	}
	client, err := c.ensureConnected(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	token := client.Publish(payload.Topic, byte(payload.QoS), payload.Retain, data)
	timeout := 10 * time.Second
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	if !token.WaitTimeout(timeout) {
		return sdk.ConnectorResponse{}, fmt.Errorf("mqtt publish timeout after %s", timeout)
	}
	if err := token.Error(); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("mqtt publish: %w", err)
	}
	return buildJSONResponse(map[string]any{"published": true}, nil)
}
