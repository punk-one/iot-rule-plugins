package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"crypto/rand"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (c *mqttConnector) openConsumerStream(resource *mqttResource, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	var payload mqttReadPayload
	if len(req.Payload) > 65536 || json.Unmarshal(req.Payload, &payload) != nil || len(payload.Topics) < 1 || len(payload.Topics) > 64 || payload.QoS < 0 || payload.QoS > 2 || req.InitialCredits < 0 || req.InitialCredits > 1024 || req.MaxBufferedMessages < 0 || req.MaxBufferedMessages > 4096 || req.MaxDeliverBatch < 0 || req.MaxDeliverBatch > 64 || len(req.SessionKey) > 256 {
		return sdk.StreamOpenResponse{}, fmt.Errorf("invalid bounded MQTT stream configuration")
	}
	for _, topic := range payload.Topics {
		if !validMQTTFilter(topic) {
			return sdk.StreamOpenResponse{}, fmt.Errorf("invalid MQTT topic filter")
		}
	}
	// Register demand and fanout targets before dialing so a persistent broker's
	// queued publications cannot arrive before the first Source exists.
	id := strings.TrimSpace(req.SessionKey) + ":" + rand.Text()
	session := newMQTTConsumerSession(id, req)
	session.reserve = resource.reserve
	session.release = resource.release
	resource.mu.Lock()
	if resource.closed || len(resource.sessions) >= 32 {
		resource.mu.Unlock()
		return sdk.StreamOpenResponse{}, fmt.Errorf("MQTT resource stopped or stream limit reached")
	}
	resource.sessions[id] = session
	for _, topic := range payload.Topics {
		session.topics[topic] = struct{}{}
		state := resource.subscriptions[topic]
		if state == nil {
			state = &mqttSubscription{qos: byte(payload.QoS), tokens: map[string]struct{}{}, sessions: map[string]struct{}{}}
			resource.subscriptions[topic] = state
		}
		state.qos = max(state.qos, byte(payload.QoS))
		state.sessions[id] = struct{}{}
	}
	resource.mu.Unlock()
	_, err := c.ensureConnected(resource)
	if err != nil {
		_ = c.closeConsumerStream(resource, sdk.StreamCloseRequest{StreamID: id})
		return sdk.StreamOpenResponse{}, err
	}
	for _, topic := range payload.Topics {
		c.flushOrphans(resource, topic)
	}
	// ensureConnected resubscribes the full current intent set on both new and
	// healthy connections. Subscription RPCs are bounded and outside resource.mu.
	return sdk.StreamOpenResponse{StreamID: id, DeliverySemantics: sdk.StreamDeliveryAtLeastOnce, InitialCredits: session.availableCredits}, nil
}

func (c *mqttConnector) closeConsumerStream(resource *mqttResource, req sdk.StreamCloseRequest) error {
	resource.subscriptionMu.Lock()
	defer resource.subscriptionMu.Unlock()
	resource.mu.Lock()
	session := resource.sessions[req.StreamID]
	if session == nil {
		resource.mu.Unlock()
		return nil
	}
	delete(resource.sessions, req.StreamID)
	topics := []string{}
	for topic := range session.topics {
		state := resource.subscriptions[topic]
		if state == nil {
			continue
		}
		delete(state.sessions, session.id)
		if len(state.tokens) == 0 && len(state.sessions) == 0 {
			delete(resource.subscriptions, topic)
			topics = append(topics, resource.effectiveTopic(topic))
		}
	}
	client := resource.client
	resource.mu.Unlock()
	session.close()
	if mqttClientReady(client) && len(topics) > 0 {
		return waitMQTT(client.Unsubscribe(topics...))
	}
	return nil
}

func (c *mqttConnector) ensureSessionSubscription(resource *mqttResource, session *mqttConsumerSession, topics []string, qos byte, client mqtt.Client) error {
	// Compatibility helper for callers within this plugin. The normal stream
	// registration path installs all intent before connecting.
	resource.mu.Lock()
	for _, topic := range topics {
		if !validMQTTFilter(topic) {
			resource.mu.Unlock()
			return fmt.Errorf("invalid MQTT topic filter")
		}
		session.topics[topic] = struct{}{}
		state := resource.subscriptions[topic]
		if state == nil {
			state = &mqttSubscription{tokens: map[string]struct{}{}, sessions: map[string]struct{}{}}
			resource.subscriptions[topic] = state
		}
		state.qos = max(state.qos, qos)
		state.sessions[session.id] = struct{}{}
	}
	resource.mu.Unlock()
	return c.resubscribeAll(resource, client)
}
