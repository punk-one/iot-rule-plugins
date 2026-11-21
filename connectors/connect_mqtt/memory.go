package main

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const mqttSessionByteLimit = 4 << 20
const mqttResourceByteLimit = 8 << 20
const mqttPackageByteLimit = 64 << 20
const mqttPayloadLimit = 256 << 10

func reserveCounter(counter *atomic.Int64, n, limit int64) bool {
	for {
		current := counter.Load()
		if n < 0 || n > limit-current {
			return false
		}
		if counter.CompareAndSwap(current, current+n) {
			return true
		}
	}
}
func (r *mqttResource) reserve(n int) bool {
	if r.retired.Load() || !reserveCounter(&r.bufferedBytes, int64(n), mqttResourceByteLimit) {
		return false
	}
	if r.owner != nil && !reserveCounter(&r.owner.bufferedBytes, int64(n), mqttPackageByteLimit) {
		r.bufferedBytes.Add(-int64(n))
		return false
	}
	return true
}
func (r *mqttResource) release(n int) {
	r.bufferedBytes.Add(-int64(n))
	if r.owner != nil {
		r.owner.bufferedBytes.Add(-int64(n))
	}
}
func inboundCost(m mqttInboundMessage) int {
	return len(m.RawPayload) + len(m.PayloadBase64) + len(m.Topic) + len(m.Subscription) + len(m.ReceivedAt) + 512
}
func streamCost(mRaw []byte, topic, subscription, at string) int {
	return len(mRaw) + len(topic) + len(subscription) + len(at) + 512
}

func validMQTTFilter(filter string) bool {
	if filter == "" || len(filter) > 1024 || !utf8.ValidString(filter) || strings.ContainsRune(filter, 0) {
		return false
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return false
		}
		if strings.Contains(level, "+") && level != "+" {
			return false
		}
	}
	return true
}
func topicMatches(filter, topic string) bool {
	if strings.HasPrefix(topic, "$") && !strings.HasPrefix(filter, "$") {
		return false
	}
	filters, levels := strings.Split(filter, "/"), strings.Split(topic, "/")
	for i, part := range filters {
		if part == "#" {
			return true
		}
		if i >= len(levels) || part != "+" && part != levels[i] {
			return false
		}
	}
	return len(filters) == len(levels)
}

func (c *mqttConnector) message(resource *mqttResource, subscription string, msg mqtt.Message) {
	if resource.retired.Load() {
		return
	}
	if len(msg.Payload()) > mqttPayloadLimit || len(msg.Topic()) > 1024 {
		resource.dropped.Add(1)
		return
	}
	inbound := mqttInboundMessage{Topic: msg.Topic(), Subscription: subscription, RawPayload: append([]byte(nil), msg.Payload()...), PayloadBase64: base64.StdEncoding.EncodeToString(msg.Payload()), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	resource.mu.Lock()
	state := resource.subscriptions[subscription]
	targets := []*mqttBinding{}
	sessions := []*mqttConsumerSession{}
	if state != nil {
		for token := range state.tokens {
			if binding := resource.bindings[token]; binding != nil {
				targets = append(targets, binding)
			}
		}
		for id := range state.sessions {
			if session := resource.sessions[id]; session != nil {
				sessions = append(sessions, session)
			}
		}
	}
	if subscription == "" {
		// A persistent broker can send queued data before later Sources have opened.
		// Keep a bounded pre-routing queue; do not silently discard those early packets.
		cost := inboundCost(inbound)
		if len(resource.orphans) >= min(resource.cfg.QueueSize, 256) || !resource.reserve(cost) {
			resource.dropped.Add(1)
		} else {
			resource.orphans = append(resource.orphans, inbound)
		}
		resource.mu.Unlock()
		return
	}
	resource.mu.Unlock()
	for _, binding := range targets {
		c.enqueueBinding(resource, binding, inbound)
	}
	for _, session := range sessions {
		if !session.enqueue(inbound) {
			resource.dropped.Add(1)
		}
	}
}
func (r *mqttResource) closeBinding(binding *mqttBinding) {
    binding.mu.Lock()
    defer binding.mu.Unlock()
    if binding.closed {
        return
    }
    binding.closed = true
    close(binding.done)
    // No callback can enqueue after the lock-protected retirement.
    for {
        select {
        case message := <-binding.inbox:
            r.release(inboundCost(message))
        default:
            return
        }
    }
}

func (c *mqttConnector) enqueueBinding(resource *mqttResource, binding *mqttBinding, m mqttInboundMessage) {
    binding.mu.Lock()
    defer binding.mu.Unlock()
    if binding.closed {
        return
    }
    cost := inboundCost(m)
	if !resource.reserve(cost) {
		resource.dropped.Add(1)
		return
	}
	select {
	case binding.inbox <- m:
		return
	default:
	}
	select {
	case old := <-binding.inbox:
		resource.release(inboundCost(old))
		resource.dropped.Add(1)
	default:
	}
	select {
	case binding.inbox <- m:
	default:
		resource.release(cost)
		resource.dropped.Add(1)
	}
}
func (c *mqttConnector) flushOrphans(resource *mqttResource, topic string) {
	resource.mu.Lock()
	ready := []mqttInboundMessage{}
	keep := []mqttInboundMessage{}
	for _, m := range resource.orphans {
		if topicMatches(topic, m.Topic) {
			resource.release(inboundCost(m))
			ready = append(ready, m)
		} else {
			keep = append(keep, m)
		}
	}
	resource.orphans = keep
	resource.mu.Unlock()
	for _, m := range ready {
		c.makeMessageHandler(resource, topic)(nil, &storedMQTTMessage{topic: m.Topic, payload: m.RawPayload})
	}
}

// Only used to feed already validated early packets through the ordinary fanout.
type storedMQTTMessage struct {
	topic   string
	payload []byte
}

func (m *storedMQTTMessage) Duplicate() bool   { return false }
func (m *storedMQTTMessage) Qos() byte         { return 1 }
func (m *storedMQTTMessage) Retained() bool    { return false }
func (m *storedMQTTMessage) Topic() string     { return m.topic }
func (m *storedMQTTMessage) MessageID() uint16 { return 0 }
func (m *storedMQTTMessage) Payload() []byte   { return m.payload }
func (m *storedMQTTMessage) Ack()              {}

func waitMQTT(token mqtt.Token) error {
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("MQTT operation deadline exceeded")
	}
	return token.Error()
}
