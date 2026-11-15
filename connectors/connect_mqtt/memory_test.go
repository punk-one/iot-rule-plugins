package main

import (
	"context"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strings"
	"testing"
	"time"
)

func TestMQTTByteBudgetIncludesPendingAndDuplicateAckDoesNotMintCredits(t *testing.T) {
	c := NewConnector().(*mqttConnector)
	_, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: mustJSON(t, map[string]any{"broker": "tcp://127.0.0.1:1883"})})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	r, _ := c.getResource("r")
	s := newMQTTConsumerSession("test", sdk.StreamOpenRequest{InitialCredits: 1, MaxBufferedMessages: 4096, OverflowPolicy: "drop_oldest"})
	s.reserve = r.reserve
	s.release = r.release
	raw := []byte(strings.Repeat("x", 128<<10))
	for i := 0; i < 100; i++ {
		s.enqueue(mqttInboundMessage{Topic: "test", RawPayload: raw})
	}
	if s.residentBytes > mqttSessionByteLimit || r.bufferedBytes.Load() > mqttResourceByteLimit || c.bufferedBytes.Load() > mqttPackageByteLimit {
		t.Fatal("memory quota escaped")
	}
	messages, _ := s.receiveBatch(1)
	if len(messages) != 1 {
		t.Fatal("missing sample")
	}
	before := s.availableCredits
	s.ack(sdk.StreamAckRequest{DeliveryID: "unknown", Credits: 1000})
	if s.availableCredits != before {
		t.Fatal("unknown ACK minted credit")
	}
	s.nack(sdk.StreamNackRequest{DeliveryID: messages[0].DeliveryID, Requeue: true, Credits: 1})
	if s.residentBytes > mqttSessionByteLimit {
		t.Fatal("requeue changed memory accounting")
	}
	s.close()
	s.close()
	if r.bufferedBytes.Load() != 0 || c.bufferedBytes.Load() != 0 {
		t.Fatal("queue memory lease leaked", r.bufferedBytes.Load(), c.bufferedBytes.Load())
	}
}
func TestMQTTOverflowClosedSessionReturnsError(t *testing.T) {
	s := newMQTTConsumerSession("test", sdk.StreamOpenRequest{MaxBufferedMessages: 1, OverflowPolicy: "fail_session"})
	s.enqueue(mqttInboundMessage{RawPayload: []byte("a")})
	s.enqueue(mqttInboundMessage{RawPayload: []byte("b")})
	_, _ = s.receiveBatch(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.receive(ctx, 1); err == nil {
		t.Fatal("closed stream looked idle forever")
	}
	s.close()
}
func TestMQTTPersistentConfigAndFilterBoundaries(t *testing.T) {
	cfg, err := parseConnectionConfig([]byte(`{"broker":"tcp://127.0.0.1:1883","clean_session":false}`))
	if err != nil || cfg.cleanSession() {
		t.Fatal(cfg, err)
	}
	cfg, err = parseConnectionConfig([]byte(`{"broker":"tcp://127.0.0.1:1883"}`))
	if err != nil || !cfg.cleanSession() {
		t.Fatal(cfg, err)
	}
	for _, filter := range []string{"", "a/#/b", "a+b", "a/##", "a/\x00"} {
		if validMQTTFilter(filter) {
			t.Fatal("invalid filter accepted", filter)
		}
	}
	if topicMatches("#", "$SYS/test") || !topicMatches("demo/+/value", "demo/a/value") || !topicMatches("demo/#", "demo") {
		t.Fatal("wildcard semantics")
	}
}
