package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestMQTTReceiveOwnsReturnedMessagesAndHonorsBatchLimit(t *testing.T) {
	stream := newMQTTConsumerSession("session", sdk.StreamOpenRequest{InitialCredits: 4, MaxDeliverBatch: 1})
	for i := 0; i < 2; i++ {
		if !stream.enqueue(mqttInboundMessage{Topic: "telemetry", RawPayload: []byte("one")}) {
			t.Fatal("fixture enqueue failed")
		}
	}
	first, _ := stream.receiveBatch(64)
	if len(first) != 1 {
		t.Fatalf("receive exceeded negotiated batch limit: %d", len(first))
	}
	first[0].RawPayload[0] = 'x'
	first[0].Metadata["mqtt_topic"] = "changed"
	stream.nack(sdk.StreamNackRequest{DeliveryID: first[0].DeliveryID, Requeue: true, Credits: 1})
	next, _ := stream.receiveBatch(1)
	if len(next) != 1 || string(next[0].RawPayload) != "one" || next[0].Metadata["mqtt_topic"] != "telemetry" {
		t.Fatal("receiver changed pending delivery")
	}
}

type receiveWaitContext struct {
	context.Context
	waiting chan<- struct{}
}

func (c receiveWaitContext) Done() <-chan struct{} {
	c.waiting <- struct{}{}
	return c.Context.Done()
}

func TestMQTTCloseWakesAllReceivers(t *testing.T) {
	stream := newMQTTConsumerSession("session", sdk.StreamOpenRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waiting := make(chan struct{}, 2)
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := stream.receive(receiveWaitContext{ctx, waiting}, 1); results <- err }()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-waiting:
		case <-ctx.Done():
			t.Fatal("receiver did not begin waiting")
		}
	}
	stream.close()
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			t.Fatal("receive returned an idle result on stream close")
		}
	}
}

func TestMQTTLegacyBindingCapsAccumulatedTopics(t *testing.T) {
	c := NewConnector().(*mqttConnector)
	resource := &mqttResource{bindings: make(map[string]*mqttBinding), subscriptions: make(map[string]*mqttSubscription)}
	topics := make([]string, 64)
	for i := range topics {
		topics[i] = fmt.Sprintf("topic/%d", i)
	}
	if _, err := c.registerLegacyBinding(resource, "token", topics, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.registerLegacyBinding(resource, "token", []string{"topic/64"}, 1); err == nil || len(resource.subscriptions) != 64 {
		t.Fatal("legacy binding allowed a 65th topic")
	}
}
