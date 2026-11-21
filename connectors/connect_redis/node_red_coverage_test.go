package main

import (
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	redislib "github.com/redis/go-redis/v9"
)

func TestReadLimitsAndPoolInfoDoNotDial(t *testing.T) {
	opened := 0
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		opened++
		return &redisClientStub{}, nil
	})
	defer restore()
	connector := NewConnector().(*redisConnector)
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID: "redis", Connection: json.RawMessage(`{"addr":"127.0.0.1:6379","pool_size":7}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	resource, err := connector.getResource(handle)
	if err != nil {
		t.Fatal(err)
	}
	info, err := connector.Execute(handle, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestMetadata, Target: "info"})
	if err != nil || !strings.Contains(string(info.Payload), `"pool":{"max_active_connections":7,"open":false}`) {
		t.Fatalf("unexpected pool diagnostics %s: %v", info.Payload, err)
	}
	for _, msg := range []string{`{"stream":"s","count":1025}`, `{"stream":"s","block_timeout_ms":10001}`} {
		if _, err := connector.Execute(handle, sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(msg)}); err == nil {
			t.Fatalf("oversized read accepted: %s", msg)
		}
		if _, err := connector.OpenStream(handle, sdk.StreamOpenRequest{Payload: json.RawMessage(msg)}); err == nil {
			t.Fatalf("oversized stream accepted: %s", msg)
		}
	}
	if err := connector.DestroyResource(handle); err != nil {
		t.Fatal(err)
	}
	if _, err := connector.openClient(resource); err == nil {
		t.Fatal("retired resource remained usable")
	}
	if opened != 0 {
		t.Fatalf("expected reserved read or metadata never to connect, opened=%d", opened)
	}
}

func TestStreamCapacityAndPendingMessageOwnership(t *testing.T) {
	stub := &redisClientStub{xreadResponses: [][]redislib.XStream{{{
		Stream: "s", Messages: []redislib.XMessage{{ID: "1-0", Values: map[string]any{"x": "first"}}},
	}}}}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) { return stub, nil })
	defer restore()
	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{ID: "redis", Connection: json.RawMessage(`{"addr":"127.0.0.1:6379"}`)})
	if err != nil {
		t.Fatal(err)
	}
	defer connector.DestroyResource(handle)
	session, err := connector.OpenStream(handle, sdk.StreamOpenRequest{
		Payload: json.RawMessage(`{"stream":"s","start_id":"0-0","count":100}`), InitialCredits: 10, MaxBufferedMessages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := sdk.StreamReceiveRequest{StreamID: session.StreamID, MaxMessages: 100}
	response, err := connector.ReceiveStream(handle, req)
	if err != nil || len(response.Messages) != 1 || stub.xreadArgs[0].Count != 1 || session.InitialCredits != 1 {
		t.Fatalf("unexpected first receive %+v, %v", response, err)
	}
	message := response.Messages[0]
	message.RawPayload[0] = 'x'
	message.Metadata["redis_stream"] = "changed"
	if err := connector.AckStream(handle, sdk.StreamAckRequest{StreamID: session.StreamID, DeliveryID: "unknown-id", Credits: 10}); err != nil {
		t.Fatal(err)
	}
	response, err = connector.ReceiveStream(handle, req)
	if err != nil || len(response.Messages) != 0 || len(stub.xreadArgs) != 1 {
		t.Fatalf("full session read again: %v", err)
	}
	if err := connector.NackStream(handle, sdk.StreamNackRequest{StreamID: session.StreamID, DeliveryID: message.DeliveryID, Requeue: true, Credits: 1}); err != nil {
		t.Fatal(err)
	}
	response, err = connector.ReceiveStream(handle, req)
	if err != nil || len(response.Messages) != 1 {
		t.Fatalf("redelivery missing: %v", err)
	}
	actual := response.Messages[0]
	if actual.DeliveryID != message.DeliveryID || !json.Valid(actual.RawPayload) || actual.Metadata["redis_stream"] != "s" {
		t.Fatalf("consumer mutated pending message: %+v", actual)
	}
}
