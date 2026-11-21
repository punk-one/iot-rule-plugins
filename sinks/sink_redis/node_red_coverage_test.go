package main

import (
	"encoding/json"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestRedisSinkHashWritesExplicitTTL(t *testing.T) {
	connector := &sinkRedisConnectorStub{status: sdk.ResourceStatusEvent{Status: sdk.ResourceStatusHealthy}}
	sink := &RedisSink{ctx: sinkRedisRuntimeContext{connector: connector}}
	config := []byte(`{"connector_ref":"redis","target":{"mode":"hash","key":"fixture"},"encode":{"expand_object":"payload"},"options":{"ttl_seconds":45}}`)
	if err := sink.validateConfig(config); err != nil {
		t.Fatal(err)
	}
	sink.setStatus(sdk.ResourceStatusHealthy)
	if err := sink.OnEvent(sdk.NewEvent(map[string]any{"value": 1}, nil)); err != nil {
		t.Fatal(err)
	}
	if len(connector.writes) != 1 || connector.writes[0].Target != "hash_set" {
		t.Fatal("expected a single transactional hash write request")
	}
	var request redisHashWritePayload
	if err := json.Unmarshal(connector.writes[0].Payload, &request); err != nil || request.TTLSeconds != 45 {
		t.Fatalf("unexpected connector request %+v: %v", request, err)
	}
	var stream RedisSink
	if err := stream.validateConfig([]byte(`{"connector_ref":"redis","target":{"mode":"stream","stream":"fixture"},"options":{"ttl_seconds":45}}`)); err == nil {
		t.Fatal("accepted a silently ignored TTL on a stream")
	}
}
