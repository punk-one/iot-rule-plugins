package main

import (
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestConnectorInfoContainsSchema(t *testing.T) {
	info := NewConnector().Info()
	if info.Type != sdk.PluginTypeConnector {
		t.Fatalf("expected connector type, got %s", info.Type)
	}
	if strings.TrimSpace(info.Capabilities.ConfigSchema) == "" {
		t.Fatal("expected config schema")
	}
}

func TestCreateResourceAndMetadataInfo(t *testing.T) {
	connector := NewConnector()
	resource := sdk.ConnectorResource{
		ID:       "mqtt-1",
		PluginID: "connect-mqtt",
		Connection: mustJSON(t, map[string]any{
			"broker": "tcp://127.0.0.1:1883",
		}),
		Enabled: true,
	}
	handle, err := connector.CreateResource(resource)
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}
	defer func() { _ = connector.DestroyResource(handle) }()

	resp, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestMetadata,
		Target: "info",
	})
	if err != nil {
		t.Fatalf("metadata info: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["plugin_id"] != "connect-mqtt" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestCreateResourceValidatesBroker(t *testing.T) {
	connector := NewConnector()
	_, err := connector.CreateResource(sdk.ConnectorResource{
		ID:         "mqtt-invalid",
		PluginID:   "connect-mqtt",
		Connection: mustJSON(t, map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "connection.broker is required") {
		t.Fatalf("expected broker validation error, got %v", err)
	}
}

func TestMQTTConsumerSessionKeepsMessagesArrivingBeforeAck(t *testing.T) {
	session := newMQTTConsumerSession("stream-1", sdk.StreamOpenRequest{
		InitialCredits:      1,
		MaxBufferedMessages: 8,
		OverflowPolicy:      "drop_oldest",
	})

	session.enqueue(mqttInboundMessage{Topic: "factory/a", RawPayload: []byte(`{"seq":1}`)})
	session.enqueue(mqttInboundMessage{Topic: "factory/a", RawPayload: []byte(`{"seq":2}`)})

	first, closed := session.receiveBatch(1)
	if closed {
		t.Fatal("session closed unexpectedly on first receive")
	}
	if len(first) != 1 {
		t.Fatalf("expected first message, got %#v", first)
	}

	session.ack(sdk.StreamAckRequest{
		StreamID:   "stream-1",
		DeliveryID: first[0].DeliveryID,
		Credits:    1,
	})

	second, closed := session.receiveBatch(1)
	if closed {
		t.Fatal("session closed unexpectedly on second receive")
	}
	if len(second) != 1 {
		t.Fatalf("expected second message after ack, got %#v", second)
	}
	if string(second[0].RawPayload) != `{"seq":2}` {
		t.Fatalf("expected second payload to be preserved, got %s", string(second[0].RawPayload))
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
}
