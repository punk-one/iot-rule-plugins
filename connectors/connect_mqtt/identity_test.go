package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestClientIDStableAcrossReconnectAndUniquePerGateway(t *testing.T) {
	r := &mqttResource{resource: sdk.ConnectorResource{ID: "mqtt-uplink"}}
	t.Setenv("IOT_RULE_INSTANCE_ID", "factory-a-gateway-01")
	one := stableClientID(r)
	if stableClientID(r) != one {
		t.Fatal("client ID changed on reconnect")
	}
	t.Setenv("IOT_RULE_INSTANCE_ID", "factory-a-gateway-02")
	if stableClientID(r) == one {
		t.Fatal("different gateways share a client ID")
	}
	r.cfg.ClientID = "explicit-client"
	if stableClientID(r) != "explicit-client" {
		t.Fatal("explicit client ID altered")
	}
}
