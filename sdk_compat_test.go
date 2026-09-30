package plugins

import (
	"strings"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func TestOfficialPluginsUseSDKV1WireContract(t *testing.T) {
	handshake := sdk.DefaultHandshakeConfig()
	if handshake.ProtocolVersion != 1 || handshake.MagicCookieKey != "PUNK_PLUGIN_MAGIC" || handshake.MagicCookieValue != "punk-rule-engine" {
		t.Fatalf("unexpected SDK v1 handshake: %#v", handshake)
	}
	event := sdk.NewEvent(map[string]interface{}{"value": 1}, nil)
	if !strings.HasPrefix(event.ID, "evt_") {
		t.Fatalf("unexpected SDK event ID: %q", event.ID)
	}
	clone := event.Clone()
	clone.Payload["value"] = 2
	if event.Payload["value"] != 1 {
		t.Fatal("SDK Event.Clone did not isolate official plugin event ownership")
	}
}
