package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestParameterLookupAndEventIdentity(t *testing.T) {
	e := sdk.NewEvent(map[string]any{"data": map[string]any{"id": "key"}}, nil)
	if v, ok := resolve(e, "payload.data.id"); !ok || v != "key" {
		t.Fatal("payload parameter missing")
	}
	if v, ok := resolve(e, "event.id"); !ok || v != e.ID {
		t.Fatal("idempotency parameter missing")
	}
	if _, ok := resolve(e, "data.missing"); ok {
		t.Fatal("missing parameter accepted")
	}
}
