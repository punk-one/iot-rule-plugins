package main

import (
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
)

func TestBufferedInputRejectedBeforeProcessing(t *testing.T) {
	p := &plugin{}
	if err := p.OnEvent(sdk.Event{Metadata: map[string]string{"buffer_record_id": "record"}}); err == nil {
		t.Fatal("buffered input accepted")
	}
}
