package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type client struct{ sdk.ConnectorClient }

func (client) Read(req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	if req.Target == "identity" {
		return sdk.ConnectorResponse{Payload: json.RawMessage(`{"identity":"test-database"}`)}, nil
	}
	var data struct {
		Params map[string]any `json:"params"`
	}
	json.Unmarshal(req.Payload, &data)
	if data.Params["watermark"] == "9007199254740993" {
		return sdk.ConnectorResponse{Payload: json.RawMessage(`{"rows":[]}`)}, nil
	}
	return sdk.ConnectorResponse{Payload: json.RawMessage(`{"rows":[{"id":"9007199254740993","device_code":"demo","value":12.5}]}`)}, nil
}

type emitter struct {
	sdk.Emitter
	events chan sdk.Event
}

func (e emitter) Publish(v sdk.Event) error { e.events <- v; return nil }

type metrics struct{ sdk.Metrics }

func (metrics) IncCounter(string, map[string]string) {}

type runtime struct {
	sdk.RuntimeContext
	out emitter
}

func (runtime) RuleID() string                 { return "rule" }
func (runtime) NodeID() string                 { return "source" }
func (runtime) Connector() sdk.ConnectorClient { return client{} }
func (r runtime) Emitter() sdk.Emitter         { return r.out }
func (runtime) Metrics() sdk.Metrics           { return metrics{} }
func TestIncrementalCursorPersistsAfterHandoff(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"connector_ref": "db", "query": "SELECT id,device_code,value FROM events WHERE id>@watermark ORDER BY id LIMIT @page_size", "mode": "incremental", "watermark_column": "id", "initial_watermark": "9007199254740992", "state_dir": t.TempDir(), "interval_ms": 100})
	events := make(chan sdk.Event, 4)
	ctx := runtime{out: emitter{events: events}}
	start := func() *plugin {
		p := &plugin{}
		if err := p.Init(sdk.PluginConfig{Raw: raw}); err != nil {
			t.Fatal(err)
		}
		if err := p.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := start()
	select {
	case e := <-events:
		if e.Payload["data"].(map[string]any)["id"] != "9007199254740993" {
			t.Fatal("cursor rounded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("row not emitted")
	}
	p.Stop()
	p = start()
	defer p.Stop()
	select {
	case <-events:
		t.Fatal("checkpointed row re-emitted")
	case <-time.After(250 * time.Millisecond):
	}
}
func TestCursorRejectsFractionalOrRoundedValues(t *testing.T) {
	for _, value := range []any{1.5, float64(9007199254740992), "invalid"} {
		if _, err := cursorValue(value); err == nil {
			t.Fatalf("unsafe cursor accepted: %v", value)
		}
	}
}
