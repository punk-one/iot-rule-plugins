package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"testing"
	"time"
)

type pageClient struct {
	sdk.ConnectorClient
	read func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c pageClient) Read(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) { return c.read(r) }

type pageEmitter struct {
	sdk.Emitter
	events []sdk.Event
}

func (e *pageEmitter) Publish(v sdk.Event) error { e.events = append(e.events, v); return nil }

type pageRuntime struct {
	sdk.RuntimeContext
	c sdk.ConnectorClient
	e sdk.Emitter
}

func (r pageRuntime) Connector() sdk.ConnectorClient { return r.c }
func (r pageRuntime) Emitter() sdk.Emitter           { return r.e }
func pageResponse(body string, status int) sdk.ConnectorResponse {
	raw, _ := json.Marshal(map[string]any{"status": status, "content_type": "application/json", "body_base64": base64.StdEncoding.EncodeToString([]byte(body))})
	return sdk.ConnectorResponse{Payload: raw}
}
func pagePlugin(t *testing.T, read func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)) (*plugin, *pageEmitter) {
	t.Helper()
	p := &plugin{}
	err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"http","path":"/records","pagination":{"items_pointer":"/items","next_pointer":"/next"}}`)})
	if err != nil {
		t.Fatal(err)
	}
	out := &pageEmitter{}
	p.Start(pageRuntime{c: pageClient{read: read}, e: out})
	return p, out
}
func TestPaginationEmitsOneOwnedCompleteResult(t *testing.T) {
	calls := 0
	p, out := pagePlugin(t, func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		calls++
		var request struct {
			Path string `json:"path"`
		}
		json.Unmarshal(r.Payload, &request)
		if calls == 1 {
			if request.Path != "/records" {
				t.Fatal(request)
			}
			return pageResponse(`{"items":[{"id":9007199254740993}],"next":"/records?cursor=two"}`, 200), nil
		}
		if request.Path != "/records?cursor=two" {
			t.Fatal(request)
		}
		return pageResponse(`{"items":[{"id":2}],"next":null}`, 200), nil
	})
	original := sdk.Event{ID: "unchanged", Payload: map[string]any{"device": "test"}}
	if err := p.OnEvent(original); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(out.events) != 1 || out.events[0].ID != original.ID {
		t.Fatal(calls, out.events)
	}
	body := out.events[0].Payload["response"].(map[string]any)["body"].(map[string]any)
	if body["items"].([]any)[0].(map[string]any)["id"] != "9007199254740993" {
		t.Fatal(body)
	}
	if _, ok := original.Payload["response"]; ok {
		t.Fatal("input mutated")
	}
}
func TestPaginationFailureNeverEmitsPartialResult(t *testing.T) {
	for _, test := range []struct {
		name, second        string
		failure             bool
		pages, items, bytes int
	}{{name: "cycle", second: `{"items":[2],"next":"/records"}`}, {name: "network", failure: true}, {name: "shape", second: `{"items":{},"next":null}`}, {name: "missing-next", second: `{"items":[]}`}, {name: "remote", second: `{"items":[],"next":"https://other.example.invalid/path"}`}, {name: "max-pages", pages: 1}, {name: "max-items", items: 1}, {name: "max-bytes", bytes: 1}} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			p, out := pagePlugin(t, func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
				calls++
				if calls == 1 {
					return pageResponse(`{"items":[1],"next":"/next"}`, 200), nil
				}
				if test.failure {
					return sdk.ConnectorResponse{}, errors.New("offline")
				}
				return pageResponse(test.second, 200), nil
			})
			if test.pages > 0 {
				p.cfg.Pagination.MaxPages = test.pages
			}
			if test.items > 0 {
				p.cfg.Pagination.MaxItems = test.items
			}
			if test.bytes > 0 {
				p.cfg.Pagination.MaxBytes = test.bytes
			}
			if err := p.OnEvent(sdk.NewEvent(nil, nil)); err == nil || len(out.events) != 0 {
				t.Fatal("partial result emitted", err, out.events)
			}
		})
	}
}
func TestPaginationTotalDeadlineIncludesAllPages(t *testing.T) {
	p, out := pagePlugin(t, func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		time.Sleep(60 * time.Millisecond)
		return pageResponse(`{"items":[],"next":null}`, 200), nil
	})
	p.cfg.Pagination.TotalTimeoutMS = 50
	if err := p.OnEvent(sdk.NewEvent(nil, nil)); err == nil || len(out.events) != 0 {
		t.Fatal("overall deadline ignored")
	}
}
func TestPaginationPointersAndPaths(t *testing.T) {
	root := map[string]any{"a/b": []any{map[string]any{"~key": "ok"}}}
	if v, ok := pointerValue(root, "/a~1b/0/~0key"); !ok || v != "ok" {
		t.Fatal(v, ok)
	}
	for _, p := range []string{"/~2", "not-a-pointer"} {
		if _, err := pointerTokens(p); err == nil {
			t.Fatal(p)
		}
	}
	for _, p := range []string{"//other/path", "https://other/path", "/ok#fragment", "/path\\evil", "relative"} {
		if pagePath(p) {
			t.Fatal(p)
		}
	}
	if err := (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"http","path":"/records","method":"POST","pagination":{"next_pointer":"/next"}}`)}); err == nil {
		t.Fatal("write pagination accepted")
	}
}
