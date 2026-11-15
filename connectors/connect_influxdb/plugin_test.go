package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInfluxV2WriteQueryAndBounds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Token synthetic" {
			t.Error("missing token")
		}
		if r.URL.Query().Get("org") != "demo" {
			t.Error("missing org")
		}
		if r.URL.Path == "/api/v2/write" {
			body, _ := io.ReadAll(r.Body)
			if r.URL.Query().Get("precision") != "ms" || !strings.Contains(string(body), "temperature=1") {
				t.Error("invalid write")
			}
			w.WriteHeader(204)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		io.WriteString(w, ",result,table,_value\n,,0,9007199254740993\n")
	}))
	defer server.Close()
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"base_url": server.URL, "org": "demo", "bucket": "test", "token": "synthetic"})
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []sdk.ConnectorRequestKind{sdk.ConnectorRequestWrite, sdk.ConnectorRequestRead} {
		target := "write"
		raw = json.RawMessage(`{"lines":"demo temperature=1 1"}`)
		if kind == sdk.ConnectorRequestRead {
			target = "query"
			raw = json.RawMessage(`{"query":"from(bucket: params.bucket)","params":{"bucket":"test"}}`)
		}
		res, err := c.Execute("r", sdk.ConnectorRequest{Kind: kind, Target: target, Payload: raw})
		if err != nil {
			t.Fatal(err)
		}
		if kind == sdk.ConnectorRequestRead && !strings.Contains(string(res.Payload), "9007199254740993") {
			t.Fatal("integer lost")
		}
	}
	if _, err := parseCSV([]byte("a,b\n1,2\n3,4\n"), 1); err == nil {
		t.Fatal("unbounded rows")
	}
}
