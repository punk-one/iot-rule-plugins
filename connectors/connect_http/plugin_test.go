package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPOriginRedirectAndSizeBoundaries(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("cross-origin request reached destination") }))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, other.URL, http.StatusFound)
		case "/big":
			w.Write([]byte("01234567890123456789"))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer server.Close()
	c := NewConnector()
	defer c.Stop()
	raw, _ := json.Marshal(map[string]any{"base_url": server.URL, "max_response_bytes": 16})
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: raw}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{other.URL, "/redirect", "/big"} {
		spec, _ := json.Marshal(request{Method: "GET", Path: path})
		if _, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: spec}); err == nil {
			t.Fatalf("unsafe request %s accepted", path)
		}
	}
	response, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"method":"GET","path":"/ok"}`)})
	if err != nil || len(response.Payload) == 0 {
		t.Fatalf("valid request failed: %v", err)
	}
}
func TestHTTPRequestKindAndCredentialProtection(t *testing.T) {
	c := NewConnector()
	defer c.Stop()
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "bad", Connection: json.RawMessage(`{"base_url":"http://example.invalid","bearer_token":"test-only"}`)}); err == nil {
		t.Fatal("plaintext remote bearer accepted")
	}
	if _, err := c.CreateResource(sdk.ConnectorResource{ID: "r", Connection: json.RawMessage(`{"base_url":"http://127.0.0.1:1"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Execute("r", sdk.ConnectorRequest{Kind: sdk.ConnectorRequestRead, Payload: json.RawMessage(`{"method":"POST","path":"/"}`)}); err == nil {
		t.Fatal("write accepted through read path")
	}
}
