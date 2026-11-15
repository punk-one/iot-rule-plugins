package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type config struct {
	BaseURL     string `json:"base_url"`
	TimeoutMS   int    `json:"timeout_ms"`
	MaxBytes    int64  `json:"max_response_bytes"`
	BearerToken string `json:"bearer_token"`
}
type request struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Body    json.RawMessage   `json:"body,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}
type resource struct {
	cfg       config
	base      *url.URL
	ctx       context.Context
	cancel    context.CancelFunc
	client    *http.Client
	transport *http.Transport
}
type connector struct {
	mu        sync.RWMutex
	resources map[string]*resource
	stopped   bool
}

func NewConnector() sdk.ConnectorPlugin { return &connector{resources: map[string]*resource{}} }
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-http", Name: "HTTP Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Bound-origin HTTP connection pool with size limits and redirect protection", Capabilities: sdk.PluginCapabilities{ConfigSchema: `{"type":"object","required":["base_url"],"properties":{"base_url":{"type":"string","format":"uri"},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"max_response_bytes":{"type":"integer","minimum":1,"maximum":8388608,"default":1048576},"bearer_token":{"type":"string","format":"password"}},"additionalProperties":false}`, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "http", Label: "HTTP", Capabilities: []string{"read", "write", "shared_duplex", "session:pooled"}, DisplayFields: []string{"base_url"}}}}
}
func (c *connector) CreateResource(r sdk.ConnectorResource) (string, error) {
	cfg := config{TimeoutMS: 5000, MaxBytes: 1048576}
	if err := json.Unmarshal(r.Connection, &cfg); err != nil {
		return "", errors.New("invalid HTTP config")
	}
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil || base.Fragment != "" || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxBytes < 1 || cfg.MaxBytes > 8388608 || r.ID == "" {
		return "", errors.New("invalid HTTP connection settings")
	}
	if cfg.BearerToken != "" && base.Scheme != "https" && base.Hostname() != "127.0.0.1" && base.Hostname() != "localhost" && base.Hostname() != "::1" {
		return "", errors.New("bearer authentication requires HTTPS")
	}
	ctx, cancel := context.WithCancel(context.Background())
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 8
	tr.MaxConnsPerHost = 8
	client := &http.Client{Transport: tr, Timeout: time.Duration(cfg.TimeoutMS) * time.Millisecond, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many redirects")
		}
		if req.URL.Scheme != base.Scheme || !strings.EqualFold(req.URL.Host, base.Host) {
			return errors.New("HTTP redirect changed origin")
		}
		return nil
	}}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		cancel()
		tr.CloseIdleConnections()
		return "", errors.New("connector stopped")
	}
	if _, exists := c.resources[r.ID]; exists {
		cancel()
		tr.CloseIdleConnections()
		return "", errors.New("resource already exists")
	}
	c.resources[r.ID] = &resource{cfg: cfg, base: base, ctx: ctx, cancel: cancel, client: client, transport: tr}
	return r.ID, nil
}
func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	r := c.resources[h]
	delete(c.resources, h)
	c.mu.Unlock()
	if r != nil {
		r.cancel()
		r.transport.CloseIdleConnections()
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.stopped = true
	for h, r := range c.resources {
		r.cancel()
		r.transport.CloseIdleConnections()
		delete(c.resources, h)
	}
	c.mu.Unlock()
	return nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	if len(req.Payload) > 1048576 {
		return sdk.ConnectorResponse{}, errors.New("request exceeds 1 MiB")
	}
	var spec request
	dec := json.NewDecoder(bytes.NewReader(req.Payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid HTTP request")
	}
	switch spec.Method {
	case "GET", "HEAD", "OPTIONS":
		if req.Kind != sdk.ConnectorRequestRead {
			return sdk.ConnectorResponse{}, errors.New("read request kind required")
		}
	case "POST", "PUT", "PATCH", "DELETE":
		if req.Kind != sdk.ConnectorRequestWrite {
			return sdk.ConnectorResponse{}, errors.New("write request kind required")
		}
	default:
		return sdk.ConnectorResponse{}, errors.New("unsupported HTTP method")
	}
	ref, err := url.Parse(spec.Path)
	if err != nil || ref.IsAbs() || ref.Host != "" || ref.User != nil || ref.Fragment != "" {
		return sdk.ConnectorResponse{}, errors.New("path must be a relative URL on the configured origin")
	}
	endpoint := r.base.ResolveReference(ref)
	timeout := time.Duration(r.cfg.TimeoutMS) * time.Millisecond
	if req.TimeoutMS > 0 && time.Duration(req.TimeoutMS)*time.Millisecond < timeout {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, spec.Method, endpoint.String(), bytes.NewReader(spec.Body))
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid request")
	}
	if len(spec.Headers) > 16 {
		return sdk.ConnectorResponse{}, errors.New("too many request headers")
	}
	for k, v := range spec.Headers {
		switch http.CanonicalHeaderKey(k) {
		case "Accept", "Content-Type", "If-None-Match", "If-Modified-Since", "Idempotency-Key":
		default:
			return sdk.ConnectorResponse{}, errors.New("request header is not allowed")
		}
		if len(v) > 4096 || strings.ContainsAny(v, "\r\n") {
			return sdk.ConnectorResponse{}, errors.New("invalid request header")
		}
		httpReq.Header.Set(k, v)
	}
	if len(spec.Body) > 0 && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	if r.cfg.BearerToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+r.cfg.BearerToken)
	}
	response, err := r.client.Do(httpReq)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("HTTP request failed; write outcome may be unknown")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, r.cfg.MaxBytes+1))
	if err != nil || int64(len(body)) > r.cfg.MaxBytes {
		return sdk.ConnectorResponse{}, errors.New("HTTP response failed or exceeded configured size")
	}
	raw, err := json.Marshal(map[string]any{"status": response.StatusCode, "body_base64": base64.StdEncoding.EncodeToString(body), "content_type": response.Header.Get("Content-Type"), "etag": response.Header.Get("ETag"), "last_modified": response.Header.Get("Last-Modified")})
	return sdk.ConnectorResponse{Payload: raw}, err
}
func (c *connector) Probe(h string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ResourceStatusEvent{}, errors.New("unknown resource")
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusHealthy, ReasonCode: "DEMAND_MANAGED", Message: "configured; actual requests verify reachability", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("HTTP streaming not supported")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("HTTP streaming not supported")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error {
	return errors.New("streaming not supported")
}
func (c *connector) NackStream(string, sdk.StreamNackRequest) error {
	return errors.New("streaming not supported")
}
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("streaming not supported")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }
