package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	BaseURL   string `json:"base_url"`
	TimeoutMS int    `json:"timeout_ms"`
	MaxBytes  int64  `json:"max_response_bytes"`
	Token     string `json:"token"`
	Org       string `json:"org"`
	Bucket    string `json:"bucket"`
	MaxRows   int    `json:"max_rows"`
}
type request struct {
	Lines  string         `json:"lines"`
	Query  string         `json:"query"`
	Params map[string]any `json:"params"`
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
	return sdk.PluginInfo{ID: "connect-influxdb", Name: "InfluxDB 2 Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Bounded InfluxDB OSS 2.x InfluxDB write and Flux query pool", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "influxdb", Label: "InfluxDB 2", Capabilities: []string{"read", "write", "shared_duplex", "session:pooled"}, DisplayFields: []string{"base_url", "org", "bucket"}}}}
}
func (c *connector) CreateResource(r sdk.ConnectorResource) (string, error) {
	cfg := config{TimeoutMS: 5000, MaxBytes: 1048576, MaxRows: 1000}
	if err := json.Unmarshal(r.Connection, &cfg); err != nil {
		return "", errors.New("invalid InfluxDB config")
	}
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil || base.Fragment != "" || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxBytes < 1 || cfg.MaxBytes > 8388608 || r.ID == "" || cfg.Org == "" || cfg.Bucket == "" || cfg.MaxRows < 1 || cfg.MaxRows > 10000 || base.RawQuery != "" || base.Path != "" && base.Path != "/" {
		return "", errors.New("invalid InfluxDB connection settings")
	}
	if cfg.Token != "" && base.Scheme != "https" && base.Hostname() != "127.0.0.1" && base.Hostname() != "localhost" && base.Hostname() != "::1" {
		return "", errors.New("bearer authentication requires InfluxDBS")
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
			return errors.New("InfluxDB redirect changed origin")
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
		return sdk.ConnectorResponse{}, errors.New("invalid InfluxDB request")
	}
	path := "/api/v2/query"
	parameters := url.Values{"org": {r.cfg.Org}}
	var body []byte
	var err error
	contentType := "application/json"
	switch req.Kind {
	case sdk.ConnectorRequestWrite:
		if req.Target != "write" || len(spec.Lines) == 0 || len(spec.Lines) > 1048576 || spec.Query != "" || strings.ContainsRune(spec.Lines, '\x00') {
			return sdk.ConnectorResponse{}, errors.New("invalid line protocol write")
		}
		path = "/api/v2/write"
		parameters.Set("bucket", r.cfg.Bucket)
		parameters.Set("precision", "ms")
		body = []byte(spec.Lines)
		contentType = "text/plain; charset=utf-8"
	case sdk.ConnectorRequestRead:
		if req.Target != "query" || spec.Query == "" || len(spec.Query) > 65536 || spec.Lines != "" || len(spec.Params) > 128 {
			return sdk.ConnectorResponse{}, errors.New("invalid Flux query")
		}
		for name, v := range spec.Params {
			if name == "" || len(name) > 128 {
				return sdk.ConnectorResponse{}, errors.New("invalid query parameter")
			}
			switch v.(type) {
			case nil, string, bool, float64:
			default:
				return sdk.ConnectorResponse{}, errors.New("query parameter must be scalar")
			}
		}
		body, err = json.Marshal(map[string]any{"query": spec.Query, "params": spec.Params, "type": "flux", "dialect": map[string]any{"header": true, "delimiter": ",", "annotations": []string{}}})
		if err != nil {
			return sdk.ConnectorResponse{}, err
		}
	default:
		return sdk.ConnectorResponse{}, errors.New("read or write required")
	}
	endpoint := *r.base
	endpoint.Path = path
	endpoint.RawQuery = parameters.Encode()
	timeout := time.Duration(r.cfg.TimeoutMS) * time.Millisecond
	if req.TimeoutMS > 0 && time.Duration(req.TimeoutMS)*time.Millisecond < timeout {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, "POST", endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid request")
	}
	httpReq.Header.Set("Content-Type", contentType)
	httpReq.Header.Set("Accept", "text/csv")
	if r.cfg.Token != "" {
		httpReq.Header.Set("Authorization", "Token "+r.cfg.Token)
	}
	response, err := r.client.Do(httpReq)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("InfluxDB request failed; write outcome may be unknown")
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, r.cfg.MaxBytes+1))
	if err != nil || int64(len(responseBody)) > r.cfg.MaxBytes {
		return sdk.ConnectorResponse{}, errors.New("InfluxDB response failed or exceeded configured size")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return sdk.ConnectorResponse{}, errors.New("InfluxDB rejected request; write may have partially succeeded")
	}
	result := map[string]any{"status": response.StatusCode}
	if req.Kind == sdk.ConnectorRequestRead {
		rows, err := parseCSV(responseBody, r.cfg.MaxRows)
		if err != nil {
			return sdk.ConnectorResponse{}, err
		}
		result["rows"] = rows
		result["row_count"] = len(rows)
	}
	raw, err := json.Marshal(result)
	return sdk.ConnectorResponse{Payload: raw}, err
}

// Preserve textual cells, including int64/uint64 and RFC3339 timestamps. Users explicitly convert downstream.
func parseCSV(raw []byte, limit int) ([]map[string]any, error) {
	reader := csv.NewReader(bytes.NewReader(raw))
	reader.FieldsPerRecord = -1
	rows := []map[string]any{}
	var header []string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.New("invalid Flux CSV")
		}
		if len(record) > 256 {
			return nil, errors.New("Flux column limit exceeded")
		}
		if len(record) == 0 {
			continue
		}
		if strings.HasPrefix(record[0], "#") {
			return nil, errors.New("unexpected Flux annotations")
		}
		if header == nil || len(record) > 2 && record[1] == "result" && record[2] == "table" {
			header = append([]string(nil), record...)
			seen := map[string]bool{}
			for i, name := range header {
				if name == "" {
					name = "_column_" + strconv.Itoa(i)
					header[i] = name
				}
				if seen[name] {
					return nil, errors.New("duplicate Flux column")
				}
				seen[name] = true
			}
			continue
		}
		if len(record) != len(header) || len(rows) >= limit {
			return nil, errors.New("Flux row shape or count limit exceeded")
		}
		row := map[string]any{}
		for i, v := range record {
			row[header[i]] = v
		}
		rows = append(rows, row)
	}
	return rows, nil
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
	return sdk.StreamOpenResponse{}, errors.New("InfluxDB streaming not supported")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("InfluxDB streaming not supported")
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
