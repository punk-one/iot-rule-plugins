package main

import (
	"bytes"
	"context"
	"encoding/base64"

	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type config struct {
	Mode          string  `json:"mode"`
	StreamFrame   framing `json:"stream_frame"`
	StreamRequest []byte  `json:"stream_request_base64"`
	Host          string  `json:"host"`
	Port          int     `json:"port"`
	TimeoutMS     int     `json:"timeout_ms"`
	MaxFrame      int     `json:"max_frame_bytes"`
	AllowWrite    bool    `json:"allow_write"`
}
type framing struct {
	Mode      string `json:"mode"`
	Length    int    `json:"length"`
	Delimiter []byte `json:"delimiter"`
	Offset    int    `json:"length_offset"`
	Width     int    `json:"length_bytes"`
	Endian    string `json:"length_endian"`
	Adjust    int    `json:"length_adjust"`
}
type request struct {
	Expires *int64  `json:"expires_at,omitempty"`
	Data    []byte  `json:"data"`
	Frame   framing `json:"frame"`
}
type connection interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}
type resource struct {
	openMu          sync.Mutex
	streamMu        sync.Mutex
	streams         map[string]*rawSubscription
	active          *rawSession
	queuedBytes     int
	sequence, opens uint64
	failures        int
	retryAfter      time.Time
	cfg             config
	ctx             context.Context
	cancel          context.CancelFunc
	gate            chan struct{}
}
type connector struct {
	mu        sync.Mutex
	resources map[string]*resource
	closed    bool
	slots     chan struct{}
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, slots: make(chan struct{}, 8)}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-udp", Name: "UDP Freeport Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Bounded poll/response or shared read-only continuous stream with explicit framing", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "udp", Label: "UDP", Capabilities: []string{"read", "write", "shared_duplex", "session:per_operation", "stream:best_effort", "session:demand"}, DisplayFields: []string{"host", "port"}}}}
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{TimeoutMS: 5000, MaxFrame: 65507}
	if len(spec.Connection) > 2097152 {
		return "", errors.New("raw config exceeds 2 MiB")
	}
	if json.Unmarshal(spec.Connection, &cfg) != nil || cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxFrame < 1 || cfg.MaxFrame > 65507 || spec.ID == "" {
		return "", errors.New("invalid UDP settings")
	}
	if cfg.Mode == "" {
		cfg.Mode = "poll"
	}
	if cfg.Mode != "poll" && cfg.Mode != "stream" || len(cfg.StreamRequest) > 65536 {
		return "", errors.New("invalid raw transport mode/initial request")
	}
	if cfg.Mode == "stream" {
		if cfg.AllowWrite {
			return "", errors.New("continuous raw stream is read-only; no concurrent request/response writes")
		}
		if err := validateFrame(cfg.StreamFrame, cfg.MaxFrame); err != nil {
			return "", err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.resources) >= 64 || c.closed || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("connector stopped or resource exists")
	}
	c.resources[spec.ID] = &resource{cfg: cfg, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1)}
	return spec.ID, nil
}
func (c *connector) get(h string) (*resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.resources[h]
	if r == nil {
		return nil, errors.New("unknown resource")
	}
	return r, nil
}
func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	r := c.resources[h]
	delete(c.resources, h)
	c.mu.Unlock()
	if r != nil {
		r.cancel()
		r.gate <- struct{}{}
		<-r.gate
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.closed = true
	resources := c.resources
	c.resources = map[string]*resource{}
	c.mu.Unlock()
	for _, r := range resources {
		r.cancel()
	}
	for _, r := range resources {
		r.gate <- struct{}{}
		<-r.gate
	}
	return nil
}
func dial(ctx context.Context, cfg config) (connection, error) {
	return (&net.Dialer{}).DialContext(ctx, "udp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
}
func validateFrame(f framing, limit int) error {
	if f.Mode != "datagram" {
		return errors.New("UDP requires datagram framing")
	}
	return nil
}
func readFrame(reader io.Reader, f framing, limit int) ([]byte, error) {
	if err := validateFrame(f, limit); err != nil {
		return nil, err
	}
	raw := make([]byte, limit+1)
	n, err := reader.Read(raw)
	if err != nil {
		return nil, err
	}
	if n > limit {
		return nil, errors.New("UDP datagram exceeds configured limit")
	}
	return raw[:n], nil
}
func writeAll(writer io.Writer, raw []byte) error {
	for len(raw) > 0 {
		n, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if r.cfg.Mode == "stream" {
		return sdk.ConnectorResponse{}, errors.New("stream-mode resource rejects poll/write operations")
	}
	if len(req.Payload) > r.cfg.MaxFrame*2+1024 {
		return sdk.ConnectorResponse{}, errors.New("request exceeds limit")
	}
	var spec request
	decoder := json.NewDecoder(bytes.NewReader(req.Payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&spec) != nil || len(spec.Data) > r.cfg.MaxFrame {
		return sdk.ConnectorResponse{}, errors.New("invalid request")
	}
	read := req.Kind == sdk.ConnectorRequestRead && req.Target == "exchange"
	write := req.Kind == sdk.ConnectorRequestWrite && req.Target == "send"
	if !read && !write || write && !r.cfg.AllowWrite {
		return sdk.ConnectorResponse{}, errors.New("request kind or raw write is not allowed")
	}
	if read {
		if err = validateFrame(spec.Frame, r.cfg.MaxFrame); err != nil {
			return sdk.ConnectorResponse{}, err
		}
	} else if len(spec.Data) == 0 {
		return sdk.ConnectorResponse{}, errors.New("write requires data")
	}
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(timeout)*time.Millisecond)
	if write {
		if spec.Expires == nil || *spec.Expires <= time.Now().UnixMilli() {
			cancel()
			return sdk.ConnectorResponse{}, errors.New("raw write requires unexpired absolute expires_at")
		}
		var expireCancel context.CancelFunc
		ctx, expireCancel = context.WithDeadline(ctx, time.UnixMilli(*spec.Expires))
		defer expireCancel()
	}

	defer cancel()
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, errors.New("operation canceled before gate")
	case r.gate <- struct{}{}:
	}
	defer func() { <-r.gate }()
	if ctx.Err() != nil {
		return sdk.ConnectorResponse{}, errors.New("operation canceled")
	}
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	case c.slots <- struct{}{}:
	}
	defer func() { <-c.slots }()
	conn, err := dial(ctx, r.cfg)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("transport open failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return sdk.ConnectorResponse{}, errors.New("transport cannot enforce deadline")
	}
	if n, e := conn.Write(spec.Data); e != nil || n != len(spec.Data) {
		return sdk.ConnectorResponse{}, errors.New("transport request write failed; outcome may be unknown")
	}
	var raw []byte
	if read {
		raw, err = readFrame(conn, spec.Frame, r.cfg.MaxFrame)
		if err != nil {
			return sdk.ConnectorResponse{}, errors.New("response framing failed or timed out")
		}
	}
	result, _ := json.Marshal(map[string]any{"raw_base64": base64.StdEncoding.EncodeToString(raw), "bytes": len(raw), "received_at": time.Now().UnixMilli(), "sent": true})
	return sdk.ConnectorResponse{Payload: result}, nil
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	status, reason := sdk.ResourceStatusHealthy, "IDLE_DEMAND_MANAGED"
	if r.cfg.Mode == "stream" {
		r.streamMu.Lock()
		if r.active != nil {
			reason = "RAW_STREAM_ACTIVE"
		}
		if r.failures > 0 {
			status = sdk.ResourceStatusDegraded
			reason = "RAW_STREAM_FAILED"
		}
		r.streamMu.Unlock()
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: status, ReasonCode: reason, Message: "Best-effort transport state is not application delivery confirmation", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
