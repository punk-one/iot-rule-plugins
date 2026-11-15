package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type config struct {
	Host           string  `json:"host"`
	Port           int     `json:"port"`
	TimeoutMS      int     `json:"timeout_ms"`
	ExpectedDevice *uint32 `json:"expected_device_instance,omitempty"`
	ReadAllow      []point `json:"read_allowlist"`
	WriteAllow     []point `json:"write_allowlist"`
	AllowWrite     bool    `json:"allow_write"`
}
type batch struct {
	Points    []point `json:"points"`
	ExpiresAt int64   `json:"expires_at,omitempty"`
}
type resource struct {
	cfg        config
	ctx        context.Context
	cancel     context.CancelFunc
	gate       chan struct{}
	mu         sync.Mutex
	failures   int
	retryAfter time.Time
	lastError  string
	opens      uint64
}
type connector struct {
	mu        sync.Mutex
	closed    bool
	resources map[string]*resource
	slots     chan struct{}
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, slots: make(chan struct{}, 8)}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-bacnet", Name: "BACnet/IP Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Direct unicast scalar ReadProperty/WriteProperty, serialized bounded batches", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "bacnet", Label: "BACnet/IP", Capabilities: []string{"read", "write", "shared_duplex", "session:per_operation"}, DisplayFields: []string{"host", "port", "expected_device_instance"}}}}
}
func strict(raw []byte, v any) error {
	if len(raw) > 262144 {
		return errors.New("BACnet configuration/request exceeds 256 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return errors.New("invalid BACnet configuration/request")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func validatePoints(points []point) error {
	if len(points) < 1 || len(points) > 128 {
		return errors.New("BACnet batch/allowlist requires 1..128 points")
	}
	seen := map[string]bool{}
	for _, p := range points {
		if err := validatePoint(p); err != nil {
			return err
		}
		if seen[p.Name] {
			return errors.New("duplicate BACnet point name")
		}
		seen[p.Name] = true
	}
	return nil
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Port: 47808, TimeoutMS: 3000}
	if strict(spec.Connection, &cfg) != nil || spec.ID == "" || net.ParseIP(cfg.Host) == nil || net.ParseIP(cfg.Host).To4() == nil || net.ParseIP(cfg.Host).IsUnspecified() || net.ParseIP(cfg.Host).IsMulticast() || cfg.Host == "255.255.255.255" || cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.ExpectedDevice != nil && *cfg.ExpectedDevice >= 4194303 {
		return "", errors.New("BACnet requires fixed unicast IPv4, valid port/deadline/device instance")
	}
	if len(cfg.ReadAllow) == 0 && (!cfg.AllowWrite || len(cfg.WriteAllow) == 0) {
		return "", errors.New("explicit BACnet read/write allowlist required")
	}
	if len(cfg.ReadAllow) > 0 {
		if err := validatePoints(cfg.ReadAllow); err != nil {
			return "", err
		}
	}
	if len(cfg.WriteAllow) > 0 {
		if err := validatePoints(cfg.WriteAllow); err != nil {
			return "", err
		}
	}
	for _, p := range cfg.WriteAllow {
		if p.ValueType == "Date" || p.ValueType == "Time" {
			return "", errors.New("Date/Time writes not supported")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &resource{cfg: cfg, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.resources) >= 64 || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("resource exists, connector stopped or limit reached")
	}
	c.resources[spec.ID] = r
	return spec.ID, nil
}
func (c *connector) get(h string) (*resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.resources[h]
	if r == nil {
		return nil, errors.New("unknown BACnet resource")
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
func acquire(ctx context.Context, gate chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case gate <- struct{}{}:
		if ctx.Err() != nil {
			<-gate
			return ctx.Err()
		}
		return nil
	}
}
func allowed(p point, list []point, write bool) bool {
	for _, a := range list {
		if pointKey(p) == pointKey(a) && p.ValueType == a.ValueType {
			if !write {
				return true
			}
			x, y := p.Priority, a.Priority
			if x == 0 {
				x = 16
			}
			if y == 0 {
				y = 16
			}
			return x == y
		}
	}
	return false
}
func exchange(conn net.Conn, invoke, service byte, p point, encoded []byte) (any, error) {
	packet := requestPacket(invoke, service, p, encoded)
	if _, err := conn.Write(packet); err != nil {
		return nil, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return responsePacket(buf[:n], invoke, service, p)
}
func (r *resource) record(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.failures = 0
		r.retryAfter = time.Time{}
		r.lastError = ""
		return
	}
	r.failures++
	r.lastError = "BACNET_BATCH_FAILED"
	if r.failures >= 3 {
		r.retryAfter = time.Now().Add(time.Duration(1<<min(r.failures-3, 5)) * time.Second)
	}
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (response sdk.ConnectorResponse, returned error) {
	r, err := c.get(h)
	if err != nil {
		return response, err
	}
	write := req.Kind == sdk.ConnectorRequestBatchWrite || req.Kind == sdk.ConnectorRequestWrite
	if (!write && req.Kind != sdk.ConnectorRequestBatchRead && req.Kind != sdk.ConnectorRequestRead) || req.Target != "points" {
		return response, errors.New("unsupported BACnet operation")
	}
	var b batch
	if strict(req.Payload, &b) != nil || validatePoints(b.Points) != nil {
		return response, errors.New("invalid BACnet points")
	}
	encoded := make([][]byte, len(b.Points))
	for i, p := range b.Points {
		list := r.cfg.ReadAllow
		if write {
			list = r.cfg.WriteAllow
		}
		if write && !r.cfg.AllowWrite || !allowed(p, list, write) {
			return response, errors.New("BACnet point/type/priority not allowed by resource")
		}
		if write {
			encoded[i], err = encodeValue(p)
			if err != nil {
				return response, err
			}
		}
	}
	now := time.Now()
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	deadline := now.Add(time.Duration(timeout) * time.Millisecond)
	if write {
		expires := time.UnixMilli(b.ExpiresAt)
		if !expires.After(now) || expires.After(now.Add(time.Minute)) {
			return response, errors.New("BACnet writes require live expires_at within 60s")
		}
		if expires.Before(deadline) {
			deadline = expires
		}
	}
	ctx, cancel := context.WithDeadline(r.ctx, deadline)
	defer cancel()
	if err = acquire(ctx, r.gate); err != nil {
		return response, err
	}
	defer func() { <-r.gate }()
	r.mu.Lock()
	blocked := time.Now().Before(r.retryAfter)
	r.mu.Unlock()
	if blocked {
		return response, errors.New("BACnet circuit open; no network request")
	}
	if err = acquire(ctx, c.slots); err != nil {
		return response, err
	}
	defer func() { <-c.slots }()
	defer func() { r.record(returned) }()
	conn, err := (&net.Dialer{}).DialContext(ctx, "udp4", net.JoinHostPort(r.cfg.Host, strconv.Itoa(r.cfg.Port)))
	if err != nil {
		return response, errors.New("BACnet socket open failed")
	}
	defer conn.Close()
	conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	r.mu.Lock()
	r.opens++
	r.mu.Unlock()
	var seed [1]byte
	if _, err = rand.Read(seed[:]); err != nil {
		return response, err
	}
	invoke := seed[0]
	if r.cfg.ExpectedDevice != nil {
		p := point{Name: "device_identity", ObjectType: 8, Instance: *r.cfg.ExpectedDevice, Property: 75, ValueType: "ObjectID"}
		v, e := exchange(conn, invoke, 12, p, nil)
		invoke++
		if e != nil {
			return response, errors.New("BACnet device identity verification failed")
		}
		id, ok := v.(map[string]any)
		if !ok || id["object_type"] != uint32(8) || id["instance"] != *r.cfg.ExpectedDevice {
			return response, errors.New("BACnet device identity mismatch")
		}
	}
	values := map[string]any{}
	quality := map[string]string{}
	for i, p := range b.Points {
		if ctx.Err() != nil {
			return response, errors.New("BACnet batch deadline reached; write outcome may be partial/unknown")
		}
		service := byte(12)
		if write {
			service = 15
		}
		v, e := exchange(conn, invoke, service, p, encoded[i])
		invoke++
		if e != nil {
			if write {
				return response, fmt.Errorf("BACnet write stopped after %d confirmed properties; outcome may be partial/unknown, no retry", i)
			}
			var device *deviceError
			if errors.As(e, &device) {
				values[p.Name] = nil
				quality[p.Name] = device.code
				continue
			}
			return response, errors.New("BACnet transport/protocol failure; entire batch discarded")
		}
		values[p.Name] = v
		quality[p.Name] = "good"
	}
	out := map[string]any{"values": values, "quality": quality, "segments": len(b.Points)}
	if write {
		out = map[string]any{"written": len(b.Points), "scope": "property_ack", "physical_completion": false}
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return response, err
	}
	return sdk.ConnectorResponse{Payload: raw}, nil
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	status, reason := sdk.ResourceStatusHealthy, "IDLE_DEMAND_MANAGED"
	if r.lastError != "" {
		status = sdk.ResourceStatusDegraded
		reason = r.lastError
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: status, ReasonCode: reason, Message: "BACnet is queried on real demand; idle is not a disconnected failure", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("BACnet polling only; COV not supported")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("no BACnet streams")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error {
	return errors.New("no BACnet streams")
}
func (c *connector) NackStream(string, sdk.StreamNackRequest) error {
	return errors.New("no BACnet streams")
}
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("no BACnet streams")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }
