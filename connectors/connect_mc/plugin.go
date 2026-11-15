package main

import (
	"bytes"
	"context"

	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"

	"net"

	"sync"
	"sync/atomic"
	"time"
)

type connectionConfig struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	Transport       string `json:"transport"`
	NetworkNumber   int    `json:"network_number"`
	PCNumber        int    `json:"pc_number"`
	IONumber        int    `json:"io_number"`
	StationNumber   int    `json:"station_number"`
	MonitoringTimer int    `json:"monitoring_timer"`
	TimeoutMS       int    `json:"timeout_ms"`
	Mode            string `json:"mode"`
	ReuseHorizonMS  int    `json:"session_reuse_horizon_ms"`
	MaxSessionAgeMS int    `json:"max_session_age_ms"`
}
type point struct {
	Name      string `json:"name"`
	Area      string `json:"area"`
	Address   int    `json:"address"`
	ValueType string `json:"valueType"`
	ByteOrder string `json:"byteOrder,omitempty"`
	UnitID    *int   `json:"unit_id,omitempty"`
	Value     any    `json:"value,omitempty"`
}
type batch struct {
	ExpiresAt      *int64  `json:"expires_at,omitempty"`
	NextDemandInMS int64   `json:"next_demand_in_ms,omitempty"`
	Points         []point `json:"points"`
}
type resource struct {
	conn            net.Conn
	slotHeld        bool
	openedAt        time.Time
	idleTimer       *time.Timer
	timerGeneration uint64
	demands         map[string]time.Time
	retryAfter      time.Time
	stats           sessionStats

	cfg    connectionConfig
	gate   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}
type connector struct {
	waiters   atomic.Int32
	sessions  chan struct{}
	mu        sync.RWMutex
	resources map[string]*resource
	stopped   bool
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, sessions: make(chan struct{}, 8)}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-mc", Name: "MC 1E/3E/4E Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Serialized MC 1E/3E/4E TCP/UDP binary/ASCII batches with validated routing and serials", Capabilities: sdk.PluginCapabilities{ConfigSchema: `{"type": "object", "required": ["host"], "properties": {"host": {"type": "string", "minLength": 1}, "port": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 5000}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 3000}, "mode": {"type": "string", "enum": ["1e_binary", "1e_ascii", "3e_binary", "3e_ascii", "4e_binary", "4e_ascii"], "default": "3e_binary"}, "transport": {"type": "string", "enum": ["tcp", "udp"], "default": "tcp"}, "network_number": {"type": "integer", "minimum": 0, "maximum": 255, "default": 0}, "pc_number": {"type": "integer", "minimum": 0, "maximum": 255, "default": 255}, "io_number": {"type": "integer", "minimum": 0, "maximum": 65535, "default": 1023}, "station_number": {"type": "integer", "minimum": 0, "maximum": 120, "default": 0}, "monitoring_timer": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 16, "description": "3E/4E PLC timer in 250 ms units; 1E FX5 profile requires wire timer zero and still uses finite client deadline"}, "session_reuse_horizon_ms": {"type": "integer", "minimum": 0, "maximum": 30000, "default": 2000}, "max_session_age_ms": {"type": "integer", "minimum": 0, "maximum": 86400000, "default": 300000}}, "additionalProperties": false}`, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "mc", Label: "MC 1E/3E/4E", Capabilities: []string{"read", "write", "shared_duplex", "session:scheduled_reuse", "session:per_operation"}, DisplayFields: []string{"host", "port"}}}}
}
func (c *connector) CreateResource(r sdk.ConnectorResource) (string, error) {
	cfg := connectionConfig{Port: 5000, PCNumber: 255, IONumber: 1023, MonitoringTimer: 16, Transport: "tcp", TimeoutMS: 3000, Mode: "3e_binary", ReuseHorizonMS: 2000, MaxSessionAgeMS: 300000}
	if len(r.Connection) > 65536 {
		return "", errors.New("MC configuration exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(r.Connection))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cfg) != nil {
		return "", errors.New("invalid MC configuration")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return "", errors.New("trailing MC configuration")
	}

	if r.ID == "" || cfg.ReuseHorizonMS < 0 || cfg.ReuseHorizonMS > 30000 || cfg.MaxSessionAgeMS < 0 || cfg.MaxSessionAgeMS > 86400000 || cfg.Host == "" || len(cfg.Host) > 253 || cfg.Port < 1 || cfg.Port > 65535 || cfg.NetworkNumber < 0 || cfg.NetworkNumber > 255 || cfg.PCNumber < 0 || cfg.PCNumber > 255 || cfg.IONumber < 0 || cfg.IONumber > 65535 || cfg.StationNumber < 0 || cfg.StationNumber > 120 || cfg.MonitoringTimer < 1 || cfg.MonitoringTimer > 65535 || (cfg.Transport != "tcp" && cfg.Transport != "udp") || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || (cfg.Mode != "1e_binary" && cfg.Mode != "1e_ascii" && cfg.Mode != "3e_binary" && cfg.Mode != "3e_ascii" && cfg.Mode != "4e_binary" && cfg.Mode != "4e_ascii") {
		return "", fmt.Errorf("invalid MC 1E/3E/4E connection settings")
	}
	if mc1E(cfg) && (cfg.NetworkNumber != 0 || cfg.PCNumber != 255 || cfg.IONumber != 1023 || cfg.StationNumber != 0) {
		return "", errors.New("1E FX5 profile supports only local FF station; routed 3E/4E fields are unsupported")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		cancel()
		return "", errors.New("connector stopped")
	}
	if _, exists := c.resources[r.ID]; exists {
		cancel()
		return "", errors.New("resource already exists")
	}
	c.resources[r.ID] = &resource{cfg: cfg, gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel, demands: map[string]time.Time{}}
	return r.ID, nil
}
func (c *connector) DestroyResource(handle string) error {
	c.mu.Lock()
	r := c.resources[handle]
	delete(c.resources, handle)
	c.mu.Unlock()
	if r != nil {
		r.cancel()
		r.gate <- struct{}{}
		c.closeSession(r)
		<-r.gate
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.stopped = true
	all := c.resources
	c.resources = map[string]*resource{}
	for _, r := range all {
		r.cancel()
	}
	c.mu.Unlock()
	for _, r := range all {
		r.gate <- struct{}{}
		c.closeSession(r)
		<-r.gate
	}
	return nil
}

type readRange struct {
	unit       int
	area       string
	start, end int
	points     []point
}

// A batch owns one device socket. All PDU exchanges are serialized, never retried.
func (c *connector) Execute(handle string, req sdk.ConnectorRequest) (result sdk.ConnectorResponse, opErr error) {
	c.mu.RLock()
	r := c.resources[handle]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	if req.Target == "release_demand" || req.Target == "session_stats" {
		return c.sessionControl(r, req)
	}
	if len(req.Payload) > 2097152 {
		return sdk.ConnectorResponse{}, errors.New("MC batch exceeds 2 MiB")
	}
	var b batch
	dec := json.NewDecoder(bytes.NewReader(req.Payload))
	dec.UseNumber()
	if err := dec.Decode(&b); err != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid point batch")
	}
	if b.NextDemandInMS < 0 || b.NextDemandInMS > 86400000 || len(req.ResourceToken) > 512 {
		return sdk.ConnectorResponse{}, errors.New("invalid demand hint")
	}
	ranges, err := plan(b.Points, 0)
	if mc1E(r.cfg) {
		ranges, err = planSized(b.Points, 64, 256)
	}
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	write := req.Kind == sdk.ConnectorRequestWrite || req.Kind == sdk.ConnectorRequestBatchWrite
	if !write && req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("unsupported request kind")
	}
	if r.cfg.Transport == "udp" && !mc4E(r.cfg) && ((write && len(b.Points) != 1) || (!write && len(ranges) != 1)) {
		return sdk.ConnectorResponse{}, errors.New("1E/3E UDP permits only one PDU per batch because it has no serial correlation; use 4E UDP for multi-segment batches")
	}
	if mcASCII(r.cfg) && !mc1E(r.cfg) {
		for _, p := range b.Points {
			if p.Area == "D" || p.Area == "M" || p.Area == "R" {
				width, _ := pointWidth(p)
				if p.Address+width-1 > 999999 {
					return sdk.ConnectorResponse{}, errors.New("MC ASCII decimal address exceeds six digits")
				}
			}
		}
	}
	// Validate every write before opening a connection; no partial effects for invalid config.
	if write {
		for _, p := range b.Points {
			if _, err := encodeWrite(p, 0); err != nil {
				return sdk.ConnectorResponse{}, err
			}
		}
	}
	timeout := time.Duration(r.cfg.TimeoutMS) * time.Millisecond
	if req.TimeoutMS > 0 && time.Duration(req.TimeoutMS)*time.Millisecond < timeout {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	if b.ExpiresAt != nil {
		if *b.ExpiresAt <= time.Now().UnixMilli() {
			return sdk.ConnectorResponse{}, errors.New("command expired before connection")
		}
		var expiryCancel context.CancelFunc
		ctx, expiryCancel = context.WithDeadline(ctx, time.UnixMilli(*b.ExpiresAt))
		defer expiryCancel()
	}

	select {
	case r.gate <- struct{}{}:
		defer func() { <-r.gate }()
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	if time.Now().Before(r.retryAfter) {
		return sdk.ConnectorResponse{}, sdk.RetryableError(errors.New("MC circuit open; wait for next permitted demand"))
	}
	r.pruneDemands(time.Now())
	token := demandToken(req.ResourceToken)
	if _, exists := r.demands[token]; !exists && b.NextDemandInMS > 0 && len(r.demands) >= 1024 {
		return sdk.ConnectorResponse{}, errors.New("demand token limit exceeded")
	}
	defer func() {
		if opErr == nil && ctx.Err() != nil {
			opErr = ctx.Err()
		}
		c.finishSession(r, req, b, opErr)
	}()
	conn, err := c.acquireSession(r, ctx)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	deadline, _ := ctx.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		return sdk.ConnectorResponse{}, err
	}
	done := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer done()
	if write {
		completed := 0
		for _, p := range b.Points {
			request, _ := encodeWrite(p, 0)
			response, e := transactMC(conn, r.cfg, request)
			if e != nil {
				return sdk.ConnectorResponse{}, fmt.Errorf("MC write outcome unknown after %d confirmed points; do not retry: %w", completed, e)
			}
			if len(response) != 0 {
				return sdk.ConnectorResponse{}, errors.New("invalid MC write response; outcome unknown")
			}
			completed++
		}
		raw, _ := json.Marshal(map[string]any{"written": completed})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}
	values := map[string]any{}
	for _, rr := range ranges {
		response, e := transactMC(conn, r.cfg, mcRequest(0x0401, rr.area, rr.start, rr.end-rr.start, nil))
		if e != nil {
			return sdk.ConnectorResponse{}, e
		}
		bit := rr.area == "M" || rr.area == "X" || rr.area == "Y" || rr.area == "B"
		expected := 2 * (rr.end - rr.start)
		if bit {
			expected = (rr.end - rr.start + 1) / 2
		}
		if len(response) != expected {
			return sdk.ConnectorResponse{}, errors.New("MC response point count mismatch")
		}
		for _, p := range rr.points {
			offset := p.Address - rr.start
			if bit {
				shift := uint(4)
				if offset%2 == 1 {
					shift = 0
				}
				v := (response[offset/2] >> shift) & 15
				if v > 1 {
					return sdk.ConnectorResponse{}, errors.New("invalid MC bit value")
				}
				values[p.Name] = v == 1
			} else {
				w, _ := pointWidth(p)
				values[p.Name] = decodeValue(response[offset*2:(offset+w)*2], p)
			}
		}
	}
	raw, err := json.Marshal(map[string]any{"values": values, "segments": len(ranges)})
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid or non-finite device value")
	}
	return sdk.ConnectorResponse{Payload: raw}, nil
}

func (c *connector) Probe(handle string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	c.mu.RLock()
	r := c.resources[handle]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ResourceStatusEvent{}, errors.New("unknown resource")
	}
	return sdk.ResourceStatusEvent{ResourceID: handle, Status: sdk.ResourceStatusHealthy, ReasonCode: "DEMAND_MANAGED", Message: "configured; reachability is verified on actual reads", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("streaming not supported; use batched reads")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("streaming not supported")
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
