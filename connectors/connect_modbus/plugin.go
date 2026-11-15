package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type connectionConfig struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	UnitID          int    `json:"unit_id"`
	TimeoutMS       int    `json:"timeout_ms"`
	Mode            string `json:"mode"`
	ReuseHorizonMS  int    `json:"session_reuse_horizon_ms"`
	MaxSessionAgeMS int    `json:"max_session_age_ms"`
	serialIdentity  uint64
	Device          string `json:"device"`
	Baud            int    `json:"baud"`
	DataBits        int    `json:"data_bits"`
	Parity          string `json:"parity"`
	StopBits        int    `json:"stop_bits"`
}
type point struct {
	Count     int    `json:"count,omitempty"`
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
	conn            connection
	slotHeld        bool
	openedAt        time.Time
	idleTimer       *time.Timer
	timerGeneration uint64
	demands         map[string]time.Time
	retryAfter      time.Time
	stats           sessionStats
	transaction     uint16
	lastFrame       time.Time

	cfg       connectionConfig
	gate      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	deviceKey uint64
}
type connector struct {
	waiters   atomic.Int32
	sessions  chan struct{}
	mu        sync.RWMutex
	resources map[string]*resource
	stopped   bool
	devices   map[uint64]bool
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, sessions: make(chan struct{}, 8), devices: map[uint64]bool{}}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-modbus", Name: "Modbus Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Serialized TCP, RTU and ASCII batches with validated CRC/LRC and bounded serial ownership", Capabilities: sdk.PluginCapabilities{ConfigSchema: `{"type": "object", "properties": {"host": {"type": "string", "minLength": 1}, "port": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 502}, "unit_id": {"type": "integer", "minimum": 1, "maximum": 247, "default": 1}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 3000}, "mode": {"type": "string", "enum": ["tcp", "rtu_tcp", "ascii_tcp", "rtu", "ascii"], "default": "tcp"}, "device": {"type": "string"}, "baud": {"enum": [1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200, 230400], "default": 9600}, "data_bits": {"enum": [7, 8]}, "parity": {"enum": ["none", "even", "odd"], "default": "even"}, "stop_bits": {"enum": [1, 2], "default": 1}, "session_reuse_horizon_ms": {"type": "integer", "minimum": 0, "maximum": 30000, "default": 2000}, "max_session_age_ms": {"type": "integer", "minimum": 0, "maximum": 86400000, "default": 300000}}, "additionalProperties": false, "allOf": [{"if": {"properties": {"mode": {"enum": ["rtu", "ascii"]}}, "required": ["mode"]}, "then": {"required": ["device"]}, "else": {"required": ["host"]}}]}`, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "modbus", Label: "Modbus", Capabilities: []string{"read", "write", "shared_duplex", "session:scheduled_reuse", "session:per_operation"}, DisplayFields: []string{"host", "port", "unit_id"}}}}
}
func (c *connector) CreateResource(r sdk.ConnectorResource) (string, error) {
	cfg := connectionConfig{Port: 502, UnitID: 1, TimeoutMS: 3000, Mode: "tcp", Baud: 9600, Parity: "even", StopBits: 1, ReuseHorizonMS: 2000, MaxSessionAgeMS: 300000}
	if len(r.Connection) > 65536 {
		return "", errors.New("Modbus config exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(r.Connection))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cfg) != nil {
		return "", errors.New("invalid Modbus configuration")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return "", errors.New("trailing Modbus configuration")
	}

	if r.ID == "" || cfg.ReuseHorizonMS < 0 || cfg.ReuseHorizonMS > 30000 || cfg.MaxSessionAgeMS < 0 || cfg.MaxSessionAgeMS > 86400000 || cfg.UnitID < 1 || cfg.UnitID > 247 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 {
		return "", errors.New("invalid Modbus settings")
	}
	serial := cfg.Mode == "rtu" || cfg.Mode == "ascii"
	var deviceKey uint64
	switch cfg.Mode {
	case "tcp", "rtu_tcp", "ascii_tcp":
		if cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 {
			return "", errors.New("network mode requires host and port")
		}
	case "rtu", "ascii":
		if cfg.DataBits == 0 {
			cfg.DataBits = 8
			if cfg.Mode == "ascii" {
				cfg.DataBits = 7
			}
		}
		var err error
		cfg.Device, deviceKey, err = validateSerial(cfg)
		if err != nil {
			return "", err
		}
	default:
		return "", errors.New("unsupported Modbus mode")
	}
	cfg.serialIdentity = deviceKey
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
	if serial && c.devices[deviceKey] {
		cancel()
		return "", errors.New("serial bus already belongs to another resource")
	}
	if serial {
		c.devices[deviceKey] = true
	}
	c.resources[r.ID] = &resource{cfg: cfg, gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel, deviceKey: deviceKey, demands: map[string]time.Time{}}
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
		c.mu.Lock()
		delete(c.devices, r.deviceKey)
		c.mu.Unlock()
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.stopped = true
	resources := c.resources
	c.resources = map[string]*resource{}
	c.mu.Unlock()
	for _, r := range resources {
		r.cancel()
	}
	for _, r := range resources {
		r.gate <- struct{}{}
		c.closeSession(r)
		<-r.gate
	}
	return nil
}

func pointWidth(p point) (int, error) {
	if p.Name == "" || p.Address < 0 || p.Address > 65535 {
		return 0, errors.New("point requires a name and 0-based address in 0..65535")
	}
	if p.Area == "coil" || p.Area == "discrete" {
		if p.ValueType == "BoolArray" {
			if p.Count < 1 || p.Count > 1968 || p.Address+p.Count > 65536 {
				return 0, errors.New("invalid coil array count/address")
			}
			return p.Count, nil
		}
		if p.ValueType != "Bool" {
			return 0, errors.New("bit areas require Bool")
		}
		return 1, nil
	}
	if p.Area != "holding" && p.Area != "input" {
		return 0, errors.New("area must be coil, discrete, holding or input")
	}
	if p.ByteOrder != "" && p.ByteOrder != "ABCD" && p.ByteOrder != "BADC" && p.ByteOrder != "CDAB" && p.ByteOrder != "DCBA" {
		return 0, errors.New("unsupported byteOrder")
	}
	switch p.ValueType {
	case "Uint16", "Int16":
		return 1, nil
	case "Uint32", "Int32", "Float32":
		if p.Address == 65535 {
			return 0, errors.New("point exceeds address space")
		}
		return 2, nil
	}
	return 0, errors.New("unsupported valueType")
}

type readRange struct {
	unit       int
	area       string
	start, end int
	points     []point
}

func plan(points []point, unit int) ([]readRange, error) {
	if len(points) == 0 || len(points) > 1024 {
		return nil, errors.New("points count must be 1..1024")
	}
	copyPoints := append([]point(nil), points...)
	names := map[string]bool{}
	for _, p := range copyPoints {
		if _, err := pointWidth(p); err != nil {
			return nil, fmt.Errorf("point %s: %w", p.Name, err)
		}
		if names[p.Name] {
			return nil, errors.New("duplicate point name")
		}
		names[p.Name] = true
		if p.UnitID != nil && (*p.UnitID < 1 || *p.UnitID > 247) {
			return nil, errors.New("unit_id must be 1..247")
		}
	}
	unitOf := func(p point) int {
		if p.UnitID != nil {
			return *p.UnitID
		}
		return unit
	}
	sort.Slice(copyPoints, func(i, j int) bool {
		a, b := copyPoints[i], copyPoints[j]
		if unitOf(a) != unitOf(b) {
			return unitOf(a) < unitOf(b)
		}
		if a.Area != b.Area {
			return a.Area < b.Area
		}
		return a.Address < b.Address
	})
	var ranges []readRange
	for _, p := range copyPoints {
		w, _ := pointWidth(p)
		end := p.Address + w
		limit := 125
		if p.Area == "coil" || p.Area == "discrete" {
			limit = 2000
		}
		if len(ranges) > 0 {
			r := &ranges[len(ranges)-1]
			nextEnd := max(r.end, end)
			if r.unit == unitOf(p) && r.area == p.Area && p.Address <= r.end && nextEnd-r.start <= limit {
				r.end = nextEnd
				r.points = append(r.points, p)
				continue
			}
		}
		ranges = append(ranges, readRange{unit: unitOf(p), area: p.Area, start: p.Address, end: end, points: []point{p}})
	}
	return ranges, nil
}

// A batch owns one TCP session. All PDU exchanges are serialized, never retried.
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
		return sdk.ConnectorResponse{}, errors.New("Modbus batch exceeds 2 MiB")
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
	ranges, err := plan(b.Points, r.cfg.UnitID)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	write := req.Kind == sdk.ConnectorRequestWrite || req.Kind == sdk.ConnectorRequestBatchWrite
	if !write && req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("unsupported request kind")
	}
	// Validate every write before opening a connection; no partial effects for invalid config.
	if write {
		for _, p := range b.Points {
			if _, err := encodeWrite(p, r.cfg.UnitID); err != nil {
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
		return sdk.ConnectorResponse{}, sdk.RetryableError(errors.New("Modbus circuit open; wait for next permitted demand"))
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

	exchange := func(unit int, pdu []byte) ([]byte, error) {
		r.transaction++
		if r.cfg.Mode == "tcp" {
			return transact(conn, r.transaction, byte(unit), pdu)
		}
		if r.cfg.Mode == "rtu" || r.cfg.Mode == "ascii" {
			quiet := time.Duration(38500000000 / int64(r.cfg.Baud))
			if r.cfg.Baud > 19200 {
				quiet = 1750 * time.Microsecond
			}
			wait := quiet - time.Since(r.lastFrame)
			if r.lastFrame.IsZero() {
				wait = quiet
			}
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.C:
				}
			}
		}
		response, err := serialTransaction(conn, byte(unit), pdu, r.cfg.Mode == "ascii" || r.cfg.Mode == "ascii_tcp")
		r.lastFrame = time.Now()
		return response, err
	}
	if write {
		completed := 0
		for _, p := range b.Points {
			request, _ := encodeWrite(p, r.cfg.UnitID)
			u := r.cfg.UnitID
			if p.UnitID != nil {
				u = *p.UnitID
			}
			response, e := exchange(u, request)
			if e != nil {
				return sdk.ConnectorResponse{}, fmt.Errorf("write outcome unknown after %d confirmed points; no automatic retry: %w", completed, e)
			}
			if len(response) != 5 || !bytes.Equal(response, request[:5]) {
				return sdk.ConnectorResponse{}, fmt.Errorf("invalid write acknowledgement after %d confirmed points; outcome unknown", completed)
			}
			completed++
		}
		raw, _ := json.Marshal(map[string]any{"written": completed})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}
	values := map[string]any{}
	for _, rr := range ranges {
		fc := map[string]byte{"coil": 1, "discrete": 2, "holding": 3, "input": 4}[rr.area]
		pdu := make([]byte, 5)
		pdu[0] = fc
		binary.BigEndian.PutUint16(pdu[1:3], uint16(rr.start))
		binary.BigEndian.PutUint16(pdu[3:5], uint16(rr.end-rr.start))
		response, e := exchange(rr.unit, pdu)
		if e != nil {
			return sdk.ConnectorResponse{}, e
		}
		expected := 2 * (rr.end - rr.start)
		if fc <= 2 {
			expected = (rr.end - rr.start + 7) / 8
		}
		if len(response) != expected+2 || int(response[1]) != expected {
			return sdk.ConnectorResponse{}, errors.New("invalid Modbus byte count")
		}
		data := response[2:]
		for _, p := range rr.points {
			offset := p.Address - rr.start
			if fc <= 2 {
				if p.ValueType == "BoolArray" {
					valuesArray := make([]bool, p.Count)
					for i := range valuesArray {
						index := offset + i
						valuesArray[i] = data[index/8]&(1<<uint(index%8)) != 0
					}
					values[p.Name] = valuesArray
				} else {
					values[p.Name] = (data[offset/8] & (1 << uint(offset%8))) != 0
				}
			} else {
				w, _ := pointWidth(p)
				values[p.Name] = decodeValue(data[offset*2:(offset+w)*2], p)
			}
		}
	}
	raw, err := json.Marshal(map[string]any{"values": values, "segments": len(ranges)})
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("non-finite or invalid device value")
	}
	return sdk.ConnectorResponse{Payload: raw}, nil
}

func transact(conn io.ReadWriter, tx uint16, unit byte, pdu []byte) ([]byte, error) {
	frame := make([]byte, 7+len(pdu))
	binary.BigEndian.PutUint16(frame[:2], tx)
	binary.BigEndian.PutUint16(frame[4:6], uint16(len(pdu)+1))
	frame[6] = unit
	copy(frame[7:], pdu)
	if _, err := io.Copy(conn, bytes.NewReader(frame)); err != nil {
		return nil, err
	}
	h := make([]byte, 7)
	if _, err := io.ReadFull(conn, h); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint16(h[4:6]))
	if binary.BigEndian.Uint16(h[:2]) != tx || binary.BigEndian.Uint16(h[2:4]) != 0 || h[6] != unit || size < 2 || size > 254 {
		return nil, errors.New("invalid Modbus MBAP response")
	}
	response := make([]byte, size-1)
	if _, err := io.ReadFull(conn, response); err != nil {
		return nil, err
	}
	if response[0] == pdu[0]|0x80 {
		if len(response) != 2 {
			return nil, errors.New("malformed Modbus exception")
		}
		return nil, fmt.Errorf("Modbus exception %d", response[1])
	}
	if response[0] != pdu[0] {
		return nil, errors.New("unexpected Modbus function")
	}
	return response, nil
}

func reorder(data []byte, order string) []byte {
	out := append([]byte(nil), data...)
	if len(out) == 2 {
		if order == "BADC" || order == "DCBA" {
			out[0], out[1] = out[1], out[0]
		}
		return out
	}
	if len(out) == 4 {
		switch order {
		case "BADC":
			out = []byte{data[1], data[0], data[3], data[2]}
		case "CDAB":
			out = []byte{data[2], data[3], data[0], data[1]}
		case "DCBA":
			out = []byte{data[3], data[2], data[1], data[0]}
		}
	}
	return out
}
func decodeValue(data []byte, p point) any {
	b := reorder(data, p.ByteOrder)
	switch p.ValueType {
	case "Int16":
		return int16(binary.BigEndian.Uint16(b))
	case "Uint16":
		return binary.BigEndian.Uint16(b)
	case "Int32":
		return int32(binary.BigEndian.Uint32(b))
	case "Uint32":
		return binary.BigEndian.Uint32(b)
	default:
		return math.Float32frombits(binary.BigEndian.Uint32(b))
	}
}
func encodeWrite(p point, unit int) ([]byte, error) {
	if p.Area == "input" || p.Area == "discrete" {
		return nil, errors.New("area is read-only")
	}
	if p.Area == "coil" {
		if p.ValueType == "BoolArray" {
			values, ok := p.Value.([]any)
			if !ok || len(values) != p.Count || p.Count < 1 || p.Count > 1968 || p.Address+p.Count > 65536 {
				return nil, errors.New("coil array requires exactly count boolean values")
			}
			packed := make([]byte, (p.Count+7)/8)
			for i, value := range values {
				v, ok := value.(bool)
				if !ok {
					return nil, errors.New("coil array values must be booleans")
				}
				if v {
					packed[i/8] |= 1 << uint(i%8)
				}
			}
			return append([]byte{15, byte(p.Address >> 8), byte(p.Address), byte(p.Count >> 8), byte(p.Count), byte(len(packed))}, packed...), nil
		}
		v, ok := p.Value.(bool)
		if !ok {
			return nil, errors.New("coil write requires boolean")
		}
		out := []byte{5, byte(p.Address >> 8), byte(p.Address), 0, 0}
		if v {
			out[3] = 255
		}
		return out, nil
	}
	text := fmt.Sprint(p.Value)
	data := make([]byte, 2)
	var err error
	switch p.ValueType {
	case "Uint16":
		var v uint64
		v, err = strconv.ParseUint(text, 10, 16)
		binary.BigEndian.PutUint16(data, uint16(v))
	case "Int16":
		var v int64
		v, err = strconv.ParseInt(text, 10, 16)
		binary.BigEndian.PutUint16(data, uint16(v))
	case "Uint32":
		data = make([]byte, 4)
		var v uint64
		v, err = strconv.ParseUint(text, 10, 32)
		binary.BigEndian.PutUint32(data, uint32(v))
	case "Int32":
		data = make([]byte, 4)
		var v int64
		v, err = strconv.ParseInt(text, 10, 32)
		binary.BigEndian.PutUint32(data, uint32(v))
	case "Float32":
		data = make([]byte, 4)
		var v float64
		v, err = strconv.ParseFloat(text, 32)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			err = errors.New("non-finite write")
		}
		binary.BigEndian.PutUint32(data, math.Float32bits(float32(v)))
	default:
		return nil, errors.New("unsupported write valueType")
	}
	if err != nil {
		return nil, fmt.Errorf("point %s has invalid write value", p.Name)
	}
	data = reorder(data, p.ByteOrder)
	if len(data) == 2 {
		return []byte{6, byte(p.Address >> 8), byte(p.Address), data[0], data[1]}, nil
	}
	return append([]byte{16, byte(p.Address >> 8), byte(p.Address), 0, 2, 4}, data...), nil
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
