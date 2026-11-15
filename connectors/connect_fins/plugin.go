package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

type config struct {
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Mode          string `json:"mode"`
	SourceNode    int    `json:"source_node"`
	DestNode      int    `json:"destination_node"`
	SourceNetwork int    `json:"source_network"`
	DestNetwork   int    `json:"destination_network"`
	TimeoutMS     int    `json:"timeout_ms"`
}
type point struct {
	Name      string `json:"name"`
	Area      string `json:"area"`
	Address   int    `json:"address"`
	Bit       int    `json:"bit"`
	ValueType string `json:"valueType"`
	ByteOrder string `json:"byteOrder"`
	Value     any    `json:"value"`
}
type batch struct {
	Points  []point `json:"points"`
	Expires *int64  `json:"expires_at"`
}
type resource struct {
	cfg    config
	ctx    context.Context
	cancel context.CancelFunc
	gate   chan struct{}
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
	return sdk.PluginInfo{ID: "connect-fins", Name: "OMRON FINS Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Bounded serialized CS/CJ FINS UDP/TCP memory reads and writes", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "fins", Label: "OMRON FINS", Capabilities: []string{"read", "write", "shared_duplex", "session:per_operation"}, DisplayFields: []string{"host", "port", "mode", "source_node", "destination_node"}}}}
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Port: 9600, Mode: "udp", TimeoutMS: 3000}
	if json.Unmarshal(spec.Connection, &cfg) != nil || spec.ID == "" || cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 || (cfg.Mode != "udp" && cfg.Mode != "tcp") || cfg.SourceNode < 1 || cfg.SourceNode > 254 || cfg.DestNode < 1 || cfg.DestNode > 254 || cfg.SourceNetwork < 0 || cfg.SourceNetwork > 127 || cfg.DestNetwork < 0 || cfg.DestNetwork > 127 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 {
		return "", errors.New("invalid FINS resource; explicit source/destination nodes required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("resource exists or connector stopped")
	}
	c.resources[spec.ID] = &resource{cfg: cfg, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1)}
	return spec.ID, nil
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
func areaCode(p point) (byte, error) {
	words := map[string]byte{"DM": 0x82, "CIO": 0xb0, "W": 0xb1, "H": 0xb2, "A": 0xb3}
	code, ok := words[p.Area]
	if !ok {
		return 0, errors.New("unsupported CS/CJ memory area")
	}
	if p.ValueType == "Bool" {
		if p.Area == "DM" {
			code = 2
		} else {
			code -= 0x80
		}
	}
	return code, nil
}
func validate(p point) (int, error) {
	if p.Name == "" || p.Address < 0 || p.Address > 65535 || p.Bit < 0 || p.Bit > 15 {
		return 0, errors.New("invalid point name/address/bit")
	}
	if _, err := areaCode(p); err != nil {
		return 0, err
	}
	if p.ValueType == "Bool" {
		return 1, nil
	}
	if p.Bit != 0 {
		return 0, errors.New("word point bit must be zero")
	}
	switch p.ValueType {
	case "Int16", "Uint16":
		if p.ByteOrder != "" && p.ByteOrder != "AB" && p.ByteOrder != "BA" {
			return 0, errors.New("16-bit order must be AB or BA")
		}
		return 1, nil
	case "Int32", "Uint32", "Float32":
		if p.Address == 65535 || (p.ByteOrder != "ABCD" && p.ByteOrder != "CDAB" && p.ByteOrder != "BADC" && p.ByteOrder != "DCBA") {
			return 0, errors.New("32-bit point requires explicit byteOrder and two valid words")
		}
		return 2, nil
	default:
		return 0, errors.New("unsupported FINS type")
	}
}

type readRange struct {
	area       byte
	start, end int
	bits       bool
	points     []point
}

func plan(points []point) ([]readRange, error) {
	if len(points) < 1 || len(points) > 128 {
		return nil, errors.New("batch requires 1..128 points")
	}
	type entry struct {
		p          point
		code       byte
		start, end int
		bits       bool
	}
	list := []entry{}
	seen := map[string]bool{}
	for _, p := range points {
		width, err := validate(p)
		if err != nil || seen[p.Name] {
			return nil, errors.New("invalid or duplicate FINS point")
		}
		seen[p.Name] = true
		code, _ := areaCode(p)
		start := p.Address
		bits := p.ValueType == "Bool"
		if bits {
			start = start*16 + p.Bit
		}
		list = append(list, entry{p: p, code: code, start: start, end: start + width, bits: bits})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].code != list[j].code {
			return list[i].code < list[j].code
		}
		return list[i].start < list[j].start
	})
	out := []readRange{}
	for _, e := range list {
		if len(out) > 0 {
			last := &out[len(out)-1]
			limit := last.start + 128
			if last.bits {
				limit = last.start/16*16 + 16
			}
			if last.area == e.code && last.bits == e.bits && e.start <= last.end && e.end <= limit {
				last.end = max(last.end, e.end)
				last.points = append(last.points, e.p)
				continue
			}
		}
		out = append(out, readRange{area: e.code, start: e.start, end: e.end, bits: e.bits, points: []point{e.p}})
	}
	return out, nil
}
func reorder(raw []byte, order string) []byte {
	out := append([]byte(nil), raw...)
	switch order {
	case "BA", "BADC":
		for i := 0; i < len(out); i += 2 {
			out[i], out[i+1] = out[i+1], out[i]
		}
	case "CDAB":
		if len(out) == 4 {
			out = []byte{out[2], out[3], out[0], out[1]}
		}
	case "DCBA":
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out
}
func decode(raw []byte, p point) any {
	raw = reorder(raw, p.ByteOrder)
	switch p.ValueType {
	case "Bool":
		return raw[0] != 0
	case "Int16":
		return int16(binary.BigEndian.Uint16(raw))
	case "Uint16":
		return binary.BigEndian.Uint16(raw)
	case "Int32":
		return int32(binary.BigEndian.Uint32(raw))
	case "Uint32":
		return binary.BigEndian.Uint32(raw)
	case "Float32":
		return math.Float32frombits(binary.BigEndian.Uint32(raw))
	}
	return nil
}
func encode(p point) ([]byte, error) {
	if _, err := validate(p); err != nil {
		return nil, err
	}
	if p.Area == "A" {
		return nil, errors.New("auxiliary area writes are disabled")
	}
	if p.ValueType == "Bool" {
		v, ok := p.Value.(bool)
		if !ok {
			return nil, errors.New("Bool write requires bool")
		}
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	}
	var number float64
	switch v := p.Value.(type) {
	case json.Number:
		var err error
		number, err = v.Float64()
		if err != nil {
			return nil, err
		}
	case float64:
		number = v
	default:
		return nil, errors.New("numeric write requires JSON number")
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return nil, errors.New("nonfinite write")
	}
	raw := make([]byte, 2)
	min, max := float64(-32768), float64(32767)
	switch p.ValueType {
	case "Uint16":
		min = 0
		max = 65535
	case "Int32":
		min = -2147483648
		max = 2147483647
		raw = make([]byte, 4)
	case "Uint32":
		min = 0
		max = 4294967295
		raw = make([]byte, 4)
	case "Float32":
		if math.Abs(number) > math.MaxFloat32 {
			return nil, errors.New("Float32 overflow")
		}
		raw = make([]byte, 4)
		binary.BigEndian.PutUint32(raw, math.Float32bits(float32(number)))
		return reorder(raw, p.ByteOrder), nil
	}
	if number < min || number > max || math.Trunc(number) != number {
		return nil, errors.New("integer write out of range")
	}
	if len(raw) == 2 {
		binary.BigEndian.PutUint16(raw, uint16(int64(number)))
	} else {
		binary.BigEndian.PutUint32(raw, uint32(int64(number)))
	}
	return reorder(raw, p.ByteOrder), nil
}
func addressBody(area byte, start, count int, bits bool) []byte {
	word, bit := start, 0
	if bits {
		word = start / 16
		bit = start % 16
	}
	return []byte{area, byte(word >> 8), byte(word), byte(bit), byte(count >> 8), byte(count)}
}
func tcpFrame(command uint32, raw []byte) []byte {
	out := make([]byte, 16+len(raw))
	copy(out, "FINS")
	binary.BigEndian.PutUint32(out[4:8], uint32(8+len(raw)))
	binary.BigEndian.PutUint32(out[8:12], command)
	copy(out[16:], raw)
	return out
}
func readTCP(conn io.Reader, command uint32) ([]byte, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(header[4:8]))
	if !bytes.Equal(header[:4], []byte("FINS")) || size < 8 || size > 4096 || binary.BigEndian.Uint32(header[8:12]) != command || binary.BigEndian.Uint32(header[12:16]) != 0 {
		return nil, errors.New("invalid FINS/TCP envelope")
	}
	raw := make([]byte, size-8)
	_, err := io.ReadFull(conn, raw)
	return raw, err
}
func handshake(conn net.Conn, cfg config) error {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, uint32(cfg.SourceNode))
	if _, err := io.Copy(conn, bytes.NewReader(tcpFrame(0, raw))); err != nil {
		return err
	}
	reply, err := readTCP(conn, 1)
	if err != nil || len(reply) != 8 || int(binary.BigEndian.Uint32(reply[:4])) != cfg.SourceNode || int(binary.BigEndian.Uint32(reply[4:])) != cfg.DestNode {
		return errors.New("FINS/TCP assigned node mismatch; configure returned node IDs")
	}
	return nil
}
func exchange(conn net.Conn, cfg config, sid byte, command byte, body []byte) ([]byte, error) {
	raw := []byte{0x80, 0, 2, byte(cfg.DestNetwork), byte(cfg.DestNode), 0, byte(cfg.SourceNetwork), byte(cfg.SourceNode), 0, sid, 1, command}
	raw = append(raw, body...)
	wire := raw
	if cfg.Mode == "tcp" {
		wire = tcpFrame(2, raw)
	}
	n, err := conn.Write(wire)
	if err != nil || n != len(wire) {
		return nil, errors.New("FINS request write failed")
	}
	var reply []byte
	if cfg.Mode == "tcp" {
		reply, err = readTCP(conn, 2)
	} else {
		buffer := make([]byte, 4097)
		var n int
		n, err = conn.Read(buffer)
		if n > 4096 {
			return nil, errors.New("FINS datagram exceeds limit")
		}
		reply = buffer[:n]
	}
	if err != nil {
		return nil, err
	}
	if len(reply) < 14 || reply[0]&0x40 == 0 || reply[9] != sid || !bytes.Equal(reply[3:6], raw[6:9]) || !bytes.Equal(reply[6:9], raw[3:6]) || reply[10] != 1 || reply[11] != command {
		return nil, errors.New("FINS response correlation mismatch")
	}
	if reply[12] != 0 || reply[13] != 0 {
		return nil, fmt.Errorf("FINS end code %02x%02x", reply[12], reply[13])
	}
	return reply[14:], nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.Lock()
	r := c.resources[h]
	c.mu.Unlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	if len(req.Payload) > 1048576 {
		return sdk.ConnectorResponse{}, errors.New("batch too large")
	}
	var b batch
	decoder := json.NewDecoder(bytes.NewReader(req.Payload))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if decoder.Decode(&b) != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid FINS batch")
	}
	ranges, err := plan(b.Points)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	write := req.Kind == sdk.ConnectorRequestWrite || req.Kind == sdk.ConnectorRequestBatchWrite
	if !write && req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("unsupported operation")
	}
	if write {
		for _, p := range b.Points {
			if _, err = encode(p); err != nil {
				return sdk.ConnectorResponse{}, err
			}
		}
	}
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()
	if b.Expires != nil {
		if *b.Expires <= time.Now().UnixMilli() {
			return sdk.ConnectorResponse{}, errors.New("expired command")
		}
		var done context.CancelFunc
		ctx, done = context.WithDeadline(ctx, time.UnixMilli(*b.Expires))
		defer done()
	}
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	case r.gate <- struct{}{}:
	}
	defer func() { <-r.gate }()
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	case c.slots <- struct{}{}:
	}
	defer func() { <-c.slots }()
	conn, err := (&net.Dialer{}).DialContext(ctx, r.cfg.Mode, net.JoinHostPort(r.cfg.Host, strconv.Itoa(r.cfg.Port)))
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("FINS connection failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	if r.cfg.Mode == "tcp" {
		if err = handshake(conn, r.cfg); err != nil {
			return sdk.ConnectorResponse{}, err
		}
	}
	seed := []byte{0}
	if _, err = rand.Read(seed); err != nil {
		return sdk.ConnectorResponse{}, err
	}
	sid := seed[0]
	values := map[string]any{}
	if write {
		for i, p := range b.Points {
			encoded, _ := encode(p)
			code, _ := areaCode(p)
			width, _ := validate(p)
			start := p.Address
			if p.ValueType == "Bool" {
				start = start*16 + p.Bit
			}
			body := append(addressBody(code, start, width, p.ValueType == "Bool"), encoded...)
			sid++
			reply, e := exchange(conn, r.cfg, sid, 2, body)
			if e != nil || len(reply) != 0 {
				return sdk.ConnectorResponse{}, fmt.Errorf("FINS write outcome unknown after %d confirmed points; no retry", i)
			}
		}
		raw, _ := json.Marshal(map[string]any{"written": len(b.Points)})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}
	for _, segment := range ranges {
		sid++
		reply, e := exchange(conn, r.cfg, sid, 1, addressBody(segment.area, segment.start, segment.end-segment.start, segment.bits))
		size := 2 * (segment.end - segment.start)
		if segment.bits {
			size = segment.end - segment.start
		}
		if e != nil {
			return sdk.ConnectorResponse{}, e
		}
		if len(reply) != size {
			return sdk.ConnectorResponse{}, errors.New("FINS memory response length mismatch")
		}
		for _, p := range segment.points {
			width, _ := validate(p)
			offset := (p.Address - segment.start) * 2
			if segment.bits {
				offset = p.Address*16 + p.Bit - segment.start
				width = 1
			} else {
				width *= 2
			}
			if segment.bits && reply[offset] > 1 {
				return sdk.ConnectorResponse{}, errors.New("invalid FINS bit value")
			}
			values[p.Name] = decode(reply[offset:offset+width], p)
		}
	}
	raw, err := json.Marshal(map[string]any{"values": values, "segments": len(ranges)})
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("nonfinite FINS value")
	}
	return sdk.ConnectorResponse{Payload: raw}, nil
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	c.mu.Lock()
	r := c.resources[h]
	c.mu.Unlock()
	if r == nil {
		return sdk.ResourceStatusEvent{}, errors.New("unknown resource")
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusHealthy, ReasonCode: "DEMAND_MANAGED", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("FINS polling only")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("FINS polling only")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error   { return errors.New("no streams") }
func (c *connector) NackStream(string, sdk.StreamNackRequest) error { return errors.New("no streams") }
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("no streams")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }
