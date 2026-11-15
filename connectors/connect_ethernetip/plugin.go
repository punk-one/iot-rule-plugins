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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Slot      *int   `json:"slot"`
	TimeoutMS int    `json:"timeout_ms"`
}
type point struct {
	Name      string `json:"name"`
	Tag       string `json:"tag"`
	ValueType string `json:"valueType"`
	Value     any    `json:"value,omitempty"`
}
type batch struct {
	Points  []point `json:"points"`
	Expires *int64  `json:"expires_at,omitempty"`
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
	return sdk.PluginInfo{ID: "connect-ethernetip", Name: "EtherNet/IP Tag Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Bounded explicit Logix tag read/write with owned session deadlines", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "ethernetip", Label: "EtherNet/IP Logix", Capabilities: []string{"read", "write", "shared_duplex", "session:per_operation"}, DisplayFields: []string{"host", "port", "slot"}}}}
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Port: 44818, TimeoutMS: 3000}
	if json.Unmarshal(spec.Connection, &cfg) != nil || spec.ID == "" || cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.Slot != nil && (*cfg.Slot < 0 || *cfg.Slot > 31) {
		return "", errors.New("invalid EtherNet/IP config")
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

var segmentPattern = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_:]*)(?:\[([0-9]+)\])?$`)

func tagPath(tag string) ([]byte, error) {
	if tag == "" || len(tag) > 200 {
		return nil, errors.New("tag must contain 1..200 characters")
	}
	out := []byte{}
	for _, segment := range strings.Split(tag, ".") {
		match := segmentPattern.FindStringSubmatch(segment)
		if match == nil || len(match[1]) > 100 {
			return nil, errors.New("unsupported tag path; symbolic members and one-dimensional index only")
		}
		name := match[1]
		out = append(out, 0x91, byte(len(name)))
		out = append(out, []byte(name)...)
		if len(name)%2 != 0 {
			out = append(out, 0)
		}
		if match[2] != "" {
			index, err := strconv.ParseUint(match[2], 10, 32)
			if err != nil {
				return nil, errors.New("array index overflow")
			}
			switch {
			case index <= 255:
				out = append(out, 0x28, byte(index))
			case index <= 65535:
				out = append(out, 0x29, 0, byte(index), byte(index>>8))
			default:
				out = append(out, 0x2a, 0, byte(index), byte(index>>8), byte(index>>16), byte(index>>24))
			}
		}
	}
	if len(out) > 220 {
		return nil, errors.New("CIP path exceeds preview limit")
	}
	return out, nil
}
func typeInfo(kind string) (uint16, int, error) {
	switch kind {
	case "Bool":
		return 0xc1, 1, nil
	case "Int16":
		return 0xc3, 2, nil
	case "Int32":
		return 0xc4, 4, nil
	case "Int64":
		return 0xc5, 8, nil
	case "Uint16":
		return 0xc7, 2, nil
	case "Uint32":
		return 0xc8, 4, nil
	case "Uint64":
		return 0xc9, 8, nil
	case "Float32":
		return 0xca, 4, nil
	case "Float64":
		return 0xcb, 8, nil
	default:
		return 0, 0, errors.New("unsupported atomic CIP type")
	}
}
func encodeValue(p point) ([]byte, error) {
	_, width, err := typeInfo(p.ValueType)
	if err != nil {
		return nil, err
	}
	out := make([]byte, width)
	if p.ValueType == "Bool" {
		value, ok := p.Value.(bool)
		if !ok {
			return nil, errors.New("Bool requires boolean")
		}
		if value {
			out[0] = 1
		}
		return out, nil
	}
	if p.ValueType == "Int64" || p.ValueType == "Uint64" {
		text, ok := p.Value.(string)
		if !ok {
			return nil, errors.New("64-bit write requires exact decimal string")
		}
		if p.ValueType == "Int64" {
			v, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint64(out, uint64(v))
		} else {
			v, err := strconv.ParseUint(text, 10, 64)
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint64(out, v)
		}
		return out, nil
	}
	var value float64
	switch v := p.Value.(type) {
	case json.Number:
		value, err = v.Float64()
	case float64:
		value = v
	default:
		err = errors.New("numeric scalar required")
	}
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, errors.New("invalid number")
	}
	switch p.ValueType {
	case "Float32":
		if math.Abs(value) > math.MaxFloat32 {
			return nil, errors.New("Float32 overflow")
		}
		binary.LittleEndian.PutUint32(out, math.Float32bits(float32(value)))
		return out, nil
	case "Float64":
		binary.LittleEndian.PutUint64(out, math.Float64bits(value))
		return out, nil
	}
	minimum, maximum := float64(0), float64(4294967295)
	switch p.ValueType {
	case "Int16":
		minimum = -32768
		maximum = 32767
	case "Uint16":
		maximum = 65535
	case "Int32":
		minimum = -2147483648
		maximum = 2147483647
	}
	if math.Trunc(value) != value || value < minimum || value > maximum {
		return nil, errors.New("integer outside type range")
	}
	if width == 2 {
		binary.LittleEndian.PutUint16(out, uint16(int64(value)))
	} else {
		binary.LittleEndian.PutUint32(out, uint32(int64(value)))
	}
	return out, nil
}
func decodeValue(raw []byte, kind string) (any, error) {
	code, width, err := typeInfo(kind)
	if err != nil || len(raw) != width+2 || binary.LittleEndian.Uint16(raw[:2]) != code {
		return nil, errors.New("CIP type or value width mismatch")
	}
	raw = raw[2:]
	switch kind {
	case "Bool":
		return raw[0] != 0, nil
	case "Int16":
		return int16(binary.LittleEndian.Uint16(raw)), nil
	case "Uint16":
		return binary.LittleEndian.Uint16(raw), nil
	case "Int32":
		return int32(binary.LittleEndian.Uint32(raw)), nil
	case "Uint32":
		return binary.LittleEndian.Uint32(raw), nil
	case "Int64":
		return strconv.FormatInt(int64(binary.LittleEndian.Uint64(raw)), 10), nil
	case "Uint64":
		return strconv.FormatUint(binary.LittleEndian.Uint64(raw), 10), nil
	case "Float32":
		return math.Float32frombits(binary.LittleEndian.Uint32(raw)), nil
	case "Float64":
		return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
	}
	return nil, errors.New("invalid type")
}
func packet(conn io.ReadWriter, command uint16, session uint32, contextID uint64, body []byte) (uint32, []byte, error) {
	if len(body) > 4096 {
		return 0, nil, errors.New("EtherNet/IP request too large")
	}
	header := make([]byte, 24)
	binary.LittleEndian.PutUint16(header, command)
	binary.LittleEndian.PutUint16(header[2:4], uint16(len(body)))
	binary.LittleEndian.PutUint32(header[4:8], session)
	binary.LittleEndian.PutUint64(header[12:20], contextID)
	if _, err := io.Copy(conn, bytes.NewReader(append(header, body...))); err != nil {
		return 0, nil, err
	}
	response := make([]byte, 24)
	if _, err := io.ReadFull(conn, response); err != nil {
		return 0, nil, err
	}
	size := int(binary.LittleEndian.Uint16(response[2:4]))
	actualSession := binary.LittleEndian.Uint32(response[4:8])
	if binary.LittleEndian.Uint16(response) != command || size > 4096 || binary.LittleEndian.Uint32(response[8:12]) != 0 || binary.LittleEndian.Uint64(response[12:20]) != contextID || binary.LittleEndian.Uint32(response[20:24]) != 0 || session != 0 && actualSession != session {
		return 0, nil, errors.New("EtherNet/IP envelope correlation/status mismatch")
	}
	raw := make([]byte, size)
	_, err := io.ReadFull(conn, raw)
	return actualSession, raw, err
}
func cipResult(raw []byte, service byte) ([]byte, error) {
	if len(raw) < 4 || raw[0] != service|0x80 || raw[1] != 0 {
		return nil, errors.New("CIP response service mismatch")
	}
	statusWords := int(raw[3]) * 2
	if len(raw) < 4+statusWords {
		return nil, errors.New("CIP additional status truncated")
	}
	if raw[2] != 0 {
		return nil, fmt.Errorf("CIP status %02x", raw[2])
	}
	return raw[4+statusWords:], nil
}
func tagRequest(p point, write bool, slot *int) ([]byte, byte, error) {
	path, err := tagPath(p.Tag)
	if err != nil {
		return nil, 0, err
	}
	service := byte(0x4c)
	if write {
		service = 0x4d
	}
	raw := append([]byte{service, byte(len(path) / 2)}, path...)
	if write {
		encoded, err := encodeValue(p)
		if err != nil {
			return nil, 0, err
		}
		code, _, _ := typeInfo(p.ValueType)
		raw = append(raw, byte(code), byte(code>>8), 1, 0)
		raw = append(raw, encoded...)
	} else {
		raw = append(raw, 1, 0)
	}
	if slot != nil {
		inner := raw
		raw = []byte{0x52, 2, 0x20, 6, 0x24, 1, 0x0a, 0x0e, byte(len(inner)), byte(len(inner) >> 8)}
		raw = append(raw, inner...)
		if len(inner)%2 != 0 {
			raw = append(raw, 0)
		}
		raw = append(raw, 1, 0, 1, byte(*slot))
	}
	return raw, service, nil
}
func cpfRequest(raw []byte) []byte {
	out := []byte{0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0xb2, 0, byte(len(raw)), byte(len(raw) >> 8)}
	return append(out, raw...)
}
func cpfResponse(raw []byte) ([]byte, error) {
	if len(raw) < 8 || binary.LittleEndian.Uint32(raw) != 0 {
		return nil, errors.New("invalid CPF prefix")
	}
	count := int(binary.LittleEndian.Uint16(raw[6:8]))
	if count < 1 || count > 8 {
		return nil, errors.New("invalid CPF item count")
	}
	offset := 8
	var data []byte
	for i := 0; i < count; i++ {
		if len(raw)-offset < 4 {
			return nil, errors.New("truncated CPF item")
		}
		kind := binary.LittleEndian.Uint16(raw[offset:])
		length := int(binary.LittleEndian.Uint16(raw[offset+2:]))
		offset += 4
		if length > len(raw)-offset {
			return nil, errors.New("CPF item exceeds frame")
		}
		if kind == 0xb2 {
			if data != nil {
				return nil, errors.New("duplicate CIP item")
			}
			data = raw[offset : offset+length]
		}
		offset += length
	}
	if offset != len(raw) || len(data) < 4 {
		return nil, errors.New("missing CIP item or trailing CPF bytes")
	}
	return data, nil
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
	if decoder.Decode(&b) != nil || len(b.Points) < 1 || len(b.Points) > 128 {
		return sdk.ConnectorResponse{}, errors.New("batch requires 1..128 tags")
	}
	write := req.Kind == sdk.ConnectorRequestWrite || req.Kind == sdk.ConnectorRequestBatchWrite
	if !write && req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("unsupported operation")
	}
	seen := map[string]bool{}
	messages := make([][]byte, len(b.Points))
	services := make([]byte, len(b.Points))
	for i, p := range b.Points {
		if p.Name == "" || seen[p.Name] {
			return sdk.ConnectorResponse{}, errors.New("duplicate/empty point")
		}
		seen[p.Name] = true
		if _, _, err := typeInfo(p.ValueType); err != nil {
			return sdk.ConnectorResponse{}, err
		}
		var err error
		messages[i], services[i], err = tagRequest(p, write, r.cfg.Slot)
		if err != nil {
			return sdk.ConnectorResponse{}, err
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
			return sdk.ConnectorResponse{}, errors.New("command expired")
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
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(r.cfg.Host, strconv.Itoa(r.cfg.Port)))
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("EtherNet/IP dial failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	seed := make([]byte, 8)
	if _, err = rand.Read(seed); err != nil {
		return sdk.ConnectorResponse{}, err
	}
	contextID := binary.LittleEndian.Uint64(seed)
	session, reply, err := packet(conn, 0x65, 0, contextID, []byte{1, 0, 0, 0})
	if err != nil || session == 0 || !bytes.Equal(reply, []byte{1, 0, 0, 0}) {
		return sdk.ConnectorResponse{}, errors.New("EtherNet/IP register session failed")
	}
	values := map[string]any{}
	for i, p := range b.Points {
		contextID++
		_, raw, err := packet(conn, 0x6f, session, contextID, cpfRequest(messages[i]))
		if err == nil {
			raw, err = cpfResponse(raw)
		}
		if err == nil && r.cfg.Slot != nil && len(raw) > 0 && raw[0] == 0xd2 {
			raw, err = cipResult(raw, 0x52)
		}
		if err == nil {
			raw, err = cipResult(raw, services[i])
		}
		if err != nil {
			if write {
				return sdk.ConnectorResponse{}, fmt.Errorf("EtherNet/IP write outcome unknown after %d confirmed tags; no retry", i)
			}
			return sdk.ConnectorResponse{}, err
		}
		if write {
			if len(raw) != 0 {
				return sdk.ConnectorResponse{}, errors.New("unexpected write response data; outcome unknown")
			}
		} else {
			value, e := decodeValue(raw, p.ValueType)
			if e != nil {
				return sdk.ConnectorResponse{}, e
			}
			values[p.Name] = value
		}
	}
	result := map[string]any{"values": values, "segments": len(b.Points)}
	if write {
		result = map[string]any{"written": len(b.Points)}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("nonfinite/invalid CIP value")
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
	return sdk.StreamOpenResponse{}, errors.New("explicit polling only")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("explicit polling only")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error   { return errors.New("no streams") }
func (c *connector) NackStream(string, sdk.StreamNackRequest) error { return errors.New("no streams") }
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("no streams")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }
