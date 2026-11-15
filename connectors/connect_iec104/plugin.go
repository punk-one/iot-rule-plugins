package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"time"
)

type config struct {
	Mode          string   `json:"mode,omitempty"`
	ReadIOAs      []uint32 `json:"read_ioas,omitempty"`
	InitialGI     bool     `json:"general_interrogation_on_connect"`
	T1MS          int      `json:"t1_ms,omitempty"`
	T3MS          int      `json:"t3_ms,omitempty"`
	GITimeoutMS   int      `json:"gi_timeout_ms,omitempty"`
	MaxFrame      int      `json:"-"`
	StreamFrame   framing  `json:"-"`
	StreamRequest []byte   `json:"-"`

	Host          string `json:"host"`
	Port          int    `json:"port"`
	TimeoutMS     int    `json:"timeout_ms"`
	CommonAddress int    `json:"common_address"`
	Originator    int    `json:"originator"`
	TimeZone      string `json:"time_zone"`
}
type point struct {
	Name string `json:"name"`
	IOA  uint32 `json:"ioa"`
}
type batch struct {
	Points []point `json:"points"`
}
type resource struct {
	openMu, streamMu sync.Mutex
	streams          map[string]*rawSubscription
	active           *rawSession
	queuedBytes      int
	sequence, opens  uint64
	failures         int
	retryAfter       time.Time
	cfg              config
	ctx              context.Context
	cancel           context.CancelFunc
	gate             chan struct{}
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
	return sdk.PluginInfo{ID: "connect-iec104", Name: "IEC104 Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Read-only interrogation snapshots or shared continuous monitoring with strict sequence/heartbeat bounds", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "iec104", Label: "IEC104", Capabilities: []string{"read", "session:per_operation", "stream:best_effort", "session:demand"}, DisplayFields: []string{"host", "port"}}}}
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Mode: "poll", InitialGI: true, T1MS: 15000, T3MS: 20000, GITimeoutMS: 30000, MaxFrame: 65536, Port: 2404, TimeoutMS: 5000, CommonAddress: 1, TimeZone: "UTC"}
	if len(spec.Connection) > 262144 {
		return "", errors.New("IEC104 config exceeds 256 KiB")
	}
	if json.Unmarshal(spec.Connection, &cfg) != nil || spec.ID == "" || cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 {
		return "", errors.New("invalid IEC104 config")
	}
	if cfg.CommonAddress < 1 || cfg.CommonAddress > 65535 || cfg.Originator < 0 || cfg.Originator > 255 {
		return "", errors.New("invalid common address/originator")
	}
	if _, err := time.LoadLocation(cfg.TimeZone); err != nil {
		return "", errors.New("invalid station time_zone")
	}
	if cfg.Mode != "poll" && cfg.Mode != "stream" || cfg.T1MS < 50 || cfg.T1MS > 60000 || cfg.T3MS < cfg.T1MS+50 || cfg.T3MS > 3600000 || cfg.GITimeoutMS < 1000 || cfg.GITimeoutMS > 120000 {
		return "", errors.New("invalid IEC104 mode/timers")
	}
	if cfg.Mode == "stream" {
		if len(cfg.ReadIOAs) < 1 || len(cfg.ReadIOAs) > 4096 {
			return "", errors.New("stream requires 1..4096 explicit read_ioas")
		}
		seen := map[uint32]bool{}
		for _, ioa := range cfg.ReadIOAs {
			if ioa > 0xffffff || seen[ioa] {
				return "", errors.New("invalid/duplicate IEC104 read IOA")
			}
			seen[ioa] = true
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.resources) >= 64 || c.closed || c.resources[spec.ID] != nil {
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

type sample struct {
	counterFlags byte
	transient    bool
	value        any
	quality      byte
	typ          byte
	timestamp    string
	badTime      bool
}

func apdu(control []byte, asdu []byte) []byte {
	out := append([]byte{0x68, byte(4 + len(asdu))}, control...)
	return append(out, asdu...)
}
func readAPDU(reader io.Reader) ([]byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	if header[0] != 0x68 || header[1] < 4 || header[1] > 253 {
		return nil, errors.New("invalid IEC104 APDU header")
	}
	raw := make([]byte, int(header[1]))
	_, err := io.ReadFull(reader, raw)
	return raw, err
}
func writeAPDU(conn io.Writer, raw []byte) error {
	_, err := io.Copy(conn, bytes.NewReader(raw))
	return err
}
func cp56(raw []byte, loc *time.Location) (string, bool) {
	if len(raw) != 7 || raw[2]&0xc0 != 0 || raw[3]&0x60 != 0 || raw[5]&0xf0 != 0 || raw[6]&0x80 != 0 {
		return "", true
	}
	ms := int(binary.LittleEndian.Uint16(raw[:2]))
	minute := int(raw[2] & 0x3f)
	hour := int(raw[3] & 0x1f)
	day := int(raw[4] & 0x1f)
	month := int(raw[5] & 0x0f)
	year := 2000 + int(raw[6]&0x7f)
	if ms >= 60000 || minute > 59 || hour > 23 || day < 1 || day > 31 || month < 1 || month > 12 || year > 2099 {
		return "", true
	}
	date := time.Date(year, time.Month(month), day, hour, minute, ms/1000, (ms%1000)*int(time.Millisecond), loc)
	if date.Day() != day || int(date.Month()) != month || date.Hour() != hour {
		return "", true
	}
	weekday := int(raw[4] >> 5)
	expectedWeekday := int(date.Weekday())
	if expectedWeekday == 0 {
		expectedWeekday = 7
	}
	if weekday != 0 && weekday != expectedWeekday || date.IsDST() != (raw[3]&0x80 != 0) {
		return "", true
	}
	return date.UTC().Format(time.RFC3339Nano), false
}
func parseASDU(raw []byte, common int, loc *time.Location) (map[uint32]sample, bool, error) {
	if len(raw) < 6 {
		return nil, false, errors.New("ASDU too short")
	}
	if int(binary.LittleEndian.Uint16(raw[4:6])) != common {
		return nil, false, errors.New("ASDU common address mismatch")
	}
	cause := raw[2] & 0x3f
	if raw[2]&0xc0 != 0 {
		return nil, false, errors.New("negative activation/interrogation confirmation")
	}
	if raw[0] == 100 {
		if len(raw) != 10 || raw[1] != 1 || !bytes.Equal(raw[6:9], []byte{0, 0, 0}) || raw[9] != 20 {
			return nil, false, errors.New("invalid interrogation confirmation")
		}
		return nil, cause == 10, nil
	}
	if raw[0] == 70 {
		if len(raw) != 10 || raw[1] != 1 || cause != 4 || !bytes.Equal(raw[6:9], []byte{0, 0, 0}) {
			return nil, false, errors.New("invalid end-of-initialization ASDU")
		}
		return nil, false, nil
	}
	typ := raw[0]
	base := typ
	stamped := false
	switch typ {
	case 30:
		base = 1
		stamped = true
	case 31:
		base = 3
		stamped = true
	case 32:
		base = 5
		stamped = true
	case 33:
		base = 7
		stamped = true
	case 34:
		base = 9
		stamped = true
	case 35:
		base = 11
		stamped = true
	case 36:
		base = 13
		stamped = true
	case 37:
		base = 15
		stamped = true
	}
	size := 0
	switch base {
	case 1, 3:
		size = 1
	case 5:
		size = 2
	case 7, 13, 15:
		size = 5
	case 9, 11:
		size = 3
	default:
		return nil, false, errors.New("unsupported IEC104 ASDU type")
	}
	if stamped {
		size += 7
	}
	count := int(raw[1] & 0x7f)
	sequential := raw[1]&0x80 != 0
	if count == 0 {
		return nil, false, errors.New("empty ASDU information objects")
	}
	offset := 6
	var address uint32
	out := map[uint32]sample{}
	for i := 0; i < count; i++ {
		if !sequential || i == 0 {
			if len(raw)-offset < 3 {
				return nil, false, errors.New("truncated IOA")
			}
			address = uint32(raw[offset]) | uint32(raw[offset+1])<<8 | uint32(raw[offset+2])<<16
			offset += 3
		} else {
			address++
			if address > 0xffffff {
				return nil, false, errors.New("IOA overflow")
			}
		}
		if len(raw)-offset < size {
			return nil, false, errors.New("truncated information object")
		}
		data := raw[offset : offset+size]
		offset += size
		v := sample{typ: typ}
		switch base {
		case 1:
			v.value = data[0]&1 != 0
			v.quality = data[0] & 0xf0
		case 3:
			v.value = int(data[0] & 3)
			v.quality = data[0] & 0xf0
			if data[0]&3 == 0 || data[0]&3 == 3 {
				v.quality |= 0x80
			}
		case 5:
			signed := int(data[0] & 0x7f)
			if signed&0x40 != 0 {
				signed -= 128
			}
			v.value = signed
			v.transient = data[0]&0x80 != 0
			v.quality = data[1]
		case 7:
			v.value = binary.LittleEndian.Uint32(data)
			v.quality = data[4]
		case 9:
			v.value = float64(int16(binary.LittleEndian.Uint16(data))) / 32768
			v.quality = data[2]
		case 11:
			v.value = int16(binary.LittleEndian.Uint16(data))
			v.quality = data[2]
		case 13:
			value := math.Float32frombits(binary.LittleEndian.Uint32(data))
			v.quality = data[4]
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				v.value = nil
				v.quality |= 0x80
			} else {
				v.value = value
			}
		case 15:
			v.value = int32(binary.LittleEndian.Uint32(data))
			v.quality = data[4] & 0x80
			v.counterFlags = data[4]
		}
		if stamped {
			v.timestamp, v.badTime = cp56(data[size-7:], loc)
		}
		if _, exists := out[address]; exists {
			return nil, false, errors.New("duplicate IOA in ASDU")
		}
		out[address] = v
	}
	if offset != len(raw) {
		return nil, false, errors.New("ASDU trailing bytes")
	}
	return out, false, nil
}
func interrogate(conn net.Conn, cfg config, wanted map[uint32]bool) (map[uint32]sample, error) {
	if err := writeAPDU(conn, apdu([]byte{7, 0, 0, 0}, nil)); err != nil {
		return nil, err
	}
	reply, err := readAPDU(conn)
	if err != nil || !bytes.Equal(reply, []byte{0x0b, 0, 0, 0}) {
		return nil, errors.New("STARTDT confirmation missing")
	}
	asdu := []byte{100, 1, 6, byte(cfg.Originator), byte(cfg.CommonAddress), byte(cfg.CommonAddress >> 8), 0, 0, 0, 20}
	if err = writeAPDU(conn, apdu([]byte{0, 0, 0, 0}, asdu)); err != nil {
		return nil, err
	}
	receive := uint16(0)
	loc, _ := time.LoadLocation(cfg.TimeZone)
	out := map[uint32]sample{}
	for frames := 0; frames < 4096; frames++ {
		reply, err = readAPDU(conn)
		if err != nil {
			return nil, err
		}
		if reply[0]&1 == 0 {
			send := binary.LittleEndian.Uint16(reply[:2]) >> 1
			ack := binary.LittleEndian.Uint16(reply[2:4]) >> 1
			if send != receive || ack > 1 || reply[2]&1 != 0 {
				return nil, errors.New("IEC104 sequence/ack mismatch")
			}
			receive = (receive + 1) & 0x7fff
			data, done, err := parseASDU(reply[4:], cfg.CommonAddress, loc)
			if err != nil {
				return nil, err
			}
			if err = writeAPDU(conn, apdu([]byte{1, 0, byte(receive << 1), byte(receive >> 7)}, nil)); err != nil {
				return nil, err
			}
			for address, value := range data {
				if wanted[address] {
					out[address] = value
				}
			}
			if done {
				return out, nil
			}
		} else if reply[0]&3 == 1 {
			if len(reply) != 4 || reply[0] != 1 || reply[1] != 0 || reply[2]&1 != 0 || binary.LittleEndian.Uint16(reply[2:4])>>1 > 1 {
				return nil, errors.New("invalid supervisory ACK")
			}
		} else {
			if len(reply) != 4 || reply[1] != 0 || reply[2] != 0 || reply[3] != 0 {
				return nil, errors.New("invalid unnumbered frame")
			}
			if reply[0] == 0x43 {
				if err = writeAPDU(conn, apdu([]byte{0x83, 0, 0, 0}, nil)); err != nil {
					return nil, err
				}
			} else if reply[0] != 0x83 {
				return nil, errors.New("unexpected U frame during interrogation")
			}
		}
	}
	return nil, errors.New("interrogation frame count limit exceeded")
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.Lock()
	r := c.resources[h]
	c.mu.Unlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	if r.cfg.Mode == "stream" {
		return sdk.ConnectorResponse{}, errors.New("stream resource rejects poll/control operations")
	}
	if req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("IEC104 control writes are not supported")
	}
	var b batch
	if len(req.Payload) > 131072 || json.Unmarshal(req.Payload, &b) != nil || len(b.Points) < 1 || len(b.Points) > 1024 {
		return sdk.ConnectorResponse{}, errors.New("bounded point table required")
	}
	names := map[string]bool{}
	wanted := map[uint32]bool{}
	for _, p := range b.Points {
		if p.Name == "" || len(p.Name) > 128 || names[p.Name] || p.IOA > 0xffffff || wanted[p.IOA] {
			return sdk.ConnectorResponse{}, errors.New("duplicate/invalid point name or IOA")
		}
		names[p.Name] = true
		wanted[p.IOA] = true
	}
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()
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
		return sdk.ConnectorResponse{}, errors.New("IEC104 dial failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	samples, err := interrogate(conn, r.cfg, wanted)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("IEC104 interrogation failed")
	}
	values := map[string]any{}
	quality := map[string]any{}
	details := map[string]any{}
	for _, p := range b.Points {
		s, ok := samples[p.IOA]
		if !ok {
			values[p.Name] = nil
			quality[p.Name] = "bad:missing"
			continue
		}
		values[p.Name] = s.value
		quality[p.Name], details[p.Name] = sampleQualityAndDetails(p.IOA, s)
	}
	raw, _ := json.Marshal(map[string]any{"values": values, "quality": quality, "details": details})
	return sdk.ConnectorResponse{Payload: raw}, nil
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
			reason = "IEC104_STREAM_ACTIVE"
		}
		if r.failures > 0 {
			status = sdk.ResourceStatusDegraded
			reason = "IEC104_STREAM_FAILED"
		}
		r.streamMu.Unlock()
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: status, ReasonCode: reason, Message: "Best-effort transport state is not application delivery confirmation", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}

func (c *connector) get(h string) (*resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.resources[h]
	if r == nil {
		return nil, errors.New("unknown IEC104 resource")
	}
	return r, nil
}
