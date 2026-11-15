package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
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
	Host      string `json:"host"`
	Port      int    `json:"port"`
	TimeoutMS int    `json:"timeout_ms"`
	AllowDNC  bool   `json:"allow_dnc_reads"`
}
type point struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}
type batch struct {
	Points []point `json:"points"`
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
	return sdk.PluginInfo{ID: "connect-lsv2", Name: "LSV2 Read Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Read-only LSV2 version and selected runtime queries with bounded framed I/O", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "lsv2", Label: "Heidenhain LSV2", Capabilities: []string{"read", "session:per_operation"}, DisplayFields: []string{"host", "port"}}}}
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Port: 19000, TimeoutMS: 3000}
	if json.Unmarshal(spec.Connection, &cfg) != nil || spec.ID == "" || cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 {
		return "", errors.New("invalid LSV2 config")
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

type query struct {
	command  string
	selector uint16
	version  bool
	dnc      bool
	format   string
}

var queries = map[string]query{
	"version":           {command: "R_VR", version: true, format: "version"},
	"control_id":        {command: "R_VR", selector: 5, version: true, format: "text"},
	"release":           {command: "R_VR", selector: 6, version: true, format: "text"},
	"splc_version":      {command: "R_VR", selector: 7, version: true, format: "text"},
	"execution_state":   {command: "R_RI", selector: 23, dnc: true, format: "u16"},
	"program_state":     {command: "R_RI", selector: 26, dnc: true, format: "u16"},
	"nc_uptime":         {command: "R_RI", selector: 30, dnc: true, format: "u32"},
	"machine_uptime":    {command: "R_RI", selector: 31, dnc: true, format: "u32"},
	"machine_runtime":   {command: "R_RI", selector: 32, dnc: true, format: "u32"},
	"spindle_runtime_0": {command: "R_RI", selector: 33, dnc: true, format: "u32"},
	"spindle_runtime_1": {command: "R_RI", selector: 34, dnc: true, format: "u32"},
	"overrides":         {command: "R_RI", selector: 25, dnc: true, format: "overrides"},
	"current_tool":      {command: "R_RI", selector: 51, dnc: true, format: "tool"},
	"axis_locations":    {command: "R_RI", selector: 22, dnc: true, format: "axes"},
	"selected_program":  {command: "R_RI", selector: 24, dnc: true, format: "stack"},
}

func telegram(command string, payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(out[:4], uint32(len(payload)))
	copy(out[4:8], command)
	copy(out[8:], payload)
	return out
}
func readTelegram(reader io.Reader) (string, []byte, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(reader, header); err != nil {
		return "", nil, err
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length > 65536 {
		return "", nil, errors.New("LSV2 payload exceeds limit")
	}
	raw := make([]byte, int(length))
	_, err := io.ReadFull(reader, raw)
	return string(header[4:8]), raw, err
}
func send(conn io.ReadWriter, command string, payload []byte) (string, []byte, error) {
	if _, err := io.Copy(conn, bytes.NewReader(telegram(command, payload))); err != nil {
		return "", nil, err
	}
	return readTelegram(conn)
}
func login(conn io.ReadWriter, level string) error {
	command, _, err := send(conn, "A_LG", append([]byte(level), 0))
	if err != nil {
		return err
	}
	if command != "T_OK" {
		return errors.New("LSV2 read login denied")
	}
	return nil
}
func textValue(raw []byte) (string, error) {
	raw = bytes.TrimRight(raw, "\x00")
	// LSV2 text fields use ISO-8859-1, not UTF-8.
	runes := make([]rune, len(raw))
	for i, b := range raw {
		runes[i] = rune(b)
	}
	return string(runes), nil
}
func decode(q query, raw []byte) (any, error) {
	switch q.format {
	case "u16":
		if len(raw) != 2 {
			return nil, errors.New("invalid u16 runtime payload")
		}
		return binary.BigEndian.Uint16(raw), nil
	case "u32":
		if len(raw) != 4 {
			return nil, errors.New("invalid u32 runtime payload")
		}
		return binary.BigEndian.Uint32(raw), nil
	case "text":
		return textValue(raw)
	case "version":
		parts := bytes.Split(bytes.TrimRight(raw, "\x00"), []byte{0})
		if len(parts) != 4 && len(parts) != 5 {
			return nil, errors.New("unsupported version response layout")
		}
		names := []string{"control", "nc_version", "plc_version", "options"}
		if len(parts) == 5 {
			names = []string{"control", "nc_version", "plc_version", "splc_version", "options"}
		}
		out := map[string]any{}
		for i, part := range parts {
			text, err := textValue(part)
			if err != nil {
				return nil, err
			}
			out[names[i]] = text
		}
		return out, nil
	case "overrides", "tool", "axes", "stack":
		return decodeComplex(q, raw)
	default:
		return map[string]any{"raw_base64": base64.StdEncoding.EncodeToString(raw), "selector": q.selector}, nil
	}
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.Lock()
	r := c.resources[h]
	c.mu.Unlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	if req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("LSV2 plugin is read-only; no raw command or override writes")
	}
	if len(req.Payload) > 65536 {
		return sdk.ConnectorResponse{}, errors.New("point request too large")
	}
	var b batch
	if json.Unmarshal(req.Payload, &b) != nil || len(b.Points) < 1 || len(b.Points) > 64 {
		return sdk.ConnectorResponse{}, errors.New("requires 1..64 named queries")
	}
	seen := map[string]bool{}
	needDNC := false
	for _, p := range b.Points {
		q, ok := queries[p.Query]
		if !ok || p.Name == "" || seen[p.Name] {
			return sdk.ConnectorResponse{}, errors.New("invalid or duplicate query")
		}
		seen[p.Name] = true
		needDNC = needDNC || q.dnc
	}
	if needDNC && !r.cfg.AllowDNC {
		return sdk.ConnectorResponse{}, errors.New("runtime queries require explicit allow_dnc_reads")
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
		return sdk.ConnectorResponse{}, errors.New("LSV2 dial failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	if err = login(conn, "INSPECT"); err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if needDNC {
		if err = login(conn, "DNC"); err != nil {
			return sdk.ConnectorResponse{}, err
		}
	}
	values := map[string]any{}
	quality := map[string]any{}
	for _, p := range b.Points {
		q := queries[p.Query]
		payload := []byte{}
		if q.version {
			if q.selector != 0 {
				payload = []byte{byte(q.selector)}
			}
		} else {
			payload = []byte{byte(q.selector >> 8), byte(q.selector)}
		}
		command, raw, err := send(conn, q.command, payload)
		if err != nil {
			return sdk.ConnectorResponse{}, errors.New("LSV2 framed read failed")
		}
		expected := "S_RI"
		if q.version {
			expected = "S_VR"
		}
		if command != expected {
			if command == "T_ER" || command == "A_ER" {
				values[p.Name] = nil
				quality[p.Name] = "bad:device_rejected"
				continue
			}
			return sdk.ConnectorResponse{}, errors.New("LSV2 response command mismatch")
		}
		value, err := decode(q, raw)
		if err != nil {
			values[p.Name] = nil
			quality[p.Name] = "bad:decode"
			continue
		}
		values[p.Name] = value
		quality[p.Name] = "good"
	}
	raw, _ := json.Marshal(map[string]any{"values": values, "quality": quality})
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
