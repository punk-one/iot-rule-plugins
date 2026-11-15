package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type config struct {
	expectedDevice uint64
	Mode           string  `json:"mode"`
	StreamFrame    framing `json:"stream_frame"`
	StreamRequest  []byte  `json:"stream_request_base64"`
	Device         string  `json:"device"`
	Baud           int     `json:"baud"`
	DataBits       int     `json:"data_bits"`
	Parity         string  `json:"parity"`
	StopBits       int     `json:"stop_bits"`
	TimeoutMS      int     `json:"timeout_ms"`
	MaxFrame       int     `json:"max_frame_bytes"`
	AllowWrite     bool    `json:"allow_write"`
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
	deviceKey       uint64
}
type connector struct {
	mu        sync.Mutex
	resources map[string]*resource
	closed    bool
	slots     chan struct{}
	devices   map[uint64]bool
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, slots: make(chan struct{}, 8), devices: map[uint64]bool{}}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-serial", Name: "Serial Freeport Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Bounded poll/response or shared read-only continuous stream with explicit framing", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "serial", Label: "Serial", Capabilities: []string{"read", "write", "shared_duplex", "session:per_operation", "stream:best_effort", "session:demand"}, DisplayFields: []string{"device", "baud"}}}}
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Baud: 9600, DataBits: 8, Parity: "none", StopBits: 1, TimeoutMS: 5000, MaxFrame: 65536}
	if len(spec.Connection) > 2097152 {
		return "", errors.New("raw config exceeds 2 MiB")
	}
	if json.Unmarshal(spec.Connection, &cfg) != nil || cfg.Device == "" || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxFrame < 1 || cfg.MaxFrame > 1048576 || spec.ID == "" || cfg.DataBits < 7 || cfg.DataBits > 8 || (cfg.Parity != "none" && cfg.Parity != "even" && cfg.Parity != "odd") || (cfg.StopBits != 1 && cfg.StopBits != 2) {
		return "", errors.New("invalid serial settings")
	}
	if _, ok := baudRates[cfg.Baud]; !ok {
		return "", errors.New("unsupported baud rate")
	}
	path, err := filepath.EvalSymlinks(cfg.Device)
	if err != nil || !strings.HasPrefix(path, "/dev/") {
		return "", errors.New("serial device must resolve beneath /dev")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode()&os.ModeCharDevice == 0 {
		return "", errors.New("serial path must be a character device")
	}
	var stat unix.Stat_t
	if unix.Stat(path, &stat) != nil {
		return "", errors.New("cannot identify serial device")
	}
	cfg.Device = path
	cfg.expectedDevice = uint64(stat.Rdev)
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
	if len(c.resources) >= 64 || c.closed || c.resources[spec.ID] != nil || c.devices[uint64(stat.Rdev)] {
		cancel()
		return "", errors.New("resource exists, serial device owned, or connector stopped")
	}
	c.devices[uint64(stat.Rdev)] = true
	c.resources[spec.ID] = &resource{cfg: cfg, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1), deviceKey: uint64(stat.Rdev)}
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
		c.mu.Lock()
		delete(c.devices, r.deviceKey)
		c.mu.Unlock()
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

var baudRates = map[int]uint32{1200: unix.B1200, 2400: unix.B2400, 4800: unix.B4800, 9600: unix.B9600, 19200: unix.B19200, 38400: unix.B38400, 57600: unix.B57600, 115200: unix.B115200, 230400: unix.B230400}

type serialConnection struct {
	*os.File
	rawFD int
	once  sync.Once
	err   error
}

func (f *serialConnection) Close() error {
	f.once.Do(func() { unix.IoctlSetInt(f.rawFD, unix.TIOCNXCL, 0); f.err = f.File.Close() })
	return f.err
}
func dial(ctx context.Context, cfg config) (connection, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	fd, err := unix.Open(cfg.Device, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	exclusive := false
	fail := func(err error) (connection, error) {
		if exclusive {
			unix.IoctlSetInt(fd, unix.TIOCNXCL, 0)
		}
		unix.Close(fd)
		return nil, err
	}
	var opened unix.Stat_t
	if unix.Fstat(fd, &opened) != nil || cfg.expectedDevice != uint64(opened.Rdev) {
		return fail(errors.New("serial device identity changed; recreate resource explicitly"))
	}
	if err = unix.IoctlSetInt(fd, unix.TIOCEXCL, 0); err != nil {
		return fail(err)
	}
	exclusive = true
	termios, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return fail(err)
	}
	termios.Iflag = unix.IGNPAR
	termios.Oflag = 0
	termios.Lflag = 0
	termios.Cflag = unix.CLOCAL | unix.CREAD | baudRates[cfg.Baud]
	if cfg.DataBits == 7 {
		termios.Cflag |= unix.CS7
	} else {
		termios.Cflag |= unix.CS8
	}
	if cfg.Parity != "none" {
		termios.Cflag |= unix.PARENB
		if cfg.Parity == "odd" {
			termios.Cflag |= unix.PARODD
		}
		termios.Iflag |= unix.INPCK
	}
	if cfg.StopBits == 2 {
		termios.Cflag |= unix.CSTOPB
	}
	termios.Ispeed = baudRates[cfg.Baud]
	termios.Ospeed = baudRates[cfg.Baud]
	termios.Cc[unix.VMIN] = 1
	termios.Cc[unix.VTIME] = 0
	if err = unix.IoctlSetTermios(fd, unix.TCSETS, termios); err != nil {
		return fail(err)
	}
	if err = unix.IoctlSetInt(fd, unix.TCFLSH, unix.TCIFLUSH); err != nil {
		return fail(err)
	}
	file := os.NewFile(uintptr(fd), cfg.Device)
	if file == nil {
		return fail(fmt.Errorf("cannot create serial file"))
	}
	return &serialConnection{File: file, rawFD: fd}, nil
}
func validateFrame(f framing, limit int) error {
	switch f.Mode {
	case "fixed":
		if f.Length < 1 || f.Length > limit {
			return errors.New("invalid fixed frame length")
		}
	case "delimiter":
		if len(f.Delimiter) < 1 || len(f.Delimiter) > 32 {
			return errors.New("delimiter requires 1..32 bytes")
		}
	case "length_prefix":
		if f.Offset < 0 || f.Offset > 60 || (f.Width != 1 && f.Width != 2 && f.Width != 4) || (f.Endian != "big" && f.Endian != "little") || f.Offset+f.Width > limit || f.Adjust < -limit || f.Adjust > limit {
			return errors.New("invalid length prefix")
		}
	default:
		return errors.New("unknown framing mode")
	}
	return nil
}
func readFrame(reader io.Reader, f framing, limit int) ([]byte, error) {
	if err := validateFrame(f, limit); err != nil {
		return nil, err
	}
	switch f.Mode {
	case "fixed":
		raw := make([]byte, f.Length)
		_, err := io.ReadFull(reader, raw)
		return raw, err
	case "delimiter":
		buf := bufio.NewReader(reader)
		out := make([]byte, 0, min(limit, 4096))
		for len(out) < limit {
			b, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			out = append(out, b)
			if bytes.HasSuffix(out, f.Delimiter) {
				return out[:len(out)-len(f.Delimiter)], nil
			}
		}
		return nil, errors.New("delimiter frame limit exceeded")
	case "length_prefix":
		header := make([]byte, f.Offset+f.Width)
		if _, err := io.ReadFull(reader, header); err != nil {
			return nil, err
		}
		var order binary.ByteOrder = binary.BigEndian
		if f.Endian == "little" {
			order = binary.LittleEndian
		}
		var length uint32
		switch f.Width {
		case 1:
			length = uint32(header[f.Offset])
		case 2:
			length = uint32(order.Uint16(header[f.Offset:]))
		case 4:
			length = order.Uint32(header[f.Offset:])
		}
		total := int64(length) + int64(f.Adjust)
		if total < int64(len(header)) || total > int64(limit) {
			return nil, errors.New("declared frame length outside limit")
		}
		out := make([]byte, int(total))
		copy(out, header)
		_, err := io.ReadFull(reader, out[len(header):])
		return out, err
	}
	return nil, errors.New("unsupported framing")
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
	if err = writeAll(conn, spec.Data); err != nil {
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
