package main

import (
	"context"
	"encoding/json"
	"errors"
	ws "github.com/gorilla/websocket"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

type config struct {
	URL         string `json:"url"`
	Token       string `json:"bearer_token"`
	TimeoutMS   int    `json:"timeout_ms"`
	MaxFrame    int64  `json:"max_frame_bytes"`
	HeartbeatMS int    `json:"heartbeat_ms"`
}
type frame struct {
	kind int
	raw  []byte
	seq  uint64
	at   int64
}
type consumer struct {
	queue   []frame
	bytes   int
	credits int
	max     int
	notify  chan struct{}
	err     error
}
type resource struct {
	cfg       config
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	dialGate  chan struct{}
	writeGate chan struct{}
	conn      *ws.Conn
	closed    bool
	seq       uint64
	streams   map[string]*consumer
	wg        sync.WaitGroup
}
type connector struct {
	mu        sync.Mutex
	resources map[string]*resource
	closed    bool
}

func NewConnector() sdk.ConnectorPlugin { return &connector{resources: map[string]*resource{}} }
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-websocket", Name: "WebSocket Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Shared persistent WebSocket with bounded best-effort consumers and ping/pong", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "websocket", Label: "WebSocket", Capabilities: []string{"read", "write", "shared_duplex", "session:persistent"}, DisplayFields: []string{"url"}}}}
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{TimeoutMS: 5000, MaxFrame: 65536, HeartbeatMS: 15000}
	if json.Unmarshal(spec.Connection, &cfg) != nil {
		return "", errors.New("invalid WebSocket config")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "ws" && u.Scheme != "wss") || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxFrame < 1 || cfg.MaxFrame > 1048576 || cfg.HeartbeatMS < 1000 || cfg.HeartbeatMS > 60000 || spec.ID == "" {
		return "", errors.New("invalid WebSocket settings")
	}
	if cfg.Token != "" && u.Scheme != "wss" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" && u.Hostname() != "localhost" {
		return "", errors.New("WebSocket credentials require TLS")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &resource{cfg: cfg, ctx: ctx, cancel: cancel, streams: map[string]*consumer{}, dialGate: make(chan struct{}, 1), writeGate: make(chan struct{}, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("connector stopped or duplicate resource")
	}
	c.resources[spec.ID] = r
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
func signal(s *consumer) {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
func (r *resource) ensure(parent context.Context) (*ws.Conn, error) {
	select {
	case <-parent.Done():
		return nil, parent.Err()
	case r.dialGate <- struct{}{}:
	}
	defer func() { <-r.dialGate }()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("resource closed")
	}
	if r.conn != nil {
		conn := r.conn
		r.mu.Unlock()
		return conn, nil
	}
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	headers := http.Header{}
	if r.cfg.Token != "" {
		headers.Set("Authorization", "Bearer "+r.cfg.Token)
	}
	dialer := ws.Dialer{HandshakeTimeout: time.Duration(r.cfg.TimeoutMS) * time.Millisecond, EnableCompression: false}
	conn, response, err := dialer.DialContext(ctx, r.cfg.URL, headers)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return nil, errors.New("WebSocket connection failed")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		conn.Close()
		return nil, errors.New("resource closed")
	}
	r.conn = conn
	conn.SetReadLimit(r.cfg.MaxFrame)
	conn.SetReadDeadline(time.Now().Add(time.Duration(r.cfg.HeartbeatMS*3) * time.Millisecond))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(time.Duration(r.cfg.HeartbeatMS*3) * time.Millisecond))
	})
	r.wg.Add(2)
	sessionDone := make(chan struct{})
	go r.readLoop(conn, sessionDone)
	go r.heartbeat(conn, sessionDone)
	return conn, nil
}
func (r *resource) broken(conn *ws.Conn) {
	r.mu.Lock()
	if r.conn == conn {
		r.conn = nil
		for _, s := range r.streams {
			s.err = errors.New("WebSocket session ended; frames may be lost")
			signal(s)
		}
	}
	r.mu.Unlock()
	conn.Close()
}
func (r *resource) readLoop(conn *ws.Conn, sessionDone chan struct{}) {
	defer close(sessionDone)
	defer r.wg.Done()
	defer r.broken(conn)
	for {
		kind, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if kind == ws.TextMessage && !utf8.Valid(raw) {
			return
		}
		r.mu.Lock()
		if r.conn != conn || r.closed {
			r.mu.Unlock()
			return
		}
		r.seq++
		f := frame{kind: kind, raw: raw, seq: r.seq, at: time.Now().UnixNano()}
		total := 0
		for _, s := range r.streams {
			total += s.bytes
		}
		for _, s := range r.streams {
			if s.err != nil {
				continue
			}
			if len(s.queue) >= s.max || s.bytes+len(raw) > 1048576 || total+len(raw) > 8388608 {
				total -= s.bytes
				s.queue = nil
				s.bytes = 0
				s.err = errors.New("WebSocket consumer overflow; best-effort messages lost")
				signal(s)
				continue
			}
			s.queue = append(s.queue, f)
			s.bytes += len(raw)
			total += len(raw)
			signal(s)
		}
		r.mu.Unlock()
	}
}
func (r *resource) heartbeat(conn *ws.Conn, sessionDone <-chan struct{}) {
	defer r.wg.Done()
	timer := time.NewTicker(time.Duration(r.cfg.HeartbeatMS) * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-sessionDone:
			return
		case <-r.ctx.Done():
			return
		case <-timer.C:
			r.mu.Lock()
			current := r.conn == conn && !r.closed
			r.mu.Unlock()
			if !current {
				return
			}
			if conn.WriteControl(ws.PingMessage, nil, time.Now().Add(time.Second)) != nil {
				r.broken(conn)
				return
			}
		}
	}
}
func (r *resource) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	conn := r.conn
	r.conn = nil
	for _, s := range r.streams {
		s.err = errors.New("resource closed")
		signal(s)
	}
	r.streams = map[string]*consumer{}
	r.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	r.dialGate <- struct{}{}
	<-r.dialGate
	r.wg.Wait()
}
func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	r := c.resources[h]
	delete(c.resources, h)
	c.mu.Unlock()
	if r != nil {
		r.close()
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
		r.close()
	}
	return nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if req.Kind != sdk.ConnectorRequestWrite || req.Target != "send" || len(req.Payload) > int(r.cfg.MaxFrame)*2+128 {
		return sdk.ConnectorResponse{}, errors.New("bounded send write required")
	}
	var spec struct {
		Type string `json:"type"`
		Data []byte `json:"data"`
	}
	if json.Unmarshal(req.Payload, &spec) != nil || len(spec.Data) > int(r.cfg.MaxFrame) {
		return sdk.ConnectorResponse{}, errors.New("invalid frame")
	}
	kind := ws.BinaryMessage
	if spec.Type == "text" {
		kind = ws.TextMessage
		if !utf8.Valid(spec.Data) {
			return sdk.ConnectorResponse{}, errors.New("text must be UTF-8")
		}
	} else if spec.Type != "binary" {
		return sdk.ConnectorResponse{}, errors.New("type must be text or binary")
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
	case r.writeGate <- struct{}{}:
	}
	defer func() { <-r.writeGate }()
	conn, err := r.ensure(ctx)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if ctx.Err() != nil {
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	deadline, _ := ctx.Deadline()
	conn.SetWriteDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if err = conn.WriteMessage(kind, spec.Data); err != nil {
		r.broken(conn)
		return sdk.ConnectorResponse{}, errors.New("WebSocket write failed; outcome unknown, not retried")
	}
	return sdk.ConnectorResponse{Payload: json.RawMessage(`{"sent":true}`)}, nil
}
func (c *connector) OpenStream(h string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	if req.Target != "messages" || req.SessionKey == "" || len(req.SessionKey) > 512 || req.InitialCredits < 1 || req.InitialCredits > 64 || req.MaxBufferedMessages < 1 || req.MaxBufferedMessages > 64 || req.OverflowPolicy != "fail_session" {
		return sdk.StreamOpenResponse{}, errors.New("stream requires 1..64 credits/buffer and fail_session policy")
	}
	r.mu.Lock()
	if r.closed || len(r.streams) >= 64 || r.streams[req.SessionKey] != nil {
		r.mu.Unlock()
		return sdk.StreamOpenResponse{}, errors.New("resource closed or consumer limit/duplicate")
	}
	s := &consumer{credits: req.InitialCredits, max: req.MaxBufferedMessages, notify: make(chan struct{}, 1)}
	r.streams[req.SessionKey] = s
	r.mu.Unlock()
	if _, err = r.ensure(r.ctx); err != nil {
		c.CloseStream(h, sdk.StreamCloseRequest{StreamID: req.SessionKey})
		return sdk.StreamOpenResponse{}, err
	}
	return sdk.StreamOpenResponse{StreamID: req.SessionKey, DeliverySemantics: sdk.StreamDeliveryBestEffort, InitialCredits: req.InitialCredits}, nil
}
func (c *connector) ReceiveStream(h string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	if req.WaitTimeoutMS < 1 || req.WaitTimeoutMS > 30000 || req.MaxMessages < 1 || req.MaxMessages > 64 {
		return sdk.StreamReceiveResponse{}, errors.New("invalid receive limits")
	}
	timer := time.NewTimer(time.Duration(req.WaitTimeoutMS) * time.Millisecond)
	defer timer.Stop()
	for {
		r.mu.Lock()
		s := r.streams[req.StreamID]
		if s == nil {
			r.mu.Unlock()
			return sdk.StreamReceiveResponse{}, errors.New("unknown stream")
		}
		out := []sdk.StreamMessage{}
		for len(s.queue) > 0 && s.credits > 0 && len(out) < req.MaxMessages {
			f := s.queue[0]
			s.queue[0] = frame{}
			s.queue = s.queue[1:]
			s.bytes -= len(f.raw)
			s.credits--
			kind := "binary"
			if f.kind == ws.TextMessage {
				kind = "text"
			}
			out = append(out, sdk.StreamMessage{StreamID: req.StreamID, DeliveryID: strconv.FormatUint(f.seq, 10), Sequence: f.seq, RawPayload: append([]byte(nil), f.raw...), Metadata: map[string]string{"frame_type": kind}, PublishedAtUnixNano: f.at})
		}
		if len(out) > 0 {
			r.mu.Unlock()
			return sdk.StreamReceiveResponse{Messages: out}, nil
		}
		if s.err != nil {
			err = s.err
			r.mu.Unlock()
			return sdk.StreamReceiveResponse{}, err
		}
		notify := s.notify
		r.mu.Unlock()
		select {
		case <-r.ctx.Done():
			return sdk.StreamReceiveResponse{}, errors.New("resource closed")
		case <-timer.C:
			return sdk.StreamReceiveResponse{}, nil
		case <-notify:
		}
	}
}
func (c *connector) GrantCredits(h string, req sdk.StreamGrantCreditsRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.streams[req.StreamID]
	if s == nil || req.Credits < 0 || req.Credits > 64 || s.credits+req.Credits > 64 {
		return errors.New("invalid stream or credit grant")
	}
	s.credits += req.Credits
	signal(s)
	return nil
}
func (c *connector) AckStream(h string, req sdk.StreamAckRequest) error {
	return c.GrantCredits(h, sdk.StreamGrantCreditsRequest{StreamID: req.StreamID, Credits: req.Credits})
}
func (c *connector) NackStream(h string, req sdk.StreamNackRequest) error {
	if req.Requeue {
		return errors.New("WebSocket best-effort stream cannot requeue")
	}
	return c.GrantCredits(h, sdk.StreamGrantCreditsRequest{StreamID: req.StreamID, Credits: req.Credits})
}
func (c *connector) CloseStream(h string, req sdk.StreamCloseRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if s := r.streams[req.StreamID]; s != nil {
		s.err = errors.New("stream closed")
		signal(s)
		delete(r.streams, req.StreamID)
	}
	var conn *ws.Conn
	if len(r.streams) == 0 {
		conn = r.conn
		r.conn = nil
	}
	r.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	return nil
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	r.mu.Lock()
	connected := r.conn != nil
	r.mu.Unlock()
	status := sdk.ResourceStatusDisconnected
	reason := "NO_SESSION"
	if connected {
		status = sdk.ResourceStatusHealthy
		reason = "CONNECTED"
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: status, ReasonCode: reason, ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
