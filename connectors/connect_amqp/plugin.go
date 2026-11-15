package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	Address         string   `json:"address"`
	Username        string   `json:"username"`
	Password        string   `json:"password"`
	VHost           string   `json:"vhost"`
	TLS             bool     `json:"tls"`
	ServerName      string   `json:"server_name"`
	TimeoutMS       int      `json:"timeout_ms"`
	MaxMessageBytes int      `json:"max_message_bytes"`
	AllowWrite      bool     `json:"allow_write"`
	Exchanges       []string `json:"allowed_exchanges"`
}
type resource struct {
	cfg         config
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	conn        *amqp.Connection
	raw         net.Conn
	dialGate    chan struct{}
	publishGate chan struct{}
	wg          sync.WaitGroup
	streams     map[string]*consumer
	reserved    map[string]int
	budget      int
}
type consumer struct {
	mu           sync.Mutex
	ch           *amqp.Channel
	raw          net.Conn
	messages     <-chan amqp.Delivery
	ctx          context.Context
	cancel       context.CancelFunc
	pending      map[string]amqp.Delivery
	credits, max int
	recvGate     chan struct{}
	notify       chan struct{}
}
type connector struct {
	mu        sync.RWMutex
	resources map[string]*resource
	stopped   bool
}

func NewConnector() sdk.ConnectorPlugin { return &connector{resources: map[string]*resource{}} }

const configSchema = `{"type":"object","required":["address","username","password"],"properties":{"address":{"type":"string","minLength":1},"username":{"type":"string","minLength":1},"password":{"type":"string","format":"password","minLength":1},"vhost":{"type":"string","default":"/"},"tls":{"type":"boolean","default":true},"server_name":{"type":"string"},"timeout_ms":{"type":"integer","minimum":100,"maximum":30000,"default":5000},"max_message_bytes":{"type":"integer","minimum":1,"maximum":1048576,"default":65536},"allow_write":{"type":"boolean","default":false},"allowed_exchanges":{"type":"array","maxItems":32,"uniqueItems":true,"items":{"type":"string","maxLength":255}}},"additionalProperties":false}`

func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-amqp", Name: "AMQP 0-9-1 Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Shared RabbitMQ connection with bounded manual-ack consumers and confirmed publishing", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "amqp", Label: "AMQP 0-9-1", Capabilities: []string{"read", "write", "shared_duplex", "session:persistent"}, DisplayFields: []string{"address", "vhost"}}}}
}
func decode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func (c *connector) CreateResource(input sdk.ConnectorResource) (string, error) {
	cfg := config{VHost: "/", TLS: true, TimeoutMS: 5000, MaxMessageBytes: 65536}
	if len(input.Connection) > 65536 || decode(input.Connection, &cfg) != nil {
		return "", errors.New("invalid AMQP configuration")
	}
	host, port, err := net.SplitHostPort(cfg.Address)
	n, _ := strconv.Atoi(port)
	if err != nil || host == "" || n < 1 || n > 65535 || input.ID == "" || cfg.Username == "" || len(cfg.Username) > 255 || strings.ContainsRune(cfg.Username, 0) || strings.ContainsRune(cfg.Password, 0) || cfg.Password == "" || len(cfg.Password) > 4096 || len(cfg.VHost) > 255 || cfg.VHost == "" || cfg.TimeoutMS < 100 || cfg.TimeoutMS > 30000 || cfg.MaxMessageBytes < 1 || cfg.MaxMessageBytes > 1048576 || len(cfg.Exchanges) > 32 {
		return "", errors.New("invalid AMQP settings")
	}
	ip := net.ParseIP(host)
	if !cfg.TLS && host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", errors.New("AMQP credentials require TLS except loopback test endpoints")
	}
	seen := map[string]bool{}
	for _, exchange := range cfg.Exchanges {
		if len(exchange) > 255 || strings.ContainsAny(exchange, "\x00\r\n") || seen[exchange] {
			return "", errors.New("invalid exchange allowlist")
		}
		seen[exchange] = true
	}
	if cfg.AllowWrite && len(cfg.Exchanges) == 0 {
		return "", errors.New("publishing requires an explicit exchange allowlist; empty string names the default exchange")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &resource{cfg: cfg, ctx: ctx, cancel: cancel, dialGate: make(chan struct{}, 1), publishGate: make(chan struct{}, 1), streams: map[string]*consumer{}, reserved: map[string]int{}}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped || c.resources[input.ID] != nil || len(c.resources) >= 32 {
		cancel()
		return "", errors.New("connector stopped or resource exists")
	}
	c.resources[input.ID] = r
	return input.ID, nil
}
func (c *connector) get(h string) (*resource, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return nil, errors.New("unknown resource")
	}
	return r, nil
}
func (r *resource) begin(timeout int) (context.Context, context.CancelFunc, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, errors.New("resource stopped")
	}
	r.wg.Add(1)
	if timeout <= 0 || timeout > r.cfg.TimeoutMS {
		timeout = r.cfg.TimeoutMS
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(timeout)*time.Millisecond)
	return ctx, func() { cancel(); r.wg.Done() }, nil
}
func (r *resource) ensure(ctx context.Context) (*amqp.Connection, net.Conn, error) {
	select {
	case r.dialGate <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	defer func() { <-r.dialGate }()
	r.mu.Lock()
	if r.conn != nil && !r.conn.IsClosed() {
		conn, raw := r.conn, r.raw
		r.mu.Unlock()
		return conn, raw, nil
	}
	r.mu.Unlock()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", r.cfg.Address)
	if err != nil {
		return nil, nil, errors.New("AMQP dial failed")
	}
	physical := raw
	stop := context.AfterFunc(ctx, func() { physical.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	raw.SetDeadline(deadline)
	if r.cfg.TLS {
		host, _, _ := net.SplitHostPort(r.cfg.Address)
		name := r.cfg.ServerName
		if name == "" {
			name = host
		}
		secured := tls.Client(raw, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: name})
		if err = secured.HandshakeContext(ctx); err != nil {
			raw.Close()
			return nil, nil, errors.New("AMQP TLS validation failed")
		}
		raw = secured
	}
	bounded := &frameConn{Conn: raw, maxBody: uint64(r.cfg.MaxMessageBytes)}
	conn, err := amqp.Open(bounded, amqp.Config{SASL: []amqp.Authentication{&amqp.PlainAuth{Username: r.cfg.Username, Password: r.cfg.Password}}, Vhost: r.cfg.VHost, ChannelMax: 64, FrameSize: 131072, Heartbeat: 10 * time.Second, Recovery: nil})
	if err != nil {
		raw.Close()
		return nil, nil, errors.New("AMQP handshake failed")
	}
	raw.SetDeadline(time.Time{})
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || ctx.Err() != nil {
		raw.Close()
		return nil, nil, errors.New("AMQP handshake canceled")
	}
	r.conn = conn
	r.raw = raw
	return conn, raw, nil
}
func (r *resource) close() {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	raw := r.raw
	for _, s := range r.streams {
		s.cancel()
	}
	r.mu.Unlock()
	if raw != nil {
		raw.Close()
	}
	r.wg.Wait()
	r.mu.Lock()
	r.streams = map[string]*consumer{}
	r.reserved = map[string]int{}
	r.budget = 0
	r.mu.Unlock()
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
	c.stopped = true
	all := c.resources
	c.resources = map[string]*resource{}
	c.mu.Unlock()
	for _, r := range all {
		r.close()
	}
	return nil
}

// Channel.Close is a protocol roundtrip. Bound it by closing the shared physical
// connection on timeout; affected consumers then requeue at the broker.
func closeChannel(ch *amqp.Channel, raw net.Conn) {
	if ch == nil {
		return
	}
	timer := time.AfterFunc(time.Second, func() { raw.Close() })
	defer timer.Stop()
	_ = ch.Close()
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	r.mu.Lock()
	live := r.conn != nil && !r.conn.IsClosed()
	r.mu.Unlock()
	reason := "CONFIGURED_LAZY"
	if live {
		reason = "SESSION_ACTIVE"
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusHealthy, ReasonCode: reason, Message: "configured; real demand establishes and verifies broker connection", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}

type publishSpec struct {
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
	Body       []byte `json:"body"`
	MessageID  string `json:"message_id"`
}

func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if req.Kind != sdk.ConnectorRequestWrite || req.Target != "publish" || !r.cfg.AllowWrite || len(req.Payload) > 2*r.cfg.MaxMessageBytes+2048 {
		return sdk.ConnectorResponse{}, errors.New("bounded opt-in publish required")
	}
	var spec publishSpec
	if decode(req.Payload, &spec) != nil || len(spec.Body) > r.cfg.MaxMessageBytes || len(spec.RoutingKey) > 255 || len(spec.MessageID) > 255 || spec.MessageID == "" {
		return sdk.ConnectorResponse{}, errors.New("invalid AMQP publish")
	}
	allowed := false
	for _, v := range r.cfg.Exchanges {
		if v == spec.Exchange {
			allowed = true
		}
	}
	if !allowed {
		return sdk.ConnectorResponse{}, errors.New("exchange is not allowed")
	}
	ctx, done, err := r.begin(req.TimeoutMS)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	defer done()
	select {
	case r.publishGate <- struct{}{}:
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	defer func() { <-r.publishGate }()
	conn, raw, err := r.ensure(ctx)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("AMQP publisher channel failed")
	}
	defer closeChannel(ch, raw)
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	if err = ch.Confirm(false); err != nil {
		return sdk.ConnectorResponse{}, errors.New("AMQP confirm setup failed")
	}
	err = ch.PublishWithContext(ctx, spec.Exchange, spec.RoutingKey, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: spec.MessageID, Timestamp: time.Now().UTC(), Body: spec.Body})
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("AMQP publish failed; outcome unknown, not retried")
	}
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, errors.New("AMQP publisher confirmation deadline; outcome unknown")
	case <-returns:
		return sdk.ConnectorResponse{}, errors.New("AMQP publish unroutable")
	case confirmation, ok := <-confirms:
		if !ok || !confirmation.Ack {
			return sdk.ConnectorResponse{}, errors.New("AMQP publisher confirmation failed")
		}
		// The client dispatches basic.return before its following basic.ack.
		select {
		case <-returns:
			return sdk.ConnectorResponse{}, errors.New("AMQP publish unroutable")
		default:
		}
		return sdk.ConnectorResponse{Payload: json.RawMessage(`{"broker_confirmed":true}`)}, nil
	}
}
