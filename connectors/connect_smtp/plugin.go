package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type config struct {
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Security  string   `json:"security"`
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	From      string   `json:"from"`
	Domains   []string `json:"recipient_domains"`
	TimeoutMS int      `json:"timeout_ms"`
}
type message struct {
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Body    string   `json:"body"`
	ID      string   `json:"message_id"`
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
	slots     chan struct{}
	closed    bool
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, slots: make(chan struct{}, 8)}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-smtp", Name: "SMTP Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Finite SMTP submission with verified TLS, sender identity and recipient domain policy", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "smtp", Label: "SMTP", Capabilities: []string{"write", "session:per_operation"}, DisplayFields: []string{"host", "port", "from", "security"}}}}
}
func address(text string) (*mail.Address, error) {
	if len(text) > 512 || strings.ContainsAny(text, "\r\n\x00") {
		return nil, errors.New("invalid mail address")
	}
	parsed, err := mail.ParseAddress(text)
	if err != nil {
		return nil, errors.New("invalid mailbox")
	}
	return parsed, nil
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{Port: 587, Security: "starttls", TimeoutMS: 5000}
	if json.Unmarshal(spec.Connection, &cfg) != nil || spec.ID == "" || cfg.Host == "" || cfg.Port < 1 || cfg.Port > 65535 || cfg.TimeoutMS < 100 || cfg.TimeoutMS > 30000 || len(cfg.Domains) < 1 || len(cfg.Domains) > 32 || (cfg.Username == "") != (cfg.Password == "") {
		return "", errors.New("invalid SMTP resource")
	}
	if _, err := address(cfg.From); err != nil {
		return "", err
	}
	switch cfg.Security {
	case "starttls", "tls":
	case "none":
		ip := net.ParseIP(cfg.Host)
		if cfg.Username != "" || (ip == nil || !ip.IsLoopback()) && cfg.Host != "localhost" {
			return "", errors.New("unencrypted SMTP only allowed anonymously on loopback")
		}
	default:
		return "", errors.New("security must be starttls or tls; none is a loopback fixture exception")
	}
	for i, domain := range cfg.Domains {
		domain = strings.ToLower(domain)
		if !regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`).MatchString(domain) || strings.Contains(domain, "..") {
			return "", errors.New("invalid recipient domain policy")
		}
		cfg.Domains[i] = domain
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

var messageID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}@[A-Za-z0-9.-]{1,100}$`)

func compose(cfg config, m message) ([]byte, string, []string, error) {
	if len(m.To) < 1 || len(m.To) > 10 || len(m.Subject) > 200 || strings.ContainsAny(m.Subject, "\r\n\x00") || !utf8.ValidString(m.Subject) || len(m.Body) > 65536 || !utf8.ValidString(m.Body) || !messageID.MatchString(m.ID) {
		return nil, "", nil, errors.New("invalid/bounded email fields required")
	}
	from, err := address(cfg.From)
	if err != nil {
		return nil, "", nil, err
	}
	to := []string{}
	headers := []string{}
	seen := map[string]bool{}
	for _, text := range m.To {
		parsed, err := address(text)
		if err != nil {
			return nil, "", nil, err
		}
		domain := strings.ToLower(parsed.Address[strings.LastIndex(parsed.Address, "@")+1:])
		allowed := false
		for _, permitted := range cfg.Domains {
			allowed = allowed || domain == permitted
		}
		if !allowed {
			return nil, "", nil, errors.New("recipient domain is not allowed")
		}
		if seen[parsed.Address] {
			return nil, "", nil, errors.New("duplicate recipient")
		}
		seen[parsed.Address] = true
		to = append(to, parsed.Address)
		headers = append(headers, parsed.String())
	}
	body := base64.StdEncoding.EncodeToString([]byte(m.Body))
	var out bytes.Buffer
	out.WriteString("From: " + from.String() + "\r\nTo: " + strings.Join(headers, ", ") + "\r\nSubject: " + mime.QEncoding.Encode("utf-8", m.Subject) + "\r\nMessage-ID: <" + m.ID + ">\r\nDate: " + time.Now().UTC().Format(time.RFC1123Z) + "\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: base64\r\n\r\n")
	for len(body) > 0 {
		n := min(76, len(body))
		out.WriteString(body[:n])
		out.WriteString("\r\n")
		body = body[n:]
	}
	return out.Bytes(), from.Address, to, nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.Lock()
	r := c.resources[h]
	c.mu.Unlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	if req.Kind != sdk.ConnectorRequestWrite || req.Target != "submit" || len(req.Payload) > 131072 {
		return sdk.ConnectorResponse{}, errors.New("bounded SMTP submit required")
	}
	var m message
	if json.Unmarshal(req.Payload, &m) != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid email")
	}
	raw, from, to, err := compose(r.cfg, m)
	if err != nil {
		return sdk.ConnectorResponse{}, err
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
		return sdk.ConnectorResponse{}, errors.New("SMTP dial failed")
	}
	defer conn.Close()
	underlying := conn
	stop := context.AfterFunc(ctx, func() { underlying.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	conn.SetDeadline(deadline)
	tlsConfig := &tls.Config{ServerName: r.cfg.Host, MinVersion: tls.VersionTLS12}
	if r.cfg.Security == "tls" {
		secured := tls.Client(conn, tlsConfig)
		if secured.HandshakeContext(ctx) != nil {
			return sdk.ConnectorResponse{}, errors.New("SMTP TLS failed")
		}
		conn = secured
	}
	client, err := smtp.NewClient(conn, r.cfg.Host)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("SMTP greeting failed")
	}
	defer client.Close()
	if r.cfg.Security == "starttls" {
		if supported, _ := client.Extension("STARTTLS"); !supported {
			return sdk.ConnectorResponse{}, errors.New("SMTP server does not offer required STARTTLS")
		}
		if client.StartTLS(tlsConfig) != nil {
			return sdk.ConnectorResponse{}, errors.New("SMTP STARTTLS verification failed")
		}
	}
	if r.cfg.Username != "" {
		if client.Auth(smtp.PlainAuth("", r.cfg.Username, r.cfg.Password, r.cfg.Host)) != nil {
			return sdk.ConnectorResponse{}, errors.New("SMTP authentication failed")
		}
	}
	if client.Mail(from) != nil {
		return sdk.ConnectorResponse{}, errors.New("SMTP sender rejected")
	}
	for _, recipient := range to {
		if client.Rcpt(recipient) != nil {
			return sdk.ConnectorResponse{}, errors.New("SMTP recipient rejected; no DATA submitted")
		}
	}
	writer, err := client.Data()
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("SMTP DATA rejected")
	}
	if _, err = io.Copy(writer, bytes.NewReader(raw)); err != nil {
		return sdk.ConnectorResponse{}, errors.New("SMTP DATA interrupted; outcome unknown")
	}
	if writer.Close() != nil {
		return sdk.ConnectorResponse{}, errors.New("SMTP DATA completion failed; outcome may be unknown")
	}
	return sdk.ConnectorResponse{Payload: json.RawMessage(`{"accepted":true,"level":"smtp_receipt"}`)}, nil
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
	return sdk.StreamOpenResponse{}, errors.New("submission only")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("submission only")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error   { return errors.New("no stream") }
func (c *connector) NackStream(string, sdk.StreamNackRequest) error { return errors.New("no stream") }
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("no stream")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }
