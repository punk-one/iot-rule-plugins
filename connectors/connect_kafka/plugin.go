package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type config struct {
	Brokers     []string `json:"brokers"`
	Topics      []string `json:"topics"`
	GroupPrefix string   `json:"group_prefix"`
	TLS         bool     `json:"tls"`
	CAPEM       string   `json:"ca_pem"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	SASL        string   `json:"sasl"`
	TimeoutMS   int      `json:"timeout_ms"`
	MaxBytes    int      `json:"max_message_bytes"`
	AllowWrite  bool     `json:"allow_write"`
}
type resource struct {
	cfg          config
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	streams      map[string]*consumer
	topics       map[string]bool
	closed       bool
	producer     *kgo.Client
	producerGate chan struct{}
}
type consumer struct {
	ctx          context.Context
	cancel       context.CancelFunc
	nativeCancel context.CancelFunc
	key          string
	gate         chan struct{}
	client       *kgo.Client
	topic        string
	credits      int
	pending      *kgo.Record
	delivery     string
	sequence     uint64
	timer        *time.Timer
	closed       bool
}
type connector struct {
	mu        sync.Mutex
	resources map[string]*resource
	closed    bool
}

var topicName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}$`)
var groupName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func NewConnector() sdk.ConnectorPlugin { return &connector{resources: map[string]*resource{}} }
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-kafka", Name: "Kafka Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Shared resource-owned producer and bounded explicit consumer groups with handoff offset commits", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "kafka", Label: "Kafka", Capabilities: []string{"read", "write", "stream:manual_ack", "session:persistent"}, DisplayFields: []string{"brokers", "topics", "group_prefix"}}}}
}
func decode(raw []byte, v any) error {
	if len(raw) > 2*1048576 {
		return errors.New("Kafka request exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid Kafka request")
	}
	return nil
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{TLS: true, GroupPrefix: "iot-rule.", TimeoutMS: 5000, MaxBytes: 65536}
	if len(spec.Connection) > 131072 || decode(spec.Connection, &cfg) != nil || spec.ID == "" || len(cfg.Brokers) < 1 || len(cfg.Brokers) > 8 || len(cfg.Topics) < 1 || len(cfg.Topics) > 8 || !groupName.MatchString(cfg.GroupPrefix) || len(cfg.GroupPrefix) > 64 || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxBytes < 1 || cfg.MaxBytes > 1048576 || len(cfg.CAPEM) > 65536 || len(cfg.Username) > 128 || len(cfg.Password) > 4096 || (cfg.Username == "") != (cfg.Password == "") {
		return "", errors.New("invalid Kafka settings")
	}
	for _, address := range cfg.Brokers {
		host, port, e := net.SplitHostPort(address)
		number, pe := strconv.Atoi(port)
		if e != nil || pe != nil || number < 1 || number > 65535 || host == "" || len(host) > 253 || strings.ContainsAny(host, "/@?#\\\x00\r\n") {
			return "", errors.New("Kafka brokers require host:port")
		}
		if !cfg.TLS {
			ip := net.ParseIP(host)
			if ip == nil || !ip.IsLoopback() {
				return "", errors.New("Kafka TLS required outside literal loopback")
			}
		}
	}
	if cfg.Username != "" && (cfg.SASL != "scram-sha-256" && cfg.SASL != "scram-sha-512" && cfg.SASL != "plain") {
		return "", errors.New("choose an explicit Kafka SASL mechanism")
	}
	if cfg.Username == "" && cfg.SASL != "" {
		return "", errors.New("Kafka SASL requires credentials")
	}
	if cfg.Username != "" && !cfg.TLS {
		return "", errors.New("Kafka SASL credentials require TLS")
	}
	if !cfg.TLS && cfg.CAPEM != "" {
		return "", errors.New("Kafka CA requires TLS")
	}
	topics := map[string]bool{}
	for _, topic := range cfg.Topics {
		if !topicName.MatchString(topic) || topics[topic] {
			return "", errors.New("invalid or duplicate Kafka topic")
		}
		topics[topic] = true
	}
	// Validate TLS configuration offline, before accepting a resource.
	if _, err := tlsConfig(cfg); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &resource{cfg: cfg, ctx: ctx, cancel: cancel, streams: map[string]*consumer{}, topics: topics, producerGate: make(chan struct{}, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.resources) >= 8 || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("connector closed, duplicate or resource limit")
	}
	c.resources[spec.ID] = r
	return spec.ID, nil
}
func tlsConfig(cfg config) (*tls.Config, error) {
	if !cfg.TLS {
		return nil, nil
	}
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAPEM != "" {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM([]byte(cfg.CAPEM)) {
			return nil, errors.New("invalid Kafka CA PEM")
		}
		out.RootCAs = roots
	}
	return out, nil
}
func (r *resource) options(ctx context.Context) []kgo.Opt {
	opts := []kgo.Opt{kgo.WithContext(ctx), kgo.SeedBrokers(r.cfg.Brokers...), kgo.ClientID("iot-rule-kafka-v1"), kgo.DialTimeout(time.Duration(r.cfg.TimeoutMS) * time.Millisecond), kgo.RequestTimeoutOverhead(time.Second), kgo.RequestRetries(2), kgo.RetryBackoffFn(func(n int) time.Duration { return time.Duration(1<<min(max(n, 0), 5)) * time.Second }), kgo.BrokerMaxReadBytes(2 * 1048576), kgo.BrokerMaxWriteBytes(2 * 1048576), kgo.MaxDecompressBatchBytes(1048576), kgo.FetchMaxBytes(1048576), kgo.FetchMaxPartitionBytes(int32(r.cfg.MaxBytes + 1024)), kgo.MaxConcurrentFetches(1), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.ProducerBatchMaxBytes(int32(r.cfg.MaxBytes + 1024)), kgo.MaxBufferedBytes(r.cfg.MaxBytes + 1024), kgo.MaxBufferedRecords(8), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(), kgo.RecordRetries(0)}
	if tlsCfg, _ := tlsConfig(r.cfg); tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	} else {
		opts = append(opts, kgo.Dialer(func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			ip := net.ParseIP(host)
			if err != nil || ip == nil || !ip.IsLoopback() {
				return nil, errors.New("plaintext Kafka advertised broker must be loopback")
			}
			return (&net.Dialer{Timeout: time.Duration(r.cfg.TimeoutMS) * time.Millisecond}).DialContext(ctx, network, address)
		}))
	}
	if r.cfg.Username != "" {
		switch r.cfg.SASL {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{User: r.cfg.Username, Pass: r.cfg.Password}.AsMechanism()))
		case "scram-sha-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: r.cfg.Username, Pass: r.cfg.Password}.AsSha256Mechanism()))
		case "scram-sha-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: r.cfg.Username, Pass: r.cfg.Password}.AsSha512Mechanism()))
		}
	}
	return opts
}
func (c *connector) get(h string) (*resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.resources[h]
	if r == nil {
		return nil, errors.New("unknown Kafka resource")
	}
	return r, nil
}
func (c *connector) stream(h, id string) (*resource, *consumer, error) {
	r, err := c.get(h)
	if err != nil {
		return nil, nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.streams[id]
	if s == nil || r.closed {
		return nil, nil, errors.New("Kafka consumer stream closed")
	}
	return r, s, nil
}
func closeClient(client *kgo.Client, ctx context.Context) {
	if client == nil {
		return
	}
	client.AllowRebalance()
	_ = client.LeaveGroupContext(ctx)
	client.CloseAllowingRebalance()
}
func (s *consumer) close() {
	s.cancel()
	s.gate <- struct{}{}
	defer func() { <-s.gate }()
	if s.closed {
		return
	}
	s.closed = true
	s.client.AllowRebalance()
	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_ = s.client.LeaveGroupContext(leaveCtx)
	leaveCancel()
	s.nativeCancel()
	if s.timer != nil {
		s.timer.Stop()
	}
	closeClient(s.client, s.ctx)
	s.pending = nil
}
func (r *resource) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	streams := r.streams
	r.streams = map[string]*consumer{}
	r.mu.Unlock()
	for _, s := range streams {
		s.close()
	}
	r.producerGate <- struct{}{}
	defer func() { <-r.producerGate }()
	if r.producer != nil {
		closeClient(r.producer, r.ctx)
		r.producer = nil
	}
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
	var request struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}
	if req.Kind != sdk.ConnectorRequestWrite || req.Target != "publish" || !r.cfg.AllowWrite || decode(req.Payload, &request) != nil || !r.topics[request.Topic] || request.Key == "" || len(request.Key) > 1024 || len(request.Value) > r.cfg.MaxBytes {
		return sdk.ConnectorResponse{}, errors.New("Kafka publish disabled or invalid bounded topic/key/value")
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
	case r.producerGate <- struct{}{}:
	}
	defer func() { <-r.producerGate }()
	if ctx.Err() != nil {
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	if r.producer == nil {
		r.producer, err = kgo.NewClient(r.options(r.ctx)...)
		if err != nil {
			return sdk.ConnectorResponse{}, errors.New("Kafka producer configuration failed")
		}
	}
	record := &kgo.Record{Topic: request.Topic, Key: []byte(request.Key), Value: append([]byte(nil), request.Value...)}
	if err = r.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return sdk.ConnectorResponse{}, errors.New("Kafka all-ISR publish failed; outcome unknown, not transparently retried")
	}
	raw, _ := json.Marshal(map[string]any{"received": true, "partition": record.Partition, "offset": strconv.FormatInt(record.Offset, 10)})
	return sdk.ConnectorResponse{Payload: raw}, nil
}
func (c *connector) OpenStream(h string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	var spec struct {
		Group string `json:"group_id"`
		Reset string `json:"reset"`
	}
	spec.Reset = "earliest"
	if decode(req.Payload, &spec) != nil || !r.topics[req.Target] || !groupName.MatchString(spec.Group) || !strings.HasPrefix(spec.Group, r.cfg.GroupPrefix) || len(spec.Group) <= len(r.cfg.GroupPrefix) || (spec.Reset != "earliest" && spec.Reset != "latest") || req.SessionKey == "" || len(req.SessionKey) > 512 || req.InitialCredits != 1 || req.MaxBufferedMessages != 1 || req.MaxDeliverBatch != 1 || req.OverflowPolicy != "fail_session" {
		return sdk.StreamOpenResponse{}, errors.New("Kafka stream requires an allowed topic/group prefix and one manual-ack in-flight record")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || len(r.streams) >= 8 {
		return sdk.StreamOpenResponse{}, errors.New("Kafka resource closed, duplicate or consumer limit")
	}
	for _, existing := range r.streams {
		if existing.key == req.SessionKey {
			return sdk.StreamOpenResponse{}, errors.New("duplicate Kafka node session")
		}
	}
	ctx, cancel := context.WithCancel(r.ctx)
	nativeCtx, nativeCancel := context.WithCancel(r.ctx)
	s := &consumer{ctx: ctx, cancel: cancel, nativeCancel: nativeCancel, key: req.SessionKey, gate: make(chan struct{}, 1), credits: 1, topic: req.Target}
	offset := kgo.NewOffset().AtStart()
	if spec.Reset == "latest" {
		offset = kgo.NewOffset().AtEnd()
	}
	opts := append(r.options(nativeCtx), kgo.ConsumerGroup(spec.Group), kgo.ConsumeTopics(req.Target), kgo.ConsumeResetOffset(offset), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(2*time.Second), kgo.RebalanceTimeout(30*time.Second))
	s.client, err = kgo.NewClient(opts...)
	if err != nil {
		cancel()
		nativeCancel()
		return sdk.StreamOpenResponse{}, errors.New("Kafka consumer configuration failed")
	}
	streamID := "kafka_" + sdk.NewEvent(nil, nil).ID
	r.streams[streamID] = s
	return sdk.StreamOpenResponse{StreamID: streamID, InitialCredits: 1, DeliverySemantics: sdk.StreamDeliveryAtLeastOnce}, nil
}
func (c *connector) ReceiveStream(h string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	r, s, err := c.stream(h, req.StreamID)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	if req.MaxMessages != 1 || req.WaitTimeoutMS < 1 || req.WaitTimeoutMS > 1000 {
		return sdk.StreamReceiveResponse{}, errors.New("Kafka receive requires one record and wait<=1000ms")
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Duration(req.WaitTimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		return sdk.StreamReceiveResponse{}, ctx.Err()
	case s.gate <- struct{}{}:
	}
	defer func() { <-s.gate }()
	if s.closed || s.ctx.Err() != nil {
		return sdk.StreamReceiveResponse{}, errors.New("Kafka stream closed")
	}
	if s.pending != nil || s.credits == 0 {
		return sdk.StreamReceiveResponse{}, nil
	}
	fetches := s.client.PollRecords(ctx, 1)
	for _, e := range fetches.Errors() {
		if errors.Is(e.Err, context.DeadlineExceeded) && s.ctx.Err() == nil {
			return sdk.StreamReceiveResponse{}, nil
		}
		if errors.Is(e.Err, kgo.ErrMaxDecompress) {
			return sdk.StreamReceiveResponse{}, errors.New("KAFKA_DECOMPRESS_LIMIT: batch expansion exceeds 1 MiB; offset not committed")
		}
		return sdk.StreamReceiveResponse{}, errors.New("Kafka fetch failed; reopen stream without committing")
	}
	records := fetches.Records()
	if len(records) == 0 {
		return sdk.StreamReceiveResponse{}, nil
	}
	record := records[0]
	if record.Offset < 0 || record.Partition < 0 || record.Topic != s.topic || len(record.Value) > r.cfg.MaxBytes || len(record.Key) > 1024 {
		return sdk.StreamReceiveResponse{}, errors.New("Kafka record exceeds limits or topic boundary")
	}
	s.pending = record
	s.credits = 0
	s.sequence++
	s.delivery = record.Topic + ":" + strconv.Itoa(int(record.Partition)) + ":" + strconv.FormatInt(record.Offset, 10)
	s.client.PauseFetchTopics(s.topic)
	s.timer = time.AfterFunc(30*time.Second, func() {
		s.gate <- struct{}{}
		expired := s.pending == record && !s.closed
		if expired {
			s.cancel()
		}
		<-s.gate
		if expired {
			_ = c.CloseStream(h, sdk.StreamCloseRequest{StreamID: req.StreamID})
		}
	})
	return sdk.StreamReceiveResponse{Messages: []sdk.StreamMessage{{StreamID: req.StreamID, DeliveryID: s.delivery, Sequence: s.sequence, Topic: record.Topic, Key: base64.StdEncoding.EncodeToString(record.Key), RawPayload: append([]byte(nil), record.Value...), Metadata: map[string]string{"key_encoding": "base64", "partition": strconv.Itoa(int(record.Partition)), "offset": strconv.FormatInt(record.Offset, 10)}, PublishedAtUnixNano: record.Timestamp.UnixNano()}}}, nil
}
func (c *connector) AckStream(h string, req sdk.StreamAckRequest) error {
	r, s, err := c.stream(h, req.StreamID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.gate <- struct{}{}:
	}
	defer func() { <-s.gate }()
	if s.closed || s.pending == nil || req.DeliveryID != s.delivery || req.Credits != 1 {
		return errors.New("Kafka ACK must match the one pending record and one credit")
	}
	if err = s.client.CommitRecords(ctx, s.pending); err != nil {
		return errors.New("Kafka offset commit failed; replay outcome unknown")
	}
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.pending = nil
	s.credits = 1
	s.client.AllowRebalance()
	s.client.ResumeFetchTopics(s.topic)
	return nil
}
func (c *connector) NackStream(h string, req sdk.StreamNackRequest) error {
	_, s, err := c.stream(h, req.StreamID)
	if err != nil {
		return err
	}
	if !req.Requeue {
		return errors.New("Kafka discard is unsupported; configure a reviewed dead-letter path")
	}
	s.gate <- struct{}{}
	valid := s.pending != nil && s.delivery == req.DeliveryID
	<-s.gate
	if !valid {
		return errors.New("Kafka NACK does not match pending record")
	}
	return c.CloseStream(h, sdk.StreamCloseRequest{StreamID: req.StreamID})
}
func (c *connector) GrantCredits(h string, req sdk.StreamGrantCreditsRequest) error {
	_, s, err := c.stream(h, req.StreamID)
	if err != nil {
		return err
	}
	s.gate <- struct{}{}
	defer func() { <-s.gate }()
	if req.Credits != 0 {
		return errors.New("Kafka credits are released only by committed ACK")
	}
	return nil
}
func (c *connector) CloseStream(h string, req sdk.StreamCloseRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.mu.Lock()
	s := r.streams[req.StreamID]
	delete(r.streams, req.StreamID)
	r.mu.Unlock()
	if s != nil {
		s.close()
	}
	return nil
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	_, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusHealthy, ReasonCode: "DEMAND_MANAGED_BROKER_CLIENTS", Message: "configured; actual publish/fetch verifies reachability", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}

const configSchema = `{"type":"object","required":["brokers","topics"],"properties":{"brokers":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"string"}},"topics":{"type":"array","minItems":1,"maxItems":8,"uniqueItems":true,"items":{"type":"string"}},"group_prefix":{"type":"string","default":"iot-rule."},"tls":{"type":"boolean","default":true},"ca_pem":{"type":"string"},"username":{"type":"string"},"password":{"type":"string","format":"password","writeOnly":true},"sasl":{"type":"string","enum":["","plain","scram-sha-256","scram-sha-512"]},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"max_message_bytes":{"type":"integer","minimum":1,"maximum":1048576,"default":65536},"allow_write":{"type":"boolean","default":false}},"additionalProperties":false}`
