package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type config struct {
	Hosts       []string `json:"hosts"`
	Database    string   `json:"database"`
	Collections []string `json:"collections"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	AuthSource  string   `json:"auth_source"`
	TLS         bool     `json:"tls"`
	CAPEM       string   `json:"ca_pem"`
	Direct      bool     `json:"direct"`
	ReplicaSet  string   `json:"replica_set"`
	TimeoutMS   int      `json:"timeout_ms"`
	MaxRows     int      `json:"max_rows"`
	AllowWrite  bool     `json:"allow_write"`
}
type resource struct {
	cfg         config
	ctx         context.Context
	cancel      context.CancelFunc
	gate        chan struct{}
	client      *mongo.Client
	opts        *options.ClientOptions
	collections map[string]bool
}
type connector struct {
	mu        sync.Mutex
	resources map[string]*resource
	closed    bool
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
var fieldPath = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,7}$`)

func NewConnector() sdk.ConnectorPlugin { return &connector{resources: map[string]*resource{}} }
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-mongodb", Name: "MongoDB Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Bounded majority reads and immutable event inserts through a shared lazy pool", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "mongodb", Label: "MongoDB", Capabilities: []string{"read", "write", "pool:shared"}, DisplayFields: []string{"hosts", "database"}}}}
}
func decode(raw []byte, v any) error {
	if len(raw) > 2*1048576 {
		return errors.New("MongoDB request too large")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid MongoDB request")
	}
	return nil
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{TLS: true, Direct: true, AuthSource: "admin", TimeoutMS: 5000, MaxRows: 1000}
	if len(spec.Connection) > 131072 || decode(spec.Connection, &cfg) != nil || spec.ID == "" || len(cfg.Hosts) < 1 || len(cfg.Hosts) > 8 || !identifier.MatchString(cfg.Database) || !identifier.MatchString(cfg.AuthSource) || len(cfg.Collections) < 1 || len(cfg.Collections) > 32 || len(cfg.Username) > 128 || len(cfg.Password) > 4096 || (cfg.Username == "") != (cfg.Password == "") || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.MaxRows < 1 || cfg.MaxRows > 1000 || len(cfg.CAPEM) > 65536 || cfg.Direct && len(cfg.Hosts) != 1 {
		return "", errors.New("invalid MongoDB connection settings")
	}
	for _, address := range cfg.Hosts {
		host, port, err := net.SplitHostPort(address)
		number, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || number < 1 || number > 65535 || host == "" || len(host) > 253 || port == "" || strings.ContainsAny(host, "/@?#\\\x00\r\n") {
			return "", errors.New("MongoDB hosts require host:port without URI options")
		}
		if !cfg.TLS {
			if !cfg.Direct {
				return "", errors.New("loopback plaintext tests require direct mode")
			}
			ip := net.ParseIP(host)
			if ip == nil || !ip.IsLoopback() {
				return "", errors.New("MongoDB TLS is required outside literal loopback")
			}
		}
	}
	allowed := map[string]bool{}
	for _, name := range cfg.Collections {
		if !identifier.MatchString(name) || allowed[name] {
			return "", errors.New("invalid or duplicate MongoDB collection")
		}
		allowed[name] = true
	}
	timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
	opts := options.Client().SetHosts(cfg.Hosts).SetDirect(cfg.Direct).SetRetryReads(false).SetRetryWrites(false).SetMaxPoolSize(2).SetMaxConnecting(1).SetMinPoolSize(0).SetMaxConnIdleTime(time.Minute).SetConnectTimeout(timeout).SetServerSelectionTimeout(timeout).SetTimeout(timeout).SetCompressors([]string{}).SetAppName("iot-rule-mongodb-v1")
	if cfg.ReplicaSet != "" {
		if !identifier.MatchString(cfg.ReplicaSet) {
			return "", errors.New("invalid replica set name")
		}
		opts.SetReplicaSet(cfg.ReplicaSet)
	}
	if cfg.Username != "" {
		opts.SetAuth(options.Credential{AuthMechanism: "SCRAM-SHA-256", AuthSource: cfg.AuthSource, Username: cfg.Username, Password: cfg.Password})
	}
	if cfg.TLS {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.CAPEM != "" {
			roots, err := x509.SystemCertPool()
			if err != nil || roots == nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM([]byte(cfg.CAPEM)) {
				return "", errors.New("invalid MongoDB CA PEM")
			}
			tlsConfig.RootCAs = roots
		}
		opts.SetTLSConfig(tlsConfig)
	} else if cfg.CAPEM != "" {
		return "", errors.New("CA PEM requires TLS")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &resource{cfg: cfg, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1), opts: opts, collections: allowed}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.resources) >= 8 || c.resources[spec.ID] != nil {
		cancel()
		return "", errors.New("connector stopped, duplicate or resource limit")
	}
	c.resources[spec.ID] = r
	return spec.ID, nil
}
func (c *connector) get(h string) (*resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.resources[h]
	if r == nil {
		return nil, errors.New("unknown MongoDB resource")
	}
	return r, nil
}
func (r *resource) close() {
	r.cancel()
	r.gate <- struct{}{}
	defer func() { <-r.gate }()
	if r.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r.client.Disconnect(ctx)
		r.client = nil
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
func (r *resource) ensure(ctx context.Context) error {
	if r.client != nil {
		return nil
	}
	client, err := mongo.Connect(r.opts)
	if err != nil {
		return errors.New("MongoDB pool configuration failed")
	}
	if err = client.Ping(ctx, nil); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client.Disconnect(closeCtx)
		return errors.New("MongoDB connection or authentication failed")
	}
	r.client = client
	return nil
}
func (r *resource) collection(name string) *mongo.Collection {
	return r.client.Database(r.cfg.Database).Collection(name, options.Collection().SetReadConcern(readconcern.Majority()).SetWriteConcern(writeconcern.Majority()))
}

var filterOperators = map[string]bool{"$and": true, "$or": true, "$nor": true, "$eq": true, "$ne": true, "$gt": true, "$gte": true, "$lt": true, "$lte": true, "$in": true, "$nin": true, "$exists": true, "$not": true}

func validateFilter(v any, depth int, count *int) error {
	*count++
	if depth > 16 || *count > 4096 {
		return errors.New("MongoDB filter nesting or item limit exceeded")
	}
	switch x := v.(type) {
	case bson.D:
		seen := map[string]bool{}
		for _, e := range x {
			if seen[e.Key] || len(e.Key) > 128 || strings.HasPrefix(e.Key, "$") && !filterOperators[e.Key] || !strings.HasPrefix(e.Key, "$") && !fieldPath.MatchString(e.Key) {
				return errors.New("unsupported or duplicate MongoDB filter field/operator")
			}
			seen[e.Key] = true
			if err := validateFilter(e.Value, depth+1, count); err != nil {
				return err
			}
		}
	case bson.A:
		for _, item := range x {
			if err := validateFilter(item, depth+1, count); err != nil {
				return err
			}
		}
	}
	return nil
}
func boundedJSON(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	depth, count := 0, 0
	for {
		token, err := d.Token()
		if err == io.EOF {
			if depth != 0 {
				return errors.New("invalid MongoDB JSON nesting")
			}
			return nil
		}
		if err != nil {
			return errors.New("invalid MongoDB JSON")
		}
		if number, ok := token.(json.Number); ok && !strings.ContainsAny(string(number), ".eE") {
			integer, err := number.Int64()
			if err != nil || integer > 9007199254740991 || integer < -9007199254740991 {
				return errors.New("large integer requires canonical Extended JSON or decimal string")
			}
		}
		count++
		if count > 65536 {
			return errors.New("MongoDB JSON token limit exceeded")
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
				if depth > 24 {
					return errors.New("MongoDB JSON nesting exceeds 24")
				}
			case '}', ']':
				depth--
			}
		}
	}
}
func parseFilter(raw json.RawMessage) (bson.D, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if len(raw) > 65536 {
		return nil, errors.New("MongoDB filter exceeds 64 KiB")
	}
	if err := boundedJSON(raw); err != nil {
		return nil, err
	}
	var filter bson.D
	if bson.UnmarshalExtJSON(raw, false, &filter) != nil {
		return nil, errors.New("invalid MongoDB Extended JSON filter")
	}
	count := 0
	if err := validateFilter(filter, 0, &count); err != nil {
		return nil, err
	}
	return filter, nil
}

type findRequest struct {
	Collection string          `json:"collection"`
	Filter     json.RawMessage `json:"filter"`
	Sort       string          `json:"sort"`
	Descending bool            `json:"descending"`
	Limit      int             `json:"limit"`
}

func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	var find findRequest
	var insert struct {
		Collection string          `json:"collection"`
		EventID    string          `json:"event_id"`
		Payload    json.RawMessage `json:"payload"`
	}
	var filter bson.D
	var document bson.D
	if req.Kind == sdk.ConnectorRequestRead && req.Target == "find" {
		find.Limit = r.cfg.MaxRows
		if decode(req.Payload, &find) != nil || !r.collections[find.Collection] || find.Limit < 1 || find.Limit > r.cfg.MaxRows || find.Sort != "" && !fieldPath.MatchString(find.Sort) {
			return sdk.ConnectorResponse{}, errors.New("invalid bounded MongoDB find request")
		}
		filter, err = parseFilter(find.Filter)
		if err != nil {
			return sdk.ConnectorResponse{}, err
		}
	} else if req.Kind == sdk.ConnectorRequestWrite && req.Target == "insert_event" {
		if !r.cfg.AllowWrite || decode(req.Payload, &insert) != nil || !r.collections[insert.Collection] || insert.EventID == "" || len(insert.EventID) > 1024 || len(insert.Payload) > 1048576 || boundedJSON(insert.Payload) != nil || bson.UnmarshalExtJSON(insert.Payload, false, &document) != nil {
			return sdk.ConnectorResponse{}, errors.New("MongoDB insert disabled or invalid bounded event document")
		}
	} else {
		return sdk.ConnectorResponse{}, errors.New("MongoDB supports bounded find and immutable insert_event only")
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
	if ctx.Err() != nil {
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	if err = r.ensure(ctx); err != nil {
		return sdk.ConnectorResponse{}, err
	}
	var result any
	if req.Target == "find" {
		opts := options.Find().SetLimit(int64(find.Limit + 1)).SetBatchSize(int32(min(find.Limit+1, 16)))
		if find.Sort != "" {
			direction := 1
			if find.Descending {
				direction = -1
			}
			opts.SetSort(bson.D{{Key: find.Sort, Value: direction}})
		}
		cursor, e := r.collection(find.Collection).Find(ctx, filter, opts)
		if e != nil {
			return sdk.ConnectorResponse{}, errors.New("MongoDB find failed")
		}
		defer func() {
			closeCtx, cancel := context.WithTimeout(r.ctx, time.Second)
			defer cancel()
			cursor.Close(closeCtx)
		}()
		docs := []json.RawMessage{}
		total := 0
		for cursor.Next(ctx) {
			if len(docs) >= find.Limit {
				return sdk.ConnectorResponse{}, errors.New("MongoDB result row limit exceeded; narrow the filter")
			}
			if len(cursor.Current) > 1048576 {
				return sdk.ConnectorResponse{}, errors.New("MongoDB document exceeds 1 MiB")
			}
			raw, e := bson.MarshalExtJSON(cursor.Current, true, false)
			if e != nil {
				return sdk.ConnectorResponse{}, errors.New("MongoDB document conversion failed")
			}
			total += len(raw)
			if len(raw) > 1048576 || total > 4*1048576 {
				return sdk.ConnectorResponse{}, errors.New("MongoDB result byte limit exceeded")
			}
			docs = append(docs, raw)
		}
		if cursor.Err() != nil {
			return sdk.ConnectorResponse{}, errors.New("MongoDB cursor failed; partial results discarded")
		}
		result = map[string]any{"documents": docs, "count": len(docs)}
	} else {
		digest := sha256.Sum256(insert.Payload)
		fingerprint := hex.EncodeToString(digest[:])
		stored := bson.D{{Key: "_id", Value: insert.EventID}, {Key: "sha256", Value: fingerprint}, {Key: "payload", Value: document}}
		_, e := r.collection(insert.Collection).InsertOne(ctx, stored)
		duplicate := false
		if e != nil {
			if !mongo.IsDuplicateKeyError(e) {
				return sdk.ConnectorResponse{}, errors.New("MongoDB majority insert failed; outcome unknown, not retried")
			}
			var existing struct {
				SHA256 string `bson:"sha256"`
			}
			if e = r.collection(insert.Collection).FindOne(ctx, bson.D{{Key: "_id", Value: insert.EventID}}).Decode(&existing); e != nil || existing.SHA256 != fingerprint {
				return sdk.ConnectorResponse{}, errors.New("MongoDB event ID conflicts or existing majority receipt unavailable")
			}
			duplicate = true
		}
		result = map[string]any{"committed": true, "duplicate": duplicate, "event_id": insert.EventID}
	}
	raw, err := json.Marshal(result)
	return sdk.ConnectorResponse{Payload: raw}, err
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	select {
	case <-r.ctx.Done():
		return sdk.ResourceStatusEvent{}, errors.New("resource stopped")
	default:
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusHealthy, ReasonCode: "DEMAND_MANAGED_POOL", Message: "configured; database operations verify actual reachability", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
func (c *connector) OpenStream(string, sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, errors.New("MongoDB change streams not supported")
}
func (c *connector) ReceiveStream(string, sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, errors.New("MongoDB change streams not supported")
}
func (c *connector) AckStream(string, sdk.StreamAckRequest) error {
	return errors.New("MongoDB streaming not supported")
}
func (c *connector) NackStream(string, sdk.StreamNackRequest) error {
	return errors.New("MongoDB streaming not supported")
}
func (c *connector) GrantCredits(string, sdk.StreamGrantCreditsRequest) error {
	return errors.New("MongoDB streaming not supported")
}
func (c *connector) CloseStream(string, sdk.StreamCloseRequest) error { return nil }

const configSchema = `{"type":"object","required":["hosts","database","collections"],"properties":{"hosts":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"string"}},"database":{"type":"string"},"collections":{"type":"array","minItems":1,"maxItems":32,"uniqueItems":true,"items":{"type":"string"}},"username":{"type":"string"},"password":{"type":"string","format":"password","writeOnly":true},"auth_source":{"type":"string","default":"admin"},"tls":{"type":"boolean","default":true},"ca_pem":{"type":"string"},"direct":{"type":"boolean","default":true},"replica_set":{"type":"string"},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"max_rows":{"type":"integer","minimum":1,"maximum":1000,"default":1000},"allow_write":{"type":"boolean","default":false}},"additionalProperties":false}`
