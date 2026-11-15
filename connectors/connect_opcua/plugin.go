package main

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	Endpoint              string `json:"endpoint"`
	SecurityMode          string `json:"security_mode"`
	ClientCertificateFile string `json:"client_certificate_file"`
	PrivateKeyFile        string `json:"private_key_file"`
	ServerCertificateFile string `json:"server_certificate_file"`
	Username              string `json:"username"`
	Password              string `json:"password"`
	TimeoutMS             int    `json:"timeout_ms"`
	ReuseMS               int    `json:"session_reuse_ms"`
}
type point struct {
	Name         string `json:"name"`
	NodeID       string `json:"node_id"`
	NamespaceURI string `json:"namespace_uri,omitempty"`
	ValueType    string `json:"valueType,omitempty"`
	Value        any    `json:"value,omitempty"`
}
type batch struct {
	ExpiresAt    *int64  `json:"expires_at,omitempty"`
	Points       []point `json:"points"`
	NodeID       string  `json:"node_id,omitempty"`
	NamespaceURI string  `json:"namespace_uri,omitempty"`
}
type uaSession interface {
	Close(context.Context) error
	Read(context.Context, *ua.ReadRequest) (*ua.ReadResponse, error)
	Write(context.Context, *ua.WriteRequest) (*ua.WriteResponse, error)
	Browse(context.Context, *ua.BrowseRequest) (*ua.BrowseResponse, error)
	BrowseNext(context.Context, *ua.BrowseNextRequest) (*ua.BrowseNextResponse, error)
}
type resource struct {
	cfg        config
	ctx        context.Context
	cancel     context.CancelFunc
	gate       chan struct{}
	client     uaSession
	lastUse    time.Time
	lastProbe  time.Time
	done       chan struct{}
	namespaces map[string]uint16
	streams    map[string]*uaConsumer
}
type connector struct {
	mu        sync.RWMutex
	resources map[string]*resource
	open      func(context.Context, config) (uaSession, error)
	stopped   bool
	subscribe subscribeFunction
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, open: openSession, subscribe: subscribeNative}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-opcua", Name: "OPC UA Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Pinned-server OPC UA read/write/browse and bounded data/event subscriptions", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "opcua", Label: "OPC UA", Capabilities: []string{"read", "write", "browse", "shared_duplex", "session:on_demand"}, DisplayFields: []string{"endpoint", "security_mode"}}}}
}
func validateConfig(cfg config) error {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Scheme != "opc.tcp" || u.Host == "" || u.User != nil || u.Fragment != "" || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 30000 || cfg.ReuseMS < 0 || cfg.ReuseMS > 30000 {
		return errors.New("invalid OPC UA connection settings")
	}
	switch cfg.SecurityMode {
	case "None":
		if cfg.Username != "" || cfg.Password != "" {
			return errors.New("username/password requires SignAndEncrypt")
		}
	case "SignAndEncrypt":
		if cfg.ClientCertificateFile == "" || cfg.PrivateKeyFile == "" || cfg.ServerCertificateFile == "" {
			return errors.New("secure OPC UA requires client certificate, private key and pinned server certificate files")
		}
	default:
		return errors.New("security_mode must be None or SignAndEncrypt")
	}
	return nil
}
func (c *connector) CreateResource(r sdk.ConnectorResource) (string, error) {
	cfg := config{SecurityMode: "SignAndEncrypt", TimeoutMS: 5000, ReuseMS: 2000}
	if err := json.Unmarshal(r.Connection, &cfg); err != nil {
		return "", errors.New("invalid connection JSON")
	}
	if r.ID == "" {
		return "", errors.New("resource id required")
	}
	if err := validateConfig(cfg); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(context.Background())
	rr := &resource{cfg: cfg, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1), done: make(chan struct{}), streams: map[string]*uaConsumer{}}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		cancel()
		return "", errors.New("connector stopped")
	}
	if _, exists := c.resources[r.ID]; exists {
		c.mu.Unlock()
		cancel()
		return "", errors.New("resource exists")
	}
	c.resources[r.ID] = rr
	c.mu.Unlock()
	go rr.idleLoop()
	return r.ID, nil
}
func (r *resource) idleLoop() {
	defer close(r.done)
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-r.ctx.Done():
			r.gate <- struct{}{}
			r.closeClient()
			<-r.gate
			return
		case <-timer.C:
			select {
			case r.gate <- struct{}{}:
				if r.client != nil && !r.sessionHealthy() {
					r.closeClient()
				}
				if r.client != nil && len(r.streams) == 0 && time.Since(r.lastUse) >= time.Duration(r.cfg.ReuseMS)*time.Millisecond {
					r.closeClient()
				}
				<-r.gate
			default:
			}
		}
	}
}
func (r *resource) closeClient() {
	r.namespaces = nil
	r.lastProbe = time.Time{}

	if r.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = r.client.Close(ctx)
		cancel()
		r.client = nil
	}
	for _, s := range r.streams {
		s.cancel()
		<-s.done
	}
}
func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	r := c.resources[h]
	delete(c.resources, h)
	c.mu.Unlock()
	if r != nil {
		r.cancel()
		<-r.done
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	c.stopped = true
	rs := make([]*resource, 0, len(c.resources))
	for h, r := range c.resources {
		r.cancel()
		rs = append(rs, r)
		delete(c.resources, h)
	}
	c.mu.Unlock()
	for _, r := range rs {
		<-r.done
	}
	return nil
}

func certificate(path string) ([]byte, error) {
	raw, err := boundedFile(path, 1048576, false)
	if err != nil {
		return nil, errors.New("cannot read configured certificate file")
	}
	if block, _ := pem.Decode(raw); block != nil {
		raw = block.Bytes
	}
	cert, err := x509.ParseCertificate(raw)
	if err != nil {
		return nil, errors.New("invalid certificate file")
	}
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return nil, errors.New("certificate outside validity period")
	}
	return raw, nil
}
func openSession(ctx context.Context, cfg config) (uaSession, error) {
	options := []opcua.Option{opcua.AutoReconnect(false), opcua.MaxMessageSize(4194304), opcua.MaxChunkCount(64), opcua.DialTimeout(time.Duration(cfg.TimeoutMS) * time.Millisecond), opcua.RequestTimeout(time.Duration(cfg.TimeoutMS) * time.Millisecond)}
	if cfg.SecurityMode == "None" {
		options = append(options, opcua.SecurityPolicy("None"), opcua.SecurityMode(ua.MessageSecurityModeNone), opcua.AuthAnonymous())
	} else {
		key, keyErr := privateKey(cfg.PrivateKeyFile)
		if keyErr != nil {
			return nil, keyErr
		}
		clientDER, certErr := certificate(cfg.ClientCertificateFile)
		if certErr != nil {
			return nil, certErr
		}
		clientCert, certErr := x509.ParseCertificate(clientDER)
		if certErr != nil {
			return nil, errors.New("invalid client certificate")
		}
		public, ok := clientCert.PublicKey.(*rsa.PublicKey)
		if !ok || public.E != key.E || public.N.Cmp(key.N) != 0 {
			return nil, errors.New("client certificate and RSA private key do not match")
		}

		pinned, err := certificate(cfg.ServerCertificateFile)
		if err != nil {
			return nil, err
		}
		endpoints, err := opcua.GetEndpoints(ctx, cfg.Endpoint, options...)
		if err != nil {
			return nil, errors.New("OPC UA discovery failed")
		}
		var selected *ua.EndpointDescription
		authType := ua.UserTokenTypeAnonymous
		if cfg.Username != "" {
			authType = ua.UserTokenTypeUserName
		}
		for _, ep := range endpoints {
			if ep.SecurityMode != ua.MessageSecurityModeSignAndEncrypt || !strings.HasSuffix(ep.SecurityPolicyURI, "#Basic256Sha256") || !bytes.Equal(ep.ServerCertificate, pinned) {
				continue
			}
			for _, token := range ep.UserIdentityTokens {
				if token.TokenType == authType {
					selected = ep
					break
				}
			}
			if selected != nil {
				break
			}
		}
		if selected == nil {
			return nil, errors.New("no endpoint matches pinned certificate and required security")
		}
		options = append(options, opcua.Certificate(clientDER), opcua.PrivateKey(key))
		if cfg.Username != "" {
			options = append(options, opcua.AuthUsername(cfg.Username, cfg.Password))
		} else {
			options = append(options, opcua.AuthAnonymous())
		}
		options = append(options, opcua.SecurityFromEndpoint(selected, authType))
	}
	client, err := opcua.NewClient(cfg.Endpoint, options...)
	if err != nil {
		return nil, errors.New("invalid OPC UA client settings")
	}
	if err = client.Connect(ctx); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = client.Close(closeCtx)
		cancel()
		return nil, errors.New("OPC UA connect failed")
	}
	return client, nil
}
func validatePoints(points []point, write bool) error {
	if len(points) == 0 || len(points) > 1024 {
		return errors.New("point count must be 1..1024")
	}
	seen := map[string]bool{}
	for _, p := range points {
		if p.Name == "" || len(p.Name) > 128 || p.NodeID == "" || len(p.NodeID) > 1024 || len(p.NamespaceURI) > 1024 || seen[p.Name] {
			return errors.New("duplicate or missing point name")
		}
		seen[p.Name] = true
		if _, err := ua.ParseNodeID(p.NodeID); err != nil {
			return errors.New("invalid OPC UA NodeId")
		}
		node, _ := ua.ParseNodeID(p.NodeID)
		if p.NamespaceURI != "" && node.Namespace() != 0 {
			return errors.New("namespace_uri requires namespace-zero node_id")
		}
		if write {
			if _, err := variant(p); err != nil {
				return err
			}
		}
	}
	return nil
}
func variant(p point) (*ua.Variant, error) {
	text := fmt.Sprint(p.Value)
	var value any
	var err error
	switch p.ValueType {
	case "Bool":
		v, ok := p.Value.(bool)
		if !ok {
			return nil, errors.New("Bool write requires boolean")
		}
		value = v
	case "String":
		v, ok := p.Value.(string)
		if !ok {
			return nil, errors.New("String write requires string")
		}
		value = v
	case "Int16":
		var v int64
		v, err = strconv.ParseInt(text, 10, 16)
		value = int16(v)
	case "Uint16":
		var v uint64
		v, err = strconv.ParseUint(text, 10, 16)
		value = uint16(v)
	case "Int32":
		var v int64
		v, err = strconv.ParseInt(text, 10, 32)
		value = int32(v)
	case "Uint32":
		var v uint64
		v, err = strconv.ParseUint(text, 10, 32)
		value = uint32(v)
	case "Float32":
		var v float64
		v, err = strconv.ParseFloat(text, 32)
		value = float32(v)
	case "Float64":
		value, err = strconv.ParseFloat(text, 64)
	default:
		return nil, errors.New("unsupported OPC UA write valueType")
	}
	if err != nil {
		return nil, errors.New("invalid typed write value")
	}
	switch v := value.(type) {
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("non-finite write value")
		}
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, errors.New("non-finite write value")
		}
	}
	return ua.NewVariant(value)
}
func serializable(value any, depth int) (any, error) {
	if depth > 32 {
		return nil, errors.New("UA value nesting exceeded")
	}
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	case []byte:
		if len(v) > 1048576 {
			return nil, errors.New("UA ByteString too large")
		}
		return base64.StdEncoding.EncodeToString(v), nil
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String:
		if len(v.String()) > 1048576 {
			return nil, errors.New("UA string too large")
		}
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := v.Int()
		if n > 9007199254740991 || n < -9007199254740991 {
			return strconv.FormatInt(n, 10), nil
		}
		return n, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n := v.Uint()
		if n > 9007199254740991 {
			return strconv.FormatUint(n, 10), nil
		}
		return n, nil
	case reflect.Float32, reflect.Float64:
		n := v.Float()
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, errors.New("UA value is not finite")
		}
		return n, nil
	case reflect.Array, reflect.Slice:
		if v.Len() > 4096 {
			return nil, errors.New("UA array too large")
		}
		out := make([]any, v.Len())
		for i := range out {
			next, err := serializable(v.Index(i).Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = next
		}
		return out, nil
	}
	return nil, errors.New("UA structured type requires an explicit decoder")
}

func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ConnectorResponse{}, errors.New("unknown resource")
	}
	var spec batch
	decoder := json.NewDecoder(bytes.NewReader(req.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&spec); err != nil {
		return sdk.ConnectorResponse{}, errors.New("invalid OPC UA request")
	}
	write := req.Kind == sdk.ConnectorRequestWrite || req.Kind == sdk.ConnectorRequestBatchWrite
	browse := req.Target == "browse"
	if !browse {
		if err := validatePoints(spec.Points, write); err != nil {
			return sdk.ConnectorResponse{}, err
		}
	} else {
		if req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestMetadata {
			return sdk.ConnectorResponse{}, errors.New("browse is read-only")
		}
		if _, err := ua.ParseNodeID(spec.NodeID); err != nil {
			return sdk.ConnectorResponse{}, errors.New("invalid browse NodeId")
		}
	}
	if !browse && !write && req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return sdk.ConnectorResponse{}, errors.New("unsupported request kind")
	}
	timeout := time.Duration(r.cfg.TimeoutMS) * time.Millisecond
	if req.TimeoutMS > 0 && time.Duration(req.TimeoutMS)*time.Millisecond < timeout {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	if spec.ExpiresAt != nil {
		if *spec.ExpiresAt <= time.Now().UnixMilli() {
			return sdk.ConnectorResponse{}, errors.New("command expired before connection")
		}
		var expiryCancel context.CancelFunc
		ctx, expiryCancel = context.WithDeadline(ctx, time.UnixMilli(*spec.ExpiresAt))
		defer expiryCancel()
	}

	select {
	case r.gate <- struct{}{}:
		defer func() {
			r.lastUse = time.Now()
			if r.cfg.ReuseMS == 0 && len(r.streams) == 0 {
				r.closeClient()
			}
			<-r.gate
		}()
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	if r.client == nil {
		client, err := c.open(ctx, r.cfg)
		if err != nil {
			return sdk.ConnectorResponse{}, err
		}
		r.client = client
	}
	var resolveErr error
	if browse {
		var resolved []point
		resolved, resolveErr = r.resolvePoints(ctx, []point{{Name: "browse", NodeID: spec.NodeID, NamespaceURI: spec.NamespaceURI}})
		if resolveErr == nil {
			spec.NodeID = resolved[0].NodeID
		}
	} else {
		spec.Points, resolveErr = r.resolvePoints(ctx, spec.Points)
	}
	if resolveErr != nil {
		return sdk.ConnectorResponse{}, resolveErr
	}
	var result any
	var err error
	if browse {
		result, err = browseNodes(ctx, r.client, spec.NodeID)
	} else if write {
		result, err = writeNodes(ctx, r.client, spec.Points)
	} else {
		result, err = readNodes(ctx, r.client, spec.Points)
	}
	if err != nil {
		r.closeClient()
		return sdk.ConnectorResponse{}, err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return sdk.ConnectorResponse{}, errors.New("OPC UA value is not JSON serializable")
	}
	return sdk.ConnectorResponse{Payload: raw}, nil
}
func readNodes(ctx context.Context, c uaSession, points []point) (any, error) {
	values := map[string]any{}
	quality := map[string]string{}
	times := map[string]int64{}
	segments := 0
	for start := 0; start < len(points); start += 256 {
		end := min(len(points), start+256)
		request := &ua.ReadRequest{TimestampsToReturn: ua.TimestampsToReturnBoth}
		for _, p := range points[start:end] {
			node, _ := ua.ParseNodeID(p.NodeID)
			request.NodesToRead = append(request.NodesToRead, &ua.ReadValueID{NodeID: node, AttributeID: ua.AttributeIDValue})
		}
		response, err := c.Read(ctx, request)
		if err != nil || response == nil || len(response.Results) != end-start {
			return nil, errors.New("OPC UA read failed or returned incomplete results")
		}
		for i, value := range response.Results {
			name := points[start+i].Name
			if value == nil {
				quality[name] = "bad:missing"
				values[name] = nil
				continue
			}
			if uint32(value.Status)&0xC0000000 != 0 {
				quality[name] = fmt.Sprintf("bad:0x%08x", uint32(value.Status))
				values[name] = nil
			} else {
				quality[name] = "good"
				if value.Value != nil {
					normalized, err := serializable(value.Value.Value(), 0)
					if err != nil {
						values[name] = nil
						quality[name] = "bad:unsupported_value"
					} else {
						values[name] = normalized
					}
				} else {
					values[name] = nil
					quality[name] = "bad:missing"
				}
			}
			if !value.SourceTimestamp.IsZero() {
				times[name] = value.SourceTimestamp.UnixMilli()
			}
		}
		segments++
	}
	return map[string]any{"values": values, "quality": quality, "source_time": times, "segments": segments}, nil
}
func writeNodes(ctx context.Context, c uaSession, points []point) (any, error) {
	request := &ua.WriteRequest{}
	for _, p := range points {
		node, _ := ua.ParseNodeID(p.NodeID)
		value, _ := variant(p)
		request.NodesToWrite = append(request.NodesToWrite, &ua.WriteValue{NodeID: node, AttributeID: ua.AttributeIDValue, Value: &ua.DataValue{EncodingMask: ua.DataValueValue, Value: value}})
	}
	response, err := c.Write(ctx, request)
	if err != nil || response == nil || len(response.Results) != len(points) {
		return nil, errors.New("OPC UA write outcome unknown; do not retry automatically")
	}
	for _, status := range response.Results {
		if uint32(status)&0xC0000000 != 0 {
			return nil, errors.New("OPC UA batch write partially failed; do not retry automatically")
		}
	}
	return map[string]any{"written": len(points)}, nil
}
func browseNodes(ctx context.Context, c uaSession, nodeID string) (any, error) {
	node, _ := ua.ParseNodeID(nodeID)
	response, err := c.Browse(ctx, &ua.BrowseRequest{RequestedMaxReferencesPerNode: 128, NodesToBrowse: []*ua.BrowseDescription{{NodeID: node, BrowseDirection: ua.BrowseDirectionForward, ReferenceTypeID: ua.NewNumericNodeID(0, id.References), IncludeSubtypes: true, ResultMask: uint32(ua.BrowseResultMaskAll)}}})
	if err != nil || response == nil || len(response.Results) != 1 {
		return nil, errors.New("OPC UA browse failed")
	}
	result := response.Results[0]
	if result == nil {
		return nil, errors.New("empty browse result")
	}
	if result.StatusCode != ua.StatusOK {
		return nil, errors.New("OPC UA browse status not good")
	}
	if len(result.References) > 128 {
		return nil, errors.New("browse references exceed requested bound")
	}
	refs := make([]map[string]any, 0, len(result.References))
	for _, ref := range result.References {
		if ref == nil || ref.NodeID == nil || ref.BrowseName == nil {
			return nil, errors.New("invalid browse reference")
		}
		refs = append(refs, map[string]any{"node_id": ref.NodeID.String(), "name": ref.BrowseName.Name, "node_class": uint32(ref.NodeClass)})
	}
	truncated := len(result.ContinuationPoint) > 0
	if truncated {
		_, err = c.BrowseNext(ctx, &ua.BrowseNextRequest{ReleaseContinuationPoints: true, ContinuationPoints: [][]byte{result.ContinuationPoint}})
		if err != nil {
			return nil, errors.New("failed to release browse continuation")
		}
	}
	return map[string]any{"references": refs, "truncated": truncated}, nil
}
func (c *connector) Probe(h string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	c.mu.RLock()
	r := c.resources[h]
	c.mu.RUnlock()
	if r == nil {
		return sdk.ResourceStatusEvent{}, errors.New("unknown resource")
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: sdk.ResourceStatusHealthy, ReasonCode: "DEMAND_MANAGED", Message: "configured; actual requests verify reachability", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}
