package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	redislib "github.com/redis/go-redis/v9"
)

type redisClient interface {
	Ping(ctx context.Context) error
	XRead(ctx context.Context, args *redislib.XReadArgs) ([]redislib.XStream, error)
	XAdd(ctx context.Context, args *redislib.XAddArgs) (string, error)
	HSet(ctx context.Context, key string, values map[string]any) error
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HGet(ctx context.Context, key, field string) (string, error)
	Close() error

	// Combined HSet + Expire in a single pipeline RTT
	HSetWithExpire(ctx context.Context, key string, values map[string]any, ttl time.Duration) error

	// TTL management & key operations
	Expire(ctx context.Context, key string, ttl time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	Persist(ctx context.Context, key string) error
	Delete(ctx context.Context, keys ...string) error
}

type goRedisClient struct {
	client *redislib.Client
}

func (c *goRedisClient) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *goRedisClient) XRead(ctx context.Context, args *redislib.XReadArgs) ([]redislib.XStream, error) {
	return c.client.XRead(ctx, args).Result()
}

func (c *goRedisClient) XAdd(ctx context.Context, args *redislib.XAddArgs) (string, error) {
	return c.client.XAdd(ctx, args).Result()
}

func (c *goRedisClient) HSet(ctx context.Context, key string, values map[string]any) error {
	return c.client.HSet(ctx, key, values).Err()
}

func (c *goRedisClient) HSetWithExpire(ctx context.Context, key string, values map[string]any, ttl time.Duration) error {
	pipe := c.client.TxPipeline()
	pipe.HSet(ctx, key, values)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *goRedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.client.HGetAll(ctx, key).Result()
}

func (c *goRedisClient) HGet(ctx context.Context, key, field string) (string, error) {
	return c.client.HGet(ctx, key, field).Result()
}

func (c *goRedisClient) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.client.Expire(ctx, key, ttl).Err()
}

func (c *goRedisClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.client.TTL(ctx, key).Result()
}

func (c *goRedisClient) Persist(ctx context.Context, key string) error {
	return c.client.Persist(ctx, key).Err()
}

func (c *goRedisClient) Delete(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

func (c *goRedisClient) Close() error {
	return c.client.Close()
}

var newRedisClient = func(cfg connectionConfig) (redisClient, error) {
	client := redislib.NewClient(&redislib.Options{
		Addr:            cfg.Addr,
		Username:        cfg.Username,
		Password:        cfg.Password,
		DB:              cfg.DB,
		DialTimeout:     cfg.dialTimeout(),
		ReadTimeout:     cfg.readTimeout(),
		WriteTimeout:    cfg.writeTimeout(),
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		ConnMaxLifetime: cfg.connMaxLifetime(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.dialTimeout())
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}

	return &goRedisClient{client: client}, nil
}

type redisConnector struct {
	mu        sync.Mutex
	resources map[string]*redisResource
}

type redisResource struct {
	resource sdk.ConnectorResource
	cfg      connectionConfig

	mu       sync.Mutex
	client   redisClient
	bindings map[string]*redisBinding
	sessions map[string]*redisStreamSession
}

type redisBinding struct {
	streamCursors map[string]string
}

type redisStreamMessage struct {
	Stream     string         `json:"stream"`
	ID         string         `json:"id"`
	Values     map[string]any `json:"values"`
	ReceivedAt string         `json:"received_at,omitempty"`
}

func NewConnector() sdk.ConnectorPlugin {
	return &redisConnector{
		resources: make(map[string]*redisResource),
	}
}

func (c *redisConnector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "connect-redis",
		Name:        "Connector Redis",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeConnector,
		Description: "Shared Redis connector for stream and hash-based source/sink plugins",
		Category:    "connector",
		Capabilities: sdk.PluginCapabilities{
			ConfigSchema: connectorConfigSchema,
			ConnectorDescriptor: &sdk.ConnectorDescriptor{
				Family:        "redis",
				Label:         "Redis",
				Capabilities:  []string{"read", "write", "shared_duplex", "session:pooled"},
				DisplayFields: []string{"addr", "db"},
			},
		},
	}
}

func (c *redisConnector) CreateResource(resource sdk.ConnectorResource) (string, error) {
	cfg, err := parseConnectionConfig(resource.Connection)
	if err != nil {
		return "", err
	}
	handle := stringsTrim(resource.ID)
	if handle == "" {
		return "", fmt.Errorf("resource id is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.resources[handle]; ok {
		current.mu.Lock()
		if current.client != nil {
			_ = current.client.Close()
			current.client = nil
		}
		current.mu.Unlock()
	}

	c.resources[handle] = &redisResource{
		resource: resource,
		cfg:      cfg,
		bindings: make(map[string]*redisBinding),
		sessions: make(map[string]*redisStreamSession),
	}
	return handle, nil
}

func (c *redisConnector) DestroyResource(providerHandle string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil
	}

	resource.mu.Lock()
	if resource.client != nil {
		_ = resource.client.Close()
		resource.client = nil
	}
	for _, session := range resource.sessions {
		session.close()
	}
	resource.mu.Unlock()

	delete(c.resources, providerHandle)
	return nil
}

func (c *redisConnector) Execute(providerHandle string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	switch req.Kind {
	case sdk.ConnectorRequestMetadata:
		return c.executeMetadata(resource, req)
	case sdk.ConnectorRequestRead, sdk.ConnectorRequestBatchRead:
		return c.executeRead(resource, req)
	case sdk.ConnectorRequestWrite, sdk.ConnectorRequestBatchWrite:
		return c.executeWrite(resource, req)
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported request kind %q", req.Kind)
	}
}

func (c *redisConnector) OpenStream(providerHandle string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	return c.openReadStream(resource, req)
}

func (c *redisConnector) ReceiveStream(providerHandle string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	return c.receiveReadStream(resource, req)
}

func (c *redisConnector) AckStream(providerHandle string, req sdk.StreamAckRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.ackReadStream(resource, req)
}

func (c *redisConnector) NackStream(providerHandle string, req sdk.StreamNackRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.nackReadStream(resource, req)
}

func (c *redisConnector) GrantCredits(providerHandle string, req sdk.StreamGrantCreditsRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.grantReadStreamCredits(resource, req)
}

func (c *redisConnector) CloseStream(providerHandle string, req sdk.StreamCloseRequest) error {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return err
	}
	return c.closeReadStream(resource, req)
}

func (c *redisConnector) Probe(providerHandle string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	return c.probe(resource, req)
}

func (c *redisConnector) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, resource := range c.resources {
		resource.mu.Lock()
		if resource.client != nil {
			_ = resource.client.Close()
			resource.client = nil
		}
		resource.mu.Unlock()
		delete(c.resources, id)
	}
	return nil
}

func (c *redisConnector) getResource(providerHandle string) (*redisResource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil, fmt.Errorf("resource %s not found", providerHandle)
	}
	return resource, nil
}

func (c *redisConnector) openClient(resource *redisResource) (redisClient, error) {
	resource.mu.Lock()
	defer resource.mu.Unlock()

	if resource.client != nil {
		return resource.client, nil
	}

	client, err := newRedisClient(resource.cfg)
	if err != nil {
		return nil, err
	}
	resource.client = client
	return resource.client, nil
}

func (c *redisConnector) cursorForStream(resource *redisResource, token string, stream string, startID string) string {
	resource.mu.Lock()
	defer resource.mu.Unlock()

	binding, ok := resource.bindings[token]
	if !ok {
		binding = &redisBinding{streamCursors: make(map[string]string)}
		resource.bindings[token] = binding
	}
	if current, ok := binding.streamCursors[stream]; ok && current != "" {
		return current
	}
	if startID != "" {
		return startID
	}
	return "$"
}

func (c *redisConnector) setCursor(resource *redisResource, token string, stream string, cursor string) {
	resource.mu.Lock()
	defer resource.mu.Unlock()

	binding, ok := resource.bindings[token]
	if !ok {
		binding = &redisBinding{streamCursors: make(map[string]string)}
		resource.bindings[token] = binding
	}
	binding.streamCursors[stream] = cursor
}

func buildJSONResponse(payload any, metadata map[string]string) (sdk.ConnectorResponse, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	return sdk.ConnectorResponse{Payload: raw, Metadata: metadata}, nil
}

func requestTimeout(timeoutMS int, fallback time.Duration) time.Duration {
	if timeoutMS > 0 {
		return time.Duration(timeoutMS) * time.Millisecond
	}
	return fallback
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}
