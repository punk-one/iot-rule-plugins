package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/punk-one/gos7"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type s7Connector struct {
	mu           sync.Mutex
	resources    map[string]*s7Resource
	openClient   s7ClientOpener
	sessionSlots chan struct{}
}

type s7Resource struct {
	resource            sdk.ConnectorResource
	cfg                 s7ConnectionConfig
	sessionPolicy       string
	operationMu         sync.Mutex
	session             gos7.Client
	sessionActive       bool
	closeSession        func() error
	sessionOpenedAt     time.Time
	sessionSlotHeld     bool
	idleTimer           *time.Timer
	idleGeneration      uint64
	nextDemands         map[string]time.Time
	consecutiveFailures uint64
	circuitOpenUntil    time.Time
	sessionOpens        uint64
	sessionReuses       uint64
	sessionCloses       uint64
	closed              bool
}

type s7ClientOpener func(s7ConnectionConfig) (gos7.Client, func() error, error)

func NewConnector() sdk.ConnectorPlugin {
	return newS7Connector(openS7Client)
}

func newS7Connector(opener s7ClientOpener) *s7Connector {
	return &s7Connector{
		resources:    make(map[string]*s7Resource),
		openClient:   opener,
		sessionSlots: make(chan struct{}, maxConcurrentS7Sessions),
	}
}

func (c *s7Connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "connect-s7",
		Name:        "Connector Siemens S7",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeConnector,
		Description: "Demand-aware Siemens S7 connector for batched source and sink operations",
		Category:    "connector",
		Capabilities: sdk.PluginCapabilities{
			ConfigSchema: connectorConfigSchema,
			ConnectorDescriptor: &sdk.ConnectorDescriptor{
				Family:        "s7",
				Label:         "Siemens S7",
				Capabilities:  []string{"read", "write", "session:scheduled_reuse", "session:per_operation", "concurrency:serialized", "scheduling:latest_only"},
				DisplayFields: []string{"host", "port", "rack", "slot"},
			},
		},
	}
}

func (c *s7Connector) CreateResource(resource sdk.ConnectorResource) (string, error) {
	cfg, err := parseConnectionConfig(resource.Connection)
	if err != nil {
		return "", err
	}

	handle := resource.ID
	if handle == "" {
		return "", fmt.Errorf("resource id is required")
	}

	newResource := &s7Resource{
		resource:      resource,
		cfg:           cfg,
		sessionPolicy: normalizeSessionPolicy(resource.SessionPolicy),
		nextDemands:   make(map[string]time.Time),
	}
	c.mu.Lock()
	oldResource := c.resources[handle]
	delete(c.resources, handle)
	c.mu.Unlock()
	if oldResource != nil {
		if err := c.closeResource(oldResource); err != nil {
			return "", fmt.Errorf("close replaced S7 resource: %w", err)
		}
	}
	c.mu.Lock()
	c.resources[handle] = newResource
	c.mu.Unlock()
	return handle, nil
}

func (c *s7Connector) DestroyResource(providerHandle string) error {
	c.mu.Lock()
	resource, ok := c.resources[providerHandle]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.resources, providerHandle)
	c.mu.Unlock()
	return c.closeResource(resource)
}

func (c *s7Connector) Execute(providerHandle string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
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

func (c *s7Connector) Probe(providerHandle string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	return c.probe(resource, req)
}

func (c *s7Connector) OpenStream(providerHandle string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, sdk.ErrNotImplemented
}

func (c *s7Connector) ReceiveStream(providerHandle string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, sdk.ErrNotImplemented
}

func (c *s7Connector) AckStream(providerHandle string, req sdk.StreamAckRequest) error {
	return sdk.ErrNotImplemented
}

func (c *s7Connector) NackStream(providerHandle string, req sdk.StreamNackRequest) error {
	return sdk.ErrNotImplemented
}

func (c *s7Connector) GrantCredits(providerHandle string, req sdk.StreamGrantCreditsRequest) error {
	return sdk.ErrNotImplemented
}

func (c *s7Connector) CloseStream(providerHandle string, req sdk.StreamCloseRequest) error {
	return sdk.ErrNotImplemented
}

func (c *s7Connector) Stop() error {
	c.mu.Lock()
	resources := make([]*s7Resource, 0, len(c.resources))
	for id, resource := range c.resources {
		resources = append(resources, resource)
		delete(c.resources, id)
	}
	c.mu.Unlock()
	var firstErr error
	for _, resource := range resources {
		if err := c.closeResource(resource); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *s7Connector) getResource(providerHandle string) (*s7Resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil, fmt.Errorf("resource %s not found", providerHandle)
	}
	return resource, nil
}

func openS7Client(cfg s7ConnectionConfig) (gos7.Client, func() error, error) {
	client, handler, _, _, err := s7ConnectClient(cfg.Host, cfg.Port, cfg.Rack, cfg.Slot, cfg.ConnectType, cfg.TimeoutSec, cfg.IdleTimeout)
	if err != nil {
		return nil, nil, err
	}
	return client, handler.Close, nil
}

func buildJSONResponse(payload any, metadata map[string]string) (sdk.ConnectorResponse, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	return sdk.ConnectorResponse{
		Payload:  raw,
		Metadata: metadata,
	}, nil
}

func requestTimeout(timeoutMS int, fallback time.Duration) time.Duration {
	if timeoutMS > 0 {
		return time.Duration(timeoutMS) * time.Millisecond
	}
	return fallback
}
