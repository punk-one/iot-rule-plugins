package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type postgresConnector struct {
	mu        sync.Mutex
	closed    bool
	resources map[string]*postgresResource
}

type postgresResource struct {
	resource sdk.ConnectorResource
	cfg      connectionConfig
	db       *sql.DB
}

func NewConnector() sdk.ConnectorPlugin {
	return &postgresConnector{
		resources: make(map[string]*postgresResource),
	}
}

func (c *postgresConnector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "connect-postgres",
		Name:        "Connector PostgreSQL",
		Version:     "1.0.3",
		Type:        sdk.PluginTypeConnector,
		Description: "Shared PostgreSQL connector for source and sink plugins",
		Category:    "connector",
		Capabilities: sdk.PluginCapabilities{
			ConfigSchema: connectorConfigSchema,
			ConnectorDescriptor: &sdk.ConnectorDescriptor{
				Family:        "postgres",
				Label:         "PostgreSQL",
				Capabilities:  []string{"read", "write", "shared_duplex", "connection_pool", "session:pooled"},
				DisplayFields: []string{"host", "port", "database"},
			},
		},
	}
}

func (c *postgresConnector) CreateResource(resource sdk.ConnectorResource) (string, error) {
	cfg, err := parseConnectionConfig(resource.Connection)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return "", fmt.Errorf("connector stopped")
	}
	previous := c.resources[resource.ID]
	c.resources[resource.ID] = &postgresResource{resource: resource, cfg: cfg}
	c.mu.Unlock()
	closeResourcePool(previous)
	return resource.ID, nil
}

func (c *postgresConnector) DestroyResource(providerHandle string) error {
	c.mu.Lock()
	resource := c.resources[providerHandle]
	delete(c.resources, providerHandle)
	c.mu.Unlock()
	closeResourcePool(resource)
	return nil
}

func closeResourcePool(resource *postgresResource) {
	if resource != nil && resource.db != nil {
		_ = resource.db.Close()
	}
}

func (c *postgresConnector) Execute(providerHandle string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
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

func (c *postgresConnector) OpenStream(providerHandle string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, sdk.ErrNotImplemented
}

func (c *postgresConnector) ReceiveStream(providerHandle string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, sdk.ErrNotImplemented
}

func (c *postgresConnector) AckStream(providerHandle string, req sdk.StreamAckRequest) error {
	return sdk.ErrNotImplemented
}

func (c *postgresConnector) NackStream(providerHandle string, req sdk.StreamNackRequest) error {
	return sdk.ErrNotImplemented
}

func (c *postgresConnector) GrantCredits(providerHandle string, req sdk.StreamGrantCreditsRequest) error {
	return sdk.ErrNotImplemented
}

func (c *postgresConnector) CloseStream(providerHandle string, req sdk.StreamCloseRequest) error {
	return sdk.ErrNotImplemented
}

func (c *postgresConnector) Probe(providerHandle string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	return c.probe(resource, req)
}

func (c *postgresConnector) Stop() error {
	c.mu.Lock()
	c.closed = true
	resources := c.resources
	c.resources = make(map[string]*postgresResource)
	c.mu.Unlock()
	for _, resource := range resources {
		closeResourcePool(resource)
	}
	return nil
}

func (c *postgresConnector) getResource(providerHandle string) (*postgresResource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil, fmt.Errorf("resource %s not found", providerHandle)
	}
	return resource, nil
}

func (c *postgresConnector) openDB(resource *postgresResource) (*sql.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.resources[resource.resource.ID] != resource {
		return nil, fmt.Errorf("database resource retired")
	}
	if resource.db != nil {
		return resource.db, nil
	}

	db, err := sql.Open("postgres", resource.cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}
	db.SetMaxOpenConns(resource.cfg.MaxOpenConns)
	db.SetMaxIdleConns(resource.cfg.MaxIdleConns)
	db.SetConnMaxLifetime(resource.cfg.connMaxLifetime())

	resource.db = db
	return resource.db, nil
}

type postgresPoolDiagnostics struct {
	Initialized        bool  `json:"initialized"`
	MaxOpenConnections int   `json:"max_open_connections"`
	OpenConnections    int   `json:"open_connections"`
	InUse              int   `json:"in_use"`
	Idle               int   `json:"idle"`
	WaitCount          int64 `json:"wait_count"`
	WaitDurationMS     int64 `json:"wait_duration_ms"`
	MaxIdleClosed      int64 `json:"max_idle_closed"`
	MaxLifetimeClosed  int64 `json:"max_lifetime_closed"`
}

func (c *postgresConnector) poolDiagnostics(resource *postgresResource) postgresPoolDiagnostics {
	c.mu.Lock()
	db := resource.db
	c.mu.Unlock()
	if db == nil {
		return postgresPoolDiagnostics{MaxOpenConnections: resource.cfg.MaxOpenConns}
	}
	stats := db.Stats()
	return postgresPoolDiagnostics{
		Initialized: true, MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections: stats.OpenConnections, InUse: stats.InUse, Idle: stats.Idle,
		WaitCount: stats.WaitCount, WaitDurationMS: stats.WaitDuration.Milliseconds(),
		MaxIdleClosed: stats.MaxIdleClosed, MaxLifetimeClosed: stats.MaxLifetimeClosed,
	}
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
