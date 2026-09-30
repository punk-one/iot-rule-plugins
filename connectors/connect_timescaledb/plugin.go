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

type timescaledbConnector struct {
	mu        sync.Mutex
	resources map[string]*timescaledbResource
}

type timescaledbResource struct {
	resource sdk.ConnectorResource
	cfg      connectionConfig
	db       *sql.DB
}

func NewConnector() sdk.ConnectorPlugin {
	return &timescaledbConnector{
		resources: make(map[string]*timescaledbResource),
	}
}

func (c *timescaledbConnector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "connect-timescaledb",
		Name:        "Connector TimescaleDB",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeConnector,
		Description: "Shared TimescaleDB/PostgreSQL connector for source and sink plugins",
		Category:    "connector",
		Capabilities: sdk.PluginCapabilities{
			ConfigSchema: connectorConfigSchema,
			ConnectorDescriptor: &sdk.ConnectorDescriptor{
				Family:        "timescaledb",
				Label:         "TimescaleDB / PostgreSQL",
				Capabilities:  []string{"read", "write", "shared_duplex", "connection_pool", "session:pooled"},
				DisplayFields: []string{"host", "port", "database"},
			},
		},
	}
}

func (c *timescaledbConnector) CreateResource(resource sdk.ConnectorResource) (string, error) {
	cfg, err := parseConnectionConfig(resource.Connection)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.resources[resource.ID]; ok {
		if current.db != nil {
			_ = current.db.Close()
		}
	}
	c.resources[resource.ID] = &timescaledbResource{
		resource: resource,
		cfg:      cfg,
	}
	return resource.ID, nil
}

func (c *timescaledbConnector) DestroyResource(providerHandle string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil
	}
	if resource.db != nil {
		_ = resource.db.Close()
	}
	delete(c.resources, providerHandle)
	return nil
}

func (c *timescaledbConnector) Execute(providerHandle string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
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

func (c *timescaledbConnector) OpenStream(providerHandle string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	return sdk.StreamOpenResponse{}, sdk.ErrNotImplemented
}

func (c *timescaledbConnector) ReceiveStream(providerHandle string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	return sdk.StreamReceiveResponse{}, sdk.ErrNotImplemented
}

func (c *timescaledbConnector) AckStream(providerHandle string, req sdk.StreamAckRequest) error {
	return sdk.ErrNotImplemented
}

func (c *timescaledbConnector) NackStream(providerHandle string, req sdk.StreamNackRequest) error {
	return sdk.ErrNotImplemented
}

func (c *timescaledbConnector) GrantCredits(providerHandle string, req sdk.StreamGrantCreditsRequest) error {
	return sdk.ErrNotImplemented
}

func (c *timescaledbConnector) CloseStream(providerHandle string, req sdk.StreamCloseRequest) error {
	return sdk.ErrNotImplemented
}

func (c *timescaledbConnector) Probe(providerHandle string, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	resource, err := c.getResource(providerHandle)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	return c.probe(resource, req)
}

func (c *timescaledbConnector) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, resource := range c.resources {
		if resource.db != nil {
			_ = resource.db.Close()
		}
		delete(c.resources, id)
	}
	return nil
}

func (c *timescaledbConnector) getResource(providerHandle string) (*timescaledbResource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resource, ok := c.resources[providerHandle]
	if !ok {
		return nil, fmt.Errorf("resource %s not found", providerHandle)
	}
	return resource, nil
}

func (c *timescaledbConnector) openDB(resource *timescaledbResource) (*sql.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
