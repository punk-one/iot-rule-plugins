package main

import (
	"sync"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type PostgresSource struct {
	sdk.BasePlugin
	cfg    PostgresSourceConfig
	ctx    sdk.RuntimeContext
	stopCh chan struct{}

	statusMu sync.RWMutex
	status   sdk.ResourceStatus

	healthMu sync.Mutex
	healthy  bool
	degraded bool
}

func (s *PostgresSource) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "source-relational-postgres",
		Name:        "Source Relational PostgreSQL",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeSource,
		Description: "Poll PostgreSQL through connector_ref and emit row events",
		Category:    "database",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   0,
			OutputPorts:  1,
			ConfigSchema: postgresSourceConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"postgres"},
				RequiredCapabilities: []string{"read"},
			},
		},
	}
}

func (s *PostgresSource) Init(cfg sdk.PluginConfig) error {
	return postgresSourceFatal(s.validateConfig(cfg.Raw))
}

func (s *PostgresSource) Start(ctx sdk.RuntimeContext) error {
	s.ctx = ctx
	s.stopCh = make(chan struct{})
	s.setStatus(sdk.ResourceStatusUnknown)

	if status, ok := ctx.Connector().CurrentStatus(s.cfg.ConnectorRef); ok {
		s.onResourceStatus(status)
	}

	go s.watchResourceEvents()
	go s.pollLoop()
	return nil
}

func (s *PostgresSource) Stop() error {
	if s.stopCh != nil {
		close(s.stopCh)
	}
	s.reportStopped()
	return nil
}
