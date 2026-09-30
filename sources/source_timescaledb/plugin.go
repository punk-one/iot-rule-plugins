package main

import (
	"sync"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type TimescaleSource struct {
	sdk.BasePlugin
	cfg    TimescaleSourceConfig
	ctx    sdk.RuntimeContext
	stopCh chan struct{}

	statusMu sync.RWMutex
	status   sdk.ResourceStatus

	healthMu sync.Mutex
	healthy  bool
	degraded bool
}

func (s *TimescaleSource) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "source-timeseries-timescaledb",
		Name:        "Source Timeseries TimescaleDB",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeSource,
		Description: "Poll TimescaleDB through connector_ref and emit row events",
		Category:    "database",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   0,
			OutputPorts:  1,
			ConfigSchema: timescaleSourceConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"timescaledb"},
				RequiredCapabilities: []string{"read"},
			},
		},
	}
}

func (s *TimescaleSource) Init(cfg sdk.PluginConfig) error {
	return timescaleSourceFatal(s.validateConfig(cfg.Raw))
}

func (s *TimescaleSource) Start(ctx sdk.RuntimeContext) error {
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

func (s *TimescaleSource) Stop() error {
	if s.stopCh != nil {
		close(s.stopCh)
	}
	s.reportStopped()
	return nil
}
