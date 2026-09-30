package main

import (
	"sync"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type RedisSink struct {
	sdk.BasePlugin
	cfg      RedisSinkConfig
	ctx      sdk.RuntimeContext
	stopCh   chan struct{}
	statusMu sync.RWMutex
	status   sdk.ResourceStatus
	healthMu sync.Mutex
	healthy  bool
	degraded bool
}

func (s *RedisSink) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "sink-redis",
		Name:        "Redis Sink",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeSink,
		Description: "Write events to Redis Stream or Redis Hash through connector_ref",
		Category:    "redis",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  0,
			SupportAck:   true,
			ConfigSchema: redisSinkConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"redis"},
				RequiredCapabilities: []string{"write"},
			},
		},
	}
}

func (s *RedisSink) Init(cfg sdk.PluginConfig) error {
	return redisSinkFatal(s.validateConfig(cfg.Raw))
}

func (s *RedisSink) Start(ctx sdk.RuntimeContext) error {
	s.ctx = ctx
	s.stopCh = make(chan struct{})
	go s.watchResourceEvents()
	if status, ok := ctx.Connector().CurrentStatus(s.cfg.ConnectorRef); ok {
		s.onResourceStatus(status)
	}
	return nil
}

func (s *RedisSink) Stop() error {
	if s.stopCh != nil {
		close(s.stopCh)
	}
	s.reportStopped()
	return nil
}
