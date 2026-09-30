package main

import (
	"sync"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type S7Sink struct {
	sdk.BasePlugin
	cfg S7SinkConfig
	ctx sdk.RuntimeContext

	stopCh chan struct{}

	statusMu sync.RWMutex
	status   sdk.ResourceStatus

	bufferMu sync.Mutex
	buffer   []sdk.Event

	healthMu sync.Mutex
	healthy  bool
	degraded bool

	pointIndex map[string]SinkPointDef
}

func (s *S7Sink) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "sink-s7",
		Name:        "S7 Sink",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeSink,
		Description: "Write event payload data fields to Siemens S7 PLC through connector_ref",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  0,
			ConfigSchema: s7SinkConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"s7"},
				RequiredCapabilities: []string{"write"},
			},
		},
	}
}

func (s *S7Sink) Init(cfg sdk.PluginConfig) error {
	return s7SinkFatal(s.validateConfig(cfg.Raw))
}

func (s *S7Sink) Start(ctx sdk.RuntimeContext) error {
	s.ctx = ctx
	s.stopCh = make(chan struct{})
	s.setStatus(sdk.ResourceStatusUnknown)

	if status, ok := ctx.Connector().CurrentStatus(s.cfg.ConnectorRef); ok {
		s.onResourceStatus(status)
	}

	go s.watchResourceEvents()
	go s.flushLoop()
	return nil
}

func (s *S7Sink) Stop() error {
	if s.stopCh != nil {
		close(s.stopCh)
	}
	s.reportStopped()
	return nil
}
