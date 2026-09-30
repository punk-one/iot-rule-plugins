package main

import (
	"sync"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type MQTTSink struct {
	sdk.BasePlugin
	cfg           SinkMQTTConfig
	ctx           sdk.RuntimeContext
	stopCh        chan struct{}
	statusMu      sync.RWMutex
	status        sdk.ResourceStatus
	healthMu      sync.Mutex
	healthy       bool
	degraded      bool
	topicTemplate compiledTopicTemplate
}

func (s *MQTTSink) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "sink-mqtt",
		Name:        "MQTT Sink",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeSink,
		Description: "Publish rule events to MQTT through connector_ref",
		Category:    "mqtt",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  0,
			SupportAck:   true,
			ConfigSchema: mqttSinkConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"mqtt"},
				RequiredCapabilities: []string{"write"},
			},
		},
	}
}

func (s *MQTTSink) Init(cfg sdk.PluginConfig) error {
	return mqttSinkFatal(s.validateConfig(cfg.Raw))
}

func (s *MQTTSink) Start(ctx sdk.RuntimeContext) error {
	s.ctx = ctx
	s.stopCh = make(chan struct{})
	go s.watchResourceEvents()
	if status, ok := ctx.Connector().CurrentStatus(s.cfg.ConnectorRef); ok {
		s.onResourceStatus(status)
	}
	return nil
}

func (s *MQTTSink) Stop() error {
	if s.stopCh != nil {
		close(s.stopCh)
	}
	s.reportStopped()
	return nil
}
