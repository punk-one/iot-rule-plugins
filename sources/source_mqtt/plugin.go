package main

import (
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type MQTTSource struct {
	sdk.BasePlugin
	cfg    MQTTSourceConfig
	ctx    sdk.RuntimeContext
	stream sdk.StreamConsumer
	stopCh chan struct{}

	streamMu sync.RWMutex
	stopOnce sync.Once
	stopErr  error
	wg       sync.WaitGroup

	statusMu sync.RWMutex
	status   sdk.ResourceStatus

	healthMu sync.Mutex
	healthy  bool
	degraded bool
}

func (m *MQTTSource) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "source-mqtt",
		Name:        "MQTT Source",
		Version:     "1.0.0",
		Description: "Read MQTT messages through connector_ref and publish decoded MQTT payload as-is",
		Type:        sdk.PluginTypeSource,
		Category:    "mqtt",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   0,
			OutputPorts:  1,
			SupportBatch: false,
			SupportAck:   false,
			Stateful:     false,
			ConfigSchema: mqttSourceConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"mqtt"},
				RequiredCapabilities: []string{"read"},
			},
		},
	}
}

func (m *MQTTSource) Init(cfg sdk.PluginConfig) error {
	return mqttSourceFatal(m.validateConfig(cfg.Raw))
}

func (m *MQTTSource) Start(ctx sdk.RuntimeContext) error {
	m.ctx = ctx
	m.stopCh = make(chan struct{})
	m.setStatus(sdk.ResourceStatusUnknown)
	if status, ok := ctx.Connector().CurrentStatus(m.cfg.ConnectorRef); ok {
		m.onResourceStatus(status)
	}
	if err := m.openStream(); err != nil {
		return err
	}
	m.wg.Add(2)
	go func() {
		defer m.wg.Done()
		m.watchResourceEvents()
	}()
	go func() {
		defer m.wg.Done()
		m.consumeLoop()
	}()
	return nil
}

func (m *MQTTSource) OnEvent(sdk.Event) error { return nil }

func (m *MQTTSource) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := m.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (m *MQTTSource) Stop() error {
	m.stopOnce.Do(func() {
		if m.stopCh != nil {
			close(m.stopCh)
		}
		m.closeCurrentStream()
		m.wg.Wait()
		m.reportStopped()
		if m.ctx != nil {
			m.stopErr = sdk.FlushHealthReporter(m.ctx.Health(), 3*time.Second)
		}
	})
	return m.stopErr
}

func (m *MQTTSource) currentStream() sdk.StreamConsumer {
	m.streamMu.RLock()
	defer m.streamMu.RUnlock()
	return m.stream
}

func (m *MQTTSource) setStream(stream sdk.StreamConsumer) {
	m.streamMu.Lock()
	m.stream = stream
	m.streamMu.Unlock()
}

func (m *MQTTSource) closeStream(stream sdk.StreamConsumer) error {
	if stream == nil {
		return nil
	}
	err := stream.Close()
	m.streamMu.Lock()
	m.stream = nil
	m.streamMu.Unlock()
	return err
}

func (m *MQTTSource) closeCurrentStream() error {
	return m.closeStream(m.currentStream())
}

func (m *MQTTSource) stopping() bool {
	select {
	case <-m.stopCh:
		return true
	default:
		return false
	}
}
