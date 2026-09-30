package main

import (
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type TimescaleSink struct {
	sdk.BasePlugin
	cfg            Config
	tableTemplate  string
	autoCreateCols bool
	writeMode      writeMode
	batchConfig    BatchConfig
	ctx            sdk.RuntimeContext
	tableColumns   map[string]map[string]bool
	healthMu       sync.Mutex
	healthy        bool
	degraded       bool
	bufferMu       sync.Mutex
	pendingRows    map[string][]map[string]interface{}
	pendingBytes   map[string]int
	flushTicker    *time.Ticker
	stopFlushCh    chan struct{}
	flushDoneCh    chan struct{}
}

func (p *TimescaleSink) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "sink-timeseries-timescaledb",
		Name:        "Sink Timeseries TimescaleDB",
		Version:     "1.0.0",
		Description: "Write row payloads to TimescaleDB through connector_ref",
		Type:        sdk.PluginTypeSink,
		Category:    "storage",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  0,
			SupportBatch: true,
			SupportAck:   false,
			ConfigSchema: timescaleConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"timescaledb"},
				RequiredCapabilities: []string{"write"},
			},
		},
	}
}

func (p *TimescaleSink) Init(cfg sdk.PluginConfig) error {
	return timescaleSinkFatal(p.validateConfig(cfg.Raw))
}

func (p *TimescaleSink) Start(ctx sdk.RuntimeContext) error {
	p.ctx = ctx
	go p.watchResourceEvents()
	if status, ok := ctx.Connector().CurrentStatus(p.cfg.ConnectorRef); ok {
		p.onResourceStatus(status)
	}

	if p.writeMode == writeModeBatch {
		p.stopFlushCh = make(chan struct{})
		p.flushDoneCh = make(chan struct{})
		p.flushTicker = time.NewTicker(time.Duration(p.batchConfig.FlushIntervalMS) * time.Millisecond)
		go p.flushLoop()
	}

	return nil
}

func (p *TimescaleSink) Stop() error {
	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}
	if p.stopFlushCh != nil {
		close(p.stopFlushCh)
		<-p.flushDoneCh
	}
	if err := p.flushAllBuffered(); err != nil && p.ctx != nil {
		p.ctx.Logger().Error("timescaledb sink flush on stop failed", sdk.Field{Key: "error", Value: err.Error()})
	}
	p.reportStopped()
	return nil
}
