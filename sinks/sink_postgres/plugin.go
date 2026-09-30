package main

import (
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type PostgresSink struct {
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

func (p *PostgresSink) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "sink-relational-postgres",
		Name:        "Sink Relational PostgreSQL",
		Version:     "1.0.0",
		Description: "Write row payloads to PostgreSQL through connector_ref",
		Type:        sdk.PluginTypeSink,
		Category:    "storage",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  0,
			SupportBatch: true,
			SupportAck:   false,
			ConfigSchema: postgresSinkConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"postgres"},
				RequiredCapabilities: []string{"write"},
			},
		},
	}
}

func (p *PostgresSink) Init(cfg sdk.PluginConfig) error {
	return postgresSinkFatal(p.validateConfig(cfg.Raw))
}

func (p *PostgresSink) Start(ctx sdk.RuntimeContext) error {
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

func (p *PostgresSink) Stop() error {
	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}
	if p.stopFlushCh != nil {
		close(p.stopFlushCh)
		<-p.flushDoneCh
	}
	if err := p.flushAllBuffered(); err != nil && p.ctx != nil {
		p.ctx.Logger().Error("postgres sink flush on stop failed", sdk.Field{Key: "error", Value: err.Error()})
	}
	p.reportStopped()
	return nil
}
