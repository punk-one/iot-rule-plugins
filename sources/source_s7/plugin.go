package main

import (
	"sync"
	"sync/atomic"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type S7Source struct {
	sdk.BasePlugin
	cfg      S7SourceConfig
	ctx      sdk.RuntimeContext
	stopCh   chan struct{}
	stopOnce sync.Once
	workers  sync.WaitGroup
	stopped  atomic.Bool
	stopErr  error

	statusMu sync.RWMutex
	status   sdk.ResourceStatus

	healthMu   sync.Mutex
	healthy    bool
	degraded   bool
	lastPollAt atomic.Int64
}

func (s *S7Source) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "source-s7",
		Name:        "S7 Source",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeSource,
		Description: "Poll multi-point Siemens S7 profiles, apply engineering-value policies, and emit telemetry events",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   0,
			OutputPorts:  1,
			ConfigSchema: s7SourceConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"s7"},
				RequiredCapabilities: []string{"read"},
			},
		},
	}
}

func (s *S7Source) Init(cfg sdk.PluginConfig) error {
	return s7SourceFatal(s.validateConfig(cfg.Raw))
}

func (s *S7Source) Start(ctx sdk.RuntimeContext) error {
	s.ctx = ctx
	s.stopCh = make(chan struct{})
	s.stopOnce = sync.Once{}
	s.workers = sync.WaitGroup{}
	s.stopped.Store(false)
	s.stopErr = nil
	s.setStatus(sdk.ResourceStatusUnknown)

	if status, ok := ctx.Connector().CurrentStatus(s.cfg.ConnectorRef); ok {
		s.onResourceStatus(status)
	}

	s.workers.Add(2)
	go func() {
		defer s.workers.Done()
		s.watchResourceEvents()
	}()
	go func() {
		defer s.workers.Done()
		s.pollLoop()
	}()
	return nil
}

func (s *S7Source) Stop() error {
	s.stopOnce.Do(func() {
		s.stopped.Store(true)
		if s.stopCh != nil {
			close(s.stopCh)
		}
		s.workers.Wait()
		if s.ctx != nil {
			if _, err := s.ctx.Connector().Read(sdk.ConnectorRequest{Target: "release_demand"}); err != nil {
				s.stopErr = err
				s.ctx.Logger().Warn("S7 Source: release demand failed", sdk.Field{Key: "error", Value: err.Error()})
			}
			s.reportStopped()
		}
	})
	return s.stopErr
}
