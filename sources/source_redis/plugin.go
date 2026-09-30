package main

import (
	"sync"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type RedisSource struct {
	sdk.BasePlugin
	cfg    RedisSourceConfig
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

func (s *RedisSource) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "source-redis",
		Name:        "Redis Source",
		Version:     "1.0.0",
		Type:        sdk.PluginTypeSource,
		Description: "Consume Redis Stream messages through connector_ref and emit decoded events",
		Category:    "redis",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   0,
			OutputPorts:  1,
			ConfigSchema: redisSourceConfigSchema,
			ConnectorBinding: &sdk.ConnectorBindingSpec{
				Required:             true,
				AcceptedFamilies:     []string{"redis"},
				RequiredCapabilities: []string{"read"},
			},
		},
	}
}

func (s *RedisSource) Init(cfg sdk.PluginConfig) error {
	return redisSourceFatal(s.validateConfig(cfg.Raw))
}

func (s *RedisSource) Start(ctx sdk.RuntimeContext) error {
	s.ctx = ctx
	s.stopCh = make(chan struct{})
	s.setStatus(sdk.ResourceStatusUnknown)
	if status, ok := ctx.Connector().CurrentStatus(s.cfg.ConnectorRef); ok {
		s.onResourceStatus(status)
	}
	if err := s.openStream(); err != nil {
		return err
	}
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.watchResourceEvents()
	}()
	go func() {
		defer s.wg.Done()
		s.consumeLoop()
	}()
	return nil
}

func (s *RedisSource) Stop() error {
	s.stopOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
		s.closeCurrentStream()
		s.wg.Wait()
		s.reportStopped()
		if s.ctx != nil {
			s.stopErr = sdk.FlushHealthReporter(s.ctx.Health(), 3*time.Second)
		}
	})
	return s.stopErr
}

func (s *RedisSource) currentStream() sdk.StreamConsumer {
	s.streamMu.RLock()
	defer s.streamMu.RUnlock()
	return s.stream
}

func (s *RedisSource) setStream(stream sdk.StreamConsumer) {
	s.streamMu.Lock()
	s.stream = stream
	s.streamMu.Unlock()
}

func (s *RedisSource) closeStream(stream sdk.StreamConsumer) error {
	if stream == nil {
		return nil
	}
	err := stream.Close()
	s.streamMu.Lock()
	s.stream = nil
	s.streamMu.Unlock()
	return err
}

func (s *RedisSource) closeCurrentStream() error {
	return s.closeStream(s.currentStream())
}

func (s *RedisSource) stopping() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}
