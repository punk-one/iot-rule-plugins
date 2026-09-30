package main

import (
	"strconv"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const (
	redisSinkHealthCodePublishFailed = "REDIS_PUBLISH_FAILED"
)

func (s *RedisSink) healthDetails(extra map[string]string) map[string]string {
	details := map[string]string{
		"component":     "sink_redis",
		"connector_ref": s.cfg.ConnectorRef,
		"stream":        s.cfg.Target.Stream,
	}
	for key, value := range extra {
		if value != "" {
			details[key] = value
		}
	}
	return details
}

func (s *RedisSink) reportStatus(event sdk.ResourceStatusEvent) {
	if s.ctx == nil {
		return
	}
	details := s.healthDetails(map[string]string{
		"status":      string(event.Status),
		"reason_code": event.ReasonCode,
	})
	switch event.Status {
	case sdk.ResourceStatusHealthy:
		recovering, shouldReportHealthy := s.markHealthy()
		if recovering {
			_ = s.ctx.Health().Recovering("REDIS_SINK_RECOVERED", event.Message, details)
		}
		if shouldReportHealthy {
			_ = s.ctx.Health().Healthy("redis sink connector healthy", details)
		}
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		s.markUnhealthy()
		_ = s.ctx.Health().Degraded("REDIS_SINK_RESOURCE_DEGRADED", event.Message, details)
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusReadonly, sdk.ResourceStatusBlocked:
		s.markUnhealthy()
		_ = s.ctx.Health().Unhealthy("REDIS_SINK_RESOURCE_UNAVAILABLE", event.Message, details)
	}
}

func (s *RedisSink) reportPublishFailure(err error) {
	if s.ctx == nil || err == nil {
		return
	}
	s.markUnhealthy()
	_ = s.ctx.Health().Degraded(redisSinkHealthCodePublishFailed, err.Error(), s.healthDetails(nil))
}

func (s *RedisSink) reportPublishSuccess() {
	if s.ctx == nil {
		return
	}
	recovering, shouldReportHealthy := s.markHealthy()
	if recovering {
		_ = s.ctx.Health().Recovering("REDIS_SINK_RECOVERED", "redis sink recovered", s.healthDetails(nil))
	}
	if shouldReportHealthy {
		_ = s.ctx.Health().Healthy("redis sink publish path ready", s.healthDetails(nil))
	}
}

func (s *RedisSink) reportHeartbeat() {
	if s.ctx == nil {
		return
	}
	_ = s.ctx.Health().Heartbeat("redis sink active", s.healthDetails(map[string]string{
		"write_timeout_sec": strconv.Itoa(s.cfg.Options.WriteTimeoutSec),
	}))
}

func (s *RedisSink) reportStopped() {
	if s.ctx == nil {
		return
	}
	s.healthMu.Lock()
	s.healthy = false
	s.degraded = false
	s.healthMu.Unlock()
	_ = s.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "redis sink stopped",
		Details: s.healthDetails(nil),
	})
}

func (s *RedisSink) markUnhealthy() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.healthy = false
	s.degraded = true
}

func (s *RedisSink) markHealthy() (recovering bool, shouldReportHealthy bool) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	recovering = s.degraded
	shouldReportHealthy = s.degraded || !s.healthy
	s.degraded = false
	s.healthy = true
	return recovering, shouldReportHealthy
}
