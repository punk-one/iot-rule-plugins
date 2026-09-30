package main

import (
	"strconv"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (s *RedisSource) healthDetails(extra map[string]string) map[string]string {
	details := map[string]string{
		"component":     "source_redis",
		"connector_ref": s.cfg.ConnectorRef,
		"stream":        s.cfg.Stream.Name,
	}
	for key, value := range extra {
		if value != "" {
			details[key] = value
		}
	}
	return details
}

func (s *RedisSource) reportReadError(err error) {
	if s.ctx == nil || err == nil {
		return
	}
	s.markUnhealthy()
	_ = s.ctx.Health().Degraded("REDIS_SOURCE_READ_FAILED", err.Error(), s.healthDetails(nil))
}

func (s *RedisSource) reportStatus(event sdk.ResourceStatusEvent) {
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
			_ = s.ctx.Health().Recovering("REDIS_SOURCE_RECOVERED", event.Message, details)
		}
		if shouldReportHealthy {
			_ = s.ctx.Health().Healthy("redis source connector healthy", details)
		}
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		s.markUnhealthy()
		_ = s.ctx.Health().Degraded("REDIS_SOURCE_RESOURCE_DEGRADED", event.Message, details)
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusReadonly, sdk.ResourceStatusBlocked:
		s.markUnhealthy()
		_ = s.ctx.Health().Unhealthy("REDIS_SOURCE_RESOURCE_UNAVAILABLE", event.Message, details)
	}
}

func (s *RedisSource) reportHeartbeat(count int) {
	if s.ctx == nil {
		return
	}
	_ = s.ctx.Health().Heartbeat("redis source active", s.healthDetails(map[string]string{
		"message_count": strconv.Itoa(count),
	}))
}

func (s *RedisSource) reportStopped() {
	if s.ctx == nil {
		return
	}
	s.healthMu.Lock()
	s.healthy = false
	s.degraded = false
	s.healthMu.Unlock()
	_ = s.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "redis source stopped",
		Details: s.healthDetails(nil),
	})
}

func (s *RedisSource) markUnhealthy() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.healthy = false
	s.degraded = true
}

func (s *RedisSource) markHealthy() (recovering bool, shouldReportHealthy bool) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	recovering = s.degraded
	shouldReportHealthy = s.degraded || !s.healthy
	s.degraded = false
	s.healthy = true
	return recovering, shouldReportHealthy
}
