package main

import (
	"strconv"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (s *S7Source) healthDetails(extra map[string]string) map[string]string {
	details := map[string]string{
		"component":     "source_s7",
		"connector_ref": s.cfg.ConnectorRef,
		"device_code":   s.cfg.Output.DeviceCode,
	}
	for key, value := range extra {
		if value != "" {
			details[key] = value
		}
	}
	return details
}

func (s *S7Source) reportPollError(err error) {
	if s.ctx == nil || err == nil {
		return
	}
	s.markUnhealthy()
	_ = s.ctx.Health().Degraded("S7_SOURCE_READ_FAILED", err.Error(), s.healthDetails(nil))
}

func (s *S7Source) reportStatus(event sdk.ResourceStatusEvent) {
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
			_ = s.ctx.Health().Recovering("S7_SOURCE_RESOURCE_RECOVERED", event.Message, details)
		}
		if shouldReportHealthy {
			_ = s.ctx.Health().Healthy("s7 source connector healthy", details)
		}
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		s.markUnhealthy()
		_ = s.ctx.Health().Degraded("S7_SOURCE_RESOURCE_DEGRADED", event.Message, details)
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusBlocked, sdk.ResourceStatusReadonly:
		s.markUnhealthy()
		_ = s.ctx.Health().Unhealthy("S7_SOURCE_RESOURCE_UNAVAILABLE", event.Message, details)
	}
}

func (s *S7Source) reportHeartbeat(count int) {
	if s.ctx == nil {
		return
	}
	_ = s.ctx.Health().Heartbeat("s7 source active", s.healthDetails(map[string]string{
		"point_count": strconv.Itoa(count),
	}))
}

func (s *S7Source) reportStopped() {
	if s.ctx == nil {
		return
	}
	s.resetHealthState()
	_ = s.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "s7 source stopped",
		Details: s.healthDetails(nil),
	})
}

func (s *S7Source) markUnhealthy() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.healthy = false
	s.degraded = true
}

func (s *S7Source) markHealthy() (recovering bool, shouldReportHealthy bool) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	recovering = s.degraded
	shouldReportHealthy = s.degraded || !s.healthy
	s.degraded = false
	s.healthy = true
	return recovering, shouldReportHealthy
}

func (s *S7Source) resetHealthState() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.healthy = false
	s.degraded = false
}

func (s *S7Source) markPollSuccess(ts time.Time) {
	s.lastPollAt.Store(ts.UnixNano())
}
