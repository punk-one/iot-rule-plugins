package main

import (
	"strconv"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (s *TimescaleSource) healthDetails(extra map[string]string) map[string]string {
	details := map[string]string{
		"component":     "source_timescaledb",
		"connector_ref": s.cfg.ConnectorRef,
	}
	if s.cfg.Output.DeviceCode != "" {
		details["device_code"] = s.cfg.Output.DeviceCode
	}
	for key, value := range extra {
		if value != "" {
			details[key] = value
		}
	}
	return details
}

func (s *TimescaleSource) reportPollError(err error) {
	if s.ctx == nil || err == nil {
		return
	}
	s.markUnhealthy()
	_ = s.ctx.Health().Degraded("POSTGRES_SOURCE_READ_FAILED", err.Error(), s.healthDetails(nil))
}

func (s *TimescaleSource) reportStatus(event sdk.ResourceStatusEvent) {
	if s.ctx == nil {
		return
	}
	details := s.healthDetails(map[string]string{
		"status":      string(event.Status),
		"reason_code": event.ReasonCode,
	})
	switch event.Status {
	case sdk.ResourceStatusHealthy, sdk.ResourceStatusReadonly:
		recovering, shouldReportHealthy := s.markHealthy()
		if recovering {
			_ = s.ctx.Health().Recovering("POSTGRES_SOURCE_RESOURCE_RECOVERED", event.Message, details)
		}
		if shouldReportHealthy {
			message := "timescaledb source connector healthy"
			if event.Status == sdk.ResourceStatusReadonly {
				message = "timescaledb source connector readonly"
			}
			_ = s.ctx.Health().Healthy(message, details)
		}
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		s.markUnhealthy()
		_ = s.ctx.Health().Degraded("POSTGRES_SOURCE_RESOURCE_DEGRADED", event.Message, details)
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusBlocked:
		s.markUnhealthy()
		_ = s.ctx.Health().Unhealthy("POSTGRES_SOURCE_RESOURCE_UNAVAILABLE", event.Message, details)
	}
}

func (s *TimescaleSource) reportHeartbeat(count int) {
	if s.ctx == nil {
		return
	}
	_ = s.ctx.Health().Heartbeat("timescaledb source active", s.healthDetails(map[string]string{
		"row_count": strconv.Itoa(count),
	}))
}

func (s *TimescaleSource) reportStopped() {
	if s.ctx == nil {
		return
	}
	s.resetHealthState()
	_ = s.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "timescaledb source stopped",
		Details: s.healthDetails(nil),
	})
}

func (s *TimescaleSource) markUnhealthy() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.healthy = false
	s.degraded = true
}

func (s *TimescaleSource) markHealthy() (recovering bool, shouldReportHealthy bool) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	recovering = s.degraded
	shouldReportHealthy = s.degraded || !s.healthy
	s.degraded = false
	s.healthy = true
	return recovering, shouldReportHealthy
}

func (s *TimescaleSource) resetHealthState() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.healthy = false
	s.degraded = false
}
