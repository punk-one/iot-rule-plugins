package main

import (
	"strconv"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (s *S7Sink) healthDetails(extra map[string]string) map[string]string {
	details := map[string]string{
		"component":     "sink_s7",
		"connector_ref": s.cfg.ConnectorRef,
		"data_field":    s.cfg.Input.DataField,
	}
	for key, value := range extra {
		if value != "" {
			details[key] = value
		}
	}
	return details
}

func (s *S7Sink) reportStatus(event sdk.ResourceStatusEvent) {
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
			_ = s.ctx.Health().Recovering("S7_SINK_RESOURCE_RECOVERED", event.Message, details)
		}
		if shouldReportHealthy {
			_ = s.ctx.Health().Healthy("s7 sink connector healthy", details)
		}
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		s.markUnhealthy()
		_ = s.ctx.Health().Degraded("S7_SINK_RESOURCE_DEGRADED", event.Message, details)
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusBlocked, sdk.ResourceStatusReadonly:
		s.markUnhealthy()
		_ = s.ctx.Health().Unhealthy("S7_SINK_RESOURCE_UNAVAILABLE", event.Message, details)
	}
}

func (s *S7Sink) reportWriteFailure(err error) {
	if s.ctx == nil || err == nil {
		return
	}
	s.markUnhealthy()
	_ = s.ctx.Health().Degraded("S7_SINK_WRITE_FAILED", err.Error(), s.healthDetails(nil))
}

func (s *S7Sink) reportBuffered(count int) {
	if s.ctx == nil {
		return
	}
	_ = s.ctx.Health().Heartbeat("s7 sink buffered event", s.healthDetails(map[string]string{
		"buffered_count": strconv.Itoa(count),
	}))
}

func (s *S7Sink) reportWriteSuccess(count int) {
	if s.ctx == nil {
		return
	}
	recovering, shouldReportHealthy := s.markHealthy()
	details := s.healthDetails(map[string]string{
		"written_count": strconv.Itoa(count),
	})
	if recovering {
		_ = s.ctx.Health().Recovering("S7_SINK_RECOVERED", "s7 sink recovered", details)
	}
	if shouldReportHealthy {
		_ = s.ctx.Health().Healthy("s7 sink write path ready", details)
	}
}

func (s *S7Sink) reportStopped() {
	if s.ctx == nil {
		return
	}
	s.healthMu.Lock()
	s.healthy = false
	s.degraded = false
	s.healthMu.Unlock()
	_ = s.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "s7 sink stopped",
		Details: s.healthDetails(nil),
	})
}

func (s *S7Sink) markUnhealthy() {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	s.healthy = false
	s.degraded = true
}

func (s *S7Sink) markHealthy() (recovering bool, shouldReportHealthy bool) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	recovering = s.degraded
	shouldReportHealthy = s.degraded || !s.healthy
	s.degraded = false
	s.healthy = true
	return recovering, shouldReportHealthy
}
