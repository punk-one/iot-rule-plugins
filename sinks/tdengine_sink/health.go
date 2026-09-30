package main

import (
	"strconv"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (p *TDengineSink) healthDetails(extra map[string]string) map[string]string {
	details := map[string]string{
		"component":     "tdengine_sink",
		"connector_ref": p.cfg.ConnectorRef,
		"table":         p.tableTemplate,
		"write_mode":    string(p.writeMode),
	}
	if p.writeMode == writeModeBatch {
		details["batch_max_rows"] = strconv.Itoa(p.batchConfig.MaxRows)
		details["batch_max_bytes"] = strconv.Itoa(p.batchConfig.MaxBytes)
		details["flush_interval_ms"] = strconv.Itoa(p.batchConfig.FlushIntervalMS)
	}
	for key, value := range extra {
		if value != "" {
			details[key] = value
		}
	}
	return details
}

func (p *TDengineSink) reportStatus(event sdk.ResourceStatusEvent) {
	if p.ctx == nil {
		return
	}
	details := p.healthDetails(map[string]string{
		"status":      string(event.Status),
		"reason_code": event.ReasonCode,
	})
	switch event.Status {
	case sdk.ResourceStatusHealthy:
		recovering, shouldReportHealthy := p.markHealthy()
		if recovering {
			_ = p.ctx.Health().Recovering("TDENGINE_SINK_RECOVERED", event.Message, details)
		}
		if shouldReportHealthy {
			_ = p.ctx.Health().Healthy("tdengine sink connector healthy", details)
		}
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		p.markUnhealthy()
		_ = p.ctx.Health().Degraded("TDENGINE_SINK_RESOURCE_DEGRADED", event.Message, details)
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusReadonly, sdk.ResourceStatusBlocked:
		p.markUnhealthy()
		_ = p.ctx.Health().Unhealthy("TDENGINE_SINK_RESOURCE_UNAVAILABLE", event.Message, details)
	}
}

func (p *TDengineSink) reportWriteFailure(err error, writeMode string) {
	if p.ctx == nil || err == nil {
		return
	}
	p.markUnhealthy()
	_ = p.ctx.Health().Degraded("TDENGINE_SINK_WRITE_FAILED", err.Error(), p.healthDetails(map[string]string{
		"write_mode": writeMode,
	}))
}

func (p *TDengineSink) reportConnected() {
	if p.ctx == nil {
		return
	}
	recovering, shouldReportHealthy := p.markHealthy()
	details := p.healthDetails(map[string]string{"mode": "database_write"})
	if recovering {
		_ = p.ctx.Health().Recovering("TDENGINE_SINK_RECOVERED", "tdengine sink recovered", details)
	}
	if shouldReportHealthy {
		_ = p.ctx.Health().Healthy("tdengine sink connected", details)
	}
}

func (p *TDengineSink) reportWriteSuccess(count int, writeMode string) {
	if p.ctx == nil || count <= 0 {
		return
	}
	recovering, shouldReportHealthy := p.markHealthy()
	details := p.healthDetails(map[string]string{
		"write_mode":    writeMode,
		"written_count": strconv.Itoa(count),
	})
	if recovering {
		_ = p.ctx.Health().Recovering("TDENGINE_SINK_RECOVERED", "tdengine sink recovered", details)
	}
	if shouldReportHealthy {
		_ = p.ctx.Health().Healthy("tdengine sink write path ready", details)
	}
}

func (p *TDengineSink) reportHeartbeat(count int, writeMode string) {
	if p.ctx == nil || count <= 0 {
		return
	}
	_ = p.ctx.Health().Heartbeat("tdengine sink active", p.healthDetails(map[string]string{
		"write_mode":    writeMode,
		"written_count": strconv.Itoa(count),
	}))
}

func (p *TDengineSink) reportStopped() {
	if p.ctx == nil {
		return
	}
	p.healthMu.Lock()
	p.healthy = false
	p.degraded = false
	p.healthMu.Unlock()
	_ = p.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "tdengine sink stopped",
		Details: p.healthDetails(nil),
	})
}

func (p *TDengineSink) markUnhealthy() {
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	p.healthy = false
	p.degraded = true
}

func (p *TDengineSink) markHealthy() (recovering bool, shouldReportHealthy bool) {
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	recovering = p.degraded
	shouldReportHealthy = p.degraded || !p.healthy
	p.degraded = false
	p.healthy = true
	return recovering, shouldReportHealthy
}
