package main

import (
	"strings"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const (
	detectChangeHealthCodeStateSaveFailed = "DETECT_CHANGE_STATE_SAVE_FAILED"
)

func (p *ChangeDetectPlugin) healthDetails(extra map[string]string) map[string]string {
	fields := p.cfg.detectFields()
	field := p.cfg.Process.Detect.Field
	if field == "" && len(fields) > 0 {
		field = fields[0]
	}
	details := map[string]string{
		"component": "detect_change",
		"mode":      p.cfg.Process.Detect.Type,
		"field":     field,
	}
	if len(fields) > 1 {
		details["fields"] = strings.Join(fields, ",")
	}
	for key, value := range extra {
		if value == "" {
			continue
		}
		details[key] = value
	}
	return details
}

func (p *ChangeDetectPlugin) reportStarted() {
	if p.ctx == nil {
		return
	}
	_ = p.ctx.Health().Healthy("detect change ready", p.healthDetails(nil))
}

func (p *ChangeDetectPlugin) reportStateFailure(err error) {
	if p.ctx == nil || err == nil {
		return
	}
	_ = p.ctx.Health().Degraded(detectChangeHealthCodeStateSaveFailed, err.Error(), p.healthDetails(nil))
}

func (p *ChangeDetectPlugin) reportHeartbeat() {
	if p.ctx == nil {
		return
	}
	_ = p.ctx.Health().Heartbeat("detect change active", p.healthDetails(nil))
}

func (p *ChangeDetectPlugin) reportStopped() {
	if p.ctx == nil {
		return
	}
	_ = p.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "detect change stopped",
		Details: p.healthDetails(nil),
	})
}
