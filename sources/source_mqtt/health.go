package main

import (
	"strconv"
	"strings"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

func (m *MQTTSource) healthDetails(extra map[string]string) map[string]string {
	details := map[string]string{
		"component":     "source_mqtt",
		"connector_ref": m.cfg.ConnectorRef,
	}
	for key, value := range extra {
		if strings.TrimSpace(value) != "" {
			details[key] = value
		}
	}
	return details
}

func (m *MQTTSource) reportPollError(err error) {
	if m.ctx == nil || err == nil {
		return
	}
	m.markUnhealthy()
	_ = m.ctx.Health().Degraded(mqttSourceHealthCodeReadFailed, err.Error(), m.healthDetails(nil))
}

func (m *MQTTSource) reportStatus(event sdk.ResourceStatusEvent) {
	if m.ctx == nil {
		return
	}
	details := m.healthDetails(map[string]string{
		"status":      string(event.Status),
		"reason_code": event.ReasonCode,
	})
	switch event.Status {
	case sdk.ResourceStatusHealthy:
		recovering, shouldReportHealthy := m.markHealthy()
		if recovering {
			_ = m.ctx.Health().Recovering("MQTT_SOURCE_RESOURCE_RECOVERED", event.Message, details)
		}
		if shouldReportHealthy {
			_ = m.ctx.Health().Healthy("mqtt source connector healthy", details)
		}
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		m.markUnhealthy()
		_ = m.ctx.Health().Degraded("MQTT_SOURCE_RESOURCE_DEGRADED", event.Message, details)
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusBlocked, sdk.ResourceStatusReadonly:
		m.markUnhealthy()
		_ = m.ctx.Health().Unhealthy("MQTT_SOURCE_RESOURCE_UNAVAILABLE", event.Message, details)
	}
}

func (m *MQTTSource) reportHeartbeat(count int) {
	if m.ctx == nil {
		return
	}
	_ = m.ctx.Health().Heartbeat("mqtt source active", m.healthDetails(map[string]string{
		"message_count": strconv.Itoa(count),
	}))
}

func (m *MQTTSource) reportStopped() {
	if m.ctx == nil {
		return
	}
	m.resetHealthState()
	_ = m.ctx.Health().Report(sdk.HealthReport{
		Status:  sdk.HealthStopped,
		Message: "mqtt source stopped",
		Details: m.healthDetails(nil),
	})
}

func (m *MQTTSource) markUnhealthy() {
	m.healthMu.Lock()
	defer m.healthMu.Unlock()
	m.healthy = false
	m.degraded = true
}

func (m *MQTTSource) markHealthy() (recovering bool, shouldReportHealthy bool) {
	m.healthMu.Lock()
	defer m.healthMu.Unlock()
	recovering = m.degraded
	shouldReportHealthy = m.degraded || !m.healthy
	m.degraded = false
	m.healthy = true
	return recovering, shouldReportHealthy
}

func (m *MQTTSource) resetHealthState() {
	m.healthMu.Lock()
	defer m.healthMu.Unlock()
	m.healthy = false
	m.degraded = false
}
