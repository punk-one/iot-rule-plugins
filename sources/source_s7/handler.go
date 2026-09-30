package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sourceReadPayload struct {
	Points         []transportPoint `json:"points"`
	BatchSize      int              `json:"batch_size"`
	NextDemandInMS int64            `json:"next_demand_in_ms,omitempty"`
}

// transportPoint is intentionally limited to protocol fields. Engineering-unit
// policies belong to source-s7 and are not sent to connect-s7.
type transportPoint struct {
	Address   string `json:"address"`
	Name      string `json:"name"`
	ValueType string `json:"value_type"`
	MaxLength int    `json:"max_length,omitempty"`
}

type sourceReadResponse struct {
	Values map[string]interface{} `json:"values"`
}

func (s *S7Source) pollLoop() {
	backoff := 0
	nextDue := time.Now().Add(s.nextDelay(backoff))
	var lastStartedAt time.Time
	for {
		delay := time.Until(nextDue)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		scheduledAt := nextDue
		startedAt := time.Now()
		if !lastStartedAt.IsZero() {
			s.ctx.Metrics().Observe("s7_source_actual_interval_ms", float64(startedAt.Sub(lastStartedAt).Microseconds())/1000, map[string]string{"device_code": s.cfg.Output.DeviceCode})
		}
		lastStartedAt = startedAt
		congested, err := s.poll()
		s.ctx.Metrics().Observe("s7_source_scan_duration_ms", float64(time.Since(startedAt).Microseconds())/1000, map[string]string{"device_code": s.cfg.Output.DeviceCode})
		if err != nil {
			s.ctx.Logger().Warn("S7 Source: poll error", sdk.Field{Key: "error", Value: err.Error()})
			s.reportPollError(err)
		}
		if err != nil || congested {
			backoff++
		} else {
			backoff = 0
		}

		period := s.nextDelay(backoff)
		var dropped int
		nextDue, dropped = advanceNextDue(scheduledAt, time.Now(), period)
		if dropped > 0 {
			s.ctx.Metrics().IncCounter("s7_source_dropped_scan_windows_total", map[string]string{"device_code": s.cfg.Output.DeviceCode})
			s.ctx.Metrics().Observe("s7_source_dropped_scans", float64(dropped), map[string]string{"device_code": s.cfg.Output.DeviceCode})
		}
	}
}

func advanceNextDue(scheduledAt, completedAt time.Time, period time.Duration) (time.Time, int) {
	if period <= 0 {
		period = time.Second
	}
	nextDue := scheduledAt.Add(period)
	dropped := 0
	for !nextDue.After(completedAt) {
		nextDue = nextDue.Add(period)
		dropped++
	}
	return nextDue, dropped
}

func (s *S7Source) poll() (bool, error) {
	// Demand-managed connectors may be idle or may retain a healthy session for a
	// near-term scan. A previous failure must back off, not permanently gate the
	// next real read. Blocked remains an administrative hard stop.
	if s.currentStatus() == sdk.ResourceStatusBlocked {
		return true, fmt.Errorf("connector status is %s", s.currentStatus())
	}

	transportPoints := make([]transportPoint, 0, len(s.cfg.Points))
	for _, point := range s.cfg.Points {
		transportPoints = append(transportPoints, transportPoint{
			Address: point.Address, Name: point.Name, ValueType: point.ValueType, MaxLength: point.MaxLength,
		})
	}
	payload, err := json.Marshal(sourceReadPayload{
		Points:         transportPoints,
		BatchSize:      s.cfg.Polling.BatchSize,
		NextDemandInMS: s.collectionInterval().Milliseconds(),
	})
	if err != nil {
		return false, err
	}

	resp, err := s.ctx.Connector().BatchRead(sdk.ConnectorRequest{
		Target:    "points",
		Payload:   payload,
		TimeoutMS: s.scanTimeoutMS(),
	})
	if err != nil {
		return false, s7SourceRetryable(err)
	}
	if s.stopped.Load() {
		return resp.Congested, nil
	}

	var decoded sourceReadResponse
	if err := json.Unmarshal(resp.Payload, &decoded); err != nil {
		return resp.Congested, err
	}

	values := make(map[string]interface{}, len(s.cfg.Points))
	quality := make(map[string]string)
	for _, point := range s.cfg.Points {
		if s.stopped.Load() {
			return resp.Congested, nil
		}
		rawValue, exists := decoded.Values[point.Name]
		if !exists {
			return resp.Congested, fmt.Errorf("connector response is missing point %s", point.Name)
		}
		value, keep, status, err := applyPointPolicy(rawValue, point)
		if err != nil {
			return resp.Congested, err
		}
		if status != "" {
			quality[point.Name] = status
		}
		if keep {
			values[point.Name] = value
		}
	}

	eventPayload := map[string]interface{}{
		"time":        time.Now().UnixMilli(),
		"device_code": s.cfg.Output.DeviceCode,
		"data":        values,
	}
	if s.cfg.Output.IncludeMeta {
		eventPayload["resource_status"] = string(s.currentStatus())
		if len(quality) > 0 {
			eventPayload["point_quality"] = quality
		}
	}

	evt := sdk.NewEvent(eventPayload, map[string]string{
		"source":      "source-s7",
		"device_code": s.cfg.Output.DeviceCode,
	})
	evt.RuleID = s.ctx.RuleID()
	evt.Source = s.ctx.NodeID()
	if sdk.GetTraceID(evt) == "" {
		sdk.SetTraceID(&evt, uuid.New().String())
	}
	if s.stopped.Load() {
		return resp.Congested, nil
	}
	if err := s.ctx.Emitter().Publish(evt); err != nil {
		return resp.Congested, err
	}
	s.markPollSuccess(time.Now())
	s.reportHeartbeat(len(values))
	return resp.Congested, nil
}

func (s *S7Source) scanTimeoutMS() int {
	budget := s.collectionInterval() * 4 / 5
	if budget < 50*time.Millisecond {
		budget = 50 * time.Millisecond
	}
	if budget > 30*time.Second {
		budget = 30 * time.Second
	}
	return int(budget / time.Millisecond)
}

func (s *S7Source) watchResourceEvents() {
	ch := s.ctx.ResourceEvents()
	if ch == nil {
		return
	}
	for {
		select {
		case <-s.stopCh:
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			s.onResourceStatus(event)
		}
	}
}

func (s *S7Source) onResourceStatus(event sdk.ResourceStatusEvent) {
	s.setStatus(event.Status)
	s.reportStatus(event)
}

func (s *S7Source) setStatus(status sdk.ResourceStatus) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.status = status
}

func (s *S7Source) currentStatus() sdk.ResourceStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

func (s *S7Source) nextDelay(backoff int) time.Duration {
	base := s.collectionInterval()
	status := s.currentStatus()
	switch status {
	case sdk.ResourceStatusDegraded, sdk.ResourceStatusCongested:
		base *= 2
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusBlocked:
		base *= 4
	}
	if backoff > 0 {
		base = base * time.Duration(1<<min(backoff, 4))
	}
	maxBackoff := time.Duration(s.cfg.Polling.MaxBackoffMs) * time.Millisecond
	if base > maxBackoff {
		return maxBackoff
	}
	return base
}

func (s *S7Source) collectionInterval() time.Duration {
	if s.cfg.interval > 0 {
		return s.cfg.interval
	}
	if s.cfg.Polling.IntervalMs > 0 {
		return time.Duration(s.cfg.Polling.IntervalMs) * time.Millisecond
	}
	return time.Second
}

func applyPointPolicy(value interface{}, point PointDef) (interface{}, bool, string, error) {
	transformed := value
	if point.Scale != nil && *point.Scale != 1 {
		number, ok := finiteNumber(transformed)
		if !ok {
			return nil, false, "", fmt.Errorf("point %s scale requires a numeric value", point.Name)
		}
		transformed = number * *point.Scale
	}
	if point.Precision != nil {
		number, ok := finiteNumber(transformed)
		if !ok {
			return nil, false, "", fmt.Errorf("point %s precision requires a numeric value", point.Name)
		}
		multiplier := math.Pow10(*point.Precision)
		transformed = math.Round(number*multiplier) / multiplier
	}
	if point.Range == nil {
		return transformed, true, "", nil
	}

	number, ok := finiteNumber(transformed)
	if !ok {
		return nil, false, "", fmt.Errorf("point %s range requires a numeric value", point.Name)
	}
	below := point.Range.Minimum != nil && number < *point.Range.Minimum
	above := point.Range.Maximum != nil && number > *point.Range.Maximum
	if !below && !above {
		return transformed, true, "", nil
	}

	switch point.Range.Policy {
	case rangePolicyUseDefault:
		return point.Range.DefaultValue, true, "defaulted", nil
	case rangePolicyClamp:
		if below {
			return *point.Range.Minimum, true, "clamped", nil
		}
		return *point.Range.Maximum, true, "clamped", nil
	case rangePolicyDrop:
		return nil, false, "dropped", nil
	case rangePolicyError:
		return nil, false, "", fmt.Errorf("point %s value %v is outside configured range", point.Name, transformed)
	default:
		return nil, false, "", fmt.Errorf("point %s has unsupported range policy %q", point.Name, point.Range.Policy)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
