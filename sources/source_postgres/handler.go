package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sqlReadPayload struct {
	SQL     string        `json:"sql"`
	Args    []interface{} `json:"args,omitempty"`
	MaxRows int           `json:"max_rows,omitempty"`
}

type sqlRowsResponse struct {
	Rows []map[string]interface{} `json:"rows"`
}

func (s *PostgresSource) pollLoop() {
	backoff := 0
	for {
		delay := s.nextDelay(backoff)
		timer := time.NewTimer(delay)
		select {
		case <-s.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		congested, err := s.poll()
		if err != nil {
			s.ctx.Logger().Warn("Postgres Source: poll error", sdk.Field{Key: "error", Value: err.Error()})
			s.reportPollError(err)
		}
		if err != nil || congested {
			backoff++
			continue
		}
		backoff = 0
	}
}

func (s *PostgresSource) poll() (bool, error) {
	if s.currentStatus() == sdk.ResourceStatusDisconnected || s.currentStatus() == sdk.ResourceStatusBlocked {
		return true, fmt.Errorf("connector status is %s", s.currentStatus())
	}

	raw, err := json.Marshal(sqlReadPayload{
		SQL:     s.cfg.Query.SQL,
		Args:    s.cfg.Query.Args,
		MaxRows: s.cfg.Query.MaxRows,
	})
	if err != nil {
		return false, err
	}

	resp, err := s.ctx.Connector().Read(sdk.ConnectorRequest{
		Target:  "sql",
		Payload: raw,
	})
	if err != nil {
		return false, postgresSourceRetryable(err)
	}

	var decoded sqlRowsResponse
	if err := json.Unmarshal(resp.Payload, &decoded); err != nil {
		return resp.Congested, err
	}

	for idx, row := range decoded.Rows {
		if err := s.emitRow(row, idx, len(decoded.Rows)); err != nil {
			return resp.Congested, err
		}
	}
	s.reportHeartbeat(len(decoded.Rows))
	return resp.Congested, nil
}

func (s *PostgresSource) emitRow(row map[string]interface{}, rowIndex, rowCount int) error {
	eventTime := resolveRowTime(row[s.cfg.Output.TimeColumn])
	if eventTime.IsZero() {
		eventTime = time.Now()
	}

	deviceCode := s.cfg.Output.DeviceCode
	if col := s.cfg.Output.DeviceCodeColumn; col != "" {
		if value, ok := row[col]; ok {
			if resolved := strings.TrimSpace(fmt.Sprint(value)); resolved != "" && resolved != "<nil>" {
				deviceCode = resolved
			}
		}
	}

	data := s.buildData(row)
	payload := map[string]interface{}{
		"time": eventTime.Format(time.RFC3339Nano),
		"data": data,
	}
	if deviceCode != "" {
		payload["device_code"] = deviceCode
	}
	if s.cfg.Output.IncludeMeta {
		payload["resource_status"] = string(s.currentStatus())
		payload["row_index"] = rowIndex
		payload["row_count"] = rowCount
	}

	metadata := map[string]string{
		"source": "source-relational-postgres",
	}
	if deviceCode != "" {
		metadata["device_code"] = deviceCode
	}

	evt := sdk.NewEvent(payload, metadata)
	evt.Timestamp = eventTime
	evt.RuleID = s.ctx.RuleID()
	evt.Source = s.ctx.NodeID()
	if sdk.GetTraceID(evt) == "" {
		sdk.SetTraceID(&evt, uuid.New().String())
	}
	return s.ctx.Emitter().Publish(evt)
}

func (s *PostgresSource) buildData(row map[string]interface{}) map[string]interface{} {
	if len(s.cfg.Output.DataColumns) > 0 {
		data := make(map[string]interface{}, len(s.cfg.Output.DataColumns))
		for _, column := range s.cfg.Output.DataColumns {
			if value, ok := row[column]; ok {
				data[column] = value
			}
		}
		return data
	}

	data := make(map[string]interface{}, len(row))
	for key, value := range row {
		if key == s.cfg.Output.TimeColumn || key == s.cfg.Output.DeviceCodeColumn {
			continue
		}
		data[key] = value
	}
	if len(data) > 0 {
		return data
	}

	fallback := make(map[string]interface{}, len(row))
	for key, value := range row {
		fallback[key] = value
	}
	return fallback
}

func (s *PostgresSource) watchResourceEvents() {
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

func (s *PostgresSource) onResourceStatus(event sdk.ResourceStatusEvent) {
	s.setStatus(event.Status)
	s.reportStatus(event)
}

func (s *PostgresSource) setStatus(status sdk.ResourceStatus) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.status = status
}

func (s *PostgresSource) currentStatus() sdk.ResourceStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

func (s *PostgresSource) nextDelay(backoff int) time.Duration {
	base := time.Duration(s.cfg.Polling.IntervalMs) * time.Millisecond
	switch s.currentStatus() {
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
