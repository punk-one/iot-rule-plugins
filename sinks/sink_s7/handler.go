package main

import (
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type sinkWritePoint struct {
	Address   string      `json:"address"`
	Name      string      `json:"name"`
	ValueType string      `json:"value_type"`
	MaxLength int         `json:"max_length,omitempty"`
	Value     interface{} `json:"value"`
}

type sinkWritePayload struct {
	Writes    []sinkWritePoint `json:"writes"`
	BatchSize int              `json:"batch_size"`
}

func (s *S7Sink) OnEvent(e sdk.Event) error {
	if s.shouldBuffer() {
		if err := s.bufferEvent(e); err != nil {
			return err
		}
		return nil
	}
	return s.writeEvent(e)
}

func (s *S7Sink) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := s.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (s *S7Sink) writeEvent(e sdk.Event) error {
	dataField := s.cfg.Input.DataField
	raw, ok := e.Payload[dataField]
	if !ok {
		return nil
	}
	dataMap, ok := raw.(map[string]interface{})
	if !ok {
		return s7SinkSkippable(fmt.Errorf("payload.%s is not a map", dataField))
	}

	writes := make([]sinkWritePoint, 0, len(dataMap))
	for name, value := range dataMap {
		pt, ok := s.pointIndex[name]
		if !ok {
			continue
		}
		writes = append(writes, sinkWritePoint{
			Address:   pt.Address,
			Name:      pt.Name,
			ValueType: pt.ValueType,
			MaxLength: pt.MaxLength,
			Value:     value,
		})
	}
	if len(writes) == 0 {
		return nil
	}

	payload, err := json.Marshal(sinkWritePayload{
		Writes:    writes,
		BatchSize: 16,
	})
	if err != nil {
		return err
	}

	resp, err := s.ctx.Connector().BatchWrite(sdk.ConnectorRequest{
		Target:  "points",
		Payload: payload,
	})
	if err != nil {
		s.reportWriteFailure(err)
		if s.shouldBuffer() && s.cfg.Buffer.Enabled {
			if bufferErr := s.bufferEvent(e); bufferErr == nil {
				return nil
			}
		}
		return s7SinkRetryable(err)
	}
	if resp.Congested {
		s.reportWriteFailure(fmt.Errorf("connector congested"))
	}
	s.reportWriteSuccess(len(writes))
	return nil
}

func (s *S7Sink) watchResourceEvents() {
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

func (s *S7Sink) onResourceStatus(event sdk.ResourceStatusEvent) {
	s.setStatus(event.Status)
	s.reportStatus(event)
}

func (s *S7Sink) setStatus(status sdk.ResourceStatus) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.status = status
}

func (s *S7Sink) currentStatus() sdk.ResourceStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

func (s *S7Sink) shouldBuffer() bool {
	switch s.currentStatus() {
	case sdk.ResourceStatusDisconnected, sdk.ResourceStatusBlocked, sdk.ResourceStatusReadonly:
		return s.cfg.Buffer.Enabled
	default:
		return false
	}
}

func (s *S7Sink) bufferEvent(e sdk.Event) error {
	s.bufferMu.Lock()
	defer s.bufferMu.Unlock()
	if len(s.buffer) >= s.cfg.Buffer.MaxEvents {
		return s7SinkRetryable(fmt.Errorf("sink buffer is full"))
	}
	s.buffer = append(s.buffer, e)
	s.reportBuffered(len(s.buffer))
	return nil
}

func (s *S7Sink) flushLoop() {
	interval := time.Duration(s.cfg.Buffer.FlushIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.currentStatus() != sdk.ResourceStatusHealthy {
				continue
			}
			_ = s.flushBuffered()
		}
	}
}

func (s *S7Sink) flushBuffered() error {
	for {
		s.bufferMu.Lock()
		if len(s.buffer) == 0 {
			s.bufferMu.Unlock()
			return nil
		}
		event := s.buffer[0]
		s.buffer = s.buffer[1:]
		s.bufferMu.Unlock()

		if err := s.writeEvent(event); err != nil {
			s.bufferMu.Lock()
			s.buffer = append([]sdk.Event{event}, s.buffer...)
			s.bufferMu.Unlock()
			return err
		}
	}
}
