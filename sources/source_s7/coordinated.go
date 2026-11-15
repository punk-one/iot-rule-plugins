package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strconv"
	"time"
)

func (s *S7Source) coordinatedLoop() {
	failures := 0
	for !s.stopped.Load() {
		points := make([]transportPoint, 0, len(s.cfg.Points))
		for _, v := range s.cfg.Points {
			points = append(points, transportPoint{Address: v.Address, Name: v.Name, ValueType: v.ValueType, MaxLength: v.MaxLength})
		}
		raw, _ := json.Marshal(map[string]any{"points": points, "batch_size": s.cfg.Polling.BatchSize, "interval_ms": s.collectionInterval().Milliseconds(), "timeout_ms": s.scanTimeoutMS()})
		stream, err := s.ctx.Connector().OpenStream(sdk.StreamOpenRequest{Target: "engine.scan.points.v1", SessionKey: s.ctx.RuleID() + "/" + s.ctx.NodeID(), Payload: raw, InitialCredits: 1, MaxBufferedMessages: 1, MaxDeliverBatch: 1, OverflowPolicy: "latest_only"})
		if err == nil {
			err = s.consumeScans(stream)
			_ = stream.Close()
		}
		if s.stopped.Load() {
			return
		}
		if err != nil {
			failures++
			s.reportPollError(err)
		} else {
			failures = 0
		}
		timer := time.NewTimer(time.Duration(1<<min(max(failures-1, 0), 5)) * time.Second)
		select {
		case <-s.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (s *S7Source) consumeScans(stream sdk.StreamConsumer) error {
	for !s.stopped.Load() {
		message, ok, err := stream.Recv(time.Second)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if s.stopped.Load() {
			return nil
		}
		if duration, e := strconv.ParseFloat(message.Metadata["scan_duration_ms"], 64); e == nil {
			s.ctx.Metrics().Observe("coordinated_scan_duration_ms", duration, nil)
		}
		if dropped, e := strconv.ParseUint(message.Metadata["scan_dropped_total"], 10, 64); e == nil {
			s.ctx.Metrics().Observe("s7_source_dropped_scans_total", float64(dropped), nil)
		}
		if message.Metadata["scan_error"] != "" {
			s.reportPollError(errors.New("coordinated S7 read failed"))
			data := map[string]any{}
			quality := map[string]any{}
			for _, v := range s.cfg.Points {
				data[v.Name] = nil
				quality[v.Name] = "bad:read"
			}
			event := sdk.NewEvent(map[string]any{"device_code": s.cfg.Output.DeviceCode, "data": data, "quality": quality, "time": time.Now().UnixMilli()}, nil)
			sdk.SetTraceID(&event, event.ID)
			if err = s.ctx.Emitter().Publish(event); err != nil {
				return err
			}
		} else if _, err = s.processScanResponse(sdk.ConnectorResponse{Payload: message.RawPayload}); err != nil {
			return err
		}
		if err = stream.Ack(message.DeliveryID, 1); err != nil {
			return err
		}
	}
	return nil
}
