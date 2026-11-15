package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"strconv"
	"time"
)

// This plugin-local adapter uses SDK v1 only. Timing, merging and read fairness
// belong to the engine's resource-scoped coordinator; engineering policies stay here.
func (p *plugin) coordinatedLoop() {
	failures := 0
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		points := make([]map[string]any, 0, len(p.cfg.Points))
		for _, v := range p.cfg.Points {
			pt := map[string]any{"name": v.Name, "area": v.Area, "address": v.Address, "valueType": v.ValueType}
			if v.ByteOrder != "" {
				pt["byteOrder"] = v.ByteOrder
			}
			if v.UnitID != nil {
				pt["unit_id"] = *v.UnitID
			}
			points = append(points, pt)
		}
		raw, _ := json.Marshal(map[string]any{"points": points, "interval_ms": p.cfg.IntervalMS, "timeout_ms": p.cfg.TimeoutMS})
		stream, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{Target: "engine.scan.points.v1", SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID(), Payload: raw, InitialCredits: 1, MaxBufferedMessages: 1, MaxDeliverBatch: 1, OverflowPolicy: "latest_only"})
		if err == nil {
			err = p.consumeScans(stream)
			_ = stream.Close()
		}
		select {
		case <-p.stop:
			return
		default:
		}
		if err != nil {
			failures++
			p.ctx.Metrics().IncCounter("scan_stream_failures", nil)
		} else {
			failures = 0
		}
		timer := time.NewTimer(time.Duration(1<<min(max(failures-1, 0), 5)) * time.Second)
		select {
		case <-p.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (p *plugin) consumeScans(stream sdk.StreamConsumer) error {
	for {
		select {
		case <-p.stop:
			return nil
		default:
		}
		message, ok, err := stream.Recv(time.Second)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		select {
		case <-p.stop:
			return nil
		default:
		}
		var readErr error
		if message.Metadata["scan_error"] != "" {
			readErr = errors.New("coordinated read failed")
		}
		if duration, e := strconv.ParseFloat(message.Metadata["scan_duration_ms"], 64); e == nil {
			p.ctx.Metrics().Observe("coordinated_scan_duration_ms", duration, nil)
		}
		if dropped, e := strconv.ParseUint(message.Metadata["scan_dropped_total"], 10, 64); e == nil {
			p.ctx.Metrics().Observe("scan_dropped_total", float64(dropped), nil)
		}
		if err = p.emitScan(sdk.ConnectorResponse{Payload: message.RawPayload}, readErr, time.Now()); err != nil && readErr == nil {
			return err
		}
		// A bad read still produces a bad-quality sample, and its local credit is
		// returned only after the emitter accepted the handoff. No durable ACK implied.
		if err != nil && err != readErr {
			return err
		}
		if err = stream.Ack(message.DeliveryID, 1); err != nil {
			return err
		}
	}
}
