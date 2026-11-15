package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"time"
)

func (p *plugin) subscriptionLoop() {
	defer p.wg.Done()
	failures := 0
	for !p.stopped() {
		target := "data_changes"
		spec := map[string]any{"points": p.cfg.Points, "interval_ms": p.cfg.IntervalMS}
		if p.cfg.Mode == "events" {
			target = "events"
			spec = map[string]any{"event_node_id": p.cfg.EventNodeID, "event_namespace_uri": p.cfg.EventNamespaceURI, "interval_ms": p.cfg.IntervalMS}
		}
		raw, _ := json.Marshal(spec)
		stream, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{ResourceRef: p.cfg.ConnectorRef, SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID(), Target: target, Payload: raw, InitialCredits: 1, MaxBufferedMessages: p.cfg.Buffer, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
		healthy := false
		if err == nil {
			p.mu.Lock()
			if p.stopped() {
				p.mu.Unlock()
				stream.Close()
				return
			}
			p.stream = stream
			p.mu.Unlock()
			healthy, err = p.consumeSubscription(stream)
			stream.Close()
			p.mu.Lock()
			p.stream = nil
			p.mu.Unlock()
		}
		if p.stopped() {
			return
		}
		if healthy {
			failures = 0
		}
		if err != nil {
			failures++
			if m := p.ctx.Metrics(); m != nil {
				m.IncCounter("subscription_failures", nil)
			}
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
func (p *plugin) stopped() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}
func (p *plugin) consumeSubscription(stream sdk.StreamConsumer) (bool, error) {
	healthy := false
	for !p.stopped() {
		message, ok, err := stream.Recv(time.Second)
		if err != nil {
			return healthy, err
		}
		if !ok {
			continue
		}
		event, err := p.subscriptionEvent(message)
		if err != nil {
			return healthy, err
		}
		if p.stopped() {
			return healthy, nil
		}
		if err = p.ctx.Emitter().Publish(event); err != nil {
			return healthy, err
		}
		if err = stream.Ack(message.DeliveryID, 1); err != nil {
			return healthy, err
		}
		healthy = true
	}
	return healthy, nil
}
func (p *plugin) subscriptionEvent(message sdk.StreamMessage) (sdk.Event, error) {
	if len(message.RawPayload) > 1048576 {
		return sdk.Event{}, errors.New("OPC UA notification exceeds 1 MiB")
	}
	var decoded struct {
		Values  map[string]any    `json:"values"`
		Quality map[string]string `json:"quality"`
		Times   map[string]int64  `json:"source_time"`
		Partial bool              `json:"partial"`
		Events  []map[string]any  `json:"events"`
	}
	if json.Unmarshal(message.RawPayload, &decoded) != nil {
		return sdk.Event{}, errors.New("invalid subscription result")
	}
	payload := map[string]any{"device_code": p.cfg.DeviceCode, "time": time.Now().UnixMilli()}
	if p.cfg.Mode == "events" {
		if decoded.Events == nil || len(decoded.Events) > 256 {
			return sdk.Event{}, errors.New("invalid event notification")
		}
		payload["data"] = map[string]any{"events": decoded.Events}
	} else {
		if decoded.Values == nil || decoded.Quality == nil || len(decoded.Values) > len(p.cfg.Points) || !decoded.Partial {
			return sdk.Event{}, errors.New("invalid data notification")
		}
		allowed := map[string]bool{}
		for _, point := range p.cfg.Points {
			allowed[point.Name] = true
		}
		for name := range decoded.Values {
			if !allowed[name] || decoded.Quality[name] == "" {
				return sdk.Event{}, errors.New("notification quality missing")
			}
		}
		payload["data"] = decoded.Values
		payload["quality"] = decoded.Quality
		payload["source_time"] = decoded.Times
		payload["partial"] = true
	}
	event := sdk.NewEvent(payload, map[string]string{"protocol": "opcua", "mode": p.cfg.Mode})
	sdk.SetTraceID(&event, event.ID)
	return event, nil
}
