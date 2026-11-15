package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"strconv"
	"sync"
	"time"
)

type point struct {
	Name string `json:"name"`
	IOA  uint32 `json:"ioa"`
}
type config struct {
	Mode         string  `json:"mode"`
	ConnectorRef string  `json:"connector_ref"`
	DeviceCode   string  `json:"device_code"`
	Points       []point `json:"points"`
	IntervalMS   int     `json:"interval_ms"`
	TimeoutMS    int     `json:"timeout_ms"`
}
type plugin struct {
	mu          sync.Mutex
	stream      sdk.StreamConsumer
	failures    int
	healthState string
	sdk.BasePlugin
	cfg  config
	ctx  sdk.RuntimeContext
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-iec104", Name: "IEC104 Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Read-only interrogation polling or best-effort continuous IEC104 updates", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"iec104"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Mode: "poll", IntervalMS: 60000, TimeoutMS: 5000}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 262144 {
		return sdk.FatalError(errors.New("IEC104 source config exceeds 256 KiB"))
	}
	if err := d.Decode(&p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if d.Decode(new(any)) != io.EOF || p.cfg.Mode != "poll" && p.cfg.Mode != "stream" || len(p.cfg.DeviceCode) > 128 || len(p.cfg.Points) < 1 || len(p.cfg.Points) > 1024 || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.IntervalMS < 1000 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid IEC104 polling/stream config"))
	}
	seen := map[string]bool{}
	addresses := map[uint32]bool{}
	for _, point := range p.cfg.Points {
		if point.Name == "" || len(point.Name) > 128 || seen[point.Name] || point.IOA > 0xffffff || addresses[point.IOA] {
			return sdk.FatalError(errors.New("invalid or duplicate IEC104 point"))
		}
		seen[point.Name] = true
		addresses[point.IOA] = true
	}

	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound connector required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.once = sync.Once{}

	p.wg.Add(1)
	if p.cfg.Mode == "stream" {
		go p.streamLoop()
	} else {
		go p.loop()
	}
	return nil
}
func (p *plugin) Stop() error {
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
	})
	p.mu.Lock()
	active := p.stream
	p.mu.Unlock()
	if active != nil {
		active.Close()
	}
	p.wg.Wait()
	return nil
}
func (p *plugin) loop() {
	defer p.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	interval := time.Duration(p.cfg.IntervalMS) * time.Millisecond
	next := time.Now()
	failures := 0
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		if err := p.poll(); err != nil {
			failures++
			p.ctx.Metrics().IncCounter("poll_failures", nil)
		} else {
			failures = 0
		}
		next = next.Add(interval)
		now := time.Now()
		if !next.After(now) {
			skipped := now.Sub(next)/interval + 1
			next = next.Add(skipped * interval)
			p.ctx.Metrics().Observe("dropped_scans", float64(skipped), nil)
		}
		if failures > 0 {
			retry := now.Add(time.Duration(1<<min(failures-1, 5)) * time.Second)
			if next.Before(retry) {
				next = retry
			}
		}
		timer.Reset(time.Until(next))
	}
}
func (p *plugin) poll() error {
	raw, _ := json.Marshal(map[string]any{"points": p.cfg.Points})
	response, readErr := p.ctx.Connector().BatchRead(sdk.ConnectorRequest{Target: "queries", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	var result struct {
		Values  map[string]any `json:"values"`
		Quality map[string]any `json:"quality"`
		Details map[string]any `json:"details"`
	}
	if readErr == nil {
		if len(response.Payload) > 262144 {
			readErr = errors.New("IEC104 poll response exceeds 256 KiB")
		} else {
			readErr = json.Unmarshal(response.Payload, &result)
			if readErr == nil && (result.Values == nil || result.Quality == nil) {
				readErr = errors.New("missing IEC104 values/quality")
			}
		}
	}
	data := map[string]any{}
	quality := map[string]any{}
	for _, point := range p.cfg.Points {
		if readErr != nil {
			data[point.Name] = nil
			quality[point.Name] = "bad:read"
		} else {
			value, exists := result.Values[point.Name]
			q, valid := result.Quality[point.Name].(string)
			if !exists || !valid {
				data[point.Name] = nil
				quality[point.Name] = "bad:missing"
			} else {
				data[point.Name] = value
				quality[point.Name] = q
			}
		}
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": data, "quality": quality, "protocol_details": result.Details, "time": time.Now().UnixMilli()}, map[string]string{"protocol": "iec104"})
	sdk.SetTraceID(&event, event.ID)
	if err := p.ctx.Emitter().Publish(event); err != nil {
		return err
	}
	return readErr
}

func (p *plugin) streamLoop() {
	defer p.wg.Done()
	for !p.stopped() {
		payload, _ := json.Marshal(map[string]any{"points": p.cfg.Points})
		s, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{ResourceRef: p.cfg.ConnectorRef, SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID(), Target: "events", Payload: payload, InitialCredits: 8, MaxBufferedMessages: 8, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
		if err == nil {
			p.mu.Lock()
			if p.stopped() {
				p.mu.Unlock()
				s.Close()
				return
			}
			p.stream = s
			p.mu.Unlock()
			err = p.consume(s)
			s.Close()
			p.mu.Lock()
			p.stream = nil
			p.mu.Unlock()
		}
		if p.stopped() {
			return
		}
		if err != nil {
			p.failures++
			p.health("degraded:stream")
			if m := p.ctx.Metrics(); m != nil {
				m.IncCounter("stream_failures", nil)
			}
		}
		timer := time.NewTimer(time.Duration(1<<min(max(p.failures-1, 0), 5)) * time.Second)
		select {
		case <-p.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (p *plugin) event(m sdk.StreamMessage) (sdk.Event, error) {
	var decoded struct {
		Values   map[string]any `json:"values"`
		Quality  map[string]any `json:"quality"`
		Details  map[string]any `json:"details"`
		Cause    int            `json:"cause"`
		Received int64          `json:"received_at"`
	}
	if len(m.RawPayload) > 65536 || json.Unmarshal(m.RawPayload, &decoded) != nil || len(decoded.Values) < 1 || decoded.Received <= 0 || m.Metadata["delivery"] != "best_effort" {
		return sdk.Event{}, errors.New("invalid IEC104 monitoring update")
	}
	selected := map[string]bool{}
	for _, p := range p.cfg.Points {
		selected[p.Name] = true
	}
	for name := range decoded.Values {
		q, ok := decoded.Quality[name].(string)
		if !selected[name] || !ok || (q != "good" && q != "bad:device_quality" && q != "bad:timestamp" && q != "uncertain:transient" && q != "uncertain:counter_adjusted") {
			return sdk.Event{}, errors.New("unexpected IEC104 point/quality")
		}
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": decoded.Values, "quality": decoded.Quality, "protocol_details": map[string]any{"points": decoded.Details, "cause": decoded.Cause, "delivery": "best_effort", "snapshot": false}, "time": decoded.Received}, map[string]string{"protocol": "iec104", "delivery": "best_effort", "sequence": strconv.FormatUint(m.Sequence, 10)})
	sdk.SetTraceID(&event, event.ID)
	return event, nil
}
func (p *plugin) stopped() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}
func (p *plugin) consume(s sdk.StreamConsumer) error {
	for !p.stopped() {
		message, ok, err := s.Recv(time.Second)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		event, err := p.event(message)
		if err != nil {
			return err
		}
		if p.stopped() {
			return nil
		}
		state := "good"
		for _, q := range event.Payload["quality"].(map[string]any) {
			if q != "good" {
				state = "degraded:device_quality"
				break
			}
		}
		p.health(state)
		if err = p.ctx.Emitter().Publish(event); err != nil {
			return err
		}
		if err = s.Ack(message.DeliveryID, 1); err != nil {
			return err
		}
		p.failures = 0
	}
	return nil
}
func (p *plugin) health(state string) {
	if p.healthState == state {
		return
	}
	p.healthState = state
	if h := p.ctx.Health(); h != nil {
		if state == "good" {
			_ = h.Healthy("IEC104 frame reception active", nil)
		} else {
			_ = h.Degraded("IEC104_STREAM_QUALITY", state, nil)
		}
	}
}
