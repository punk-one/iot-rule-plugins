package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

type config struct {
	Mode         string          `json:"mode"`
	ConnectorRef string          `json:"connector_ref"`
	DeviceCode   string          `json:"device_code"`
	Request      []byte          `json:"request_base64"`
	Frame        json.RawMessage `json:"frame"`
	IntervalMS   int             `json:"interval_ms"`
	TimeoutMS    int             `json:"timeout_ms"`
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
	return sdk.PluginInfo{ID: "source-freeport", Name: "Freeport Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded raw frame polling or shared continuous best-effort stream over TCP/serial/UDP", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"tcp", "serial", "udp"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Mode: "poll", IntervalMS: 5000, TimeoutMS: 5000}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 2097152 {
		return sdk.FatalError(errors.New("freeport config exceeds 2 MiB"))
	}
	if err := d.Decode(&p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if d.Decode(new(any)) != io.EOF || (p.cfg.Mode != "poll" && p.cfg.Mode != "stream") || p.cfg.Mode == "stream" && len(p.cfg.Request) > 0 || len(p.cfg.DeviceCode) > 128 || !json.Valid(p.cfg.Frame) || len(p.cfg.Request) > 1048576 || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.IntervalMS < 50 || p.cfg.IntervalMS > 86400000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid freeport polling/stream config"))
	}
	var frame struct {
		Mode      string `json:"mode"`
		Length    int    `json:"length"`
		Delimiter []byte `json:"delimiter"`
		Offset    int    `json:"length_offset"`
		Width     int    `json:"length_bytes"`
		Endian    string `json:"length_endian"`
		Adjust    int    `json:"length_adjust"`
	}
	if json.Unmarshal(p.cfg.Frame, &frame) != nil {
		return sdk.FatalError(errors.New("framing must be an object"))
	}
	switch frame.Mode {
	case "fixed":
		if frame.Length < 1 || frame.Length > 1048576 {
			return sdk.FatalError(errors.New("invalid fixed frame length"))
		}
	case "delimiter":
		if len(frame.Delimiter) < 1 || len(frame.Delimiter) > 32 {
			return sdk.FatalError(errors.New("delimiter must contain 1..32 base64 decoded bytes"))
		}
	case "length_prefix":
		if frame.Offset < 0 || frame.Offset > 60 || (frame.Width != 1 && frame.Width != 2 && frame.Width != 4) || (frame.Endian != "big" && frame.Endian != "little") || frame.Adjust < -1048576 || frame.Adjust > 1048576 {
			return sdk.FatalError(errors.New("invalid length prefix"))
		}
	case "datagram":
	default:
		return sdk.FatalError(errors.New("unsupported framing mode"))
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
	raw, _ := json.Marshal(map[string]any{"data": p.cfg.Request, "frame": p.cfg.Frame})
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "exchange", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return err
	}
	var data struct {
		Raw  string `json:"raw_base64"`
		Time int64  `json:"received_at"`
	}
	if json.Unmarshal(response.Payload, &data) != nil {
		return errors.New("invalid raw transport response")
	}
	decoded, err := base64.StdEncoding.DecodeString(data.Raw)
	if err != nil {
		return err
	}
	values := map[string]any{"raw_base64": data.Raw}
	if utf8.Valid(decoded) {
		values["raw"] = string(decoded)
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": values, "time": data.Time}, map[string]string{"protocol": "freeport"})
	sdk.SetTraceID(&event, event.ID)
	return p.ctx.Emitter().Publish(event)
}

func (p *plugin) streamLoop() {
	defer p.wg.Done()
	for !p.stopped() {
		payload, _ := json.Marshal(map[string]any{"frame": p.cfg.Frame})
		s, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{ResourceRef: p.cfg.ConnectorRef, SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID(), Target: "frames", Payload: payload, InitialCredits: 8, MaxBufferedMessages: 8, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
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
	if len(m.RawPayload) > 1048576 || m.Metadata["delivery"] != "best_effort" || m.PublishedAtUnixNano <= 0 {
		return sdk.Event{}, errors.New("invalid raw stream frame")
	}
	values := map[string]any{"raw_base64": base64.StdEncoding.EncodeToString(m.RawPayload)}
	if utf8.Valid(m.RawPayload) {
		values["raw"] = string(m.RawPayload)
	}
	e := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": values, "quality": map[string]any{"frame": "good"}, "time": m.PublishedAtUnixNano / int64(time.Millisecond)}, map[string]string{"protocol": "freeport", "delivery": "best_effort", "sequence": strconv.FormatUint(m.Sequence, 10)})
	sdk.SetTraceID(&e, e.ID)
	return e, nil
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
		p.health(event.Payload["quality"].(map[string]any)["frame"].(string))
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
			_ = h.Healthy("FREEPORT frame reception active", nil)
		} else {
			_ = h.Degraded("FREEPORT_STREAM_QUALITY", state, nil)
		}
	}
}
