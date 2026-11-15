package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync"
	"time"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	DeviceCode   string `json:"device_code"`
	Buffer       int    `json:"buffer_messages"`
}
type plugin struct {
	sdk.BasePlugin
	cfg    config
	ctx    sdk.RuntimeContext
	stop   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	stream sdk.StreamConsumer
	wg     sync.WaitGroup
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-websocket", Name: "WebSocket Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded best-effort WebSocket frame source", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"websocket"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Buffer: 16}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || p.cfg.Buffer < 1 || p.cfg.Buffer > 64 {
		return sdk.FatalError(errors.New("invalid WebSocket source settings"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound connector required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.wg.Add(1)
	go p.loop()
	return nil
}
func (p *plugin) Stop() error {
	p.once.Do(func() {
		if p.stop != nil {
			close(p.stop)
		}
		p.mu.Lock()
		s := p.stream
		p.stream = nil
		p.mu.Unlock()
		if s != nil {
			s.Close()
		}
	})
	p.wg.Wait()
	return nil
}
func (p *plugin) stopped() bool {
	select {
	case <-p.stop:
		return true
	default:
		return false
	}
}
func (p *plugin) loop() {
	defer p.wg.Done()
	failures := 0
	for !p.stopped() {
		s, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{ResourceRef: p.cfg.ConnectorRef, SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID(), Target: "messages", InitialCredits: 1, MaxBufferedMessages: p.cfg.Buffer, OverflowPolicy: "fail_session", MaxDeliverBatch: 1})
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
			failures++
			p.ctx.Metrics().IncCounter("stream_failures", nil)
		}
		timer := time.NewTimer(time.Duration(1<<min(failures, 5)) * time.Second)
		select {
		case <-p.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
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
		data := map[string]any{"raw_base64": base64.StdEncoding.EncodeToString(message.RawPayload), "frame_type": message.Metadata["frame_type"]}
		if message.Metadata["frame_type"] == "text" {
			data["raw"] = string(message.RawPayload)
		}
		event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": data, "time": message.PublishedAtUnixNano / int64(time.Millisecond)}, map[string]string{"protocol": "websocket"})
		sdk.SetTraceID(&event, event.ID)
		if err = p.ctx.Emitter().Publish(event); err != nil {
			return err
		}
		if err = s.Ack(message.DeliveryID, 1); err != nil {
			return err
		}
	}
	return nil
}
