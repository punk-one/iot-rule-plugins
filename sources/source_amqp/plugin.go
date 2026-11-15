package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"sync"
	"time"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	DeviceCode   string `json:"device_code"`
	Queue        string `json:"queue"`
	Prefetch     int    `json:"prefetch"`
	Decode       string `json:"decode"`
}
type plugin struct {
	sdk.BasePlugin
	cfg      config
	ctx      sdk.RuntimeContext
	stop     chan struct{}
	once     sync.Once
	mu       sync.Mutex
	stream   sdk.StreamConsumer
	wg       sync.WaitGroup
	failures int
}

const configSchema = `{"type":"object","required":["connector_ref","device_code","queue"],"properties":{"connector_ref":{"type":"string","minLength":1},"device_code":{"type":"string","minLength":1,"maxLength":128},"queue":{"type":"string","minLength":1,"maxLength":255},"prefetch":{"type":"integer","minimum":1,"maximum":8,"default":1},"decode":{"type":"string","enum":["json","raw"],"default":"json"}},"additionalProperties":false}`

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-amqp", Name: "AMQP Queue Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded manual-ack AMQP queue consumption; acknowledgement after engine handoff", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"amqp"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Prefetch: 1, Decode: "json"}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || p.cfg.Queue == "" || len(p.cfg.Queue) > 255 || p.cfg.Prefetch < 1 || p.cfg.Prefetch > 8 || (p.cfg.Decode != "json" && p.cfg.Decode != "raw") {
		return sdk.FatalError(errors.New("invalid AMQP source settings"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("bound connector and emitter required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.failures = 0
	p.wg.Add(1)
	go p.loop()
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
func (p *plugin) loop() {
	defer p.wg.Done()
	for !p.stopped() {
		s, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{ResourceRef: p.cfg.ConnectorRef, SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID(), Target: p.cfg.Queue, InitialCredits: 1, MaxBufferedMessages: p.cfg.Prefetch, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
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
	if len(m.RawPayload) > 1048576 {
		return sdk.Event{}, errors.New("AMQP message exceeds 1 MiB")
	}

	data := map[string]any{"raw_base64": base64.StdEncoding.EncodeToString(m.RawPayload)}
	if p.cfg.Decode == "json" {
		v, err := decodeSafeJSON(m.RawPayload)
		if err != nil {
			return sdk.Event{}, errors.New("invalid bounded JSON AMQP message")
		}
		if object, ok := v.(map[string]any); ok {
			data = object
		} else {
			data = map[string]any{"value": v}
		}
	}
	at := m.PublishedAtUnixNano / int64(time.Millisecond)
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": data, "time": at}, map[string]string{"protocol": "amqp", "queue": p.cfg.Queue, "redelivered": m.Metadata["redelivered"]})
	if id := m.Metadata["message_id"]; id != "" {
		digest := sha256.Sum256([]byte(p.cfg.ConnectorRef + "\x00" + p.cfg.Queue + "\x00" + id))
		event.ID = "evt_amqp_" + hex.EncodeToString(digest[:])
		event.Metadata["amqp_message_id"] = id
	}
	sdk.SetTraceID(&event, event.ID)
	return event, nil
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
