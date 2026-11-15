package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"regexp"
	"sync"
	"time"
)

type config struct {
	ConnectorRef string `json:"connector_ref"`
	DeviceCode   string `json:"device_code"`
	Topic        string `json:"topic"`
	GroupID      string `json:"group_id"`
	Reset        string `json:"reset"`
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

const configSchema = `{"type":"object","required":["connector_ref","device_code","topic","group_id"],"properties":{"connector_ref":{"type":"string"},"device_code":{"type":"string","maxLength":128},"topic":{"type":"string"},"group_id":{"type":"string","description":"Explicit stable group unique per intended projection; same group load-balances, different groups fan out"},"reset":{"type":"string","enum":["earliest","latest"],"default":"earliest"},"decode":{"type":"string","enum":["json","raw"],"default":"json"}},"additionalProperties":false}`

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-kafka", Name: "Kafka Group Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Bounded Kafka group consumption with offset commit only after engine handoff", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"kafka"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Reset: "earliest", Decode: "json"}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 65536 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}$`).MatchString(p.cfg.Topic) || !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`).MatchString(p.cfg.GroupID) || (p.cfg.Reset != "earliest" && p.cfg.Reset != "latest") || (p.cfg.Decode != "json" && p.cfg.Decode != "raw") {
		return sdk.FatalError(errors.New("invalid Kafka source configuration"))
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
		payload, _ := json.Marshal(map[string]any{"group_id": p.cfg.GroupID, "reset": p.cfg.Reset})
		s, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{ResourceRef: p.cfg.ConnectorRef, SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID() + "/" + sdk.NewEvent(nil, nil).ID, Target: p.cfg.Topic, Payload: payload, InitialCredits: 1, MaxBufferedMessages: 1, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
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
		return sdk.Event{}, errors.New("Kafka message exceeds 1 MiB")
	}

	data := map[string]any{"raw_base64": base64.StdEncoding.EncodeToString(m.RawPayload)}
	if p.cfg.Decode == "json" {
		v, err := decodeSafeJSON(m.RawPayload)
		if err != nil {
			return sdk.Event{}, errors.New("invalid bounded JSON Kafka message")
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
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": data, "time": at}, map[string]string{"protocol": "kafka", "topic": p.cfg.Topic, "group_id": p.cfg.GroupID, "partition": m.Metadata["partition"], "offset": m.Metadata["offset"], "key_base64": m.Key})
	if m.Topic != p.cfg.Topic || m.Metadata["partition"] == "" || m.Metadata["offset"] == "" {
		return sdk.Event{}, errors.New("Kafka topic/offset identity missing")
	}
	digest := sha256.Sum256([]byte(p.cfg.ConnectorRef + "\x00" + p.cfg.GroupID + "\x00" + m.Topic + "\x00" + m.Metadata["partition"] + "\x00" + m.Metadata["offset"]))
	event.ID = "evt_kafka_" + hex.EncodeToString(digest[:])

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
