package main

import (
	"encoding/base64"

	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

type identifier struct {
	ID       string `json:"id"`
	Extended bool   `json:"extended"`
}
type config struct {
	ConnectorRef  string       `json:"connector_ref"`
	DeviceCode    string       `json:"device_code"`
	IDs           []identifier `json:"ids"`
	IncludeErrors bool         `json:"include_errors"`
	IncludeRemote bool         `json:"include_remote"`
}

type plugin struct {
	sdk.BasePlugin
	cfg         config
	ctx         sdk.RuntimeContext
	stop        chan struct{}
	once        sync.Once
	mu          sync.Mutex
	stream      sdk.StreamConsumer
	wg          sync.WaitGroup
	failures    int
	healthState string
}

const configSchema = `{"type":"object","required":["connector_ref","device_code","ids"],"properties":{"connector_ref":{"type":"string"},"device_code":{"type":"string"},"ids":{"type":"array","minItems":1,"maxItems":256,"items":{"type":"object","required":["id"],"properties":{"id":{"type":"string"},"extended":{"type":"boolean","default":false}},"additionalProperties":false}},"include_errors":{"type":"boolean","default":false},"include_remote":{"type":"boolean","default":false}},"additionalProperties":false}`

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "source-can", Name: "CAN Frame Source", Version: "1.0.0", Type: sdk.PluginTypeSource, Description: "Shared SocketCAN frame stream with explicit best-effort/overflow quality", Capabilities: sdk.PluginCapabilities{OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"can"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 65536 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || p.cfg.DeviceCode == "" || len(p.cfg.DeviceCode) > 128 || len(p.cfg.IDs) < 1 || len(p.cfg.IDs) > 256 {
		return sdk.FatalError(errors.New("invalid CAN source configuration"))
	}
	seen := map[string]bool{}
	for _, id := range p.cfg.IDs {
		k, err := canKey(id.ID, id.Extended)
		if err != nil || seen[k] {
			return sdk.FatalError(errors.New("invalid or duplicate CAN source ID"))
		}
		seen[k] = true
	}
	return nil
}
func canKey(raw string, extended bool) (string, error) {
	if raw == "" || len(raw) > 10 || strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, "+-") {
		return "", errors.New("invalid CAN ID")
	}
	value, base := raw, 10
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		value = raw[2:]
		base = 16
	} else if len(raw) > 1 && raw[0] == '0' {
		return "", errors.New("ambiguous leading-zero CAN ID")
	}
	n, err := strconv.ParseUint(value, base, 32)
	limit := uint64(0x7ff)
	prefix := "s:"
	if extended {
		limit = 0x1fffffff
		prefix = "e:"
	}
	if err != nil || n > limit {
		return "", errors.New("CAN ID outside frame format")
	}
	return prefix + strconv.FormatUint(n, 16), nil
}

func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("bound connector and emitter required")
	}
	p.ctx = ctx
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	p.failures = 0
	p.healthState = ""
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
		payload, _ := json.Marshal(map[string]any{"ids": p.cfg.IDs, "include_errors": p.cfg.IncludeErrors, "include_remote": p.cfg.IncludeRemote})
		s, err := p.ctx.Connector().OpenStream(sdk.StreamOpenRequest{ResourceRef: p.cfg.ConnectorRef, SessionKey: p.ctx.RuleID() + "/" + p.ctx.NodeID(), Target: "frames", Payload: payload, InitialCredits: 8, MaxBufferedMessages: 64, MaxDeliverBatch: 1, OverflowPolicy: "fail_session"})
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
	if len(m.RawPayload) > 64 {
		return sdk.Event{}, errors.New("CAN data exceeds 64 bytes")
	}
	flags := map[string]bool{}
	for _, name := range []string{"extended", "fd", "remote", "bitrate_switch", "error_state", "bus_error"} {
		value, err := strconv.ParseBool(m.Metadata[name])
		if err != nil {
			return sdk.Event{}, errors.New("invalid CAN flag metadata")
		}
		flags[name] = value
	}
	if !flags["bus_error"] {
		if _, err := canKey(m.Metadata["can_id"], flags["extended"]); err != nil {
			return sdk.Event{}, err
		}
	}
	if flags["bus_error"] {
		if _, err := canKey(m.Metadata["can_id"], true); err != nil {
			return sdk.Event{}, err
		}
		if flags["fd"] || flags["remote"] || flags["extended"] || len(m.RawPayload) != 8 {
			return sdk.Event{}, errors.New("invalid CAN error frame")
		}
	} else if flags["remote"] && (flags["fd"] || len(m.RawPayload) != 0) || !flags["fd"] && len(m.RawPayload) > 8 || flags["fd"] && !validFDLength(len(m.RawPayload)) || !flags["fd"] && (flags["bitrate_switch"] || flags["error_state"]) {
		return sdk.Event{}, errors.New("invalid CAN frame length/flags")
	}
	data := map[string]any{"can_id": m.Metadata["can_id"], "raw_base64": base64.StdEncoding.EncodeToString(m.RawPayload), "length": len(m.RawPayload)}
	for name, value := range flags {

		data[name] = value
	}
	for _, name := range []string{"rtr_length", "raw_dlc"} {
		value, err := strconv.Atoi(m.Metadata[name])
		if err != nil || value < 0 || value > 15 {
			return sdk.Event{}, errors.New("invalid CAN length metadata")
		}
		if name == "rtr_length" && (value > 8 || !flags["remote"] && value != 0) {
			return sdk.Event{}, errors.New("invalid CAN RTR length")
		}
		if name == "raw_dlc" && value != 0 && (flags["fd"] || len(m.RawPayload) != 8 || value < 9) {
			return sdk.Event{}, errors.New("invalid classic raw CAN DLC")
		}
		data[name] = value
	}
	drops, err := strconv.ParseUint(m.Metadata["rx_dropped_total"], 10, 64)
	if err != nil {
		return sdk.Event{}, errors.New("invalid CAN dropped counter")
	}
	delta, err := strconv.ParseUint(m.Metadata["rx_dropped_delta"], 10, 32)
	if err != nil {
		return sdk.Event{}, errors.New("invalid CAN dropped delta")
	}
	data["rx_dropped_total"] = strconv.FormatUint(drops, 10)
	quality := "good"
	if flags["bus_error"] {
		quality = "bad:can_bus_error"
	} else if delta > 0 {
		quality = "uncertain:rx_overflow"
	}
	at := m.PublishedAtUnixNano / int64(time.Millisecond)
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	event := sdk.NewEvent(map[string]any{"device_code": p.cfg.DeviceCode, "data": data, "quality": map[string]any{"frame": quality}, "time": at}, map[string]string{"protocol": "can", "resource_ref": p.cfg.ConnectorRef, "sequence": strconv.FormatUint(m.Sequence, 10)})
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

func validFDLength(n int) bool {
	return n >= 0 && n <= 8 || n == 12 || n == 16 || n == 20 || n == 24 || n == 32 || n == 48 || n == 64
}

func (p *plugin) health(state string) {
	if p.healthState == state {
		return
	}
	p.healthState = state
	if h := p.ctx.Health(); h != nil {
		if state == "good" {
			_ = h.Healthy("CAN frame reception active", nil)
		} else {
			_ = h.Degraded("CAN_STREAM_QUALITY", state, nil)
		}
	}
}
