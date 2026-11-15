package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type config struct {
	Interface     string       `json:"interface"`
	ReadIDs       []identifier `json:"read_ids"`
	WriteIDs      []identifier `json:"write_ids"`
	FD            bool         `json:"can_fd"`
	IncludeErrors bool         `json:"include_error_frames"`
	ReceiveOwn    bool         `json:"receive_own_messages"`
	AllowWrite    bool         `json:"allow_write"`
	AllowRTR      bool         `json:"allow_rtr_write"`
	TimeoutMS     int          `json:"timeout_ms"`
}
type canDevice interface {
	Identity() string
	Read(context.Context) (canFrame, error)
	Write(context.Context, canFrame) error
	Close() error
}
type sample struct {
	frame    canFrame
	sequence uint64
	at       int64
	drops    uint64
}
type subscription struct {
	ids                          map[string]bool
	includeErrors, includeRemote bool
	key                          string
	queue                        []sample
	credits                      int
	pending                      map[string]bool
	notify                       chan struct{}
	err                          error
}
type session struct {
	device canDevice
	ctx    context.Context
	cancel context.CancelFunc
}
type resource struct {
	cfg                                                    config
	ctx                                                    context.Context
	cancel                                                 context.CancelFunc
	mu                                                     sync.Mutex
	ioGate                                                 chan struct{}
	closed                                                 bool
	streams                                                map[string]*subscription
	readIDs, writeIDs                                      map[string]bool
	session                                                *session
	writers                                                int
	sequence, rxDropped, busErrors, opened, closedSessions uint64
	lastError                                              string
	identity                                               string
	open                                                   func(config) (canDevice, error)
	wg                                                     sync.WaitGroup
}
type connector struct {
	mu         sync.Mutex
	resources  map[string]*resource
	interfaces map[string]bool
	closed     bool
	open       func(config) (canDevice, error)
}

func NewConnector() sdk.ConnectorPlugin {
	return &connector{resources: map[string]*resource{}, interfaces: map[string]bool{}, open: openDevice}
}
func (c *connector) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "connect-can", Name: "SocketCAN Connector", Version: "1.0.0", Type: sdk.PluginTypeConnector, Description: "Linux shared CAN/CAN-FD resource with bounded best-effort streams and explicitly expiring writes", Capabilities: sdk.PluginCapabilities{ConfigSchema: configSchema, ConnectorDescriptor: &sdk.ConnectorDescriptor{Family: "can", Label: "SocketCAN", Capabilities: []string{"read", "write", "stream:best_effort", "session:demand"}, DisplayFields: []string{"interface", "can_fd"}}}}
}
func decode(raw []byte, v any) error {
	if len(raw) > 65536 {
		return errors.New("CAN request exceeds 64 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid CAN configuration/request")
	}
	return nil
}
func identifiers(list []identifier) (map[string]bool, error) {
	if len(list) > 256 {
		return nil, errors.New("CAN ID allowlist exceeds 256")
	}
	out := map[string]bool{}
	for _, item := range list {
		k, err := identifierKey(item)
		if err != nil || out[k] {
			return nil, errors.New("invalid or duplicate CAN ID")
		}
		out[k] = true
	}
	return out, nil
}
func (c *connector) CreateResource(spec sdk.ConnectorResource) (string, error) {
	cfg := config{TimeoutMS: 1000}
	if decode(spec.Connection, &cfg) != nil || spec.ID == "" || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,14}$`).MatchString(cfg.Interface) || cfg.TimeoutMS < 50 || cfg.TimeoutMS > 5000 {
		return "", errors.New("invalid SocketCAN resource")
	}
	reads, err := identifiers(cfg.ReadIDs)
	if err != nil {
		return "", err
	}
	writes, err := identifiers(cfg.WriteIDs)
	if err != nil {
		return "", err
	}
	if len(reads) == 0 && (!cfg.AllowWrite || len(writes) == 0) || cfg.AllowWrite && len(writes) == 0 {
		return "", errors.New("explicit CAN read/write ID allowlist required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &resource{cfg: cfg, ctx: ctx, cancel: cancel, ioGate: make(chan struct{}, 1), streams: map[string]*subscription{}, readIDs: reads, writeIDs: writes, open: c.open}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.resources) >= 8 || c.resources[spec.ID] != nil || c.interfaces[cfg.Interface] {
		cancel()
		return "", errors.New("connector stopped, duplicate resource/interface or resource limit")
	}
	c.resources[spec.ID] = r
	c.interfaces[cfg.Interface] = true
	return spec.ID, nil
}
func (c *connector) get(h string) (*resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.resources[h]
	if r == nil {
		return nil, errors.New("unknown CAN resource")
	}
	return r, nil
}
func signal(s *subscription) {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
func (r *resource) hasDemandLocked() bool {
	if r.writers > 0 {
		return true
	}
	for _, s := range r.streams {
		if s.err == nil {
			return true
		}
	}
	return false
}
func (r *resource) retireLocked() {
	if r.session != nil {
		s := r.session
		r.session = nil
		s.cancel()
		_ = s.device.Close()
		r.closedSessions++
	}
}

// Caller owns ioGate. The reader and writer may use one healthy raw socket
// concurrently; writes remain serial and no second resource owns this bus.
func (r *resource) ensure(ctx context.Context) (*session, error) {
	r.mu.Lock()
	if r.closed || ctx.Err() != nil {
		r.mu.Unlock()
		return nil, errors.New("CAN resource stopped")
	}
	if r.session != nil {
		s := r.session
		r.mu.Unlock()
		return s, nil
	}
	r.mu.Unlock()
	device, err := r.open(r.cfg)
	if err != nil {
		r.mu.Lock()
		r.lastError = "CAN_OPEN_FAILED"
		r.mu.Unlock()
		return nil, err
	}
	child, cancel := context.WithCancel(r.ctx)
	s := &session{device: device, ctx: child, cancel: cancel}
	r.mu.Lock()
	if r.closed || ctx.Err() != nil {
		r.mu.Unlock()
		cancel()
		device.Close()
		return nil, errors.New("CAN demand cancelled")
	}
	if device.Identity() == "" || r.identity != "" && r.identity != device.Identity() {
		r.lastError = "CAN_INTERFACE_REPLACED"
		r.mu.Unlock()
		cancel()
		device.Close()
		return nil, errors.New("CAN interface identity changed; recreate resource explicitly")
	}
	r.identity = device.Identity()
	r.session = s
	r.opened++
	r.lastError = ""
	r.wg.Add(1)
	r.mu.Unlock()
	go r.readLoop(s)
	return s, nil
}
func (r *resource) readLoop(current *session) {
	defer r.wg.Done()
	for {
		frame, err := current.device.Read(current.ctx)
		if err != nil {
			r.mu.Lock()
			if r.session == current && current.ctx.Err() == nil {
				r.lastError = "CAN_SESSION_FAILED"
				for _, s := range r.streams {
					s.err = errors.New("CAN device/session failed; best-effort frames may be lost")
					s.queue = nil
					signal(s)
				}
				r.retireLocked()
			}
			r.mu.Unlock()
			return
		}
		frame.data = append([]byte(nil), frame.data...)
		r.mu.Lock()
		if r.closed || r.session != current {
			r.mu.Unlock()
			return
		}
		r.sequence++
		r.rxDropped += uint64(frame.dropDelta)
		if frame.busError {
			r.busErrors++
			r.lastError = "CAN_BUS_ERROR_FRAME"
		} else {
			r.lastError = ""
		}
		value := sample{frame: frame, sequence: r.sequence, at: time.Now().UnixNano(), drops: r.rxDropped}
		for _, s := range r.streams {
			if s.err != nil || frame.busError && !s.includeErrors || !frame.busError && !s.ids[key(frame.id, frame.extended)] || frame.remote && !s.includeRemote {
				continue
			}
			if len(s.queue) >= 64 {
				s.queue = nil
				s.err = errors.New("CAN consumer overflow; best-effort frames lost")
				signal(s)
				continue
			}
			s.queue = append(s.queue, value)
			signal(s)
		}
		if !r.hasDemandLocked() {
			r.retireLocked()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}
func (r *resource) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	for _, s := range r.streams {
		s.err = errors.New("CAN resource stopped")
		s.queue = nil
		signal(s)
	}
	r.retireLocked()
	r.mu.Unlock()
	r.ioGate <- struct{}{}
	<-r.ioGate
	r.wg.Wait()
}
func (c *connector) DestroyResource(h string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.resources[h]; r != nil {
		r.close()
		delete(c.resources, h)
		delete(c.interfaces, r.cfg.Interface)
	}
	return nil
}
func (c *connector) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for h, r := range c.resources {
		r.close()
		delete(c.resources, h)
		delete(c.interfaces, r.cfg.Interface)
	}
	return nil
}
func (c *connector) Execute(h string, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	if req.Kind == sdk.ConnectorRequestRead && req.Target == "stats" {
		r.mu.Lock()
		defer r.mu.Unlock()
		raw, _ := json.Marshal(map[string]any{"session_active": r.session != nil, "session_opens": r.opened, "session_closes": r.closedSessions, "rx_dropped": r.rxDropped, "bus_errors": r.busErrors, "streams": len(r.streams), "last_error": r.lastError})
		return sdk.ConnectorResponse{Payload: raw}, nil
	}
	var spec struct {
		Frame     frameSpec `json:"frame"`
		ExpiresAt int64     `json:"expires_at"`
	}
	if req.Kind != sdk.ConnectorRequestWrite || req.Target != "send" || !r.cfg.AllowWrite || decode(req.Payload, &spec) != nil {
		return sdk.ConnectorResponse{}, errors.New("CAN send disabled or invalid command")
	}
	frame, err := fromSpec(spec.Frame)
	if err != nil || !r.writeIDs[key(frame.id, frame.extended)] || frame.fd && !r.cfg.FD || frame.remote && !r.cfg.AllowRTR {
		return sdk.ConnectorResponse{}, errors.New("CAN frame format/ID is not authorized")
	}
	now := time.Now()
	expiry := time.UnixMilli(spec.ExpiresAt)
	if !expiry.After(now) || expiry.After(now.Add(time.Minute)) {
		return sdk.ConnectorResponse{}, errors.New("CAN command expired or future horizon exceeds 60s")
	}
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	deadline := now.Add(time.Duration(timeout) * time.Millisecond)
	if expiry.Before(deadline) {
		deadline = expiry
	}
	ctx, cancel := context.WithDeadline(r.ctx, deadline)
	defer cancel()
	select {
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	case r.ioGate <- struct{}{}:
	}
	defer func() { <-r.ioGate }()
	r.mu.Lock()
	r.writers++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.writers--
		if !r.hasDemandLocked() {
			r.retireLocked()
		}
		r.mu.Unlock()
	}()
	if ctx.Err() != nil {
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	s, err := r.ensure(ctx)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	frame.expiresAt = spec.ExpiresAt
	if err = s.device.Write(ctx, frame); err != nil {
		return sdk.ConnectorResponse{}, errors.New("CAN write failed; outcome unknown, not retried")
	}
	return sdk.ConnectorResponse{Payload: json.RawMessage(`{"queued":true,"scope":"local_socket"}`)}, nil
}
func (c *connector) OpenStream(h string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	var spec struct {
		IDs           []identifier `json:"ids"`
		IncludeErrors bool         `json:"include_errors"`
		IncludeRemote bool         `json:"include_remote"`
	}
	if decode(req.Payload, &spec) != nil || req.Target != "frames" || req.SessionKey == "" || len(req.SessionKey) > 512 || req.InitialCredits < 1 || req.InitialCredits > 64 || req.MaxBufferedMessages != 64 || req.OverflowPolicy != "fail_session" || spec.IncludeErrors && !r.cfg.IncludeErrors {
		return sdk.StreamOpenResponse{}, errors.New("CAN stream requires explicit bounded ID selection and fail_session overflow")
	}
	ids, err := identifiers(spec.IDs)
	if err != nil || len(ids) == 0 {
		return sdk.StreamOpenResponse{}, errors.New("CAN stream ID selection required")
	}
	for id := range ids {
		if !r.readIDs[id] {
			return sdk.StreamOpenResponse{}, errors.New("CAN stream ID is not allowed by resource")
		}
	}
	s := &subscription{ids: ids, key: req.SessionKey, includeErrors: spec.IncludeErrors, includeRemote: spec.IncludeRemote, credits: req.InitialCredits, pending: map[string]bool{}, notify: make(chan struct{}, 1)}
	streamID := "can_" + sdk.NewEvent(nil, nil).ID
	r.mu.Lock()
	if r.closed || len(r.streams) >= 32 {
		r.mu.Unlock()
		return sdk.StreamOpenResponse{}, errors.New("CAN resource closed or stream limit")
	}
	for _, existing := range r.streams {
		if existing.key == req.SessionKey {
			r.mu.Unlock()
			return sdk.StreamOpenResponse{}, errors.New("duplicate CAN source session")
		}
	}
	r.streams[streamID] = s
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		c.CloseStream(h, sdk.StreamCloseRequest{StreamID: streamID})
		return sdk.StreamOpenResponse{}, ctx.Err()
	case r.ioGate <- struct{}{}:
	}
	_, err = r.ensure(ctx)
	<-r.ioGate
	if err != nil {
		c.CloseStream(h, sdk.StreamCloseRequest{StreamID: streamID})
		return sdk.StreamOpenResponse{}, err
	}
	return sdk.StreamOpenResponse{StreamID: streamID, InitialCredits: req.InitialCredits, DeliverySemantics: sdk.StreamDeliveryBestEffort}, nil
}
func (c *connector) ReceiveStream(h string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	if req.MaxMessages < 1 || req.MaxMessages > 32 || req.WaitTimeoutMS < 1 || req.WaitTimeoutMS > 30000 {
		return sdk.StreamReceiveResponse{}, errors.New("invalid CAN receive limits")
	}
	timer := time.NewTimer(time.Duration(req.WaitTimeoutMS) * time.Millisecond)
	defer timer.Stop()
	for {
		r.mu.Lock()
		s := r.streams[req.StreamID]
		if s == nil {
			r.mu.Unlock()
			return sdk.StreamReceiveResponse{}, errors.New("CAN stream closed")
		}
		if s.err != nil {
			err = s.err
			r.mu.Unlock()
			return sdk.StreamReceiveResponse{}, err
		}
		out := []sdk.StreamMessage{}
		for len(s.queue) > 0 && s.credits > 0 && len(out) < req.MaxMessages {
			item := s.queue[0]
			s.queue[0] = sample{}
			s.queue = s.queue[1:]
			s.credits--
			id := strconv.FormatUint(item.sequence, 10)
			s.pending[id] = true
			f := item.frame
			metadata := map[string]string{"can_id": "0x" + strconv.FormatUint(uint64(f.id), 16), "extended": strconv.FormatBool(f.extended), "fd": strconv.FormatBool(f.fd), "remote": strconv.FormatBool(f.remote), "rtr_length": strconv.Itoa(f.rtrLength), "raw_dlc": strconv.Itoa(f.rawDLC), "bitrate_switch": strconv.FormatBool(f.brs), "error_state": strconv.FormatBool(f.esi), "bus_error": strconv.FormatBool(f.busError), "rx_dropped_total": strconv.FormatUint(item.drops, 10), "rx_dropped_delta": strconv.FormatUint(uint64(f.dropDelta), 10)}
			out = append(out, sdk.StreamMessage{StreamID: req.StreamID, DeliveryID: id, Sequence: item.sequence, Key: key(f.id, f.extended), RawPayload: append([]byte(nil), f.data...), Metadata: metadata, PublishedAtUnixNano: item.at})
		}
		if len(out) > 0 {
			r.mu.Unlock()
			return sdk.StreamReceiveResponse{Messages: out}, nil
		}
		notify := s.notify
		r.mu.Unlock()
		select {
		case <-r.ctx.Done():
			return sdk.StreamReceiveResponse{}, errors.New("CAN resource stopped")
		case <-timer.C:
			return sdk.StreamReceiveResponse{}, nil
		case <-notify:
		}
	}
}
func (c *connector) AckStream(h string, req sdk.StreamAckRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.streams[req.StreamID]
	if s == nil || s.err != nil || !s.pending[req.DeliveryID] || req.Credits != 1 {
		return errors.New("CAN ACK must release one actually delivered frame")
	}
	delete(s.pending, req.DeliveryID)
	s.credits++
	signal(s)
	return nil
}
func (c *connector) GrantCredits(h string, req sdk.StreamGrantCreditsRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.streams[req.StreamID]
	if s == nil || s.err != nil || req.Credits < 0 || s.credits+len(s.pending)+req.Credits > 64 {
		return errors.New("CAN credit window exceeds 64")
	}
	s.credits += req.Credits
	signal(s)
	return nil
}
func (c *connector) NackStream(h string, req sdk.StreamNackRequest) error {
	if req.Requeue {
		return errors.New("CAN best-effort frames cannot be requeued")
	}
	return c.AckStream(h, sdk.StreamAckRequest{StreamID: req.StreamID, DeliveryID: req.DeliveryID, Credits: req.Credits})
}
func (c *connector) CloseStream(h string, req sdk.StreamCloseRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if s := r.streams[req.StreamID]; s != nil {
		s.err = errors.New("CAN source stopped")
		s.queue = nil
		signal(s)
		delete(r.streams, req.StreamID)
	}
	if !r.hasDemandLocked() {
		r.retireLocked()
	}
	r.mu.Unlock()
	return nil
}
func (c *connector) Probe(h string, _ sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	status, reason := sdk.ResourceStatusHealthy, "IDLE_DEMAND_MANAGED"
	if r.session != nil {
		reason = "CAN_INTERFACE_BOUND"
	}
	if r.lastError != "" {
		status = sdk.ResourceStatusDegraded
		if r.session == nil {
			status = sdk.ResourceStatusDisconnected
		}
		reason = r.lastError
	}
	return sdk.ResourceStatusEvent{ResourceID: h, Status: status, ReasonCode: reason, Message: "Socket state is not proof that an ECU received a frame", ObservedAtUnixNano: time.Now().UnixNano()}, nil
}

const configSchema = `{"type":"object","required":["interface"],"properties":{"interface":{"type":"string","maxLength":15},"read_ids":{"type":"array","maxItems":256,"items":{"type":"object","required":["id"],"properties":{"id":{"type":"string"},"extended":{"type":"boolean","default":false}},"additionalProperties":false}},"write_ids":{"type":"array","maxItems":256,"items":{"type":"object","required":["id"],"properties":{"id":{"type":"string"},"extended":{"type":"boolean","default":false}},"additionalProperties":false}},"can_fd":{"type":"boolean","default":false},"include_error_frames":{"type":"boolean","default":false},"receive_own_messages":{"type":"boolean","default":false},"allow_write":{"type":"boolean","default":false},"allow_rtr_write":{"type":"boolean","default":false},"timeout_ms":{"type":"integer","minimum":50,"maximum":5000,"default":1000}},"additionalProperties":false}`
