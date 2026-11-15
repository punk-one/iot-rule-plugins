package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"strconv"
	"time"
)

type rawFrame struct {
	data     []byte
	sequence uint64
	at       int64
}
type rawSubscription struct {
	points  map[uint32]string
	key     string
	queue   []rawFrame
	pending map[string]bool
	credits int
	notify  chan struct{}
	err     error
}
type rawSession struct {
	done   chan struct{}
	conn   connection
	cancel context.CancelFunc
	stop   func() bool
}

func wakeRaw(s *rawSubscription) {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
func (r *resource) dropRawQueue(s *rawSubscription) {
	for _, v := range s.queue {
		r.queuedBytes -= len(v.data)
	}
	s.queue = nil
}
func (r *resource) rawDemand() bool {
	for _, s := range r.streams {
		if s.err == nil {
			return true
		}
	}
	return false
}
func (c *connector) OpenStream(h string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	r.openMu.Lock()
	defer r.openMu.Unlock()
	var spec struct {
		Points []point `json:"points"`
	}
	d := json.NewDecoder(bytes.NewReader(req.Payload))
	d.DisallowUnknownFields()
	if r.cfg.Mode != "stream" || req.Target != "events" || len(req.Payload) > 262144 || d.Decode(&spec) != nil || d.Decode(new(any)) != io.EOF || len(spec.Points) < 1 || len(spec.Points) > 1024 || req.SessionKey == "" || len(req.SessionKey) > 512 || req.InitialCredits < 1 || req.InitialCredits > 8 || req.MaxBufferedMessages != 8 || req.OverflowPolicy != "fail_session" {
		return sdk.StreamOpenResponse{}, errors.New("IEC104 stream requires bounded selection/credit<=8/queue=8/fail_session")
	}
	allowed := map[uint32]bool{}
	for _, ioa := range r.cfg.ReadIOAs {
		allowed[ioa] = true
	}
	selected := map[uint32]string{}
	names := map[string]bool{}
	for _, p := range spec.Points {
		if p.Name == "" || len(p.Name) > 128 || names[p.Name] || selected[p.IOA] != "" || !allowed[p.IOA] {
			return sdk.StreamOpenResponse{}, errors.New("IEC104 stream IOA not allowed or duplicate point")
		}
		selected[p.IOA] = p.Name
		names[p.Name] = true
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if r.ctx.Err() != nil || len(r.streams) >= 16 {
		return sdk.StreamOpenResponse{}, errors.New("IEC104 resource stopped or consumer limit")
	}
	for _, s := range r.streams {
		if s.key == req.SessionKey {
			return sdk.StreamOpenResponse{}, errors.New("duplicate raw source session")
		}
	}
	if r.active == nil {
		if time.Now().Before(r.retryAfter) {
			return sdk.StreamOpenResponse{}, errors.New("IEC104 stream backoff; no dial")
		}
		select {
		case <-ctx.Done():
			return sdk.StreamOpenResponse{}, ctx.Err()
		case r.gate <- struct{}{}:
		}
		release := true
		defer func() {
			if release {
				<-r.gate
			}
		}()
		select {
		case <-ctx.Done():
			return sdk.StreamOpenResponse{}, ctx.Err()
		case c.slots <- struct{}{}:
		}
		slotRelease := true
		defer func() {
			if slotRelease {
				<-c.slots
			}
		}()
		conn, e := dial(ctx, r.cfg)
		if e != nil {
			r.failures++
			r.retryAfter = time.Now().Add(time.Duration(1<<min(r.failures-1, 5)) * time.Second)
			return sdk.StreamOpenResponse{}, errors.New("IEC104 stream open failed")
		}
		operationStop := context.AfterFunc(ctx, func() { conn.Close() })
		deadline, _ := ctx.Deadline()
		if conn.SetDeadline(deadline) != nil || writeAll(conn, r.cfg.StreamRequest) != nil || ctx.Err() != nil {
			operationStop()
			conn.Close()
			r.failures++
			r.retryAfter = time.Now().Add(time.Duration(1<<min(r.failures-1, 5)) * time.Second)
			return sdk.StreamOpenResponse{}, errors.New("IEC104 stream initialization failed; do not retry physical commands")
		}
		if !operationStop() {
			conn.Close()
			return sdk.StreamOpenResponse{}, errors.New("IEC104 stream opening canceled")
		}
		if conn.SetDeadline(time.Time{}) != nil {
			conn.Close()
			return sdk.StreamOpenResponse{}, errors.New("IEC104 stream deadline reset unavailable")
		}
		sessionCtx, sessionCancel := context.WithCancel(r.ctx)
		s := &rawSession{conn: conn, cancel: sessionCancel, done: make(chan struct{})}
		s.stop = context.AfterFunc(sessionCtx, func() { conn.Close() })
		r.active = s
		r.opens++
		release = false
		slotRelease = false
		go c.rawLoop(r, s)
	}
	id := "raw_" + sdk.NewEvent(nil, nil).ID
	if r.streams == nil {
		r.streams = map[string]*rawSubscription{}
	}
	r.streams[id] = &rawSubscription{points: selected, key: req.SessionKey, credits: req.InitialCredits, pending: map[string]bool{}, notify: make(chan struct{}, 1)}
	return sdk.StreamOpenResponse{StreamID: id, InitialCredits: req.InitialCredits, DeliverySemantics: sdk.StreamDeliveryBestEffort}, nil
}
func (c *connector) rawLoop(r *resource, s *rawSession) {
	defer func() {
		s.cancel()
		s.stop()
		s.conn.Close()
		<-c.slots
		<-r.gate
		close(s.done)
	}()
	reader := streamReader(s.conn, r.cfg.StreamFrame)
	for {
		raw, err := readFrame(reader, r.cfg.StreamFrame, r.cfg.MaxFrame)
		r.streamMu.Lock()
		if r.active != s {
			r.streamMu.Unlock()
			return
		}
		if err != nil {
			r.active = nil
			s.cancel()
			_ = s.conn.Close()
			r.failures++
			r.retryAfter = time.Now().Add(time.Duration(1<<min(r.failures-1, 5)) * time.Second)
			for _, sub := range r.streams {
				if sub.err == nil {
					sub.err = errors.New("IEC104 stream failed/closed; best-effort frames may be lost")
					r.dropRawQueue(sub)
					wakeRaw(sub)
				}
			}
			r.streamMu.Unlock()
			return
		}
		r.failures = 0
		r.retryAfter = time.Time{}
		r.sequence++
		frame := rawFrame{data: raw, sequence: r.sequence, at: time.Now().UnixNano()}
		for _, sub := range r.streams {
			if sub.err != nil {
				continue
			}
			projected, projectionErr := projectIECFrame(raw, sub.points)
			if projectionErr != nil {
				sub.err = projectionErr
				r.dropRawQueue(sub)
				wakeRaw(sub)
				continue
			}
			if len(projected) == 0 {
				continue
			}
			frame.data = projected
			if len(sub.queue) >= 8 || r.queuedBytes+len(projected) > 4<<20 {
				sub.err = errors.New("raw consumer/resource byte budget exceeded; frames lost")
				r.dropRawQueue(sub)
				wakeRaw(sub)
				continue
			}
			sub.queue = append(sub.queue, frame)
			r.queuedBytes += len(projected)
			wakeRaw(sub)
		}
		if !r.rawDemand() {
			r.active = nil
			r.streamMu.Unlock()
			return
		}
		r.streamMu.Unlock()
	}
}
func (c *connector) ReceiveStream(h string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	if req.MaxMessages < 1 || req.MaxMessages > 8 || req.WaitTimeoutMS < 1 || req.WaitTimeoutMS > 30000 {
		return sdk.StreamReceiveResponse{}, errors.New("invalid raw receive limits")
	}
	timer := time.NewTimer(time.Duration(req.WaitTimeoutMS) * time.Millisecond)
	defer timer.Stop()
	for {
		r.streamMu.Lock()
		s := r.streams[req.StreamID]
		if s == nil {
			r.streamMu.Unlock()
			return sdk.StreamReceiveResponse{}, errors.New("IEC104 stream closed")
		}
		if s.err != nil {
			e := s.err
			r.streamMu.Unlock()
			return sdk.StreamReceiveResponse{}, e
		}
		if len(s.queue) > 0 && s.credits > 0 {
			frame := s.queue[0]
			s.queue[0] = rawFrame{}
			s.queue = s.queue[1:]
			r.queuedBytes -= len(frame.data)
			id := strconv.FormatUint(frame.sequence, 10)
			s.pending[id] = true
			s.credits--
			r.streamMu.Unlock()
			return sdk.StreamReceiveResponse{Messages: []sdk.StreamMessage{{StreamID: req.StreamID, DeliveryID: id, Sequence: frame.sequence, RawPayload: append([]byte(nil), frame.data...), PublishedAtUnixNano: frame.at, Metadata: map[string]string{"delivery": "best_effort", "frame_mode": r.cfg.StreamFrame.Mode}}}}, nil
		}
		notify := s.notify
		r.streamMu.Unlock()
		select {
		case <-r.ctx.Done():
			return sdk.StreamReceiveResponse{}, errors.New("IEC104 resource stopped")
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
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	s := r.streams[req.StreamID]
	if s == nil || s.err != nil || !s.pending[req.DeliveryID] || req.Credits != 1 {
		return errors.New("raw ACK must release one delivered frame")
	}
	delete(s.pending, req.DeliveryID)
	s.credits++
	wakeRaw(s)
	return nil
}
func (c *connector) NackStream(h string, req sdk.StreamNackRequest) error {
	if req.Requeue {
		return errors.New("best-effort raw frames cannot requeue")
	}
	return c.AckStream(h, sdk.StreamAckRequest{StreamID: req.StreamID, DeliveryID: req.DeliveryID, Credits: req.Credits})
}
func (c *connector) GrantCredits(h string, req sdk.StreamGrantCreditsRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	s := r.streams[req.StreamID]
	if s == nil || s.err != nil || req.Credits < 0 || s.credits+len(s.pending)+req.Credits > 8 {
		return errors.New("raw credit window exceeds eight")
	}
	s.credits += req.Credits
	wakeRaw(s)
	return nil
}
func (c *connector) CloseStream(h string, req sdk.StreamCloseRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	r.openMu.Lock()
	defer r.openMu.Unlock()
	r.streamMu.Lock()
	if s := r.streams[req.StreamID]; s != nil {
		s.err = errors.New("IEC104 Source stopped")
		r.dropRawQueue(s)
		wakeRaw(s)
		delete(r.streams, req.StreamID)
	}
	var closing *rawSession
	if !r.rawDemand() && r.active != nil {
		closing = r.active
		r.active = nil
		closing.cancel()
		closing.conn.Close()
	}
	r.streamMu.Unlock()
	if closing != nil {
		<-closing.done
	}
	return nil
}
