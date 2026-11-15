package main

import (
	"context"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	amqp "github.com/rabbitmq/amqp091-go"
	"strconv"
	"time"
)

func (c *connector) OpenStream(h string, req sdk.StreamOpenRequest) (sdk.StreamOpenResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	if req.SessionKey == "" || len(req.SessionKey) > 512 || req.Target == "" || len(req.Target) > 255 || req.MaxBufferedMessages < 1 || req.MaxBufferedMessages > 8 || req.InitialCredits < 1 || req.InitialCredits > req.MaxBufferedMessages || req.OverflowPolicy != "fail_session" || len(req.Payload) > 0 {
		return sdk.StreamOpenResponse{}, errors.New("queue, session key and 1..8 bounded credits/prefetch required")
	}
	ctx, done, err := r.begin(0)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	defer done()
	budget := req.MaxBufferedMessages * r.cfg.MaxMessageBytes
	r.mu.Lock()
	if _, exists := r.reserved[req.SessionKey]; exists || len(r.reserved) >= 16 || r.budget+budget > 8388608 {
		r.mu.Unlock()
		return sdk.StreamOpenResponse{}, errors.New("consumer duplicate, count or 8 MiB reservation limit")
	}
	r.reserved[req.SessionKey] = budget
	r.budget += budget
	r.mu.Unlock()
	keep := false
	defer func() {
		if !keep {
			r.mu.Lock()
			delete(r.reserved, req.SessionKey)
			r.budget -= budget
			r.mu.Unlock()
		}
	}()
	conn, raw, err := r.ensure(ctx)
	if err != nil {
		return sdk.StreamOpenResponse{}, err
	}
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		return sdk.StreamOpenResponse{}, errors.New("AMQP consumer channel failed")
	}
	if err = ch.Qos(req.MaxBufferedMessages, 0, false); err != nil {
		closeChannel(ch, raw)
		return sdk.StreamOpenResponse{}, errors.New("AMQP QoS failed")
	}
	streamCtx, cancel := context.WithCancel(r.ctx)
	deliveries, err := ch.Consume(req.Target, "", false, false, false, false, nil)
	if err != nil {
		cancel()
		closeChannel(ch, raw)
		return sdk.StreamOpenResponse{}, errors.New("AMQP consume failed; queue must already exist")
	}
	s := &consumer{ch: ch, raw: raw, messages: deliveries, ctx: streamCtx, cancel: cancel, pending: map[string]amqp.Delivery{}, credits: req.InitialCredits, max: req.MaxBufferedMessages, recvGate: make(chan struct{}, 1), notify: make(chan struct{}, 1)}
	r.mu.Lock()
	if r.closed || ctx.Err() != nil {
		r.mu.Unlock()
		cancel()
		closeChannel(ch, raw)
		return sdk.StreamOpenResponse{}, errors.New("consumer open canceled")
	}
	r.streams[req.SessionKey] = s
	r.mu.Unlock()
	keep = true
	return sdk.StreamOpenResponse{StreamID: req.SessionKey, DeliverySemantics: sdk.StreamDeliveryAtLeastOnce, InitialCredits: req.InitialCredits}, nil
}
func (r *resource) stream(id string) (*consumer, error) {
	r.mu.Lock()
	s := r.streams[id]
	r.mu.Unlock()
	if s == nil {
		return nil, errors.New("unknown stream")
	}
	return s, nil
}
func (c *connector) ReceiveStream(h string, req sdk.StreamReceiveRequest) (sdk.StreamReceiveResponse, error) {
	r, err := c.get(h)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	s, err := r.stream(req.StreamID)
	if err != nil {
		return sdk.StreamReceiveResponse{}, err
	}
	if req.WaitTimeoutMS < 1 || req.WaitTimeoutMS > 30000 || req.MaxMessages < 1 || req.MaxMessages > 8 {
		return sdk.StreamReceiveResponse{}, errors.New("invalid receive limits")
	}
	ctx, cancel := context.WithTimeout(s.ctx, time.Duration(req.WaitTimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case s.recvGate <- struct{}{}:
	case <-ctx.Done():
		if s.ctx.Err() != nil {
			return sdk.StreamReceiveResponse{}, errors.New("stream closed")
		}
		return sdk.StreamReceiveResponse{}, nil
	}
	defer func() { <-s.recvGate }()
	for {
		s.mu.Lock()
		ready := s.credits > 0 && len(s.pending) < s.max
		s.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-s.notify:
		case <-ctx.Done():
			if s.ctx.Err() != nil {
				return sdk.StreamReceiveResponse{}, errors.New("stream closed")
			}
			return sdk.StreamReceiveResponse{}, nil
		}
	}
	select {
	case <-ctx.Done():
		if s.ctx.Err() != nil {
			return sdk.StreamReceiveResponse{}, errors.New("stream closed")
		}
		return sdk.StreamReceiveResponse{}, nil
	case d, ok := <-s.messages:
		if !ok {
			return sdk.StreamReceiveResponse{}, errors.New("AMQP consumer session lost; unacknowledged messages may redeliver")
		}
		if len(d.Body) > r.cfg.MaxMessageBytes || d.DeliveryTag == 0 {
			return sdk.StreamReceiveResponse{}, errors.New("invalid AMQP delivery")
		}
		id := strconv.FormatUint(d.DeliveryTag, 10)
		s.mu.Lock()
		if _, exists := s.pending[id]; exists {
			s.mu.Unlock()
			return sdk.StreamReceiveResponse{}, errors.New("duplicate delivery tag")
		}
		s.pending[id] = d
		s.credits--
		s.mu.Unlock()
		at := time.Now().UnixNano()
		if !d.Timestamp.IsZero() {
			at = d.Timestamp.UnixNano()
		}
		m := sdk.StreamMessage{StreamID: req.StreamID, DeliveryID: id, Sequence: d.DeliveryTag, Topic: d.RoutingKey, RawPayload: append([]byte(nil), d.Body...), Metadata: map[string]string{"message_id": d.MessageId, "content_type": d.ContentType, "redelivered": strconv.FormatBool(d.Redelivered), "exchange": d.Exchange}, PublishedAtUnixNano: at}
		return sdk.StreamReceiveResponse{Messages: []sdk.StreamMessage{m}}, nil
	}
}
func (c *connector) settle(h, id, delivery string, credits int, nack, requeue bool) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	ctx, done, err := r.begin(0)
	if err != nil {
		return err
	}
	defer done()
	s, err := r.stream(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, exists := s.pending[delivery]
	if !exists || credits < 0 || credits > 1 || s.credits+credits+len(s.pending)-1 > s.max {
		return errors.New("unknown delivery or invalid ACK credit")
	}
	stop := context.AfterFunc(ctx, func() { s.raw.Close() })
	defer stop()
	if nack {
		err = d.Nack(false, requeue)
	} else {
		err = d.Ack(false)
	}
	if err != nil {
		return errors.New("AMQP settlement failed; session may redeliver")
	}
	delete(s.pending, delivery)
	s.credits += credits
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}
func (c *connector) AckStream(h string, q sdk.StreamAckRequest) error {
	return c.settle(h, q.StreamID, q.DeliveryID, q.Credits, false, false)
}
func (c *connector) NackStream(h string, q sdk.StreamNackRequest) error {
	return c.settle(h, q.StreamID, q.DeliveryID, q.Credits, true, q.Requeue)
}
func (c *connector) GrantCredits(h string, q sdk.StreamGrantCreditsRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	s, err := r.stream(q.StreamID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if q.Credits < 0 || s.credits+q.Credits+len(s.pending) > s.max {
		return errors.New("credit limit exceeded")
	}
	s.credits += q.Credits
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}
func (c *connector) CloseStream(h string, q sdk.StreamCloseRequest) error {
	r, err := c.get(h)
	if err != nil {
		return err
	}
	_, done, err := r.begin(0)
	if err != nil {
		return err
	}
	defer done()
	r.mu.Lock()
	s := r.streams[q.StreamID]
	if s != nil {
		delete(r.streams, q.StreamID)

	}
	r.mu.Unlock()
	if s != nil {
		s.cancel()
		closeChannel(s.ch, s.raw)
		r.mu.Lock()
		r.budget -= r.reserved[q.StreamID]
		delete(r.reserved, q.StreamID)
		r.mu.Unlock()
	}
	return nil
}
