package main

import (
	"context"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net"
	"strconv"
	"time"
)

type sessionStats struct {
	Opens      uint64 `json:"opens"`
	Reuses     uint64 `json:"reuses"`
	Closes     uint64 `json:"closes"`
	Failures   int    `json:"consecutive_failures"`
	Active     bool   `json:"active"`
	Demands    int    `json:"demands"`
	RetryAfter int64  `json:"retry_after_ms"`
}

func demandToken(token string) string {
	if token == "" {
		return "_anonymous"
	}
	return token
}
func (c *connector) stopTimer(r *resource) {
	r.timerGeneration++
	if r.idleTimer != nil {
		r.idleTimer.Stop()
		r.idleTimer = nil
	}
}
func (c *connector) closeSession(r *resource) {
	c.stopTimer(r)
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
		r.stats.Closes++
	}
	if r.slotHeld {
		<-c.sessions
		r.slotHeld = false
	}
	r.stats.Active = false
}
func (c *connector) acquireSession(r *resource, ctx context.Context) (net.Conn, error) {
	c.stopTimer(r)
	if r.conn != nil && r.cfg.MaxSessionAgeMS > 0 && time.Since(r.openedAt) >= time.Duration(r.cfg.MaxSessionAgeMS)*time.Millisecond {
		c.closeSession(r)
	}
	if r.conn != nil {
		r.stats.Reuses++
		return r.conn, nil
	}
	c.waiters.Add(1)
	select {
	case c.sessions <- struct{}{}:
		r.slotHeld = true
		c.waiters.Add(-1)
	case <-ctx.Done():
		c.waiters.Add(-1)
		return nil, ctx.Err()
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, r.cfg.Transport, net.JoinHostPort(r.cfg.Host, strconv.Itoa(r.cfg.Port)))
	if err != nil {
		c.closeSession(r)
		return nil, err
	}
	r.conn = conn
	r.openedAt = time.Now()
	r.stats.Opens++
	r.stats.Active = true
	return conn, nil
}
func (r *resource) pruneDemands(now time.Time) {
	for token, due := range r.demands {
		if !due.Add(r.demandGrace()).After(now) {
			delete(r.demands, token)
		}
	}
}
func (r *resource) demandGrace() time.Duration {
	return time.Duration(min(max(r.cfg.ReuseHorizonMS/8, 10), 250)) * time.Millisecond
}
func (r *resource) nearDemandDeadline(now time.Time) time.Time {
	r.pruneDemands(now)
	var latest time.Time
	horizon := now.Add(time.Duration(r.cfg.ReuseHorizonMS) * time.Millisecond)
	for _, due := range r.demands {
		if !due.After(horizon) && due.Add(r.demandGrace()).After(now) && due.After(latest) {
			latest = due
		}
	}
	if !latest.IsZero() {
		return latest.Add(r.demandGrace())
	}
	return time.Time{}
}
func (c *connector) retainOrClose(r *resource) {
	if r.conn == nil {
		return
	}
	now := time.Now()
	if r.ctx.Err() != nil || r.cfg.ReuseHorizonMS == 0 || (r.cfg.Transport == "udp" && !mc4E(r.cfg)) || c.waiters.Load() > 0 {
		c.closeSession(r)
		return
	}
	until := r.nearDemandDeadline(now)
	if until.IsZero() {
		c.closeSession(r)
		return
	}
	c.stopTimer(r)
	generation := r.timerGeneration
	r.idleTimer = time.AfterFunc(time.Until(until), func() {
		select {
		case r.gate <- struct{}{}:
		case <-r.ctx.Done():
			return
		}
		defer func() { <-r.gate }()
		if r.timerGeneration != generation {
			return
		}
		c.retainOrClose(r)
	})
}
func (c *connector) finishSession(r *resource, req sdk.ConnectorRequest, b batch, err error) {
	if err != nil {
		c.closeSession(r)
		r.stats.Failures++
		if r.stats.Failures >= 3 {
			delay := time.Duration(1<<min(r.stats.Failures-3, 5)) * time.Second
			r.retryAfter = time.Now().Add(min(delay, 30*time.Second))
		}
		return
	}
	r.stats.Failures = 0
	r.retryAfter = time.Time{}
	if req.Kind == sdk.ConnectorRequestRead || req.Kind == sdk.ConnectorRequestBatchRead {
		token := demandToken(req.ResourceToken)
		if b.NextDemandInMS > 0 && b.NextDemandInMS <= int64(r.cfg.ReuseHorizonMS) {
			r.demands[token] = time.Now().Add(time.Duration(b.NextDemandInMS) * time.Millisecond)
		} else {
			delete(r.demands, token)
		}
	}
	if r.conn != nil {
		r.conn.SetDeadline(time.Time{})
	}
	c.retainOrClose(r)
}
func (c *connector) sessionControl(r *resource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	if req.Kind != sdk.ConnectorRequestRead {
		return sdk.ConnectorResponse{}, errors.New("session control is read-only")
	}
	timeout := r.cfg.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeout {
		timeout = req.TimeoutMS
	}
	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()
	select {
	case r.gate <- struct{}{}:
	case <-ctx.Done():
		return sdk.ConnectorResponse{}, ctx.Err()
	}
	defer func() { <-r.gate }()
	if req.Target == "release_demand" {
		delete(r.demands, demandToken(req.ResourceToken))
		c.retainOrClose(r)
		return sdk.ConnectorResponse{Payload: json.RawMessage(`{"released":true}`)}, nil
	}
	r.pruneDemands(time.Now())
	stats := r.stats
	stats.Active = r.conn != nil
	stats.Demands = len(r.demands)
	if !r.retryAfter.IsZero() {
		stats.RetryAfter = r.retryAfter.UnixMilli()
	}
	raw, _ := json.Marshal(stats)
	return sdk.ConnectorResponse{Payload: raw}, nil
}
