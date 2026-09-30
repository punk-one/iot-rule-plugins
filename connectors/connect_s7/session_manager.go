package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/punk-one/gos7"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const maxConcurrentS7Sessions = 8

type demandHint struct {
	NextDemandInMS int64 `json:"next_demand_in_ms,omitempty"`
}

type s7SessionStats struct {
	State               string
	Opened              uint64
	Reused              uint64
	Closed              uint64
	ConsecutiveFailures uint64
	CircuitOpenUntil    time.Time
}

func (c *s7Connector) withClient(resource *s7Resource, req sdk.ConnectorRequest, fn func(gos7.Client) error) (err error) {
	operationTimeout := requestTimeout(req.TimeoutMS, time.Duration(resource.cfg.TimeoutSec)*time.Second)
	deadline := time.Now().Add(operationTimeout)
	// The engine serializes a resource too, but this lock is the authoritative
	// device gate for direct RPC callers and the idle-close timer. Waiting for it
	// consumes the same request budget as connection and I/O.
	if err := lockS7OperationUntil(resource, deadline); err != nil {
		return err
	}
	defer resource.operationMu.Unlock()

	if resource.closed {
		return fmt.Errorf("S7 resource %s is closed", resource.resource.ID)
	}
	resource.stopIdleTimerLocked()

	now := time.Now()
	resource.pruneDemandsLocked(now)
	if now.Before(resource.circuitOpenUntil) {
		return sdk.NewRetryableError(fmt.Sprintf("S7 circuit open until %s", resource.circuitOpenUntil.UTC().Format(time.RFC3339Nano)))
	}

	if resource.sessionActive && resource.maxSessionAgeReachedLocked(now) {
		if closeErr := c.closeSessionLocked(resource); closeErr != nil {
			return fmt.Errorf("rotate expired S7 session: %w", closeErr)
		}
	}

	if !resource.sessionActive {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			resource.recordFailureLocked(time.Now())
			return s7OperationDeadlineError(req)
		}
		if err := c.acquireSessionSlot(remaining); err != nil {
			resource.recordFailureLocked(now)
			return err
		}
		client, closeClient, openErr := c.openClient(resource.cfg)
		if openErr != nil {
			c.releaseSessionSlot()
			resource.recordFailureLocked(now)
			return openErr
		}
		resource.session = client
		resource.sessionActive = true
		resource.closeSession = closeClient
		resource.sessionOpenedAt = now
		resource.sessionSlotHeld = true
		resource.sessionOpens++
		if time.Now().After(deadline) {
			resource.recordFailureLocked(time.Now())
			_ = c.closeSessionLocked(resource)
			return s7OperationDeadlineError(req)
		}
	} else {
		resource.sessionReuses++
	}

	err = fn(resource.session)
	completedAt := time.Now()
	if err == nil && completedAt.After(deadline) {
		err = s7OperationDeadlineError(req)
	}
	if err != nil {
		resource.recordFailureLocked(completedAt)
		if closeErr := c.closeSessionLocked(resource); closeErr != nil {
			return fmt.Errorf("S7 operation failed: %v; close failed: %w", err, closeErr)
		}
		return err
	}

	resource.consecutiveFailures = 0
	resource.circuitOpenUntil = time.Time{}
	resource.recordDemandLocked(req, completedAt)
	if resource.mustCloseAfterOperationLocked(completedAt) {
		if closeErr := c.closeSessionLocked(resource); closeErr != nil {
			return fmt.Errorf("close S7 connection: %w", closeErr)
		}
		return nil
	}
	resource.armIdleCloseLocked(c)
	return nil
}

func lockS7OperationUntil(resource *s7Resource, deadline time.Time) error {
	for {
		if resource.operationMu.TryLock() {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return sdk.NewRetryableError("S7 operation deadline exceeded while waiting for device gate")
		}
		pause := time.Millisecond
		if remaining < pause {
			pause = remaining
		}
		timer := time.NewTimer(pause)
		<-timer.C
	}
}

func s7OperationDeadlineError(req sdk.ConnectorRequest) error {
	if req.Kind == sdk.ConnectorRequestWrite || req.Kind == sdk.ConnectorRequestBatchWrite {
		return fmt.Errorf("S7 operation deadline exceeded; write outcome may be unknown")
	}
	return sdk.NewRetryableError("S7 operation deadline exceeded")
}

func (c *s7Connector) acquireSessionSlot(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case c.sessionSlots <- struct{}{}:
		return nil
	case <-timer.C:
		return sdk.NewRetryableError("S7 package session limit reached")
	}
}

func (c *s7Connector) releaseSessionSlot() {
	select {
	case <-c.sessionSlots:
	default:
	}
}

func (r *s7Resource) recordDemandLocked(req sdk.ConnectorRequest, now time.Time) {
	if req.Kind != sdk.ConnectorRequestRead && req.Kind != sdk.ConnectorRequestBatchRead {
		return
	}
	var hint demandHint
	if len(req.Payload) == 0 || json.Unmarshal(req.Payload, &hint) != nil || hint.NextDemandInMS <= 0 {
		return
	}
	token := req.ResourceToken
	if token == "" {
		token = "_anonymous"
	}
	r.nextDemands[token] = now.Add(time.Duration(hint.NextDemandInMS) * time.Millisecond)
}

func (r *s7Resource) pruneDemandsLocked(now time.Time) {
	for token, due := range r.nextDemands {
		if !due.After(now) {
			delete(r.nextDemands, token)
		}
	}
}

func (r *s7Resource) hasNearDemandLocked(now time.Time) bool {
	horizon := r.reuseHorizon()
	if horizon <= 0 {
		return false
	}
	deadline := now.Add(horizon)
	for _, due := range r.nextDemands {
		if due.After(now) && !due.After(deadline) {
			return true
		}
	}
	return false
}

func (r *s7Resource) mustCloseAfterOperationLocked(now time.Time) bool {
	if r.sessionPolicy == sdk.ConnectorSessionPolicyPerOperation || r.reuseHorizon() <= 0 {
		return true
	}
	if r.maxSessionAgeReachedLocked(now) {
		return true
	}
	if r.hasNearDemandLocked(now) {
		return false
	}
	if r.cfg.CloseWhenNoDemand != nil && !*r.cfg.CloseWhenNoDemand {
		return false
	}
	return true
}

func (r *s7Resource) reuseHorizon() time.Duration {
	if r.cfg.SessionReuseHorizonMillis == nil {
		return time.Duration(s7DefaultReuseHorizonMillis) * time.Millisecond
	}
	return time.Duration(*r.cfg.SessionReuseHorizonMillis) * time.Millisecond
}

func (r *s7Resource) maxSessionAgeReachedLocked(now time.Time) bool {
	if !r.sessionActive || r.cfg.MaxSessionAgeMillis <= 0 || r.sessionOpenedAt.IsZero() {
		return false
	}
	return now.Sub(r.sessionOpenedAt) >= time.Duration(r.cfg.MaxSessionAgeMillis)*time.Millisecond
}

func (r *s7Resource) recordFailureLocked(now time.Time) {
	r.consecutiveFailures++
	if r.consecutiveFailures < uint64(r.cfg.FailureThreshold) {
		return
	}
	shift := r.consecutiveFailures - uint64(r.cfg.FailureThreshold)
	if shift > 8 {
		shift = 8
	}
	backoff := time.Duration(r.cfg.RetryBackoffMillis) * time.Millisecond * time.Duration(1<<shift)
	maximum := time.Duration(r.cfg.MaxRetryBackoffMillis) * time.Millisecond
	if backoff > maximum {
		backoff = maximum
	}
	r.circuitOpenUntil = now.Add(backoff)
}

func (r *s7Resource) stopIdleTimerLocked() {
	if r.idleTimer != nil {
		r.idleTimer.Stop()
		r.idleTimer = nil
	}
	r.idleGeneration++
}

func (r *s7Resource) armIdleCloseLocked(connector *s7Connector) {
	horizon := r.reuseHorizon()
	if horizon <= 0 || !r.sessionActive {
		return
	}
	r.idleGeneration++
	generation := r.idleGeneration
	r.idleTimer = time.AfterFunc(horizon, func() {
		r.operationMu.Lock()
		defer r.operationMu.Unlock()
		if r.closed || generation != r.idleGeneration {
			return
		}
		r.pruneDemandsLocked(time.Now())
		_ = connector.closeSessionLocked(r)
		r.idleTimer = nil
	})
}

func (c *s7Connector) closeSessionLocked(resource *s7Resource) error {
	resource.stopIdleTimerLocked()
	closeClient := resource.closeSession
	hadSession := resource.sessionActive
	resource.session = nil
	resource.sessionActive = false
	resource.closeSession = nil
	resource.sessionOpenedAt = time.Time{}
	heldSlot := resource.sessionSlotHeld
	resource.sessionSlotHeld = false
	if hadSession {
		resource.sessionCloses++
	}
	var err error
	if closeClient != nil {
		err = closeClient()
	}
	if heldSlot {
		c.releaseSessionSlot()
	}
	return err
}

func (c *s7Connector) closeResource(resource *s7Resource) error {
	resource.operationMu.Lock()
	defer resource.operationMu.Unlock()
	resource.closed = true
	resource.nextDemands = make(map[string]time.Time)
	return c.closeSessionLocked(resource)
}

func (c *s7Connector) releaseDemand(resource *s7Resource, token string) error {
	resource.operationMu.Lock()
	defer resource.operationMu.Unlock()
	if token == "" {
		token = "_anonymous"
	}
	delete(resource.nextDemands, token)
	now := time.Now()
	resource.pruneDemandsLocked(now)
	if !resource.sessionActive {
		return nil
	}
	if resource.hasNearDemandLocked(now) {
		resource.stopIdleTimerLocked()
		resource.armIdleCloseLocked(c)
		return nil
	}
	return c.closeSessionLocked(resource)
}

func (r *s7Resource) stats() s7SessionStats {
	r.operationMu.Lock()
	defer r.operationMu.Unlock()
	state := "idle"
	if r.sessionActive {
		state = "open"
	}
	if time.Now().Before(r.circuitOpenUntil) {
		state = "circuit_open"
	}
	return s7SessionStats{
		State:               state,
		Opened:              r.sessionOpens,
		Reused:              r.sessionReuses,
		Closed:              r.sessionCloses,
		ConsecutiveFailures: r.consecutiveFailures,
		CircuitOpenUntil:    r.circuitOpenUntil,
	}
}
