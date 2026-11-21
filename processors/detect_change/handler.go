package main

import (
	"fmt"
	"hash/crc32"
	"math"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

// OnEvent 处理事件（v1.2）
func (p *ChangeDetectPlugin) OnEvent(event sdk.Event) error {
	if p.ctx == nil || p.stateMgr == nil {
		return detectChangeFatal(fmt.Errorf("RuntimeContext or StateManager not initialized"))
	}

	keyValues := make([]string, 0, len(p.cfg.State.KeyBy))
	for _, groupField := range p.cfg.State.KeyBy {
		if val, ok := getPathValue(event.Payload, groupField); ok {
			keyValues = append(keyValues, fmt.Sprintf("%v", val))
		} else {
			p.ctx.Logger().Warn("Group by field not found in event, skipping", sdk.Field{Key: "field", Value: groupField})
			return nil
		}
	}

	stateKey := p.stateMgr.GenerateKey(keyValues)
	// A key must be compared and advanced as one operation. Striping keeps
	// unrelated streams from waiting on the same state handoff by default.
	lock := &p.stateLocks[crc32.ChecksumIEEE([]byte(stateKey))%uint32(len(p.stateLocks))]
	lock.Lock()
	defer lock.Unlock()

	var state ChangeState
	if err := p.stateMgr.GetState(stateKey, &state); err != nil {
		state = *NewChangeState()
	}

	var (
		matched       bool
		changeValue   interface{}
		changedFields []string
		shouldEmit    bool
	)

	if p.cfg.Process.Detect.Type == string(ModeChanged) {
		currentValues := make(map[string]interface{}, len(p.cfg.detectFields()))
		for _, field := range p.cfg.detectFields() {
			value, ok := getPathValue(event.Payload, field)
			if !ok {
				p.ctx.Logger().Warn("Field not found in event, skipping", sdk.Field{Key: "field", Value: field})
				return nil
			}
			currentValues[field] = value
		}

		var changeCount int
		matched, changeCount, changedFields, shouldEmit = detectFieldChanges(&state, currentValues, p.cfg)
		changeValue = changeCount
		updateFieldState(&state, currentValues)
	} else {
		fieldValue, ok := getPathValue(event.Payload, p.cfg.Process.Detect.Field)
		if !ok {
			p.ctx.Logger().Warn("Field not found in event, skipping", sdk.Field{Key: "field", Value: p.cfg.Process.Detect.Field})
			return nil
		}

		newValue, valid := numericSample(fieldValue)
		if !valid {
			p.ctx.Logger().Warn("Change detection skipped a nonnumeric or nonfinite sample", sdk.Field{Key: "field", Value: p.cfg.Process.Detect.Field})
			return nil
		}
		var numericChangeValue float64
		matched, numericChangeValue, shouldEmit = detectChange(&state, newValue, p.cfg)
		if math.IsNaN(numericChangeValue) || math.IsInf(numericChangeValue, 0) {
			p.ctx.Logger().Warn("Change detection skipped a nonfinite difference", sdk.Field{Key: "field", Value: p.cfg.Process.Detect.Field})
			return nil
		}
		changeValue = numericChangeValue
		updateNumericState(&state, newValue, matched, p.cfg)
	}

	if shouldEmit {
		outputEvent := event.Clone()
		outputEvent.RuleID = p.ctx.RuleID()
		outputEvent.Source = p.ctx.NodeID()
		outputPayload := outputEvent.Payload
		if p.cfg.annotateEnabled() {
			outputPayload["_change_detected"] = matched
			outputPayload["_change_value"] = changeValue
			outputPayload["_change_mode"] = p.cfg.Process.Detect.Type
			if p.cfg.Process.Detect.Type == string(ModeChanged) {
				outputPayload["_change_fields"] = changedFields
			}
		}

		if err := p.ctx.Emitter().Publish(outputEvent); err != nil {
			p.ctx.Logger().Error("Failed to publish event", sdk.Field{Key: "error", Value: err.Error()})
			return detectChangeRetryable(err)
		}

		if matched {
			p.ctx.Logger().Debug("ChangeDetect: change detected",
				sdk.Field{Key: "mode", Value: p.cfg.Process.Detect.Type},
				sdk.Field{Key: "value", Value: changeValue},
				sdk.Field{Key: "threshold", Value: p.cfg.Process.Detect.Value},
				sdk.Field{Key: "fields", Value: changedFields})
		} else {
			p.ctx.Logger().Debug("ChangeDetect: no change detected, but emitting due to output.emit=always")
		}
	} else {
		p.ctx.Logger().Debug("ChangeDetect: no change detected, skipping emit")
	}

	// Advance the reference only after any required handoff succeeds. A
	// retryable publish failure must not suppress the next delivery attempt.
	if err := p.saveState(stateKey, &state); err != nil {
		p.ctx.Logger().Error("Failed to save state", sdk.Field{Key: "error", Value: err.Error()})
		p.reportStateFailure(err)
	}
	p.reportHeartbeat()
	return nil
}

func (p *ChangeDetectPlugin) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *ChangeDetectPlugin) saveState(stateKey string, state *ChangeState) error {
	if p.stateTTL > 0 {
		return p.stateMgr.SetStateWithTTL(stateKey, state, p.stateTTL)
	}
	return p.stateMgr.SetState(stateKey, state)
}
