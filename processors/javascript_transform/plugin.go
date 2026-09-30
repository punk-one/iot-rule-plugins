package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const (
	maxJavaScriptInputBytes  = 1 << 20
	maxJavaScriptOutputBytes = 1 << 20
	maxJavaScriptLogCalls    = 100
	maxJavaScriptLogBytes    = 8 << 10
)

type scriptLogBudget struct {
	calls int
	bytes int
}

type JavaScriptTransformProcessor struct {
	sdk.BasePlugin
	cfg     pluginConfig
	program *goja.Program
	ctx     sdk.RuntimeContext
}

func (p *JavaScriptTransformProcessor) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID: "javascript-transform", Name: "JavaScript Transform", Version: "1.0.0",
		Description: "Transform JSON-compatible events with isolated, time-bounded JavaScript",
		Type:        sdk.PluginTypeProcessor, Category: "transform",
		Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema},
	}
}

func (p *JavaScriptTransformProcessor) Init(cfg sdk.PluginConfig) error {
	if err := json.Unmarshal(cfg.Raw, &p.cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	p.cfg.Code = strings.TrimSpace(p.cfg.Code)
	if p.cfg.Code == "" {
		return fmt.Errorf("code is required")
	}
	if len(p.cfg.Code) > 65536 {
		return fmt.Errorf("code exceeds 65536 bytes")
	}
	if p.cfg.TimeoutMS == 0 {
		p.cfg.TimeoutMS = 50
	}
	if p.cfg.TimeoutMS < 1 || p.cfg.TimeoutMS > 1000 {
		return fmt.Errorf("timeout_ms must be between 1 and 1000")
	}

	program, err := goja.Compile(
		"transform.js",
		"\"use strict\";\n"+p.cfg.Code+"\nif (typeof main !== \"function\") { throw new Error(\"function main(input) is required\"); }\nmain(input);",
		true,
	)
	if err != nil {
		return fmt.Errorf("compile javascript: %w", err)
	}
	p.program = program
	return nil
}

func (p *JavaScriptTransformProcessor) Start(ctx sdk.RuntimeContext) error {
	p.ctx = ctx
	return nil
}

func (p *JavaScriptTransformProcessor) Stop() error { return nil }

func (p *JavaScriptTransformProcessor) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *JavaScriptTransformProcessor) OnEvent(event sdk.Event) (returnErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			returnErr = fmt.Errorf("javascript processing failed safely: %v", recovered)
		}
	}()
	if p.ctx == nil || p.program == nil {
		return fmt.Errorf("javascript runtime is not initialized")
	}
	deadline := time.Now().Add(time.Duration(p.cfg.TimeoutMS) * time.Millisecond)

	owned := event.Clone()
	input := scriptInput{
		Payload: owned.Payload, Metadata: owned.Metadata,
		TimestampMS: owned.Timestamp.UnixMilli(), EventID: owned.ID,
		TraceID: sdk.GetTraceID(owned), RuleID: owned.RuleID, Source: owned.Source,
	}
	runtime := goja.New()
	if err := p.installConsole(runtime, &scriptLogBudget{}); err != nil {
		return err
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode javascript input: %w", err)
	}
	if len(inputJSON) > maxJavaScriptInputBytes {
		return fmt.Errorf("javascript input exceeds %d bytes", maxJavaScriptInputBytes)
	}
	var inputValue interface{}
	if err := json.Unmarshal(inputJSON, &inputValue); err != nil {
		return fmt.Errorf("normalize javascript input: %w", err)
	}
	if err := runtime.Set("input", inputValue); err != nil {
		return fmt.Errorf("prepare javascript input: %w", err)
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return fmt.Errorf("javascript execution exceeded %dms", p.cfg.TimeoutMS)
	}
	var interrupted atomic.Bool
	timer := time.AfterFunc(remaining, func() {
		interrupted.Store(true)
		runtime.Interrupt("javascript execution timeout")
	})
	value, err := runtime.RunProgram(p.program)
	if err != nil {
		if !timer.Stop() {
			runtime.ClearInterrupt()
		}
		if _, interrupted := err.(*goja.InterruptedError); interrupted {
			return fmt.Errorf("javascript execution exceeded %dms", p.cfg.TimeoutMS)
		}
		return fmt.Errorf("execute javascript: %w", err)
	}
	if goja.IsNull(value) || goja.IsUndefined(value) {
		if !timer.Stop() {
			runtime.ClearInterrupt()
		}
		return nil
	}

	raw, err := exportJavaScriptJSON(value)
	timedOut := interrupted.Load() || time.Now().After(deadline)
	if !timer.Stop() {
		runtime.ClearInterrupt()
	}
	if timedOut {
		return fmt.Errorf("javascript execution exceeded %dms", p.cfg.TimeoutMS)
	}
	if err != nil {
		return fmt.Errorf("javascript result must be JSON-compatible: %w", err)
	}
	if len(raw) > maxJavaScriptOutputBytes {
		return fmt.Errorf("javascript result exceeds %d bytes", maxJavaScriptOutputBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var output scriptOutput
	if err := decoder.Decode(&output); err != nil {
		return fmt.Errorf("invalid javascript result: %w", err)
	}
	if output.Payload == nil {
		return fmt.Errorf("javascript result.payload must be an object")
	}

	owned.Payload = output.Payload
	if output.Metadata != nil {
		owned.Metadata = *output.Metadata
	}
	if output.TimestampMS != nil {
		owned.Timestamp = time.UnixMilli(*output.TimestampMS).UTC()
	}
	owned.RuleID = p.ctx.RuleID()
	owned.Source = p.ctx.NodeID()
	if err := p.ctx.Emitter().Publish(owned); err != nil {
		return fmt.Errorf("publish javascript result: %w", err)
	}
	return nil
}

func exportJavaScriptJSON(value goja.Value) (raw []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("export javascript value: %v", recovered)
			raw = nil
		}
	}()
	return json.Marshal(value.Export())
}

func (p *JavaScriptTransformProcessor) installConsole(runtime *goja.Runtime, budget *scriptLogBudget) error {
	console := runtime.NewObject()
	bindings := []struct {
		name  string
		level string
	}{
		{name: "log", level: "info"},
		{name: "info", level: "info"},
		{name: "warn", level: "warn"},
		{name: "error", level: "error"},
	}
	for _, binding := range bindings {
		level := binding.level
		if err := console.Set(binding.name, func(call goja.FunctionCall) goja.Value {
			if budget.calls >= maxJavaScriptLogCalls || budget.bytes >= maxJavaScriptLogBytes {
				return goja.Undefined()
			}
			parts := make([]string, 0, len(call.Arguments))
			for _, argument := range call.Arguments {
				parts = append(parts, argument.String())
			}
			message := strings.Join(parts, " ")
			remaining := maxJavaScriptLogBytes - budget.bytes
			if len(message) > remaining {
				message = message[:remaining]
			}
			budget.calls++
			budget.bytes += len(message)
			switch level {
			case "warn":
				p.ctx.Logger().Warn("javascript console", sdk.Field{Key: "message", Value: message})
			case "error":
				p.ctx.Logger().Error("javascript console", sdk.Field{Key: "message", Value: message})
			default:
				p.ctx.Logger().Info("javascript console", sdk.Field{Key: "message", Value: message})
			}
			return goja.Undefined()
		}); err != nil {
			return fmt.Errorf("install console.%s: %w", binding.name, err)
		}
	}
	if err := runtime.Set("console", console); err != nil {
		return fmt.Errorf("install javascript console: %w", err)
	}
	return nil
}

var _ sdk.Plugin = (*JavaScriptTransformProcessor)(nil)
