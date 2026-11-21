package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
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

var scriptVersionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type scriptLogBudget struct {
	calls int
	bytes int
}

type JavaScriptTransformProcessor struct {
	sdk.BasePlugin
	cfg          pluginConfig
	program      *goja.Program
	scriptDigest string
	ctx          sdk.RuntimeContext
}

func (p *JavaScriptTransformProcessor) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID: "javascript-transform", Name: "JavaScript Transform", Version: "1.0.2",
		Description: "Transform JSON-compatible events with isolated, time-bounded JavaScript",
		Type:        sdk.PluginTypeProcessor, Category: "transform",
		Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema},
	}
}

func (p *JavaScriptTransformProcessor) Init(cfg sdk.PluginConfig) error {
	var config pluginConfig
	if err := json.Unmarshal(cfg.Raw, &config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	config.Code = strings.TrimSpace(config.Code)
	if config.ScriptVersion != "" && !scriptVersionPattern.MatchString(config.ScriptVersion) {
		return fmt.Errorf("script_version must start with an alphanumeric and use at most 64 letters, digits, dots, underscores or hyphens")
	}
	if config.Code == "" {
		return fmt.Errorf("code is required")
	}
	if len(config.Code) > 65536 {
		return fmt.Errorf("code exceeds 65536 bytes")
	}
	if config.TimeoutMS == 0 {
		config.TimeoutMS = 50
	}
	if config.TimeoutMS < 1 || config.TimeoutMS > 1000 {
		return fmt.Errorf("timeout_ms must be between 1 and 1000")
	}

	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(config.Code)))
	program, err := goja.Compile(
		"transform-"+digest[:12]+".js",
		"\"use strict\";\n"+config.Code+"\nif (typeof main !== \"function\") { throw new Error(\"function main(input) is required\"); }\nmain(input);",
		true,
	)
	if err != nil {
		return fmt.Errorf("compile javascript: %w", err)
	}
	p.program = program
	p.cfg = config
	p.scriptDigest = digest
	return nil
}

func (p *JavaScriptTransformProcessor) Start(ctx sdk.RuntimeContext) error {
	p.ctx = ctx
	if ctx != nil && ctx.Logger() != nil {
		ctx.Logger().Info("javascript script activated", p.scriptLogFields()...)
	}
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

func (p *JavaScriptTransformProcessor) scriptLogFields() []sdk.Field {
	fields := []sdk.Field{{Key: "script_sha256", Value: p.scriptDigest}}
	if p.cfg.ScriptVersion != "" {
		fields = append(fields, sdk.Field{Key: "script_version", Value: p.cfg.ScriptVersion})
	}
	return fields
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
			logger := p.ctx.Logger()
			if logger == nil {
				return goja.Undefined()
			}
			fields := append(p.scriptLogFields(), sdk.Field{Key: "message", Value: message})
			switch level {
			case "warn":
				logger.Warn("javascript console", fields...)
			case "error":
				logger.Error("javascript console", fields...)
			default:
				logger.Info("javascript console", fields...)
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
