package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type scriptTraceLogger struct {
	noopLogger
	rows []map[string]any
}

func (l *scriptTraceLogger) Info(_ string, fields ...sdk.Field) {
	row := make(map[string]any, len(fields))
	for _, field := range fields {
		row[field.Key] = field.Value
	}
	l.rows = append(l.rows, row)
}

type scriptTraceContext struct {
	testContext
	logger *scriptTraceLogger
}

func (c scriptTraceContext) Logger() sdk.Logger { return c.logger }

func TestScriptVersionAndDigestTraceActivationAndConsole(t *testing.T) {
	code := "function main(input) { console.log('ready'); return { payload: { answer: 42 } }; }"
	cfg, err := json.Marshal(pluginConfig{Code: "  " + code + "  ", ScriptVersion: "hmi-v2.3.1", TimeoutMS: 1000})
	if err != nil {
		t.Fatal(err)
	}
	p := new(JavaScriptTransformProcessor)
	if err := p.Init(sdk.PluginConfig{Raw: cfg}); err != nil {
		t.Fatal(err)
	}
	logger := new(scriptTraceLogger)
	emitter := new(testEmitter)
	if err := p.Start(scriptTraceContext{testContext: testContext{emitter}, logger: logger}); err != nil {
		t.Fatal(err)
	}
	if err := p.OnEvent(sdk.NewEvent(map[string]any{}, nil)); err != nil {
		t.Fatal(err)
	}
	if len(logger.rows) != 2 {
		t.Fatalf("rows: %v", logger.rows)
	}
	for _, row := range logger.rows {
		if row["script_version"] != "hmi-v2.3.1" || row["script_sha256"] != fmt.Sprintf("%x", sha256.Sum256([]byte(code))) {
			t.Fatalf("missing script identity: %v", row)
		}
	}
	if len(emitter.events) != 1 || len(emitter.events[0].Payload) != 1 || len(emitter.events[0].Metadata) != 0 {
		t.Fatalf("script identity changed the event contract: %v", emitter.events)
	}
	bad, _ := json.Marshal(pluginConfig{Code: code, ScriptVersion: "invalid version"})
	if err := p.Init(sdk.PluginConfig{Raw: bad}); err == nil {
		t.Fatal("invalid version accepted")
	}
}
