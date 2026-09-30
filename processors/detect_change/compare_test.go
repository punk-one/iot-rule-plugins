package main

import "testing"

func TestDetectFieldChangesCanEmitFirstSnapshot(t *testing.T) {
	state := NewChangeState()
	var cfg Config
	cfg.Process.Detect.Type = string(ModeChanged)
	cfg.Process.Detect.Field = "data.total_output"
	cfg.Process.Detect.Fields = []string{"data.total_output"}
	cfg.Output.Emit = string(EmitPolicyOnChange)
	cfg.Output.EmitFirst = true

	matched, count, fields, shouldEmit := detectFieldChanges(state, map[string]interface{}{"data.total_output": float64(1)}, cfg)
	if matched || count != 0 || len(fields) != 0 || !shouldEmit {
		t.Fatalf("unexpected first-snapshot result matched=%v count=%d fields=%v emit=%v", matched, count, fields, shouldEmit)
	}
}
