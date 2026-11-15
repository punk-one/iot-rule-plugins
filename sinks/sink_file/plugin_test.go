package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type runtime struct{ sdk.RuntimeContext }

func TestAtomicEventReplayConflictAndSymlink(t *testing.T) {
	dir := t.TempDir()
	raw, _ := json.Marshal(map[string]any{"directory": dir})
	p := &plugin{}
	p.Init(sdk.PluginConfig{Raw: raw})
	if err := p.Start(runtime{}); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	e := sdk.NewEvent(map[string]any{"v": 1.0}, nil)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- p.persist(e) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatal("non-idempotent files")
	}
	e.Payload["v"] = 2.0
	if p.persist(e) == nil {
		t.Fatal("ID conflict accepted")
	}
	other := filepath.Join(t.TempDir(), "private")
	os.WriteFile(other, []byte("keep"), 0600)
	os.Remove(filepath.Join(dir, entries[0].Name()))
	os.Symlink(other, filepath.Join(dir, entries[0].Name()))
	if p.persist(e) == nil {
		t.Fatal("symlink followed")
	}
	content, _ := os.ReadFile(other)
	if string(content) != "keep" {
		t.Fatal("outside file modified")
	}
}
func TestJSONLBoundAndStop(t *testing.T) {
	dir := t.TempDir()
	raw, _ := json.Marshal(map[string]any{"directory": dir, "mode": "jsonl", "max_event_bytes": 64, "max_file_bytes": 64})
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: raw}); err != nil {
		t.Fatal(err)
	}
	p.Start(runtime{})
	e := sdk.NewEvent(map[string]any{"v": 1.0}, nil)
	e.ID = "a"
	if err := p.persist(e); err != nil {
		t.Fatal(err)
	}
	if p.persist(e) == nil {
		t.Fatal("file cap ignored")
	}
	p.Stop()
	if p.persist(e) == nil {
		t.Fatal("write after stop")
	}
}
