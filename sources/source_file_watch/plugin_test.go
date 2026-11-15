package main

import (
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type emitter struct {
	sdk.Emitter
	events chan sdk.Event
}

func (e emitter) Publish(v sdk.Event) error { e.events <- v; return nil }

type metrics struct{ sdk.Metrics }

func (metrics) IncCounter(string, map[string]string) {}

type runtime struct {
	sdk.RuntimeContext
	out emitter
}

func (runtime) RuleID() string         { return "rule" }
func (runtime) NodeID() string         { return "node" }
func (r runtime) Emitter() sdk.Emitter { return r.out }
func (runtime) Metrics() sdk.Metrics   { return metrics{} }
func TestStableFilesPersistCheckpointAndDoNotFollowSymlinks(t *testing.T) {
	input := t.TempDir()
	state := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.xml")
	os.WriteFile(outside, []byte("synthetic-secret"), 0600)
	if err := os.Symlink(outside, filepath.Join(input, "link.xml")); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(input, "sample.xml"), []byte("<value>12</value>"), 0600)
	raw, _ := json.Marshal(config{Directory: input, Pattern: "*.xml", DeviceCode: "demo", StateDir: state, IntervalMS: 100, StableMS: 100, MaxBytes: 1024, MaxFiles: 100, Encoding: "utf8"})
	events := make(chan sdk.Event, 4)
	ctx := runtime{out: emitter{events: events}}
	start := func() *plugin {
		p := &plugin{}
		if err := p.Init(sdk.PluginConfig{Raw: raw}); err != nil {
			t.Fatal(err)
		}
		if err := p.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := start()
	select {
	case e := <-events:
		if e.Payload["raw"] != "<value>12</value>" {
			t.Fatal("unexpected file content")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stable file was not emitted")
	}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	p = start()
	defer p.Stop()
	select {
	case e := <-events:
		t.Fatalf("duplicate or symlink content emitted: %v", e.Payload)
	case <-time.After(350 * time.Millisecond):
	}
}
func TestFilePatternRejectsTraversal(t *testing.T) {
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"directory":"/tmp","device_code":"demo","pattern":"../*.xml"}`)}); err == nil {
		t.Fatal("traversal pattern accepted")
	}
}
