package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type testClient struct {
	sdk.ConnectorClient
	read func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error)
}

func (c testClient) Read(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) { return c.read(r) }

type testEmitter struct {
	sdk.Emitter
	events []sdk.Event
	err    error
}

func (e *testEmitter) Publish(v sdk.Event) error { e.events = append(e.events, v); return e.err }

type testContext struct {
	sdk.RuntimeContext
	client  sdk.ConnectorClient
	emitter sdk.Emitter
}

func (c testContext) Connector() sdk.ConnectorClient { return c.client }
func (c testContext) Emitter() sdk.Emitter           { return c.emitter }
func (c testContext) RuleID() string                 { return "rule" }
func (c testContext) NodeID() string                 { return "node" }
func fixturePlugin(t *testing.T, emit *testEmitter) (*plugin, string) {
	t.Helper()
	p := &plugin{}
	if err := p.Init(sdk.PluginConfig{Raw: json.RawMessage(`{"connector_ref":"files","device_code":"demo","stable_ms":100}`)}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "state.db")
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.db = db
	if err = db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists([]byte("delivered")); return err }); err != nil {
		t.Fatal(err)
	}
	p.seen = map[string]observation{}
	p.stop = make(chan struct{})
	p.ctx = testContext{emitter: emit}
	return p, file
}
func TestStableFileHandoffAndCheckpointReopen(t *testing.T) {
	out := &testEmitter{}
	p, file := fixturePlugin(t, out)
	data := []byte("<x>42</x>")
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	reads := 0
	client := testClient{read: func(r sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		var value any
		if r.Target == "list" {
			value = map[string]any{"files": []fileEntry{{"a.xml", int64(len(data)), 1000}}}
		} else {
			reads++
			value = map[string]any{"data": data, "sha256": digest, "size": len(data), "modified_ms": 1000}
		}
		raw, _ := json.Marshal(value)
		return sdk.ConnectorResponse{Payload: raw}, nil
	}}
	p.ctx = testContext{client: client, emitter: out}
	now := time.Now()
	if err := p.scan(now); err != nil {
		t.Fatal(err)
	}
	if reads != 0 || len(out.events) != 0 {
		t.Fatal("read before stable")
	}
	if err := p.scan(now.Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 1 || out.events[0].Payload["raw"] != string(data) {
		t.Fatal(out.events)
	}
	id := out.events[0].ID
	p.Stop()
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.db = db
	p.once = sync.Once{}
	p.stop = make(chan struct{})
	defer p.Stop()
	if err := p.scan(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 1 {
		t.Fatal("restart duplicate")
	}
	data = []byte("<x>43</x>")
	hash = sha256.Sum256(data)
	digest = hex.EncodeToString(hash[:])
	if err := p.scan(now.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 2 || out.events[1].ID == id {
		t.Fatal("changed bytes lost or identity reused")
	}
}
func TestFailedHandoffAndLateResultDoNotCheckpoint(t *testing.T) {
	out := &testEmitter{err: errors.New("queue full")}
	p, _ := fixturePlugin(t, out)
	defer p.Stop()
	data := []byte("x")
	hash := sha256.Sum256(data)
	raw, _ := json.Marshal(map[string]any{"data": data, "sha256": hex.EncodeToString(hash[:]), "size": 1, "modified_ms": 1000})
	p.ctx = testContext{emitter: out, client: testClient{read: func(sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
		return sdk.ConnectorResponse{Payload: raw}, nil
	}}}
	entry := fileEntry{"x.xml", 1, 1000}
	if err := p.deliver(entry); err == nil {
		t.Fatal("handoff error lost")
	}
	out.err = nil
	if err := p.deliver(entry); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 2 || out.events[0].ID != out.events[1].ID {
		t.Fatal("retry identity unstable")
	}
	close(p.stop)
	p.stop = closedChannel()
	p.once.Do(func() {})
	if err := p.deliver(fileEntry{"new.xml", 1, 1000}); err != nil {
		t.Fatal(err)
	}
	if len(out.events) != 2 {
		t.Fatal("late emission")
	}
	p.db.Close()
}
func closedChannel() chan struct{} { c := make(chan struct{}); close(c); return c }
func TestRejectUnsafeConfig(t *testing.T) {
	for _, raw := range []string{`{"connector_ref":"r","device_code":"d","directory":"../x"}`, `{"connector_ref":"r","device_code":"d","max_files":1025}`, `{"connector_ref":"r","device_code":"d","pattern":"../*"}`, `{"connector_ref":"r","device_code":"d","typo":true}`} {
		if (&plugin{}).Init(sdk.PluginConfig{Raw: json.RawMessage(raw)}) == nil {
			t.Fatal(raw)
		}
	}
}
