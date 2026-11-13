package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
)

type testContext struct {
	sdk.RuntimeContext
	rule, node string
	emitter    sdk.Emitter
}

func (c testContext) RuleID() string       { return c.rule }
func (c testContext) NodeID() string       { return c.node }
func (c testContext) Emitter() sdk.Emitter { return c.emitter }

type testEmitter struct {
	sdk.Emitter
	events chan sdk.Event
}

func (e testEmitter) Publish(event sdk.Event) error { e.events <- event; return nil }

func testBuffer(t *testing.T) (*buffer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.db")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("pending")); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists([]byte("usage"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	b := &buffer{db: db, ctx: testContext{rule: "rule-1", node: "buffer-1"}}
	b.cfg.Storage.MaxRecords = 2
	b.cfg.Storage.MaxBytes = 1 << 20
	b.cfg.AckNode = "mqtt-sink"
	b.cfg.RetryMS = 2000
	b.cfg.Rate = 100
	t.Cleanup(func() { _ = b.db.Close() })
	return b, path
}

func TestDurableReplayKeepsEventIDAfterRestart(t *testing.T) {
	b, path := testBuffer(t)
	event := sdk.NewEvent(map[string]interface{}{"value": 7}, nil)
	if err := b.OnEvent(event); err != nil {
		t.Fatal(err)
	}
	if _, ok := event.Payload["event_id"]; ok {
		t.Fatal("caller event mutated")
	}
	if err := b.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.db = db
	emit := testEmitter{events: make(chan sdk.Event, 2)}
	b.ctx = testContext{rule: "rule-1", node: "buffer-1", emitter: emit}
	b.stop = make(chan struct{})
	b.wg.Add(1)
	go b.run()
	select {
	case replay := <-emit.events:
		if replay.ID != event.ID || replay.Payload["event_id"] != event.ID {
			t.Fatal("replay changed identity")
		}
	case <-time.After(time.Second):
		t.Fatal("record not replayed")
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestOnlyDesignatedSinkCanConfirmAndCapacityIsBounded(t *testing.T) {
	b, _ := testBuffer(t)
	e := sdk.NewEvent(nil, nil)
	if err := b.OnEvent(e); err != nil {
		t.Fatal(err)
	}
	if err := b.OnEvent(e); err != nil {
		t.Fatal(err)
	}
	if err := b.OnEvent(sdk.NewEvent(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if err := b.OnEvent(sdk.NewEvent(nil, nil)); err == nil {
		t.Fatal("capacity not enforced")
	}
	var id string
	_ = b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("pending")).ForEach(func(k, v []byte) error {
			var r record
			_ = json.Unmarshal(v, &r)
			if r.Event.ID == e.ID {
				id = string(k)
			}
			return nil
		})
	})
	ack := sdk.AckMessage{RecordID: id, RuleID: "rule-1", BufferNodeID: "buffer-1", FromNodeID: "wrong-sink", EventID: e.ID, Status: sdk.AckStatusSuccess}
	confirm := func() { raw, _ := json.Marshal(ack); b.confirm(&nats.Msg{Data: raw}) }
	confirm()
	_ = b.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("pending")).Get([]byte(id)) == nil {
			t.Fatal("wrong sink deleted data")
		}
		return nil
	})
	ack.FromNodeID = "mqtt-sink"
	confirm()
	_ = b.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("pending")).Get([]byte(id)) != nil {
			t.Fatal("confirmed record retained")
		}
		count, _ := readUsage(tx.Bucket([]byte("usage")))
		if count != 1 {
			t.Fatalf("incorrect record count: %d", count)
		}
		return nil
	})
}
