package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	bolt "go.etcd.io/bbolt"
)

type config struct {
	Storage struct {
		DBPath     string `json:"db_path"`
		MaxRecords int    `json:"max_records"`
		MaxBytes   int64  `json:"max_bytes"`
	} `json:"storage"`
	AckNode string `json:"ack_node_id"`
	RetryMS int    `json:"retry_ms"`
	Rate    int    `json:"rate_per_second"`
	AckBus  struct {
		URL       string `json:"url"`
		Username  string `json:"username"`
		Password  string `json:"password"`
		Token     string `json:"token"`
		CredsFile string `json:"creds_file"`
	} `json:"ack_bus"`
}

type record struct {
	Event       sdk.Event `json:"event"`
	NextAttempt int64     `json:"next_attempt"`
}
type buffer struct {
	sdk.BasePlugin
	cfg    config
	ctx    sdk.RuntimeContext
	db     *bolt.DB
	nc     *nats.Conn
	sub    *nats.Subscription
	mu     sync.RWMutex
	closed bool
	stop   chan struct{}
	wg     sync.WaitGroup
}

func (b *buffer) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "buffer-node", Name: "Persistent Buffer", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded disk queue with sink confirmation and restart replay", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, Stateful: true, ConfigSchema: `{"type":"object","required":["ack_node_id"],"properties":{"ack_node_id":{"type":"string","title":"确认 Sink 节点 ID"},"retry_ms":{"type":"integer","minimum":2000,"default":5000},"rate_per_second":{"type":"integer","minimum":1,"maximum":1000,"default":100},"storage":{"type":"object","properties":{"db_path":{"type":"string"},"max_records":{"type":"integer","default":100000},"max_bytes":{"type":"integer","default":268435456}}}}}`}}
}

func (b *buffer) Init(c sdk.PluginConfig) error {
	if err := json.Unmarshal(c.Raw, &b.cfg); err != nil {
		return err
	}
	if b.cfg.AckNode == "" {
		return fmt.Errorf("ack_node_id is required; one designated downstream sink must confirm delivery")
	}
	if b.cfg.RetryMS < 2000 {
		b.cfg.RetryMS = 5000
	}
	if b.cfg.Rate <= 0 {
		b.cfg.Rate = 100
	}
	if b.cfg.Rate > 1000 {
		return fmt.Errorf("rate_per_second exceeds 1000")
	}
	if b.cfg.Storage.MaxRecords <= 0 {
		b.cfg.Storage.MaxRecords = 100000
	}
	if b.cfg.Storage.MaxBytes <= 0 {
		b.cfg.Storage.MaxBytes = 256 << 20
	}
	if b.cfg.Storage.DBPath == "" {
		b.cfg.Storage.DBPath = "data/buffer.db"
	}
	return nil
}

func (b *buffer) Start(ctx sdk.RuntimeContext) error {
	if b.cfg.AckBus.URL == "" {
		return fmt.Errorf("ack_bus.url must be supplied by the engine")
	}
	b.ctx = ctx
	// Each rule/node owns a separate file, even when a common base path was injected.
	scope := sha256.Sum256([]byte(ctx.RuleID() + "/" + ctx.NodeID()))
	path := fmt.Sprintf("%s.%x", b.cfg.Storage.DBPath, scope[:12])
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	b.db = db
	if err = b.db.Update(func(tx *bolt.Tx) error {
		q, err := tx.CreateBucketIfNotExists([]byte("pending"))
		if err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists([]byte("usage"))
		if err != nil {
			return err
		}
		if err = tx.DeleteBucket([]byte("due")); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		due, err := tx.CreateBucket([]byte("due"))
		if err != nil {
			return err
		}
		var count, bytes uint64
		if err = q.ForEach(func(k, v []byte) error {
			count++
			bytes += uint64(len(k) + len(v))
			var r record
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			return due.Put(dueKey(r.NextAttempt, k), k)
		}); err != nil {
			return err
		}
		return writeUsage(meta, count, bytes)
	}); err != nil {
		_ = db.Close()
		return err
	}
	opts := []nats.Option{nats.Timeout(5 * time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second)}
	if b.cfg.AckBus.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(b.cfg.AckBus.CredsFile))
	}
	if b.cfg.AckBus.Username != "" {
		opts = append(opts, nats.UserInfo(b.cfg.AckBus.Username, b.cfg.AckBus.Password))
	}
	if b.cfg.AckBus.Token != "" {
		opts = append(opts, nats.Token(b.cfg.AckBus.Token))
	}
	nc, err := nats.Connect(b.cfg.AckBus.URL, opts...)
	if err != nil {
		_ = db.Close()
		return err
	}
	b.nc = nc
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		_ = db.Close()
		return err
	}
	b.sub, err = js.Subscribe("rule.ack."+ctx.RuleID(), b.confirm, nats.Durable(fmt.Sprintf("buffer-%x", scope[:12])), nats.ManualAck(), nats.DeliverAll())
	if err != nil {
		nc.Close()
		_ = db.Close()
		return err
	}
	b.stop = make(chan struct{})
	b.wg.Add(1)
	go b.run()
	return ctx.Health().Healthy("disk queue ready", nil)
}

func (b *buffer) OnEvent(in sdk.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed || b.db == nil {
		return fmt.Errorf("buffer stopped")
	}
	e := in.Clone()
	if e.ID == "" {
		e.ID = sdk.NewEvent(nil, nil).ID
	}
	if e.Payload == nil {
		e.Payload = map[string]interface{}{}
	}
	if _, ok := e.Payload["event_id"]; !ok {
		e.Payload["event_id"] = e.ID
	}
	if e.Metadata == nil {
		e.Metadata = map[string]string{}
	}
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(e.ID)))
	e.Metadata["buffer_record_id"] = id
	e.Metadata["buffer_node_id"] = b.ctx.NodeID()
	raw, err := json.Marshal(record{Event: e})
	if err != nil {
		return err
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		q := tx.Bucket([]byte("pending"))
		if q.Get([]byte(id)) != nil {
			return nil
		}
		meta := tx.Bucket([]byte("usage"))
		count, used := readUsage(meta)
		if count >= uint64(b.cfg.Storage.MaxRecords) {
			return fmt.Errorf("persistent buffer record limit reached")
		}
		if used+uint64(len(raw)+len(id)) > uint64(b.cfg.Storage.MaxBytes) {
			return fmt.Errorf("persistent buffer byte limit reached")
		}
		if err := q.Put([]byte(id), raw); err != nil {
			return err
		}
		due, err := tx.CreateBucketIfNotExists([]byte("due"))
		if err != nil {
			return err
		}
		if err = due.Put(dueKey(0, []byte(id)), []byte(id)); err != nil {
			return err
		}
		return writeUsage(meta, count+1, used+uint64(len(raw)+len(id)))
	})
}

func (b *buffer) confirm(msg *nats.Msg) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	var ack sdk.AckMessage
	if json.Unmarshal(msg.Data, &ack) != nil {
		_ = msg.Term()
		return
	}
	if ack.RuleID != b.ctx.RuleID() || ack.BufferNodeID != b.ctx.NodeID() || ack.FromNodeID != b.cfg.AckNode || ack.Status != sdk.AckStatusSuccess {
		_ = msg.Ack()
		return
	}
	err := b.db.Update(func(tx *bolt.Tx) error {
		q := tx.Bucket([]byte("pending"))
		raw := q.Get([]byte(ack.RecordID))
		if raw == nil {
			return nil
		}
		var r record
		if err := json.Unmarshal(raw, &r); err != nil {
			return err
		}
		if r.Event.ID != ack.EventID {
			return fmt.Errorf("confirmation event does not match record")
		}
		if due := tx.Bucket([]byte("due")); due != nil {
			if err := due.Delete(dueKey(r.NextAttempt, []byte(ack.RecordID))); err != nil {
				return err
			}
		}
		meta := tx.Bucket([]byte("usage"))
		count, used := readUsage(meta)
		size := uint64(len(raw) + len(ack.RecordID))
		if err := writeUsage(meta, count-1, used-size); err != nil {
			return err
		}
		return q.Delete([]byte(ack.RecordID))
	})
	if err == nil {
		_ = msg.Ack()
	} else {
		_ = msg.NakWithDelay(time.Second)
	}
}

func (b *buffer) run() {
	defer b.wg.Done()
	ticker := time.NewTicker(time.Second / time.Duration(b.cfg.Rate))
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			var out *sdk.Event
			err := b.db.Update(func(tx *bolt.Tx) error {
				q, due := tx.Bucket([]byte("pending")), tx.Bucket([]byte("due"))
				if due == nil {
					return nil
				}
				key, id := due.Cursor().First()
				now := time.Now().UnixMilli()
				if len(key) < 8 || int64(binary.BigEndian.Uint64(key[:8])) > now {
					return nil
				}
				id = append([]byte(nil), id...)
				oldKey := append([]byte(nil), key...)
				v := q.Get(id)
				if v == nil {
					return due.Delete(oldKey)
				}
				var r record
				if err := json.Unmarshal(v, &r); err != nil {
					return err
				}
				r.NextAttempt = now + int64(b.cfg.RetryMS)
				raw, err := json.Marshal(r)
				if err != nil {
					return err
				}
				meta := tx.Bucket([]byte("usage"))
				count, used := readUsage(meta)
				used = used - uint64(len(v)) + uint64(len(raw))
				if err = writeUsage(meta, count, used); err != nil {
					return err
				}
				if err = q.Put(id, raw); err != nil {
					return err
				}
				if err = due.Delete(oldKey); err != nil {
					return err
				}
				if err = due.Put(dueKey(r.NextAttempt, id), id); err != nil {
					return err
				}
				e := r.Event.Clone()
				out = &e
				return nil
			})
			if err != nil {
				b.ctx.Logger().Error("buffer read failed")
				continue
			}
			if out != nil {
				if err = b.ctx.Emitter().Publish(*out); err != nil {
					b.ctx.Metrics().IncCounter("buffer.publish_failure", nil)
				}
			}
		}
	}
}

func (b *buffer) Stop() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	if b.stop != nil {
		close(b.stop)
	}
	b.mu.Unlock()
	b.wg.Wait()
	if b.sub != nil {
		_ = b.sub.Unsubscribe()
	}
	if b.nc != nil {
		b.nc.Close()
	}
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func readUsage(meta *bolt.Bucket) (uint64, uint64) {
	raw := meta.Get([]byte("totals"))
	if len(raw) != 16 {
		return 0, 0
	}
	return binary.BigEndian.Uint64(raw[:8]), binary.BigEndian.Uint64(raw[8:])
}
func writeUsage(meta *bolt.Bucket, count, bytes uint64) error {
	raw := make([]byte, 16)
	binary.BigEndian.PutUint64(raw[:8], count)
	binary.BigEndian.PutUint64(raw[8:], bytes)
	return meta.Put([]byte("totals"), raw)
}
func dueKey(at int64, id []byte) []byte {
	key := make([]byte, 8+len(id))
	binary.BigEndian.PutUint64(key[:8], uint64(at))
	copy(key[8:], id)
	return key
}
