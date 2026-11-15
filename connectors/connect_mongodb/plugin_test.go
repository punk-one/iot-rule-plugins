package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"go.mongodb.org/mongo-driver/v2/bson"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type wireServer struct {
	listener    net.Listener
	mu          sync.Mutex
	connections map[net.Conn]bool
	wg          sync.WaitGroup
	documents   map[string]bson.D
	rows        bson.A
	holdFind    bool
	inserts     atomic.Int32
	accepts     atomic.Int32
	active      atomic.Int32
	finds       atomic.Int32
}

func startWire(t *testing.T) *wireServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &wireServer{listener: listener, connections: map[net.Conn]bool{}, documents: map[string]bson.D{}, rows: bson.A{bson.D{{Key: "value", Value: int64(9007199254740993)}}}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.connections[conn] = true
			s.mu.Unlock()
			s.accepts.Add(1)
			s.active.Add(1)
			s.wg.Add(1)
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		s.mu.Lock()
		for conn := range s.connections {
			conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}
func (s *wireServer) serve(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer func() { s.active.Add(-1); s.mu.Lock(); delete(s.connections, conn); s.mu.Unlock() }()
	for {
		header := make([]byte, 16)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		size := int(binary.LittleEndian.Uint32(header))
		if size < 16 || size > 4*1048576 {
			return
		}
		body := make([]byte, size-16)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		opcode := binary.LittleEndian.Uint32(header[12:])
		var command bson.D
		sequences := map[string]bson.A{}
		if opcode == 2004 {
			if len(body) < 5 {
				return
			}
			end := bytes.IndexByte(body[4:], 0)
			if end < 0 {
				return
			}
			at := 4 + end + 1 + 8
			if at >= len(body) || bson.Unmarshal(body[at:], &command) != nil {
				return
			}
		} else if opcode == 2013 {
			if len(body) < 5 {
				return
			}
			at := 4
			for at < len(body) {
				kind := body[at]
				at++
				if kind == 0 {
					if at+4 > len(body) {
						return
					}
					n := int(binary.LittleEndian.Uint32(body[at:]))
					if n < 5 || at+n > len(body) || bson.Unmarshal(body[at:at+n], &command) != nil {
						return
					}
					at += n
				} else if kind == 1 {
					start := at
					if at+4 > len(body) {
						return
					}
					n := int(binary.LittleEndian.Uint32(body[at:]))
					if n < 5 || at+n > len(body) {
						return
					}
					at += 4
					nameEnd := bytes.IndexByte(body[at:start+n], 0)
					if nameEnd < 0 {
						return
					}
					name := string(body[at : at+nameEnd])
					at += nameEnd + 1
					for at < start+n {
						if at+4 > len(body) {
							return
						}
						length := int(binary.LittleEndian.Uint32(body[at:]))
						if length < 5 || at+length > start+n {
							return
						}
						var doc bson.D
						if bson.Unmarshal(body[at:at+length], &doc) != nil {
							return
						}
						sequences[name] = append(sequences[name], doc)
						at += length
					}
				} else {
					return
				}
			}
		} else {
			return
		}
		if len(command) == 0 {
			return
		}
		result := s.respond(command, sequences)
		if result == nil {
			continue
		}
		raw, err := bson.Marshal(result)
		if err != nil {
			return
		}
		response := make([]byte, 16)
		binary.LittleEndian.PutUint32(response[4:], 1)
		copy(response[8:12], header[4:8])
		if opcode == 2004 {
			binary.LittleEndian.PutUint32(response[12:], 1)
			response = append(response, make([]byte, 20)...)
			binary.LittleEndian.PutUint32(response[32:], 1)
			response = append(response, raw...)
		} else {
			binary.LittleEndian.PutUint32(response[12:], 2013)
			response = append(response, 0, 0, 0, 0, 0)
			response = append(response, raw...)
		}
		binary.LittleEndian.PutUint32(response, uint32(len(response)))
		if _, err := conn.Write(response); err != nil {
			return
		}
	}
}
func value(doc bson.D, key string) any {
	for _, e := range doc {
		if e.Key == key {
			return e.Value
		}
	}
	return nil
}
func (s *wireServer) respond(command bson.D, sequences map[string]bson.A) bson.D {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch command[0].Key {
	case "isMaster", "ismaster", "hello":
		return bson.D{{Key: "ok", Value: 1}, {Key: "isWritablePrimary", Value: true}, {Key: "ismaster", Value: true}, {Key: "minWireVersion", Value: 0}, {Key: "maxWireVersion", Value: 25}, {Key: "logicalSessionTimeoutMinutes", Value: 30}, {Key: "maxBsonObjectSize", Value: 2 * 1048576}, {Key: "maxMessageSizeBytes", Value: 4 * 1048576}, {Key: "maxWriteBatchSize", Value: 1000}}
	case "find":
		s.finds.Add(1)
		if s.holdFind {
			return nil
		}
		rows := s.rows
		if filter, ok := value(command, "filter").(bson.D); ok {
			if id, ok := value(filter, "_id").(string); ok {
				rows = bson.A{}
				if doc := s.documents[id]; doc != nil {
					rows = append(rows, doc)
				}
			}
		}
		return bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "test.events"}, {Key: "firstBatch", Value: rows}}}}
	case "insert":
		docs := sequences["documents"]
		if len(docs) == 0 {
			docs, _ = value(command, "documents").(bson.A)
		}
		if len(docs) != 1 {
			return bson.D{{Key: "ok", Value: 0}, {Key: "errmsg", Value: "one doc required"}, {Key: "code", Value: 2}}
		}
		doc, ok := docs[0].(bson.D)
		if !ok {
			return nil
		}
		id, _ := value(doc, "_id").(string)
		if s.documents[id] != nil {
			return bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}, {Key: "writeErrors", Value: bson.A{bson.D{{Key: "index", Value: 0}, {Key: "code", Value: 11000}, {Key: "errmsg", Value: "duplicate"}}}}}
		}
		if concern, ok := value(command, "writeConcern").(bson.D); !ok || value(concern, "w") != "majority" {
			return bson.D{{Key: "ok", Value: 0}, {Key: "errmsg", Value: "majority required"}, {Key: "code", Value: 2}}
		}
		s.documents[id] = doc
		s.inserts.Add(1)
		return bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}
	default:
		return bson.D{{Key: "ok", Value: 1}}
	}
}
func wireResource(t *testing.T, s *wireServer) (*connector, string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"hosts": []string{s.listener.Addr().String()}, "database": "test", "collections": []string{"events"}, "tls": false, "allow_write": true, "timeout_ms": 1000})
	c := NewConnector().(*connector)
	h, err := c.CreateResource(sdk.ConnectorResource{ID: "db", Connection: raw})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Stop() })
	return c, h
}
func run(t *testing.T, c *connector, h, target string, payload any, timeout int) (sdk.ConnectorResponse, error) {
	t.Helper()
	raw, _ := json.Marshal(payload)
	kind := sdk.ConnectorRequestRead
	if target == "insert_event" {
		kind = sdk.ConnectorRequestWrite
	}
	return c.Execute(h, sdk.ConnectorRequest{Kind: kind, Target: target, Payload: raw, TimeoutMS: timeout})
}
func TestActualDriverWireAndIdempotentMajorityInsert(t *testing.T) {
	s := startWire(t)
	c, h := wireResource(t, s)
	request := map[string]any{"collection": "events", "event_id": "stable", "payload": map[string]any{"x": 1}}
	for i := 0; i < 2; i++ {
		response, err := run(t, c, h, "insert_event", request, 0)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		json.Unmarshal(response.Payload, &result)
		if result["committed"] != true || result["duplicate"] != (i == 1) {
			t.Fatal(string(response.Payload))
		}
	}
	if s.inserts.Load() != 1 {
		t.Fatal(s.inserts.Load())
	}
	request["payload"] = map[string]any{"x": 2}
	if _, err := run(t, c, h, "insert_event", request, 0); err == nil {
		t.Fatal("stable ID accepted different bytes")
	}
	response, err := run(t, c, h, "find", map[string]any{"collection": "events", "filter": map[string]any{}, "limit": 10}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(response.Payload, []byte(`"$numberLong":"9007199254740993"`)) {
		t.Fatal(string(response.Payload))
	}
	if s.accepts.Load() > 3 {
		t.Fatal("pool not shared", s.accepts.Load())
	}
	c.DestroyResource(h)
	deadline := time.Now().Add(time.Second)
	for s.active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.active.Load() != 0 {
		t.Fatal("database sockets retained")
	}
}
func TestUnsafeRequestsRejectedBeforeNetwork(t *testing.T) {
	s := startWire(t)
	c, h := wireResource(t, s)
	for _, filter := range []string{`{"$where":"evil"}`, `{"$expr":{"$function":{}}}`, `{"$unknown":1}`, `{"x":1,"x":2}`} {
		if _, err := run(t, c, h, "find", map[string]any{"collection": "events", "filter": json.RawMessage(filter)}, 0); err == nil {
			t.Fatal(filter)
		}
	}
	if _, err := run(t, c, h, "find", map[string]any{"collection": "forbidden"}, 0); err == nil {
		t.Fatal("collection bypass")
	}
	if s.accepts.Load() != 0 {
		t.Fatal("rejected request dialed")
	}
}
func TestResultLimitAndDeadlineDiscardPartial(t *testing.T) {
	s := startWire(t)
	c, h := wireResource(t, s)
	if _, err := run(t, c, h, "find", map[string]any{"collection": "events", "limit": 1}, 0); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.rows = append(s.rows, bson.D{{Key: "x", Value: 2}})
	s.mu.Unlock()
	if _, err := run(t, c, h, "find", map[string]any{"collection": "events", "limit": 1}, 0); err == nil {
		t.Fatal("partial rows accepted")
	}
	s.mu.Lock()
	s.holdFind = true
	s.mu.Unlock()
	started := time.Now()
	if _, err := run(t, c, h, "find", map[string]any{"collection": "events"}, 60); err == nil {
		t.Fatal("hung cursor accepted")
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("deadline ignored")
	}
}
func TestFilterTypedBoundsAndConfiguration(t *testing.T) {
	if _, err := parseFilter(json.RawMessage(`{"value":{"$gte":{"$numberLong":"9007199254740993"}}}`)); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"example.invalid:27017", "mongodb://x:1/", "127.0.0.1:invalid"} {
		raw, _ := json.Marshal(map[string]any{"hosts": []string{address}, "database": "test", "collections": []string{"events"}, "tls": false})
		if _, err := NewConnector().CreateResource(sdk.ConnectorResource{ID: "db", Connection: raw}); err == nil {
			t.Fatal(address)
		}
	}
}

func TestDeepAndUnsafeIntegerInputRejectedBeforeBSON(t *testing.T) {
	for _, raw := range []string{`{"x":9007199254740993}`, `{"x":18446744073709551615}`, string(bytes.Repeat([]byte("["), 100)) + "0" + string(bytes.Repeat([]byte("]"), 100))} {
		if boundedJSON([]byte(raw)) == nil {
			t.Fatal("unbounded input accepted")
		}
	}
}
