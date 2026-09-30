package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	redislib "github.com/redis/go-redis/v9"
)

type redisClientStub struct {
	pingErr error

	xreadResponses [][]redislib.XStream
	xreadErrors    []error
	xreadArgs      []*redislib.XReadArgs

	xaddResponse string
	xaddErr      error
	xaddArgs     []*redislib.XAddArgs

	hsetErr    error
	hsetCalls  int
	hsetKey    string
	hsetValues map[string]any

	hsetWithExpireCalls int
	hsetWithExpireKey   string
	hsetWithExpireTTL   time.Duration

	expireErr   error
	expireCalls int
	expireKey   string
	expireTTL   time.Duration

	ttlResult time.Duration
	ttlErr    error

	persistErr   error
	persistCalls int

	deleteErr   error
	deleteCalls int
	deleteKeys  []string
}

func (s *redisClientStub) Ping(context.Context) error {
	return s.pingErr
}

func (s *redisClientStub) XRead(_ context.Context, args *redislib.XReadArgs) ([]redislib.XStream, error) {
	s.xreadArgs = append(s.xreadArgs, args)
	if len(s.xreadResponses) == 0 {
		if len(s.xreadErrors) == 0 {
			return nil, redislib.Nil
		}
		err := s.xreadErrors[0]
		s.xreadErrors = s.xreadErrors[1:]
		return nil, err
	}

	resp := s.xreadResponses[0]
	s.xreadResponses = s.xreadResponses[1:]
	var err error
	if len(s.xreadErrors) > 0 {
		err = s.xreadErrors[0]
		s.xreadErrors = s.xreadErrors[1:]
	}
	return resp, err
}

func (s *redisClientStub) XAdd(_ context.Context, args *redislib.XAddArgs) (string, error) {
	s.xaddArgs = append(s.xaddArgs, args)
	return s.xaddResponse, s.xaddErr
}

func (s *redisClientStub) HSet(_ context.Context, key string, values map[string]any) error {
	s.hsetCalls++
	s.hsetKey = key
	s.hsetValues = values
	return s.hsetErr
}

func (s *redisClientStub) HGetAll(_ context.Context, key string) (map[string]string, error) {
	return nil, nil
}

func (s *redisClientStub) HGet(_ context.Context, key, field string) (string, error) {
	return "", nil
}

func (s *redisClientStub) HSetWithExpire(_ context.Context, key string, values map[string]any, ttl time.Duration) error {
	s.hsetWithExpireCalls++
	s.hsetWithExpireKey = key
	s.hsetWithExpireTTL = ttl
	s.hsetKey = key
	s.hsetValues = values
	return s.hsetErr
}

func (s *redisClientStub) Expire(_ context.Context, key string, ttl time.Duration) error {
	s.expireCalls++
	s.expireKey = key
	s.expireTTL = ttl
	return s.expireErr
}

func (s *redisClientStub) TTL(_ context.Context, key string) (time.Duration, error) {
	return s.ttlResult, s.ttlErr
}

func (s *redisClientStub) Persist(_ context.Context, key string) error {
	s.persistCalls++
	return s.persistErr
}

func (s *redisClientStub) Delete(_ context.Context, keys ...string) error {
	s.deleteCalls++
	s.deleteKeys = keys
	return s.deleteErr
}

func (s *redisClientStub) Close() error {
	return nil
}

func TestConnectorInfoContainsSchema(t *testing.T) {
	info := NewConnector().Info()
	if info.Type != sdk.PluginTypeConnector {
		t.Fatalf("expected connector type, got %s", info.Type)
	}
	if strings.TrimSpace(info.Capabilities.ConfigSchema) == "" {
		t.Fatal("expected config schema")
	}
}

func TestCreateResourceAndMetadataInfo(t *testing.T) {
	connector := NewConnector()
	resource := sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
			"db":   2,
		}),
		Enabled: true,
	}
	handle, err := connector.CreateResource(resource)
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}
	defer func() { _ = connector.DestroyResource(handle) }()

	resp, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestMetadata,
		Target: "info",
	})
	if err != nil {
		t.Fatalf("metadata info: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["plugin_id"] != "connect-redis" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload["database"] != float64(2) {
		t.Fatalf("unexpected database: %+v", payload)
	}
}

func TestCreateResourceValidatesAddr(t *testing.T) {
	connector := NewConnector()
	_, err := connector.CreateResource(sdk.ConnectorResource{
		ID:         "redis-invalid",
		PluginID:   "connect-redis",
		Connection: mustJSON(t, map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "connection.addr is required") {
		t.Fatalf("expected address validation error, got %v", err)
	}
}

func TestExecuteReadTracksStreamCursor(t *testing.T) {
	stub := &redisClientStub{
		xreadResponses: [][]redislib.XStream{
			{{
				Stream: "rules:events",
				Messages: []redislib.XMessage{{
					ID:     "1740000000000-0",
					Values: map[string]any{"payload": `{"value":42}`},
				}},
			}},
			nil,
		},
		xreadErrors: []error{nil, redislib.Nil},
	}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	reqPayload := mustJSON(t, map[string]any{
		"stream":           "rules:events",
		"start_id":         "0-0",
		"count":            1,
		"block_timeout_ms": 50,
	})
	resp, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:          sdk.ConnectorRequestRead,
		ResourceToken: "source-1",
		Payload:       reqPayload,
	})
	if err != nil {
		t.Fatalf("execute read: %v", err)
	}

	var decoded redisReadResponse
	if err := json.Unmarshal(resp.Payload, &decoded); err != nil {
		t.Fatalf("unmarshal read response: %v", err)
	}
	if len(decoded.Messages) != 1 || decoded.Messages[0].ID != "1740000000000-0" {
		t.Fatalf("unexpected read messages: %+v", decoded.Messages)
	}

	if _, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:          sdk.ConnectorRequestRead,
		ResourceToken: "source-1",
		Payload:       reqPayload,
	}); err != nil {
		t.Fatalf("execute second read: %v", err)
	}

	if len(stub.xreadArgs) != 2 {
		t.Fatalf("expected 2 xread calls, got %d", len(stub.xreadArgs))
	}
	if got := stub.xreadArgs[0].Streams[1]; got != "0-0" {
		t.Fatalf("expected first cursor 0-0, got %s", got)
	}
	if got := stub.xreadArgs[1].Streams[1]; got != "1740000000000-0" {
		t.Fatalf("expected second cursor to advance, got %s", got)
	}
}

func TestExecuteWriteUsesXAdd(t *testing.T) {
	stub := &redisClientStub{xaddResponse: "1740000000001-0"}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind: sdk.ConnectorRequestWrite,
		Payload: mustJSON(t, map[string]any{
			"stream":  "rules:events",
			"values":  map[string]any{"payload": map[string]any{"value": 42}},
			"max_len": 1000,
			"approx":  true,
		}),
	})
	if err != nil {
		t.Fatalf("execute write: %v", err)
	}

	if len(stub.xaddArgs) != 1 {
		t.Fatalf("expected one xadd call, got %d", len(stub.xaddArgs))
	}
	if stub.xaddArgs[0].Stream != "rules:events" {
		t.Fatalf("unexpected stream: %+v", stub.xaddArgs[0])
	}
	if payload, ok := stub.xaddArgs[0].Values.(map[string]any)["payload"].(string); !ok || !strings.Contains(payload, `"value":42`) {
		t.Fatalf("expected payload json string, got %#v", stub.xaddArgs[0].Values)
	}
}

func TestExecuteWriteHashUsesHSet(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestWrite,
		Target: "hash_set",
		Payload: mustJSON(t, map[string]any{
			"key": "iot:device:status:dev-1",
			"fields": map[string]any{
				"online":        true,
				"active_events": []map[string]any{{"eventKey": "status.offline"}},
			},
		}),
	})
	if err != nil {
		t.Fatalf("execute hash write: %v", err)
	}
	if stub.hsetCalls != 1 {
		t.Fatalf("expected one hset call, got %d", stub.hsetCalls)
	}
	if stub.hsetKey != "iot:device:status:dev-1" {
		t.Fatalf("unexpected hset key: %s", stub.hsetKey)
	}
	if stub.hsetValues["online"] != true {
		t.Fatalf("unexpected hset values: %+v", stub.hsetValues)
	}
	if raw, ok := stub.hsetValues["active_events"].(string); !ok || !strings.Contains(raw, `"eventKey":"status.offline"`) {
		t.Fatalf("expected active_events JSON string, got %#v", stub.hsetValues["active_events"])
	}
}

func TestOpenStreamReceivesMessagesAndAdvancesCursor(t *testing.T) {
	stub := &redisClientStub{
		xreadResponses: [][]redislib.XStream{
			{{
				Stream: "rules:events",
				Messages: []redislib.XMessage{{
					ID:     "1740000000000-0",
					Values: map[string]any{"payload": `{"value":42}`},
				}},
			}},
			nil,
		},
		xreadErrors: []error{nil, redislib.Nil},
	}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	openResp, err := connector.OpenStream(handle, sdk.StreamOpenRequest{
		SessionKey:     "rule-1:source-redis:redis-1",
		InitialCredits: 2,
		Payload: mustJSON(t, map[string]any{
			"stream":           "rules:events",
			"start_id":         "0-0",
			"count":            2,
			"block_timeout_ms": 50,
		}),
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	recvResp, err := connector.ReceiveStream(handle, sdk.StreamReceiveRequest{
		StreamID:      openResp.StreamID,
		MaxMessages:   1,
		WaitTimeoutMS: 50,
	})
	if err != nil {
		t.Fatalf("receive stream: %v", err)
	}
	if len(recvResp.Messages) != 1 {
		t.Fatalf("expected one stream message, got %#v", recvResp.Messages)
	}
	if recvResp.Messages[0].DeliveryID != "rules:events|1740000000000-0" {
		t.Fatalf("unexpected stream message: %#v", recvResp.Messages[0])
	}

	if err := connector.AckStream(handle, sdk.StreamAckRequest{
		StreamID:   openResp.StreamID,
		DeliveryID: recvResp.Messages[0].DeliveryID,
		Credits:    1,
	}); err != nil {
		t.Fatalf("ack stream: %v", err)
	}

	if _, err := connector.ReceiveStream(handle, sdk.StreamReceiveRequest{
		StreamID:      openResp.StreamID,
		MaxMessages:   1,
		WaitTimeoutMS: 50,
	}); err != nil {
		t.Fatalf("receive second stream batch: %v", err)
	}

	if len(stub.xreadArgs) != 2 {
		t.Fatalf("expected 2 xread calls, got %d", len(stub.xreadArgs))
	}
	if got := stub.xreadArgs[0].Streams[1]; got != "0-0" {
		t.Fatalf("expected first cursor 0-0, got %s", got)
	}
	if got := stub.xreadArgs[1].Streams[1]; got != "1740000000000-0" {
		t.Fatalf("expected second cursor to advance, got %s", got)
	}
}

func TestNackStreamRequeuesMessageWithoutAnotherXRead(t *testing.T) {
	stub := &redisClientStub{
		xreadResponses: [][]redislib.XStream{
			{{
				Stream: "rules:events",
				Messages: []redislib.XMessage{{
					ID:     "1740000000000-0",
					Values: map[string]any{"payload": `{"value":42}`},
				}},
			}},
		},
		xreadErrors: []error{nil},
	}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	openResp, err := connector.OpenStream(handle, sdk.StreamOpenRequest{
		SessionKey:     "rule-1:source-redis:redis-1",
		InitialCredits: 1,
		Payload: mustJSON(t, map[string]any{
			"stream":           "rules:events",
			"start_id":         "0-0",
			"count":            1,
			"block_timeout_ms": 50,
		}),
	})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	first, err := connector.ReceiveStream(handle, sdk.StreamReceiveRequest{
		StreamID:      openResp.StreamID,
		MaxMessages:   1,
		WaitTimeoutMS: 50,
	})
	if err != nil {
		t.Fatalf("receive stream: %v", err)
	}
	if len(first.Messages) != 1 {
		t.Fatalf("expected one stream message, got %#v", first.Messages)
	}

	if err := connector.NackStream(handle, sdk.StreamNackRequest{
		StreamID:   openResp.StreamID,
		DeliveryID: first.Messages[0].DeliveryID,
		Requeue:    true,
		Credits:    1,
	}); err != nil {
		t.Fatalf("nack stream: %v", err)
	}

	second, err := connector.ReceiveStream(handle, sdk.StreamReceiveRequest{
		StreamID:      openResp.StreamID,
		MaxMessages:   1,
		WaitTimeoutMS: 50,
	})
	if err != nil {
		t.Fatalf("receive requeued message: %v", err)
	}
	if len(second.Messages) != 1 || second.Messages[0].DeliveryID != first.Messages[0].DeliveryID {
		t.Fatalf("expected requeued message, got %#v", second.Messages)
	}
	if len(stub.xreadArgs) != 1 {
		t.Fatalf("expected requeue to avoid a second xread, got %d calls", len(stub.xreadArgs))
	}
}

func TestExecuteWriteHashWithTTL(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestWrite,
		Target: "hash_set",
		Payload: mustJSON(t, map[string]any{
			"key": "emc:device:status:dev-1",
			"fields": map[string]any{
				"online": "true",
			},
			"ttl_seconds": 180,
		}),
	})
	if err != nil {
		t.Fatalf("execute hash write with TTL: %v", err)
	}

	if stub.hsetWithExpireCalls != 1 {
		t.Fatalf("expected 1 HSetWithExpire call, got %d", stub.hsetWithExpireCalls)
	}
	if stub.hsetWithExpireKey != "emc:device:status:dev-1" {
		t.Fatalf("expected key emc:device:status:dev-1, got %s", stub.hsetWithExpireKey)
	}
	if stub.hsetWithExpireTTL != 180*time.Second {
		t.Fatalf("expected TTL 180s, got %v", stub.hsetWithExpireTTL)
	}
	if stub.hsetValues["online"] != "true" {
		t.Fatalf("unexpected hset values: %+v", stub.hsetValues)
	}
}

func TestExecuteWriteHashWithoutTTL(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestWrite,
		Target: "hash_set",
		Payload: mustJSON(t, map[string]any{
			"key": "emc:device:status:dev-1",
			"fields": map[string]any{
				"online": "true",
			},
		}),
	})
	if err != nil {
		t.Fatalf("execute hash write without TTL: %v", err)
	}

	if stub.hsetCalls != 1 {
		t.Fatalf("expected 1 HSet call, got %d", stub.hsetCalls)
	}
	if stub.hsetWithExpireCalls != 0 {
		t.Fatalf("expected 0 HSetWithExpire calls, got %d", stub.hsetWithExpireCalls)
	}
	if stub.hsetKey != "emc:device:status:dev-1" {
		t.Fatalf("expected key emc:device:status:dev-1, got %s", stub.hsetKey)
	}
}

func TestExecuteWriteExpire(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestWrite,
		Target: "expire",
		Payload: mustJSON(t, map[string]any{
			"key":         "emc:device:status:dev-1",
			"ttl_seconds": 300,
		}),
	})
	if err != nil {
		t.Fatalf("execute expire: %v", err)
	}

	if stub.expireCalls != 1 {
		t.Fatalf("expected 1 Expire call, got %d", stub.expireCalls)
	}
	if stub.expireKey != "emc:device:status:dev-1" {
		t.Fatalf("expected key emc:device:status:dev-1, got %s", stub.expireKey)
	}
	if stub.expireTTL != 300*time.Second {
		t.Fatalf("expected TTL 300s, got %v", stub.expireTTL)
	}
}

func TestExecuteWritePersist(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestWrite,
		Target: "persist",
		Payload: mustJSON(t, map[string]any{
			"key": "emc:device:status:dev-1",
		}),
	})
	if err != nil {
		t.Fatalf("execute persist: %v", err)
	}

	if stub.persistCalls != 1 {
		t.Fatalf("expected 1 Persist call, got %d", stub.persistCalls)
	}
}

func TestExecuteWriteDel(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestWrite,
		Target: "del",
		Payload: mustJSON(t, map[string]any{
			"key": "emc:device:status:dev-1",
		}),
	})
	if err != nil {
		t.Fatalf("execute del: %v", err)
	}

	if stub.deleteCalls != 1 {
		t.Fatalf("expected 1 Delete call, got %d", stub.deleteCalls)
	}
	if len(stub.deleteKeys) != 1 || stub.deleteKeys[0] != "emc:device:status:dev-1" {
		t.Fatalf("unexpected delete keys: %v", stub.deleteKeys)
	}
}

func TestExecuteReadTTL(t *testing.T) {
	stub := &redisClientStub{ttlResult: 120 * time.Second}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	resp, err := connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestRead,
		Target: "ttl",
		Payload: mustJSON(t, map[string]any{
			"key": "emc:device:status:dev-1",
		}),
	})
	if err != nil {
		t.Fatalf("execute read ttl: %v", err)
	}

	var ttlResp redisTTLResponse
	if err := json.Unmarshal(resp.Payload, &ttlResp); err != nil {
		t.Fatalf("unmarshal ttl response: %v", err)
	}
	if ttlResp.Key != "emc:device:status:dev-1" {
		t.Fatalf("expected key emc:device:status:dev-1, got %s", ttlResp.Key)
	}
	if ttlResp.TTLSeconds != 120 {
		t.Fatalf("expected TTL 120s, got %d", ttlResp.TTLSeconds)
	}
}

func TestExecuteWriteDelRequiresKey(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:    sdk.ConnectorRequestWrite,
		Target:  "del",
		Payload: mustJSON(t, map[string]any{}),
	})
	if err == nil || !strings.Contains(err.Error(), "del.key or del.keys is required") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestExecuteWriteExpireRequiresTTL(t *testing.T) {
	stub := &redisClientStub{}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Execute(handle, sdk.ConnectorRequest{
		Kind:   sdk.ConnectorRequestWrite,
		Target: "expire",
		Payload: mustJSON(t, map[string]any{
			"key": "emc:device:status:dev-1",
		}),
	})
	if err == nil || !strings.Contains(err.Error(), "ttl_seconds > 0 are required") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func swapRedisClientFactory(factory func(connectionConfig) (redisClient, error)) func() {
	previous := newRedisClient
	newRedisClient = factory
	return func() {
		newRedisClient = previous
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
}

func TestProbeReturnsPingError(t *testing.T) {
	stub := &redisClientStub{pingErr: errors.New("boom")}
	restore := swapRedisClientFactory(func(connectionConfig) (redisClient, error) {
		return stub, nil
	})
	defer restore()

	connector := NewConnector()
	handle, err := connector.CreateResource(sdk.ConnectorResource{
		ID:       "redis-1",
		PluginID: "connect-redis",
		Connection: mustJSON(t, map[string]any{
			"addr": "127.0.0.1:6379",
		}),
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("create resource: %v", err)
	}

	_, err = connector.Probe(handle, sdk.ConnectorRequest{})
	if err == nil || !strings.Contains(err.Error(), "ping redis") {
		t.Fatalf("expected probe ping error, got %v", err)
	}
}
