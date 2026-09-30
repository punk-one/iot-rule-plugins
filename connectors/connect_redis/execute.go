package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	redislib "github.com/redis/go-redis/v9"
)

type redisReadPayload struct {
	Stream         string `json:"stream"`
	StartID        string `json:"start_id,omitempty"`
	Count          int64  `json:"count,omitempty"`
	BlockTimeoutMS int    `json:"block_timeout_ms,omitempty"`
}

type redisReadResponse struct {
	Messages []redisStreamMessage `json:"messages"`
}

type redisHashReadPayload struct {
	Key   string `json:"key"`
	Field string `json:"field,omitempty"`
}

type redisHashReadResponse struct {
	Key    string            `json:"key"`
	Fields map[string]string `json:"fields"`
}

type redisWritePayload struct {
	Stream string         `json:"stream"`
	ID     string         `json:"id,omitempty"`
	MaxLen int64          `json:"max_len,omitempty"`
	Approx bool           `json:"approx,omitempty"`
	Values map[string]any `json:"values"`
}

type redisHashWritePayload struct {
	Key        string         `json:"key"`
	Fields     map[string]any `json:"fields"`
	TTLSeconds int64          `json:"ttl_seconds,omitempty"`
}

// expire payload
type redisExpirePayload struct {
	Key        string `json:"key"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

// key payload (用于 persist/del/ttl)
type redisKeyPayload struct {
	Key  string   `json:"key"`
	Keys []string `json:"keys,omitempty"`
}

// ttl response
type redisTTLResponse struct {
	Key        string `json:"key"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

func (c *redisConnector) executeMetadata(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	switch req.Target {
	case "", "info":
		return buildJSONResponse(map[string]any{
			"resource_id": resource.resource.ID,
			"plugin_id":   c.Info().ID,
			"driver":      "redis",
			"database":    resource.cfg.DB,
			"addr":        resource.cfg.Addr,
			"supported_targets": []string{
				"metadata/info",
				"metadata/ping",
				"read/stream",
				"read/hash_getall",
				"read/hash_get",
				"read/ttl",
				"write/stream_add",
				"write/hash_set",
				"write/expire",
				"write/persist",
				"write/del",
			},
		}, map[string]string{
			"resource_id": resource.resource.ID,
			"plugin_id":   c.Info().ID,
		})
	case "ping":
		client, err := c.openClient(resource)
		if err != nil {
			return sdk.ConnectorResponse{}, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, resource.cfg.dialTimeout()))
		defer cancel()
		if err := client.Ping(ctx); err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("ping redis: %w", err)
		}
		return buildJSONResponse(map[string]any{"ok": true}, map[string]string{"status": "healthy"})
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported metadata target %q", req.Target)
	}
}

func (c *redisConnector) probe(resource *redisResource, req sdk.ConnectorRequest) (sdk.ResourceStatusEvent, error) {
	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ResourceStatusEvent{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, resource.cfg.dialTimeout()))
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		return sdk.ResourceStatusEvent{}, fmt.Errorf("ping redis: %w", err)
	}

	return sdk.ResourceStatusEvent{
		ResourceID:         resource.resource.ID,
		ProviderPluginID:   c.Info().ID,
		Status:             sdk.ResourceStatusHealthy,
		ReasonCode:         "READY",
		Message:            "redis reachable",
		ObservedAtUnixNano: time.Now().UnixNano(),
	}, nil
}

func (c *redisConnector) executeRead(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	switch req.Target {
	case "", "stream":
		return c.executeReadStream(resource, req)
	case "hash_getall", "hash_get":
		return c.executeReadHash(resource, req)
	case "ttl":
		return c.executeReadTTL(resource, req)
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported read target %q", req.Target)
	}
}

func (c *redisConnector) executeReadStream(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisReadPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse read payload: %w", err)
	}
	payload.Stream = stringsTrim(payload.Stream)
	payload.StartID = stringsTrim(payload.StartID)
	if payload.Stream == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("read.stream is required")
	}
	if payload.Count <= 0 {
		payload.Count = 1
	}
	if payload.BlockTimeoutMS <= 0 {
		payload.BlockTimeoutMS = 1000
	}

	token := stringsTrim(req.ResourceToken)
	if token == "" {
		token = resource.resource.ID
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	cursor := c.cursorForStream(resource, token, payload.Stream, payload.StartID)
	blockTimeout := time.Duration(payload.BlockTimeoutMS) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, blockTimeout+2*time.Second))
	defer cancel()

	streams, err := client.XRead(ctx, &redislib.XReadArgs{
		Streams: []string{payload.Stream, cursor},
		Count:   payload.Count,
		Block:   blockTimeout,
	})
	if err != nil {
		if errors.Is(err, redislib.Nil) {
			return buildJSONResponse(redisReadResponse{Messages: []redisStreamMessage{}}, nil)
		}
		return sdk.ConnectorResponse{}, fmt.Errorf("xread stream %s: %w", payload.Stream, err)
	}

	messages := make([]redisStreamMessage, 0)
	for _, stream := range streams {
		lastID := cursor
		for _, message := range stream.Messages {
			lastID = message.ID
			messages = append(messages, redisStreamMessage{
				Stream:     stream.Stream,
				ID:         message.ID,
				Values:     normalizeRedisValues(message.Values),
				ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano),
			})
		}
		if lastID != cursor {
			c.setCursor(resource, token, stream.Stream, lastID)
		}
	}

	return buildJSONResponse(redisReadResponse{Messages: messages}, nil)
}

func (c *redisConnector) executeWrite(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	switch req.Target {
	case "", "stream_add":
		return c.executeWriteStream(resource, req)
	case "hash_set":
		return c.executeWriteHash(resource, req)
	case "expire":
		return c.executeWriteExpire(resource, req)
	case "persist":
		return c.executeWritePersist(resource, req)
	case "del":
		return c.executeWriteDel(resource, req)
	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported write target %q", req.Target)
	}
}

func (c *redisConnector) executeWriteStream(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisWritePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse write payload: %w", err)
	}
	payload.Stream = stringsTrim(payload.Stream)
	if payload.Stream == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.stream is required")
	}
	if len(payload.Values) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.values is required")
	}

	values := normalizeRedisValues(payload.Values)
	if len(values) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.values must contain at least one non-empty field")
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 10*time.Second))
	defer cancel()
	entryID, err := client.XAdd(ctx, &redislib.XAddArgs{
		Stream: payload.Stream,
		ID:     payload.ID,
		MaxLen: payload.MaxLen,
		Approx: payload.Approx,
		Values: values,
	})
	if err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("xadd stream %s: %w", payload.Stream, err)
	}

	return buildJSONResponse(map[string]any{"entry_id": entryID}, nil)
}

func (c *redisConnector) executeWriteHash(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisHashWritePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse hash write payload: %w", err)
	}
	payload.Key = stringsTrim(payload.Key)
	if payload.Key == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.key is required")
	}
	if len(payload.Fields) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.fields is required")
	}

	fields := normalizeRedisValues(payload.Fields)
	if len(fields) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("write.fields must contain at least one non-empty field")
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 10*time.Second))
	defer cancel()

	if payload.TTLSeconds > 0 {
		// TxPipeline: HSet + Expire in one RTT
		if err := client.HSetWithExpire(ctx, payload.Key, fields, time.Duration(payload.TTLSeconds)*time.Second); err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("hset+expire key %s: %w", payload.Key, err)
		}
	} else {
		if err := client.HSet(ctx, payload.Key, fields); err != nil {
			return sdk.ConnectorResponse{}, fmt.Errorf("hset key %s: %w", payload.Key, err)
		}
	}

	return buildJSONResponse(map[string]any{"key": payload.Key, "fields_written": len(fields)}, nil)
}

func (c *redisConnector) executeWriteExpire(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisExpirePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse expire payload: %w", err)
	}
	payload.Key = stringsTrim(payload.Key)
	if payload.Key == "" || payload.TTLSeconds <= 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("expire.key and expire.ttl_seconds > 0 are required")
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 10*time.Second))
	defer cancel()
	if err := client.Expire(ctx, payload.Key, time.Duration(payload.TTLSeconds)*time.Second); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("expire key %s: %w", payload.Key, err)
	}

	return buildJSONResponse(map[string]any{"key": payload.Key, "ttl_seconds": payload.TTLSeconds}, nil)
}

func (c *redisConnector) executeWritePersist(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisKeyPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse persist payload: %w", err)
	}
	payload.Key = stringsTrim(payload.Key)
	if payload.Key == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("persist.key is required")
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 10*time.Second))
	defer cancel()
	if err := client.Persist(ctx, payload.Key); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("persist key %s: %w", payload.Key, err)
	}

	return buildJSONResponse(map[string]any{"key": payload.Key, "persisted": true}, nil)
}

func (c *redisConnector) executeWriteDel(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisKeyPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse del payload: %w", err)
	}
	keys := make([]string, 0)
	if payload.Key != "" {
		keys = append(keys, stringsTrim(payload.Key))
	}
	for _, k := range payload.Keys {
		keys = append(keys, stringsTrim(k))
	}
	if len(keys) == 0 {
		return sdk.ConnectorResponse{}, fmt.Errorf("del.key or del.keys is required")
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, 10*time.Second))
	defer cancel()
	if err := client.Delete(ctx, keys...); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("del keys %v: %w", keys, err)
	}

	return buildJSONResponse(map[string]any{"deleted": len(keys)}, nil)
}

func (c *redisConnector) executeReadTTL(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisKeyPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse ttl payload: %w", err)
	}
	payload.Key = stringsTrim(payload.Key)
	if payload.Key == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("ttl.key is required")
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, resource.cfg.readTimeout()))
	defer cancel()
	d, err := client.TTL(ctx, payload.Key)
	if err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("ttl key %s: %w", payload.Key, err)
	}

	return buildJSONResponse(redisTTLResponse{
		Key:        payload.Key,
		TTLSeconds: int64(d.Seconds()),
	}, nil)
}

func (c *redisConnector) executeReadHash(resource *redisResource, req sdk.ConnectorRequest) (sdk.ConnectorResponse, error) {
	var payload redisHashReadPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return sdk.ConnectorResponse{}, fmt.Errorf("parse hash read payload: %w", err)
	}
	payload.Key = stringsTrim(payload.Key)
	if payload.Key == "" {
		return sdk.ConnectorResponse{}, fmt.Errorf("read.hash key is required")
	}

	client, err := c.openClient(resource)
	if err != nil {
		return sdk.ConnectorResponse{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout(req.TimeoutMS, resource.cfg.readTimeout()))
	defer cancel()

	switch req.Target {
	case "hash_getall":
		result, err := client.HGetAll(ctx, payload.Key)
		if err != nil {
			if errors.Is(err, redislib.Nil) {
				return buildJSONResponse(redisHashReadResponse{Key: payload.Key, Fields: map[string]string{}}, nil)
			}
			return sdk.ConnectorResponse{}, fmt.Errorf("hgetall key %s: %w", payload.Key, err)
		}
		return buildJSONResponse(redisHashReadResponse{Key: payload.Key, Fields: result}, nil)

	case "hash_get":
		payload.Field = stringsTrim(payload.Field)
		if payload.Field == "" {
			return sdk.ConnectorResponse{}, fmt.Errorf("read.hash_get field is required")
		}
		val, err := client.HGet(ctx, payload.Key, payload.Field)
		if err != nil {
			if errors.Is(err, redislib.Nil) {
				return buildJSONResponse(redisHashReadResponse{Key: payload.Key, Fields: map[string]string{}}, nil)
			}
			return sdk.ConnectorResponse{}, fmt.Errorf("hget key %s field %s: %w", payload.Key, payload.Field, err)
		}
		return buildJSONResponse(redisHashReadResponse{Key: payload.Key, Fields: map[string]string{payload.Field: val}}, nil)

	default:
		return sdk.ConnectorResponse{}, fmt.Errorf("unsupported hash read target %q", req.Target)
	}
}

func normalizeRedisValues(values map[string]any) map[string]any {
	normalized := make(map[string]any, len(values))
	for key, value := range values {
		key = stringsTrim(key)
		if key == "" {
			continue
		}
		normalized[key] = normalizeRedisValue(value)
	}
	return normalized
}

func normalizeRedisValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return typed
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return ""
	}
	switch rv.Kind() {
	case reflect.Map, reflect.Struct, reflect.Slice, reflect.Array:
		raw, err := json.Marshal(value)
		if err == nil {
			return string(raw)
		}
	}

	return fmt.Sprintf("%v", value)
}
