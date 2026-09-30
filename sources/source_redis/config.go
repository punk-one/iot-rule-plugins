package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type RedisSourceConfig struct {
	ConnectorRef string `json:"connector_ref"`
	Streaming    struct {
		InitialCredits      int    `json:"initial_credits,omitempty"`
		MaxBufferedMessages int    `json:"max_buffered_messages,omitempty"`
		OverflowPolicy      string `json:"overflow_policy,omitempty"`
		AckTimeoutMS        int    `json:"ack_timeout_ms,omitempty"`
		MaxDeliverBatch     int    `json:"max_deliver_batch,omitempty"`
	} `json:"streaming,omitempty"`
	Stream struct {
		Name           string `json:"name"`
		StartID        string `json:"start_id,omitempty"`
		Count          int64  `json:"count,omitempty"`
		BlockTimeoutMS int    `json:"block_timeout_ms,omitempty"`
	} `json:"stream"`
	Decode struct {
		Mode         string `json:"mode,omitempty"`
		PayloadField string `json:"payload_field,omitempty"`
	} `json:"decode,omitempty"`
	Output struct {
		IncludeMeta bool `json:"include_meta,omitempty"`
	} `json:"output,omitempty"`
}

const redisSourceConfigSchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"type": "object",
	"required": ["connector_ref", "stream"],
	"properties": {
		"connector_ref": {"type": "string", "title": "Connector Ref"},
		"streaming": {
			"type": "object",
			"properties": {
				"initial_credits": {"type": "integer", "minimum": 1, "default": 128},
				"max_buffered_messages": {"type": "integer", "minimum": 1, "default": 4096},
				"overflow_policy": {
					"type": "string",
					"enum": ["pause_fetch", "drop_oldest", "drop_newest", "fail_session"],
					"default": "pause_fetch"
				},
				"ack_timeout_ms": {"type": "integer", "minimum": 1, "default": 120000},
				"max_deliver_batch": {"type": "integer", "minimum": 1, "default": 128}
			},
			"additionalProperties": false
		},
		"stream": {
			"type": "object",
			"required": ["name"],
			"properties": {
				"name": {"type": "string", "title": "Stream Name"},
				"start_id": {"type": "string", "title": "Start ID", "default": "$"},
				"count": {"type": "integer", "title": "Count", "minimum": 1, "default": 1},
				"block_timeout_ms": {"type": "integer", "title": "Block Timeout", "minimum": 1, "default": 1000}
			},
			"additionalProperties": false
		},
		"decode": {
			"type": "object",
			"properties": {
				"mode": {"type": "string", "enum": ["auto", "payload_json", "values"], "default": "auto"},
				"payload_field": {"type": "string", "default": "payload"}
			},
			"additionalProperties": false
		},
		"output": {
			"type": "object",
			"properties": {
				"include_meta": {"type": "boolean", "default": true}
			},
			"additionalProperties": false
		}
	},
	"additionalProperties": false
}`

func (s *RedisSource) validateConfig(raw []byte) error {
	if err := json.Unmarshal(raw, &s.cfg); err != nil {
		return err
	}
	s.cfg.ConnectorRef = strings.TrimSpace(s.cfg.ConnectorRef)
	s.cfg.Stream.Name = strings.TrimSpace(s.cfg.Stream.Name)
	s.cfg.Stream.StartID = strings.TrimSpace(s.cfg.Stream.StartID)
	s.cfg.Decode.Mode = strings.TrimSpace(strings.ToLower(s.cfg.Decode.Mode))
	s.cfg.Decode.PayloadField = strings.TrimSpace(s.cfg.Decode.PayloadField)

	if s.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if s.cfg.Stream.Name == "" {
		return fmt.Errorf("stream.name is required")
	}
	if s.cfg.Stream.StartID == "" {
		s.cfg.Stream.StartID = "$"
	}
	if s.cfg.Stream.Count <= 0 {
		s.cfg.Stream.Count = 1
	}
	if s.cfg.Stream.BlockTimeoutMS <= 0 {
		s.cfg.Stream.BlockTimeoutMS = 1000
	}
	if s.cfg.Decode.Mode == "" {
		s.cfg.Decode.Mode = "auto"
	}
	if s.cfg.Decode.PayloadField == "" {
		s.cfg.Decode.PayloadField = "payload"
	}
	if s.cfg.Streaming.InitialCredits <= 0 {
		s.cfg.Streaming.InitialCredits = 128
	}
	if s.cfg.Streaming.MaxBufferedMessages <= 0 {
		s.cfg.Streaming.MaxBufferedMessages = 4096
	}
	switch s.cfg.Streaming.OverflowPolicy {
	case "", "pause_fetch":
		s.cfg.Streaming.OverflowPolicy = "pause_fetch"
	case "drop_oldest", "drop_newest", "fail_session":
	default:
		return fmt.Errorf("streaming.overflow_policy must be one of pause_fetch, drop_oldest, drop_newest, fail_session")
	}
	if s.cfg.Streaming.AckTimeoutMS <= 0 {
		s.cfg.Streaming.AckTimeoutMS = 120000
	}
	if s.cfg.Streaming.MaxDeliverBatch <= 0 {
		s.cfg.Streaming.MaxDeliverBatch = 128
	}
	return nil
}
