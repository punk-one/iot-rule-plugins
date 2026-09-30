package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	bufferRecordIDMetadataKey = "buffer_record_id"
	bufferNodeIDMetadataKey   = "buffer_node_id"
)

type SinkMQTTConfig struct {
	ConnectorRef string `json:"connector_ref"`
	Target       struct {
		Topic  string `json:"topic"`
		QoS    int    `json:"qos,omitempty"`
		Retain bool   `json:"retain,omitempty"`
	} `json:"target"`
	Encode struct {
		Mode string `json:"mode,omitempty"`
	} `json:"encode,omitempty"`
	Options struct {
		QoS             *int  `json:"qos,omitempty"`
		Retain          *bool `json:"retain,omitempty"`
		WriteTimeoutSec int   `json:"write_timeout_sec,omitempty"`
	} `json:"options,omitempty"`
}

const mqttSinkConfigSchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"type": "object",
	"required": ["connector_ref", "target"],
	"properties": {
		"connector_ref": {
			"type": "string",
			"title": "Connector Ref",
			"description": "ConnectorResource ID"
		},
		"target": {
			"type": "object",
			"title": "Target",
			"required": ["topic"],
			"properties": {
				"topic": {
					"type": "string",
					"title": "Topic Template",
					"description": "MQTT publish topic, supports ${payload.x}, ${metadata.key}, ${event.source}, ${event.rule_id}"
				},
				"qos": {
					"type": "integer",
					"title": "QoS",
					"description": "MQTT Quality of Service: 0=at most once, 1=at least once, 2=exactly once",
					"enum": [0, 1, 2],
					"default": 0
				},
				"retain": {
					"type": "boolean",
					"title": "Retain",
					"description": "Whether the broker should retain the last message on this topic",
					"default": false
				}
			},
			"additionalProperties": false
		},
		"encode": {
			"type": "object",
			"title": "Encoding",
			"properties": {
				"mode": {
					"type": "string",
					"title": "Encode Mode",
					"description": "payload: publish only the event payload; event: publish the full event structure",
					"enum": ["payload", "event"],
					"default": "payload"
				}
			},
			"additionalProperties": false
		},
		"options": {
			"type": "object",
			"title": "Advanced Options",
			"properties": {
				"write_timeout_sec": {
					"type": "integer",
					"title": "Write Timeout (sec)",
					"description": "MQTT publish timeout, default 10s",
					"default": 10,
					"minimum": 1
				}
			},
			"additionalProperties": false
		}
	},
	"additionalProperties": false
}`

func (s *MQTTSink) validateConfig(raw []byte) error {
	if err := json.Unmarshal(raw, &s.cfg); err != nil {
		return err
	}
	s.cfg.ConnectorRef = strings.TrimSpace(s.cfg.ConnectorRef)
	s.cfg.Target.Topic = strings.TrimSpace(s.cfg.Target.Topic)
	s.cfg.Encode.Mode = strings.TrimSpace(strings.ToLower(s.cfg.Encode.Mode))
	if s.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if s.cfg.Target.Topic == "" {
		return fmt.Errorf("target.topic is required")
	}
	if s.cfg.Options.QoS != nil {
		s.cfg.Target.QoS = *s.cfg.Options.QoS
	}
	if s.cfg.Options.Retain != nil {
		s.cfg.Target.Retain = *s.cfg.Options.Retain
	}
	if s.cfg.Options.WriteTimeoutSec <= 0 {
		s.cfg.Options.WriteTimeoutSec = 10
	}
	if s.cfg.Target.QoS < 0 || s.cfg.Target.QoS > 2 {
		s.cfg.Target.QoS = 0
	}
	if s.cfg.Encode.Mode == "" {
		s.cfg.Encode.Mode = "payload"
	}
	if s.cfg.Encode.Mode != "payload" && s.cfg.Encode.Mode != "event" {
		return fmt.Errorf("encode.mode must be 'payload' or 'event'")
	}

	tpl, err := compileTopicTemplate(s.cfg.Target.Topic)
	if err != nil {
		return err
	}
	s.topicTemplate = tpl
	return nil
}
