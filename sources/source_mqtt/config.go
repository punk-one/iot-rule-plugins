package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type mqttExtractField struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type MQTTSourceConfig struct {
	ConnectorRef string `json:"connector_ref"`
	UsageWeight  int    `json:"usage_weight,omitempty"`
	Stream       struct {
		InitialCredits      int    `json:"initial_credits,omitempty"`
		MaxBufferedMessages int    `json:"max_buffered_messages,omitempty"`
		OverflowPolicy      string `json:"overflow_policy,omitempty"`
		AckTimeoutMS        int    `json:"ack_timeout_ms,omitempty"`
		MaxDeliverBatch     int    `json:"max_deliver_batch,omitempty"`
	} `json:"stream,omitempty"`
	Process struct {
		Subscribe struct {
			Topics []string `json:"topics"`
			QoS    int      `json:"qos,omitempty"`
		} `json:"subscribe"`
		Decode struct {
			Format string `json:"format"`
		} `json:"decode,omitempty"`
		Extract struct {
			Fields []mqttExtractField `json:"fields,omitempty"`
		} `json:"extract,omitempty"`
	} `json:"process"`
	Polling struct {
		WaitTimeoutMS int `json:"wait_timeout_ms,omitempty"`
		MaxBackoffMS  int `json:"max_backoff_ms,omitempty"`
	} `json:"polling,omitempty"`
}

const (
	mqttSourceHealthCodeReadFailed = "MQTT_SOURCE_READ_FAILED"
)

var extractTargetPattern = regexp.MustCompile(`^(payload\.[^.]+(?:\.[^.]+)*|metadata\.[^.]+)$`)

const mqttSourceConfigSchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"type": "object",
	"required": ["connector_ref", "process"],
	"properties": {
		"connector_ref": {
			"type": "string",
			"title": "Connector Ref",
			"description": "ConnectorResource ID"
		},
		"usage_weight": {
			"type": "integer",
			"title": "Usage Weight",
			"minimum": 1,
			"default": 1
		},
		"stream": {
			"type": "object",
			"properties": {
				"initial_credits": {"type": "integer", "minimum": 1, "default": 64},
				"max_buffered_messages": {"type": "integer", "minimum": 1, "default": 4096},
				"overflow_policy": {
					"type": "string",
					"enum": ["drop_oldest", "drop_newest", "fail_session", "pause_fetch"],
					"default": "drop_oldest"
				},
				"ack_timeout_ms": {"type": "integer", "minimum": 1, "default": 120000},
				"max_deliver_batch": {"type": "integer", "minimum": 1, "default": 64}
			},
			"additionalProperties": false
		},
		"process": {
			"type": "object",
			"required": ["subscribe"],
			"properties": {
				"subscribe": {
					"type": "object",
					"required": ["topics"],
					"properties": {
						"topics": {
							"type": "array",
							"items": {"type": "string"},
							"minItems": 1
						},
						"qos": {
							"type": "integer",
							"enum": [0, 1, 2],
							"default": 0
						}
					},
					"additionalProperties": false
				},
				"decode": {
					"type": "object",
					"properties": {
						"format": {
							"type": "string",
							"enum": ["auto", "json", "text", "binary"],
							"default": "auto"
						}
					},
					"additionalProperties": false
				},
				"extract": {
					"type": "object",
					"properties": {
						"fields": {
							"type": "array",
							"items": {
								"type": "object",
								"required": ["from", "to"],
								"properties": {
									"from": {
										"type": "string",
										"pattern": "^topic\\[\\d+\\]$"
									},
									"to": {
										"type": "string",
										"pattern": "^(payload\\.[^.]+(?:\\.[^.]+)*|metadata\\.[^.]+)$"
									}
								},
								"additionalProperties": false
							}
						}
					},
					"additionalProperties": false
				}
			},
			"additionalProperties": false
		},
		"polling": {
			"type": "object",
			"properties": {
				"wait_timeout_ms": {
					"type": "integer",
					"minimum": 100,
					"default": 1000
				},
				"max_backoff_ms": {
					"type": "integer",
					"minimum": 100,
					"default": 10000
				}
			},
			"additionalProperties": false
		}
	},
	"additionalProperties": false
}`

func (m *MQTTSource) validateConfig(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m.cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	m.cfg.ConnectorRef = strings.TrimSpace(m.cfg.ConnectorRef)
	if m.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if len(m.cfg.Process.Subscribe.Topics) == 0 {
		return fmt.Errorf("process.subscribe.topics is required and must not be empty")
	}
	for i := range m.cfg.Process.Subscribe.Topics {
		m.cfg.Process.Subscribe.Topics[i] = strings.TrimSpace(m.cfg.Process.Subscribe.Topics[i])
		if m.cfg.Process.Subscribe.Topics[i] == "" {
			return fmt.Errorf("process.subscribe.topics[%d] is empty", i)
		}
	}
	if m.cfg.Process.Decode.Format == "" {
		m.cfg.Process.Decode.Format = "auto"
	}
	for i := range m.cfg.Process.Extract.Fields {
		field := &m.cfg.Process.Extract.Fields[i]
		field.From = strings.TrimSpace(field.From)
		field.To = strings.TrimSpace(field.To)
		if field.From == "" {
			return fmt.Errorf("process.extract.fields[%d].from is required", i)
		}
		if !topicIndexRulePattern.MatchString(field.From) {
			return fmt.Errorf("process.extract.fields[%d].from must match topic[n]", i)
		}
		if field.To == "" {
			return fmt.Errorf("process.extract.fields[%d].to is required", i)
		}
		if !extractTargetPattern.MatchString(field.To) {
			return fmt.Errorf("process.extract.fields[%d].to must start with payload. or metadata.", i)
		}
	}
	if m.cfg.Process.Subscribe.QoS < 0 || m.cfg.Process.Subscribe.QoS > 2 {
		m.cfg.Process.Subscribe.QoS = 0
	}
	if m.cfg.Stream.InitialCredits <= 0 {
		m.cfg.Stream.InitialCredits = 64
	}
	if m.cfg.Stream.MaxBufferedMessages <= 0 {
		m.cfg.Stream.MaxBufferedMessages = 4096
	}
	switch m.cfg.Stream.OverflowPolicy {
	case "", "drop_oldest":
		m.cfg.Stream.OverflowPolicy = "drop_oldest"
	case "drop_newest", "fail_session", "pause_fetch":
	default:
		return fmt.Errorf("stream.overflow_policy must be one of drop_oldest, drop_newest, fail_session, pause_fetch")
	}
	if m.cfg.Stream.AckTimeoutMS <= 0 {
		m.cfg.Stream.AckTimeoutMS = 120000
	}
	if m.cfg.Stream.MaxDeliverBatch <= 0 {
		m.cfg.Stream.MaxDeliverBatch = 64
	}
	if m.cfg.Polling.WaitTimeoutMS <= 0 {
		m.cfg.Polling.WaitTimeoutMS = 1000
	}
	if m.cfg.Polling.MaxBackoffMS <= 0 {
		m.cfg.Polling.MaxBackoffMS = 10000
	}
	return nil
}
