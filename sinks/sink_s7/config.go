package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type S7SinkConfig struct {
	ConnectorRef string `json:"connector_ref"`
	Input        struct {
		DataField string `json:"data_field"`
	} `json:"input"`
	Buffer struct {
		Enabled         bool `json:"enabled"`
		MaxEvents       int  `json:"max_events"`
		FlushIntervalMs int  `json:"flush_interval_ms"`
	} `json:"buffer"`
	Points []SinkPointDef `json:"points"`
}

type SinkPointDef struct {
	Address   string `json:"address"`
	Name      string `json:"name"`
	ValueType string `json:"value_type"`
	MaxLength int    `json:"max_length,omitempty"`
}

const s7SinkConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["connector_ref", "points"],
  "properties": {
    "connector_ref": {"type": "string", "title": "Connector Ref"},
    "priority": {"type": "string", "enum": ["high", "normal"], "default": "high"},
    "usage_weight": {"type": "integer", "minimum": 1, "default": 1},
    "max_usage_weight": {"type": "integer", "minimum": 1},
    "input": {
      "type": "object",
      "properties": {
        "data_field": {"type": "string", "default": "data"}
      },
      "additionalProperties": false
    },
    "buffer": {
      "type": "object",
      "properties": {
        "enabled": {"type": "boolean", "default": true},
        "max_events": {"type": "integer", "minimum": 1, "default": 1000},
        "flush_interval_ms": {"type": "integer", "minimum": 100, "default": 1000}
      },
      "additionalProperties": false
    },
    "points": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "required": ["address", "name", "value_type"],
        "properties": {
          "address": {"type": "string"},
          "name": {"type": "string"},
          "value_type": {"type": "string", "enum": ["bool", "int8", "uint8", "int16", "uint16", "int32", "uint32", "float32", "float64", "string", "wstring", "String", "WString"]},
          "max_length": {"type": "integer", "minimum": 1, "maximum": 254}
        },
        "additionalProperties": false
      }
    }
  },
  "additionalProperties": false
}`

func (s *S7Sink) validateConfig(raw []byte) error {
	if err := json.Unmarshal(raw, &s.cfg); err != nil {
		return err
	}
	s.cfg.ConnectorRef = strings.TrimSpace(s.cfg.ConnectorRef)
	if s.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if s.cfg.Input.DataField == "" {
		s.cfg.Input.DataField = "data"
	}
	if s.cfg.Buffer.MaxEvents <= 0 {
		s.cfg.Buffer.MaxEvents = 1000
	}
	if s.cfg.Buffer.FlushIntervalMs <= 0 {
		s.cfg.Buffer.FlushIntervalMs = 1000
	}
	if len(s.cfg.Points) == 0 {
		return fmt.Errorf("points is required")
	}
	s.pointIndex = make(map[string]SinkPointDef, len(s.cfg.Points))
	for _, p := range s.cfg.Points {
		s.pointIndex[p.Name] = p
	}
	return nil
}
