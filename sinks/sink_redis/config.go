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

type RedisSinkConfig struct {
	ConnectorRef string `json:"connector_ref"`
	Target       struct {
		Mode      string `json:"mode,omitempty"`
		Stream    string `json:"stream,omitempty"`
		Key       string `json:"key,omitempty"`
		KeyPrefix string `json:"key_prefix,omitempty"`
		KeyField  string `json:"key_field,omitempty"`
		KeyMode   string `json:"key_mode,omitempty"`
	} `json:"target"`
	Encode struct {
		FieldMappings   []RedisFieldMapping `json:"field_mappings,omitempty"`
		ExpandObject    string              `json:"expand_object,omitempty"`
		IncludeMetadata bool                `json:"include_metadata,omitempty"`
		IncludeContext  bool                `json:"include_context,omitempty"`
	} `json:"encode,omitempty"`
	Options struct {
		WriteTimeoutSec int   `json:"write_timeout_sec,omitempty"`
		MaxLen          int64 `json:"max_len,omitempty"`
		Approx          bool  `json:"approx,omitempty"`
	} `json:"options,omitempty"`
}

type RedisFieldMapping struct {
	Field string `json:"field,omitempty"`
	Path  string `json:"path,omitempty"`
}

const redisSinkConfigSchema = `{
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
			"properties": {
				"mode": {
					"type": "string",
					"title": "Target Mode",
					"description": "hash: merge-update Redis Hash fields; stream: append to Redis Stream",
					"enum": ["stream", "hash"],
					"default": "hash"
				},
				"stream": {
					"type": "string",
					"title": "Stream Name",
					"description": "Redis Stream name, used only when mode=stream"
				},
				"key_mode": {
					"type": "string",
					"title": "Key Mode",
					"description": "fixed: use a static key name; dynamic: resolve key from event path",
					"enum": ["fixed", "dynamic"],
					"default": "dynamic"
				},
				"key": {
					"type": "string",
					"title": "Fixed Key",
					"description": "Static Redis Hash key, used as fallback when dynamic key does not resolve"
				},
				"key_prefix": {
					"type": "string",
					"title": "Key Prefix",
					"description": "Optional prefix prepended to the resolved key, e.g. iot:device:status:"
				},
				"key_field": {
					"type": "string",
					"title": "Dynamic Key Path",
					"description": "Event path to extract the hash key, e.g. payload.device_code"
				}
			},
			"additionalProperties": false
		},
		"encode": {
			"type": "object",
			"title": "Encoding",
			"properties": {
				"field_mappings": {
					"type": "array",
					"title": "Field Mappings",
					"description": "Manual Hash field → event path mappings, used with or instead of expand_object",
					"items": {
						"type": "object",
						"required": ["field", "path"],
						"properties": {
							"field": {
								"type": "string",
								"title": "Field Name",
								"description": "Redis Hash field name, e.g. connection_state",
								"minLength": 1
							},
							"path": {
								"type": "string",
								"title": "Value Path",
								"description": "Event path to read the value, e.g. payload.data.connection_state",
								"minLength": 1
							}
						},
						"additionalProperties": false
					}
				},
				"expand_object": {
					"type": "string",
					"title": "Expand Object Path",
					"description": "Auto-expand all fields from an object into Hash fields, e.g. payload.data"
				},
				"include_metadata": {
					"type": "boolean",
					"title": "Include Metadata",
					"description": "Write event metadata as meta_* fields"
				},
				"include_context": {
					"type": "boolean",
					"title": "Include Context",
					"description": "Write rule_id, source, and trace_id fields"
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
					"description": "Redis write operation timeout, default 10s",
					"default": 10,
					"minimum": 1
				},
				"max_len": {
					"type": "integer",
					"title": "Max Stream Length",
					"description": "XADD MAXLEN cap, stream mode only, 0 = unlimited",
					"minimum": 0,
					"default": 0
				},
				"approx": {
					"type": "boolean",
					"title": "Approximate Trimming",
					"description": "Use ~MAXLEN for efficient approximate capping, stream mode only",
					"default": true
				}
			},
			"additionalProperties": false
		}
	},
	"additionalProperties": false
}`

func (s *RedisSink) validateConfig(raw []byte) error {
	if err := json.Unmarshal(raw, &s.cfg); err != nil {
		return err
	}

	s.cfg.ConnectorRef = strings.TrimSpace(s.cfg.ConnectorRef)
	s.cfg.Target.Mode = strings.TrimSpace(strings.ToLower(s.cfg.Target.Mode))
	s.cfg.Target.Stream = strings.TrimSpace(s.cfg.Target.Stream)
	s.cfg.Target.Key = strings.TrimSpace(s.cfg.Target.Key)
	s.cfg.Target.KeyPrefix = strings.TrimSpace(s.cfg.Target.KeyPrefix)
	s.cfg.Target.KeyField = strings.TrimSpace(s.cfg.Target.KeyField)
	s.cfg.Target.KeyMode = strings.TrimSpace(strings.ToLower(s.cfg.Target.KeyMode))
	s.cfg.Encode.ExpandObject = strings.TrimSpace(s.cfg.Encode.ExpandObject)

	normalizedMappings := make([]RedisFieldMapping, 0, len(s.cfg.Encode.FieldMappings))
	for _, mapping := range s.cfg.Encode.FieldMappings {
		mapping.Field = strings.TrimSpace(mapping.Field)
		mapping.Path = strings.TrimSpace(mapping.Path)
		if mapping.Field == "" || mapping.Path == "" {
			continue
		}
		normalizedMappings = append(normalizedMappings, mapping)
	}
	s.cfg.Encode.FieldMappings = normalizedMappings

	if s.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if s.cfg.Target.Mode == "" {
		s.cfg.Target.Mode = "hash"
	}
	if hasRawSectionField(raw, "encode", "payload_field") {
		return fmt.Errorf("encode.payload_field is no longer supported; stream mode always writes to payload")
	}
	switch s.cfg.Target.Mode {
	case "stream":
		if s.cfg.Target.Stream == "" {
			return fmt.Errorf("target.stream is required when target.mode=stream")
		}
	case "hash":
		if s.cfg.Target.Key == "" && s.cfg.Target.KeyField == "" {
			return fmt.Errorf("target.key or target.key_field is required when target.mode=hash")
		}
		if hasRawSectionField(raw, "encode", "fields_field") {
			return fmt.Errorf("hash mode does not support encode.fields_field")
		}
		if len(s.cfg.Encode.FieldMappings) == 0 && s.cfg.Encode.ExpandObject == "" {
			return fmt.Errorf("hash mode requires encode.field_mappings or encode.expand_object")
		}
	default:
		return fmt.Errorf("target.mode must be 'stream' or 'hash'")
	}

	defaultInclude := s.cfg.Target.Mode == "stream"
	s.cfg.Encode.IncludeMetadata = resolveBoolDefault(raw, "encode", "include_metadata", defaultInclude)
	s.cfg.Encode.IncludeContext = resolveBoolDefault(raw, "encode", "include_context", defaultInclude)
	if s.cfg.Options.WriteTimeoutSec <= 0 {
		s.cfg.Options.WriteTimeoutSec = 10
	}
	if s.cfg.Options.MaxLen < 0 {
		s.cfg.Options.MaxLen = 0
	}
	s.cfg.Options.Approx = resolveBoolDefault(raw, "options", "approx", true)
	return nil
}

func hasRawSectionField(raw []byte, section string, field string) bool {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return false
	}
	sectionRaw, ok := parsed[section]
	if !ok {
		return false
	}
	var sectionValues map[string]json.RawMessage
	if err := json.Unmarshal(sectionRaw, &sectionValues); err != nil {
		return false
	}
	_, ok = sectionValues[field]
	return ok
}

func resolveBoolDefault(raw []byte, section string, field string, defaultValue bool) bool {
	sectionValues, ok := rawSectionValues(raw, section)
	if !ok {
		return defaultValue
	}
	valueRaw, ok := sectionValues[field]
	if !ok {
		return defaultValue
	}
	var value bool
	if err := json.Unmarshal(valueRaw, &value); err != nil {
		return defaultValue
	}
	return value
}

func rawSectionValues(raw []byte, section string) (map[string]json.RawMessage, bool) {
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, false
	}
	sectionRaw, ok := parsed[section]
	if !ok {
		return nil, false
	}
	var sectionValues map[string]json.RawMessage
	if err := json.Unmarshal(sectionRaw, &sectionValues); err != nil {
		return nil, false
	}
	return sectionValues, true
}
