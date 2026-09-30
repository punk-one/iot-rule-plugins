package main

import (
	"encoding/json"
	"fmt"
)

// TransformSpec defines the transform configuration.
type TransformSpec struct {
	Name     string       `json:"name,omitempty"`
	Metadata MetadataSpec `json:"metadata,omitempty"`
	Mappings []Mapping    `json:"mappings"`
}

// MetadataSpec defines metadata copy strategy.
type MetadataSpec struct {
	Copy []string `json:"copy,omitempty"`
}

// MappingType enumerates the types of mapping operations.
type MappingType string

const (
	MappingCopy     MappingType = "copy"
	MappingObject   MappingType = "object"
	MappingTemplate MappingType = "template"
)

// Mapping defines a single mapping rule.
type Mapping struct {
	Name     string      `json:"name,omitempty"`
	Type     MappingType `json:"type"`
	From     string      `json:"from,omitempty"`
	To       string      `json:"to,omitempty"`
	Template string      `json:"template,omitempty"`
	Key      *KeySpec    `json:"key,omitempty"`
	Value    *ValueSpec  `json:"value,omitempty"`
	Exclude  []string    `json:"exclude,omitempty"`
}

// KeySpec defines a dynamic key for object type mappings.
type KeySpec struct {
	Template string `json:"template"`
}

// ValueSpec defines a dynamic value for object type mappings.
type ValueSpec struct {
	JSONPath string   `json:"jsonpath,omitempty"`
	Exclude  []string `json:"exclude,omitempty"`
}

// Config is the top-level plugin configuration.
type Config struct {
	Process TransformSpec `json:"process"`
}

const transformConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["process"],
  "properties": {
    "process": {
      "type": "object",
      "required": ["mappings"],
      "properties": {
        "name": {
          "type": "string",
          "description": "Transform name for identification"
        },
        "metadata": {
          "type": "object",
          "properties": {
            "copy": {
              "type": "array",
              "items": { "type": "string" },
              "description": "Metadata fields to copy from input to output"
            }
          },
          "additionalProperties": false
        },
        "mappings": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "object",
            "required": ["type"],
            "oneOf": [
              {
                "properties": {
                  "type": { "type": "string", "enum": ["copy"] },
                  "from": { "type": "string", "description": "Source path expression" },
                  "to": { "type": "string", "description": "Target field name" }
                },
                "required": ["from", "to"],
                "additionalProperties": false
              },
              {
                "properties": {
                  "type": { "type": "string", "enum": ["object"] },
                  "key": {
                    "type": "object",
                    "required": ["template"],
                    "properties": {
                      "template": { "type": "string", "description": "Go template for key" }
                    },
                    "additionalProperties": false
                  },
                  "value": {
                    "type": "object",
                    "properties": {
                      "jsonpath": { "type": "string", "description": "JSONPath source" },
                      "exclude": { "type": "array", "items": { "type": "string" } }
                    },
                    "additionalProperties": false
                  },
                  "exclude": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["key"],
                "additionalProperties": false
              },
              {
                "properties": {
                  "type": { "type": "string", "enum": ["template"] },
                  "to": { "type": "string", "description": "Target field name" },
                  "template": { "type": "string", "description": "Go template string" }
                },
                "required": ["to", "template"],
                "additionalProperties": false
              }
            ]
          }
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}`

// validateConfig validates the parsed config.
func validateConfig(cfg *Config) error {
	if len(cfg.Process.Mappings) == 0 {
		return fmt.Errorf("process.mappings is required and must have at least one mapping")
	}
	for i, m := range cfg.Process.Mappings {
		switch m.Type {
		case MappingCopy:
			if m.From == "" {
				return fmt.Errorf("mappings[%d].from is required for copy type", i)
			}
			if m.To == "" {
				return fmt.Errorf("mappings[%d].to is required for copy type", i)
			}
		case MappingObject:
			if m.Key == nil || m.Key.Template == "" {
				return fmt.Errorf("mappings[%d].key.template is required for object type", i)
			}
		case MappingTemplate:
			if m.To == "" {
				return fmt.Errorf("mappings[%d].to is required for template type", i)
			}
			if m.Template == "" {
				return fmt.Errorf("mappings[%d].template is required for template type", i)
			}
		default:
			return fmt.Errorf("mappings[%d].type must be one of: copy, object, template", i)
		}
	}
	return nil
}

// compileConfigJSON compiles a JSON raw config into a Config struct.
func compileConfigJSON(raw json.RawMessage) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
