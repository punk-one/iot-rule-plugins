package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

type S7SourceConfig struct {
	ConnectorRef string     `json:"connector_ref"`
	Output       outputConf `json:"output"`
	Points       []PointDef `json:"points"`
	Polling      struct {
		BatchSize    int    `json:"batch_size"`
		Interval     string `json:"interval,omitempty"`
		IntervalMs   int    `json:"interval_ms"`
		MaxBackoffMs int    `json:"max_backoff_ms"`
	} `json:"polling"`
	interval time.Duration
}

type outputConf struct {
	DeviceCode  string `json:"device_code"`
	IncludeMeta bool   `json:"include_meta"`
}

type PointDef struct {
	Address     string      `json:"address"`
	Name        string      `json:"name"`
	ValueType   string      `json:"value_type"`
	ReadWrite   string      `json:"read_write,omitempty"`
	Description string      `json:"description,omitempty"`
	MaxLength   int         `json:"max_length,omitempty"`
	Scale       *float64    `json:"scale,omitempty"`
	Precision   *int        `json:"precision,omitempty"`
	Range       *PointRange `json:"range,omitempty"`
}

type PointRange struct {
	Minimum      *float64 `json:"minimum,omitempty"`
	Maximum      *float64 `json:"maximum,omitempty"`
	Policy       string   `json:"policy,omitempty"`
	DefaultValue any      `json:"default_value,omitempty"`
}

const (
	rangePolicyUseDefault = "use_default"
	rangePolicyClamp      = "clamp"
	rangePolicyDrop       = "drop"
	rangePolicyError      = "error"
)

const s7SourceConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["connector_ref", "output", "points"],
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
    "output": {
      "type": "object",
      "required": ["device_code"],
      "properties": {
        "device_code": {"type": "string", "title": "Device Code"},
        "include_meta": {"type": "boolean", "title": "Include Meta", "default": false}
      },
      "additionalProperties": false
    },
    "points": {
      "type": "array",
	  "title": "S7 Points (JSON / Excel)",
	  "description": "Multiple S7 points. The UI supports JSON editing and Excel import/export.",
      "minItems": 1,
	  "maxItems": 5000,
      "items": {
        "type": "object",
        "required": ["address", "name", "value_type"],
        "properties": {
		  "address": {"type": "string", "title": "Address", "description": "S7 address, for example DB24.DBD440 or MX101.0"},
		  "name": {"type": "string", "title": "Name", "description": "Unique output field name"},
		  "value_type": {"type": "string", "title": "Value Type", "enum": ["Bool", "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Float32", "Float64", "Byte", "Word", "DWord", "String", "WString"]},
		  "read_write": {"type": "string", "title": "Read / Write", "enum": ["R", "RO", "RW"], "default": "R"},
		  "description": {"type": "string", "title": "Description"},
		  "max_length": {"type": "integer", "title": "Max Length", "minimum": 1, "maximum": 254},
		  "scale": {"type": "number", "title": "Scale", "exclusiveMinimum": 0, "default": 1},
		  "precision": {"type": "integer", "title": "Precision", "description": "Decimal places after scaling; 0 is valid", "minimum": 0, "maximum": 15},
		  "range": {
			"type": "object",
			"title": "Valid Range",
			"description": "Optional engineering-value bounds applied after scaling and precision",
			"properties": {
			  "minimum": {"type": "number", "title": "Minimum"},
			  "maximum": {"type": "number", "title": "Maximum"},
			  "policy": {"type": "string", "title": "Out-of-range Policy", "enum": ["use_default", "clamp", "drop", "error"], "default": "use_default"},
			  "default_value": {"type": "number", "title": "Default Value", "description": "Required when policy is use_default"}
			},
			"additionalProperties": false
		  }
        },
        "additionalProperties": false
      }
    },
    "polling": {
      "type": "object",
      "properties": {
        "batch_size": {"type": "integer", "minimum": 1, "maximum": 19, "default": 16},
		"interval": {"type": "string", "title": "Collection Interval", "description": "Go duration such as 500ms, 5s, or 1m; takes precedence over interval_ms", "pattern": "^[0-9]+(ms|s|m|h)$"},
		"interval_ms": {"type": "integer", "title": "Legacy Interval (ms)", "description": "Backward-compatible millisecond interval; interval takes precedence", "minimum": 100},
        "max_backoff_ms": {"type": "integer", "minimum": 100, "default": 10000}
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}`

func (s *S7Source) validateConfig(raw []byte) error {
	if err := json.Unmarshal(raw, &s.cfg); err != nil {
		return err
	}
	s.cfg.ConnectorRef = strings.TrimSpace(s.cfg.ConnectorRef)
	s.cfg.Output.DeviceCode = strings.TrimSpace(s.cfg.Output.DeviceCode)
	if s.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if s.cfg.Output.DeviceCode == "" {
		return fmt.Errorf("output.device_code is required")
	}
	if len(s.cfg.Points) == 0 {
		return fmt.Errorf("points is required")
	}
	if len(s.cfg.Points) > 5000 {
		return fmt.Errorf("points must not contain more than 5000 items")
	}
	seenNames := make(map[string]struct{}, len(s.cfg.Points))
	for i := range s.cfg.Points {
		pt := &s.cfg.Points[i]
		pt.Name = strings.TrimSpace(pt.Name)
		pt.Address = strings.TrimSpace(pt.Address)
		pt.ValueType = normalizeValueType(pt.ValueType)
		pt.ReadWrite = strings.ToUpper(strings.TrimSpace(pt.ReadWrite))
		if pt.Name == "" {
			return fmt.Errorf("points[%d].name is required", i)
		}
		if _, exists := seenNames[pt.Name]; exists {
			return fmt.Errorf("point name %q must be unique", pt.Name)
		}
		seenNames[pt.Name] = struct{}{}
		if pt.Address == "" {
			return fmt.Errorf("point %s address is required", pt.Name)
		}
		if !supportedValueType(pt.ValueType) {
			return fmt.Errorf("point %s value_type %q is not supported", pt.Name, pt.ValueType)
		}
		if pt.ReadWrite == "" {
			pt.ReadWrite = "R"
		}
		if pt.ReadWrite != "R" && pt.ReadWrite != "RO" && pt.ReadWrite != "RW" {
			return fmt.Errorf("point %s read_write must be R, RO, or RW", pt.Name)
		}
		if pt.Scale != nil {
			if math.IsNaN(*pt.Scale) || math.IsInf(*pt.Scale, 0) || *pt.Scale <= 0 {
				return fmt.Errorf("point %s scale must be a finite number greater than 0", pt.Name)
			}
		}
		if pt.Precision != nil && (*pt.Precision < 0 || *pt.Precision > 15) {
			return fmt.Errorf("point %s precision must be between 0 and 15", pt.Name)
		}
		if !isNumericValueType(pt.ValueType) && ((pt.Scale != nil && *pt.Scale != 1) || pt.Precision != nil) {
			return fmt.Errorf("point %s scale and precision are only supported for numeric value types", pt.Name)
		}
		if err := validatePointRange(*pt); err != nil {
			return err
		}
	}
	if s.cfg.Polling.BatchSize <= 0 || s.cfg.Polling.BatchSize > 19 {
		s.cfg.Polling.BatchSize = 16
	}
	if strings.TrimSpace(s.cfg.Polling.Interval) != "" {
		interval, err := time.ParseDuration(strings.TrimSpace(s.cfg.Polling.Interval))
		if err != nil || interval < 100*time.Millisecond || interval > 24*time.Hour {
			return fmt.Errorf("polling.interval must be between 100ms and 24h")
		}
		s.cfg.interval = interval
	} else {
		if s.cfg.Polling.IntervalMs <= 0 {
			s.cfg.Polling.IntervalMs = 1000
		}
		if s.cfg.Polling.IntervalMs < 100 {
			return fmt.Errorf("polling.interval_ms must be at least 100")
		}
		s.cfg.interval = time.Duration(s.cfg.Polling.IntervalMs) * time.Millisecond
	}
	if s.cfg.Polling.MaxBackoffMs <= 0 {
		s.cfg.Polling.MaxBackoffMs = 10000
	}
	if time.Duration(s.cfg.Polling.MaxBackoffMs)*time.Millisecond < s.cfg.interval {
		s.cfg.Polling.MaxBackoffMs = int(s.cfg.interval / time.Millisecond)
	}
	return nil
}

func normalizeValueType(valueType string) string {
	switch strings.ToLower(strings.TrimSpace(valueType)) {
	case "bool":
		return "Bool"
	case "int8":
		return "Int8"
	case "uint8":
		return "Uint8"
	case "int16":
		return "Int16"
	case "uint16":
		return "Uint16"
	case "int32":
		return "Int32"
	case "uint32":
		return "Uint32"
	case "float32":
		return "Float32"
	case "float64":
		return "Float64"
	case "byte":
		return "Byte"
	case "word":
		return "Word"
	case "dword":
		return "DWord"
	case "string":
		return "String"
	case "wstring":
		return "WString"
	default:
		return strings.TrimSpace(valueType)
	}
}

func supportedValueType(valueType string) bool {
	switch valueType {
	case "Bool", "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Float32", "Float64", "Byte", "Word", "DWord", "String", "WString":
		return true
	default:
		return false
	}
}

func validatePointRange(pt PointDef) error {
	if pt.Range == nil {
		return nil
	}
	if pt.Range.Minimum == nil && pt.Range.Maximum == nil {
		return fmt.Errorf("point %s range requires minimum or maximum", pt.Name)
	}
	if !isNumericValueType(pt.ValueType) {
		return fmt.Errorf("point %s range is only supported for numeric value types", pt.Name)
	}
	for label, bound := range map[string]*float64{"minimum": pt.Range.Minimum, "maximum": pt.Range.Maximum} {
		if bound != nil && (math.IsNaN(*bound) || math.IsInf(*bound, 0)) {
			return fmt.Errorf("point %s range.%s must be finite", pt.Name, label)
		}
	}
	if pt.Range.Minimum != nil && pt.Range.Maximum != nil && *pt.Range.Minimum > *pt.Range.Maximum {
		return fmt.Errorf("point %s range.minimum must not exceed range.maximum", pt.Name)
	}
	pt.Range.Policy = strings.ToLower(strings.TrimSpace(pt.Range.Policy))
	if pt.Range.Policy == "" {
		pt.Range.Policy = rangePolicyUseDefault
	}
	switch pt.Range.Policy {
	case rangePolicyUseDefault:
		if pt.Range.DefaultValue == nil {
			return fmt.Errorf("point %s range.default_value is required for use_default", pt.Name)
		}
		defaultValue, ok := finiteNumber(pt.Range.DefaultValue)
		if !ok {
			return fmt.Errorf("point %s range.default_value must be a finite number", pt.Name)
		}
		if pt.Range.Minimum != nil && defaultValue < *pt.Range.Minimum || pt.Range.Maximum != nil && defaultValue > *pt.Range.Maximum {
			return fmt.Errorf("point %s range.default_value must be inside the configured range", pt.Name)
		}
	case rangePolicyClamp, rangePolicyDrop, rangePolicyError:
	default:
		return fmt.Errorf("point %s range.policy is invalid", pt.Name)
	}
	return nil
}

func isNumericValueType(valueType string) bool {
	switch valueType {
	case "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Float32", "Float64", "Byte", "Word", "DWord":
		return true
	default:
		return false
	}
}

func finiteNumber(value any) (float64, bool) {
	var number float64
	switch typed := value.(type) {
	case float64:
		number = typed
	case float32:
		number = float64(typed)
	case int:
		number = float64(typed)
	case int8:
		number = float64(typed)
	case int16:
		number = float64(typed)
	case int32:
		number = float64(typed)
	case int64:
		number = float64(typed)
	case uint:
		number = float64(typed)
	case uint8:
		number = float64(typed)
	case uint16:
		number = float64(typed)
	case uint32:
		number = float64(typed)
	case uint64:
		number = float64(typed)
	default:
		return 0, false
	}
	return number, !math.IsNaN(number) && !math.IsInf(number, 0)
}
