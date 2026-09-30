package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type PostgresSourceConfig struct {
	ConnectorRef string `json:"connector_ref"`
	Output       struct {
		DeviceCode       string   `json:"device_code,omitempty"`
		DeviceCodeColumn string   `json:"device_code_column,omitempty"`
		TimeColumn       string   `json:"time_column,omitempty"`
		DataColumns      []string `json:"data_columns,omitempty"`
		IncludeMeta      bool     `json:"include_meta,omitempty"`
	} `json:"output"`
	Query struct {
		SQL     string        `json:"sql"`
		Args    []interface{} `json:"args,omitempty"`
		MaxRows int           `json:"max_rows,omitempty"`
	} `json:"query"`
	Polling struct {
		IntervalMs   int `json:"interval_ms,omitempty"`
		MaxBackoffMs int `json:"max_backoff_ms,omitempty"`
	} `json:"polling"`
}

const (
	defaultPostgresSourcePollingMs  = 1000
	defaultPostgresSourceMaxBackoff = 10000
	defaultPostgresSourceMaxRows    = 100
)

const postgresSourceConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["connector_ref", "query"],
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
      "title": "Output",
      "properties": {
        "device_code": {"type": "string", "title": "Fallback Device Code"},
        "device_code_column": {"type": "string", "title": "Device Code Column", "default": "device_code"},
        "time_column": {"type": "string", "title": "Time Column", "default": "time"},
        "data_columns": {
          "type": "array",
          "title": "Data Columns",
          "items": {"type": "string"},
          "default": []
        },
        "include_meta": {"type": "boolean", "title": "Include Meta", "default": false}
      },
      "additionalProperties": false
    },
    "query": {
      "type": "object",
      "title": "Query",
      "required": ["sql"],
      "properties": {
        "sql": {"type": "string", "title": "SQL"},
        "args": {
          "type": "array",
          "title": "Args",
          "items": {
            "type": ["string", "number", "integer", "boolean", "null"]
          },
          "default": []
        },
        "max_rows": {
          "type": "integer",
          "title": "Max Rows",
          "minimum": 1,
          "default": 100
        }
      },
      "additionalProperties": false
    },
    "polling": {
      "type": "object",
      "title": "Polling",
      "properties": {
        "interval_ms": {"type": "integer", "minimum": 100, "default": 1000},
        "max_backoff_ms": {"type": "integer", "minimum": 100, "default": 10000}
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}`

func (s *PostgresSource) validateConfig(raw []byte) error {
	s.cfg.Polling.IntervalMs = defaultPostgresSourcePollingMs
	s.cfg.Polling.MaxBackoffMs = defaultPostgresSourceMaxBackoff
	s.cfg.Query.MaxRows = defaultPostgresSourceMaxRows
	s.cfg.Output.DeviceCodeColumn = "device_code"
	s.cfg.Output.TimeColumn = "time"

	if err := json.Unmarshal(raw, &s.cfg); err != nil {
		return err
	}

	s.cfg.ConnectorRef = strings.TrimSpace(s.cfg.ConnectorRef)
	s.cfg.Output.DeviceCode = strings.TrimSpace(s.cfg.Output.DeviceCode)
	s.cfg.Output.DeviceCodeColumn = strings.TrimSpace(s.cfg.Output.DeviceCodeColumn)
	s.cfg.Output.TimeColumn = strings.TrimSpace(s.cfg.Output.TimeColumn)
	s.cfg.Query.SQL = strings.TrimSpace(s.cfg.Query.SQL)

	if s.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}
	if s.cfg.Query.SQL == "" {
		return fmt.Errorf("query.sql is required")
	}
	if s.cfg.Polling.IntervalMs <= 0 {
		s.cfg.Polling.IntervalMs = defaultPostgresSourcePollingMs
	}
	if s.cfg.Polling.MaxBackoffMs <= 0 {
		s.cfg.Polling.MaxBackoffMs = defaultPostgresSourceMaxBackoff
	}
	if s.cfg.Query.MaxRows <= 0 {
		s.cfg.Query.MaxRows = defaultPostgresSourceMaxRows
	}
	for i := range s.cfg.Output.DataColumns {
		s.cfg.Output.DataColumns[i] = strings.TrimSpace(s.cfg.Output.DataColumns[i])
		if s.cfg.Output.DataColumns[i] == "" {
			return fmt.Errorf("output.data_columns[%d] is empty", i)
		}
	}
	return nil
}
