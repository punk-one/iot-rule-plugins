package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Config struct {
	ConnectorRef string `json:"connector_ref"`
	Output       struct {
		Table              string      `json:"table"`
		AutoCreateColumns  bool        `json:"auto_create_columns,omitempty"`
		WriteMode          string      `json:"write_mode,omitempty"`
		ConflictKeyColumns []string    `json:"conflict_key_columns,omitempty"`
		Batch              BatchConfig `json:"batch,omitempty"`
	} `json:"output"`
}

type BatchConfig struct {
	MaxRows         int `json:"max_rows,omitempty"`
	FlushIntervalMS int `json:"flush_interval_ms,omitempty"`
	MaxBytes        int `json:"max_bytes,omitempty"`
}

type writeMode string

const (
	writeModeSingle writeMode = "single"
	writeModeBatch  writeMode = "batch"
)

const (
	defaultBatchMaxRows         = 200
	defaultBatchFlushIntervalMS = 200
	defaultBatchMaxBytes        = 512 * 1024
)

const postgresSinkConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["connector_ref", "output"],
  "properties": {
    "connector_ref": {
      "type": "string",
      "title": "Connector Ref",
      "description": "ConnectorResource ID"
    },
    "output": {
      "type": "object",
      "title": "Output",
      "required": ["table"],
      "properties": {
        "table": {
          "type": "string",
          "title": "Table",
          "description": "Target PostgreSQL table name"
        },
        "auto_create_columns": {
          "type": "boolean",
          "title": "Auto Create Columns",
          "description": "Automatically add missing columns before writing rows",
          "default": false
        },
        "write_mode": {
          "type": "string",
          "title": "Write Mode",
          "description": "single writes each row immediately; batch buffers rows and flushes according to batch settings",
          "enum": ["single", "batch"],
          "default": "single"
        },
        "conflict_key_columns": {
          "type": "array",
          "title": "Conflict Key Columns",
          "description": "Optional columns backed by a matching PostgreSQL unique constraint or index. Duplicate inserts for the same non-null key leave the original row unchanged.",
          "maxItems": 32,
          "uniqueItems": true,
          "items": {"type": "string", "minLength": 1, "maxLength": 63, "pattern": "^[a-zA-Z_][a-zA-Z0-9_$]*$"}
        },
        "batch": {
          "type": "object",
          "title": "Batch Settings",
          "description": "Flush thresholds used only when Write Mode is batch",
          "properties": {
            "max_rows": {
              "type": "integer",
              "title": "Max Rows",
              "description": "Flush when buffered row count reaches this threshold",
              "minimum": 1,
              "default": 200
            },
            "flush_interval_ms": {
              "type": "integer",
              "title": "Flush Interval (ms)",
              "description": "Periodic flush interval in milliseconds",
              "minimum": 1,
              "default": 200
            }
          },
          "additionalProperties": false
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}`

func (p *PostgresSink) validateConfig(raw []byte) error {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	keys, err := normalizeSQLConflictKeys(cfg.Output.ConflictKeyColumns)
	if err != nil {
		return err
	}
	cfg.Output.ConflictKeyColumns = keys
	p.cfg = cfg

	p.cfg.ConnectorRef = strings.TrimSpace(p.cfg.ConnectorRef)
	if p.cfg.ConnectorRef == "" {
		return fmt.Errorf("connector_ref is required")
	}

	p.tableTemplate = strings.TrimSpace(p.cfg.Output.Table)
	if p.tableTemplate == "" {
		return fmt.Errorf("output.table is required")
	}

	p.autoCreateCols = p.cfg.Output.AutoCreateColumns

	mode := writeMode(strings.TrimSpace(p.cfg.Output.WriteMode))
	if mode == "" {
		mode = writeModeSingle
	}
	switch mode {
	case writeModeSingle, writeModeBatch:
	default:
		return fmt.Errorf("output.write_mode must be single or batch")
	}
	p.writeMode = mode

	p.batchConfig = p.cfg.Output.Batch
	if p.writeMode == writeModeBatch {
		if p.batchConfig.MaxRows <= 0 {
			p.batchConfig.MaxRows = defaultBatchMaxRows
		}
		if p.batchConfig.FlushIntervalMS <= 0 {
			p.batchConfig.FlushIntervalMS = defaultBatchFlushIntervalMS
		}
		if p.batchConfig.MaxBytes <= 0 {
			p.batchConfig.MaxBytes = defaultBatchMaxBytes
		}
	} else if p.batchConfig.MaxRows > 0 || p.batchConfig.FlushIntervalMS > 0 || p.batchConfig.MaxBytes > 0 {
		return fmt.Errorf("output.batch can only be configured when output.write_mode is batch")
	}

	p.tableColumns = make(map[string]map[string]bool)
	p.pendingRows = make(map[string][]map[string]interface{})
	p.pendingBytes = make(map[string]int)
	return nil
}
