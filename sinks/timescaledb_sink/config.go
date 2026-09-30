package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Config struct {
	ConnectorRef string `json:"connector_ref"`
	Output       struct {
		Table             string      `json:"table"`
		AutoCreateColumns bool        `json:"auto_create_columns,omitempty"`
		WriteMode         string      `json:"write_mode,omitempty"`
		Batch             BatchConfig `json:"batch,omitempty"`
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

const timescaleConfigSchema = `{
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
      "required": ["table"],
      "properties": {
        "table": {
          "type": "string",
          "title": "Table",
          "description": "Target TimescaleDB table or hypertable name. This can be a fixed table name or a metadata-aware template when supported by the sink"
        },
        "auto_create_columns": {
          "type": "boolean",
          "title": "Auto Create Columns",
          "default": false,
          "description": "Automatically add missing columns before writing rows. Disable this when the table schema is managed separately"
        },
        "write_mode": {
          "type": "string",
          "title": "Write Mode",
          "enum": ["single", "batch"],
          "default": "single",
          "description": "single writes each row immediately. batch buffers rows and flushes them according to the batch settings below"
        },
        "batch": {
          "type": "object",
          "title": "Batch Settings",
          "description": "Flush thresholds used only when Write Mode is batch",
          "properties": {
            "max_rows": {
              "type": "integer",
              "title": "Max Rows",
              "minimum": 1,
              "default": 200,
              "description": "Flush buffered rows immediately once the row count reaches this threshold"
            },
            "flush_interval_ms": {
              "type": "integer",
              "title": "Flush Interval (ms)",
              "minimum": 1,
              "default": 200,
              "description": "Periodic flush interval in milliseconds for buffered rows"
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

func (p *TimescaleSink) validateConfig(raw []byte) error {
	if err := json.Unmarshal(raw, &p.cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

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
	} else {
		p.batchConfig = BatchConfig{}
	}

	p.tableColumns = make(map[string]map[string]bool)
	p.pendingRows = make(map[string][]map[string]interface{})
	p.pendingBytes = make(map[string]int)
	return nil
}
