package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Config struct {
	ConnectorRef string `json:"connector_ref"`
	Output       struct {
		Table             string              `json:"table"`
		ChildTable        string              `json:"child_table,omitempty"`
		TagFields         []string            `json:"tag_fields,omitempty"`
		ColumnMapping     []ColumnMappingItem `json:"column_mapping,omitempty"`
		AutoCreateColumns bool                `json:"auto_create_columns,omitempty"`
		WriteMode         string              `json:"write_mode,omitempty"`
		Batch             BatchConfig         `json:"batch,omitempty"`
	} `json:"output"`
}

type ColumnMappingItem struct {
	Field  string `json:"field"`
	Column string `json:"column"`
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

const tdengineConfigSchema = `{
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
          "title": "Super Table",
          "description": "Target TDengine supertable name. Supports ${metadata.xxx} template variables. Required."
        },
        "child_table": {
          "type": "string",
          "title": "Child Table",
          "description": "Template for child table name when writing to a supertable. Use ${field_name} to reference payload fields. e.g. ${device}_telemetry. When empty, uses regular table INSERT syntax."
        },
        "tag_fields": {
          "type": "array",
          "title": "Tag Fields",
          "description": "Payload field names to use as TAGS when writing to a supertable. The order matches the supertable's TAGS definition order. Fields not listed go into VALUES.",
          "items": {
            "type": "string"
          }
        },
        "column_mapping": {
          "type": "array",
          "title": "Column Mapping",
          "description": "Map event payload field names to TDengine column names.",
          "items": {
            "type": "object",
            "required": ["field", "column"],
            "properties": {
              "field": {
                "type": "string",
                "title": "Payload Field",
                "description": "Field name in the incoming event payload"
              },
              "column": {
                "type": "string",
                "title": "TDengine Column",
                "description": "Target column name in TDengine"
              }
            }
          }
        },
        "auto_create_columns": {
          "type": "boolean",
          "title": "Auto Create Columns",
          "default": false,
          "description": "Automatically add missing columns via ALTER TABLE ADD COLUMN"
        },
        "write_mode": {
          "type": "string",
          "title": "Write Mode",
          "enum": ["single", "batch"],
          "default": "single",
          "description": "single writes each row immediately. batch buffers rows and flushes according to batch settings."
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
            },
            "max_bytes": {
              "type": "integer",
              "title": "Max Bytes",
              "minimum": 1,
              "default": 524288,
              "description": "Flush buffered rows when estimated byte size reaches this threshold"
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

func (p *TDengineSink) validateConfig(raw []byte) error {
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

	p.childTableTemplate = strings.TrimSpace(p.cfg.Output.ChildTable)
	if p.childTableTemplate != "" {
		p.tagFieldSet = make(map[string]bool, len(p.cfg.Output.TagFields))
		for _, f := range p.cfg.Output.TagFields {
			f = strings.TrimSpace(f)
			if f != "" {
				p.tagFieldSet[f] = true
			}
		}
	} else {
		p.cfg.Output.TagFields = nil
	}

	p.autoCreateCols = p.cfg.Output.AutoCreateColumns

	if p.cfg.Output.ColumnMapping == nil {
		p.cfg.Output.ColumnMapping = []ColumnMappingItem{}
	}

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
	p.pendingRows = make(map[string][]queuedRow)
	p.pendingBytes = make(map[string]int)
	return nil
}
