package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

// Mode 变化检测模式
type Mode string

const (
	ModeDelta   Mode = "delta"   // 绝对值变化
	ModePercent Mode = "percent" // 百分比变化
	ModeTrend   Mode = "trend"   // 趋势判断（连续上升/下降）
	ModeChanged Mode = "changed" // 任意类型的值变化
)

// Direction 变化方向
type Direction string

const (
	DirectionIncrease Direction = "increase" // 只检测增加
	DirectionDecrease Direction = "decrease" // 只检测减少
	DirectionAny      Direction = "any"      // 任意方向
)

// EmitPolicy 输出策略
type EmitPolicy string

const (
	EmitPolicyOnChange EmitPolicy = "on_change" // 只在变化时输出
	EmitPolicyAlways   EmitPolicy = "always"    // 总是输出
)

// Config ChangeDetect Plugin 配置（v1.5）
type Config struct {
	State struct {
		KeyBy   []string `json:"key_by"`             // 状态键字段列表（原 group_by）
		TTL     string   `json:"ttl,omitempty"`      // 状态TTL（如 "1h"）
		MaxKeys int      `json:"max_keys,omitempty"` // 最大键数量
		Evict   string   `json:"evict,omitempty"`    // 淘汰策略：lru, fifo, random
	} `json:"state"`
	Process struct {
		Detect struct {
			Type      string   `json:"type"`                // 检测模式：delta | percent | trend
			Value     float64  `json:"value"`               // 阈值（原 threshold）
			Field     string   `json:"field"`               // 监控字段名
			Fields    []string `json:"fields,omitempty"`    // 监控字段列表
			Direction string   `json:"direction,omitempty"` // 方向：increase | decrease | any
		} `json:"detect"`
	} `json:"process"`
	Output struct {
		Emit      string `json:"emit"`                 // 输出策略：on_change | always（原 emit_policy）
		EmitFirst bool   `json:"emit_first,omitempty"` // 首个快照是否立即输出
		Annotate  *bool  `json:"annotate,omitempty"`   // 是否向 payload 写入 _change_* 字段
	} `json:"output"`
}

const detectChangeConfigSchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"type": "object",
	"required": ["state", "process", "output"],
	"properties": {
		"state": {
			"type": "object",
			"title": "State",
			"description": "How change history is partitioned and retained between events",
			"required": ["key_by"],
			"properties": {
				"key_by": {
					"type": "array",
					"title": "Key By",
					"description": "Fields that define one independent change-tracking stream, for example ['device_code'] or ['device_code', 'metric_code']",
					"items": {"type": "string"},
					"minItems": 1
				},
				"ttl": {
					"type": "string",
					"title": "TTL",
					"description": "Optional retention time for remembered state, for example 30m, 2h, or 24h",
					"pattern": "^[0-9]+(s|m|h)$"
				},
				"max_keys": {
					"type": "integer",
					"title": "Max Keys",
					"description": "Maximum number of distinct key_by combinations kept in memory",
					"minimum": 1
				},
				"evict": {
					"type": "string",
					"title": "Eviction Policy",
					"description": "How old state entries are evicted when Max Keys is reached",
					"enum": ["lru", "fifo", "random"],
					"default": "lru"
				}
			},
			"additionalProperties": false
		},
		"process": {
			"type": "object",
			"title": "Process",
			"description": "What value is compared and what qualifies as a meaningful change",
			"required": ["detect"],
			"properties": {
				"detect": {
					"type": "object",
					"title": "Detect",
					"required": ["type"],
					"properties": {
						"type": {
							"type": "string",
							"title": "Detection Type",
							"description": "delta compares absolute difference, percent compares percentage change, trend checks monotonic movement, and changed checks whether values differ at all",
							"enum": ["delta", "percent", "trend", "changed"]
						},
						"value": {
							"type": "number",
							"title": "Threshold Value",
							"description": "Numeric threshold for delta, percent, or trend modes. This field is ignored by changed mode"
						},
						"field": {
							"type": "string",
							"title": "Field",
							"description": "Primary field path to monitor, such as temperature, payload.temperature, or status.code"
						},
						"fields": {
							"type": "array",
							"title": "Fields",
							"description": "Optional list of fields to compare together. Most useful with changed mode when multiple fields must be tracked as one snapshot",
							"items": {"type": "string"},
							"minItems": 1
						},
						"direction": {
							"type": "string",
							"title": "Direction",
							"description": "Restrict detection to increases, decreases, or any direction",
							"enum": ["increase", "decrease", "any"],
							"default": "any"
						}
					},
					"anyOf": [
						{"required": ["field"]},
						{"required": ["fields"]}
					],
					"additionalProperties": false
				}
			},
			"additionalProperties": false
		},
		"output": {
			"type": "object",
			"title": "Output",
			"description": "Controls whether unchanged events are suppressed or still forwarded downstream",
			"required": ["emit"],
			"properties": {
				"emit": {
					"type": "string",
					"title": "Emit Policy",
					"description": "on_change only emits when a change is detected; always forwards every event and annotates the result",
					"enum": ["on_change", "always"],
					"default": "on_change"
				},
				"emit_first": {
					"type": "boolean",
					"title": "Emit First Snapshot",
					"description": "Emit the first complete snapshot before change comparison; defaults to false for compatibility",
					"default": false
				},
				"annotate": {
					"type": "boolean",
					"title": "Annotate Payload",
					"description": "Add _change_* diagnostic fields to emitted payload; disable to preserve an exact downstream message contract",
					"default": true
				}
			},
			"additionalProperties": false
		}
	},
	"additionalProperties": false
}`

func (p *ChangeDetectPlugin) validateConfig(cfg sdk.PluginConfig) error {
	p.config = cfg

	if err := json.Unmarshal(cfg.Raw, &p.cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	if len(p.cfg.State.KeyBy) == 0 {
		return fmt.Errorf("state.key_by is required and must have at least one field")
	}

	for i := range p.cfg.State.KeyBy {
		p.cfg.State.KeyBy[i] = strings.TrimSpace(p.cfg.State.KeyBy[i])
		if p.cfg.State.KeyBy[i] == "" {
			return fmt.Errorf("state.key_by[%d] must not be empty", i)
		}
	}

	p.cfg.Process.Detect.Field = strings.TrimSpace(p.cfg.Process.Detect.Field)
	normalizedFields := make([]string, 0, len(p.cfg.Process.Detect.Fields)+1)
	seen := make(map[string]struct{}, len(p.cfg.Process.Detect.Fields)+1)
	appendField := func(field string) {
		field = strings.TrimSpace(field)
		if field == "" {
			return
		}
		if _, ok := seen[field]; ok {
			return
		}
		seen[field] = struct{}{}
		normalizedFields = append(normalizedFields, field)
	}
	appendField(p.cfg.Process.Detect.Field)
	for _, field := range p.cfg.Process.Detect.Fields {
		appendField(field)
	}
	if len(normalizedFields) == 0 {
		return fmt.Errorf("process.detect.field or process.detect.fields is required")
	}
	p.cfg.Process.Detect.Field = normalizedFields[0]
	p.cfg.Process.Detect.Fields = normalizedFields

	if p.cfg.Process.Detect.Type != string(ModeDelta) &&
		p.cfg.Process.Detect.Type != string(ModePercent) &&
		p.cfg.Process.Detect.Type != string(ModeTrend) &&
		p.cfg.Process.Detect.Type != string(ModeChanged) {
		return fmt.Errorf("process.detect.type must be 'delta', 'percent', 'trend', or 'changed'")
	}
	if p.cfg.Process.Detect.Type != string(ModeChanged) && len(p.cfg.Process.Detect.Fields) != 1 {
		return fmt.Errorf("numeric detect mode requires exactly one field")
	}
	if p.cfg.Process.Detect.Type != string(ModeChanged) && p.cfg.Process.Detect.Value <= 0 {
		return fmt.Errorf("process.detect.value must be greater than 0")
	}
	if p.cfg.State.TTL != "" {
		ttl, err := time.ParseDuration(p.cfg.State.TTL)
		if err != nil {
			return fmt.Errorf("state.ttl must be a valid duration: %w", err)
		}
		if ttl <= 0 {
			return fmt.Errorf("state.ttl must be greater than 0")
		}
		p.stateTTL = ttl
	}

	if p.cfg.Process.Detect.Direction == "" {
		p.cfg.Process.Detect.Direction = string(DirectionAny)
	}
	if p.cfg.Output.Emit == "" {
		p.cfg.Output.Emit = string(EmitPolicyOnChange)
	}
	if p.cfg.State.Evict == "" {
		p.cfg.State.Evict = "lru"
	}

	return nil
}

func (c Config) detectFields() []string {
	if len(c.Process.Detect.Fields) > 0 {
		return c.Process.Detect.Fields
	}
	if c.Process.Detect.Field == "" {
		return nil
	}
	return []string{c.Process.Detect.Field}
}

func (c Config) annotateEnabled() bool {
	return c.Output.Annotate == nil || *c.Output.Annotate
}
