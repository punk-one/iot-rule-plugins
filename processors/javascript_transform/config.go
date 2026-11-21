package main

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["code"],
  "properties": {
    "code": {
      "type": "string",
      "format": "code",
      "minLength": 1,
      "maxLength": 65536,
      "title": "JavaScript 代码",
      "description": "定义 function main(input)。通过 console 输出调试信息，返回输出对象或 null",
      "x-ui-height": "520px",
      "x-ui-placeholder": "请定义 function main(input)",
      "default": "function main(input) {\n  console.log(\"JavaScript Transform input:\", JSON.stringify(input));\n\n  // ===== 在这里编写业务处理代码 =====\n  var outputPayload = {};\n  Object.keys(input.payload || {}).forEach(function (key) {\n    outputPayload[key] = input.payload[key];\n  });\n  outputPayload.processed_by = \"javascript-transform\";\n\n  return {\n    payload: outputPayload,\n    metadata: input.metadata,\n    timestamp_ms: input.timestamp_ms\n  };\n}"
    },
    "script_version": {
      "type": "string",
      "pattern": "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",
      "title": "脚本版本",
      "description": "可选的版本标签，随规则配置保存并用于排查日志"
    },
    "timeout_ms": {
      "type": "integer",
      "minimum": 1,
      "maximum": 1000,
      "default": 50,
      "title": "单事件执行超时（毫秒）"
    }
  },
  "additionalProperties": false
}`

type pluginConfig struct {
	ScriptVersion string `json:"script_version,omitempty"`
	Code          string `json:"code"`
	TimeoutMS     int    `json:"timeout_ms,omitempty"`
}

type scriptInput struct {
	Payload     map[string]interface{} `json:"payload"`
	Metadata    map[string]string      `json:"metadata"`
	TimestampMS int64                  `json:"timestamp_ms"`
	EventID     string                 `json:"event_id"`
	TraceID     string                 `json:"trace_id"`
	RuleID      string                 `json:"rule_id"`
	Source      string                 `json:"source"`
}

type scriptOutput struct {
	Payload     map[string]interface{} `json:"payload"`
	Metadata    *map[string]string     `json:"metadata,omitempty"`
	TimestampMS *int64                 `json:"timestamp_ms,omitempty"`
}
