package main

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["connector_ref", "mappings"],
  "properties": {
    "connector_ref": {
      "type": "string",
      "minLength": 1,
      "title": "Redis 连接器",
      "description": "用于查询关系表的 Redis Connector 资源 ID"
    },
    "default_hash_key": {
      "type": "string",
      "title": "默认 Hash Key",
      "description": "映射项未指定 hash_key 时使用"
    },
    "timeout_ms": {
      "type": "integer",
      "minimum": 10,
      "maximum": 30000,
      "default": 3000,
      "title": "单次查询超时（毫秒）"
    },
    "mappings": {
      "type": "array",
      "minItems": 1,
      "title": "关系映射",
      "items": {
        "type": "object",
        "required": ["source_path", "target_path"],
        "properties": {
          "source_path": {"type": "string", "minLength": 1, "title": "输入字段路径"},
          "target_path": {"type": "string", "minLength": 1, "title": "输出字段路径"},
          "hash_key": {"type": "string", "title": "Redis Hash Key"},
          "on_missing": {
            "type": "string",
            "enum": ["skip", "default", "error"],
            "default": "skip",
            "title": "未匹配时"
          },
          "default_value": {"title": "默认值"}
        },
        "additionalProperties": false
      }
    }
  },
  "additionalProperties": false
}`

type mappingConfig struct {
	SourcePath   string      `json:"source_path"`
	TargetPath   string      `json:"target_path"`
	HashKey      string      `json:"hash_key,omitempty"`
	OnMissing    string      `json:"on_missing,omitempty"`
	DefaultValue interface{} `json:"default_value,omitempty"`
}

type pluginConfig struct {
	ConnectorRef   string          `json:"connector_ref"`
	DefaultHashKey string          `json:"default_hash_key,omitempty"`
	TimeoutMS      int             `json:"timeout_ms,omitempty"`
	Mappings       []mappingConfig `json:"mappings"`
}
