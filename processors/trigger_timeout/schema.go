package main

const configSchema = `{"type": "object", "properties": {"key_path": {"type": "string", "minLength": 1, "default": "device_code"}, "delay_ms": {"type": "integer", "minimum": 10, "maximum": 86400000, "default": 1000}, "max_keys": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 1024}, "max_bytes": {"type": "integer", "minimum": 1024, "maximum": 67108864, "default": 8388608}}, "additionalProperties": false}`
