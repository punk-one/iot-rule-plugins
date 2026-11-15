package main

const configSchema = `{"type": "object", "required": ["directory", "file"], "properties": {"directory": {"type": "string", "minLength": 1}, "file": {"type": "string", "minLength": 1, "default": "telemetry.db"}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "max_connections": {"type": "integer", "enum": [1], "default": 1}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 1000}, "allow_write": {"type": "boolean", "default": false}}, "additionalProperties": false}`
