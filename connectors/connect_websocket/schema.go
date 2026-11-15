package main

const configSchema = `{"type": "object", "required": ["url"], "properties": {"url": {"type": "string", "format": "uri"}, "bearer_token": {"type": "string", "format": "password"}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "max_frame_bytes": {"type": "integer", "minimum": 1, "maximum": 1048576, "default": 65536}, "heartbeat_ms": {"type": "integer", "minimum": 1000, "maximum": 60000, "default": 15000}}, "additionalProperties": false}`
