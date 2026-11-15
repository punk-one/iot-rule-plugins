package main

const configSchema = `{"type": "object", "required": ["base_url", "org", "bucket"], "properties": {"base_url": {"type": "string", "format": "uri"}, "org": {"type": "string", "minLength": 1}, "bucket": {"type": "string", "minLength": 1}, "token": {"type": "string", "format": "password"}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "max_response_bytes": {"type": "integer", "minimum": 1, "maximum": 8388608, "default": 1048576}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 1000}}, "additionalProperties": false}`
