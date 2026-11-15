package main

const configSchema = `{"type": "object", "required": ["connector_ref", "device_code"], "properties": {"connector_ref": {"type": "string", "minLength": 1}, "device_code": {"type": "string", "minLength": 1}, "input_path": {"type": "string", "default": "data.raw_base64"}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "max_bytes": {"type": "integer", "minimum": 1, "maximum": 1048576, "default": 65507}}, "additionalProperties": false}`
