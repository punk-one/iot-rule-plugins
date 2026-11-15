package main

const configSchema = `{"type": "object", "required": ["connector_ref", "device_code"], "properties": {"connector_ref": {"type": "string", "minLength": 1}, "device_code": {"type": "string", "minLength": 1}, "interval_ms": {"type": "integer", "minimum": 50, "maximum": 86400000, "default": 5000}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}}, "additionalProperties": false}`
