package main

const configSchema = `{"type": "object", "required": ["connector_ref"], "properties": {"connector_ref": {"type": "string", "minLength": 1}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}}, "additionalProperties": false}`
