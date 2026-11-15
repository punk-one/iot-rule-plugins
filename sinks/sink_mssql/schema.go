package main

const configSchema = `{"type": "object", "properties": {"connector_ref": {"type": "string", "minLength": 1}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "params": {"type": "object", "maxProperties": 128, "additionalProperties": {"type": "string", "minLength": 1}}, "statement": {"type": "string", "minLength": 1}}, "required": ["connector_ref", "statement"], "additionalProperties": false}`
