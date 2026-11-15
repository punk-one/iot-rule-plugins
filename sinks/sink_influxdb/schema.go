package main

const configSchema = `{"type": "object", "required": ["connector_ref", "measurement"], "properties": {"connector_ref": {"type": "string", "minLength": 1}, "measurement": {"type": "string", "minLength": 1}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "ack_on_success": {"type": "boolean", "default": false}, "field_types": {"type": "object", "maxProperties": 256, "additionalProperties": {"enum": ["int64", "uint64"]}}}, "additionalProperties": false}`
