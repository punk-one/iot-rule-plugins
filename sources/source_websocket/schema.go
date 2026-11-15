package main

const configSchema = `{"type": "object", "required": ["connector_ref", "device_code"], "properties": {"connector_ref": {"type": "string", "minLength": 1}, "device_code": {"type": "string", "minLength": 1}, "buffer_messages": {"type": "integer", "minimum": 1, "maximum": 64, "default": 16}}, "additionalProperties": false}`
