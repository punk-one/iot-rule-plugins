package main

const configSchema = `{"type": "object", "required": ["origin_instance", "secret"], "properties": {"origin_instance": {"type": "string", "minLength": 1, "maxLength": 256}, "key_id": {"type": "string", "minLength": 1, "maxLength": 256, "default": "v1"}, "secret": {"type": "string", "minLength": 32, "maxLength": 512, "format": "password"}, "max_age_ms": {"type": "integer", "minimum": 60000, "maximum": 31622400000, "default": 604800000}}, "additionalProperties": false}`
