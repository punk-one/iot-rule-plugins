package main

const configSchema = `{"type": "object", "properties": {"sample_every": {"type": "integer", "minimum": 1, "maximum": 1000000, "default": 100}, "max_per_second": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10}, "include_values": {"type": "boolean", "default": false}, "fields": {"type": "array", "maxItems": 16, "items": {"type": "string", "minLength": 1, "maxLength": 256}}}, "additionalProperties": false}`
