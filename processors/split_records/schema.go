package main

const configSchema = `{"type": "object", "properties": {"input_path": {"type": "string", "default": "records"}, "output_field": {"type": "string", "default": "record"}, "max_parts": {"type": "integer", "minimum": 1, "maximum": 4096, "default": 1024}}, "additionalProperties": false}`
