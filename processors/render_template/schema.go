package main

const configSchema = `{"type": "object", "required": ["template"], "properties": {"template": {"type": "string", "minLength": 1, "maxLength": 65536}, "output_field": {"type": "string", "default": "rendered"}, "escape": {"enum": ["html", "json", "text"], "default": "html"}, "max_bytes": {"type": "integer", "minimum": 1, "maximum": 1048576, "default": 1048576}}, "additionalProperties": false}`
