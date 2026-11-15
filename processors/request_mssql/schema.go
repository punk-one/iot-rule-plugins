package main

const configSchema = `{"type": "object", "properties": {"connector_ref": {"type": "string", "minLength": 1}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "params": {"type": "object", "maxProperties": 128, "additionalProperties": {"type": "string", "minLength": 1}}, "query": {"type": "string", "minLength": 1}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 1000}, "result_field": {"type": "string", "minLength": 1, "default": "query_result"}}, "required": ["connector_ref", "query"], "additionalProperties": false}`
