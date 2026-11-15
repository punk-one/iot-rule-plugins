package main

const configSchema = `{"type": "object", "required": ["host"], "properties": {"host": {"type": "string", "minLength": 1}, "port": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 19000}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 3000}, "allow_dnc_reads": {"type": "boolean", "default": false, "description": "Explicitly allow DNC login for read-only runtime selectors; no control writes enabled"}}, "additionalProperties": false}`
