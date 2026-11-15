package main

const configSchema = `{"type": "object", "required": ["host"], "properties": {"host": {"type": "string", "minLength": 1}, "port": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 44818}, "slot": {"type": "integer", "minimum": 0, "maximum": 31, "description": "Omit for direct UCMM; specify backplane slot for one-hop unconnected send"}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 3000}}, "additionalProperties": false}`
