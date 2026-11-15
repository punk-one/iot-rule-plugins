package main

const configSchema = `{"type": "object", "properties": {"points": {"type": "array", "minItems": 1, "maxItems": 1024, "items": {"type": "object", "properties": {"name": {"type": "string", "minLength": 1}, "scale": {"type": "number"}, "offset": {"type": "number"}, "precision": {"type": "integer", "minimum": 0, "maximum": 9}, "min": {"type": "number"}, "max": {"type": "number"}, "outOfRangePolicy": {"type": "string", "enum": ["null", "default"]}, "defaultValue": {}}, "required": ["name"], "additionalProperties": false}}}, "required": ["points"], "additionalProperties": false}`
