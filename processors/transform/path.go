package main

import (
	"fmt"
	"strconv"
	"strings"
)

// resolvePath resolves a path expression against the given payload map.
// Supported syntax:
//
//	$.a.b.c    — nested field access
//	$keys()    — returns all top-level keys of the payload
//	$.*        — returns the first non-metadata-key field value
//	metadata.key — access metadata field (special case)
func resolvePath(payload map[string]interface{}, expression string) (interface{}, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil, fmt.Errorf("empty path expression")
	}

	// Handle $keys() — special function
	if expression == "$keys()" {
		keys := make([]string, 0, len(payload))
		for k := range payload {
			keys = append(keys, k)
		}
		return keys, nil
	}

	// Handle $.* — wildcard matching
	if expression == "$.*" {
		return resolveWildcard(payload), nil
	}

	// Handle $.a.b.c — nested field access
	if strings.HasPrefix(expression, "$.") {
		parts := strings.Split(strings.TrimPrefix(expression, "$."), ".")
		return walkValue(payload, parts)
	}

	// Handle metadata.key
	if strings.HasPrefix(expression, "metadata.") {
		key := strings.TrimPrefix(expression, "metadata.")
		return key, nil // Return the key name, not the value — caller handles metadata separately
	}

	// Fallback: treat as dot-separated path
	parts := strings.Split(expression, ".")
	return walkValue(payload, parts)
}

// resolveWildcard returns the first top-level field value that is not a
// metadata key. It returns the value as-is, not wrapped in a map.
func resolveWildcard(payload map[string]interface{}) interface{} {
	metaKeys := map[string]bool{
		"trace_id": true, "time": true, "send_at": true,
		"timestamp": true, "event_type": true, "source": true,
		"rule_id": true, "metadata": true,
	}
	for k, v := range payload {
		if !metaKeys[k] {
			return v
		}
	}
	return nil
}

// walkValue walks a dot-separated path into a nested value.
func walkValue(current interface{}, parts []string) (interface{}, error) {
	if len(parts) == 0 {
		return current, nil
	}
	for _, part := range parts {
		switch typed := current.(type) {
		case map[string]interface{}:
			next, ok := typed[part]
			if !ok {
				return nil, fmt.Errorf("path segment %q not found", part)
			}
			current = next
		case []interface{}:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, fmt.Errorf("invalid array index %q", part)
			}
			current = typed[index]
		default:
			return nil, fmt.Errorf("cannot traverse into %T for segment %q", current, part)
		}
	}
	return current, nil
}

// isMetadataKey returns true if the key is a common metadata field.
func isMetadataKey(key string) bool {
	switch key {
	case "trace_id", "time", "send_at", "timestamp", "event_type", "source", "rule_id", "metadata":
		return true
	}
	return false
}
