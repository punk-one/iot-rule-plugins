package main

import "strings"

func getPathValue(payload map[string]interface{}, path string) (interface{}, bool) {
	if payload == nil || path == "" {
		return nil, false
	}

	current := interface{}(payload)
	for _, segment := range strings.Split(path, ".") {
		if segment == "" {
			return nil, false
		}

		obj, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}

		value, exists := obj[segment]
		if !exists {
			return nil, false
		}
		current = value
	}

	return current, true
}
