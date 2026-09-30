package main

import "fmt"

// executeCopyMapping executes a copy mapping.
// It resolves the source path from the payload, gets the value, and writes it to the output under the target field name.
// If the source path is not found, it silently skips.
func executeCopyMapping(payload map[string]interface{}, m Mapping) (string, interface{}, error) {
	// Resolve the source path
	value, err := resolvePath(payload, m.From)
	if err != nil {
		return "", nil, nil // silently skip if path not found
	}
	if value == nil {
		return "", nil, nil
	}

	// Handle $keys() special case — returns the first key as the device_code value
	if m.From == "$keys()" {
		keys, ok := value.([]string)
		if ok && len(keys) > 0 {
			// Find the first non-metadata key
			for _, k := range keys {
				if !isMetadataKey(k) {
					return m.To, k, nil
				}
			}
		}
	}

	return m.To, cloneValue(value), nil
}

// executeObjectMapping executes an object mapping.
// It renders the key template, resolves the value from the JSONPath,
// applies exclusions, and writes the result to the output.
func executeObjectMapping(payload map[string]interface{}, metadata map[string]string, traceID string, m Mapping) (string, interface{}, error) {
	// Build the template context
	ctx := buildTemplateContext(payload, metadata, traceID, nil)

	// Render the key template
	tpl, err := compileTemplate("object-key", m.Key.Template)
	if err != nil {
		return "", nil, fmt.Errorf("compile key template: %w", err)
	}
	key, err := renderTemplate(tpl, ctx)
	if err != nil {
		return "", nil, fmt.Errorf("render key template: %w", err)
	}
	key = stringsTrimSpace(key)
	if key == "" {
		return "", nil, nil
	}

	// Resolve the value
	var value interface{}
	if m.Value != nil && m.Value.JSONPath != "" {
		value, err = resolvePath(payload, m.Value.JSONPath)
		if err != nil {
			return "", nil, fmt.Errorf("resolve value path %q: %w", m.Value.JSONPath, err)
		}
		if value == nil {
			return "", nil, nil
		}
	} else {
		// Default: use the entire payload
		value = cloneMap(payload)
	}

	// Apply exclusions
	exclude := m.Exclude
	if m.Value != nil && len(m.Value.Exclude) > 0 {
		exclude = append(exclude, m.Value.Exclude...)
	}
	if len(exclude) > 0 {
		if m, ok := value.(map[string]interface{}); ok {
			value = excludeFields(m, exclude)
		}
	}

	return key, cloneValue(value), nil
}

// executeTemplateMapping executes a template mapping.
// It renders the template string and writes the result to the output under the target field name.
func executeTemplateMapping(payload map[string]interface{}, metadata map[string]string, traceID string, m Mapping) (string, interface{}, error) {
	ctx := buildTemplateContext(payload, metadata, traceID, nil)

	tpl, err := compileTemplate("mapping-template", m.Template)
	if err != nil {
		return "", nil, fmt.Errorf("compile template: %w", err)
	}

	rendered, err := renderTemplate(tpl, ctx)
	if err != nil {
		return "", nil, fmt.Errorf("render template: %w", err)
	}

	// Decode typed values (json, toInt, toFloat, toBool)
	decoded, err := decodeRenderedTemplate(m.Template, rendered)
	if err != nil {
		return "", nil, fmt.Errorf("decode template result: %w", err)
	}

	return m.To, decoded, nil
}

// stringsTrimSpace is a helper to avoid import cycle.
func stringsTrimSpace(s string) string {
	// Simple trim to avoid importing strings in the mapping package
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}
