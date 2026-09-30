package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// templateFuncs returns the shared template function map.
// This is identical to transform_map's templateFuncs for consistency.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"json":     func(v interface{}) (string, error) { raw, err := json.Marshal(v); return string(raw), err },
		"dict":     dict,
		"list":     func(args ...interface{}) []interface{} { return args },
		"default":  defaultValue,
		"coalesce": coalesce,
		"toString": func(v interface{}) string { return fmt.Sprintf("%v", v) },
		"toInt": func(v interface{}) (int64, error) {
			return parseInt64(v)
		},
		"toFloat": func(v interface{}) (float64, error) {
			return parseFloat64(v)
		},
		"toBool": func(v interface{}) (bool, error) {
			return parseBool(v)
		},
		"upper": func(v interface{}) string { return strings.ToUpper(fmt.Sprintf("%v", v)) },
		"lower": func(v interface{}) string { return strings.ToLower(fmt.Sprintf("%v", v)) },
		"trim":  func(v interface{}) string { return strings.TrimSpace(fmt.Sprintf("%v", v)) },
		"hasKey": func(v interface{}, key string) bool {
			m, ok := asMap(v)
			if !ok {
				return false
			}
			_, ok = m[key]
			return ok
		},
		"now": func() string {
			return time.Now().UTC().Format(time.RFC3339Nano)
		},
		"unixMilli": func(v interface{}) (string, error) {
			ms, err := parseInt64(v)
			if err != nil {
				return "", err
			}
			return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano), nil
		},
		"unixSec": func(v interface{}) (string, error) {
			sec, err := parseInt64(v)
			if err != nil {
				return "", err
			}
			return time.Unix(sec, 0).UTC().Format(time.RFC3339Nano), nil
		},
		"rfc3339": func(v interface{}) (string, error) {
			switch typed := v.(type) {
			case time.Time:
				return typed.UTC().Format(time.RFC3339Nano), nil
			case string:
				ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(typed))
				if err != nil {
					return "", err
				}
				return ts.UTC().Format(time.RFC3339Nano), nil
			default:
				return "", fmt.Errorf("unsupported rfc3339 value type %T", v)
			}
		},
		"keys": func(v interface{}) ([]string, error) {
			m, ok := asMap(v)
			if !ok {
				return nil, fmt.Errorf("keys expects a map, got %T", v)
			}
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			return keys, nil
		},
	}
}

// compileTemplate compiles a Go template string for use in template mappings.
func compileTemplate(name, tplStr string) (*template.Template, error) {
	return template.New(name).
		Funcs(templateFuncs()).
		Option("missingkey=zero").
		Parse(tplStr)
}

// renderTemplate renders a template with the given context.
func renderTemplate(tpl *template.Template, ctx map[string]interface{}) (string, error) {
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, ctx); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// buildTemplateContext builds the template context from an event.
// The context includes .payload, .metadata, .meta, .trace_id, .event.
func buildTemplateContext(payload map[string]interface{}, metadata map[string]string, traceID string, extra map[string]interface{}) map[string]interface{} {
	ctx := map[string]interface{}{
		"payload":  payload,
		"metadata": metadata,
		"meta":     metadata,
		"trace_id": traceID,
		"event": map[string]interface{}{
			"trace_id": traceID,
		},
	}
	for k, v := range extra {
		ctx[k] = v
	}
	return ctx
}

// decodeRenderedTemplate decodes a rendered template string based on the
// original template. If the original is a pure template action ({{ json ... }},
// {{ toInt ... }}, etc.), the rendered string is decoded into the appropriate type.
func decodeRenderedTemplate(rawTemplate, rendered string) (interface{}, error) {
	actionName, typed := pureTemplateAction(rawTemplate)
	if !typed {
		return rendered, nil
	}
	switch actionName {
	case "json":
		var decoded interface{}
		if err := json.Unmarshal([]byte(rendered), &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	case "toInt":
		return strconv.ParseInt(strings.TrimSpace(rendered), 10, 64)
	case "toFloat":
		return strconv.ParseFloat(strings.TrimSpace(rendered), 64)
	case "toBool":
		return strconv.ParseBool(strings.TrimSpace(rendered))
	default:
		return rendered, nil
	}
}

// pureTemplateAction checks if a template string is a single pure action call
// like {{ json .x }} or {{ toInt .y }}.
func pureTemplateAction(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "{{") || !strings.HasSuffix(trimmed, "}}") {
		return "", false
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "{{"), "}}"))
	if strings.Contains(body, "{{") || strings.Contains(body, "}}") {
		return "", false
	}
	fields := strings.Fields(body)
	if len(fields) == 0 {
		return "", false
	}
	switch fields[0] {
	case "json", "toInt", "toFloat", "toBool":
		return fields[0], true
	default:
		return "", false
	}
}

// --- Utility functions (shared with transform_map) ---

func dict(args ...interface{}) (map[string]interface{}, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("dict expects an even number of arguments")
	}
	out := make(map[string]interface{}, len(args)/2)
	for idx := 0; idx < len(args); idx += 2 {
		key, ok := args[idx].(string)
		if !ok {
			return nil, fmt.Errorf("dict key at index %d must be a string", idx)
		}
		out[key] = args[idx+1]
	}
	return out, nil
}

func defaultValue(value interface{}, fallback interface{}) interface{} {
	if isEmptyValue(value) {
		return fallback
	}
	return value
}

func coalesce(args ...interface{}) interface{} {
	for _, arg := range args {
		if !isEmptyValue(arg) {
			return arg
		}
	}
	return nil
}

func isEmptyValue(value interface{}) bool {
	if value == nil {
		return true
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) == ""
	case bool:
		return !typed
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() == 0
	}
	return false
}

func asMap(value interface{}) (map[string]interface{}, bool) {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneMap(typed), true
	case map[string]string:
		out := make(map[string]interface{}, len(typed))
		for k, v := range typed {
			out[k] = v
		}
		return out, true
	default:
		return nil, false
	}
}

func cloneMap(src map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(src))
	for k, v := range src {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneMap(typed)
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = cloneValue(item)
		}
		return out
	default:
		return typed
	}
}

func parseInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		return int64(typed), nil
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		return int64(typed), nil
	case float32:
		return int64(typed), nil
	case float64:
		return int64(typed), nil
	case json.Number:
		return typed.Int64()
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer value type %T", value)
	}
}

func parseFloat64(value interface{}) (float64, error) {
	switch typed := value.(type) {
	case int:
		return float64(typed), nil
	case int8:
		return float64(typed), nil
	case int16:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case uint:
		return float64(typed), nil
	case uint8:
		return float64(typed), nil
	case uint16:
		return float64(typed), nil
	case uint32:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	case json.Number:
		return typed.Float64()
	case string:
		return strconv.ParseFloat(strings.TrimSpace(typed), 64)
	default:
		return 0, fmt.Errorf("unsupported float value type %T", value)
	}
}

func parseBool(value interface{}) (bool, error) {
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case string:
		return strconv.ParseBool(strings.TrimSpace(typed))
	default:
		return false, fmt.Errorf("unsupported bool value type %T", value)
	}
}

// setPathValue sets a value at a dot-separated path in a map.
func setPathValue(dst map[string]interface{}, path string, value interface{}, overwrite bool) error {
	parts := strings.Split(path, ".")
	current := dst
	for i, part := range parts {
		if i == len(parts)-1 {
			if _, exists := current[part]; exists && !overwrite {
				return nil
			}
			current[part] = value
			return nil
		}
		next, ok := current[part]
		if !ok {
			child := make(map[string]interface{})
			current[part] = child
			current = child
			continue
		}
		child, ok := next.(map[string]interface{})
		if !ok {
			return fmt.Errorf("path %q collides with non-object field %q", path, part)
		}
		current = child
	}
	return nil
}

// mergeMaps merges src into dst, optionally overwriting existing keys.
func mergeMaps(dst map[string]interface{}, src map[string]interface{}, overwrite bool) {
	for k, v := range src {
		if _, exists := dst[k]; exists && !overwrite {
			continue
		}
		dst[k] = cloneValue(v)
	}
}

// excludeFields returns a copy of the map with the specified keys removed.
func excludeFields(m map[string]interface{}, exclude []string) map[string]interface{} {
	if len(exclude) == 0 {
		return m
	}
	excludeSet := make(map[string]bool, len(exclude))
	for _, e := range exclude {
		excludeSet[e] = true
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if !excludeSet[k] {
			out[k] = v
		}
	}
	return out
}
