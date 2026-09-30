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

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

const transformMapConfigSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["process"],
  "properties": {
    "process": {
      "type": "object",
      "properties": {
        "template": {
          "type": "string",
          "title": "Template",
          "format": "json",
          "description": "JSON object template text rendered with Go templates, supports expressions like {{ .payload.device_code }}"
        },
        "iterate": {
          "type": "string",
          "description": "Optional path to an array or object to iterate. Each element produces one output event and is exposed to templates as .item/.value, for example payload.data.metrics"
        },
        "expand": {
          "type": "array",
          "description": "Optional field expansion rules. Each rule copies a source value into the rendered payload without creating additional events",
          "items": {
            "type": "object",
            "required": ["from"],
            "properties": {
              "from": {
                "type": "string",
                "description": "Source path resolved from the input event, for example payload.data, metadata.trace_id, event.source, or payload.data.metrics. If no prefix is provided, the path is resolved from payload"
              },
              "to": {
                "type": "string",
                "description": "Destination path inside the output payload. Leave empty or use root to merge an object into the payload root; use a dotted path like raw.data to store the whole value under a nested field"
              },
              "overwrite": {
                "type": "boolean",
                "default": false,
                "description": "Whether to overwrite an existing destination field. When false, existing values in the rendered payload are preserved"
              }
            },
            "additionalProperties": false
          }
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}`

type Config struct {
	Process struct {
		Template interface{}  `json:"template,omitempty"`
		Iterate  string       `json:"iterate,omitempty"`
		Expand   []ExpandRule `json:"expand,omitempty"`
	} `json:"process"`
}

type ExpandRule struct {
	From      string `json:"from"`
	To        string `json:"to,omitempty"`
	Overwrite bool   `json:"overwrite,omitempty"`
}

type valueRenderer interface {
	Render(ctx map[string]interface{}) (interface{}, error)
}

type literalRenderer struct {
	value interface{}
}

func (r literalRenderer) Render(_ map[string]interface{}) (interface{}, error) {
	return cloneValue(r.value), nil
}

type templateStringRenderer struct {
	raw string
	tpl *template.Template
}

func (r templateStringRenderer) Render(ctx map[string]interface{}) (interface{}, error) {
	var buf bytes.Buffer
	if err := r.tpl.Execute(&buf, ctx); err != nil {
		return nil, err
	}
	return decodeRenderedTemplate(r.raw, buf.String())
}

type mapRenderer map[string]valueRenderer

func (r mapRenderer) Render(ctx map[string]interface{}) (interface{}, error) {
	out := make(map[string]interface{}, len(r))
	keys := make([]string, 0, len(r))
	for key := range r {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		rendered, err := r[key].Render(ctx)
		if err != nil {
			return nil, err
		}
		out[key] = rendered
	}
	return out, nil
}

type sliceRenderer []valueRenderer

func (r sliceRenderer) Render(ctx map[string]interface{}) (interface{}, error) {
	out := make([]interface{}, 0, len(r))
	for _, item := range r {
		rendered, err := item.Render(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, rendered)
	}
	return out, nil
}

type TransformMapPlugin struct {
	sdk.BasePlugin
	cfg      Config
	ctx      sdk.RuntimeContext
	renderer valueRenderer
}

func (p *TransformMapPlugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "transform-map",
		Name:        "Transform Map",
		Version:     "1.0.0",
		Description: "Render row payloads with Go templates and optional field expansion",
		Type:        sdk.PluginTypeProcessor,
		Category:    "transform",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  1,
			SupportBatch: false,
			SupportAck:   false,
			ConfigSchema: transformMapConfigSchema,
		},
	}
}

func (p *TransformMapPlugin) Init(cfg sdk.PluginConfig) error {
	if err := json.Unmarshal(cfg.Raw, &p.cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	p.cfg.Process.Iterate = strings.TrimSpace(p.cfg.Process.Iterate)
	for idx := range p.cfg.Process.Expand {
		p.cfg.Process.Expand[idx].From = strings.TrimSpace(p.cfg.Process.Expand[idx].From)
		p.cfg.Process.Expand[idx].To = strings.TrimSpace(p.cfg.Process.Expand[idx].To)
		if p.cfg.Process.Expand[idx].From == "" {
			return fmt.Errorf("process.expand[%d].from is required", idx)
		}
	}

	if p.cfg.Process.Template == nil && len(p.cfg.Process.Expand) == 0 {
		return fmt.Errorf("process.template or process.expand is required")
	}

	if p.cfg.Process.Template != nil {
		templateSpec, err := normalizeTemplateSpec(p.cfg.Process.Template)
		if err != nil {
			return err
		}
		renderer, err := compileRenderer(templateSpec)
		if err != nil {
			return err
		}
		p.renderer = renderer
	}

	return nil
}

func (p *TransformMapPlugin) Start(ctx sdk.RuntimeContext) error {
	p.ctx = ctx
	return nil
}

func (p *TransformMapPlugin) Stop() error {
	return nil
}

func (p *TransformMapPlugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return fmt.Errorf("runtime context not initialized")
	}

	contexts, err := p.iterationContexts(e)
	if err != nil {
		return err
	}
	if len(contexts) == 0 {
		return nil
	}

	for _, extra := range contexts {
		payload, err := p.renderPayload(e, extra)
		if err != nil {
			return err
		}

		out := e
		out.Payload = payload
		out.RuleID = p.ctx.RuleID()
		out.Source = p.ctx.NodeID()
		if err := p.ctx.Emitter().Publish(out); err != nil {
			return err
		}
	}
	return nil
}

func (p *TransformMapPlugin) OnEvents(events []sdk.Event) error {
	for _, event := range events {
		if err := p.OnEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *TransformMapPlugin) iterationContexts(e sdk.Event) ([]map[string]interface{}, error) {
	if p.cfg.Process.Iterate == "" {
		return []map[string]interface{}{{}}, nil
	}

	value, ok := resolveSourcePath(e, p.cfg.Process.Iterate)
	if !ok || value == nil {
		return nil, nil
	}

	switch typed := value.(type) {
	case []interface{}:
		contexts := make([]map[string]interface{}, 0, len(typed))
		for idx, item := range typed {
			contexts = append(contexts, map[string]interface{}{
				"index": idx,
				"item":  item,
				"value": item,
			})
		}
		return contexts, nil
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		contexts := make([]map[string]interface{}, 0, len(keys))
		for _, key := range keys {
			contexts = append(contexts, map[string]interface{}{
				"key":   key,
				"item":  typed[key],
				"value": typed[key],
			})
		}
		return contexts, nil
	default:
		return nil, fmt.Errorf("process.iterate path %q must resolve to array or object", p.cfg.Process.Iterate)
	}
}

func (p *TransformMapPlugin) renderPayload(e sdk.Event, extra map[string]interface{}) (map[string]interface{}, error) {
	ctx := buildTemplateContext(e, extra)
	payload := make(map[string]interface{})

	if p.renderer != nil {
		rendered, err := p.renderer.Render(ctx)
		if err != nil {
			return nil, err
		}
		renderedMap, ok := rendered.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("process.template must render a JSON object")
		}
		payload = renderedMap
	}

	for _, expand := range p.cfg.Process.Expand {
		value, ok := resolveSourcePath(e, expand.From)
		if !ok || value == nil {
			continue
		}

		targetPath := expand.To
		if targetPath == "" || targetPath == "root" {
			sourceMap, ok := asMap(value)
			if !ok {
				return nil, fmt.Errorf("process.expand.from %q must resolve to an object when to=root", expand.From)
			}
			mergeMaps(payload, sourceMap, expand.Overwrite)
			continue
		}

		if err := setPathValue(payload, targetPath, cloneValue(value), expand.Overwrite); err != nil {
			return nil, err
		}
	}

	return payload, nil
}

func normalizeTemplateSpec(spec interface{}) (interface{}, error) {
	switch typed := spec.(type) {
	case string:
		var decoded interface{}
		if err := json.Unmarshal([]byte(typed), &decoded); err != nil {
			return nil, fmt.Errorf("failed to parse process.template JSON: %w", err)
		}
		return decoded, nil
	default:
		return spec, nil
	}
}

func compileRenderer(spec interface{}) (valueRenderer, error) {
	switch typed := spec.(type) {
	case map[string]interface{}:
		renderers := make(mapRenderer, len(typed))
		for key, value := range typed {
			renderer, err := compileRenderer(value)
			if err != nil {
				return nil, err
			}
			renderers[key] = renderer
		}
		return renderers, nil
	case []interface{}:
		renderers := make(sliceRenderer, 0, len(typed))
		for _, value := range typed {
			renderer, err := compileRenderer(value)
			if err != nil {
				return nil, err
			}
			renderers = append(renderers, renderer)
		}
		return renderers, nil
	case string:
		if !strings.Contains(typed, "{{") {
			return literalRenderer{value: typed}, nil
		}
		tpl, err := template.New("transform-map").
			Funcs(templateFuncs()).
			Option("missingkey=zero").
			Parse(typed)
		if err != nil {
			return nil, fmt.Errorf("parse template %q: %w", typed, err)
		}
		return templateStringRenderer{raw: typed, tpl: tpl}, nil
	default:
		return literalRenderer{value: typed}, nil
	}
}

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
	}
}

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

func buildTemplateContext(e sdk.Event, extra map[string]interface{}) map[string]interface{} {
	ctx := map[string]interface{}{
		"payload":  e.Payload,
		"metadata": e.Metadata,
		"meta":     e.Metadata,
		"trace_id": sdk.GetTraceID(e),
		"event": map[string]interface{}{
			"id":        e.ID,
			"timestamp": e.Timestamp,
			"rule_id":   e.RuleID,
			"source":    e.Source,
		},
	}
	for key, value := range extra {
		ctx[key] = value
	}
	return ctx
}

func resolveSourcePath(e sdk.Event, path string) (interface{}, bool) {
	parts := strings.Split(strings.TrimSpace(path), ".")
	if len(parts) == 0 || parts[0] == "" {
		return nil, false
	}

	switch parts[0] {
	case "payload":
		return walkPath(e.Payload, parts[1:])
	case "metadata", "meta":
		if len(parts) == 1 {
			return e.Metadata, true
		}
		value, ok := e.Metadata[parts[1]]
		return value, ok
	case "event":
		eventMap := map[string]interface{}{
			"id":        e.ID,
			"timestamp": e.Timestamp,
			"rule_id":   e.RuleID,
			"source":    e.Source,
		}
		return walkPath(eventMap, parts[1:])
	default:
		return walkPath(e.Payload, parts)
	}
}

func walkPath(current interface{}, parts []string) (interface{}, bool) {
	if len(parts) == 0 {
		return current, true
	}
	for _, part := range parts {
		switch typed := current.(type) {
		case map[string]interface{}:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		case map[string]string:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		case []interface{}:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			current = typed[index]
		default:
			return nil, false
		}
	}
	return current, true
}

func asMap(value interface{}) (map[string]interface{}, bool) {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneMap(typed), true
	case map[string]string:
		out := make(map[string]interface{}, len(typed))
		for key, val := range typed {
			out[key] = val
		}
		return out, true
	default:
		return nil, false
	}
}

func mergeMaps(dst map[string]interface{}, src map[string]interface{}, overwrite bool) {
	for key, value := range src {
		if _, exists := dst[key]; exists && !overwrite {
			continue
		}
		dst[key] = cloneValue(value)
	}
}

func setPathValue(dst map[string]interface{}, path string, value interface{}, overwrite bool) error {
	parts := strings.Split(path, ".")
	current := dst
	for idx, part := range parts {
		if idx == len(parts)-1 {
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

func cloneValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneMap(typed)
	case []interface{}:
		out := make([]interface{}, len(typed))
		for idx, item := range typed {
			out[idx] = cloneValue(item)
		}
		return out
	default:
		return typed
	}
}

func cloneMap(src map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(src))
	for key, value := range src {
		out[key] = cloneValue(value)
	}
	return out
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
