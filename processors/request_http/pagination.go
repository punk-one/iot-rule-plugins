package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type paginationConfig struct {
	ItemsPointer   string `json:"items_pointer"`
	NextPointer    string `json:"next_pointer"`
	MaxPages       int    `json:"max_pages"`
	MaxItems       int    `json:"max_items"`
	MaxBytes       int    `json:"max_total_bytes"`
	TotalTimeoutMS int    `json:"total_timeout_ms"`
}

func validatePagination(cfg *paginationConfig) error {
	if cfg == nil {
		return nil
	}
	if cfg.MaxPages == 0 {
		cfg.MaxPages = 10
	}
	if cfg.MaxItems == 0 {
		cfg.MaxItems = 1000
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 4194304
	}
	if cfg.TotalTimeoutMS == 0 {
		cfg.TotalTimeoutMS = 30000
	}
	if cfg.MaxPages < 1 || cfg.MaxPages > 100 || cfg.MaxItems < 1 || cfg.MaxItems > 10000 || cfg.MaxBytes < 1 || cfg.MaxBytes > 8388608 || cfg.TotalTimeoutMS < 50 || cfg.TotalTimeoutMS > 30000 || cfg.NextPointer == "" || len(cfg.ItemsPointer) > 1024 || len(cfg.NextPointer) > 1024 {
		return errors.New("invalid pagination bounds or pointers")
	}
	for _, pointer := range []string{cfg.ItemsPointer, cfg.NextPointer} {
		if _, err := pointerTokens(pointer); err != nil {
			return err
		}
	}
	return nil
}
func pointerTokens(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("pagination uses RFC6901 JSON pointers")
	}
	parts := strings.Split(pointer[1:], "/")
	if len(parts) > 64 {
		return nil, errors.New("pointer nesting exceeds 64")
	}
	for i, part := range parts {
		var decoded strings.Builder
		for n := 0; n < len(part); n++ {
			if part[n] != '~' {
				decoded.WriteByte(part[n])
				continue
			}
			n++
			if n >= len(part) || (part[n] != '0' && part[n] != '1') {
				return nil, errors.New("invalid JSON pointer escape")
			}
			if part[n] == '0' {
				decoded.WriteByte('~')
			} else {
				decoded.WriteByte('/')
			}
		}
		parts[i] = decoded.String()
	}
	return parts, nil
}
func pointerValue(value any, pointer string) (any, bool) {
	parts, err := pointerTokens(pointer)
	if err != nil {
		return nil, false
	}
	for _, part := range parts {
		switch node := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = node[part]
			if !ok {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) || strconv.Itoa(index) != part {
				return nil, false
			}
			value = node[index]
		default:
			return nil, false
		}
	}
	return value, true
}
func pagePath(path string) bool {
	if len(path) == 0 || len(path) > 8192 || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.ContainsAny(path, "\x00\r\n\\") {
		return false
	}
	parsed, err := url.Parse(path)
	return err == nil && parsed.Scheme == "" && parsed.Host == "" && parsed.User == nil && parsed.Fragment == "" && parsed.Opaque == ""
}
func (p *plugin) paginate(e sdk.Event) error {
	cfg := p.cfg.Pagination
	path := p.cfg.Path
	deadline := time.Now().Add(time.Duration(cfg.TotalTimeoutMS) * time.Millisecond)
	seen := map[string]bool{}
	items := []any{}
	totalBytes := 0
	pages := 0
	for {
		if !pagePath(path) || seen[path] {
			return sdk.SkippableError(errors.New("pagination path is unsafe or cyclic"))
		}
		seen[path] = true
		remaining := time.Until(deadline).Milliseconds()
		if remaining < 1 {
			return sdk.RetryableError(errors.New("pagination total deadline exceeded"))
		}
		raw, _ := json.Marshal(map[string]any{"method": "GET", "path": path})
		response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "request", Payload: raw, TimeoutMS: min(p.cfg.TimeoutMS, int(remaining))})
		if err != nil {
			return sdk.RetryableError(errors.New("pagination request failed; no partial output"))
		}
		totalBytes += len(response.Payload)
		if totalBytes > cfg.MaxBytes {
			return sdk.SkippableError(errors.New("pagination connector-envelope byte budget exceeded"))
		}
		decoded, err := decodeResponse(response.Payload)
		if err != nil {
			return sdk.SkippableError(err)
		}
		status := decoded["status"].(int)
		if status == 429 || status >= 500 {
			return sdk.RetryableError(errors.New("pagination server unavailable; no partial output"))
		}
		if status < 200 || status >= 300 {
			return sdk.SkippableError(errors.New("pagination non-success status"))
		}
		value, exists := pointerValue(decoded["body"], cfg.ItemsPointer)
		if !exists {
			return sdk.SkippableError(errors.New("pagination items pointer missing"))
		}
		page, ok := value.([]any)
		if !ok || len(page) > cfg.MaxItems-len(items) {
			return sdk.SkippableError(errors.New("pagination items must be an array within the total item budget"))
		}
		items = append(items, page...)
		pages++
		next, exists := pointerValue(decoded["body"], cfg.NextPointer)
		if !exists {
			return sdk.SkippableError(errors.New("pagination next pointer missing; use explicit null or empty string at end"))
		}
		if time.Now().After(deadline) {
			return sdk.RetryableError(errors.New("pagination total deadline exceeded"))
		}
		if next == nil {
			break
		}
		nextPath, ok := next.(string)
		if !ok {
			return sdk.SkippableError(errors.New("pagination next path must be string or null"))
		}
		if nextPath == "" {
			break
		}
		if pages >= cfg.MaxPages {
			return sdk.SkippableError(errors.New("pagination page budget exceeded; no truncated output"))
		}
		path = nextPath
	}
	out := e.Clone()
	if out.Payload == nil {
		out.Payload = map[string]any{}
	}
	out.Payload[p.cfg.ResponseField] = map[string]any{"status": 200, "body": map[string]any{"items": items, "page_count": pages, "item_count": len(items)}}
	return p.ctx.Emitter().Publish(out)
}
