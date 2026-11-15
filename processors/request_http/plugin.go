package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"strings"
)

type config struct {
	ConnectorRef  string            `json:"connector_ref"`
	Method        string            `json:"method"`
	Path          string            `json:"path"`
	TimeoutMS     int               `json:"timeout_ms"`
	ResponseField string            `json:"response_field"`
	Pagination    *paginationConfig `json:"pagination"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "request-http", Name: "HTTP Request", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Execute an HTTP request on a bound origin and forward the response", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: `{"type": "object", "required": ["connector_ref", "path"], "properties": {"connector_ref": {"type": "string", "minLength": 1}, "path": {"type": "string"}, "method": {"type": "string", "enum": ["GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"], "default": "GET"}, "timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 5000}, "response_field": {"type": "string", "minLength": 1, "default": "response"}, "pagination": {"type": "object", "required": ["items_pointer", "next_pointer"], "properties": {"items_pointer": {"type": "string", "maxLength": 1024}, "next_pointer": {"type": "string", "minLength": 1, "maxLength": 1024}, "max_pages": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10}, "max_items": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 1000}, "max_total_bytes": {"type": "integer", "minimum": 1, "maximum": 8388608, "default": 4194304}, "total_timeout_ms": {"type": "integer", "minimum": 50, "maximum": 30000, "default": 30000}}, "additionalProperties": false}}, "additionalProperties": false}`, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"http"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Method: "GET", TimeoutMS: 5000, ResponseField: "response"}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.ConnectorRef == "" || p.cfg.ResponseField == "" || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 {
		return sdk.FatalError(errors.New("invalid request configuration"))
	}
	if err := validatePagination(p.cfg.Pagination); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.Pagination != nil && (p.cfg.Method != "GET" || !pagePath(p.cfg.Path)) {
		return sdk.FatalError(errors.New("pagination requires GET with a fixed origin-relative absolute path"))
	}
	switch p.cfg.Method {
	case "GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE":
	default:
		return sdk.FatalError(errors.New("unsupported HTTP method"))
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil {
		return errors.New("bound connector required")
	}
	p.ctx = ctx
	return nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	if p.cfg.Pagination != nil {
		return p.paginate(e)
	}
	spec := map[string]any{"method": p.cfg.Method, "path": p.cfg.Path}
	read := p.cfg.Method == "GET" || p.cfg.Method == "HEAD" || p.cfg.Method == "OPTIONS"
	if !read {
		spec["body"] = e.Payload
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return sdk.SkippableError(err)
	}
	req := sdk.ConnectorRequest{Target: "request", Payload: raw, TimeoutMS: p.cfg.TimeoutMS}
	var response sdk.ConnectorResponse
	if read {
		response, err = p.ctx.Connector().Read(req)
	} else {
		response, err = p.ctx.Connector().Write(req)
	}
	if err != nil {
		if read {
			return sdk.RetryableError(errors.New("HTTP read failed"))
		}
		return sdk.SkippableError(errors.New("HTTP write failed or outcome unknown; no automatic retry"))
	}
	decoded, err := decodeResponse(response.Payload)
	if err != nil {
		return sdk.SkippableError(err)
	}
	status := decoded["status"].(int)
	if status >= 500 || status == 429 {
		if read {
			return sdk.RetryableError(errors.New("HTTP server unavailable"))
		}
		return sdk.SkippableError(errors.New("HTTP write rejected or result unknown"))
	}
	if status < 200 || status >= 300 {
		return sdk.SkippableError(errors.New("HTTP status outside 2xx"))
	}
	out := e.Clone()
	if out.Payload == nil {
		out.Payload = map[string]any{}
	}
	out.Payload[p.cfg.ResponseField] = decoded
	return p.ctx.Emitter().Publish(out)
}
func decodeResponse(raw []byte) (map[string]any, error) {
	var r struct {
		Status      int    `json:"status"`
		Body        string `json:"body_base64"`
		ContentType string `json:"content_type"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || r.Status < 100 || r.Status > 599 {
		return nil, errors.New("invalid HTTP connector response")
	}
	body, err := base64.StdEncoding.DecodeString(r.Body)
	if err != nil {
		return nil, errors.New("invalid HTTP body encoding")
	}
	var value any = string(body)
	if strings.Contains(strings.ToLower(r.ContentType), "json") && len(body) > 0 {
		if value, err = decodeSafeJSON(body); err != nil {
			return nil, errors.New("invalid JSON HTTP response")
		}
	}
	return map[string]any{"status": r.Status, "body": value}, nil
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}

func decodeSafeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("invalid JSON body")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("trailing JSON data")
	}
	return safeJSONValue(value, 0)
}
func safeJSONValue(value any, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting limit exceeded")
	}
	switch v := value.(type) {
	case json.Number:
		n, err := v.Float64()
		if err != nil || math.Abs(n) > 9007199254740991 {
			return string(v), nil
		}
		return n, nil
	case []any:
		if len(v) > 4096 {
			return nil, errors.New("JSON array item limit exceeded")
		}
		for i, x := range v {
			next, err := safeJSONValue(x, depth+1)
			if err != nil {
				return nil, err
			}
			v[i] = next
		}
		return v, nil
	case map[string]any:
		if len(v) > 4096 {
			return nil, errors.New("JSON object field limit exceeded")
		}
		for k, x := range v {
			next, err := safeJSONValue(x, depth+1)
			if err != nil {
				return nil, err
			}
			v[k] = next
		}
		return v, nil
	default:
		return value, nil
	}
}
