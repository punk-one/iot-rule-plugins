package main

import (
	"bytes"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"regexp"
	"strings"
)

type config struct {
	ConnectorRef string            `json:"connector_ref"`
	Collection   string            `json:"collection"`
	Filter       json.RawMessage   `json:"filter"`
	Bindings     map[string]string `json:"bindings"`
	Sort         string            `json:"sort"`
	Descending   bool              `json:"descending"`
	Limit        int               `json:"limit"`
	TimeoutMS    int               `json:"timeout_ms"`
	ResultField  string            `json:"result_field"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

var fieldPath = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,7}$`)
var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "request-mongodb", Name: "MongoDB Query", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded find with scalar equality bindings and canonical Extended JSON results", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema, ConnectorBinding: &sdk.ConnectorBindingSpec{Required: true, AcceptedFamilies: []string{"mongodb"}, RequiredCapabilities: []string{"read"}}}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{Filter: json.RawMessage(`{}`), Limit: 1000, TimeoutMS: 5000, ResultField: "mongodb"}
	d := json.NewDecoder(bytes.NewReader(raw.Raw))
	d.DisallowUnknownFields()
	if len(raw.Raw) > 131072 || d.Decode(&p.cfg) != nil || d.Decode(new(any)) != io.EOF || p.cfg.ConnectorRef == "" || !identifier.MatchString(p.cfg.Collection) || !identifier.MatchString(p.cfg.ResultField) || p.cfg.Sort != "" && !fieldPath.MatchString(p.cfg.Sort) || p.cfg.Limit < 1 || p.cfg.Limit > 1000 || p.cfg.TimeoutMS < 50 || p.cfg.TimeoutMS > 30000 || len(p.cfg.Bindings) > 64 || len(p.cfg.Filter) > 65536 {
		return sdk.FatalError(errors.New("invalid MongoDB query config"))
	}
	var filter map[string]json.RawMessage
	if json.Unmarshal(p.cfg.Filter, &filter) != nil || filter == nil {
		return sdk.FatalError(errors.New("MongoDB filter must be an object"))
	}
	for field, path := range p.cfg.Bindings {
		if !fieldPath.MatchString(field) || path == "" || len(path) > 256 {
			return sdk.FatalError(errors.New("invalid MongoDB equality binding"))
		}
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil || ctx.Connector() == nil || ctx.Emitter() == nil {
		return errors.New("bound MongoDB connector and emitter required")
	}
	p.ctx = ctx
	return nil
}
func resolve(e sdk.Event, path string) (any, bool) {
	if path == "event.id" {
		return e.ID, e.ID != ""
	}
	value := any(e.Payload)
	for _, part := range strings.Split(strings.TrimPrefix(path, "payload."), ".") {
		m, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return value, true
}
func scalar(v any) bool {
	switch x := v.(type) {
	case nil, bool, string, int8, int16, int32, uint8, uint16, uint32:
		return true
	case int:
		return int64(x) >= -9007199254740991 && int64(x) <= 9007199254740991
	case int64:
		return x >= -9007199254740991 && x <= 9007199254740991
	case uint:
		return uint64(x) <= 9007199254740991
	case uint64:
		return x <= 9007199254740991
	case float64:
		return !math.IsNaN(x) && !math.IsInf(x, 0) && math.Abs(x) <= 9007199254740991
	case float32:
		return !math.IsNaN(float64(x)) && !math.IsInf(float64(x), 0)
	case json.Number:
		_, err := x.Int64()
		return err == nil && scalarInt(x)
	}
	return false
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	and := []any{json.RawMessage(p.cfg.Filter)}
	for field, path := range p.cfg.Bindings {
		v, ok := resolve(e, path)
		if !ok || !scalar(v) {
			return sdk.SkippableError(errors.New("MongoDB binding requires a finite scalar value"))
		}
		and = append(and, map[string]any{field: map[string]any{"$eq": v}})
	}
	filter := any(json.RawMessage(p.cfg.Filter))
	if len(and) > 1 {
		filter = map[string]any{"$and": and}
	}
	raw, err := json.Marshal(map[string]any{"collection": p.cfg.Collection, "filter": filter, "sort": p.cfg.Sort, "descending": p.cfg.Descending, "limit": p.cfg.Limit})
	if err != nil || len(raw) > 131072 {
		return sdk.SkippableError(errors.New("MongoDB query request too large"))
	}
	response, err := p.ctx.Connector().Read(sdk.ConnectorRequest{Target: "find", Payload: raw, TimeoutMS: p.cfg.TimeoutMS})
	if err != nil {
		return sdk.RetryableError(errors.New("MongoDB query failed"))
	}
	result, err := decodeResult(response.Payload, p.cfg.Limit)
	if err != nil {
		return sdk.SkippableError(err)
	}
	out := e.Clone()
	if out.Payload == nil {
		out.Payload = map[string]any{}
	}
	out.Payload[p.cfg.ResultField] = result
	return p.ctx.Emitter().Publish(out)
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
func decodeResult(raw []byte, limit int) (map[string]any, error) {
	var result struct {
		Documents []map[string]any `json:"documents"`
		Count     int              `json:"count"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	d.UseNumber()
	if len(raw) > 5*1048576 || d.Decode(&result) != nil || d.Decode(new(any)) != io.EOF || result.Documents == nil || result.Count != len(result.Documents) || len(result.Documents) > limit {
		return nil, errors.New("invalid or oversized MongoDB result")
	}
	return map[string]any{"documents": result.Documents, "count": result.Count}, nil
}

const configSchema = `{"type":"object","required":["connector_ref","collection"],"properties":{"connector_ref":{"type":"string"},"collection":{"type":"string"},"filter":{"type":"object","default":{},"additionalProperties":true},"bindings":{"type":"object","maxProperties":64,"additionalProperties":{"type":"string"},"description":"Collection field to event scalar path; always combined as equality constraints"},"sort":{"type":"string"},"descending":{"type":"boolean","default":false},"limit":{"type":"integer","minimum":1,"maximum":1000,"default":1000},"timeout_ms":{"type":"integer","minimum":50,"maximum":30000,"default":5000},"result_field":{"type":"string","default":"mongodb"}},"additionalProperties":false}`

func scalarInt(n json.Number) bool {
	v, err := n.Int64()
	return err == nil && v >= -9007199254740991 && v <= 9007199254740991
}
