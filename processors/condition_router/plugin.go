package main

import (
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"math"
	"reflect"
	"strings"
)

type condition struct {
	Path  string `json:"path"`
	Op    string `json:"op"`
	Value any    `json:"value,omitempty"`
}
type config struct {
	Mode       string      `json:"mode"`
	Conditions []condition `json:"conditions"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "condition-router", Name: "Condition Router", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Type-aware all/any conditions routed to matched or otherwise labels", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 2, ConfigSchema: `{"type":"object","required":["conditions"],"properties":{"mode":{"type":"string","enum":["all","any"],"default":"all"},"conditions":{"type":"array","minItems":1,"maxItems":64,"items":{"type":"object","required":["path","op"],"properties":{"path":{"type":"string","minLength":1},"op":{"type":"string","enum":["eq","ne","gt","gte","lt","lte","exists","missing","contains"]},"value":{}},"additionalProperties":false}}},"additionalProperties":false}`}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg.Mode = "all"
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if (p.cfg.Mode != "all" && p.cfg.Mode != "any") || len(p.cfg.Conditions) == 0 || len(p.cfg.Conditions) > 64 {
		return sdk.FatalError(errors.New("invalid condition config"))
	}
	for _, c := range p.cfg.Conditions {
		if c.Path == "" || strings.HasPrefix(c.Path, ".") || strings.HasSuffix(c.Path, ".") || strings.Contains(c.Path, "..") {
			return sdk.FatalError(errors.New("invalid payload path"))
		}
		switch c.Op {
		case "eq", "ne", "exists", "missing":
		case "contains":
			if _, ok := c.Value.(string); !ok {
				return sdk.FatalError(errors.New("contains requires string value"))
			}
		case "gt", "gte", "lt", "lte":
			if _, ok := c.Value.(float64); !ok {
				return sdk.FatalError(errors.New("ordered comparison requires numeric value"))
			}
		default:
			return sdk.FatalError(errors.New("unsupported condition operator"))
		}
	}
	return nil
}
func (p *plugin) Start(ctx sdk.RuntimeContext) error {
	if ctx == nil {
		return errors.New("runtime required")
	}
	p.ctx = ctx
	return nil
}
func lookup(payload map[string]any, path string) (any, bool) {
	var cur any = payload
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
func evaluate(payload map[string]any, c condition) bool {
	value, exists := lookup(payload, c.Path)
	switch c.Op {
	case "exists":
		return exists
	case "missing":
		return !exists
	case "eq":
		if a, ok := value.(float64); ok {
			if b, ok := c.Value.(float64); ok {
				return !math.IsNaN(a) && !math.IsNaN(b) && a == b
			}
		}
		return exists && reflect.DeepEqual(value, c.Value)
	case "ne":
		return exists && !reflect.DeepEqual(value, c.Value)
	case "contains":
		text, ok := value.(string)
		needle, _ := c.Value.(string)
		return ok && strings.Contains(text, needle)
	}
	a, ok := value.(float64)
	b, ok2 := c.Value.(float64)
	if !ok || !ok2 {
		return false
	}
	switch c.Op {
	case "gt":
		return a > b
	case "gte":
		return a >= b
	case "lt":
		return a < b
	case "lte":
		return a <= b
	}
	return false
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	matched := p.cfg.Mode == "all"
	for _, c := range p.cfg.Conditions {
		result := evaluate(e.Payload, c)
		if p.cfg.Mode == "all" && !result {
			matched = false
			break
		}
		if p.cfg.Mode == "any" && result {
			matched = true
			break
		}
	}
	label := "otherwise"
	if matched {
		label = "matched"
	}
	return p.ctx.Emitter().EmitTo(label, e.Clone())
}
func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
