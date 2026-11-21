package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

type condition struct {
	Path  string `json:"path"`
	Op    string `json:"op"`
	Value any    `json:"value,omitempty"`
}

type route struct {
	Label      string      `json:"label"`
	Mode       string      `json:"mode,omitempty"`
	Conditions []condition `json:"conditions"`
}

type config struct {
	Mode         string      `json:"mode,omitempty"`
	Conditions   []condition `json:"conditions,omitempty"`
	Routes       []route     `json:"routes,omitempty"`
	Match        string      `json:"match,omitempty"`
	DefaultLabel string      `json:"default_label,omitempty"`
}

type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

const conditionSchema = `{
  "type":"array","minItems":1,"maxItems":64,
  "items":{"type":"object","required":["path","op"],"additionalProperties":false,
    "properties":{
      "path":{"type":"string","minLength":1,"maxLength":256},
      "op":{"type":"string","enum":["eq","ne","gt","gte","lt","lte","exists","missing","contains"]},
      "value":{}
    }}
}`

const configSchema = `{
  "type":"object","additionalProperties":false,
  "oneOf":[{"required":["conditions"]},{"required":["routes"]}],
  "properties":{
    "mode":{"type":"string","enum":["all","any"],"default":"all"},
    "conditions":` + conditionSchema + `,
    "match":{"type":"string","enum":["first","all"],"default":"first"},
    "default_label":{"type":"string","minLength":1,"maxLength":64,"default":"otherwise"},
    "routes":{"type":"array","minItems":1,"maxItems":32,"items":{
      "type":"object","required":["label","conditions"],"additionalProperties":false,
      "properties":{
        "label":{"type":"string","minLength":1,"maxLength":64},
        "mode":{"type":"string","enum":["all","any"],"default":"all"},
        "conditions":` + conditionSchema + `
      }
    }}
  }
}`

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID: "condition-router", Name: "Condition Router", Version: "1.0.1", Type: sdk.PluginTypeProcessor,
		Description: "Type-aware condition groups routed to one or all matching labels",
		// Named labels share the normal SDK EmitTo output mechanism.
		Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 2, ConfigSchema: configSchema},
	}
}

func (p *plugin) Init(raw sdk.PluginConfig) error {
	var cfg config
	decoder := json.NewDecoder(bytes.NewReader(raw.Raw))
	decoder.UseNumber()
	if !json.Valid(raw.Raw) {
		return sdk.FatalError(errors.New("invalid condition config JSON"))
	}
	if err := decoder.Decode(&cfg); err != nil {
		return sdk.FatalError(err)
	}
	if err := cfg.validate(); err != nil {
		return sdk.FatalError(err)
	}
	p.cfg = cfg
	return nil
}

func (cfg *config) validate() error {
	if cfg.Mode == "" {
		cfg.Mode = "all"
	}
	if cfg.Match == "" {
		cfg.Match = "first"
	}
	if cfg.DefaultLabel == "" {
		cfg.DefaultLabel = "otherwise"
	}
	if (cfg.Match != "first" && cfg.Match != "all") || !validLabel(cfg.DefaultLabel) {
		return errors.New("invalid match policy or default_label")
	}
	if cfg.Routes == nil {
		return validateConditions(cfg.Mode, cfg.Conditions)
	}
	if cfg.Conditions != nil || len(cfg.Routes) == 0 || len(cfg.Routes) > 32 {
		return errors.New("configure either conditions or one to 32 routes")
	}
	labels := make(map[string]bool, len(cfg.Routes))
	count := 0
	for i := range cfg.Routes {
		route := &cfg.Routes[i]
		if !validLabel(route.Label) || labels[route.Label] {
			return fmt.Errorf("routes[%d] label must be valid and unique", i)
		}
		labels[route.Label] = true
		if route.Mode == "" {
			route.Mode = cfg.Mode
		}
		if err := validateConditions(route.Mode, route.Conditions); err != nil {
			return fmt.Errorf("routes[%d]: %w", i, err)
		}
		count += len(route.Conditions)
	}
	if count > 256 {
		return errors.New("routes support at most 256 conditions in total")
	}
	return nil
}

func validLabel(label string) bool {
	return label != "" && len(label) <= 64 && strings.TrimSpace(label) == label && !strings.ContainsAny(label, "\r\n\x00")
}

func validateConditions(mode string, conditions []condition) error {
	if (mode != "all" && mode != "any") || len(conditions) == 0 || len(conditions) > 64 {
		return errors.New("invalid condition group")
	}
	for _, c := range conditions {
		if c.Path == "" || len(c.Path) > 256 || strings.HasPrefix(c.Path, ".") || strings.HasSuffix(c.Path, ".") || strings.Contains(c.Path, "..") {
			return errors.New("invalid payload path")
		}
		switch c.Op {
		case "eq", "ne", "exists", "missing":
		case "contains":
			if _, ok := c.Value.(string); !ok {
				return errors.New("contains requires string value")
			}
		case "gt", "gte", "lt", "lte":
			if _, ok := numericValue(c.Value); !ok {
				return errors.New("ordered comparison requires a finite numeric value")
			}
		default:
			return errors.New("unsupported condition operator")
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
	var current any = payload
	for _, key := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[key]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// Use bounded exact decimal comparison for JSON numbers and Go integer types.
// Strings stay strings. Values already rounded by an upstream float transport
// cannot be converted back to their original representation.
func numericValue(value any) (*big.Rat, bool) {
	var literal string
	if n, ok := value.(json.Number); ok {
		literal = string(n)
	} else {
		number := reflect.ValueOf(value)
		switch number.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			literal = strconv.FormatInt(number.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			literal = strconv.FormatUint(number.Uint(), 10)
		case reflect.Float32, reflect.Float64:
			f := number.Float()
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, false
			}
			literal = strconv.FormatFloat(f, 'g', -1, number.Type().Bits())
		default:
			return nil, false
		}
	}
	if len(literal) > 80 {
		return nil, false
	}
	if at := strings.IndexAny(literal, "eE"); at >= 0 {
		exponent, err := strconv.Atoi(literal[at+1:])
		if err != nil || exponent < -324 || exponent > 308 {
			return nil, false
		}
	}
	if _, err := strconv.ParseFloat(literal, 64); err != nil {
		return nil, false
	}
	return new(big.Rat).SetString(literal)
}

func equalValue(a, b any) bool {
	if aNum, ok := numericValue(a); ok {
		bNum, ok := numericValue(b)
		return ok && aNum.Cmp(bNum) == 0
	}
	switch a := a.(type) {
	case map[string]any:
		b, ok := b.(map[string]any)
		if !ok || (a == nil) != (b == nil) || len(a) != len(b) {
			return false
		}
		for key, value := range a {
			other, ok := b[key]
			if !ok || !equalValue(value, other) {
				return false
			}
		}
		return true
	case []any:
		b, ok := b.([]any)
		if !ok || (a == nil) != (b == nil) || len(a) != len(b) {
			return false
		}
		for i, value := range a {
			if !equalValue(value, b[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

func evaluate(payload map[string]any, c condition) bool {
	value, exists := lookup(payload, c.Path)
	switch c.Op {
	case "exists":
		return exists
	case "missing":
		return !exists
	case "eq":
		return exists && equalValue(value, c.Value)
	case "ne":
		return exists && !equalValue(value, c.Value)
	case "contains":
		text, ok := value.(string)
		needle, _ := c.Value.(string)
		return ok && strings.Contains(text, needle)
	}
	a, ok := numericValue(value)
	b, bOk := numericValue(c.Value)
	if !ok || !bOk {
		return false
	}
	order := a.Cmp(b)
	switch c.Op {
	case "gt":
		return order > 0
	case "gte":
		return order >= 0
	case "lt":
		return order < 0
	case "lte":
		return order <= 0
	default:
		return false
	}
}

func matches(payload map[string]any, mode string, conditions []condition) bool {
	all := mode != "any"
	for _, c := range conditions {
		if evaluate(payload, c) != all {
			return !all
		}
	}
	return all
}

func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	fallback := p.cfg.DefaultLabel
	if fallback == "" {
		fallback = "otherwise"
	}
	if len(p.cfg.Routes) == 0 {
		if matches(e.Payload, p.cfg.Mode, p.cfg.Conditions) {
			fallback = "matched"
		}
		return p.ctx.Emitter().EmitTo(fallback, e.Clone())
	}
	labels := make([]string, 0, len(p.cfg.Routes))
	for _, route := range p.cfg.Routes {
		if matches(e.Payload, route.Mode, route.Conditions) {
			labels = append(labels, route.Label)
			if p.cfg.Match != "all" {
				break
			}
		}
	}
	if len(labels) == 0 {
		labels = append(labels, fallback)
	}
	if len(labels) > 1 && e.Metadata["buffer_record_id"] != "" {
		return sdk.FatalError(errors.New("route fanout cannot aggregate buffered input acknowledgements; put durable buffering after routing"))
	}
	for _, label := range labels {
		if err := p.ctx.Emitter().EmitTo(label, e.Clone()); err != nil {
			return err
		}
	}
	return nil
}

func (p *plugin) OnEvents(events []sdk.Event) error {
	for _, e := range events {
		if err := p.OnEvent(e); err != nil {
			return err
		}
	}
	return nil
}
