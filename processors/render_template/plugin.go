package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"html"
	"regexp"
	"strings"
)

var placeholder = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_.]*)\s*\}\}`)

type config struct {
	Template    string `json:"template"`
	OutputField string `json:"output_field"`
	Escape      string `json:"escape"`
	MaxBytes    int    `json:"max_bytes"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "render-template", Name: "Render Template", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded path interpolation with explicit HTML, JSON or text escaping", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{OutputField: "rendered", Escape: "html", MaxBytes: 1048576}
	if json.Unmarshal(raw.Raw, &p.cfg) != nil || p.cfg.OutputField == "" || len(p.cfg.Template) > 65536 || p.cfg.MaxBytes < 1 || p.cfg.MaxBytes > 1048576 || (p.cfg.Escape != "html" && p.cfg.Escape != "json" && p.cfg.Escape != "text") || len(placeholder.FindAllStringIndex(p.cfg.Template, -1)) > 256 {
		return sdk.FatalError(errors.New("invalid template settings"))
	}
	remaining := placeholder.ReplaceAllString(p.cfg.Template, "")
	if strings.Contains(remaining, "{{") || strings.Contains(remaining, "}}") || p.cfg.Template == "" {
		return sdk.FatalError(errors.New("only {{payload.path}} and {{event.id}} placeholders are supported"))
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
func resolve(e sdk.Event, path string) (any, bool) {
	if path == "event.id" {
		return e.ID, true
	}
	if !strings.HasPrefix(path, "payload.") {
		return nil, false
	}
	var v any = e.Payload
	for _, part := range strings.Split(strings.TrimPrefix(path, "payload."), ".") {
		object, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return v, true
}
func (p *plugin) render(e sdk.Event) (string, error) {
	var out bytes.Buffer
	last := 0
	for _, match := range placeholder.FindAllStringSubmatchIndex(p.cfg.Template, -1) {
		out.WriteString(p.cfg.Template[last:match[0]])
		path := p.cfg.Template[match[2]:match[3]]
		value, ok := resolve(e, path)
		if !ok {
			return "", errors.New("template path missing")
		}
		var text string
		if p.cfg.Escape == "json" {
			raw, err := json.Marshal(value)
			if err != nil {
				return "", err
			}
			text = string(raw)
		} else {
			switch value.(type) {
			case string, bool, float64, int, int64, uint64, json.Number:
			default:
				return "", errors.New("text placeholders require scalar values")
			}
			raw, err := json.Marshal(value)
			if err != nil || len(raw) > p.cfg.MaxBytes {
				return "", errors.New("invalid or oversized template value")
			}
			text = fmt.Sprint(value)
			if p.cfg.Escape == "html" {
				text = html.EscapeString(text)
			}
		}
		if out.Len()+len(text) > p.cfg.MaxBytes {
			return "", errors.New("render limit exceeded")
		}
		out.WriteString(text)
		last = match[1]
	}
	if out.Len()+len(p.cfg.Template[last:]) > p.cfg.MaxBytes {
		return "", errors.New("render limit exceeded")
	}
	out.WriteString(p.cfg.Template[last:])
	return out.String(), nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	text, err := p.render(e)
	if err != nil {
		return sdk.SkippableError(err)
	}
	out := e.Clone()
	if out.Payload == nil {
		out.Payload = map[string]any{}
	}
	out.Payload[p.cfg.OutputField] = text
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
