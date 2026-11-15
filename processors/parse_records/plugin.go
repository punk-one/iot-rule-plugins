package main

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"io"
	"math"
	"strings"
	"unicode/utf8"
)

type config struct {
	Format      string `json:"format"`
	InputField  string `json:"input_field"`
	OutputField string `json:"output_field"`
	Encoding    string `json:"encoding"`
	Separator   string `json:"separator"`
	Header      bool   `json:"header"`
	MaxBytes    int    `json:"max_bytes"`
	MaxRecords  int    `json:"max_records"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "parse-records", Name: "Parse Records", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded JSON/CSV/XML parsing with explicit encoding and independent event ownership", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{InputField: "raw", OutputField: "parsed", Encoding: "utf8", Separator: ",", Header: true, MaxBytes: 1048576, MaxRecords: 4096}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.InputField == "" || p.cfg.OutputField == "" || p.cfg.MaxBytes < 1 || p.cfg.MaxBytes > 8388608 || p.cfg.MaxRecords < 1 || p.cfg.MaxRecords > 4096 || (p.cfg.Encoding != "utf8" && p.cfg.Encoding != "base64") || utf8.RuneCountInString(p.cfg.Separator) != 1 {
		return sdk.FatalError(errors.New("invalid parser settings"))
	}
	switch p.cfg.Format {
	case "json", "csv", "xml":
	default:
		return sdk.FatalError(errors.New("format must be json, csv or xml"))
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
	var value any = payload
	for _, part := range strings.Split(path, ".") {
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
func (p *plugin) parse(input string) (any, error) {
	if len(input) > p.cfg.MaxBytes*2 {
		return nil, errors.New("encoded input too large")
	}
	raw := []byte(input)
	if p.cfg.Encoding == "base64" {
		var err error
		raw, err = base64.StdEncoding.DecodeString(input)
		if err != nil {
			return nil, errors.New("invalid base64 input")
		}
	}
	if len(raw) > p.cfg.MaxBytes || !utf8.Valid(raw) {
		return nil, errors.New("input exceeds byte limit or is not UTF-8")
	}
	switch p.cfg.Format {
	case "json":
		value, err := decodeSafeJSON(raw)
		if err != nil {
			return nil, err
		}
		if rows, ok := value.([]any); ok && len(rows) > p.cfg.MaxRecords {
			return nil, errors.New("JSON record limit exceeded")
		}
		return value, nil
	case "csv":
		return p.parseCSV(raw)
	default:
		return p.parseXML(raw)
	}
}
func (p *plugin) parseCSV(raw []byte) (any, error) {
	reader := csv.NewReader(bytes.NewReader(raw))
	reader.Comma = []rune(p.cfg.Separator)[0]
	first, err := reader.Read()
	if err == io.EOF {
		return []any{}, nil
	}
	if err != nil || len(first) > 256 {
		return nil, errors.New("invalid CSV header/row")
	}
	rows := make([]any, 0)
	if !p.cfg.Header {
		rows = append(rows, first)
	}
	if p.cfg.Header {
		seen := map[string]bool{}
		for _, name := range first {
			if name == "" || seen[name] {
				return nil, errors.New("CSV headers must be nonempty and unique")
			}
			seen[name] = true
		}
	}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.New("invalid CSV record")
		}
		if len(rows) >= p.cfg.MaxRecords {
			return nil, errors.New("CSV record limit exceeded")
		}
		if p.cfg.Header {
			row := map[string]any{}
			for i, key := range first {
				row[key] = record[i]
			}
			rows = append(rows, row)
		} else {
			rows = append(rows, record)
		}
	}
	return rows, nil
}
func (p *plugin) parseXML(raw []byte) (any, error) {
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	type element struct {
		node    map[string]any
		content []any
	}
	stack := []*element{}
	var root map[string]any
	count := 0
	tokens := 0
	for {
		token, err := decoder.Token()
		tokens++
		if tokens > p.cfg.MaxRecords*4 {
			return nil, errors.New("XML token limit exceeded")
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.New("invalid XML")
		}
		switch t := token.(type) {
		case xml.Directive:
			return nil, errors.New("XML directives/DTD are not allowed")
		case xml.StartElement:
			count++
			if count > p.cfg.MaxRecords || len(stack) >= 64 || len(t.Attr) > 256 {
				return nil, errors.New("XML structure limit exceeded")
			}
			attrs := map[string]any{}
			for _, attr := range t.Attr {
				key := attr.Name.Local
				if attr.Name.Space != "" {
					key = "{" + attr.Name.Space + "}" + key
				}
				if _, exists := attrs[key]; exists {
					return nil, errors.New("duplicate XML attribute")
				}
				attrs[key] = attr.Value
			}
			node := map[string]any{"name": t.Name.Local, "namespace": t.Name.Space, "attributes": attrs}
			if len(stack) == 0 {
				if root != nil {
					return nil, errors.New("multiple XML roots")
				}
				root = node
			}
			stack = append(stack, &element{node: node})
		case xml.CharData:
			if len(stack) == 0 {
				if strings.TrimSpace(string(t)) != "" {
					return nil, errors.New("text outside XML root")
				}
			} else {
				stack[len(stack)-1].content = append(stack[len(stack)-1].content, string(t))
			}
		case xml.EndElement:
			if len(stack) == 0 {
				return nil, errors.New("unbalanced XML")
			}
			done := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			done.node["content"] = done.content
			if len(stack) > 0 {
				parent := stack[len(stack)-1]
				parent.content = append(parent.content, done.node)
			}
		}
	}
	if root == nil || len(stack) != 0 {
		return nil, errors.New("incomplete XML document")
	}
	return root, nil
}
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	raw, ok := lookup(e.Payload, p.cfg.InputField)
	input, isString := raw.(string)
	if !ok || !isString {
		return sdk.SkippableError(errors.New("parser input must be a string"))
	}
	value, err := p.parse(input)
	if err != nil {
		return sdk.SkippableError(err)
	}
	out := e.Clone()
	if out.Payload == nil {
		out.Payload = map[string]any{}
	}
	out.Payload[p.cfg.OutputField] = value
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
