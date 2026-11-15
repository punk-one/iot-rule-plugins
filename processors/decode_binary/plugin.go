package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	sdk "github.com/punk-one/iot-rule-plugin-sdk"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/unicode"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

type field struct {
	Name         string `json:"name"`
	Offset       int    `json:"offset"`
	Type         string `json:"type"`
	ByteOrder    string `json:"byte_order"`
	Length       int    `json:"length,omitempty"`
	Bit          int    `json:"bit,omitempty"`
	TextEncoding string `json:"text_encoding,omitempty"`
	BitOffset    int    `json:"bit_offset,omitempty"`
	BitLength    int    `json:"bit_length,omitempty"`
}
type config struct {
	InputField  string  `json:"input_field"`
	OutputField string  `json:"output_field"`
	Encoding    string  `json:"encoding"`
	MaxBytes    int     `json:"max_bytes"`
	Fields      []field `json:"fields"`
}
type plugin struct {
	sdk.BasePlugin
	cfg config
	ctx sdk.RuntimeContext
}

func (p *plugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{ID: "decode-binary", Name: "Decode Binary", Version: "1.0.0", Type: sdk.PluginTypeProcessor, Description: "Bounded base64/hex binary frames decoded by explicit offset, width and byte order", Capabilities: sdk.PluginCapabilities{InputPorts: 1, OutputPorts: 1, ConfigSchema: configSchema}}
}
func width(f field) (int, error) {
	switch f.Type {
	case "Bool", "Uint8", "Int8":
		return 1, nil
	case "Uint16", "Int16":
		return 2, nil
	case "Uint32", "Int32", "Float32":
		return 4, nil
	case "Uint64", "Int64", "Float64":
		return 8, nil
	case "String", "Hex", "BCD", "SignedBCD", "Bits":
		if f.Length < 1 {
			return 0, errors.New("String/Hex require length")
		}
		return f.Length, nil
	}
	return 0, errors.New("unsupported binary field type")
}
func (p *plugin) Init(raw sdk.PluginConfig) error {
	p.cfg = config{InputField: "raw_base64", OutputField: "decoded", Encoding: "base64", MaxBytes: 1048576}
	if err := json.Unmarshal(raw.Raw, &p.cfg); err != nil {
		return sdk.FatalError(err)
	}
	if p.cfg.InputField == "" || p.cfg.OutputField == "" || (p.cfg.Encoding != "base64" && p.cfg.Encoding != "hex") || p.cfg.MaxBytes < 1 || p.cfg.MaxBytes > 8388608 || len(p.cfg.Fields) == 0 || len(p.cfg.Fields) > 256 {
		return sdk.FatalError(errors.New("invalid binary decoder settings"))
	}
	seen := map[string]bool{}
	for _, f := range p.cfg.Fields {
		w, err := width(f)
		if err != nil || f.Name == "" || seen[f.Name] || f.Offset < 0 || f.Offset > p.cfg.MaxBytes || w > p.cfg.MaxBytes-f.Offset || f.Bit < 0 || f.Bit > 7 || (f.ByteOrder != "" && f.ByteOrder != "big" && f.ByteOrder != "little") {
			return sdk.FatalError(errors.New("invalid binary field layout"))
		}
		if (f.Type == "BCD" || f.Type == "SignedBCD" || f.Type == "Bits") && f.Length > 8 {
			return sdk.FatalError(errors.New("BCD/bitfield width must be 1..8 bytes"))
		}
		if f.Type == "Bits" && (f.BitOffset < 0 || f.BitLength < 1 || f.BitLength > 64 || f.BitOffset+f.BitLength > f.Length*8) {
			return sdk.FatalError(errors.New("invalid bitfield span"))
		}
		switch f.TextEncoding {
		case "", "utf8", "ascii", "latin1", "utf16le", "utf16be", "gb18030":
		default:
			return sdk.FatalError(errors.New("unsupported text_encoding"))
		}
		seen[f.Name] = true
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
func decodeValue(data []byte, f field) (any, error) {
	var order binary.ByteOrder = binary.BigEndian
	if f.ByteOrder == "little" {
		order = binary.LittleEndian
	}
	switch f.Type {
	case "BCD", "SignedBCD":
		if f.ByteOrder == "little" {
			copy := append([]byte(nil), data...)
			for i, j := 0, len(copy)-1; i < j; i, j = i+1, j-1 {
				copy[i], copy[j] = copy[j], copy[i]
			}
			data = copy
		}
		negative := false
		var digits strings.Builder
		for i, b := range data {
			for j, nibble := range []byte{b >> 4, b & 15} {
				last := i == len(data)-1 && j == 1
				if f.Type == "SignedBCD" && last {
					if nibble != 0xc && nibble != 0xd && nibble != 0xf {
						return nil, errors.New("invalid BCD sign nibble")
					}
					negative = nibble == 0xd
					continue
				}
				if nibble > 9 {
					return nil, errors.New("invalid BCD digit")
				}
				digits.WriteByte('0' + nibble)
			}
		}
		out := digits.String()
		if negative {
			out = "-" + out
		}
		return out, nil
	case "Bits":
		var raw uint64
		if f.ByteOrder == "little" {
			for i := len(data) - 1; i >= 0; i-- {
				raw = raw<<8 | uint64(data[i])
			}
		} else {
			for _, b := range data {
				raw = raw<<8 | uint64(b)
			}
		}
		raw >>= uint(f.BitOffset)
		if f.BitLength < 64 {
			raw &= (uint64(1) << uint(f.BitLength)) - 1
		}
		if raw > 9007199254740991 {
			return strconv.FormatUint(raw, 10), nil
		}
		return raw, nil

	case "Bool":
		return data[0]&(1<<uint(f.Bit)) != 0, nil
	case "Uint8":
		return data[0], nil
	case "Int8":
		return int8(data[0]), nil
	case "Uint16":
		return order.Uint16(data), nil
	case "Int16":
		return int16(order.Uint16(data)), nil
	case "Uint32":
		return order.Uint32(data), nil
	case "Int32":
		return int32(order.Uint32(data)), nil
	case "Uint64":
		v := order.Uint64(data)
		if v > 9007199254740991 {
			return strconv.FormatUint(v, 10), nil
		}
		return v, nil
	case "Int64":
		v := int64(order.Uint64(data))
		if v > 9007199254740991 || v < -9007199254740991 {
			return strconv.FormatInt(v, 10), nil
		}
		return v, nil
	case "Float32":
		v := math.Float32frombits(order.Uint32(data))
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("non-finite float field")
		}
		return v, nil
	case "Float64":
		v := math.Float64frombits(order.Uint64(data))
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, errors.New("non-finite float field")
		}
		return v, nil
	case "String":
		var decoded []byte
		var err error
		switch f.TextEncoding {
		case "ascii":
			for _, b := range data {
				if b > 127 {
					return nil, errors.New("non-ASCII byte")
				}
			}
			decoded = data
		case "latin1":
			decoded, err = charmap.ISO8859_1.NewDecoder().Bytes(data)
		case "utf16le", "utf16be":
			if len(data)%2 != 0 {
				return nil, errors.New("odd UTF-16 byte count")
			}
			endian := unicode.LittleEndian
			if f.TextEncoding == "utf16be" {
				endian = unicode.BigEndian
			}
			decoded, err = unicode.UTF16(endian, unicode.IgnoreBOM).NewDecoder().Bytes(data)
		case "gb18030":
			decoded, err = simplifiedchinese.GB18030.NewDecoder().Bytes(data)
		default:
			decoded = data
		}
		if err != nil || !utf8.Valid(decoded) || f.TextEncoding != "" && f.TextEncoding != "utf8" && strings.ContainsRune(string(decoded), '\uFFFD') {
			return nil, errors.New("invalid encoded string")
		}
		return strings.TrimRight(string(decoded), "\x00"), nil

	case "Hex":
		return hex.EncodeToString(data), nil
	}
	return nil, errors.New("unsupported field type")
}
func (p *plugin) decode(encoded string) (map[string]any, error) {
	if len(encoded) > p.cfg.MaxBytes*2+4 {
		return nil, errors.New("encoded frame too large")
	}
	var raw []byte
	var err error
	if p.cfg.Encoding == "hex" {
		raw, err = hex.DecodeString(encoded)
	} else {
		raw, err = base64.StdEncoding.DecodeString(encoded)
	}
	if err != nil || len(raw) > p.cfg.MaxBytes {
		return nil, errors.New("invalid or oversized frame")
	}
	out := map[string]any{}
	for _, f := range p.cfg.Fields {
		w, _ := width(f)
		if f.Offset > len(raw) || w > len(raw)-f.Offset {
			return nil, errors.New("frame shorter than configured field")
		}
		value, err := decodeValue(raw[f.Offset:f.Offset+w], f)
		if err != nil {
			return nil, err
		}
		out[f.Name] = value
	}
	return out, nil
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
func (p *plugin) OnEvent(e sdk.Event) error {
	if p.ctx == nil {
		return sdk.FatalError(errors.New("not started"))
	}
	raw, ok := lookup(e.Payload, p.cfg.InputField)
	encoded, isString := raw.(string)
	if !ok || !isString {
		return sdk.SkippableError(errors.New("encoded frame string required"))
	}
	value, err := p.decode(encoded)
	if err != nil {
		return sdk.SkippableError(err)
	}
	out := e.Clone()
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
