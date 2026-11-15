package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
)

// Format evidence and supported variants are documented in README.md.
// Unknown variants fail closed; raw data is retained only alongside valid values.
func decodeComplex(q query, raw []byte) (any, error) {
	out := map[string]any{"raw_base64": base64.StdEncoding.EncodeToString(raw), "selector": q.selector}
	invalid := errors.New("unsupported or invalid complex LSV2 payload")
	switch q.format {
	case "overrides":
		if len(raw) != 12 {
			return nil, invalid
		}
		for i, k := range []string{"feed_percent", "spindle_percent", "rapid_percent"} {
			out[k] = float64(binary.BigEndian.Uint32(raw[i*4:i*4+4])) / 100
		}
	case "tool":
		if len(raw) != 8 && len(raw) != 24 {
			return nil, invalid
		}
		out["number"] = binary.BigEndian.Uint32(raw[:4])
		out["index"] = binary.BigEndian.Uint16(raw[4:6])
		code := binary.BigEndian.Uint16(raw[6:8])
		out["axis_code"] = code
		out["axis"] = "unknown"
		if code < 3 {
			out["axis"] = []string{"X", "Y", "Z"}[code]
		}
		if len(raw) == 24 {
			for i, k := range []string{"length", "radius"} {
				v := math.Float64frombits(binary.LittleEndian.Uint64(raw[8+i*8 : 16+i*8]))
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return nil, invalid
				}
				out[k] = v
			}
		}
	case "axes":
		if len(raw) < 2 || raw[1] < 1 || raw[1] > 32 {
			return nil, invalid
		}
		count := int(raw[1])
		parts, err := complexStrings(raw[2:], count*2)
		if err != nil {
			return nil, err
		}
		positions := map[string]any{}
		for i := 0; i < count; i++ {
			name := parts[count+i]
			if name == "" || len(name) > 64 {
				return nil, invalid
			}
			if _, exists := positions[name]; exists {
				return nil, invalid
			}
			value, err := strconv.ParseFloat(strings.TrimSpace(parts[i]), 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, invalid
			}
			positions[name] = value
		}
		out["header_byte"] = raw[0]
		out["positions"] = positions
	case "stack":
		if len(raw) < 6 {
			return nil, invalid
		}
		parts, err := complexStrings(raw[4:], 2)
		if err != nil {
			return nil, err
		}
		out["line_number"] = binary.BigEndian.Uint32(raw[:4])
		out["main_program"] = parts[0]
		out["current_program"] = parts[1]
	default:
		return nil, invalid
	}
	return out, nil
}

func complexStrings(raw []byte, count int) ([]string, error) {
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		n := bytes.IndexByte(raw, 0)
		if n < 0 || n > 4096 {
			return nil, errors.New("invalid terminated LSV2 string")
		}
		text, _ := textValue(raw[:n])
		out = append(out, text)
		raw = raw[n+1:]
	}
	if len(raw) != 0 {
		return nil, errors.New("trailing complex LSV2 data")
	}
	return out, nil
}
