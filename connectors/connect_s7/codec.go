package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf16"

	"github.com/punk-one/gos7"
)

func s7ResolveReadSize(info *s7DBInfo, valueType string, maxLength int) (int, int) {
	vt := strings.ToLower(strings.TrimSpace(valueType))
	switch vt {
	case "string":
		max := s7NormalizeStringMaxLength(maxLength)
		return s7WLByte, max + 2
	case "wstring":
		max := s7NormalizeWStringMaxLength(maxLength)
		return s7WLByte, max*2 + 4
	default:
		wl := info.WordLength
		if wl == s7WLBit {
			wl = s7WLByte
		}
		return wl, s7BufSize(wl, info.Amount)
	}
}

func s7ExtractValue(h gos7.Helper, buf []byte, info *s7DBInfo, valueType string) (interface{}, error) {
	vt := strings.ToLower(strings.TrimSpace(valueType))
	switch {
	case info.WordLength == s7WLBit || vt == "bool":
		if len(buf) == 0 {
			return nil, fmt.Errorf("empty buffer for bit read")
		}
		bit := info.Bit
		if bit < 0 {
			bit = 0
		}
		return (buf[0]>>uint(bit))&1 == 1, nil
	case vt == "int8":
		var v int8
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "uint8" || vt == "byte":
		var v uint8
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "int16":
		var v int16
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "uint16" || vt == "word":
		var v uint16
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "int32":
		var v int32
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "uint32" || vt == "dword":
		var v uint32
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "float32":
		var v float32
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "float64":
		var v float64
		h.GetValueAt(buf, 0, &v)
		return v, nil
	case vt == "string":
		if len(buf) < 2 {
			return "", nil
		}
		return h.GetStringAt(buf, 0), nil
	case vt == "wstring":
		if len(buf) < 4 {
			return "", nil
		}
		return h.GetWStringAt(buf, 0), nil
	default:
		return fmt.Sprintf("%x", buf), nil
	}
}

func s7ApplyPointTransforms(value interface{}, pt s7Point) (interface{}, error) {
	scaledValue, err := s7ApplyScale(value, pt)
	if err != nil {
		return scaledValue, err
	}
	roundedValue, err := s7ApplyPrecision(scaledValue, pt)
	if err != nil {
		return roundedValue, err
	}
	return roundedValue, nil
}

func s7ApplyScale(value interface{}, pt s7Point) (interface{}, error) {
	if pt.Scale == nil || *pt.Scale == 1 {
		return value, nil
	}
	scale := *pt.Scale
	switch v := value.(type) {
	case bool, string:
		return value, nil
	case float32:
		return float32(float64(v) * scale), nil
	case float64:
		return v * scale, nil
	case int:
		return float64(v) * scale, nil
	case int8:
		return float64(v) * scale, nil
	case int16:
		return float64(v) * scale, nil
	case int32:
		return float64(v) * scale, nil
	case int64:
		return float64(v) * scale, nil
	case uint:
		return float64(v) * scale, nil
	case uint8:
		return float64(v) * scale, nil
	case uint16:
		return float64(v) * scale, nil
	case uint32:
		return float64(v) * scale, nil
	case uint64:
		return float64(v) * scale, nil
	default:
		return value, fmt.Errorf("scale is not supported for value type %T", value)
	}
}

func s7ApplyPrecision(value interface{}, pt s7Point) (interface{}, error) {
	if pt.Precision <= 0 {
		return value, nil
	}
	switch v := value.(type) {
	case float32:
		return float32(s7RoundToPrecision(float64(v), pt.Precision)), nil
	case float64:
		return s7RoundToPrecision(v, pt.Precision), nil
	default:
		return value, nil
	}
}

func s7RoundToPrecision(value float64, precision int) float64 {
	if precision <= 0 {
		return value
	}
	multiplier := math.Pow(10, float64(precision))
	return math.Round(value*multiplier) / multiplier
}

func s7EncodeValue(h gos7.Helper, info *s7DBInfo, valueType string, maxLength int, val interface{}) ([]byte, error) {
	vt := strings.ToLower(strings.TrimSpace(valueType))

	var buf []byte
	switch vt {
	case "string":
		max := s7NormalizeStringMaxLength(maxLength)
		buf = make([]byte, max+2)
	case "wstring":
		max := s7NormalizeWStringMaxLength(maxLength)
		buf = make([]byte, max*2+4)
	default:
		buf = make([]byte, s7BufSize(info.WordLength, info.Amount))
	}

	toFloat64 := func(v interface{}) (float64, bool) {
		switch n := v.(type) {
		case float64:
			return n, true
		case float32:
			return float64(n), true
		case int:
			return float64(n), true
		case int8:
			return float64(n), true
		case int16:
			return float64(n), true
		case int32:
			return float64(n), true
		case int64:
			return float64(n), true
		case uint:
			return float64(n), true
		case uint8:
			return float64(n), true
		case uint16:
			return float64(n), true
		case uint32:
			return float64(n), true
		case uint64:
			return float64(n), true
		case json.Number:
			f, err := n.Float64()
			return f, err == nil
		}
		return 0, false
	}

	switch vt {
	case "bool":
		boolVal := false
		switch v := val.(type) {
		case bool:
			boolVal = v
		case string:
			boolVal = v == "true" || v == "1"
		default:
			if f, ok := toFloat64(val); ok {
				boolVal = f != 0
			}
		}
		if boolVal {
			buf[0] = 1
		}
	case "int8":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, int8(f))
		}
	case "uint8":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, uint8(f))
		}
	case "int16":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, int16(f))
		}
	case "uint16":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, uint16(f))
		}
	case "int32":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, int32(f))
		}
	case "uint32":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, uint32(f))
		}
	case "float32":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, float32(f))
		}
	case "float64":
		if f, ok := toFloat64(val); ok {
			h.SetValueAt(buf, 0, f)
		}
	case "string":
		h.SetStringAt(buf, 0, s7NormalizeStringMaxLength(maxLength), fmt.Sprintf("%v", val))
	case "wstring":
		s7SetWStringAt(h, buf, 0, s7NormalizeWStringMaxLength(maxLength), fmt.Sprintf("%v", val))
	default:
		return nil, fmt.Errorf("unsupported value_type: %s", valueType)
	}
	return buf, nil
}

func s7BuildStringWriteItem(h gos7.Helper, info *s7DBInfo, pt s7WritePoint) (gos7.S7DataItem, error) {
	buf, err := s7EncodeValue(h, info, pt.ValueType, pt.MaxLength, pt.Value)
	if err != nil {
		return gos7.S7DataItem{}, err
	}
	return gos7.S7DataItem{
		Area:     info.Area,
		WordLen:  s7WLByte,
		DBNumber: info.DBNumber,
		Start:    info.Start,
		Amount:   len(buf),
		Data:     buf,
	}, nil
}

func s7IsStringType(valueType string) bool {
	switch strings.ToLower(strings.TrimSpace(valueType)) {
	case "string", "wstring":
		return true
	default:
		return false
	}
}

func s7NormalizeStringMaxLength(maxLength int) int {
	if maxLength <= 0 {
		return 254
	}
	return maxLength
}

func s7NormalizeWStringMaxLength(maxLength int) int {
	if maxLength <= 0 {
		return 127
	}
	return maxLength
}

func s7SetWStringAt(h gos7.Helper, buffer []byte, pos int, maxLen int, value string) {
	runes := []rune(value)
	if maxLen < 0 {
		maxLen = 0
	}
	if len(runes) > maxLen {
		runes = runes[:maxLen]
	}
	encoded := utf16.Encode(runes)
	h.SetValueAt(buffer, pos, int16(maxLen))
	h.SetValueAt(buffer, pos+2, int16(len(encoded)))
	for i, c := range encoded {
		h.SetValueAt(buffer, pos+4+i*2, c)
	}
}
