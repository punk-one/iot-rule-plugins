package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"
)

// This is a bounded scalar BACnet/IP client, not a routed or segmented stack.
type point struct {
	Name       string  `json:"name"`
	ObjectType uint32  `json:"object_type"`
	Instance   uint32  `json:"instance"`
	Property   uint32  `json:"property"`
	ArrayIndex *uint32 `json:"array_index,omitempty"`
	ValueType  string  `json:"valueType"`
	Priority   int     `json:"priority,omitempty"`
	Value      any     `json:"value,omitempty"`
}

func validatePoint(p point) error {
	if p.Name == "" || len(p.Name) > 128 || p.ObjectType > 1023 || p.Instance >= 4194303 || p.Property > 4194303 || p.Priority < 0 || p.Priority > 16 {
		return errors.New("invalid BACnet point address/priority")
	}
	switch p.ValueType {
	case "Null", "Bool", "Uint32", "Int32", "Float32", "Float64", "Enum", "String", "Bytes", "Bits", "Date", "Time", "ObjectID":
		return nil
	}
	return errors.New("unsupported BACnet scalar type")
}
func objectID(p point) uint32 { return p.ObjectType<<22 | p.Instance }
func uintBytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	i := 0
	for i < 3 && b[i] == 0 {
		i++
	}
	return b[i:]
}
func contextUint(tag byte, n uint32) []byte {
	b := uintBytes(n)
	return append([]byte{tag<<4 | 8 | byte(len(b))}, b...)
}
func address(p point) []byte {
	b := []byte{0x0c, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], objectID(p))
	b = append(b, contextUint(1, p.Property)...)
	if p.ArrayIndex != nil {
		b = append(b, contextUint(2, *p.ArrayIndex)...)
	}
	return b
}
func app(tag byte, b []byte) []byte {
	n := len(b)
	out := []byte{tag << 4}
	if n < 5 {
		out[0] |= byte(n)
	} else if n < 254 {
		out[0] |= 5
		out = append(out, byte(n))
	} else {
		out[0] |= 5
		out = append(out, 254, byte(n>>8), byte(n))
	}
	return append(out, b...)
}
func number(v any) (float64, error) {
	var n float64
	switch x := v.(type) {
	case json.Number:
		var err error
		n, err = x.Float64()
		if err != nil {
			return 0, err
		}
	case float64:
		n = x
	case int:
		n = float64(x)
	case int64:
		n = float64(x)
	case uint32:
		n = float64(x)
	case float32:
		n = float64(x)
	default:
		return 0, errors.New("numeric BACnet value required")
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, errors.New("nonfinite BACnet value")
	}
	return n, nil
}
func encodeValue(p point) ([]byte, error) {
	switch p.ValueType {
	case "Null":
		if p.Value != nil {
			return nil, errors.New("Null requires null")
		}
		return []byte{0}, nil
	case "Bool":
		v, ok := p.Value.(bool)
		if !ok {
			return nil, errors.New("Bool requires boolean")
		}
		if v {
			return []byte{0x11}, nil
		}
		return []byte{0x10}, nil
	case "Uint32", "Enum", "Int32", "Float32", "Float64":
		n, err := number(p.Value)
		if err != nil {
			return nil, err
		}
		switch p.ValueType {
		case "Uint32", "Enum":
			if n < 0 || n > math.MaxUint32 || n != math.Trunc(n) {
				return nil, errors.New("unsigned value outside 32 bits")
			}
			tag := byte(2)
			if p.ValueType == "Enum" {
				tag = 9
			}
			return app(tag, uintBytes(uint32(n))), nil
		case "Int32":
			if n < math.MinInt32 || n > math.MaxInt32 || n != math.Trunc(n) {
				return nil, errors.New("signed value outside 32 bits")
			}
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(int32(n)))
			for len(b) > 1 && (b[0] == 0 && b[1]&128 == 0 || b[0] == 255 && b[1]&128 != 0) {
				b = b[1:]
			}
			return app(3, b), nil
		case "Float32":
			if math.Abs(n) > math.MaxFloat32 {
				return nil, errors.New("Float32 overflow")
			}
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, math.Float32bits(float32(n)))
			return app(4, b), nil
		default:
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, math.Float64bits(n))
			return app(5, b), nil
		}
	case "String":
		v, ok := p.Value.(string)
		if !ok || !utf8.ValidString(v) || len(v) > 512 {
			return nil, errors.New("String requires <=512 UTF-8 bytes")
		}
		return app(7, append([]byte{0}, []byte(v)...)), nil
	case "Bytes":
		v, ok := p.Value.(string)
		if !ok {
			return nil, errors.New("Bytes requires base64 string")
		}
		b, err := base64.StdEncoding.Strict().DecodeString(v)
		if err != nil || len(b) > 512 {
			return nil, errors.New("invalid bounded base64")
		}
		return app(6, b), nil
	case "Bits":
		v, ok := p.Value.(string)
		if !ok || len(v) > 512 {
			return nil, errors.New("Bits requires <=512 binary characters")
		}
		b := make([]byte, 1+(len(v)+7)/8)
		b[0] = byte((8 - len(v)%8) % 8)
		for i, c := range v {
			if c != '0' && c != '1' {
				return nil, errors.New("invalid bit string")
			}
			if c == '1' {
				b[1+i/8] |= 1 << uint(7-i%8)
			}
		}
		return app(8, b), nil
	case "ObjectID":
		v, ok := p.Value.(map[string]any)
		if !ok || len(v) != 2 {
			return nil, errors.New("ObjectID requires object_type and instance")
		}
		t, e := number(v["object_type"])
		i, e2 := number(v["instance"])
		if e != nil || e2 != nil || t < 0 || t > 1023 || i < 0 || i >= 4194303 || t != math.Trunc(t) || i != math.Trunc(i) {
			return nil, errors.New("invalid ObjectID")
		}
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(t)<<22|uint32(i))
		return app(12, b), nil
	default:
		return nil, errors.New("Date/Time are read-only raw components")
	}
}

type tagValue struct {
	tag              byte
	context          bool
	opening, closing bool
	b                []byte
	boolean          bool
}

func takeTag(b []byte) (tagValue, []byte, error) {
	t := tagValue{}
	if len(b) == 0 {
		return t, nil, errors.New("truncated BACnet tag")
	}
	first := b[0]
	b = b[1:]
	t.tag = first >> 4
	t.context = first&8 != 0
	l := int(first & 7)
	if t.tag == 15 {
		return t, nil, errors.New("extended tags not supported in scalar response")
	}
	if t.context && (l == 6 || l == 7) {
		t.opening = l == 6
		t.closing = l == 7
		return t, b, nil
	}
	if !t.context && t.tag == 1 {
		if l > 1 {
			return t, nil, errors.New("invalid boolean")
		}
		t.boolean = l == 1
		return t, b, nil
	}
	if l == 6 || l == 7 {
		return t, nil, errors.New("invalid application tag")
	}
	if l == 5 {
		if len(b) < 1 {
			return t, nil, errors.New("truncated extended length")
		}
		l = int(b[0])
		b = b[1:]
		if l == 254 {
			if len(b) < 2 {
				return t, nil, errors.New("truncated length")
			}
			l = int(binary.BigEndian.Uint16(b))
			b = b[2:]
		} else if l == 255 {
			return t, nil, errors.New("oversized scalar length")
		}
	}
	if l > 1024 || len(b) < l {
		return t, nil, errors.New("truncated/oversized scalar")
	}
	t.b = b[:l]
	return t, b[l:], nil
}
func tagUint(t tagValue) (uint32, error) {
	if len(t.b) < 1 || len(t.b) > 4 || t.opening || t.closing {
		return 0, errors.New("invalid integer tag")
	}
	var n uint32
	for _, b := range t.b {
		n = n<<8 | uint32(b)
	}
	return n, nil
}
func decodeValue(b []byte, kind string) (any, error) {
	t, rest, err := takeTag(b)
	if err != nil || len(rest) != 0 || t.context {
		return nil, errors.New("value must be one complete application scalar")
	}
	expected := map[string]byte{"Null": 0, "Bool": 1, "Uint32": 2, "Int32": 3, "Float32": 4, "Float64": 5, "Bytes": 6, "String": 7, "Bits": 8, "Enum": 9, "Date": 10, "Time": 11, "ObjectID": 12}
	tag, ok := expected[kind]
	if !ok || tag != t.tag {
		return nil, errors.New("BACnet type differs from configured type")
	}
	switch tag {
	case 0:
		if len(t.b) != 0 {
			return nil, errors.New("invalid null")
		}
		return nil, nil
	case 1:
		return t.boolean, nil
	case 2, 9:
		n, e := tagUint(t)
		return n, e
	case 3:
		n, e := tagUint(t)
		if e != nil {
			return nil, e
		}
		shift := uint(32 - 8*len(t.b))
		return int32(n<<shift) >> shift, nil
	case 4:
		if len(t.b) != 4 {
			return nil, errors.New("invalid real length")
		}
		n := float64(math.Float32frombits(binary.BigEndian.Uint32(t.b)))
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, errors.New("nonfinite real")
		}
		return n, nil
	case 5:
		if len(t.b) != 8 {
			return nil, errors.New("invalid double length")
		}
		n := math.Float64frombits(binary.BigEndian.Uint64(t.b))
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, errors.New("nonfinite double")
		}
		return n, nil
	case 6:
		if len(t.b) > 512 {
			return nil, errors.New("octets exceed bound")
		}
		return base64.StdEncoding.EncodeToString(t.b), nil
	case 7:
		if len(t.b) < 1 || len(t.b) > 513 || t.b[0] != 0 || !utf8.Valid(t.b[1:]) {
			return nil, errors.New("only bounded UTF-8/ASCII character strings supported")
		}
		return string(t.b[1:]), nil
	case 8:
		if len(t.b) < 1 || len(t.b) > 65 || t.b[0] > 7 || len(t.b) == 1 && t.b[0] != 0 {
			return nil, errors.New("invalid bit string")
		}
		n := (len(t.b)-1)*8 - int(t.b[0])
		if n < 0 || len(t.b) > 1 && t.b[len(t.b)-1]&byte((1<<t.b[0])-1) != 0 {
			return nil, errors.New("invalid unused bits")
		}
		out := make([]byte, n)
		for i := range out {
			out[i] = '0'
			if t.b[1+i/8]&(1<<uint(7-i%8)) != 0 {
				out[i] = '1'
			}
		}
		return string(out), nil
	case 10, 11:
		if len(t.b) != 4 {
			return nil, errors.New("invalid date/time length")
		}
		if tag == 10 {
			if t.b[1] != 255 && (t.b[1] < 1 || t.b[1] > 14) || t.b[2] != 255 && (t.b[2] < 1 || t.b[2] > 34) || t.b[3] != 255 && (t.b[3] < 1 || t.b[3] > 7) {
				return nil, errors.New("invalid date components")
			}
		} else {
			limits := []byte{23, 59, 59, 99}
			for i, x := range t.b {
				if x != 255 && x > limits[i] {
					return nil, errors.New("invalid time components")
				}
			}
		}
		out := make([]any, 4)
		for i, x := range t.b {
			out[i] = int(x)
		}
		return out, nil
	case 12:
		if len(t.b) != 4 {
			return nil, errors.New("invalid object id")
		}
		n := binary.BigEndian.Uint32(t.b)
		return map[string]any{"object_type": n >> 22, "instance": n & 0x3fffff}, nil
	}
	return nil, errors.New("unsupported scalar")
}
func requestPacket(invoke, service byte, p point, value []byte) []byte {
	body := address(p)
	if service == 15 {
		body = append(body, 0x3e)
		body = append(body, value...)
		body = append(body, 0x3f)
		priority := p.Priority
		if priority == 0 {
			priority = 16
		}
		body = append(body, contextUint(4, uint32(priority))...)
	}
	out := append([]byte{0x81, 0x0a, 0, 0, 1, 4, 0, 4, invoke, service}, body...)
	binary.BigEndian.PutUint16(out[2:4], uint16(len(out)))
	return out
}

type deviceError struct{ code string }

func (e *deviceError) Error() string { return e.code }
func responsePacket(raw []byte, invoke, service byte, p point) (any, error) {
	if len(raw) < 9 || len(raw) > 1030 || raw[0] != 0x81 || raw[1] != 0x0a || int(binary.BigEndian.Uint16(raw[2:4])) != len(raw) || raw[4] != 1 || raw[5] != 0 {
		return nil, errors.New("unsupported/malformed BVLC/NPDU")
	}
	b := raw[6:]
	if b[1] != invoke {
		return nil, errors.New("BACnet invoke mismatch")
	}
	switch b[0] {
	case 0x20:
		if service != 15 || len(b) != 3 || b[2] != service {
			return nil, errors.New("unexpected simple ACK")
		}
		return nil, nil
	case 0x50:
		if len(b) < 5 || b[2] != service {
			return nil, errors.New("malformed BACnet error")
		}
		t, rest, e := takeTag(b[3:])
		if e != nil || t.context || t.tag != 9 {
			return nil, errors.New("malformed error class")
		}
		cl, e := tagUint(t)
		if e != nil {
			return nil, e
		}
		t, rest, e = takeTag(rest)
		if e != nil || len(rest) != 0 || t.context || t.tag != 9 {
			return nil, errors.New("malformed error code")
		}
		code, e := tagUint(t)
		if e != nil {
			return nil, e
		}
		return nil, &deviceError{fmt.Sprintf("bad:device_%d_%d", cl, code)}
	case 0x60, 0x70, 0x71:
		if len(b) != 3 {
			return nil, errors.New("malformed abort/reject")
		}
		return nil, &deviceError{fmt.Sprintf("bad:abort_%d_%d", b[0], b[2])}
	case 0x30:
		if service != 12 || len(b) < 4 || b[2] != service {
			return nil, errors.New("unexpected complex ACK")
		}
		rest := b[3:]
		for _, expected := range []struct {
			tag byte
			n   uint32
		}{{0, objectID(p)}, {1, p.Property}} {
			t, r, e := takeTag(rest)
			if e != nil || !t.context || t.tag != expected.tag || t.opening || t.closing {
				return nil, errors.New("read address missing")
			}
			n, e := tagUint(t)
			if e != nil || n != expected.n {
				return nil, errors.New("read address mismatch")
			}
			rest = r
		}
		if p.ArrayIndex != nil {
			t, r, e := takeTag(rest)
			if e != nil || !t.context || t.tag != 2 {
				return nil, errors.New("array index missing")
			}
			n, e := tagUint(t)
			if e != nil || n != *p.ArrayIndex {
				return nil, errors.New("array index mismatch")
			}
			rest = r
		}
		if len(rest) < 2 || rest[0] != 0x3e || rest[len(rest)-1] != 0x3f {
			return nil, errors.New("missing property value enclosure")
		}
		value, e := decodeValue(rest[1:len(rest)-1], p.ValueType)
		if e != nil {
			return nil, &deviceError{"bad:type_or_encoding"}
		}
		return value, nil
	default:
		return nil, errors.New("segmented/unsupported BACnet APDU")
	}
}
func pointKey(p point) string {
	index := "-"
	if p.ArrayIndex != nil {
		index = strconv.FormatUint(uint64(*p.ArrayIndex), 10)
	}
	return fmt.Sprintf("%d/%d/%d/%s", p.ObjectType, p.Instance, p.Property, index)
}
