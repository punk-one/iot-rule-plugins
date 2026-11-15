package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
)

var deviceCodes = map[string]byte{"D": 0xA8, "W": 0xB4, "R": 0xAF, "M": 0x90, "X": 0x9C, "Y": 0x9D, "B": 0xA0}

func pointWidth(p point) (int, error) {
	if p.Name == "" || p.Address < 0 || p.Address > 0xffffff {
		return 0, errors.New("point requires name and explicit 24-bit protocol address")
	}
	if _, ok := deviceCodes[p.Area]; !ok {
		return 0, errors.New("unsupported MC device area")
	}
	if p.ByteOrder != "" && p.ByteOrder != "ABCD" && p.ByteOrder != "BADC" && p.ByteOrder != "CDAB" && p.ByteOrder != "DCBA" {
		return 0, errors.New("invalid byteOrder")
	}
	if p.UnitID != nil {
		return 0, errors.New("MC has no unit_id; configure routing on the resource")
	}
	switch p.Area {
	case "M", "X", "Y", "B":
		if p.ValueType != "Bool" {
			return 0, errors.New("bit device requires Bool")
		}
		return 1, nil
	}
	switch p.ValueType {
	case "Int16", "Uint16":
		return 1, nil
	case "Int32", "Uint32", "Float32":
		if p.Address == 0xffffff {
			return 0, errors.New("point exceeds address range")
		}
		return 2, nil
	}
	return 0, errors.New("unsupported MC valueType")
}
func plan(points []point, unit int) ([]readRange, error) { return planSized(points, 120, 2000) }
func planSized(points []point, wordLimit, bitLimit int) ([]readRange, error) {
	if len(points) == 0 || len(points) > 1024 {
		return nil, errors.New("point count must be 1..1024")
	}
	names := map[string]bool{}
	points = append([]point(nil), points...)
	for _, p := range points {
		if _, err := pointWidth(p); err != nil {
			return nil, err
		}
		if names[p.Name] {
			return nil, errors.New("duplicate point name")
		}
		names[p.Name] = true
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].Area != points[j].Area {
			return points[i].Area < points[j].Area
		}
		return points[i].Address < points[j].Address
	})
	var ranges []readRange
	for _, p := range points {
		w, _ := pointWidth(p)
		limit := wordLimit
		if p.ValueType == "Bool" {
			limit = bitLimit
		}
		end := p.Address + w
		if len(ranges) > 0 {
			r := &ranges[len(ranges)-1]
			if r.area == p.Area && p.Address <= r.end && max(end, r.end)-r.start <= limit {
				r.end = max(end, r.end)
				r.points = append(r.points, p)
				continue
			}
		}
		ranges = append(ranges, readRange{area: p.Area, start: p.Address, end: end, points: []point{p}})
	}
	return ranges, nil
}

func mcRequest(command uint16, area string, address, count int, data []byte) []byte {
	body := make([]byte, 12+len(data))
	binary.LittleEndian.PutUint16(body[:2], 16)
	binary.LittleEndian.PutUint16(body[2:4], command)
	if area == "M" || area == "X" || area == "Y" || area == "B" {
		body[4] = 1
	}
	body[6] = byte(address)
	body[7] = byte(address >> 8)
	body[8] = byte(address >> 16)
	body[9] = deviceCodes[area]
	binary.LittleEndian.PutUint16(body[10:12], uint16(count))
	copy(body[12:], data)
	return body
}
func mcOrder(p point) string {
	if p.ByteOrder != "" {
		return p.ByteOrder
	}
	if p.ValueType == "Uint16" || p.ValueType == "Int16" {
		return "BADC"
	}
	return "DCBA"
}
func reorder(data []byte, order string) []byte {
	out := append([]byte(nil), data...)
	if len(out) == 2 {
		if order == "BADC" || order == "DCBA" {
			out[0], out[1] = out[1], out[0]
		}
		return out
	}
	if len(out) == 4 {
		switch order {
		case "BADC":
			out = []byte{data[1], data[0], data[3], data[2]}
		case "CDAB":
			out = []byte{data[2], data[3], data[0], data[1]}
		case "DCBA":
			out = []byte{data[3], data[2], data[1], data[0]}
		}
	}
	return out
}
func decodeValue(data []byte, p point) any {
	b := reorder(data, mcOrder(p))
	switch p.ValueType {
	case "Int16":
		return int16(binary.BigEndian.Uint16(b))
	case "Uint16":
		return binary.BigEndian.Uint16(b)
	case "Int32":
		return int32(binary.BigEndian.Uint32(b))
	case "Uint32":
		return binary.BigEndian.Uint32(b)
	default:
		return math.Float32frombits(binary.BigEndian.Uint32(b))
	}
}
func encodeWrite(p point, unit int) ([]byte, error) {
	if p.Area == "X" {
		return nil, errors.New("X inputs are read-only")
	}
	if p.ValueType == "Bool" {
		v, ok := p.Value.(bool)
		if !ok {
			return nil, errors.New("bit write requires Bool")
		}
		data := byte(0)
		if v {
			data = 0x10
		}
		return mcRequest(0x1401, p.Area, p.Address, 1, []byte{data}), nil
	}
	text := fmt.Sprint(p.Value)
	data := make([]byte, 2)
	var err error
	switch p.ValueType {
	case "Uint16":
		var v uint64
		v, err = strconv.ParseUint(text, 10, 16)
		binary.BigEndian.PutUint16(data, uint16(v))
	case "Int16":
		var v int64
		v, err = strconv.ParseInt(text, 10, 16)
		binary.BigEndian.PutUint16(data, uint16(v))
	case "Uint32":
		data = make([]byte, 4)
		var v uint64
		v, err = strconv.ParseUint(text, 10, 32)
		binary.BigEndian.PutUint32(data, uint32(v))
	case "Int32":
		data = make([]byte, 4)
		var v int64
		v, err = strconv.ParseInt(text, 10, 32)
		binary.BigEndian.PutUint32(data, uint32(v))
	case "Float32":
		data = make([]byte, 4)
		var v float64
		v, err = strconv.ParseFloat(text, 32)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			err = errors.New("non-finite value")
		}
		binary.BigEndian.PutUint32(data, math.Float32bits(float32(v)))
	default:
		return nil, errors.New("unsupported write type")
	}
	if err != nil {
		return nil, errors.New("invalid MC write value")
	}
	return mcRequest(0x1401, p.Area, p.Address, len(data)/2, reorder(data, mcOrder(p))), nil
}

// Number is used by JSON decode before the transport is opened.
var _ json.Number
