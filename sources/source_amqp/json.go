package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
)

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
