//go:build windows

package main

import (
	"errors"
	ole "github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
	"math"
	"runtime"
	"sort"
	"strconv"
)

func call(d *ole.IDispatch, method string, args []any) (any, error) {
	normalized := make([]any, len(args))
	for i, v := range args {
		if number, ok := v.(float64); ok && math.Trunc(number) == number {
			normalized[i] = int32(number)
		} else {
			normalized[i] = v
		}
	}
	value, err := oleutil.CallMethod(d, method, normalized...)
	if err != nil {
		return nil, err
	}
	defer value.Clear()
	raw := value.Value()
	switch v := raw.(type) {
	case nil, string, bool, int8, int16, int32, uint8, uint16, uint32:
		return v, nil
	case int64:
		if v > 9007199254740991 || v < -9007199254740991 {
			return strconv.FormatInt(v, 10), nil
		}
		return v, nil
	case uint64:
		if v > 9007199254740991 {
			return strconv.FormatUint(v, 10), nil
		}
		return v, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, errors.New("nonfinite COM value")
		}
		return v, nil
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("nonfinite COM value")
		}
		return v, nil
	default:
		return nil, errors.New("unsupported COM variant; no lossy type coercion")
	}
}
func readCOM(device device) result {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := ole.CoInitializeEx(0, ole.COINIT_MULTITHREADED); err != nil {
		if oleErr, ok := err.(*ole.OleError); !ok || oleErr.Code() != 1 {
			return result{err: errors.New("COM initialization failed")}
		}
	}
	defer ole.CoUninitialize()
	unknown, err := oleutil.CreateObject(device.ProgID)
	if err != nil {
		return result{err: errors.New("COM class unavailable")}
	}
	defer unknown.Release()
	dispatch, err := unknown.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		return result{err: errors.New("COM dispatch unavailable")}
	}
	defer dispatch.Release()
	if _, err = call(dispatch, device.OpenMethod, device.OpenArguments); err != nil {
		return result{err: errors.New("COM Open failed")}
	}
	defer call(dispatch, "Close", nil)
	keys := make([]string, 0, len(device.Points))
	for key := range device.Points {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	data := map[string]any{}
	quality := map[string]any{}
	for _, key := range keys {
		point := device.Points[key]
		value, err := call(dispatch, point.Method, point.Arguments)
		if err != nil {
			data[key] = nil
			quality[key] = "bad:com_read"
		} else {
			data[key] = value
			quality[key] = "good"
		}
	}
	return result{data: data, quality: quality}
}
