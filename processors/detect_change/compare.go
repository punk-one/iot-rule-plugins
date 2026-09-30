package main

import (
	"encoding/json"
	"math"
	"reflect"
	"time"
)

// toFloat 转换为 float64
func toFloat(v interface{}) float64 {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int8:
		return float64(t)
	case int16:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case uint:
		return float64(t)
	case uint8:
		return float64(t)
	case uint16:
		return float64(t)
	case uint32:
		return float64(t)
	case uint64:
		return float64(t)
	case float32:
		return float64(t)
	case float64:
		return t
	default:
		return 0
	}
}

// detectChange 检测变化（v1.5）
// 返回：是否匹配、变化值（delta/percent）、是否应该输出
func detectChange(state *ChangeState, newValue float64, cfg Config) (matched bool, changeValue float64, shouldEmit bool) {
	// 首次出现默认只记录状态；新规则可显式启用 emit_first。
	if state.LastTs.IsZero() {
		return false, 0, cfg.Output.Emit == string(EmitPolicyAlways) || cfg.Output.EmitFirst
	}

	// 计算变化值
	delta := newValue - state.LastValue
	var changeVal float64

	// v1.5: 使用 process.detect.type 和 process.detect.value
	switch cfg.Process.Detect.Type {
	case string(ModeDelta):
		// 绝对值变化
		changeVal = math.Abs(delta)
		matched = changeVal >= cfg.Process.Detect.Value

	case string(ModePercent):
		// 百分比变化
		if state.LastValue != 0 {
			changeVal = math.Abs(delta / state.LastValue * 100)
			matched = changeVal >= cfg.Process.Detect.Value
		} else {
			// 如果上次值为0，使用绝对值
			changeVal = math.Abs(delta)
			matched = changeVal >= cfg.Process.Detect.Value
		}

	case string(ModeTrend):
		// 趋势判断：连续上升或下降
		// 简化实现：判断当前变化方向
		changeVal = math.Abs(delta)
		if cfg.Process.Detect.Direction == string(DirectionIncrease) {
			matched = delta > 0 && changeVal >= cfg.Process.Detect.Value
		} else if cfg.Process.Detect.Direction == string(DirectionDecrease) {
			matched = delta < 0 && changeVal >= cfg.Process.Detect.Value
		} else {
			matched = changeVal >= cfg.Process.Detect.Value
		}
	}

	// 检查方向
	if matched && cfg.Process.Detect.Direction != string(DirectionAny) {
		if cfg.Process.Detect.Direction == string(DirectionIncrease) && delta <= 0 {
			matched = false
		} else if cfg.Process.Detect.Direction == string(DirectionDecrease) && delta >= 0 {
			matched = false
		}
	}

	// v1.5: 决定是否输出（使用 output.emit）
	if matched {
		shouldEmit = cfg.Output.Emit == string(EmitPolicyOnChange) || cfg.Output.Emit == string(EmitPolicyAlways)
	} else {
		shouldEmit = cfg.Output.Emit == string(EmitPolicyAlways)
	}

	return matched, changeVal, shouldEmit
}

func detectFieldChanges(state *ChangeState, newValues map[string]interface{}, cfg Config) (matched bool, changeCount int, changedFields []string, shouldEmit bool) {
	if state.LastTs.IsZero() || len(state.LastValues) == 0 {
		return false, 0, nil, cfg.Output.Emit == string(EmitPolicyAlways) || cfg.Output.EmitFirst
	}

	for _, field := range cfg.detectFields() {
		previous, exists := state.LastValues[field]
		current := newValues[field]
		if !exists || !reflect.DeepEqual(previous, current) {
			changedFields = append(changedFields, field)
		}
	}

	matched = len(changedFields) > 0
	changeCount = len(changedFields)
	if matched {
		shouldEmit = cfg.Output.Emit == string(EmitPolicyOnChange) || cfg.Output.Emit == string(EmitPolicyAlways)
	} else {
		shouldEmit = cfg.Output.Emit == string(EmitPolicyAlways)
	}
	return matched, changeCount, changedFields, shouldEmit
}

// updateNumericState 更新数值检测状态
func updateNumericState(state *ChangeState, newValue float64) {
	state.LastValue = newValue
	state.LastTs = time.Now()
}

func updateFieldState(state *ChangeState, newValues map[string]interface{}) {
	state.LastValues = cloneFieldValues(newValues)
	state.LastTs = time.Now()
}

func cloneFieldValues(values map[string]interface{}) map[string]interface{} {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]interface{}, len(values))
	for key, value := range values {
		cloned[key] = cloneComparableValue(value)
	}
	return cloned
}

func cloneComparableValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return typed
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var cloned interface{}
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return value
	}
	return cloned
}
