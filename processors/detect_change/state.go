package main

import "time"

// ChangeState ChangeDetect 状态（符合 doc/场景与测试指南.md:59-65）
// 只存 1 条历史，这是 ChangeDetect 高性能的关键
type ChangeState struct {
	LastValue  float64                `json:"last_value"`            // 单字段数值模式下的上一次值
	LastValues map[string]interface{} `json:"last_values,omitempty"` // 多字段变化模式下的上一次值
	LastTs     time.Time              `json:"last_ts"`               // 上一次的时间戳
}

// NewChangeState 创建新状态
func NewChangeState() *ChangeState {
	return &ChangeState{}
}
