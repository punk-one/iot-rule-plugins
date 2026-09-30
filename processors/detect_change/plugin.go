package main

import (
	"fmt"
	"time"

	sdk "github.com/punk-one/iot-rule-plugin-sdk"
)

// ChangeDetectPlugin ChangeDetect Processor 插件（符合 doc/场景与测试指南.md:44-121）
// 功能：检测单个字段的变化（delta/percent/trend）
type ChangeDetectPlugin struct {
	sdk.BasePlugin
	config   sdk.PluginConfig
	cfg      Config
	ctx      sdk.RuntimeContext // v1.2: 保存 RuntimeContext 供 OnEvent 使用
	stateMgr sdk.StateManager   // v1.2: 保存 StateManager 供 OnEvent 使用
	stateTTL time.Duration
}

// Info 返回插件信息
func (p *ChangeDetectPlugin) Info() sdk.PluginInfo {
	return sdk.PluginInfo{
		ID:          "detect-change",
		Name:        "Detect Change",
		Version:     "1.0.0",
		Description: "Detect changes in a single field based on group_by keys (delta/percent/trend)",
		Type:        sdk.PluginTypeProcessor,
		Category:    "stateful",
		Capabilities: sdk.PluginCapabilities{
			InputPorts:   1,
			OutputPorts:  1,
			SupportBatch: false,
			SupportAck:   false,
			ConfigSchema: detectChangeConfigSchema,
		},
	}
}

// Init 初始化插件
func (p *ChangeDetectPlugin) Init(cfg sdk.PluginConfig) error {
	return detectChangeFatal(p.validateConfig(cfg))
}

// Start 启动插件（v1.2）
func (p *ChangeDetectPlugin) Start(ctx sdk.RuntimeContext) error {
	// v1.2: 检查 State 支持并保存
	type StateContext interface {
		State() sdk.StateManager
	}

	if impl, ok := ctx.(StateContext); ok {
		p.stateMgr = impl.State()
	} else {
		return detectChangeFatal(fmt.Errorf("state manager not available, ChangeDetect requires stateful support"))
	}

	if p.stateMgr == nil {
		return detectChangeFatal(fmt.Errorf("state manager not available, ChangeDetect requires stateful support"))
	}

	// v1.2: 保存 RuntimeContext 供 OnEvent 使用
	p.ctx = ctx
	p.reportStarted()
	return nil
}

func (p *ChangeDetectPlugin) Stop() error {
	p.reportStopped()
	return nil
}
