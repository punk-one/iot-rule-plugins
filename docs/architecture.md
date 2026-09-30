# 插件隔离与进程模型

## 独立性边界

每个 `connectors|processors|sources|sinks/<name>` 目录必须同时包含：

- `go.mod` 与 `go.sum`：独立依赖图；
- `plugin.json`：ID、版本、类型、SDK 和 module 的机器可读事实源；
- `main.go`：只服务该插件；
- 自己的实现与测试。

插件 module 禁止导入 `github.com/punk-one/iot-rule-plugins/...` 中的其他 module。
不得新建仓库级运行时公共包。真正稳定且对所有第三方插件有价值的能力，应以向后
兼容方式加入 SDK；否则保留在插件目录内，即使这意味着少量重复。

## 单进程、多规则实例

```text
source-mqtt-1.0.0.so（一个 OS 进程）
├── rule-A / node-1：MQTTSource 对象 A + RuntimeContext A
├── rule-B / node-3：MQTTSource 对象 B + RuntimeContext B
└── rule-C / node-2：MQTTSource 对象 C + RuntimeContext C
```

SDK 的 `ServeFactory` 在每次 host `Dispense("plugin_factory")` 时创建新对象。引擎按
解析软链后的 artifact 绝对路径缓存进程，并为每个规则节点取得不同 RPC 对象。
反向 Engine RPC 使用 `MuxBroker.NextId()` 的唯一 ID，避免并发 Start 时把规则上下文
接到错误实例。

进程所有权规则：

1. 同一个 artifact 同时最多一个 go-plugin Client/OS 进程。
2. `Init`、`Start`、事件和 `Stop` 只作用于规则节点自己的插件对象。
3. 活动实例计数大于零时禁止卸载或替换进程。
4. 释放最后一个实例后进程保持预热，由 Loader 卸载或引擎关闭时终止。
5. 老的 `sdk.Serve` 插件没有工厂隔离能力，引擎拒绝把它接入共享进程池。

该模型降低进程数量，但并不让规则共享业务状态。单个插件进程崩溃仍会同时影响使用
该 artifact 的规则，因此 Loader 必须把进程故障视为插件级故障，并由规则恢复流程
重新创建所有实例。
