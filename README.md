# IoT Rule Engine 官方插件

[English](README.en.md) · [插件目录](docs/PLUGIN_CATALOG.md) · [Node-RED 对照](docs/NODE_RED_COVERAGE.zh-CN.md)

IoT Rule Engine 的插件把连接、采集、处理和输出拆成独立模块。规则节点引用 Connector 资源，同一连接配置可由多条规则复用。

仓库现有 **120 个独立插件**，包括 24 个原有插件和 96 个开发预览。源码、正式发布制品和生产验收的状态分别见[插件目录](docs/PLUGIN_CATALOG.md)和[交付记录](docs/PLUGIN_DEVELOPMENT_STATUS.zh-CN.md)。插件各自管理版本，统一使用 **SDK v1.0.0**。

| 类型 | 数量 | 代表能力 |
|---|---:|---|
| Connector | 30 | MQTT、S7、Modbus、MC、OPC UA、HTTP、TCP/UDP/串口、数据库、消息队列 |
| Source | 32 | 按周期采集、订阅、文件监控、定时、手动触发、Webhook、运行事件 |
| Processor | 29 | JavaScript、映射、条件路由、归一化、窗口、拆分/聚合、解析、应用回执 |
| Sink | 29 | MQTT、HTTP、设备写入、数据库、文件、SFTP、邮件、调试 |

## 使用方式

1. 选定插件 ID、版本和目标架构。在 Engine 的插件管理中安装官方 Hub 已发布的制品，或在开发环境从源码构建所选插件。
2. 在 Engine 创建具名 Connector 资源，把设备地址、凭据引用和连接策略配置在资源上。
3. 在规则中按需连接 Source、Processor 和 Sink，并为节点绑定资源。例如 `source-modbus → normalize-points → buffer-node → sink-mqtt`。保证业务落库需要额外的事务回执，MQTT 的 Broker 确认属于传输层。

每个插件目录都有独立的 `go.mod`、`plugin.json` 和可执行 `.so` 制品。一个已加载制品最多启动一个插件进程。各规则节点通过 `sdk.ServeFactory` 保有自己的实例和停止生命周期，Connector 通过 `sdk.ServeConnector` 管理隔离的资源。S7 按采集需求复用会话，MQTT 可持续持有 Broker 连接。

## 本地构建与验证

仓库当前使用 Go 1.27.4。以下示例只构建 MQTT 三个模块，生成的文件位于 `dist/plugins`。设置 Engine 的 `IOT_RULE_PLUGINS_DIR` 指向该目录即可在开发环境加载。

```bash
export GOWORK=off
PLUGINS_OVERRIDE=$'connectors/connect_mqtt\nsources/source_mqtt\nsinks/sink_mqtt' ./build-plugins.sh
```

单插件验证示例：

```bash
cd processors/javascript_transform
go test -race .
go vet .
```

在仓库根目录运行 `go test ./...`、`go vet ./...` 和 `./scripts/test-plugins.sh` 检查清单契约及所有独立模块。各插件通过公开 SDK 标签构建，仓库无需 `go.work`。

## 发布与兼容性

- `release-plugins.txt` 选择正式发布的模块。流程按插件 ID、版本和平台打包，并生成 SHA-256、CycloneDX SBOM、签名清单及 Sigstore bundle。Engine 下载所选插件。
- `plugin.json`、插件文档与对照表记录协议模式、输入输出和平台限制。`source-manual`、`source-webhook`、`source-runtime-events` 需要 Engine 的配套入口，可使用[配套 Engine 源码](https://github.com/punk-one/iot-rule-engine/tree/engineering/multi-gateway)。FANUC 需要本地厂商库，当前原生 Connector 支持 Linux amd64 / ARMv7，ARM64 暂无对应库。
- JavaScript 节点用 `function main(input)` 返回 JSON 兼容输出，提供受控 `console`。使用范围、数据格式和脚本示例见 [JavaScript 节点文档](processors/javascript_transform/README.md)。
- 开发预览可从源码构建。现场设备型号、服务版本、ARM 原生运行和长时间稳定性按实际环境验收。

## 相关文档

- [接入架构](docs/architecture.md) · [Connector 会话生命周期](docs/connector-session-lifecycle.md) · [统一点表](docs/POINT_PROFILE_V1.md)
- [MQTT 可靠性](docs/MQTT_RELIABILITY.md) · [持久缓冲](processors/buffer_node/README.md) · [业务回执](docs/BUSINESS_RECEIPTS.md)
- [Node-RED 参考规划](docs/PLUGIN_DEVELOPMENT_ROADMAP.zh-CN.md) · [验证与生产验收边界](docs/DEVELOPMENT_VERIFICATION_2026-11-14.zh-CN.md) · [2026-11-21 审计](docs/audit-2026-11-21.md)
- [SDK v1 接入](docs/sdk-v1-integration.md) · [发布流程](docs/release.md) · [安全报告](SECURITY.md) · [贡献约定](CONTRIBUTING.md)

本项目采用 [GNU AGPL 3.0 only](LICENSE)。厂商库和商标适用各自的授权条款。
