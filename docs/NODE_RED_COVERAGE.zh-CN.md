# Node-RED 参考能力复核

2026-11-21 按[上次规划](PLUGIN_DEVELOPMENT_ROADMAP.zh-CN.md#3-node-red-对照清单)复核 N01–N24。每项都有对应源码，部分事件入口需要 Engine 配套。表中的“交付范围”指本项目实现的功能，可用于判断配置用途和验收边界。插件 ID 和版本以[完整目录](PLUGIN_CATALOG.md)及 `plugin.json` 为准。

| 编号 | 能力 | 实现位置 | 交付范围和边界 |
|---|---|---|---|
| N01 | MQTT 输入/输出 | `connect-mqtt`、`source-mqtt`、`sink-mqtt`、`buffer-node` | 稳定客户端身份、持久会话、重连和队列指标。Broker 确认与事务提交回执分别配置。 |
| N02 | S7 输入/输出及端点 | `connect-s7`、`source-s7`、`sink-s7` | 绝对地址点表、分段、质量码、需求感知会话。现场采集周期和设备兼容需实测。 |
| N03 | Modbus | `connect-modbus`、`source-modbus`、`sink-modbus` | TCP、RTU、ASCII 和相应 TCP 透传，FC 1–4 / 5、6、15、16。按型号确认字节序和串口时序。 |
| N04 | 三菱 MC | `connect-mc`、`source-mc`、`sink-mc` | 首版路径为 3E TCP binary，其他 1E / 3E / 4E、TCP / UDP、ASCII 组合按模块支持矩阵使用。 |
| N05 | OPC UA | `connect-opcua`、`source-opcua`、`sink-opcua` | Browse、读写、数据与事件订阅、证书固定和命名空间重建。Method Call 在原规划中单独立项。 |
| N06 | HTTP 请求、轮询与接入 | `connect-http`、`request-http`、`source-http-poll`、`source-webhook`、`sink-http` | 有界请求、认证、ETag / 分页，Webhook 由 Engine 鉴权和入站调度。 |
| N07 | Function | `javascript-transform` | `main(input)`、标准 ECMAScript、受控日志和执行预算。本轮补齐可选 `script_version` 及日志中的 `script_sha256`，脚本仍随规则配置保存。 |
| N08 | Change / Template | `transform-map`、`render-template` | 原有模板和路径映射继续支持。本轮增加有序 `process.operations`，可设置常量、移动字段或删除字段。JSONata 为单独评估项。 |
| N09 | Switch | `condition-router` | 保留原有 `matched` 和 `otherwise` 配置。本轮增加 `routes`、`match`（首匹配或全匹配）、`default_label`。需配置同名标签边；持久缓冲放在多路分发之后。 |
| N10 | 值变化、范围和窗口 | `detect-change`、`normalize-points`、`window-aggregate` | 死区、缩放、偏移、范围、精度及有界窗口。本轮补齐 `process.detect.baseline: last_change`，使小幅变化累计越过阈值时也可输出。默认沿用 `previous_sample`。 |
| N11 | Delay / Trigger | `rate-limit`、`debounce`、`trigger-timeout` | 有界限流、折叠和触发。要求重启后继续送达的链路使用持久缓冲。 |
| N12 | Split / Join / Batch | `split-records`、`join-records`、`batch-events` | 关联、批量和超时边界。多入多出节点之后再配置需要 ACK 的持久缓冲。 |
| N13 | Inject / 定时 | `source-schedule`、`source-manual` | 周期、cron、时区、错过周期策略，管理员显式触发由 Engine 实现。 |
| N14 | Catch / Status / Complete / Debug | `source-runtime-events`、`sink-debug` | Engine 提供节点运行事实、隔离和反馈保护。调试有采样、输出上限与脱敏。 |
| N15 | JSON / XML / CSV / 二进制 | `parse-records`、`decode-binary` | 有界解析、位域、字节序、BCD、字符编码与大整数保真选项。 |
| N16 | TCP、UDP、串口 | `connect-tcp`、`connect-udp`、`connect-serial`、`source-freeport`、`sink-freeport` | 轮询、共享流、串口资源独占与受限写入。UDP 交付语义由上层协议决定。 |
| N17 | SQL Server、MySQL、SQLite | 对应 Connector、查询 Processor、关系库 Source 和 Sink | 参数绑定、结果上限、事务、可选水位。水位要求严格递增字段，快照与 CDC 的交付语义不同。 |
| N18 | InfluxDB | `connect-influxdb`、`request-influxdb`、`source-timeseries-influxdb`、`sink-timeseries-influxdb` | 当前以 OSS 2.x HTTP、Flux 与行协议为范围。 |
| N19 | Redis、PostgreSQL | `connect-redis`、`connect-postgres`、关系库 Source/Sink 与 Redis 映射 | 本轮补齐 Redis 返回上限、可选 Hash TTL 和只读连接池指标。PostgreSQL 可通过 `output.conflict_key_columns` 按目标表已有唯一约束保留首条写入，并提供无副作用的池指标。 |
| N20 | 文件与 SFTP | `source-file-watch`、`sink-file`、`connect-sftp`、`source-sftp`、`sink-sftp` | 目录边界、稳定写入判定和检查点，SFTP 固定主机指纹与有限会话。 |
| N21 | WebSocket | `connect-websocket`、`source-websocket`、`sink-websocket` | 共享会话、帧限额、心跳和有界队列。需要持久重放的流程采用应用层策略。 |
| N22 | FINS、EtherNet/IP | 两个协议各自的 Connector / Source / Sink | FINS UDP/TCP，EtherNet/IP 首版为规定的 Logix 原子标签。型号、路由和写入权限需验收。 |
| N23 | 邮件、Webhook 通知 | `connect-smtp`、`sink-email`、`request-http`、`sink-http` | 邮件节流、防重、凭据、错误上报，HTTP 通知使用现有 HTTP 插件。 |
| N24 | AMQP、Kafka、MongoDB、BACnet、CAN | 对应的 Connector / Source / Sink，另有 `request-mongodb` | 覆盖 AMQP 0-9-1、Kafka 消费组、MongoDB 有界查询、BACnet/IP 直连标量、SocketCAN 等预览范围。按服务版本、设备和部署平台单独验收。 |

使用本仓库的规则和模块 schema，Node-RED flow 转换、npm 模块兼容、JSONata 与 OPC UA Method Call 均在原规划独立评估范围内。N06、N13、N14 的托管入口需要[配套 Engine 分支](https://github.com/punk-one/iot-rule-engine/tree/engineering/multi-gateway)。现场 PLC/CNC、三方服务版本、ARM 实际运行和长稳仍需按各自验收记录确认。

本轮本地联调使用定时 Source、JavaScript 字段标记、有序字段操作和条件节点，分别核对首匹配、全匹配、默认出口及删字段结果。PostgreSQL 使用独立 PostgreSQL 16.15 实例验证单条及批量重复写入；Redis 限额和 TTL 使用本地测试夹具。
