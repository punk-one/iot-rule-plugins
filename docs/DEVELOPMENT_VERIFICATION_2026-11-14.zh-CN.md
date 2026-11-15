# M0–M5 软件交付与验证记录

更新：2026-11-15。按本次确认，M0–M5 全部列出的能力均纳入开发，包括原可选的 MySQL、SQLite、InfluxDB、WebSocket、FINS、EtherNet/IP、AMQP、Kafka、MongoDB、CAN 和 BACnet。当前为 **24 个既有插件的兼容补丁 + 96 个新开发预览模块**；软件验证完成，生产适用性仍按插件和平台分别验收。

## 实现和入口

- [完整 120 插件目录](PLUGIN_CATALOG.md)列出版本、分类和独立目录；各插件 README 包含配置、输入输出、错误与未支持清单。SDK 固定 v1.0.0，无 go.work、本地 replace 或插件间实现导入。
- PLG-001/002/011：版本化 JSON/Excel 点表、坏质量保留、scale→offset→范围→精度；S7/Modbus/MC 可选共享 ScanCoordinator；MQTT 持久会话、重连、订阅预登记、字节预算与 ACK 信用；buffer 持久化、command-guard、签名业务回执。相关文档：[点表](POINT_PROFILE_V1.md)、[MQTT](MQTT_RELIABILITY.md)、[业务回执](BUSINESS_RECEIPTS.md)、[buffer 故障](BUFFER_FAULT_TESTING.md)。
- PLG-003/004/005：Modbus TCP/RTU/ASCII/over-TCP 和 FC1–4/5/6/15/16；MC 1E/3E/4E、binary/ASCII、TCP/UDP 的已记录边界；批次复用、串行/公平写入、退避、无过期积压。
- PLG-006/007/008：OPC UA 加密 Read/Write/Browse、URI/数据和事件订阅重建；HTTP 请求/分页/轮询/Sink、Engine 托管 HMAC Webhook；MSSQL 参数化查询、事务与整型水位。
- PLG-009/012：自由口 poll/共享 stream、文件/SFTP、解析/二进制、条件、归一化、定时、窗口、限流、防抖、触发、拆分/合并/批处理、模板、Debug；Engine 的管理员手动触发和有界运行事件。易失窗口和本地流 ACK 不代表持久业务提交。
- PLG-010/M4：FANUC 原生只读/受控宏写入与位置/刀具/受限 PMC；LSV2 复杂结构；IEC104 持续共享采集；独立 Windows EZSocket 桥。厂商库不入 Git；ARMv7 与 ARM64 ABI 不兼容。
- Engine/Hub：签名 requirements v1、安装原生依赖检查、Browse/点表 UI/手动触发、运行事实隔离、SQLite/PostgreSQL 规则版本原子性、失败转 DLQ 后只重试交接、NATS 代际关闭/恢复、暂存构建内嵌 UI 单文件发行包。

## 最终本地验证

测试目录为用户指定 `/home/sun/punkiot-work` 下的 `plugin-development-2026-11-14`；日志和私有状态不入仓库。全部设备写入均针对回环模拟器/虚拟设备，无生产 PLC 写入。

| 检查 | 结果/证据 |
|---|---|
| 120 独立插件模块 | 自动发现，race/vet/build 通过；FANUC 原生合成 ABI 单独测试，另有 fanucutil stub 回归；`logs/plugins-final-full.log` |
| Engine/Hub | 全仓 race/vet/build 通过；`logs/engine-final-full.log`、`hub-final-full.log` |
| UI | 25 组/76 项通过，严格 CI 生产构建通过；`logs/ui-final-full.log`、`ui-final-build.log` |
| 漏洞扫描 | 120 插件 + Engine 共 121 模块全部 exit 0；`vulnerability-module-results.json`。Hub 和重建 Cosign 二进制独立通过；不把官方完整 CLI 的依赖状态等同于修补版 |
| 架构/SBOM | amd64 默认119、ARM64默认117；FANUC Connector amd64显式构建；摘要、SBOM、release manifest齐全。仅编译，不冒充 ARM 运行 |
| 单文件发行包 | 最新 UI 在独立暂存源码内嵌入；四文件压缩包、版本、同源 UI/API、空 SQLite/管理员初始化与认证冒烟通过；`logs/package-smoke-final.log` |
| 签名 | 修补版 Cosign 3.1.4 本地密钥原文验证/篡改拒绝；真实 transform-map 和 release manifest bundle 验证。生产 issuer/identity/透明日志策略未弱化；现场 OIDC 发布另行验收 |
| 数据库 | 真实 SQLite；独立 PostgreSQL16.15 临时私有 Unix 套接字集群验证迁移、两规则同版本、并发赢家及快照失败回滚 |

真实 Engine 集成覆盖：Modbus/MC→处理→HTTP、SQLite 回读、20 规则共用进程与单规则停止、Webhook/手动触发鉴权及停止代际、运行事件同节点名双规则隔离、Kafka/SocketCAN/BACnet/IEC104/自由口闭环。当前最终报告：

- `integration/coordinated-engine-kdq_fvh8/report.json`：100ms/500ms/20s 合并/别名/归一化，写入约54ms；模拟设备已执行但回包丢失时不自动重做；快速采集停止后连接释放；4次会话建立/4次关闭。回环延迟不代表 PLC p99。
- `integration/business-ack-y3q0ato2/report.json`：两个真实 Engine/嵌入 NATS/SQLite；假回执拒绝、丢回执重放不重复提交、服务端及边缘端分别 SIGKILL/重启，原持久投递 ID 恢复，合法回执释放。
- `integration/runtime-events-engine-i_dtapsx/report.json`：完成/错误/健康值对象、反馈环拒绝、停止隔离与不同规则同节点 ID 隔离。

独立互操作已覆盖：asyncua1.1.8 加密读写/订阅/强杀恢复；Mosquitto2.0.18 离线消息/启动顺序/Broker重启；OpenSSH 原子扩展；Linux PTY/vcan；lib60870 2.4.1 与 bacnet-stack1.7.0。Kafka kfake、MongoDB/AMQP 线协议模拟器不是对应真实产品认证。

## 保留的生产放行项

1. 真实 PLC/CNC/仪表型号、固件、权限/证书、RS485 严格时序、网络 p99 和读写测试区验收。
2. ARM64/ARMv7 实际运行；FANUC ARM64 没有厂商库，明确不交付。EZSocket 必须验证厂商 Windows COM 映射。
3. 真实 EMQX/TLS/QoS2、RabbitMQ、MongoDB、Apache Kafka、更多数据库版本和 72 小时长稳。
4. buffer 已通过进程 SIGKILL 和私有 tmpfs 的 ENOSPC；完整双引擎链路同时磁盘耗尽仍为额外故障验收。设备动作与持久结果之间崩溃仍可能“结果未知”，不承诺物理 exactly-once。
5. 本机没有 Docker/ARM 运行环境；Hub Dockerfile 的组成组件已构建/扫描，完整容器构建由 GitHub CI 执行。Git push 不等于新增正式 Release、Hub 全量上传或生产部署。

## 复现

每仓使用自身 go.mod，不创建工作区：`GOWORK=off go test -race ./...`、`go vet ./...`；插件根目录再运行 `scripts/test-plugins.sh`。使用已安装 govulncheck1.9.0 执行 `scripts/scan-vulnerabilities.sh`。构建由 `build-plugins.sh` 生成，设置 `PLUGINS_OVERRIDE` 选定目录；发布选择仍为 `release-plugins.txt`。各 `scripts/test-*-engine.sh` 说明可选环境变量，测试状态必须留在私有目录。
