# M0–M5 开发交付台账

更新：2026-11-15。用户已确认本次包含 M0–M5 全部列出的能力；原“按需求开发”不再作为排除 M5 的理由。**M0–M5 所列软件能力及 Engine/Hub 配套已完成本地开发验证；真机、原生 ARM 运行和长稳仍为独立生产放行项。**

当前 manifest 共 120 个独立插件模块：原有 24 个，新增 96 个开发预览模块。版本号从 1.0.0 开始不等于已经经过真机、跨架构或生产发布验收。SDK 仍固定 v1.0.0，无本地 replace、go.work 或插件间实现导入。生产发布集合保持按需选择，没有自动上传全部插件。

## 已有代码与阶段边界

| 阶段 | 本轮已有实现 | 尚待开发/验收 |
|---|---|---|
| M0 | 每任务事件复制沿用 SDK；source-s7 v1.0.1 修复先范围后精度并始终保留内部坏质量；Engine Excel 实际解压/稀疏模型/公式/重复列防护；command-guard 持久防重；签名投递包络、服务端认证与稳定身份、事务提交回执、同规则业务 ACK 四件套；20 规则共享进程及单规则停止隔离通过 | 已补齐 profile v1/JSON/Excel、S7/Modbus/MC 跨 Source 合并协调器、MQTT 持久会话及内存限额；双 Engine SIGKILL/重启和 buffer 边界 ENOSPC 通过。全链路同时磁盘耗尽、真实 Broker/TLS/QoS2 与 72 小时长稳仍待生产验收 |
| M1 | Modbus TCP/RTU/ASCII 及 raw RTU/ASCII over TCP，FC1–4/5/6/15/16；MC 1E/3E/4E × TCP/UDP × binary/ASCII（1E 限本机 FX5 配置）；两套 Source/Sink，合并读取、需求感知复用/公平让出、熔断、租约和资源停止释放；100ms/500ms/20s 的 60 秒本地频率验收通过 | 两协议已记录型号的真机验收；Modbus UART/RS485 严格时序、更多 MC 路由/型号；延迟证据为回环模拟器，不代表真实网络性能 |
| M2 | OPC UA Read/Write/首屏 Browse、证书固定、namespace URI、数据/事件订阅与重建；独立 asyncua 加密互操作、强杀及命名空间变更后恢复通过；HTTP Request/Poll/Sink 及有界 JSON 分页；Engine 托管 HMAC Webhook Source，鉴权/重放/限额、规则停止及配置代际验证通过；SQL Server 查询/事务写入/快照与持久整型水位 Source；Engine SQLite/PostgreSQL 规则重建/快照原子性修复，真实 PostgreSQL 16.15 迁移/并发/回滚通过 | 已补齐管理员只读 Browse API/资源弹框；更多 OPC UA 厂商/认证策略、SQL Server 真实互操作与长稳 |
| M3 | TCP/串口/UDP 自由口 poll 和共享只读连续 stream、受控过期写入；真实 TCP/UDP/PTY 共享/慢消费者/停止/重开与 Engine 断线恢复通过；管理员显式启用的 source-manual、受控 API 和节点详情触发面板；source-runtime-events，错误/健康/状态/任务完成值对象、有界观察/限流、反馈环预检及真实 Engine 双规则隔离验证；本地文件与 SFTP 稳定监控/去重；原子文件/JSONL/SFTP Sink，SFTP 指纹/有限会话/远端硬链接无覆盖发布及 OpenSSH fsync 互操作；JSON/XML/CSV、BCD/跨字节位域/常用编码；防抖、触发、拆分/合并、批处理、限流、窗口、模板、定时、限额脱敏调试 Sink | 更多故障注入与真实文件系统验收；运行事件为有界 best-effort，不能代替持久审计 |
| M4 | FANUC 原生 Connector/Source/受控宏变量 Sink，合成 ABI 与本机真实 x64 库加载通过；IEC104 总召/持续监控，序列和 TESTFR、共享会话/点位选择，独立 lib60870 2.4.1 和真实 Engine 单设备连接/双规则/末订阅关闭通过；LSV2 只读，纠正旧帧/子码；独立 Windows EZSocket 只读桥接与 Linux Source，Windows 两种位数构建通过 | 已增加 FANUC 位置/刀具/受限 PMC 字节读取及 LSV2 倍率/刀具/轴位置/程序结构解码；FANUC ARMv7 原生运行、LSV2 真机、EZSocket 厂商映射与真实设备验收仍待完成；FANUC ARM64 明确不支持 |
| M5 | MySQL/SQLite 查询/Source/事务 Sink；InfluxDB OSS 2.x Connector/Flux Request/周期 Source/Sink；MongoDB 共享池、查询/快照 Source/不可变事件 Sink，官方驱动线协议测试；WebSocket 三件套；FINS UDP/TCP 三件套；EtherNet/IP 原子标签三件套；SMTP Connector 与节流/防重邮件 Sink；AMQP 0-9-1 共享连接、手动 ACK Source、mandatory/confirm Sink；Kafka 三件套，消费者组/手动提交、TLS/SASL、解压预算及真实 Engine 闭环；SocketCAN 三件套，经典/扩展/RTR/FD、质量/溢出、过期受限写入，真实 vcan 内核及 Engine 共用进程/防重/停止隔离验证；BACnet/IP 三件套，直连 IPv4 UDP 标量属性读写、设备身份检查、逐点质量与受控写入，独立 bacnet-stack 1.7.0 及真实 Engine 闭环通过 | 所有新协议的模型/版本互操作；AMQP/MongoDB/Apache Kafka 真实服务、CAN/BACnet 实机和长稳；BACnet 不承诺路由/分段/COV；不能用模拟器或交叉编译代替这些验收 |

## 已新增模块清单

详见 [完整插件目录](PLUGIN_CATALOG.md)，各目录 README 说明配置、输入输出、错误、未支持能力。新模块都为独立目录、go.mod/go.sum、manifest、入口、实现和测试。

- 工业协议：Modbus、MC、OPC UA、FINS、EtherNet/IP 的 Connector/Source/Sink。
- 数据库：SQL Server、MySQL、SQLite 各 Connector、查询 Processor、Source、Sink；查询参数与 SQL 分离，单次结果有上限。增量水位要求严格递增唯一 int64，交给引擎后保存，不宣称通用 CDC；晚提交的低 ID/原记录更新可能不被此模式捕获。
- HTTP：共享 Connector、Request Processor、ETag Poll Source、Sink。固定 origin、有限响应和重定向校验；可选有界 JSON 分页，任一页失败、循环或超限时整次失败，无半份结果。2xx 接收与业务事务提交分开。
- InfluxDB：OSS 2.x HTTP API、参数化 Flux、周期快照 Source、文本 CSV 结果、转义行协议、精确 64 位写入类型。快照不是 CDC，重叠查询可能重复；无 1.x/3.x/异步 Cloud 全覆盖承诺。
- AMQP：0-9-1 共享物理连接、多消费者独立通道和有界 prefetch、手动 ACK；mandatory + publisher confirm 拒绝不可路由消息。Source ACK 仅在 Engine 交接后，不代表持久化或业务提交；Sink 的本地缓冲确认需显式选择 Broker 接收目标。
- WebSocket：一资源一共享物理会话，多规则独立有界队列，心跳/信用/超限失败和停止释放。best_effort 不伪装成业务 ACK 或持久重放。
- 通用处理：normalize-points、condition-router、command-guard、rate-limit、window-aggregate、debounce、trigger-timeout、split-records、join-records、batch-events、render-template、parse-records、decode-binary；delivery-envelope、verify-delivery、transaction-receipt、sink-business-ack 形成业务回执链。
- 通用 Source：source-schedule、source-file-watch、source-freeport。

拆分、合并、防抖/超时、限流及窗口节点拒绝已有 buffer_record_id 的输入：尚无安全的多输入/多输出 ACK 聚合契约，应将持久缓冲放在这些节点之后。窗口是处理时间的易失分析状态，不能代替可靠队列。限流及窗口分组区分字符串与数字键，避免异类设备错误共用状态。

## 验证证据

- 新模块逐个执行 go test -race -count=1 与 go vet；最新完整回归结果以测试目录的 logs 为准，增量新增后的全仓回归必须重跑。
- 已有真实 Engine 本地闭环：Modbus/MC → 归一化/条件/HTTP；SQLite 事务写入/回读；拆分/合并/模板；20 规则共用 Source/Connector 进程和资源，停止一条不影响其余；命令防重只写一次模拟 PLC，过期写入不触达设备。
- SQLite 使用真实临时数据库验证事务、读写和只读模式；其他关系库主要使用 database/sql 替身。
- WebSocket 使用实际本地升级握手/回声服务验证一连接多消费者、信用和停止释放。串口使用 Linux PTY 验证物理端口重复资源拒绝、成帧与超时。
- Modbus 有标准 CRC/LRC 固定向量、TCP 模拟器、校验失败；FINS 有区域/合并/UDP 模拟器，TCP 分配节点等互操作仍需补足。EtherNet/IP 有符号路径固定向量、封包/标签读写代码和本地会话模拟器。
- OPC UA 使用独立 asyncua 1.1.8 服务端验证 Basic256Sha256 SignAndEncrypt、实际读写、数据/事件订阅、错误证书拒绝、服务器强杀及更换 namespace index 后重建；尚非全部厂商互操作或长稳验收。业务回执使用两个真实 Engine、回环 MQTT 故障注入与真实 SQLite 验证，最终结果以本机 integration/business-ack-*/report.json 为准。
- AMQP 用本地线协议模拟器验证两个消费者和发布端共用一条 TCP 连接、手动 ACK、mandatory return 优先于 confirm、阻塞 RPC 的截止时间。真实 RabbitMQ 互操作与 TLS 故障仍待验证。

测试均限定本机测试目录、回环模拟器或临时数据库，没有执行真实设备写入。没有厂商 ARM64 FANUC 库：ARMv7 与 ARM64 即使版本相同也不能直接链接/加载；先限定 amd64/ARMv7，ARM64 不生成可用设备插件。厂商库不进入 Git 或默认发行包。

## 软件验证和生产放行边界

1. 本轮软件验证已完成：120 独立模块 race/vet/build；Engine/Hub 全仓 race/vet/build；前端 25 组/76 项及严格生产构建；120 插件 + Engine 共 121 个模块可达依赖漏洞扫描无发现，Hub 与修补版 Cosign 二进制另行扫描通过。
2. Linux amd64 默认 119 个插件、ARM64 默认 117 个插件均完成校验/SBOM/清单构建；FANUC Connector 单独显式构建 amd64。缺少厂商 ARM64 库时不伪造 Source/Sink 支持，ARMv7/ARM64 实际运行不能由交叉编译替代。
3. Hub 已提供经过签名验证的 requirements v1 清单；Engine 校验同一制品的身份/摘要/依赖、原生库哈希/ELF 架构以及伴随服务声明。未知版本/限制字段拒绝。依赖不会自动下载。
4. CI 全量测试、制品按 `release-plugins.txt` 选择；本轮不创建正式版本标签、不上传全部插件、不携带厂商库/数据库/凭据。Git 源码提交不等于生产发布签名或真机验收。
5. 真机型号/固件/安全选件、ARM 原生运行、真实 RabbitMQ/MongoDB/Apache Kafka 等服务、72 小时长稳，以及私有 Keyless/OIDC 现场发布验证保留为未验收。未接受这些条目时，不宣称整个生产审计全部关闭。

完整证据、边界和复现入口见 [本轮验证记录](DEVELOPMENT_VERIFICATION_2026-11-14.zh-CN.md)。
