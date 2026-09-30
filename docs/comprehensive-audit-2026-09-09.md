# IoT Rule Plugins 当前实现综合审计（2026-09-09）

## 基线、覆盖范围与结论

审计基线为 HEAD `5ffaca9086640bad39c13856c9bcab484af0ac23` 加当前未提交工作树，环境为 Go 1.25.0、WSL Linux amd64。仓库含 23 个官方插件模块和独立可选 FANUC stub；运行进程、Connector Resource、物理会话三个生命周期分别审查。

全部 23 个官方模块做了契约、race 测试、vet 和构建检查；深度静态审查重点是 JavaScript Transform、S7 Source/Connector、SQL 结果读入、共享进程边界和发布脚本。其他模块的“自动化通过”不表示逐行证明无缺陷。没有连接真实 PLC、MQTT、Redis、SQL、FANUC 进行运行期压力或故障注入。

**结论：自动化门禁通过，生产发布仍需整改。** JavaScript 返回值转换和 S7 停止路径可能影响同插件进程中的所有规则；S7 总 deadline 和目标端口行为需要先明确。旧审计中的“通过/有条件通过”没有覆盖本轮新发现。

## 测试与复现

| 检查 | 本轮结果 |
| --- | --- |
| 根模块 `go test -race -count=1 .`、`go vet .` | 通过 |
| `scripts/test-plugins.sh` | 23 个模块 race/vet/build 通过；该次脚本的 test 输出命中缓存 |
| 23 模块显式 `go test -race -count=1 ./<module>/...` | 随后全部无缓存重跑通过 |
| 可选 fanucutil stub | 脚本验证通过（缓存）；没有厂商硬件/二进制验证 |
| JavaScript 返回值转换诊断 | 复现运行超时范围外执行和 Export 抛 panic |
| 真实设备 100ms/500ms/20s、72h 长稳 | 本轮未做，不能据此承诺性能或停止时限 |
| 正式发布打包/签名 | 本轮未重建运行中的 dist；仅临时构建与脚本静态审查 |

官方模块清单（均无缓存 race 通过）：

| 类别 | 模块目录 |
| --- | --- |
| Connector | connect_mqtt、connect_postgres、connect_redis、connect_s7、connect_tdengine、connect_timescaledb |
| Processor | detect_change、javascript_transform、process_status_event、redis_relationship_mapping、transform、transform_map |
| Sink | sink_mqtt、sink_postgres、sink_redis、sink_s7、tdengine_sink、timescaledb_sink |
| Source | source_mqtt、source_postgres、source_redis、source_s7、source_timescaledb |

## 发现

### PLG-A01 · P1 · JS 返回值导出在超时保护之外，可执行 getter 或抛 panic（已隔离复现）

**证据：** `processors/javascript_transform/plugin.go:102` 启动计时器，`:105` RunProgram，`:106` 停止计时器，而 `:119` 才执行 `value.Export()` 和 JSON marshal。Export 会求值返回对象属性，包括 getter；getter 抛异常可成为 Go panic。插件 OnEvent 没有围绕该阶段的恢复，SDK `internal/transport/plugin_rpc.go:89` 也直接调用 OnEvent。

诊断程序 [js_export_repro.go](audit-repros-2026-09-09/js_export_repro.go) 使用项目固定 Goja 版本，精确复现“RunProgram→停止计时器→Export”的顺序。从 `processors/javascript_transform` 运行：

```bash
go run ../../docs/audit-repros-2026-09-09/js_export_repro.go
```

本轮结果：

```text
execution_timer_stopped_before_export=true export_elapsed_ms=99 configured_timeout_ms=5
export_throw_panics=true
```

诊断 getter 仅运行约 100ms，抛 panic 被诊断程序捕获；没有对运行插件发送恶意脚本，没有做无限循环或 OOM 攻击。该复现证明底层转换语义与代码路径风险，不冒充完整 RPC 子进程崩溃测试。

**影响：** 脚本主函数很快返回，不代表事件处理已受 timeout 约束。一个规则可占住处理或触发共享 JS artifact 进程崩溃，连带同进程其他实例。
**建议：** 将返回值规范化放在仍受中断控制的 VM 阶段，捕获导出异常，限制输出深度/字节，防循环引用；整个处理 deadline 包含输入、执行、转换与输出。不要仅在更外层增加 recover 而保留无限执行窗口。
**验收：** 返回 getter、Proxy、循环结果、异常属性均在预算内失败且进程存活；两个规则实例互不影响。

### PLG-A02 · P1 · S7 Source Stop 不幂等，且未等待在途轮询结束（静态确认）

**证据：** `sources/source_s7/plugin.go:63` 每次直接 close(stopCh)，没有 sync.Once；第二次 Stop 会触发 close-of-closed-channel panic。`:68` 发送 release_demand 后即返回，没有 WaitGroup 等待两个后台循环。`handler.go:113` 同步 BatchRead 返回后一直处理到 `:166` Publish，没有停止状态检查。

如果 Stop 与轮询并发，当前读取仍可能继续处理或发布；release_demand 与最后一轮需求登记也存在时序竞争。SDK/宿主是否在某条路径中保证 Stop 只调用一次不能替代插件自身生命周期保护。该问题未通过真实设备停机测试复现，结论来自明确控制流。

**建议：** Stop 使用 once+cancel+wg；停止新的读取，约束/取消在途请求，等待循环退出，再释放最后需求并报告 Stopped。并发调用须共享同一完成结果，不能在锁内等待自身回调。
**验收：** 两次/并发 Stop 无 panic；阻塞读期间 Stop 有界返回，返回后无 Publish、无需求重新登记，其他规则实例继续工作。

### PLG-A03 · P1 · S7 请求 TimeoutMS 没有覆盖整批操作（静态确认）

**证据：** `connectors/connect_s7/session_manager.go:30` 使用不可取消 mutex；`:51` 仅把 req.TimeoutMS 用于等待会话名额。`client.go:41` 设置独立的 timeout_sec，`execute.go:147,177` 按分段循环调用 AGReadMulti，没有统一剩余 deadline 检查。

请求可能在门禁等待、名额等待、建连和多个分段分别消耗时间。SDK 对 RPC 的超时会关闭传输 client，但不能等价认为 PLC 服务端操作已取消；Stop 也要等待操作锁。超时上层重试对写入尤其不能当作原写入未执行。

**建议：** 建立从入队到最终分段的单一 deadline；门禁和名额获取可取消；每次 I/O 使用剩余时间，过期时终止会话并明确结果可能未知。保持写入不透明重试。
**验收：** 模拟 3 分段和慢握手，整批总时长受预算约束；超时后不继续后续写入，Stop 有界、socket 关闭；用真实设备验证 100ms 目标，而非仅调短 timeout_sec。

### PLG-A04 · P1 · 非默认端口握手失败后自动改连 102（静态确认）

**证据：** `connectors/connect_s7/client.go:14` 首次建连失败，在 `:82` 符合 invalid-PDU 且原端口非 102 时，`:19` 自动改用同 host 的默认 102。

用户的不同转发端口可能代表不同 PLC。102 上如果存在另一个有效 S7 服务，自动回退可能读到错误设备；共享该 opener 的写入路径风险更高。本轮没有连接任何实际端口，不断言现场一定存在错误目标。

**建议：** 默认严格使用配置地址；把回退做成显式可选配置并提供目标身份验证，或只返回诊断建议。重试同一目标与更换目标必须区分。
**验收：** 配置 20101 失败时不得自动连 102；用两个本地模拟端点验证，不使用真实 PLC 写入验证。

### PLG-A05 · P2 · SQL 结果上限为可选值，缺少硬容量边界（静态确认）

**证据：** `connectors/connect_postgres/execute.go:134` 将所有行累积到 slice，仅在 MaxRows>0 时限制；TimescaleDB `execute.go:136`、TDengine `execute.go:130` 同样如此。没有单行/列值/总字节上限。

普通 source_postgres 配置有默认 max_rows=100，这是已有保护；但 Connector 接口允许其他调用方传 0 或很大数值。timeout 限制时间，不能限制短时间内的大结果内存占用。

**建议：** Connector 层强制默认和最大行数、最大列数及响应总字节；返回明确截断/分页标志，超限及时关闭 Rows。保留 Source 默认但不能只依赖 Source。
**验收：** mock SQL 驱动产生大量行/大列，默认有界，超限结构化失败，无持续内存增长；同步覆盖三个 SQL Connector。

### PLG-A06 · P2 · 八个高频 S7 会话可能长期占满包级名额（静态确认的容量风险）

**证据：** `session_manager.go:12,94` 固定八个会话名额；成功复用时保留名额，直到 `:226` closeSessionLocked 才释放。默认 maxSessionAge=0，持续高频请求重置 idle timer。`plugin.go:75` CreateResource 并未拒绝第九个 Resource。

八个持续采集资源可长期占满容量，第九个资源反复等待并进入熔断。现有“最多八个会话”是明确限制，但缺少资源准入反馈/公平轮换，用户可能把容量问题误判为设备离线。

**建议：** 明确容量上限并在资源/规则启用时提示；可增加等待资源优先的安全批次轮换或可配置名额，避免无限等待。不能为公平性在读写批次中途抢占会话。
**验收：** 九个 mock Resource 连续运行，超额资源得到明确容量错误或在承诺时间内被服务，已运行资源无中途断批。

### PLG-A07 · P2 · JS 与日志仍缺少进程/输出资源限额（静态确认，未执行 OOM）

**证据：** `javascript_transform/plugin.go` 虽限制代码 65536 字节和 1–1000ms timeout，但没有输入/输出字节上限；`:162` console 拼接全部参数。运行时之间共享同一个 artifact 进程，进程内存不是逐 Runtime 隔离。

修复 PLG-A01 后仍不能仅靠 CPU 中断防止内存压力；SDK logger 又有独立生命周期缺陷，见 SDK-A01。当前 Engine 规则 CRUD 路由允许已认证用户访问，不应笼统声称所有 JS 编辑都只限 admin。

**建议：** 进程资源限额、最大并发脚本数、输入/输出大小与日志条数/字节限制；明确规则编辑权限。限制及错误必须可观测。
**验收：** 在单独受控测试进程测试上限与恢复，不对共享现场进程进行 OOM 注入。

### PLG-A08 · P2（发布门槛）· 正式制品验签与分发资料未形成强制门禁（静态确认）

**证据：** `build-plugins.sh:110` 仅在设置签名环境变量时签名；`:119` 的强制签名默认关闭；`.github/workflows/ci.yml` 明确上传 plugins-unsigned。当前脚本生成二进制、摘要、SBOM、manifest，但未看到同一发布包自动收集 LICENSE/NOTICE 和对应版本源码获取说明。

这是发布流程未完成，不代表未签名开发构建本身错误。AGPL 选择已经确定，本轮不重新评判许可方案，也没有对外发布制品。

**建议：** 单独建立 protected release job，强制验签并绑定源码提交、SDK 版本、摘要、SBOM、许可证/来源说明；FANUC 厂商库继续按独立授权边界处理。
**验收：** 无签名禁止正式发布；从干净环境按说明构建，能核对全部制品来源；发布包内容与源码/许可声明一致。

## 架构现状与未完成能力

- 23 个插件独立目录/module/manifest/产物，普通节点使用 ServeFactory，Connector 使用 ServeConnector；根契约测试通过。
- `.so` 是项目命名约定下的独立可执行制品，通过 RPC 运行，不是 Go 进程内动态库。
- S7 健康会话可跨高频批次复用，错误关闭与退避已有实现；一个批次的分段使用同会话。
- source_s7 是同步 latest-only 调度，推进错过的时间窗口，未看到按历史 tick 无限排队。
- 共享 Resource 只串行会话/请求，不会自动合并不同 Source 的点位列表；不能宣称已经实现跨规则统一 ScanCoordinator。
- SQL/Redis 有调用 deadline，但结果大小与 Stop context 传播仍需加强。
- 规则业务转换已拆到独立处理插件，Redis 映射和 JS 返回契约有测试；Redis 关系映射仍按条 HGET，高映射量需评估批量协议。
- 真实设备数据质量、p50/p95/p99、长稳、网络分区恢复和内存容量没有本轮证据。

## 整改优先级与复验

1. 首先修 PLG-A01/A02；补“同插件进程两条规则，一条异常/停止另一条继续”的隔离测试。
2. 修 PLG-A03/A04，再做授权设备实验：100ms 与 500ms 各 60 秒、20s 三轮；同时测 Stop、慢分段、端口错误、写入结果不确定。
3. 补 PLG-A05/A06/A07 容量策略，联动 SDK 日志/Clone 修复；重复启停 1000 次后 goroutine、FD、内存回落到稳定范围。
4. 进行 24h/72h 长稳与发布流程验收，再创建正式发布版本。

本报告与诊断文件仅记录现状；上述发现尚未在生产代码中修复。
