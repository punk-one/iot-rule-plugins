# IoT Rule Plugins 审计整改记录（2026-09-09）

本文跟踪 `comprehensive-audit-2026-09-09.md` 的整改。每个插件仍是独立目录、独立 Go module、独立可执行 `.so` 制品；公共能力只依赖 SDK，没有新增插件间共用业务代码。

## 结果

| 编号 | 状态 | 本轮处理 |
| --- | --- | --- |
| PLG-A01 | 已完成 | JavaScript 的执行、返回 getter/Proxy 导出和 JSON 规范化处于同一 deadline；导出异常转换为普通错误；意外 panic 被实例边界兜底，不会终止共享制品进程。 |
| PLG-A02 | 已完成 | S7 Source 使用 `sync.Once` 和 WaitGroup；Stop 可重复/并发调用，先阻止新发布，等待轮询与资源监听退出，再释放 demand；在途读取返回后不再发布事件。 |
| PLG-A03 | 部分完成 | 设备串行门禁等待、会话名额和分段循环共用请求预算；超时停止后续分段并关闭坏会话，写超时明确提示结果可能未知。底层单次 gos7 I/O 仍受 socket handler timeout 约束，尚不能宣称任意阻塞驱动都被立即取消。 |
| PLG-A04 | 已完成 | 删除非默认端口失败后自动改连 102 的行为；连接严格使用配置的 host/port，握手错误仅给诊断建议。 |
| PLG-A05 | 已完成 | PostgreSQL、TimescaleDB、TDengine Connector 默认最多 100 行，调用方上限 10000 行、256 列、4 MiB 估算响应；返回 `truncated`，超过硬限制明确失败。TDengine 同时移除 SQL/参数 stderr 调试输出。 |
| PLG-A06 | 部分完成 | 第九个 S7 会话继续返回明确、可重试的容量错误，门禁等待可超时；名额仍固定为 8，公平轮换和启用前容量提示待真实多设备方案验证。 |
| PLG-A07 | 部分完成 | JavaScript 输入/输出各限制 1 MiB；单事件 console 最多 100 条、8 KiB；仍需依赖部署层设置进程内存/CPU 限额。 |
| PLG-A08 | 待发布门禁 | 开发构建、SBOM 和可选签名保持不变；正式 GitHub Release 的强制签名、源码绑定和许可材料打包仍需单独 protected release workflow。 |

## 验证

```text
go test -race -count=1 .  PASS
go vet .                    PASS
./scripts/test-plugins.sh   PASS（23 个官方模块 + 可选 FANUC stub）
```

新增测试覆盖：无限循环 getter 超时、getter 抛异常不 panic、JS 输出超限、S7 Stop 重复调用、Stop 等待在途扫描且无尾部发布、100ms 扫描预算、设备门禁超时，以及三个 SQL Connector 的默认/最大行数。

## 仍需设备与发布环境验证

- 使用授权 PLC 分别执行 100ms、500ms、20s 场景，记录 p50/p95/p99、dropped scan、session opens/reuses/closes。
- 慢握手、慢单次 I/O、三分段读写和 Stop 并发故障注入；确认 socket 关闭与写结果未知语义。
- 九个及以上设备的容量、公平性和准入提示。
- JavaScript 制品的 OS/cgroup 资源上限，以及共享进程内两个规则的异常隔离长稳。
- 正式制品的强制签名、验签、SBOM、LICENSE/NOTICE 和对应源码提交绑定。
