# Connector 外部连接生命周期

## 统一原则

Connector 插件进程与设备连接分离管理。同一插件产物维持一个 sidecar 进程，多个
规则共享该进程；每个 Connector Resource 独立保存配置、调度状态、串行门禁和会话。

| 插件 | 默认 `session_policy` | 行为 |
| --- | --- | --- |
| `connect-s7` | `scheduled_reuse` | 根据下一次采集需求短暂复用，默认窗口 2 秒 |
| `connect-fanuc`（规划） | `per_operation` | 每批分配并释放 FOCAS handle |
| `connect-mqtt` | `persistent` | 长连接与自动重连 |
| SQL / TDengine / Redis | `pooled` | 使用受限连接池 |

## S7 需求感知会话

S7 Resource 的 `connection` JSON 支持：

```json
{
  "sessionReuseHorizonMillis": 2000,
  "closeWhenNoDemand": true,
  "maxSessionAgeMillis": 0,
  "failureThreshold": 3,
  "retryBackoffMillis": 1000,
  "maxRetryBackoffMillis": 30000
}
```

- `sessionReuseHorizonMillis=0`：严格每批次连接、读取、关闭。
- 下一次需求不超过窗口：保留健康会话；100ms、500ms、1s 默认复用。
- 下一次需求超过窗口：本批完成立即关闭；5s、20s 默认每批连接。
- `closeWhenNoDemand=true`：没有需求时立即关闭；设为 `false` 时最多保留到空闲窗口。
- `maxSessionAgeMillis=0`：不做主动年龄轮换；大于 0 时在安全批次边界轮换。

一个批次最多建立一个会话，全部点位及因 PDU 限制产生的分段都使用该会话。多个
Source 通过各自的 Resource Token 报告下一次需求；同一 Resource 的读写严格串行，
不同 Resource 并行，单个 `connect-s7` 进程最多同时占用 8 个设备会话。

Source 停止时会发送 `read/release_demand`，最后一个近期消费者退出会立即释放会话；
异常、资源删除/停止、配置替换也会立即关闭。旧 `shared_duplex`、`split_rw` 输入继续
可读并归一化为 `scheduled_reuse`；显式 `per_operation` 仍受支持。

## 调度、失败与观测

`source-s7` 使用固定节拍、同步 latest-only 轮询。上一轮跨过后续 Tick 时直接推进到
未来最近时间，不补跑历史 Tick，也不会重叠批次。它记录实际间隔、单轮耗时和跳过
窗口指标，并在读请求中携带 `next_demand_in_ms`。

高频读取失败不在同一 Tick 内立即重试，下一次真实采集自然重试。连续失败达到阈值
后打开指数退避熔断；熔断期间不拨号，窗口到期后由一个串行请求 half-open。写入不
透明重试。任何会话级或协议级操作错误都会丢弃当前会话。

Connector 的 `metadata/info` 返回会话状态、建立/复用/关闭次数、连续失败数和熔断
截止时间；批量读取响应元数据返回分段数与本轮耗时。引擎关闭周期 Probe，健康状态
由真实操作驱动。

## 验收基线

- 100ms 连续 60 秒：约 600 批；正常时一次或极少量建连，批次不重叠，停止后释放。
- 500ms 连续 60 秒：采集窗口内复用，停止或无需求后不超过 2 秒释放。
- 20s 连续 3 批：每批分别建立并关闭一次。
- PLC 离线时不会每 100ms 拨号；失败和 Resource Stop 后不遗留连接。
- MQTT 的 `persistent` 行为不受影响。

当前协调边界是“同一 Resource 的物理会话和串行门禁”。多个 Source 同时到期时会
共享会话并按 ResourceBus 顺序执行，但不会跨请求重写或合并各自的点位集合；如需
把多个独立规则的点位合成单次 S7 PDU 请求，应另行增加引擎级批次聚合协议。
