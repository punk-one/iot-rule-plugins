# S7 采集设计与实现审计（2026-09-06）

## 结论

本次以 `D:\Lh\workspace\edge-apps\edge-service-s7` 为只读参考，对当前 `connect-s7`、`source-s7`、`detect-change` 与 `sink-mqtt` 进行职责审计。推荐并已采用的链路为：

```text
connect-s7（协议资源）
    → source-s7（点表、周期、工程值处理）
    → detect-change（有状态变化过滤）
    → sink-mqtt（消息发布）
```

四项职责保持在四个独立插件目录和独立 SO 中，不引入插件间共享业务代码；跨插件只依赖 `iot-rule-plugin-sdk v1.0.0` 的接口和 JSON 契约。

## 参考项目审计

参考项目具备以下值得保留的设计：

- Profile 可复用，设备实例与点表定义分离。
- 一个 Profile 支持多个点位，并允许采集组拥有独立周期、变化检测、关注字段与心跳策略。
- 先合并到期点位再批量读，减少 S7 请求次数。
- 工程值按 `scale → precision` 顺序处理。
- Bool、数值、字符串拥有明确的编解码路径。

不直接复制的部分：

- 参考实现的多组 GCD 调度、结构体/数组解码和自动 PDU 分包复杂度较高；当前现场点表只有一个 5 秒采集组、19 个标量点，直接引入会扩大故障面。
- 当前引擎已有通用状态插件 `detect-change`，在 `source-s7` 内再实现 onChange 会造成状态、内存上限和恢复语义重复。
- Excel 属于控制面配置能力，不能放入 `connect-s7` 或实时采集循环。

## 已发现问题与处置

| 风险 | 原状态 | 处置 | 结果 |
|---|---|---|---|
| 工程值职责混杂 | `connect-s7` 同时执行缩放和精度 | `source-s7` 发给连接器的请求只保留 address/name/value_type/max_length，工程策略在源插件执行 | 连接器可独立演进，旧调用仍兼容 |
| 配置周期可读性差 | 只有 `interval_ms` | 增加 `interval: "5s"`，旧字段继续支持 | 配置不易填错 |
| 越界数据无策略 | 超范围值直接上报 | 增加 `use_default`、`clamp`、`drop`、`error` | 异常值行为可预测 |
| 精度 0 语义不清 | `precision` 为普通 int，缺省和 0 无法区分 | 使用可选字段；显式 0 表示取整，缺省表示不处理 | 语义完整 |
| 点名冲突 | 未校验重复 name | 初始化时拒绝重复、空名称和空地址 | 防止 map 静默覆盖 |
| 类型别名不完整 | schema 声明 Byte/Word/DWord，但连接器可能返回十六进制文本 | 明确按 Uint8/Uint16/Uint32 解码 | schema 与运行行为一致 |
| 首帧丢失 | on_change 首个快照默认不发送 | `detect-change` 增加显式 `emit_first`；默认保持旧行为 | 新规则可首帧上报且不破坏旧规则 |
| MQTT 契约被污染 | 变化插件固定增加 `_change_*` 字段 | 增加 `annotate:false` | 保持 `device_code/data/time` 精确契约 |
| Excel 点表不可用 | UI 只能手填 JSON | UI 增加 JSON/Excel 导入导出，导入时做类型、重复名、范围与策略校验 | 可维护性提升 |
| 高频重复握手 | S7 每批强制建连，100ms 时会造成连接风暴和抖动 | 增加下一次需求感知 `scheduled_reuse`，默认窗口 2 秒 | 高频复用、低频自动短连接 |
| 慢扫描积压 | 同步完成后再等待一个周期，会漂移且缺少跳过观测 | 改为固定节拍 latest-only，跨过的 Tick 直接推进并计数 | 不重叠、不补跑历史扫描 |
| PLC 离线拨号风暴 | 高频自然重试仍可能每 Tick 拨号 | 连续失败阈值、指数退避和单请求 half-open | 离线压力有界 |

Excel 实现使用 `exceljs 4.4.0`。依赖审计发现其默认 `uuid 8.3.2` 命中后续版本公告，因此 UI 对 ExcelJS 单独覆盖为 `uuid 11.1.1`；往返测试与生产构建均已通过。项目原有 React 构建链仍有存量审计项，不属于本次 S7 改动，应另立前端依赖升级任务处理。

## 点位配置契约

标准 JSON 使用 snake_case：

```json
{
  "name": "temperature",
  "address": "DB1.DBD0",
  "value_type": "Float32",
  "read_write": "R",
  "scale": 0.1,
  "precision": 2,
  "range": {
    "minimum": 0,
    "maximum": 120,
    "policy": "use_default",
    "default_value": 0
  }
}
```

处理顺序固定为：PLC 原始值 → scale → precision → range。范围是闭区间。`use_default` 必须提供有限数值 `default_value`；`clamp` 截断到边界；`drop` 只移除当前点；`error` 使本轮采集失败并进入现有退避/健康上报路径。

`read_write` 是 Profile 元数据，允许 `R`、`RO`、`RW`；source 插件始终只读。写入必须经独立的 `sink-s7`，避免采集插件意外写 PLC。

## Excel 契约

工作表首行列名为：

```text
name,address,value_type,read_write,description,max_length,scale,precision,
range_min,range_max,out_of_range,default_value
```

导入还兼容参考 Profile 的 `nodeName`、`valueType`、`readWrite`、`minimum`、`maximum`、`policy`、`defaultValue` 和对应中文表头。导入结果统一转换成标准 JSON，不把 Excel 文件传给插件进程。

## 稳定性与扩展性边界

- 单个 S7 MultiRead 批量上限继续限制为 19；更多点位由 source/connector 分批。单节点点位总数限制为 5000，Excel 文件限制为 5 MB，防止错误配置耗尽内存。
- 同一个 `connect-s7` 资源可以由多个规则实例绑定，SO 进程复用、实例和状态隔离沿用引擎现有模型。
- 多个规则通过 Resource Token 汇总近期需求并共享同一物理会话；同一资源严格串行，
  单 sidecar 最多同时持有 8 个设备会话。独立规则的点位请求目前不跨请求合并。
- `detect-change` 的状态按 `device_code` 分区，并支持 TTL、最大 key 数和淘汰策略。
- 当前点位模型仅允许标量与 S7 String/WString。数组、结构体、符号地址、分组 GCD 调度留作后续兼容扩展，不纳入本次 v1.0.0 契约。
- `unload_line_alrm` 沿用用户提供的拼写，避免遥测字段静默变更；若改为 `unload_line_alarm`，应通过显式映射和下游迁移完成。

## 剩余生产风险

1. 当前配置中的数值范围采用 PLC 数据类型上下界，不代表设备工艺有效范围；生产前必须由设备方确认工程范围和默认值。
2. DB/M 地址、PLC 字节序、rack/slot 与远程端口映射需做现场读值验收。
3. `RW` 仅表示点表能力，不授权写操作；生产环境应对 `sink-s7` 单独做权限与白名单控制。
4. Excel 导入是配置便利功能，最终部署仍以经过 JSON Schema 与插件 Init 双重验证的规则 JSON 为准。
