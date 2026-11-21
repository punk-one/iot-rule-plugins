# PostgreSQL Sink

`sink-relational-postgres` 1.0.3 的单行和批量模式均可显式配置 `output.conflict_key_columns`。

```json
{"connector_ref":"telemetry-postgres","output":{"table":"public.telemetry","conflict_key_columns":["device_id","sample_id"]}}
```

目标表需事先具有对应列的 PostgreSQL 主键、唯一约束或可推断的唯一索引。事件必须在顶层提供这些非空键值。写入生成明确键范围的 `ON CONFLICT ... DO NOTHING`，同键重复写保留原行。其他数据库约束错误仍返回错误。省略或传入空数组时继续使用普通 INSERT。该选项不会创建索引、自动重试或增加业务回执保证。
