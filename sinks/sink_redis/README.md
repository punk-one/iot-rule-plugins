# sink-redis

`sink-redis` 将输入事件写入 Redis Stream。
当上游是 `buffer-node` 时，写入成功后还会通过规则引擎 ACK 总线回传成功 ACK，驱动缓存删除。

## 行为说明

- 底层通过 `connect-redis` 执行 `XADD`
- `stream` 模式固定将 `event.Payload` 序列化后写入 `payload` 字段
- 当 `target.mode=hash` 时，通过 `encode.field_mappings` 把事件路径映射成 Redis Hash 字段
- 默认额外附带：
  - `timestamp`
  - `event_id`
  - `rule_id`
  - `source`
  - `trace_id`
  - `meta_*` 元数据字段
- 当 `event.Metadata` 中包含 `buffer_record_id` 和 `buffer_node_id` 时，Redis 写入成功后发送 `AckMessage{status: "success"}`

## 配置示例

```json
{
  "connector_ref": "redis-main",
  "target": {
    "stream": "rules:telemetry"
  },
  "encode": {
    "include_metadata": true,
    "include_context": true
  },
  "options": {
    "write_timeout_sec": 10,
    "max_len": 10000,
    "approx": true
  }
}
```

## Hash 模式示例

```json
{
  "connector_ref": "redis-main",
  "target": {
    "mode": "hash",
    "key_prefix": "iot:device:status:",
    "key_field": "payload.device_code"
  },
  "encode": {
    "field_mappings": [
      {"field": "device_code", "path": "payload.device_code"},
      {"field": "online", "path": "payload.data.online"},
      {"field": "connection_state", "path": "payload.data.connection_state"},
      {"field": "last_seen_at", "path": "payload.data.last_seen_at"},
      {"field": "updated_at", "path": "payload.time"}
    ],
    "include_metadata": false,
    "include_context": false
  }
}
```

## 上游事件 payload 示例

```json
{
  "device_code": "line-01",
  "data": {
    "temperature": 42.5,
    "pressure": 0.86
  }
}
```

假设事件还带有：

```json
{
  "metadata": {
    "product_type": "compressor",
    "site": "hz-1"
  },
  "rule_id": "rule-telemetry",
  "source": "normalize-node"
}
```

则写入 Redis Stream 的 fields 近似为：

```json
{
  "payload": "{\"device_code\":\"line-01\",\"data\":{\"temperature\":42.5,\"pressure\":0.86}}",
  "timestamp": "2026-04-08T10:00:00Z",
  "rule_id": "rule-telemetry",
  "source": "normalize-node",
  "meta_product_type": "compressor",
  "meta_site": "hz-1"
}
```

## 适用场景

- 把规则链结果推送到 Redis Stream，供外部服务异步消费
- 与 `buffer-node` 配合，在 Redis 写入失败时继续保留并重试
- 作为规则引擎与下游服务之间的轻量级解耦出口
