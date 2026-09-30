# Redis Relationship Mapping

通用 Redis Hash 关系映射处理器。字段路径均相对于 `event.payload`，插件通过绑定的
Redis Connector 使用 `HGET` 查询单个关系，不依赖 MES、设备或具体业务模型。

```json
{
  "connector_ref": "redis-main",
  "default_hash_key": "relations:product-name",
  "timeout_ms": 3000,
  "mappings": [
    {
      "source_path": "data.product_id",
      "target_path": "data.product_name",
      "on_missing": "default",
      "default_value": "unknown"
    }
  ]
}
```

`on_missing` 支持 `skip`、`default` 和 `error`。每个映射项可以用自己的 `hash_key`
覆盖 `default_hash_key`。
