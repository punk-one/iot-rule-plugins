# source-redis

`source-redis` 通过 `connector_ref` 复用 `connect-redis` 的连接资源，使用流式会话消费 Redis Stream，并将消息解码为规则事件。

默认会优先解析 Redis Stream 字段中的 `payload` JSON；若不存在或解析失败，可切换为 `values` 模式直接输出字段映射。
