# connect-redis

`connect-redis` 提供一个可复用的 Redis 连接资源，给 `source-redis` 和 `sink-redis` 共享使用。

当前实现基于 `github.com/redis/go-redis/v9`，读写模型使用 **Redis Stream**：

- 读：`XREAD`
- 写：`XADD`
- 健康检查：`PING`

## Connector Resource 配置示例

```json
{
  "id": "redis-main",
  "plugin_id": "connect-redis",
  "enabled": true,
  "connection": {
    "addr": "127.0.0.1:6379",
    "db": 0,
    "username": "",
    "password": "",
    "dial_timeout_sec": 5,
    "read_timeout_sec": 5,
    "write_timeout_sec": 5,
    "pool_size": 8,
    "min_idle_conns": 0,
    "conn_max_lifetime_sec": 300
  }
}
```

## 支持能力

- `metadata/info`
- `metadata/ping`
- `read/stream`
- `write/stream_add`

## 读请求 payload 示例

```json
{
  "stream": "rules:events",
  "start_id": "$",
  "count": 1,
  "block_timeout_ms": 1000
}
```

说明：

- `stream`：Redis Stream 名称
- `start_id`：初始游标；为空时默认 `$`
- `count`：单次最多返回消息数
- `block_timeout_ms`：阻塞读取超时
- 同一个 `resource_token` 会维护独立游标，适合 source 节点长期轮询

## 写请求 payload 示例

```json
{
  "stream": "rules:events",
  "max_len": 10000,
  "approx": true,
  "values": {
    "payload": "{\"value\":42}",
    "timestamp": "2026-04-08T10:00:00Z"
  }
}
```

说明：

- `stream`：目标 Redis Stream
- `max_len`：可选，写入时附带 `MAXLEN`
- `approx`：是否使用近似裁剪 `~`
- `values`：写入的 field/value map；复杂值会被序列化为 JSON 字符串

## 适用场景

- 规则引擎和外部系统之间通过 Redis Stream 做解耦
- 多个 source/sink 节点共享同一个 Redis 连接资源
- 需要在规则链路里引入轻量级消息缓冲或转发层
