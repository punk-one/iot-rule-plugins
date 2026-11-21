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

## 查询边界、连接池与 TTL

- `metadata/info` 返回 `pool` 快照。尚未使用的资源返回 `open:false`，查看元数据不会发起 Redis 连接。已打开的资源还返回连接数、空闲数、命中、缺失和超时次数。`pool_size` 限制活动连接，`min_idle_conns` 不可超过此值。
- 单次读取最多 1024 条 Redis Stream 消息，阻塞读取配置不超过 10000 毫秒。每个资源最多 256 个流会话，每会话保留最多 4096 条及 4 MiB 原始消息。达到硬上限时暂停继续读取，确认或终止重排队后可继续。旧轮询和 Hash 查询的序列化响应也限制为 4 MiB，`hash_getall` 最多交付 4096 个字段。这个上限在 Connector 回复 RPC 前校验，Redis 驱动仍会读入 HGETALL 的完整结果，单字段查询优先使用 `hash_get`。
- `write/hash_set` 支持 `ttl_seconds`。正数使用 `WATCH` 和 `TYPE` 校验目标键，在一笔 `MULTI/EXEC` 事务中写入字段并更新键的过期时间。访问控制需允许这些命令及 `UNWATCH`。并发修改导致事务放弃时返回错误。省略或为零时保留原 TTL。另支持 `read/ttl`、`write/expire`、`write/persist` 和 `write/del`。
- 客户端不自动重做写操作。Redis Stream 通过 `XADD` 追加，调用方重发仍可能生成重复条目。规则可保留稳定的 `event_id` 供下游去重。流会话确认只表示连接器的本地交付进度。
