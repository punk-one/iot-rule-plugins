# sink-mqtt

`sink-mqtt` 通过 `connector_ref` 复用 `connect-mqtt` 的物理连接，将规则事件发布到目标 MQTT Topic。

## 行为说明

- 默认发布 `event.payload` 的 JSON
- `encode.mode = event` 时发布包含 `payload`、`metadata`、`rule_id`、`source` 的事件包络
- Topic 使用插件内置的轻量模板语法，例如 `${payload.device_code}`、`${metadata.product_type}`、`${event.source}`
- 当上游是 `buffer-node` 且事件携带 `buffer_record_id`、`buffer_node_id` 时，发布成功后会回传成功 ACK，驱动缓存删除

## 配置兼容性

- 推荐使用 `target.topic`、`target.qos`、`target.retain`
- 保留 `options.qos`、`options.retain` 作为兼容字段，内部会折叠到 `target`
