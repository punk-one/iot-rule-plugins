# source-mqtt

`source-mqtt` 直连外部 MQTT Broker 订阅消息，并将解码后的 MQTT payload 原样发布到规则引擎。

## 行为说明

- `json` / `auto`：解析 JSON 后直接作为事件 `payload`
- `text`：输出 `{"data":"<原始文本>"}`
- `binary`：输出 `{"raw":"<base64>"}`
- 不做宽表转换，不强制改写为 `device_code + data`
- 不会根据 payload 中的 `time` 字段改写事件时间戳
- 可选地从 topic 提取片段，按配置写入 `payload.*` 或 `metadata.*`

## 适用场景

- 原始 MQTT 遥测接入
- 需要保留上游消息结构的链路转发
- 由下游 sink 或 processor 自行决定如何解析 payload

## 提取配置

`process.extract.fields` 支持把 topic 片段写入自定义事件字段：

```json
{
  "connector_ref": "mqtt-1",
  "process": {
    "subscribe": {
      "topics": ["factory/+/+/telemetry"]
    },
    "decode": {
      "format": "json"
    },
    "extract": {
      "fields": [
        {
          "from": "topic[2]",
          "to": "payload.device_code"
        },
        {
          "from": "topic[1]",
          "to": "metadata.product_type"
        },
        {
          "from": "topic[0]",
          "to": "payload.tags.site"
        }
      ]
    }
  }
}
```

约束：

- `from` 目前仅支持 `topic[n]`
- `to` 支持：
  - `payload.xxx`
  - `payload.xxx.yyy`
  - `metadata.xxx`
- 已存在的目标字段不会被覆盖
- `metadata` 仅支持一级 key

## 说明

如果你需要把原始 MQTT 事件进一步转换为项目约定结构，建议在下游 processor 中完成，而不是把字段语义固化在 `source-mqtt` 里。
