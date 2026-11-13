# Persistent Buffer

Independent SDK v1 processor. Stores incoming events in a bounded bbolt queue
before returning success, then emits them at a configurable rate until one
designated downstream sink confirms success through `PublishAck`.

```json
{
  "ack_node_id": "mqtt-uplink",
  "retry_ms": 5000,
  "rate_per_second": 100,
  "storage": {"max_records": 100000, "max_bytes": 268435456}
}
```

Connect `buffer-node → sink-mqtt` with MQTT QoS 1 or 2. The engine injects the
internal ACK bus configuration and storage base path. Each rule/node gets a
separate file derived from that base, including after a process restart.

Input: an SDK event. Output: a cloned event with stable `event_id` in its payload
and `buffer_record_id`/`buffer_node_id` in its metadata. Preserve those fields on
the path to the designated sink. The event's original collection time is kept.
Pending duplicate event IDs are idempotent. Completion is at least once: a crash
after publish but before confirmation may replay the same ID. The receiver must
enforce business idempotence.

The queue rejects new records when its count or logical byte limit is reached.
An ordered due-time index avoids scanning the entire queue on every replay tick.
`max_bytes` bounds logical record bytes, not the physical bbolt file size; allow
space for indexes and copy-on-write pages and monitor the filesystem. Disk-full
errors propagate to the upstream reliable transport.

An MQTT PUBACK confirms broker receipt, not database commit. End-to-end business
confirmation requires a separate application receipt chain. Do not claim exactly
once delivery or unbounded offline retention.

Build and test independently with `GOWORK=off go test -race .` and
`GOWORK=off go build .`. This plugin does not change the SDK contract.
