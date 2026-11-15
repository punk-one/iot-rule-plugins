# Kafka Group Source

```json
{"connector_ref":"broker","device_code":"gateway-a","topic":"telemetry","group_id":"iot-rule.gateway-a.telemetry","reset":"earliest","decode":"json"}
```

No input, one output. Use an explicit stable group ID per intended projection and gateway. Reusing it deliberately load-balances; use different IDs when each rule must receive every event. The resource must allow that topic and group prefix. Do not copy the same group ID blindly to multiple gateways.

JSON mode emits `{device_code,data:<decoded object or {value:scalar/array}>,time:<Kafka record timestamp in ms, receive time fallback>}`. Raw mode emits data.raw_base64. JSON body<=1MiB, depth<=64, per-container items<=4096; unsafe numeric magnitudes are preserved as decimal text. Internal metadata includes protocol, topic, group_id, partition, offset and key_base64. Event.ID hashes resource/group/topic/partition/offset for stable replay identity, TraceID uses Event.ID. Same Kafka key at different offsets remains a different log record; this does not suppress duplicate application payloads.

Source publishes to Engine, then commits the pending Kafka offset only after successful handoff. Failed decode/handoff/commit closes the stream without advancing further, with1–32s reopen backoff. Invalid messages can block a partition until repaired; no automatic discard. Stop closes this node's stream and joins its loop. ACK denotes Engine handoff, **not downstream durable buffer persistence, database transaction or business execution**. A crash after handoff/before commit can replay; a crash after offset commit/before downstream persistence can lose an event unless the deployment provides a coordinated persistent inbox. Do not treat the `reliable` label alone as proof of atomicity at that boundary.

New independent SDK v1.0.0 module, plugin version1.0.0 development preview. Requires the Go1.27.4 security build baseline. franz-go v1.22.3/kmsg v1.15.1 are BSD-3-Clause, pure Go; no JVM, Kafka server, vendor native library or CGO runtime. kfake is a separate pinned test-only module; it is not imported by production implementation or linked into the plugin artifact. Preserve its test notices and the production codec/SASL notices in the SBOM.

Tests use actual Kafka wire protocol against upstream kfake, including consumers/groups, offset replay, shared-resource isolation, TLS plus PLAIN/SCRAM-SHA-256/SCRAM-SHA-512, rejection of an untrusted CA and a real gzip expansion beyond the configured budget. This does not certify a real Apache Kafka deployment, every broker version, ACL topology, failover, ARM runtime or long-duration operation. No exactly-once transaction or business commit guarantee.
