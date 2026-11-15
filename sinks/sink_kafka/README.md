# Kafka Publish Sink

```json
{"connector_ref":"broker","topic":"telemetry","timeout_ms":5000,"ack_broker_receipt":false}
```

One input, no output. JSON-encodes the incoming Payload (<=1MiB), uses stable Event.ID as Kafka record key and asks the Connector for an all-ISR broker receipt. No event mutation, implicit application deduplication or transparent record retry. More restrictive resource size/permissions still apply. A failed deadline has unknown write outcome: preserve the same application ID when a durable buffer deliberately replays it.

Buffered input requires explicit ack_broker_receipt=true and original buffer IDs. Only a positive receipt containing a nonnegative partition and decimal offset releases that original buffer record; malformed/failed/unknown results do not ACK. Broker receipt does not prove consumer processing or database commit. Use the signed application-receipt pattern for those goals rather than selecting broker ACK by accident.

New independent SDK v1.0.0 module, plugin version1.0.0 development preview. Requires the Go1.27.4 security build baseline. franz-go v1.22.3/kmsg v1.15.1 are BSD-3-Clause, pure Go; no JVM, Kafka server, vendor native library or CGO runtime. kfake is a separate pinned test-only module; it is not imported by production implementation or linked into the plugin artifact. Preserve its test notices and the production codec/SASL notices in the SBOM.

Tests use actual Kafka wire protocol against upstream kfake, including consumers/groups, offset replay, shared-resource isolation, TLS plus PLAIN/SCRAM-SHA-256/SCRAM-SHA-512, rejection of an untrusted CA and a real gzip expansion beyond the configured budget. This does not certify a real Apache Kafka deployment, every broker version, ACL topology, failover, ARM runtime or long-duration operation. No exactly-once transaction or business commit guarantee.
