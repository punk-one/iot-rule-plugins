# AMQP Confirmed Publish Sink

Development preview 1.0.0. One input, no output; requires opt-in `connect-amqp` write capability and exchange allowlist.

```json
{"connector_ref":"broker","exchange":"telemetry","routing_key":"machines.line-001","timeout_ms":5000,"ack_broker_receipt":false}
```

Input event payload becomes UTF-8 JSON; original Event.ID becomes AMQP message_id. Publishes are persistent with `mandatory=true` and confirm mode. Routing is static trusted configuration. Missing confirm, nack, return or timeout is an error, with no automatic retry.

Persistent-buffer input is rejected unless `ack_broker_receipt:true` explicitly selects **broker receipt** as the delivery goal. Then a confirmed routed publish emits a local ACK retaining the original rule/buffer/record/event identity. This ACK cannot stand in for a remote database commit. Use the signed business-receipt chain for that goal. No publisher message IDs or full payloads are logged.

## Delivery and deployment boundaries

This is AMQP **0-9-1**, using RabbitMQ's maintained `amqp091-go` v1.16.0 (BSD-2-Clause), not AMQP 1.0 or a universal broker adapter. Automatic library topology recovery is disabled. Sources reconnect on real demand with 1–32 s backoff; no connection-per-message. One resource owns one physical TCP/TLS session, multiple independent AMQP channels, and at most 16 consumers/8 MiB reserved content body capacity. There are at most 32 resources and one publisher in flight per resource. Configure broker queue lengths, delivery limits and consumer timeout separately. Broker QoS must be honored; the peer is a trusted configured endpoint.

An input frame guard rejects oversized frames and advertised message bodies before the library can allocate the complete message. Default message limit 64 KiB, configurable up to 1 MiB. The 8 MiB reservation bounds message body capacity, not all AMQP/library/JSON allocations. TLS verifies the server using OS trust and TLS >=1.2; plaintext credentials are allowed only on loopback. Supply secrets through Engine connector secret configuration. No automatic exchange/queue declaration or destructive topology operations; provision durable queues/exchanges/bindings externally. Consumers of the **same queue compete**; use distinct bound queues for fanout.

Consumer ACK after Engine handoff, publisher confirm, and a downstream business commit are different boundaries. The first two **do not prove a business transaction committed**. Publisher confirms with `mandatory=true` reject unroutable messages. A publish timeout has unknown outcome and is never transparently retried; callers need stable message IDs, durable replay and idempotent consumers. Connection timeout can retire the shared socket, causing all its consumers to reconnect and requeue unacknowledged messages. Stop/delete closes the socket without waiting for a graceful broker roundtrip. Closing an individual channel is bounded by one second; timeout retires the shared connection.

References: [client API](https://pkg.go.dev/github.com/rabbitmq/amqp091-go), [RabbitMQ acknowledgement/confirm semantics](https://www.rabbitmq.com/docs/confirms).

Tests use a local AMQP wire fixture for handshake, shared consumer/publisher connection, manual acknowledgement, mandatory returns and blocked protocol deadlines. These are protocol regression tests, not full RabbitMQ version/TLS interoperability or production acceptance. Both portable Linux targets must build; real ARM execution and broker fault/long-run testing remain separate. Upgrade/rollback by draining and stopping nodes, replacing the artifact and restarting; no local persistent state is migrated. Pending deliveries may redeliver and require idempotency.
