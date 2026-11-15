# OPC UA Source

Development preview 1.0.0. One output, no input; binds `connect-opcua` read capability. Configure `mode:"poll"` (default), `"subscription"` or `"events"`.

```json
{"connector_ref":"ua","device_code":"machine-001","mode":"subscription","interval_ms":100,"timeout_ms":5000,"buffer_messages":16,"points":[{"name":"temperature","node_id":"s=temperature","namespace_uri":"urn:example:machine"}]}
```

Poll mode uses one batched read, 1..1024 points, interval 50 ms..24 h, skipped overdue ticks and failure backoff. Failed/missing/incomplete results emit per-point bad quality, never good zero. Subscription mode allows 1..256 points, interval 50..60000 ms. Output `{device_code,data:{temperature:12.5},quality:{temperature:"good"},source_time:{temperature:1794614400000},partial:true,time:...}`. Only changed points appear; combine with a bounded stateful rule if a complete snapshot is needed. Use normalize-points separately for engineering scaling/ranges.

```json
{"connector_ref":"ua","device_code":"machine-001","mode":"events","event_node_id":"i=2253","interval_ms":100,"buffer_messages":16}
```

Event output `{device_code,data:{events:[{EventId:"base64",Message:{locale:"en",text:"alarm"},Severity:500,...}]},time:...}`. Events do not accept `points`. Invalid/oversized/overflowed native results fail the consumer; the loop recreates it after bounded backoff. Engine handoff must succeed before local credit ACK. No transparent durable event replay or business acknowledgement.

## Security, lifecycle and delivery

Defaults: Basic256Sha256 SignAndEncrypt, client certificate/private key, exact pinned server certificate. Both certificates must be currently valid. RSA private keys support unencrypted PKCS1/PKCS8 PEM or DER by **content**, independent of `.key`/`.pem` suffix; 2048–8192 bits, bounded regular file <=64 KiB, no group/other permissions, matching client public key. Certificates <=1 MiB. None is explicit anonymous-only test mode. No trust-on-first-use or automatic security downgrade. Configure trusted server endpoints; native message negotiation is capped at 4 MiB/64 chunks, emitted notifications at 1 MiB.

One resource owns a serialized session for read/write/browse and native subscriptions. Polling reuses it for `session_reuse_ms` (default 2000) then closes idle; **active subscriptions keep it alive**. At most 16 independent subscriptions/resource, 256 points/subscription, 1 MiB queued result bytes/subscription and 1..32 bounded notifications. One slow/overflowed subscriber fails explicitly and continues draining native notifications until closed, so it cannot block other subscribers. Last subscriber closes its session; stop/delete closes all consumers. Socket/session error invalidates consumers. The library can silently pause Publish on EOF/session loss, so the resource checks native connection state every 100 ms and verifies a session with Server.CurrentTime every 5 s while subscriptions exist. The probe has a <=1 s deadline; it does not create a new session. Source reconnects/recreates on real subscription demand with 1–32 s backoff.

`namespace_uri` resolves the node identifier against the server NamespaceArray. When provided, use an unqualified/namespace-zero node_id (`s=temperature`, `i=17`); conflicting numeric namespaces are rejected. Indexes are cached **only for one session generation**, then rebuilt after session replacement. Numeric namespace addresses without a URI remain valid for fixed servers.

Live data/event subscriptions are **best_effort**, not durable message queues. UA Publish acknowledgement, SDK credit return and Engine handoff do not prove business completion. Overflow/uncertain/bad statuses never become good values. Server sampling/queue revision can discard transitions; bounded queue failures and reconnection can lose notifications. Data-change output is a **partial update**, not a complete current snapshot. Event recovery/history/alarms acknowledgement and Method Call are not implemented; use server history interfaces or periodic full polling when the application needs reconciliation.

Verification includes race tests for two sources sharing one session, independent stop, overflow/draining, event filters/encoding and namespace-generation changes. Independent asyncua 1.1.8 loopback integration exercises real Basic256Sha256 secure read/write, data and BaseEventType subscriptions, wrong-pin rejection, server process kill, reconstruction and changed namespace indexes. This is one protocol-stack interoperability result, not certification of all vendor servers, usernames/PKI policies, ARM runtime or long-run operation. No real PLC writes are made. Upgrade/rollback by stopping bound nodes, replacing the artifact and rebuilding subscriptions; no local state migration, historical catch-up or replay is promised.

Optional independent-stack test:

```sh
python3 -m venv /tmp/iot-opcua-test
/tmp/iot-opcua-test/bin/pip install -r testdata/requirements-interop.txt
TEST_OPCUA_PYTHON=/tmp/iot-opcua-test/bin/python go test -race -count=1 .
```

Run from the Connector directory. The optional interpreter is test-only; plugin binaries do not depend on Python or ship asyncua. Default unit tests explicitly skip this interoperability test when the environment variable is absent.
