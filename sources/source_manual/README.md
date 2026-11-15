# Controlled manual Source

`source-manual` v1.0.0 is an independent, passive Source. It emits no scheduled
data, opens no listening port and accepts no ordinary OnEvent input. It uses
published SDK v1.0.0 and requires an Engine build containing managed manual
ingress. A single artifact process serves independent Source instances.

Configuration:

```json
{"allow_live_trigger":false,"max_body_bytes":262144,"qps":2,"burst":4}
```

The default is disabled. Enable allow_live_trigger explicitly before deploying
the rule. It must be a root with exactly one outgoing edge; fan out downstream.
max_body_bytes is 1024–1048576; qps 1–10; burst 1–20. Stop the rule before editing
these values. Only the current running rule/node/configuration may receive a
trigger. Disable/Delete or configuration replacement invalidates the old target.

## Manager API

```text
POST /api/v1/rules/<rule-id>/nodes/<node-id>/trigger
Authorization: Bearer <administrator-session-or-explicit-admin-service-token>
Content-Type: application/json
```

```json
{"confirm_live":true,"payload":{"device_code":"demo-device","data":{"temperature":22.75},"time":1794614400000}}
```

This is **live execution** of the deployed rule and its downstream Sinks. It is
not a simulated preview, and the request cannot bypass a command-guard or
Connector allowlist. confirm_live=true and an object payload are both mandatory;
unknown envelope fields fail. Anonymous callers receive 401 and ordinary users
403. Webhook signing keys never authorize this endpoint. No manual injection
into arbitrary PLC/MQTT/other Source nodes is permitted; use a configured
source-manual root. IDs use the same bounded components as managed Webhooks.

202 returns accepted, event_id, trace_id, scope=engine_dispatch, dry_run=false
and business_completed=false. Acceptance follows the Engine delivery mode and
does not wait for business or device completion. A timeout/error can leave an
unknown outcome: inspect trace/business state before retrying. Repeated manual
requests create distinct events; physical commands must still carry a durable
command_id, expiry and the normal command-guard path. Never use a manual repeat
button as an exactly-once command mechanism.

Body format/depth/item/number limits match managed Webhooks, with no compressed
bodies or duplicate keys. Large numbers outside JavaScript's safe integer range
are preserved as exact text by the Engine. At most 32 concurrent manual requests
and 1024 bounded rate buckets; no unbounded queue or automatic retries.

## UI

Open the running manual Source's node detail, then the **手动触发** tab. Admins
can enter a JSON object, explicitly confirm execution of the current rule and
its Sinks, and press **执行一次**. A Source with allow_live_trigger=false exposes
no executable control. Ordinary users only see the privilege explanation.
Submit locks immediately until completion; the confirmation resets afterward.
UI sends exact original JSON bytes inside the envelope, preserving large numeric
literals without JSON.parse/stringify rounding. Response displays dispatch
acceptance and event ID, not an invented business success. Server authorization
and validation remain authoritative even if the UI is bypassed.

## Tests and operations

Source unit tests cover disabled default, configuration limits and direct-input
rejection. Engine tests cover explicit live confirmation, object/size/rate
bounds and Manager authentication isolation. The real local Engine test creates
enabled/disabled manual Sources, verifies admin execution/downstream capture,
anonymous denial and non-admin denial, alongside signed Webhook rules. Both
families share one process per artifact and independent rule instances.

```bash
# In iot-rule-plugins, using an Engine containing both managed ingress types:
TEST_ENGINE_BINARY=/absolute/path/to/iot-rule-engine ./scripts/test-webhook-engine.sh
```

UI tests cover double-click suppression, unknown-outcome display, permissions,
disabled sources and exact large-number transport. Native/real Engine fixtures
use disposable local state and HTTP capture only, never production equipment.
Install/rollback through the normal signed artifact path after stopping affected
rules. Old Engines cannot serve this endpoint. AGPL-3.0-only; no new dependency.
