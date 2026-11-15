# Managed signed Webhook ingress

Development preview; requires an Engine build containing managed webhook
support and the independent `source-webhook` v1.0.0 artifact. SDK remains v1.0.0.
The Source declares its configuration and lifecycle. Manager owns HTTP routing,
authentication, request bounds and admission; no rule/plugin opens a new port.
Unified local Executor mode is required; remote legacy Executor fallback is
not implemented for this entry point.

## Configure a rule

Install source-webhook using the normal signed artifact path, then create a
Source node with this configuration:

```json
{"route_key":"telemetry","secret_env":"IOT_RULE_WEBHOOK_DEMO","max_body_bytes":262144,"qps":10,"burst":20}
```

The node must have no incoming edge and exactly **one** outgoing edge. Fan out
after that first processing node. This keeps ingress admission from partially
accepting several initial branches. The route is derived, not freely mounted:

`POST /api/v1/hooks/<rule-id>/<node-id>/<route_key>`

IDs and route_key use 1–64 ASCII letters/digits/dot/underscore/hyphen, starting
with a letter or digit. Only a fully running, enabled rule with the matching
source-webhook node registers a usable target. Disabled/deleted/failed rules
return 404. Re-deployment revalidates the current configuration fingerprint,
so an authenticated request cannot switch target configuration between lookup
and dispatch. No broadcasting to other source/root nodes.

Set IOT_RULE_WEBHOOK_DEMO in the **Engine** service environment before startup.
Its value must be 32–256 bytes; generate at least 32 cryptographically random
bytes (hex encoding is convenient). Source configuration contains only the
variable name; neither SQLite rule JSON nor the plugin receives the signing
key. Only names matching IOT_RULE_WEBHOOK_[A-Z0-9_]+ are accepted. Do not commit
service EnvironmentFiles or keys; protect files with mode 0600. This mechanism
does not add a general secret manager or persist environment values to SQLite.
Missing/invalid keys fail closed with 503. Rotate the service key and restart
the Engine; old signatures must then fail.

## Request contract

HTTPS is required on untrusted networks; terminate it at the normal trusted
reverse proxy. Request Content-Type must be application/json (optional charset
is accepted). Content-Encoding must be absent or identity. Body is a JSON object
used directly as SDK Event.Payload. Headers are **not** copied into metadata.
Query parameters, escaped/ambiguous paths, compressed bodies, duplicate JSON
keys and trailing JSON values are rejected. Depth <=32, item budget <=65536,
max_body_bytes 1024–1048576 (default 262144). Integers/numbers outside JavaScript's
safe integer range become exact textual literals; nonfinite/oversized numeric
literals fail. Apply an explicit schema/normalize node downstream for business
requirements; a valid JSON object alone does not validate its business schema.

Headers, each exactly once:

- X-Iot-Timestamp: canonical decimal Unix **seconds**, within +/-60 seconds.
- X-Iot-Request-Id: a unique 16–128 character ASCII [A-Za-z0-9_-] nonce.
- X-Iot-Signature: lowercase hex HMAC-SHA256 of the exact concatenation below,
  using the raw UTF-8 environment-variable value as key.

```text
timestamp + "\n" + request_id + "\n" + "POST" + "\n" + exact_path + "\n" + exact_body_bytes
```

Independent Python example (environment values are supplied by the caller):

```python
import hashlib, hmac, json, os, time, uuid, urllib.request
path = '/api/v1/hooks/demo-rule/webhook/telemetry'
body = json.dumps({'device_code':'demo','data':{'temperature':22.75},
                   'time':int(time.time()*1000)}, separators=(',',':')).encode()
stamp, nonce = str(int(time.time())), uuid.uuid4().hex
message = (stamp+'\n'+nonce+'\nPOST\n'+path+'\n').encode()+body
signature = hmac.new(os.environ['IOT_RULE_WEBHOOK_DEMO'].encode(), message,
                     hashlib.sha256).hexdigest()
request = urllib.request.Request(os.environ['ENGINE_BASE_URL']+path, body,
    {'Content-Type':'application/json','X-Iot-Timestamp':stamp,
     'X-Iot-Request-Id':nonce,'X-Iot-Signature':signature}, method='POST')
with urllib.request.urlopen(request, timeout=10) as response:
    print(response.status, response.read().decode())
```

Webhook callers receive **no Manager/admin token**. Only the exact registered
Webhook route owns this separate authentication. A URL prefix never bypasses
Manager authentication; plugin/config/user routes retain existing permissions.
Each route should use a different key. If keys are intentionally shared, a
holder can sign for every route using that key; path binding prevents moving
an already signed request to a different route, not access by another key holder.

## Response and reliability

202 returns accepted=true, event_id, trace_id, scope=engine_dispatch and
business_completed=false. The request does not wait for downstream nodes and
cannot choose an HTTP response node/body/status. Acceptance follows the
configured Engine delivery mode: reliable mode persists the initial edge;
best_effort only admits work to bounded local scheduling. Neither means a
business transaction or physical device action completed. Use the existing
transaction-receipt/business-ack chain for that boundary.

401: invalid/stale signature; 404: no active matching target; 400/413/415:
body/format error; 429: per-route rate or global concurrency capacity; 409:
replayed nonce or replay cache full; 503: key/configuration/dispatch failure.
Rate is configurable 1–100/s, burst 1–200. At most 32 concurrent Webhook requests,
1024 active route buckets and 16384 unexpired nonce hashes are retained.
Nonce hashes are kept for two minutes; expired entries are reclaimed. Replay
state is **process-local and volatile**. A restart can accept a still-valid
signed request again; do not treat it as durable idempotency. Use persistent
command-guard or business-key transactions for actions. Once a nonce is
reserved it remains consumed even if dispatch fails, so failure/timeout can
mean an unknown outcome. Reconcile before retry; creating a new nonce is not
proof that the earlier request did nothing. HTTP server read limits/timeouts
also apply. No background unbounded request queue or per-request worker leak.

## Verification

Engine ingress tests exercise signatures, timestamps, duplicate headers/keys,
large integers, body/depth bounds, per-route refill, replay races, concurrency
limits and missing keys. Manager tests reject prefix-based authentication
bypasses. Native Source tests reject plaintext key fields and bad config.

```bash
GOWORK=off go test -race ./... # run from Engine repository
# From iot-rule-plugins; Engine binary must contain this feature:
TEST_ENGINE_BINARY=/absolute/path/to/iot-rule-engine ./scripts/test-webhook-engine.sh
```

The real Engine test signs with independent Python hashlib, captures downstream
HTTP payloads, verifies exact large integers, two rules/one artifact process,
replay/wrong-key/size rejection, rule disable, same-ID rule deletion/recreation
and route-generation replacement. It uses temporary local state and generated
keys only. It does not certify production TLS, multi-node coordination, 72-hour
load or durable webhook inbox semantics. Stop affected rules before removing
the Source artifact; old Engines cannot provide this managed route.
