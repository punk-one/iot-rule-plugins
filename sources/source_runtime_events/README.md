# Managed node runtime event Source

`source-runtime-events` v1.0.0 is an independent passive Source, using published
SDK v1.0.0. It needs an Engine containing the managed runtime event bridge; no
new SDK methods, ports or runtime dependencies are introduced. One artifact
process hosts independent rule-node instances. Engine owns observation delivery
and cancels it before stopping each rule's dispatcher.

## Configuration

```json
{"watch_nodes":["collect","process"],"kinds":["node_error","node_health","node_status","node_done"],"device_code":"rule-runtime","max_events_per_second":10,"burst":20}
```

watch_nodes is an explicit list of 1–128 unique node IDs **in the same rule**.
The Source must be a root with exactly one output edge; fan out afterward.
It cannot observe itself, any of its downstream descendants or another
source-runtime-events node. This is validated before plugin initialization
and rule activation, preventing notifications from recursively observing their
own failures or execution. Missing/duplicate targets fail deployment. To monitor
another rule, add a Source within that rule; cross-rule/global subscriptions
are intentionally unavailable. Max 16 runtime event Sources per rule instance,
1024 subscribers per Engine, queue 64 value objects per subscriber.

Kinds:

| Kind | Meaning |
|---|---|
| node_error | Engine records a node execution/runtime error |
| node_health | Accepted non-heartbeat health state/code changes |
| node_status | Node runtime status changes |
| node_done | One Processor/Sink task callback returned success or failure |

Default kinds omit node_done to avoid observing every successful task. Heartbeats
are not emitted as repeated status events. node_done means a task callback
returned, **not** whole-rule completion, business commit, buffer drain or device
physical completion. A failed task may be retried by the Engine delivery layer;
repeated error/done facts may therefore reference the same input event. Deduplicate
or throttle notification side effects deliberately.

## Value boundary and output

```json
{"device_code":"rule-runtime","data":{"kind":"node_error","rule_id":"demo-rule","instance_id":"runtime-instance","node_id":"process","plugin_id":"javascript-transform","input_event_id":"original-event","status":"failed","code":"NODE_EXECUTION_ERROR","sequence":"42","dropped_events":"0","delivery":"best_effort"},"quality":{"runtime_event":"good"},"time":1794614400000}
```

Every observation is an independent bounded value object; it carries no business
Event pointer, business payload, input metadata, error message, health Details,
configuration or credentials. Error codes are Engine classifications;
plugin health changes use the generic PLUGIN_HEALTH_STATE classification. Use
the authenticated node inspector for original diagnostic text. IDs/status text
are capped at 128 bytes. Output sequence and cumulative drop count are decimal
strings, preserving counters beyond JavaScript integer precision. Each emitted
SDK Event gets a fresh event/trace ID; input_event_id identifies the original
task when one exists. No original business map is retained or mutated.

## Delivery and lifecycle

Observation is **best effort**, with no durable history, ACK, replay or startup
snapshot. Subscription begins after rule activation; events from startup before
that point are not replayed. Use the runtime API for current state and persisted
auditing/business receipts when those guarantees are required. Source delivery
uses the normal dispatcher and its current delivery mode, but that does not make
the earlier observation queue durable.

Publisher never blocks a task or health reporter. Each subscriber has a separate
queue, and an overflowing subscriber increments its own drop counter while other
consumers continue. Source uses a token bucket (1–100/s, burst 1–200), dropping
excess observations rather than building an unbounded backlog. Default 10/s,
burst 20. Queue/rate/dispatch losses appear in the runtime_events_dropped custom
metric and cumulative dropped_events. The next successful event after loss has
quality uncertain:runtime_event_loss; a replacement/default never makes missing
facts good. No observation is automatically retried by this pump. Downstream
delivery behavior remains the configured Engine policy.

Stop/Delete cancels the pump and subscription before stopping dispatch, waits
for in-flight observation emission and releases its queue. A stopped rule cannot
keep emitting, and stopping one instance does not cancel another. Runtime instance
deletion also closes any remaining subscription. Slow consumers cannot block the
observer or share a mutable map with worker tasks.

## Example topology and verification

```text
Source -> process -> ordinary Sink
             |
       observed by configuration
             |
source-runtime-events -> condition/notification mapping -> notification Sink
```

The diagram's observation relation is a configuration selection, **not** an
incoming DAG edge. Do not wire process into the event Source. Never include
the notification branch's own nodes in watch_nodes.

Native tests cover immutable/sanitized facts, queue overflow isolation, capacity,
concurrent cancellation/deletion and all selected kinds. Graph tests reject
feedback/missing targets and event-Source-to-event-Source monitoring. Plugin
tests reject unbounded/unknown configuration and raw diagnostic-message options.

```bash
# Engine repository
GOWORK=off go test -race ./internal/core/runtime ./internal/core/engine
# Plugins repository; build Engine with this feature first:
TEST_ENGINE_BINARY=/absolute/path/to/iot-rule-engine ./scripts/test-runtime-events-engine.sh
```

Real Engine testing runs intentionally failing and successful JavaScript tasks
and two independent rules with identical node IDs, verifies all four fact kinds,
absence of diagnostic text, feedback deployment rejection, one process per
artifact and stop isolation. Fixtures use only local HTTP capture and synthetic
diagnostics, no production equipment. Production long-run/ARM runtime acceptance
is separate. Install/rollback through the normal signed artifact path after
stopping affected rules. Old Engines cannot pump these managed observations.
AGPL-3.0-only; no new runtime dependency.
