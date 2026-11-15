# IEC104 read-only polling and continuous monitoring

`connect-iec104` / `source-iec104` v1.0.0 development previews, consuming published
SDK v1.0.0. Independent artifacts; one process per artifact serves all rules.
Resource mode defaults to poll (finite general-interrogation snapshot), and may
explicitly select stream (continuous monitoring demand). There is no control
Sink, application write, clock synchronization, parameter write or file transfer.

## Continuous configuration

Connector connection:

```json
{"host":"192.0.2.10","port":2404,"mode":"stream","common_address":1,"originator":0,"time_zone":"UTC","read_ioas":[100,110,500],"general_interrogation_on_connect":true,"timeout_ms":5000,"t1_ms":15000,"t3_ms":20000,"gi_timeout_ms":30000}
```

Source node:

```json
{"connector_ref":"station","device_code":"station-demo","mode":"stream","points":[{"name":"counter","ioa":110}]}
```

read_ioas is a unique explicit allowlist of 1–4096 24-bit information-object
addresses. Each Source selects 1–1024 unique addresses/names within that list;
names are <=128 bytes. Multiple Sources may label the same allowed IOA
differently without sharing mutable output. One resource represents one station
endpoint/common address/originator/timezone, not arbitrary routed stations.
Different resources do not automatically share a physical connection.

First Source opens the device TCP session, STARTDT is confirmed, and optional
initial general interrogation is sent **once per physical session**, not per
Source. Sources then share future received monitoring ASDUs. Last Source stop,
Resource Stop/Delete, protocol failure or device STOPDT closes the socket.
Stream resources explicitly reject poll/application-write Execute calls, so a
second reader cannot steal monitoring bytes. Existing mode=poll remains a
finite per-operation snapshot path; use it when a complete selected-point result
per scan is required. Poll interval_ms defaults to 60000 and is unused in stream
mode. Source/Connector modes must match.

## Output is a delta, not a snapshot

```json
{"device_code":"station-demo","data":{"counter":123},"quality":{"counter":"good"},"protocol_details":{"points":{"counter":{"ioa":110,"type_id":11,"quality_raw":0,"source_time":"","counter_flags":0,"transient":false}},"cause":1,"delivery":"best_effort","snapshot":false},"time":1794614400000}
```

Each update contains only selected IOAs present in that received ASDU. An absent
point is not fabricated as zero, null, a stale cached success or a missing-point
failure. Late subscribers receive **new deltas**; they do not replay an earlier
GI or cached values. Initial GI and subsequent periodic/spontaneous/background
monitoring may all produce delta events. There is no atomic all-point snapshot,
automatic new GI per late subscriber, durable event history or reconnect replay.
After reconnect, a new optional GI is a new device request, not historical recovery.
Use downstream keyed state or poll mode for a full current point table, with an
explicit freshness policy.

Supported monitoring Type IDs: 1/3 (single/double point), 5 (step position),
7 (32-bit bit-string), 9/11/13 (normalized/scaled/short float), 15 (integrated
total), and their CP56Time2a counterparts 30/31/32/33/34/35/36/37. CP24Time2a,
protection/event extensions and other ASDU families are not claimed. Unknown
types, malformed framing, duplicate IOAs, wrong common address/originator or
unexpected sequence/cause fail the session instead of silently misdecoding.
End-of-initialization and GI confirmations are validated as control information.

Device invalid/blocked/substituted/non-topical/overflow bits remain explicit bad
quality; transient step position and adjusted counters are uncertain. Counter
flags are retained separately. Nonfinite floats become null/bad quality.
CP56 requires a configured station timezone; invalid fields/reserved bits,
weekday or DST inconsistency produce bad:timestamp, not a guessed UTC value.
Missing CP56 timestamps stay empty; event time is local receive time in ms.

## Protocol and resource bounds

Strict 15-bit receive sequence and send-ack window checks; a regressing/future
ACK or out-of-order I frame closes the session. Only initial GI uses an I-frame
send window, with at most one unacknowledged command. Every valid received I
frame is acknowledged immediately (W=1), fulfilling receive acknowledgement
without a delayed t2 window. Idle sessions send TESTFR; its confirmation and
unacknowledged I frames obey t1_ms. No confirmation closes the connection.
t3_ms must exceed t1_ms by at least 50 ms. Initial GI has an independent
gi_timeout_ms deadline (1000–120000), so traffic cannot keep an unfinished GI
open forever. Partial headers/bodies timing out fail, rather than being mistaken
for an idle connection and losing framing. APDU length is bounded to 253 bytes.

Connection/initialization and each control write obey timeout_ms. At most 64
resources, eight physical sessions/operations per package, 16 Source streams
per resource, queue eight frames/consumer, credit window eight, 4 MiB total
queued bytes/resource, decoded/projected frame <=64 KiB. Slow consumers fail
and clear their own queue. Errors close the physical socket before notifying
waiting consumers. Backoff is bounded 1–32 seconds and only real Source demand
reopens a connection; no background permanent reconnect thread. Each plugin
contains its own implementation; no plugin imports another plugin's runtime code.

The stream ACK releases local credit **after Engine handoff**. It is not an IEC
application/business commit, and cannot replay frames. Session failure/overflow
means possible loss and changes Source/Resource health. Source data maps and
metadata are independent; original byte buffers are never shared with consumers.
Healthy idle demand is not a failed/disconnected resource. No TLS/IEC62351 or
cryptographic station authentication is included: use a trusted isolated OT
network or an externally managed secure tunnel. This preview is not a full IEC
conformance/certification claim.

## Verification

Normal module tests cover aliases on one shared session, copied data, slow
consumer failure, malformed sequences, send-ack bounds, TESTFR confirmation and
timeout, last-demand close, backoff and added type/quality boundaries. Source
tests cover delta shape, explicit quality, no invented unchanged values, handoff
before local ACK and no ACK on failed handoff. Poll tests remain enabled.

Independent interoperability uses lib60870 v2.4.1, commit
7a388e3e133999e1ca77ba7521d55d074b7cd2bc, built **outside** this repo, never linked
into the Go plugin or bundled in its release. Native service tests verify GI,
bit-string and continuing periodic data. Real Engine testing uses two rules,
one artifact process each and **one ESTABLISHED device TCP socket**, then checks
stop isolation and last-Source release. Everything runs in a private Linux
user/network namespace, no real station/control command.

```bash
# Build an independently obtained lib60870 2.4.1 cs104_server example first.
export TEST_IEC104_SERVER=/absolute/path/to/simple_server
unshare -Urn -- bash -c 'ip link set lo up; cd connectors/connect_iec104; TEST_IEC104_ISOLATED=1 GOWORK=off go test -race -count=1 -v .'
TEST_ENGINE_BINARY=/absolute/path/to/iot-rule-engine ./scripts/test-iec104-engine.sh
```

Missing independent test prerequisites skip explicitly; they are not hardware
acceptance. No physical station, ARM runtime, 72-hour or all-vendor acceptance
is inferred. Stop dependent rules before changing mode/endpoint/allowlist, then
install/rollback through signed artifact management. Keep matching Connector
and Source configurations. AGPL-3.0-only; production remains pure Go with no
new protocol/native dependency.

Protocol timing reference: [lib60870 primary implementation user guide](https://github.com/mz-automation/lib60870/blob/v2.4.1/user_guide.adoc).
