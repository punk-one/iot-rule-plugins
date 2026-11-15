# MongoDB Poll Source

```json
{"connector_ref":"database","collection":"events","device_code":"server-001","filter":{"enabled":true},"sort":"_id","limit":100,"timeout_ms":5000,"interval_ms":5000}
```

No input, one output. Runs the configured bounded majority-read snapshot. Emits `{device_code,data:{documents:[canonical Extended JSON objects],count:N},time:<milliseconds>}` with new Event.ID/TraceID per snapshot. This is polling, not CDC or incremental watermarking: unchanged documents repeat; use detect-change/explicit business deduplication if required. More than limit rows fails instead of silently omitting data.

Interval100 ms–24 h, timeout50–30000 ms, latest-only synchronous scans, skip expired ticks and exponential1–32 s failure backoff. Stopping waits for the finite in-flight call and discards late results. No hidden historical replay guarantee.

These are independent development-preview modules, version 1.0.0, consuming SDK v1.0.0. Native MongoDB Go driver v2.10.0 is Apache-2.0, pure Go (no libmongoc/CGO/encryption helper), optional artifact only; preserve its LICENSE/NOTICE and transitive notices in the SBOM. No MongoDB server binary is redistributed. Tests use the real driver against a local OP_QUERY/OP_MSG wire fixture, plus node ownership/limits/ACK tests. This is not a real MongoDB replica-set, TLS/SCRAM or failure-election acceptance test.

No aggregation, arbitrary command execution, updates/deletes, multi-document transactions, GridFS, change streams or resume-token claims. Use dedicated database permissions and indexes; the query subset is not a tenant isolation boundary. A majority receipt is a MongoDB single-document database acknowledgment, not device execution or downstream business completion.
