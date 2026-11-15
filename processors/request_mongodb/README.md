# MongoDB Query

```json
{"connector_ref":"database","collection":"events","filter":{"enabled":true},"bindings":{"device":"payload.device_code"},"sort":"_id","descending":false,"limit":100,"timeout_ms":5000,"result_field":"mongodb"}
```

One input/output. Binds event scalar paths as `$eq` constraints combined with the static filter under `$and`; object/array operands and unsafe large numeric bindings are rejected, never interpreted as operators. `event.id` is supported. Numeric literals beyond the exact JSON integer range require explicit canonical Extended JSON in the static filter, or string-based application schema.

Clones the input, preserves its identity/metadata and writes `{documents,count}` to result_field. Canonical BSON scalar wrappers remain intact. Results are bounded and any query/cursor/shape error emits no partial event. This processor performs a query, not mutation or acknowledgment of prior buffer records.

These are independent development-preview modules, version 1.0.0, consuming SDK v1.0.0. Native MongoDB Go driver v2.10.0 is Apache-2.0, pure Go (no libmongoc/CGO/encryption helper), optional artifact only; preserve its LICENSE/NOTICE and transitive notices in the SBOM. No MongoDB server binary is redistributed. Tests use the real driver against a local OP_QUERY/OP_MSG wire fixture, plus node ownership/limits/ACK tests. This is not a real MongoDB replica-set, TLS/SCRAM or failure-election acceptance test.

No aggregation, arbitrary command execution, updates/deletes, multi-document transactions, GridFS, change streams or resume-token claims. Use dedicated database permissions and indexes; the query subset is not a tenant isolation boundary. A majority receipt is a MongoDB single-document database acknowledgment, not device execution or downstream business completion.
