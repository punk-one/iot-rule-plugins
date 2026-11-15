# MongoDB Event Sink

```json
{"connector_ref":"database","collection":"events","timeout_ms":5000,"ack_after_commit":false}
```

One input, no output. Requires nonempty stable Event.ID and JSON-object payload <=1 MiB. Uses Connector immutable insert_event; does not expose arbitrary update operators. Existing ID with different payload fails. Do not reuse IDs for revised data. Large integers use decimal strings or explicit canonical Extended JSON values, not implicit floating conversion.

Buffered inputs require explicit ack_after_commit=true and original buffer_node_id/buffer_record_id. Only a positive majority receipt matching the original Event.ID publishes sdk.AckStatusSuccess for that record. Failed, mismatched and unknown results never ACK. Replaying after lost replies relies on the same event ID and bytes; no transparent driver retry, no cross-document transaction or exactly-once physical effect claim.

These are independent development-preview modules, version 1.0.0, consuming SDK v1.0.0. Native MongoDB Go driver v2.10.0 is Apache-2.0, pure Go (no libmongoc/CGO/encryption helper), optional artifact only; preserve its LICENSE/NOTICE and transitive notices in the SBOM. No MongoDB server binary is redistributed. Tests use the real driver against a local OP_QUERY/OP_MSG wire fixture, plus node ownership/limits/ACK tests. This is not a real MongoDB replica-set, TLS/SCRAM or failure-election acceptance test.

No aggregation, arbitrary command execution, updates/deletes, multi-document transactions, GridFS, change streams or resume-token claims. Use dedicated database permissions and indexes; the query subset is not a tenant isolation boundary. A majority receipt is a MongoDB single-document database acknowledgment, not device execution or downstream business completion.
