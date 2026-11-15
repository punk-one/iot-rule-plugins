# command-guard

SDK v1, version 1.0.0. Input command envelope: command_id, device_code, operation, issued_at/expires_at (integer Unix milliseconds), arguments (object). Target and allowed operations are configured, TTL is bounded and future-issued commands beyond configured skew are rejected. Wire explicit accepted and rejected routes.

Persistent bbolt claims are stored BEFORE forwarding; a crash/forward error after claim is an uncertain outcome and is not automatically re-emitted. Status forwarded means handed to the next rule node, not device completion. Deduplication scope is rule/node/device; it is not global across engines or duplicate guard paths. Keep command IDs unique and use one controlled path per device action. State retention is finite; capacity exhaustion fails closed. Changing command content while reusing a retained ID is rejected.

Accepted events preserve the envelope. Use a mapping stage to map arguments to the protocol Sink's data fields while preserving device_code/expires_at. Rejected events contain only ID, target, rejection status/reason. This guard is not a device-side exactly-once transaction. Protocol Sinks must re-check expiry at the actual operation boundary; physical effects after network failure may require device status reconciliation.
