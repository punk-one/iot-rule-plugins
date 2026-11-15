# SFTP File Source — 1.0.0 development preview

```json
{"connector_ref":"incoming","device_code":"machine-001","directory":".","pattern":"*.xml","interval_ms":5000,"stable_ms":1000,"timeout_ms":5000,"max_bytes":1048576,"max_files":1024,"encoding":"utf8","state_dir":"./state/sftp"}
```

No input port. Polls one nonrecursive relative directory through connect-sftp. Observes unchanged size/mtime for stable_ms, then reads a bounded file and verifies matching metadata and SHA256. UTF-8 text is emitted as `raw`; binary mode emits `raw_base64`. Output includes device_code, file{name,size,sha256,modified_at}, time (milliseconds), and stable Event.ID/TraceID scoped to rule/node/resource/directory/name/content. Connect to parse-records then validation/normalization and durable buffer. This is complete-file polling, not tailing or recursive watch.

Checkpoint is an independent bbolt file per rule/node/resource/directory. Only successful Engine handoff advances its digest. Reopening the store suppresses unchanged content; failed handoff retries with the same Event.ID. Directory listing failure/overflow does not delete checkpoints. Removed filenames are pruned only after a complete successful listing. Queue is latest-only, missed scans are skipped; failures back off 1–32 seconds. Stop waits for the finite in-flight operation and discards late results.

Handoff is not an atomic transaction with downstream persistent buffering. A crash after handoff but before checkpoint may replay; a crash after checkpoint but before downstream persistence can lose an item. Do not claim exactly-once or durable inbox. Keep immutable input files until an explicit business acknowledgment if end-to-end durability is required. Changed content at the same path gets a new ID; identical bytes recreated after absence intentionally have the same content identity. SFTP mtime is often second-resolution; stable_ms alone cannot prove a writer has finished.

All requests have a total deadline (50–30000 ms), one serialized gate per resource and a package-wide maximum of eight open device sessions. Resource configuration does not connect; each list/read/upload operation opens one SSH/SFTP session and always closes it. No background reconnect or transparent write retry. Maximum 32 resources.

Use a dedicated, trusted SFTP account with a server-enforced chroot and immutable/atomic producer files. Client path and symlink checks reduce mistakes but SFTP v3 has no openat/no-follow handle semantics: a hostile server or concurrent directory replacement cannot be sandboxed by a client. Set server-side quotas and retention. Deleting remote data and arbitrary shell execution are not offered.

This preview has local SSH and independent OpenSSH sftp-server interoperability tests, including file sync, competing output writers, wrong host pin, symlinks, bounded listings, oversized packet rejection and resource-stop interruption. No claim of every SFTP server, remote filesystem power-loss durability, ARM runtime or 72-hour acceptance.
