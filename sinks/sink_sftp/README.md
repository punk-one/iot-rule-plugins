# SFTP File Sink — 1.0.0 development preview

```json
{"connector_ref":"outgoing","directory":".","timeout_ms":5000,"require_sync":true,"ack_remote_receipt":false}
```

One input, no output. Serializes `{event_id: Event.ID, payload: Event.Payload}` as at most 1 MiB JSON. Filename is SHA256(Event.ID)+`.json` inside the relative directory. Stable ID and identical bytes make replay idempotent; the same ID with different content fails. Connector uploads a temporary regular file, fsyncs when required, then atomically creates a no-replace hard link. Needs an OpenSSH-compatible hardlink extension and, by default, fsync extension; unsupported servers fail rather than downgrade. Read-only connectors reject writes before dialing.

Buffered input is rejected unless ack_remote_receipt=true is explicitly selected. ACK additionally requires require_sync=true and valid original buffer IDs; it is sent only after a verified positive synchronized-file receipt. It means remote file reception/sync, **not application consumption, database commit or power-loss-durable directory metadata**. Unknown write outcomes and invalid receipts never ACK; downstream buffer replay must retain the same event ID and payload. No implicit network write retry.

All requests have a total deadline (50–30000 ms), one serialized gate per resource and a package-wide maximum of eight open device sessions. Resource configuration does not connect; each list/read/upload operation opens one SSH/SFTP session and always closes it. No background reconnect or transparent write retry. Maximum 32 resources.

Use a dedicated, trusted SFTP account with a server-enforced chroot and immutable/atomic producer files. Client path and symlink checks reduce mistakes but SFTP v3 has no openat/no-follow handle semantics: a hostile server or concurrent directory replacement cannot be sandboxed by a client. Set server-side quotas and retention. Deleting remote data and arbitrary shell execution are not offered.

This preview has local SSH and independent OpenSSH sftp-server interoperability tests, including file sync, competing output writers, wrong host pin, symlinks, bounded listings, oversized packet rejection and resource-stop interruption. No claim of every SFTP server, remote filesystem power-loss durability, ARM runtime or 72-hour acceptance.
