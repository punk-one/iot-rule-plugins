# SFTP Connector — 1.0.0 development preview

Independent SDK v1.0.0 module. Pure Go pkg/sftp v1.13.12 (BSD-2-Clause) and x/crypto v0.59.0 (BSD-3-Clause); the build baseline is Go 1.27.4 with patched transport dependencies. Dependencies are pinned, not runtime-installed; see docs/SECURITY_BUILD_BASELINE_2026-11-14.md.

## Resource configuration

```json
{"host":"sftp.example.invalid","port":22,"username":"collector","host_key_sha256":"SHA256:<verified-server-fingerprint>","password":"<secret-reference-resolved-by-engine>","root":"/incoming","timeout_ms":5000,"max_file_bytes":1048576,"max_files":1024,"allow_write":false}
```

Exactly one of password or private_key_pem is required; private keys are parsed in memory, unencrypted PEM only. Secret values belong in Engine connector secret handling, never checked-in configuration. Pin a fingerprint verified out of band; no accept-any-host option. root must be an existing absolute canonical path and contain no symbolic links. Limits: configuration 64 KiB, file 1 MiB, directory 1024 entries, SSH response packet 256 KiB and aggregate response bytes at most max_file_bytes + 2 MiB. Directory overflow fails the operation; no truncated successful listing. Credentials/errors are not logged.

## Operations and output

Read target `list`: `{ "path":"." }` → `{ "files":[{"name":"sample.xml","size":10,"modified_ms":1794614400000}] }`. Nonrecursive, regular files only; entry limit counts all directory entries.

Read target `read`: `{ "path":"sample.xml" }` → `{ "data":"<base64>","size":10,"modified_ms":1794614400000,"sha256":"<hex>" }`. Lstat/open-handle stat/re-stat must agree. Paths are relative beneath root; absolute paths, traversal, backslashes and symlinks are rejected.

Write target `put`: `{ "path":"event.json","data":"<base64>","sha256":"<hex>","require_sync":true }` → `{ "received":true,"synced":true,"duplicate":false }`. Requires allow_write. A same-directory exclusive temporary file is uploaded, chmod 0600, optionally fsynced, then published with `hardlink@openssh.com` (atomic no-replace); no plain RENAME or replace fallback. Temporary link is removed on best effort. The server must support hardlink, and fsync@openssh.com when require_sync=true. Existing identical content is an idempotent receipt; different bytes, directories and symlinks fail. A lost reply has unknown outcome; reconcile stable name and digest, do not invent successful receipt. Stale `.iot-*.tmp` files after process/server crash need operator retention.

Probe reports demand-managed idle without making a network call; it is not a failed connection test.

All requests have a total deadline (50–30000 ms), one serialized gate per resource and a package-wide maximum of eight open device sessions. Resource configuration does not connect; each list/read/upload operation opens one SSH/SFTP session and always closes it. No background reconnect or transparent write retry. Maximum 32 resources.

Use a dedicated, trusted SFTP account with a server-enforced chroot and immutable/atomic producer files. Client path and symlink checks reduce mistakes but SFTP v3 has no openat/no-follow handle semantics: a hostile server or concurrent directory replacement cannot be sandboxed by a client. Set server-side quotas and retention. Deleting remote data and arbitrary shell execution are not offered.

This preview has local SSH and independent OpenSSH sftp-server interoperability tests, including file sync, competing output writers, wrong host pin, symlinks, bounded listings, oversized packet rejection and resource-stop interruption. No claim of every SFTP server, remote filesystem power-loss durability, ARM runtime or 72-hour acceptance.
