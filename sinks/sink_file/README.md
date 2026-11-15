# sink-file

Linux local filesystem output. `event_file` (default) writes {event_id,payload} to an event-ID hash filename through fsync + atomic no-replace link; replay of the same ID/content is idempotent, changed content is rejected. Existing targets and outputs use anchored openat/O_NOFOLLOW and must be regular files. `jsonl` appends the same envelope under a bounded advisory flock, with explicit file-size cap; crashes/partial appends can require trailing-line repair and replay may duplicate records. Consumers must deduplicate event_id. Do not rotate while an instance is writing.

Limits: event bytes, JSONL file size, event-file count. Retention/rotation is operational policy; no unbounded disk growth is implied. Event-file count is a conservative preflight bound and concurrent creators can temporarily exceed it by active operations. Use a dedicated service-owned directory on a local filesystem; filesystem fsync latency cannot be canceled like a socket and remote/network filesystems are not validated.

`ack_after_sync` is false by default; opt in only when local file fsync and directory fsync satisfy the delivery goal. This is local file durability, not an external business receipt. Stop waits for current filesystem operations and closes the directory. Crash-created dot temp files may be removed while stopped; they are never treated as committed output.
