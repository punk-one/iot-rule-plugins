# source-relational-mssql

SDK v1, version 1.0.0. Default snapshot mode polls a bounded configured query and emits device_code/data/time. Query parameters include @page_size; use TOP (@page_size) or another explicit SQL bound. Missing time_column uses collection time; configured time columns must be RFC3339 strings or safe integer milliseconds.

Incremental mode is opt-in and requires watermark_column, @watermark and a strictly increasing unique signed-int64 cursor. Queries must return rows above the saved cursor in ascending order; complete batches are validated before emission. Cursor state advances per row only after successful event handoff. Scope includes rule/node, database identity, query, watermark column and cursor_namespace. Use reliable Engine delivery for durable handoff; replay can occur after handoff-before-checkpoint crashes, so event IDs are deterministic and sinks must be idempotent.

IMPORTANT: IDENTITY allocation order is not transaction commit order. This generic integer-cursor mode cannot guarantee capture of late commits with lower IDs, mutable rows, tied timestamps or deleted rows. Use snapshot mode for current machine state; use a separately validated commit-ordered feed/CDC design for loss-sensitive change capture. Timestamp-only cursors and general CDC are not provided. Database endpoint/database changes produce a new identity scope; aliases of the same database can cause replay rather than silent cursor reuse.

Tests use a Connector fixture and persistent state, not a live SQL Server. State must reside on persistent local storage; disk-full/recovery remains deployment acceptance.
