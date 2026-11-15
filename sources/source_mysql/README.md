# source-relational-mysql

SDK v1, version 1.0.0. MySQL uses :name placeholders, safely rewritten to positional prepared parameters outside SQL literals/comments. Backslash query escapes, native ? markers and executable comments are rejected; bound values may contain arbitrary text. Driver TLS verification is required; no multi-statements or parameter interpolation. Configure private certificate roots through the platform trust store. SQL read-only policy requires database grants.

Snapshot and opt-in incremental Source follow source-relational-mssql semantics: strictly ascending unique integer cursors, persistent per-row handoff checkpoints, deterministic replay IDs, no general CDC/late-commit guarantee. Use :watermark/:page_size for MySQL, @watermark/@page_size for SQLite.
