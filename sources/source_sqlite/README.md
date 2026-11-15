# source-relational-sqlite

SDK v1, version 1.0.0. SQLite uses @name parameters and a dedicated directory/file. File must be a basename, with existing file symlinks rejected. One connection per resource; WAL/FULL sync for writable resources. allow_write=false opens the main database read-only. SQL text is trusted code: restrict editor permissions and service filesystem access; no claim of a SQL sandbox or encrypted database contents. Data files and state are not source artifacts. Linux file identity is used for incremental cursor scope.

Snapshot and opt-in incremental Source follow source-relational-mssql semantics: strictly ascending unique integer cursors, persistent per-row handoff checkpoints, deterministic replay IDs, no general CDC/late-commit guarantee. Use :watermark/:page_size for MySQL, @watermark/@page_size for SQLite.
