# request-sqlite

SDK v1, version 1.0.0. SQLite uses @name parameters and a dedicated directory/file. File must be a basename, with existing file symlinks rejected. One connection per resource; WAL/FULL sync for writable resources. allow_write=false opens the main database read-only. SQL text is trusted code: restrict editor permissions and service filesystem access; no claim of a SQL sandbox or encrypted database contents. Data files and state are not source artifacts. Linux file identity is used for incremental cursor scope.

Queries and results are bounded. Writes are transactional and buffer ACK follows successful commit; idempotent SQL is required for replay and uncertain commit outcomes.
