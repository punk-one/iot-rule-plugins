# connect-mssql

Version 1.0.0, SDK v1. SQL Server named-parameter requests, bounded rows/columns/bytes, finite deadlines and a shared connection pool. Connector uses microsoft/go-mssqldb v1.11.0 (BSD-3-Clause) with encryption and server-certificate validation enforced. No legacy SQL Server 2008/TLS downgrade mode. Configure the platform trust store for the SQL Server certificate.

allow_write gates Exec requests only; SQL text is configured by trusted rule editors, and actual read-only security MUST be enforced by database account grants. Parameters are bound values, never interpolated. Use separate least-privilege accounts when read/write permissions differ. request-mssql returns rows in result_field; parameter values are resolved from payload paths or event.id.

Sink commits a transaction before confirming a buffer record. A failed commit acknowledgement can have unknown effects; use idempotent SQL with an event_id unique constraint for replay. ACK failure after confirmed commit also requires idempotent SQL. Incremental scheduled source/cursor recovery is not yet implemented. Tests use a database/sql driver fixture, not a live SQL Server instance.
