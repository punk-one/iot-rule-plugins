# sink-relational-mysql

SDK v1, version 1.0.0. MySQL uses :name placeholders, safely rewritten to positional prepared parameters outside SQL literals/comments. Backslash query escapes, native ? markers and executable comments are rejected; bound values may contain arbitrary text. Driver TLS verification is required; no multi-statements or parameter interpolation. Configure private certificate roots through the platform trust store. SQL read-only policy requires database grants.

Queries and results are bounded. Writes are transactional and buffer ACK follows successful commit; idempotent SQL is required for replay and uncertain commit outcomes.
