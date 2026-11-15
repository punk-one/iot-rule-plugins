# connect-tcp

Poll/response only; one connection per exchange, all bytes in that operation share one session. Per-resource gate and absolute deadline include queue wait, cancellation closes the socket. Framing: fixed byte count, delimiter (removed), or length prefix (returned including header). length_adjust converts the decoded length into total frame length. Limits apply before allocation. No transparent retry. Standalone raw writes require allow_write=true; read exchanges still transmit their configured polling request and are not a security sandbox. No TLS/UDP/continuous-stream claim for this plugin. Source-freeport plus decode-binary/parse-records supplies framing and interpretation.

## Continuous stream extension

Default poll behavior is preserved. Shared, read-only continuous stream mode is now implemented; see [configuration, semantics and tests](../../docs/FREEPORT_STREAM.md). This preview does not provide durable frame replay or business acknowledgements.
