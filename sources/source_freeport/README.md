# source-freeport

Input configuration: request_base64 and explicit frame mode. Source sends one polling request per interval and receives one frame. Output {device_code,data:{raw_base64,raw?},time}; use decode-binary/parse-records downstream. TCP/serial share the same request contract; physical operations are serialized. Latest-only polling and exponential failure backoff; no missed-tick accumulation. Continuous streaming and general device-write semantics remain separate capabilities.

## Continuous stream extension

Default poll behavior is preserved. Shared, read-only continuous stream mode is now implemented; see [configuration, semantics and tests](../../docs/FREEPORT_STREAM.md). This preview does not provide durable frame replay or business acknowledgements.
