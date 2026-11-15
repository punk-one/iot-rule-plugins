# connect-udp

Bound-peer UDP poll/response transport. Each operation creates one connected UDP socket, sends one datagram and reads at most one response datagram, preserving its boundary. Max65507 bytes; oversize replies fail. Per-resource gate, package request deadline/cancellation, no transparent retry. Source-freeport uses frame.mode=datagram. No reliable delivery, ordering, duplicate suppression, application correlation, broadcast/multicast/listening-server or continuous stream claim. A server must reply to the operation source port; unsolicited data between operations is not collected. Raw writes require resource allow_write and absolute expires_at; a local send does not prove peer acceptance.

## Continuous stream extension

Default poll behavior is preserved. Shared, read-only continuous stream mode is now implemented; see [configuration, semantics and tests](../../docs/FREEPORT_STREAM.md). This preview does not provide durable frame replay or business acknowledgements.
