# Freeport poll and continuous stream

Development preview v1.0.0. `connect-tcp`, Linux `connect-serial` and `connect-udp`
are independent resource-provider artifacts; `source-freeport` binds any of them.
One artifact process serves all rules. Each resource chooses an explicit mode:

- `mode: "poll"` (default): existing bounded per-operation request/response;
  fixed, delimiter or length_prefix framing (UDP: datagram). `sink-freeport`
  uses explicitly enabled, expiring raw writes in this mode only.
- `mode: "stream"`: read-only persistent-on-demand session, shared by up to 16
  Source instances with identical framing. No polling or event writes on this
  resource, so competing readers cannot consume each other's bytes. Configure
  a separate resource/physical channel only when the device supports that.

## Stream example

TCP Connector connection:

```json
{"host":"192.0.2.10","port":9000,"mode":"stream","timeout_ms":5000,"max_frame_bytes":65536,"stream_frame":{"mode":"delimiter","delimiter":"Cg=="}}
```

Source node configuration:

```json
{"connector_ref":"raw-device","device_code":"sensor-demo","mode":"stream","frame":{"mode":"delimiter","delimiter":"Cg=="}}
```

For serial, replace host/port with `device`, `baud`, `data_bits`, `parity` and
`stop_bits`; Linux device ownership uses resolved device identity plus TIOCEXCL.
For UDP use `stream_frame: {"mode":"datagram"}` on both Connector and Source.
UDP is a connected client to the configured remote peer, **not** a listener,
multicast server or guaranteed delivery protocol. If the remote device needs a
greeting to learn the ephemeral return address, configure
`stream_request_base64` on the Connector (maximum 65536 decoded bytes). This is
sent once per newly opened session, never once per Source. It must be an
idempotent protocol greeting; never put a physical action command there.
Source `request_base64` is forbidden in stream mode. Empty greeting sends no
datagram. No TCP server or TLS tunnel is implied by the raw TCP transport.

Framing is fixed at resource scope. Sources must match it exactly, preventing
different consumers from inventing incompatible boundaries on the same byte
stream. Delimiters are removed from returned payloads, fixed/length-prefix
headers are retained, UDP datagram boundaries are preserved. A persistent
buffered reader retains bytes after a delimiter when several frames arrive
in one underlying read. Oversized, truncated or malformed frames fail the
session rather than producing partial good data. UDP over-limit datagrams are
rejected, never silently truncated. No automatic binary field interpretation:
chain decode-binary or parse-records as needed.

## Bounds and lifecycle

First real Source opens the resource session. All Source instances share it;
last Source close, Resource Stop/Delete or framing/connection error closes it.
Package capacity: eight physical sessions/operations. At most 64 resources,
16 consumers/resource, queue eight frames/consumer and total queued-byte budget
4 MiB/resource. Immutable internal frame bytes are copied on delivery. Local
credit window is at most eight, duplicate/unknown ACKs fail, and no more than
one frame is returned per receive. A slow consumer fails and clears its own
queue; other consumers continue within the global byte budget. Idle stream
reads wait for data without periodically reconnecting; cancellation closes the
underlying connection. An obsolete stream ID cannot close a newer session.

Connector and Source use bounded 1–32 second retry backoff. No busy reconnect
loop or unbounded tick queue. Opening/initialization uses timeout_ms. Errors
change Source/Resource health and explicitly mean possible best-effort loss;
they do not invent a zero-valued success record. There is no frame replay,
resume token or device/application transaction ACK. The local stream ACK only
releases credit after the Engine accepts the event; it does not prove durable
storage or final processing. Choose a reliable device protocol when loss is
unacceptable.

## Input/output

```json
{"device_code":"sensor-demo","data":{"raw_base64":"YWJj","raw":"abc"},"quality":{"frame":"good"},"time":1794614400000}
```

`raw` is present only for valid UTF-8. Metadata includes protocol=freeport,
delivery=best_effort and per-resource sequence. SDK event and trace IDs are
fresh per Source event; no shared mutable payload is retained. Poll output
keeps its existing shape for compatibility. Never infer network delivery from
a raw write's local send result.

## Tests, installation and rollback

Each module: `GOWORK=off go test -race -count=1 .` and `go vet .` . Native tests
use actual loopback TCP/UDP and a Linux PTY, covering coalesced frames, one
session/two consumers, overflow isolation, copied bytes, duplicate credits,
last-demand close, session reopen and stale-close isolation. Source tests
verify handoff before credit ACK and no ACK on failed handoff. These do not
certify physical UART/RS485 electrical behavior, UDP loss-free delivery or
72-hour stability.

`TEST_ENGINE_BINARY=/absolute/path/to/iot-rule-engine ./scripts/test-freeport-engine.sh`
builds only four necessary artifacts, uses temporary local services and reports
shared process/session, frame preservation, rule-stop isolation and actual
disconnect recovery. Reports are saved under TEST_OUTPUT_DIR when provided.

Stop dependent rules before changing resource mode or framing. Install the
matching Connector before Source via the signed Hub path; do not replace a
poll resource with stream mode while a poll/Sink rule still depends on it.
Restore backed-up resource and node configuration together when rolling back.
No new runtime dependency or change to SDK v1.0.0. AGPL-3.0-only.
