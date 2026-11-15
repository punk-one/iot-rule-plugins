# source-mc

Version 1.0.0, SDK v1. MC 3E/4E TCP/UDP binary/ASCII (see matrix below), local routing (network=0, PC=255, I/O=1023, station=0). The additional local 1E profile is described below. Addresses are explicit decimal protocol offsets; areas D/W/R (16/32-bit words), M/X/Y/B (Bool). X is read-only. Default word encoding is little-endian low-word first; override byteOrder explicitly when needed.

Connector validates complete batches, serializes access and uses a single device socket per batch. Writes have no transparent retries and failures may leave partial/unknown effects. Source quality and engineering policies match Modbus Source. Sink requires exact device_code and all configured fields. Loopback protocol tests do not replace real PLC or ARM acceptance.

## MC transport capability matrix

Development preview: **3E/4E × binary/ASCII × TCP/UDP**. Default stays 3E binary TCP. Configure Connector `mode` as `3e_binary`, `3e_ascii`, `4e_binary` or `4e_ascii`, plus `transport:"tcp"|"udp"`. Explicit routing fields: network_number (default 0), pc_number (255), io_number (1023), station_number (0); check allowed values for the target PLC/module. monitoring_timer is in 250 ms units (default 16), separate from the finite client deadline. Unsupported modes fail configuration; there is no silent protocol fallback.

Protocol addresses remain **numeric offsets** in JSON. ASCII converts D/M/R to six decimal digits (0..999999), X/Y/B/W to six hexadecimal digits; binary uses 24 bits. Points remain D/W/R words and M/X/Y/B bits. Writes to X are forbidden. Default word/byte order remains little-endian low word first; e.g. Uint32 0x12345678 writes ASCII words `56781234`, not a reversed whole-value assumption.

Every batch has one device socket; all merged/chunked PDUs use it, with resource serialization and package limit 8. 4E includes request serial matching, reserved fields and routing verification. UDP reads one complete datagram, validates declared length and peer/route, discards at most eight stale 4E serial responses within the original deadline. There is no transparent network retry, including writes.

**3E UDP permits only one PDU per batch** (one read range or one write point): without a serial field, duplicated/reordered responses of equal size could otherwise silently be assigned to the wrong read range. Use 4E UDP for multi-segment batches. UDP remains lossy and cannot provide physical-actuation exactly-once; a write timeout is unknown. Stop/delete waits until in-flight I/O observes cancellation and releases the socket.

Fixed vectors and local TCP/UDP simulators cover all eight combinations, word/bit ASCII conversion, stale serial discard, route mismatch, end/length checks and stop cancellation. Hardware/model compatibility, routed networks, UDP fragmentation limits, strict load/latency and long-run acceptance are separate. The additional local 1E profile is described below; no blanket MELSEC certification.

Wire reference: [Mitsubishi SLMP Reference Manual, SH-080956ENG-N](https://dl.mitsubishielectric.com/dl/fa/document/manual/plc/sh080956eng/sh080956engn.pdf), request/response 3E/4E format and device access. Implementation is independent; no vendor SDK or legacy application runtime is imported. Upgrade/rollback by stopping bound nodes and replacing their independently versioned artifacts; no persistent local state migration.

## Demand-managed sessions

`session_reuse_horizon_ms` defaults to 2000 (0 = strict per-batch), `max_session_age_ms` to 300000 (0 = no age rotation). Source includes `next_demand_in_ms` on batch reads; the Engine supplies its resource token. No TCP connection is opened at resource creation. A healthy session can serve multiple continuous high-frequency batches, while 5 s/20 s intervals close after each batch. Anonymous direct callers without a hint always close after a standalone operation. 3E UDP remains strict per-batch even with hints, because reusing an uncorrelated socket across batches would risk stale datagrams.

Stop of a Source waits for its in-flight scan, then sends read target `release_demand` for only that node's token. With no other demand in the horizon, the socket closes immediately; remaining nearby consumers keep it alive. Abandoned hints expire automatically, with 10–250 ms scheduling grace, without a background redial loop. At most 1024 tokens/resource. I/O remains serialized and overdue scans are skipped. Read/write are not interleaved within a batch. Package limit 8 applies to **open** sockets, not merely concurrent requests; a retained session yields at the next safe boundary when another resource is waiting for a slot. Operation deadlines include gate, slot wait, dial and all segments. Retained sockets rotate at max age on a batch boundary.

Any failed operation retires its socket. Three consecutive failures open a resource circuit; permitted real attempts back off 1–30 s, with one half-open operation under the same device gate. No write retry is hidden in the manager. Read target `session_stats` returns opens/reuses/closes, active, demand count, failures and retry_after_ms without dialing. Source retry scheduling remains latest-only and adds its own bounded failure backoff.

Regular tests cover reuse, distant/strict close, abandoned-token expiry, per-token release, age rotation, no dial storm and yielding a retained session under package pressure. Optional real-duration loopback acceptance uses `TEST_MC_FREQUENCY_60S=1 go test -race -run TestMCFrequencySixtySeconds -v .` from Connector directory: 100 ms and 500 ms for 60 s, 20 s for three periods, reporting scans/drops/session counts and latency p50/p95/p99. These tests do not replace PLC/network timing acceptance.

## A-compatible 1E profile

`mode:"1e_binary"` or `"1e_ascii"`, TCP or UDP. This is an explicit **local FX5-compatible profile**, separate from SLMP 3E/4E framing. PC is FF, protocol timer/fixed field zero, no routed network/I/O/station representation; non-default routed settings fail configuration. The finite client deadline still applies. `monitoring_timer` is a 3E/4E setting; 1E sends the required zero wire field.

Supported existing D/W/R word and M/X/Y/B bit areas use their 1E device codes. Address in JSON is a numeric protocol offset; 1E ASCII sends eight hexadecimal address digits and four hex device-code characters (e.g. D = 4420), **not the 3E `D*`/six-decimal representation**. Do not supply human X/Y octal labels as decimal offsets. The conservative FX5 profile reads at most 64 word points or 256 bit points/PDU; the planner never splits a 32-bit point between PDUs. Quantity 256 encodes as the protocol's zero count byte. Typed writes remain one configured point/PDU and are not retried. Other command classes, random writes, remote RUN/STOP/password operations and arbitrary A-series device extensions are unsupported.

TCP may reuse the demand-managed connection. **1E UDP, like 3E UDP, is limited to one PDU per batch and strict per-batch socket release** because it has no response serial. Response opcode, end code, expected payload size and bit/word values are validated. An error retires the socket, discarding any detailed-error suffix. No model-wide certification is claimed: validate the PLC Ethernet module's supported frame/data code and actual device ranges.

Independent fixed vectors and local TCP/UDP simulators cover all four 1E combinations, Bool writes, word reads, zero-for-256 encoding, bad replies, unsupported routing and 32-bit point chunk boundaries. Reference: [Mitsubishi FX5 SLMP manual JY997D56001-K](https://dl.mitsubishielectric.com/dl/fa/document/manual/plcf/jy997d56001/jy997d56001k.pdf), chapter 5. Hardware/firmware interoperability remains unaccepted until recorded on a real device.


Set `scan_mode: "coordinated"` on all relevant Sources to use the Engine resource-level scan coordinator. Equal-time points are merged and duplicate native reads projected to each Source alias; engineering transforms remain per Source. A slow Source retains at most one latest pending sample. Its local stream ACK follows emitter handoff, not durable/business completion. Default/legacy `poll` remains compatible with older Engines and does not merge across Sources. See Engine `docs/COORDINATED_POINT_SCANS.md`.
