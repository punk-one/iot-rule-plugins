# sink-modbus

Independent SDK v1 module, version 1.0.0. Modbus TCP only; serial/ASCII/RTU are not implemented in this release. Addresses are explicit zero-based protocol offsets, not 4xxxx notation.

Connector: one serialized TCP session per batch, no write retry. Functions 1/2/3/4, 5/6/15/16; sink supports BoolArray coil points with an explicit count (1..1968). UInt/Int16/32, Float32 and Bool. Source applies engineering policies and preserves bad quality when substituting defaults. Sink requires an exact device_code and all configured values. A failed write can have partial or unknown effects and must not be automatically retried.

See ConfigSchema exposed by PluginInfo for all configuration fields. Tests use loopback simulation; real PLC and ARM hardware acceptance is pending.

## Demand-managed physical sessions

`session_reuse_horizon_ms` defaults to 2000 (0 = strict per-batch); `max_session_age_ms` defaults to 300000 (0 disables age rotation). Resource creation does not dial. Source sends `next_demand_in_ms`, scoped by the resource token injected by Engine. A healthy connection/serial FD serves continuous high-frequency batches; 5 s/20 s scans close after each batch. Standalone operations without a nearby consumer close immediately. Hints beyond the reuse horizon are not retained as long-lived node leases; at most 1024 nearby leases/resource, expired with 10–250 ms scheduling grace.

One physical serial bus remains one named resource, with multiple per-point unit_id/slave IDs in the same gate and session. Source stop waits for its in-flight scan then releases only its own token via read target `release_demand`; last demand closes immediately. Abandoned hints expire without any background redial. Resource stop/delete waits for in-flight I/O, closes retained sockets/FDs and releases package slots. The package limit is 8 open device sessions, including retained ones. Waiting resources cause a session to yield at a safe batch boundary rather than being starved by continuously polling peers. Maximum age rotates at a batch boundary; all segments of the current batch still use one session.

Three consecutive actual failures open a circuit; subsequent permitted demand attempts back off 1–30 s, with serialized half-open recovery. Writes are never transparently retried. Read target `session_stats` returns opens/reuses/closes/active/demands/failures/retry_after_ms without dialing. TCP transaction IDs persist across reused batches; serial quiet time persists across batches. RTU/ASCII still cannot provide message identity or physical exactly-once; configure finite timeouts, device-side command confirmation and explicit command expiry.

TTY opens verify the configured character-device identity, prohibit symlink substitution and own TIOCEXCL while the FD is active. Failed setup and normal close clear exclusive state. A Linux PTY test validates merged two-slave reads through one FD, duplicate-resource refusal, immediate release, replacement-resource creation and identity mismatch. PTY tests do not establish hardware UART/RS485 timing/parity or t1.5 certification.

Regular race tests cover short/long demand, abandoned leases, stop release, age rotation, circuit dial suppression and package pressure. Optional real-duration loopback acceptance: `TEST_MODBUS_FREQUENCY_60S=1 go test -race -run TestModbusFrequencySixtySeconds -v .` from Connector directory. It runs 100 ms and 500 ms for 60 s and 20 s for three periods, reporting counts, dropped scans, session opens/reuses/closes and latency p50/p95/p99. Real PLC/network acceptance is separate.
