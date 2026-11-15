# InfluxDB Flux Poll Source

Development preview 1.0.0. Bind `connect-influxdb` (OSS 2.x API). Each tick executes one bounded Flux snapshot; slow ticks are skipped, failures back off 1–32 seconds. No overlapping query, backlog or transparent query retry. Stop waits at most the configured connector request deadline before discarding the late result.

```json
{"connector_ref":"metrics","device_code":"line-001","query":"from(bucket: params.bucket) |> range(start: -1m) |> limit(n: 100)","params":{"bucket":"telemetry"},"interval_ms":5000,"timeout_ms":5000,"max_rows":1000}
```

Output: `{device_code,data:{rows:[{_time:"...",_value:"..."}],row_count:1},time:1794614400000}`. CSV cells remain strings to preserve int64/uint64 and timestamps. Parameters are fixed scalar values, never interpolated into query text. Use decimal strings plus explicit Flux conversion for integers outside ±(2^53−1). The connector and source both enforce row/byte limits and fail instead of emitting truncated snapshots. Required connector capability: `read`; no input port, one output port.

This is a repeated snapshot, **not incremental ingestion/CDC**. Overlapping ranges can emit duplicate rows; downstream persistence needs a suitable unique key. No persistent watermark, event-time late-data guarantee or business ACK. Query text is trusted configuration with the connector token's permissions; this is not a query sandbox. Supply an explicit bounded range and limit. No 1.x/3.x coverage claim.

Tests cover fixed query parameters, exact text cells, malformed/oversized row counts, unsafe numeric parameters and late-result discard. Real InfluxDB interoperability and ARM runtime acceptance remain separate. Upgrade/rollback by stopping the node and replacing its artifact; no local state migration.
