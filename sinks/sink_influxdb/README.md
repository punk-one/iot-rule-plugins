# sink-timeseries-influxdb

Input: {device_code:string,data:{scalar fields},time:integer milliseconds}. Measurement is configuration, device_code a tag. Fields are sorted, keys/tags/strings escaped; nonfinite numbers, control characters and nested fields fail. Exact signed/unsigned 64-bit decimal strings require field_types to avoid accidental type changes. Same series/field/timestamp may be replayed; be aware of InfluxDB overwrite/union semantics. Optional buffer ACK only after server 2xx. No transparent write retry or exactly-once promise.
