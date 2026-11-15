# source-fanuc

FANUC read-only canonical telemetry {device_code,data,quality,time}. Named status/program/feed/spindle/alarm/macro queries share one native session per batch; latest-only polling/backoff and explicit bad quality. Minimum1s interval; device throughput is not promised. Requires a supported native connect-fanuc installation (Linux amd64/ARMv7 only); a portable Source binary does not make the vendor library ARM64-compatible. Native ABI and real-device acceptance remain independent requirements.
