# CAN Frame Source

```json
{"connector_ref":"bus","device_code":"machine-001","ids":[{"id":"0x123"}],"include_errors":false,"include_remote":false}
```

No input, one output. Consumes selected raw CAN frames from a shared connect-can resource. Standard/extended selections are explicit; decimal/hex aliases of one identifier are duplicates. Error and RTR delivery are opt-in. A Source stopping does not close another Source's resource session.

Emits `{device_code,data:{can_id:"0x123",raw_base64:"AQI=",length:2,extended:false,fd:false,remote:false,...},quality:{frame:"good"},time:<receive milliseconds>}`. Flags, RTR/raw DLC, kernel-drop total and sequence metadata are preserved. Error frames use bad:can_bus_error; a kernel drop increment uses uncertain:rx_overflow. Invalid lengths/flags are rejected, never converted to good zeroes. New Event.ID/TraceID per received event; no replay identity is invented for a non-durable bus.

Credits are released only after Engine handoff. Failed handoff closes the stream; it cannot recover already received bus frames. Stream failures back off1–32s; no backlog replay is promised. Health changes are reported on quality transitions rather than every high-rate frame. Use decode-binary against `data.raw_base64` with the exact ECU format, then normalization/validation; a CAN ID is not automatically a PLC point name. External telemetry field selection is explicit, preserving internal quality until the output policy decides how to handle it.

Independent version1.0.0 development-preview artifact, SDK v1.0.0 unchanged, Go1.27.4 security baseline. Linux amd64/arm64; pure Go x/sys, no CGO/vendor runtime. Configure and authorize the host CAN interface outside the plugin. The engine/plugin does not load arbitrary drivers, change bitrate, bring physical interfaces up/down or broadcast discovery commands. A successful raw-socket write is local kernel acceptance, not an ECU acknowledgment or completed physical action.

Native tests use Linux SocketCAN and vcan in a newly created user/network namespace. No physical ECU is accessed. Fixed-vector/limits/race tests, two-Sources sharing one resource, stop isolation and command-guard preventing repeated physical writes have local evidence; ARM runtime, real controller/ECU, error recovery on a real bus and high-load/long-run acceptance remain pending. No CANopen, J1939, ISO-TP, DBC import, UDS or proprietary USB API support is implied; raw frames can feed decode-binary with an explicit field/byte-order configuration.
