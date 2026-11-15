# CAN Frame Sink

```json
{"connector_ref":"bus","device_code":"machine-001","id":"0x321","extended":false,"can_fd":false,"remote":false,"rtr_length":0,"bitrate_switch":false,"data_path":"data.raw_base64","timeout_ms":1000,"max_command_horizon_ms":10000}
```

One input, no output. ID and frame format are fixed by the node and cannot be overridden by message content. Connector must independently allow the ID/write/FD/RTR settings. CAN FD lengths must be canonical; bytes are not silently padded. Remote frames have no data and use the configured RTR length.

Input must contain matching device_code, nonempty bounded command_id, live integer expires_at milliseconds and bounded base64 at data_path (unless RTR). Default horizon10s, configurable100..60000ms. Example `{device_code:"machine-001",command_id:"cmd-001",expires_at:1794614405000,data:{raw_base64:"AQI="}}` uses a synthetic timestamp; generate a fresh expiration for actual execution. Rejects buffered events to prevent uncontrolled physical-command replay. Place command-guard before this Sink for durable duplicate claims and explicit reconciliation of unknown outcomes; this Sink alone does not deduplicate command IDs.

Only accepts Connector's queued/local_socket receipt; does not emit buffer ACK or claim ECU execution. Failed/unknown writes return errors and are not transparently retried. Repeated explicit upstream events are repeated commands unless a guard rejects them. Use a private virtual bus for tests before separately authorized equipment testing.

Independent version1.0.0 development-preview artifact, SDK v1.0.0 unchanged, Go1.27.4 security baseline. Linux amd64/arm64; pure Go x/sys, no CGO/vendor runtime. Configure and authorize the host CAN interface outside the plugin. The engine/plugin does not load arbitrary drivers, change bitrate, bring physical interfaces up/down or broadcast discovery commands. A successful raw-socket write is local kernel acceptance, not an ECU acknowledgment or completed physical action.

Native tests use Linux SocketCAN and vcan in a newly created user/network namespace. No physical ECU is accessed. Fixed-vector/limits/race tests, two-Sources sharing one resource, stop isolation and command-guard preventing repeated physical writes have local evidence; ARM runtime, real controller/ECU, error recovery on a real bus and high-load/long-run acceptance remain pending. No CANopen, J1939, ISO-TP, DBC import, UDS or proprietary USB API support is implied; raw frames can feed decode-binary with an explicit field/byte-order configuration.
