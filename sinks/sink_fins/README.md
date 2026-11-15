# sink-fins

Bound to connect-fins. Input {device_code,data:{configured point values},expires_at?}. Exact target, required fields and expiry validated. No transparent retry or physical-completion ACK. Point schema mirrors the Source; Connector validates all writes before I/O. Auxiliary writes disabled.
