# sink-fanuc

Input {command_id,device_code,expires_at,data:{mapped field:exact decimal string/number}}. Resource writable_macros independently allowlists numbers; both Sink and Connector enforce expiry/target/value constraints before I/O. command_id is mandatory but this Sink alone is not a durable deduplicator: command-guard must precede it, and unknown physical outcomes require reconciliation. No transparent retries, program/tool/PLC control operations or buffer ACK are exposed. Native vendor call timeout/ABI/platform limits follow connect-fanuc; real machine write acceptance is pending.
