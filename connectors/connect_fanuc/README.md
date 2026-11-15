# connect-fanuc

Native development preview, Linux amd64/ARMv7 + CGO only. Requires an operator-provided FOCAS ELF shared library, matching architecture, absolute path and SHA256. No vendor header/library is included or redistributed. ARM64 deliberately fails compilation rather than generating a stub. `platforms.json` documents requirements; installation/release must enforce them. Build explicitly with `INCLUDE_NATIVE_PLUGINS=1`; default pure-Go builds exclude this Connector.

One FOCAS library generation per plugin process, process init once, per-resource serial gate, package8-session limit, thread-affine handle lifecycle. A batch connects once, selects CNC path, performs queries, frees handle and exits thread. Supported read queries: status (default non-FS15 structure), 16-bit program numbers, raw feed/spindle, alarm bits and macro1..9999. No program download/delete, offset/tool writes, PMC writes, 8-digit program mode or universal CNC-model support claim. ABI is explicitly `focas32_compat`: fixed-width 32-bit data layout, never a platform C long-sized struct. It must be verified against the installed vendor library/controller combination.

Controlled macro writes require the number in resource writable_macros, absolute expires_at, exact decimal conversion and 9-digit range. No transparent retries or physical-action-complete ACK. A native library call cannot be interrupted by Go context; deadline is checked before every call and the vendor socket timeout is finite, but an in-flight call/cleanup can overrun the batch deadline. Stop waits for the native call before freeing the library; process isolation contains vendor faults. No abandoned goroutine/parallel handle-close workaround is used.

Tests use a locally compiled synthetic shared library to check dynamic loading, fixed ABI offsets and arguments. This is not real FANUC device acceptance. Native library path must remain operator-controlled and immutable while loaded; digest verification is not a sandbox for untrusted native code. `/dev/null` is used for vendor startup log output.


## Optional read-only queries

- `positions`: `cnc_rdposition(-1)` up to 32 axes, absolute/machine/relative/distance; raw int32, native decimal precision, numeric unit/display/suffix codes retained. Does not guess units or axis suffix meanings.
- `tool_management`, `number` 1..32767: one `cnc_rdtool` entry; tool code and life counters/status/H/D codes. Controller option-dependent; not a universal current-tool selector.
- `pmc_byte`, `pmc_area` 0..12, `number` 0..32767: one unsigned byte via `pmc_rdpmcrng`, data type 0. Requires an explicit Connector `readable_pmc_ranges:[{"area":0,"start":0,"end":15}]`; empty by default. Numeric area codes must match the target controller manual. No bit/word/dword or PMC write support is implied.

Optional symbols may be absent: such points receive bad quality while basic reads remain available. These ABI layouts were checked against the operator-owned FOCAS declarations and synthetic C fixtures; neither the proprietary header nor library is distributed. Actual device, model/option, unit and ABI acceptance is still required. ARM64 remains unsupported.
