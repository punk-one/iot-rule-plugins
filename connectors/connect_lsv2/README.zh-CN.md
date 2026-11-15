# connect-lsv2

Read-only Heidenhain LSV2 connector. One session per bounded scan batch; INSPECT login, explicit `allow_dnc_reads` for runtime reads. No control/program/file writes. SDK v1.0.0.

Framing: uint32-BE **payload-only** length + four command bytes + payload; version selectors use one byte, runtime selectors two-byte BE. Runtime queries: execution_state(23), program_state(26), overrides(25), current_tool(51), axis_locations(22), selected_program(24), uptime counters(30–34). Version strings use ISO-8859-1.

Complex values retain `raw_base64` and `selector` and additionally return:

| Query | Decoded fields | Accepted layout |
|---|---|---|
| overrides | feed_percent, spindle_percent, rapid_percent | exactly 3 uint32-BE / 100 |
| current_tool | number, index, axis_code, axis; optional length, radius | 8 bytes or 24 bytes; doubles are **little-endian** |
| axis_locations | positions: {axis: value}, header_byte | header byte + axis count + NUL-terminated numeric values then names; max 32 distinct axes |
| selected_program | line_number, main_program, current_program | uint32-BE + exactly two NUL-terminated paths |

Malformed/trailing/non-finite/duplicate-axis data produces per-query `bad:decode`, never fabricated zeroes. Header/unknown axis codes are retained without assigning undocumented meaning. No axis units or uptime units are inferred. Tool selector support varies by controller; permission/unsupported replies produce bad quality.

Independent protocol reference: [pyLSV2 misc decoders](https://github.com/drunsinn/pyLSV2/blob/master/pyLSV2/misc.py). This is a format reference, not a runtime dependency or copied implementation. Fixed vectors, loopback batch tests and fuzz checks cover software behavior; actual controller models/options still require acceptance. No claim of full LSV2 support.

中文：只读批量采集；倍率、刀具、轴位置、当前程序可结构化输出，同时保留原始字节。异常格式、重复轴名、非有限数值不作为正常采集值。设备型号、权限、轴单位与真机行为仍需现场验证；不包含控制写入。
