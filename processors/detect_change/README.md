# detect-change

Track the selected payload field(s) independently for each `state.key_by`.
`changed` compares values or multiple fields. `delta`, `percent` and `trend`
use a numeric field, optionally filtered by direction. Nonnumeric and nonfinite
numeric samples do not advance state. Use `normalize-points` first when decoding
values and applying engineering or bad-quality policies.

Version 1.0.2 adds an optional deadband baseline:

```json
{
  "state":{"key_by":["device_code"],"ttl":"30m"},
  "process":{"detect":{"type":"delta","field":"data.temperature","value":5,"baseline":"last_change"}},
  "output":{"emit":"on_change","emit_first":true}
}
```

`previous_sample` (default) preserves adjacent-sample comparison. In
`last_change` mode, only for `delta` and `percent`, the first valid sample
sets the reference and a later sample advances it only after crossing the
configured threshold and direction. For example, 20 → 22 → 24 → 25 crosses a
delta of 5 at 25. The first snapshot is emitted only if `emit_first` is set or
`output.emit` is `always`. A failed handoff leaves the reference unchanged.
State depends on the Engine state store and its configured TTL. Processing does
not provide a transactional state-and-publish commit.
