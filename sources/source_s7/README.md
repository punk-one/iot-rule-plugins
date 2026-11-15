# S7 Source

Version 1.0.1; SDK remains v1.0.0. Binds a demand-managed connect-s7 resource, batches configured absolute-address points, preserves independent rule-node state and uses latest-only scan scheduling/backoff. No connection per point and no transparent write operations from a Source.

## 1.0.1 engineering-quality correction

Pipeline: decode → scale → **range policy on unrounded engineering value** → precision. A value 10.04, maximum 10, precision 1 now applies the configured out-of-range policy instead of silently becoming a good 10.0. Default/clamped replacement values also receive configured precision. Very large finite float64 values are not multiplied past overflow merely to request decimal places that their representation cannot express. Non-finite arithmetic is an error, never a good telemetry value.

The internal event now always includes `quality`, mapping every configured point to `good`, `bad:range_default`, `bad:range_clamped` or `bad:range_dropped`. A dropped point is omitted from data and remains represented in quality. The legacy `point_quality` labels (`defaulted`, `clamped`, `dropped`) and resource_status remain optional under `output.include_meta`; those labels are unchanged. Bad-quality replacements can therefore be distinguished even with optional metadata disabled.

The additive quality field and near-boundary classification are intentional bug fixes. If an external consumer needs exactly `{device_code,data,time}`, configure an explicit outbound mapping of those three fields; keep quality within the rule for filtering/monitoring. Existing point config keys and supported types remain unchanged; SDK v1 types are unchanged. Range policy `error` still fails the scan rather than publishing a successful replacement.

Tests cover scaling/precision/range, boundary values masked by rounding, huge finite values, overflow and quality with include_meta=false, in addition to existing Source lifecycle tests. Real PLC/ARM timing and long-run acceptance remain separate. Upgrade/rollback by stopping the bound Source node and replacing its artifact; rollback restores the older rounding-before-range behavior and may hide near-boundary violations.

Use `scan_mode: "coordinated"` for cross-Source batching through the Engine resource coordinator (SDK v1 stream). Protocol fields are shared; scaling/range/precision and output aliases remain node-local. Latest-only, bounded results and explicit missed-scan metrics apply. Default `poll` preserves older Engine compatibility.
