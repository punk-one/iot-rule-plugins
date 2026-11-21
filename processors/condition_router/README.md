# condition-router

Independent SDK v1 plugin, version 1.0.1. The existing `conditions` config
accepts `mode: "all"` or `"any"` and routes to `matched` or `otherwise`.

Use `routes` instead of `conditions` to evaluate ordered named groups:

```json
{
  "match":"all",
  "default_label":"normal",
  "routes":[
    {"label":"warn","conditions":[{"path":"data.temperature","op":"gte","value":80}]},
    {"label":"audit","mode":"any","conditions":[{"path":"data.alert","op":"eq","value":true}]}
  ]
}
```

Match `first` (default) stops at the first matching route. Match `all` sends an
independent clone to each matching route in config order. If none match it uses
`default_label`, which defaults to `otherwise`. Configure rule edges with these
exact labels. Route labels must be unique. Groups inherit the top-level
`mode` (default `all`) unless they set their own. Limits are 32 routes,
64 conditions per group and 256 total conditions. Operators include equality,
order, existence and substring tests on dotted payload paths. JSON numbers
and supported Go number representations are compared numerically, strings are
never coerced to numbers, and numbers already rounded upstream cannot be
recovered. Evaluation leaves event data unchanged.

Place durable buffering after a multi-output router. Fanout refuses an event
carrying `buffer_record_id` when it matches multiple routes, because their
ACKs cannot safely be collapsed. If a later handoff fails, earlier outputs may
already have been delivered. Use idempotent sinks if upstream retries are
enabled. Deployment/runtime error events are separate and are handled by the
Engine bridge with `source-runtime-events`.
