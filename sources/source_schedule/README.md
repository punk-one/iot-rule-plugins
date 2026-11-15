# source-schedule

Independent SDK v1 plugin, version 1.0.0. Configuration is exposed by PluginInfo.ConfigSchema.

Configure exactly one of interval_ms or five-field cron. Timezone is explicit (default UTC); cron does not accept an embedded timezone prefix. No overlap or catch-up backlog. Missed ticks are skipped; schedules are recomputed after restart, with optional on_start trigger. Payload and event ownership are independent per emitted event. This is a trigger, not a durable job scheduler. New dependency robfig/cron v3.0.1, MIT; pure Go, no native runtime, one module only.
