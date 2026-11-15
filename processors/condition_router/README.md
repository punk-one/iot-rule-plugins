# condition-router

Independent SDK v1 plugin, version 1.0.0. Configuration is exposed by PluginInfo.ConfigSchema.

Input: event payload. Configure all/any conditions with dotted payload paths. Output: one cloned event sent to the matched or otherwise label. Configure rule edges with these exact labels. No JS coercion, no arbitrary expressions, no implicit default output.
