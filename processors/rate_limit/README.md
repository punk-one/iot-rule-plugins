# rate-limit

Version 1.0.0, SDK v1. Per-key token bucket, no waiting queue: excess events are intentionally dropped and counted. A full key table rejects new keys until idle keys can be removed. State is volatile and resets after restart. Place before a persistent buffer when sampling is intended; this is not a reliable queue. Configuration is exposed by PluginInfo.ConfigSchema.

Persistent buffered input is rejected: place this node before buffer-node. Dropped/merged analytical events do not preserve a one-input/one-output business ACK relationship. Key partitions preserve the scalar type; numeric `1` and string `"1"` do not share state.
