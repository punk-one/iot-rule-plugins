# window-aggregate

Version 1.0.0, SDK v1. Bounded, processing-time analytical windows, flushed on max_count, timeout or Stop. Input value_path must hold a JSON number. Output contains device_code/data(count,min,max,sum,average)/window/time. State is volatile; input events and failed output windows are not durable and no automatic replay is requested. Place before persistent buffering. This is not an event-time/watermark engine or a replacement for reliable delivery. Configuration is exposed by PluginInfo.ConfigSchema.

Persistent buffered input is rejected: place this node before buffer-node. Dropped/merged analytical events do not preserve a one-input/one-output business ACK relationship. Key partitions preserve the scalar type; numeric `1` and string `"1"` do not share state.
