# normalize-points

Independent SDK v1 plugin, version 1.0.0. Configuration is exposed by PluginInfo.ConfigSchema.

Input: payload.data object, optional payload.quality object. Output: cloned payload with configured engineering policies and explicit quality. Processing order is scale/offset, range, then precision. Existing bad quality cannot become good by substituting a default. Integers outside the safe engineering-number range (2^53-1) are rejected as bad:number rather than silently rounded. Nonconfigured fields are preserved.
