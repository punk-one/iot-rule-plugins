# connect-http

Version 1.0.0, SDK v1. Bind to connect-http. Only configured-origin URLs are permitted; remote bearer auth requires HTTPS. Redirects cannot change origin. Connector limits request/response size and open connections, and closes/cancels them on resource retirement.

request-http adds a response field to an independent cloned event, sends the input payload as JSON for write methods and never requests automatic retries for writes. Read failures can be retried. source-http-poll emits device_code/data/time, remembers ETag only after successful emission and skips catch-up polling; HTTP 304 emits no event. ETag state is in-memory, not a durable receipt. No webhook server or long-lived HTTP streaming is implemented by these modules. See PluginInfo ConfigSchema for config.
