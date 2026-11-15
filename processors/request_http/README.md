# request-http

Version 1.0.0, SDK v1. Bind to connect-http. Only configured-origin URLs are permitted; remote bearer auth requires HTTPS. Redirects cannot change origin. Connector limits request/response size and open connections, and closes/cancels them on resource retirement.

request-http adds a response field to an independent cloned event, sends the input payload as JSON for write methods and never requests automatic retries for writes. Read failures can be retried. source-http-poll emits device_code/data/time, remembers ETag only after successful emission and skips catch-up polling; HTTP 304 emits no event. ETag state is in-memory, not a durable receipt. No webhook server or long-lived HTTP streaming is implemented by these modules. See PluginInfo ConfigSchema for config.

## Optional bounded JSON pagination

GET only. Configure `pagination:{"items_pointer":"/items","next_pointer":"/next","max_pages":10,"max_items":1000,"max_total_bytes":4194304,"total_timeout_ms":30000}`. The fixed path and each next link must be origin-relative absolute paths such as `/records?cursor=next`; absolute URLs, `//host`, fragments and relative paths are rejected. This never changes the bound Connector origin or authentication. Pointers use RFC6901 escaping (`~0`, `~1`), with arrays supported.

Each page must contain an items array and an explicit next path, null or empty string; a missing next field is an error rather than silently truncating. Per-page timeouts are reduced by the total remaining deadline. Limits apply to every page and the complete operation; max_total_bytes counts serialized Connector response envelopes including base64 overhead. Cycles, oversized results, page/item limit, malformed shape, timeout or any unsuccessful page emit **no partial result**. Only one cloned input event is forwarded, preserving its ID/metadata, with `response:{status:200,body:{items:[...],page_count:2,item_count:20}}`.

No offset/cursor synthesis, Link-header interpretation, background retry, cross-page atomic snapshot or CDC guarantee. APIs with absolute next URLs require an explicit trusted mapping to the relative path contract. Source polling can be combined with request-http in a schedule rule when pagination is required. Repeating a whole GET collection can see a changed remote dataset; downstream persistence should use stable business keys. Existing single-response behavior stays unchanged when pagination is absent.
