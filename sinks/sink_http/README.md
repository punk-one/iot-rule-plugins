# sink-http

SDK v1, version 1.0.0. Sends event payload JSON to a configured relative path through connect-http. Only write methods are accepted. Failures/unknown results do not request automatic retries. An endpoint must implement idempotency for any external replay. ack_on_success defaults false: enable it only if a 2xx receipt is the intended delivery goal. A 2xx HTTP response does not inherently prove a remote business transaction committed. Buffer ACK is emitted only after 2xx and only when explicitly enabled.
