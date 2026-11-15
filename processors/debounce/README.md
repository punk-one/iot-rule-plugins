# debounce

Trailing debounce: each input replaces the pending owned event and resets its deadline; only the latest event is published after quiet time. Key count, individual event (1 MiB) and total retained bytes are bounded. Processing-time, volatile state; stop discards pending events. Put before buffer-node; buffered inputs are rejected because replacing events cannot safely aggregate their ACKs. Publish failure increments timer_emit_failures and is not durable. Do not use this timer for durable business deadlines.
