# source-websocket

Output: device_code, data.raw_base64, data.frame_type, optional data.raw for text, time (receive ms). Use parse-records to decode JSON explicitly. Best-effort connection/queue semantics; no durable replay or per-peer ACK. One consumer per rule/node, bounded buffers, credits after engine handoff. Offline restore does not prevent rule startup. Stop closes consumer before waiting for the loop.
