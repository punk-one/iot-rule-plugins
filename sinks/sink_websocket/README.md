# sink-websocket

Input event.Payload is JSON-encoded into a UTF-8 text frame. No automatic retries, no buffer acknowledgement: a successful socket write does not prove remote processing. Implement application ACK in a rule/protocol before using durable delivery guarantees. Source and Sink share the resource socket.
