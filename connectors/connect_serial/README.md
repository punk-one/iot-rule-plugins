# connect-serial

Linux serial poll/response transport; same explicit framing and bounds as TCP. One physical /dev character device is reserved per Connector Resource (device aliases share rdev identity); kernel TIOCEXCL is acquired during I/O. Finite nonblocking descriptor deadlines and cancellation close the port; input queue is flushed at batch start. 7/8 data bits, none/even/odd parity, 1/2 stop bits and enumerated baud rates. No RS485 direction control, hardware/software flow control or continuous-stream claim. Do not mix unrelated pollers on a bus; one Source/profile should coordinate bus requests. Raw standalone writes require allow_write; operation errors are not transparently retried. Linux PTY validates ownership/framing/deadline, not electrical or real-device timing.

## Continuous stream extension

Default poll behavior is preserved. Shared, read-only continuous stream mode is now implemented; see [configuration, semantics and tests](../../docs/FREEPORT_STREAM.md). This preview does not provide durable frame replay or business acknowledgements.
