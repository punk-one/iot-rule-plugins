# BACnet/IP plugin family

Development preview 1.0.0. Independent `connect-bacnet`, `source-bacnet` and
`sink-bacnet` processes consume published SDK v1.0.0. Each artifact is shared
across rules; resources and rule instances are isolated. No BACnet C library,
vendor binary or external service is needed by the production plugins.

## Supported protocol boundary

Direct, fixed **IPv4 unicast UDP BACnet/IP**, BVLC Original-Unicast-NPDU,
unrouted NPDU, confirmed ReadProperty and WriteProperty. A batch of 1–128 points
uses one UDP socket and sequential services under one resource gate and one
total deadline. Resource operations share a package limit of eight sockets.
This release uses ReadProperty per configured property, **not**
ReadPropertyMultiple; one socket per batch does not imply one datagram.

Read/Write scalar types: Null, Bool, Uint32, Int32, Float32, Float64, Enum,
UTF-8/ASCII String, base64 Bytes, binary-text Bits and structured ObjectID.
Date/Time are read-only four raw components; 255 and standard date selectors
are preserved, with no invented timezone. Explicit array_index selects a scalar
element, including index 0 where the device supports it. Whole arrays and
constructed values fail with bad:type_or_encoding instead of truncation.
Character encodings other than BACnet UTF-8/ASCII are rejected.

No broadcast discovery/Who-Is, BBMD, foreign-device registration, BACnet/SC,
MS/TP, routed/forwarded NPDU, segmented APDUs, COV subscriptions, RPM, schedules
or command execution. Use an isolated trusted OT network; BACnet/IP UDP has no
encryption or cryptographic device authentication. expected_device_instance
checks the configured Device object's Object_Identifier on every batch; it is
an identity consistency check, not authentication or proof of hardware origin.

## Connector configuration

```json
{
  "host": "192.0.2.10",
  "port": 47808,
  "timeout_ms": 3000,
  "expected_device_instance": 1234,
  "read_allowlist": [
    {
      "name": "temperature",
      "object_type": 2,
      "instance": 1,
      "property": 85,
      "valueType": "Float32"
    }
  ],
  "allow_write": false
}
```

Addresses use numeric BACnet object type, instance and property identifiers;
`2/1/85` means Analog Value instance 1, Present_Value. No inference from a
display label. Literal unicast IPv4 only. Use explicit read/write allowlists
with exact addresses, array index, valueType and write priority (default 16).
Changing only point names cannot bypass an address allowlist. Write is disabled
by default. Enabling it requires allow_write=true and a write_allowlist; choose
priorities with the device operator. No point may choose priority from an event.

No startup dial or background reconnect. Each batch closes its socket. After
three transport/protocol failures, resource-wide exponential backoff suppresses
dials for 1–32 seconds; the next eligible real operation probes recovery. Device
property errors are returned per point and do not pretend the connection failed.
Stop/Delete cancels active sockets and waits for the resource gate. Recreate a
resource after changing endpoint or allowlists. Never configure multiple
resources for one device expecting them to share the same gate.

## Source configuration and output

```json
{
  "connector_ref": "hvac",
  "device_code": "hvac-demo",
  "interval_ms": 5000,
  "points": [
    {
      "name": "temperature",
      "object_type": 2,
      "instance": 1,
      "property": 85,
      "valueType": "Float32"
    }
  ]
}
```

interval_ms: 50–86400000, default 5000; timeout_ms: 50–30000, default 3000.
Latest-only scans skip missed intervals and back off on batch failures; no
queue of stale ticks. Numeric point options: scale, offset, min/max,
outOfRangePolicy=null/default, defaultValue and precision=0..9. Order is decode,
scale/offset, range on the unrounded value, replacement, then precision for a
valid value. A default replacement retains bad quality. Never strip quality
before applying the business quality policy.

```json
{"device_code":"hvac-demo","data":{"temperature":22.75},"quality":{"temperature":"good"},"time":1794614400000}
```

Missing object/property, Abort/Reject and incompatible scalar types become
null with a bounded bad:* reason for that point. Timeout, malformed envelope,
wrong invoke/address or transport failure discards the whole batch: every point
is null/bad:read, with no mixed stale/fresh result. SDK event/trace IDs remain
independent per scan. This is polling, not an atomic device snapshot or COV log.

## Controlled Sink

```json
{
  "connector_ref": "hvac",
  "device_code": "hvac-demo",
  "points": [
    {
      "name": "temperature",
      "object_type": 2,
      "instance": 1,
      "property": 85,
      "valueType": "Float32"
    }
  ]
}
```

Enable and allowlist the same address on the Connector separately. Incoming
event addresses/priority are ignored; only data values use the configured names.

```json
{"device_code":"hvac-demo","command_id":"setpoint-001","expires_at":1794614405000,"data":{"temperature":23.5}}
```

Use a fresh timestamp when running the example. expires_at is mandatory,
integer milliseconds and within max_command_horizon_ms (default 10000, maximum
60000). A command_id is mandatory. Put command-guard before physical writes for
durable duplicate rejection; the Sink alone does not deduplicate. Automatic
buffered replay is rejected. All addresses/types/values are validated before
the first network write. Deadline covers queue, identity read and all writes.
No transparent write retries. A later error may leave a partially applied or
unknown outcome; reconcile device state, do not blindly resend.

The Connector's `{written,scope:"property_ack",physical_completion:false}`
receipt means all requested WriteProperty services were acknowledged. It does
not prove process action completion, relinquishment of other priorities,
durable storage or application exactly-once delivery. Sink verifies this scope.

## Verification and release

Run each module with GOWORK=off, go test -race . and go vet . . Fixed protocol
vectors, malformed/scalar boundaries, deadline, circuit breaker, one-socket
batch, preflight-before-write and node quality/expiry/receipt tests are included.

Optional `TEST_BACNET_SERVER=/absolute/path/to/bacserv` runs an independently
built bacnet-stack 1.7.0 server (tag commit
03acade70f4586b4f9aacd77c13d63f6192a021c) in a private Linux user/network namespace.
See [testing instructions](../../docs/BACNET_TESTING.md) from a module README.
The protocol fixture is an external test tool, not a production dependency.
Real Engine testing verifies two Sources sharing one resource, bad quality,
controlled write/readback, stopping one rule, and one process per artifact.
No physical-device or ARM runtime acceptance is claimed.

Install Connector before Source/Sink, keep configuration backups and use the
normal signed Hub artifact installation. Uninstall dependent rules/resources
before removing the Connector; rollback the three artifacts as a compatible
set after stopping the affected rules. Artifacts remain independently built
and versioned. License: AGPL-3.0-only; no new protocol runtime dependency.
