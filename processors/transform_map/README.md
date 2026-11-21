# transform-map

`transform-map` builds a payload from Go templates and optional field expansion.
`process.iterate` emits one row for each member of an input array or object.
Existing templates and expansions are still supported.

Version 1.0.2 adds `process.operations`. Without `template` or `expand`, these
operations edit an owned copy of the input payload. Otherwise they edit the
mapped result after template rendering and expansion.

```json
{
  "process": {
    "operations": [
      {"op":"move","from":"data.temp","path":"data.temperature"},
      {"op":"delete","path":"debug"},
      {"op":"set","path":"data.origin","value":"line-1"}
    ]
  }
}
```

Apply at most 128 operations in their configured order. Paths are dotted object
keys with at most 16 segments and 256 bytes. `set` and `move` create missing
parent objects and replace existing destination fields. A missing source for a
move or a missing key for a delete is a no-op. Traversal through an existing
scalar or array is rejected. Parent/child move paths cannot overlap. An explicit
JSON null is valid for `set`. Literal numbers are encoded without conversion to
binary float, and separately emitted events own their payload and metadata.
