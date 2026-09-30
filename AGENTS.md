# AGENTS.md

This file is operational guidance for AI coding agents working in `iot-rule-plugins`.

## Repository role

This repository owns official protocol connectors and rule-node plugins for IoT Rule Engine. It consumes
the stable sibling SDK and must not contain engine internals. Each plugin is independently releasable.

## Hard boundaries

- Every plugin lives in its own `connectors/`, `sources/`, `processors/`, or `sinks/` subdirectory.
- Every plugin directory has its own `go.mod`, `go.sum`, `plugin.json`, entry point, tests, and output artifact.
- Plugins must not import another plugin or a shared repository implementation package. Put truly generic
  runtime contracts in the SDK; otherwise duplicate small protocol-local helpers within the plugin.
- `fanucutil` is an optional standalone module and is not shared by official plugin modules.
- All current official plugin manifests and `PluginInfo` versions are `1.0.0`.
- Do not add local `replace` directives to plugin modules. Local SDK replacement belongs only in `go.work`.

## Process and instance model

- Source, processor, and sink entry points use `sdk.ServeFactory`.
- The engine starts one OS process per plugin artifact, not one process per rule.
- Each rule node gets a new plugin object, configuration, context, health lifecycle, and stop lifecycle.
- Never keep rule-specific mutable state in package globals or singleton plugin objects.
- Connector plugins use `sdk.ServeConnector` and isolate resources by `provider_handle`.

## Protocol rules

- MQTT resources may keep persistent connections and must close them on resource stop/delete.
- S7 physical sessions use demand-aware scheduled reuse. A scan batch gets at most one device session; a
  healthy session may serve consecutive high-frequency batches within the reuse horizon.
- Never connect once per point. Merge due points, chunk by PDU/read constraints, and return one batch result.
- S7 uses per-resource serialization, latest-only scan scheduling, write fairness, circuit breaking, and
  bounded reconnect attempts. Do not queue missed high-frequency scans.
- Database, Redis, and broker calls require finite deadlines and bounded results.
- JavaScript transform scripts cannot access Node.js/browser/I/O APIs or install packages at runtime.

## Manifest and release rules

- `plugin.json` and `PluginInfo` identity, type, version, SDK version, and module path must match.
- Build only through `build-plugins.sh`; release artifacts include checksum, SBOM, and release manifest.
- Production releases require signatures. Do not commit `dist/`, `.so`, generated links, vendor SDKs, or
  proprietary FANUC libraries.
- Review every new dependency for license, maintenance, binary size, CGO, and supported-platform impact.
- Do not commit, tag, push, upload artifacts, or change license terms without explicit user authorization.
- The repository is `AGPL-3.0-only`. Preserve license notices and keep separately licensed vendor libraries
  out of release artifacts unless redistribution rights are documented.

## Security and data hygiene

- Configuration schemas must use secret references or connector bindings; never hard-code credentials.
- Samples use synthetic device names, payloads, and documentation IP ranges.
- Clone incoming events before mutation or retention. Never share payload/metadata maps across instances.
- Emit structured, bounded errors. Do not log connection secrets or full sensitive payloads.

## Verification

For one plugin:

```bash
cd <plugin-directory>
go test -race .
go vet .
```

For repository changes:

```bash
go test .
go vet .
./scripts/test-plugins.sh
./build-plugins.sh
git diff --check
```

Keep English as the default `README.md`, Chinese in `README.zh-CN.md`, and update plugin-specific README or
architecture documents when configuration or lifecycle behavior changes.
