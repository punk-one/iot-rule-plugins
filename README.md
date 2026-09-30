# IoT Rule Engine Official Plugins

[English](README.md) | [简体中文](README.zh-CN.md)

This repository contains the official `v1.0.0` plugin set for IoT Rule Engine. Every plugin is an
independent directory, Go module, dependency graph, manifest, and executable artifact.

## Design rules

- Plugins never import source code from another plugin directory.
- Shared runtime contracts belong only in `iot-rule-plugin-sdk`.
- One built artifact maps to one OS process, regardless of how many rules use it.
- Every source, processor, or sink rule node gets an independent object and runtime context through
  `sdk.ServeFactory`.
- Connector processes may host multiple resource instances isolated by `provider_handle`.
- `plugin.json` is the release authority for plugin ID, version, type, SDK requirement, and module path.

## Layout

```text
connectors/   reusable protocol and database resources
processors/   event transformations and stateful processing
sources/      rule event sources
sinks/        rule event destinations
fanucutil/    optional standalone FANUC FOCAS adapter module
cmd/          repository release tooling
scripts/      module synchronization and verification
dist/         generated release artifacts; never commit
```

Official connectors cover MQTT, PostgreSQL, Redis, S7, TDengine, and TimescaleDB. Sources and sinks bind
to connector resources instead of duplicating credentials and physical session management.

## Local development

Clone the SDK as a sibling directory, then synchronize and verify every independent module:

```bash
./scripts/sync-modules.sh
./scripts/test-plugins.sh
./build-plugins.sh
```

Test one plugin in isolation:

```bash
cd processors/javascript_transform
go test -race .
go vet .
```

The repository `go.work` exists only for local development. Released plugin modules depend on the tagged
SDK module and must not contain local `replace` directives.

## Release artifacts

`build-plugins.sh` produces one versioned executable `.so` per plugin plus:

- SHA-256 checksums.
- CycloneDX SBOM documents.
- A repository-level `release-manifest.json`.
- Optional cosign signatures when `PLUGIN_SIGNING_KEY` is configured.

Set `PLUGIN_REQUIRE_SIGNATURE=1` in a production release pipeline so an unsigned artifact fails the build.

## Protocol lifecycle

Connection policy belongs to the connector implementation:

- MQTT keeps a persistent session while the resource is active.
- S7 uses demand-aware scheduled reuse: low-frequency scans connect per batch; high-frequency scans reuse a
  healthy session within the configured horizon.
- S7 scans are serialized per resource, read all due points in one session, use latest-only scheduling, and
  back off instead of creating an offline reconnect storm.
- Broken, stopped, reconfigured, or unused resources release physical sessions immediately.

## JavaScript transform

The JavaScript transform defines `function main(input)` and returns a JSON-compatible output object. It
provides standard ECMAScript objects and `console.log/info/warn/error`, but no Node.js, browser, filesystem,
network, environment, `require`, or runtime npm installation. Third-party libraries must be pinned and
packaged as separately reviewed plugins.

See [`processors/javascript_transform/README.md`](processors/javascript_transform/README.md).

## Documentation

- [Plugin architecture](docs/architecture.md)
- [SDK v1 integration](docs/sdk-v1-integration.md)
- [Connector session lifecycle](docs/connector-session-lifecycle.md)
- [Release process](docs/release.md)
- [Public-release readiness audit](docs/open-source-readiness-audit-2026-09-09.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)

## License

The official plugins are licensed under the
[GNU Affero General Public License v3.0 only](LICENSE) (`AGPL-3.0-only`). Modified plugins offered to users
over a network may trigger section 13's Corresponding Source obligations. The AGPL permits commercial use;
it does not grant trademark rights or rights to separately supplied vendor libraries.
