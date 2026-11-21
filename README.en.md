# IoT Rule Engine Official Plugins

[简体中文](README.md) · [Plugin catalog](docs/PLUGIN_CATALOG.md) · [Node-RED coverage (Chinese)](docs/NODE_RED_COVERAGE.zh-CN.md)

IoT Rule Engine plugins separate connection management, data collection, event processing, and delivery. Rule nodes reference named Connector resources, allowing rules to share the same connection configuration.

The source tree contains **120 independent plugins**: 24 original plugins and 96 development previews. See the [catalog](docs/PLUGIN_CATALOG.md) for per-plugin versions and the [delivery ledger](docs/PLUGIN_DEVELOPMENT_STATUS.zh-CN.md) for software and production acceptance status. Each module uses the stable **SDK v1.0.0** contract.

| Type | Count | Examples |
|---|---:|---|
| Connector | 30 | MQTT, S7, Modbus, MC, OPC UA, HTTP, TCP/UDP/serial, databases, brokers |
| Source | 32 | Polling, subscriptions, file watch, schedules, manual triggers, webhooks, runtime events |
| Processor | 29 | JavaScript, mapping, routing, normalization, windows, split/join, parsing, receipts |
| Sink | 29 | MQTT, HTTP, device writes, databases, files, SFTP, email, debug |

## Use the plugins

Install the selected version and architecture from the Hub when a published artifact is available. Create a named Connector resource in Engine, and bind rule nodes to that resource. A typical chain is `source-modbus → normalize-points → buffer-node → sink-mqtt`. Business commit requires an application receipt. MQTT broker acknowledgments confirm transport delivery.

Each plugin has its own directory, `go.mod`, `plugin.json`, and executable `.so` artifact. Engine can share one process per loaded artifact across rules. `sdk.ServeFactory` gives rule nodes separate instance lifecycles. `sdk.ServeConnector` hosts isolated resources in a Connector process. S7 reuses sessions according to current scan demand, while MQTT can keep a broker connection open.

## Build from source

The repository currently uses Go 1.27.4. This example builds the three MQTT modules to `dist/plugins`. Set `IOT_RULE_PLUGINS_DIR` to this directory when running a development Engine.

```bash
export GOWORK=off
PLUGINS_OVERRIDE=$'connectors/connect_mqtt\nsources/source_mqtt\nsinks/sink_mqtt' ./build-plugins.sh
```

Run `go test ./...`, `go vet ./...`, and `./scripts/test-plugins.sh` at the repository root for the repository contracts and independent module tests. Each module builds against the published SDK without `go.work`.

## Release and compatibility

- `release-plugins.txt` selects release modules. Packages are per plugin ID, version, and platform, with SHA-256, a CycloneDX SBOM, and Sigstore verification material.
- See the module manifests and documentation for supported protocol modes and input/output. Webhook, manual, and runtime-event sources require the matching features in the [companion Engine branch](https://github.com/punk-one/iot-rule-engine/tree/engineering/multi-gateway). FANUC needs a vendor library and currently supports Linux amd64 or ARMv7.
- JavaScript transforms use `function main(input)` with JSON-compatible output and a controlled console. See the [JavaScript examples](processors/javascript_transform/README.md).
- Development previews can be built from source. Verify hardware models, service versions, ARM runtime and endurance in the target environment.

See [architecture](docs/architecture.md), [SDK integration](docs/sdk-v1-integration.md), [release process](docs/release.md), [security](SECURITY.md), and [contributing](CONTRIBUTING.md).

Licensed under [GNU AGPL 3.0 only](LICENSE). Vendor libraries and trademarks have separate terms.
