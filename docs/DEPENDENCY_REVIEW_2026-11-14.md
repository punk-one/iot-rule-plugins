# Dependency review for development modules

- Modbus TCP, MC 3E, HTTP and general processors: standard library plus existing SDK; no new protocol runtime dependency.
- source-schedule: github.com/robfig/cron/v3 v3.0.1, MIT, pure Go. Used for five-field cron, timezone and calendar correctness. No Node.js or native library; module-local dependency, not added to SDK.
- OPC UA modules: github.com/gopcua/opcua v0.7.4, MIT, pure Go, selected to match the locally reviewed reference API. Adds UA types and secure-channel/client implementation. Fixed-version review and actual security interoperability remain necessary before stable release; no runtime vendor library.
- connect-mssql: github.com/microsoft/go-mssqldb v1.11.0, BSD-3-Clause, maintained Microsoft fork. Uses modern TLS-verified connectivity; does not retain the old service's SQL Server 2008 compatibility downgrade. Transitive Azure/Kerberos packages are recorded in go.sum/SBOM; only this Connector imports the driver. Actual database-version validation and dependency vulnerability scanning remain release gates.

All modules pin SDK v1.0.0 and retain AGPL-3.0-only project licensing. New binary sizes and platform behavior must be measured from the build outputs; no estimate is treated as a release guarantee. Third-party notices accompany this review and generated SBOMs.

## Expanded M5 and transport dependencies

| Dependency | Pinned version | License | Runtime/platform decision |
|---|---|---|---|
| go-sql-driver/mysql | v1.9.3 | MPL-2.0 | Pure Go, verified TLS, prepared values; preserve upstream file notices, do not relabel upstream code as AGPL |
| modernc.org/sqlite | v1.55.0 | BSD-3-Clause | Pure Go SQLite, no CGO; larger artifact than a protocol-only module, included only on demand; Linux file identity test |
| gorilla/websocket | v1.5.3 | BSD-2-Clause | Pure Go, one reader/serialized writer, bounded frames, no compression; isolate per-resource lifecycle |
| bbolt | v1.4.3 | MIT | Already used by buffer; additional Source/guard state uses separate bounded files and explicit lifecycle |
| golang.org/x/sys | v0.31.0 | BSD-3-Clause | Already in repository; Linux tty/PTY/openat helpers, amd64/arm64 separate build checks required |
| golang.org/x/text | v0.23.0 | BSD-3-Clause | Existing dependency, explicit bounded character decoding; invalid encoding rejected |
| go-ole/go-ole | v1.3.0 | MIT | Separate Windows bridge only, amd64/386; not linked into Linux plugins |
| rabbitmq/amqp091-go | v1.16.0 | BSD-2-Clause | Pure Go; explicit shared connection, automatic recovery disabled, bounded frame/body guard and manual ACK/confirm lifecycle |
| pkg/sftp | v1.13.12 | BSD-2-Clause | Pure Go; pinned host identity, finite per-operation SSH sessions, bounded response packets and total wire bytes; OpenSSH-only atomic output extensions |
| golang.org/x/crypto | v0.59.0 | BSD-3-Clause | Security runtime baseline; SDK API/tag stays v1.0.0, while independent consumers pin patched transitive dependencies |
| golang.org/x/net | v0.62.0 | BSD-3-Clause | Patched HTTP/2 implementation, selected consistently with x/crypto |
| google.golang.org/grpc | v1.85.0 | Apache-2.0 | Patched SDK transport runtime; real Engine interoperability regression required after upgrade |
| MongoDB official Go driver | v2.10.0 | Apache-2.0 | Pure Go shared bounded pool, majority reads/immutable inserts, no CGO or server redistribution; LICENSE/NOTICE preserved |
| franz-go / kmsg | v1.22.3 / v1.15.1 | BSD-3-Clause | Pure Go Kafka clients; explicit groups, manual commit, finite producer requests, bounded native wire/decompression; no Java or Kafka server in release |
| franz-go kfake | v0.0.0-20261109062748-d6ea02a174bc | BSD-3-Clause | Independent test-only Kafka wire cluster, not imported by production plugin files; not Apache Kafka server certification |
| klauspost/compress | v1.20.3 where used | BSD-3-Clause | Patched S2/Zstd/gzip codecs; Engine, buffer, MongoDB and Kafka pin compatible versions; per-artifact SBOM captures runtime linkage |

The independent OPC UA test fixture uses asyncua 1.1.8 (LGPL-3.0), in a separate optional Python virtual environment with frozen test requirements. It is not copied into plugin implementation, imported by Go production code, linked into artifacts, or installed at plugin runtime. Its TLS/crypto/transitive dependencies belong only to the test environment.

FINS and EtherNet/IP preview framing is implemented within each Connector using the Go standard library; no additional protocol library is linked. A downloaded gologix candidate was inspected but not added as a dependency. FANUC native libraries and Windows COM components remain outside Git and default artifacts. Full transitive dependency inventory is generated in each artifact SBOM; the license copies here supplement, rather than replace, that inventory.
