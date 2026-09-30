# IoT Rule Engine Official Plugins

[English](README.md) | [简体中文](README.zh-CN.md)

本仓库维护 IoT Rule Engine 的 23 个官方插件。每个插件都是独立目录、独立 Go
module、独立依赖图和独立 `.so` 产物。插件之间禁止源码依赖；可复用的公共运行时
能力只能进入 `iot-rule-plugin-sdk v1`。

## 目录结构

- `connectors/<plugin>/`：共享资源与连接插件，每个目录有自己的 `go.mod` 和 `plugin.json`。
- `processors/<plugin>/`：事件处理插件。
- `sources/<plugin>/`：事件源插件。
- `sinks/<plugin>/`：事件输出插件。
- `fanucutil/`：可选、独立且未被官方插件引用的 FANUC FOCAS 适配模块。
- `go.work`：仅用于本仓库本地联调，不进入任何插件依赖。
- `build-plugins.sh`：验证 manifest 后分别构建独立 `.so`、SHA-256、CycloneDX SBOM 和发布清单。

## 进程模型

普通 Source、Processor 和 Sink 使用 `sdk.ServeFactory`。引擎对同一个插件 artifact
只启动一个 OS 进程；每个规则节点在该进程中拥有独立插件对象、配置、RuntimeContext
和 Stop 生命周期。一个规则停止不会终止其他规则实例或共享进程。

Connector 本身已经在单进程中按 `provider_handle` 管理多个资源，因此继续使用
`sdk.ServeConnector`。

详细约束见 [`docs/architecture.md`](docs/architecture.md)。

## 本地开发

仓库自己的 `go.work` 将 23 个插件 module 与同级本地 SDK 关联：

```bash
./scripts/sync-modules.sh
./scripts/test-plugins.sh
./build-plugins.sh
```

单独测试一个插件：

```bash
cd sources/source_mqtt
go test -race .
go vet .
```

正式发布时，各插件使用 `plugin.json` 中的版本。构建不再从 Go 源码正则提取版本，
manifest 与 `PluginInfo`、module 路径和入口模式由仓库测试统一校验。

## 发布产物

`dist/plugins/release-manifest.json` 记录 commit、Go 版本、SDK 版本、artifact、SHA-256、
SBOM 和可选签名。配置 `PLUGIN_SIGNING_KEY` 后使用 `cosign sign-blob` 生成 `.sig`；
设置 `PLUGIN_REQUIRE_SIGNATURE=1` 可让缺少签名直接导致构建失败。

- SDK v1 接入：[`docs/sdk-v1-integration.md`](docs/sdk-v1-integration.md)
- 发布流程：[`docs/release.md`](docs/release.md)
- 安全报告：[`SECURITY.md`](SECURITY.md)
- 贡献约定：[`CONTRIBUTING.md`](CONTRIBUTING.md)

## 许可证

官方插件采用 [GNU Affero General Public License v3.0 only](LICENSE)（`AGPL-3.0-only`）。
修改后的插件通过网络向用户提供服务时，可能触发 AGPL 第 13 条规定的对应源代码提供义务。
AGPL 允许商业使用，但不授予商标权，也不涵盖需要单独取得许可的厂商库。
