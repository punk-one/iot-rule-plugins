# Contributing

新增或修改插件必须满足：

1. 插件位于自己的类别目录，并拥有独立 `go.mod`、`go.sum` 和 `plugin.json`。
2. 不导入其他插件 module 或引擎 internal 包；公共能力只能依赖 SDK。
3. 普通插件使用 `sdk.ServeFactory`，Connector 使用 `sdk.ServeConnector`。
4. 增加配置、失败路径、Start/Stop、数据所有权和并发测试。
5. 运行 `./scripts/sync-modules.sh`、`./scripts/test-plugins.sh`、`go test .` 和完整构建。
6. 版本或依赖变化必须更新 manifest，并说明兼容性和迁移影响。

涉及专有 SDK、CGO 或外部动态库的插件必须使用显式 build tag 和独立 CI runner，
不得阻断通用插件门禁。
