# 插件发布流程

1. 运行 `./scripts/sync-modules.sh`，确认每个 module 依赖已整理且没有本地 replace。
2. 运行 `./scripts/test-plugins.sh`、仓库级 manifest 测试和 `go vet`。
3. 使用已发布且不可移动的 `iot-rule-plugin-sdk v1.0.0` 在干净环境构建。
4. 受保护的 GitHub Release 工作流使用 OIDC 执行
   `PLUGIN_REQUIRE_SIGNATURE=1 PLUGIN_SIGNING_MODE=keyless ./build-plugins.sh`。
5. 对 `release-manifest.json` 本身进行签名，并归档全部版本化 `.so`、`.sbom.json`、`.sigstore.json`、`release-manifest.json` 与 `release-manifest.json.sigstore.json`。
6. 校验 release manifest 中的 commit、SDK、module 和 SHA-256 后再发布。

本地验证可省略签名，但公开 release 不得省略。无密钥签名身份必须是受保护的
`punk-one/iot-rule-plugins/.github/workflows/release.yml`，签发者为 GitHub Actions OIDC。
`latest` 软链仅供部署选择，不能代替版本化 artifact 或发布清单。

下载发布包后可验证单个插件：

```bash
cosign verify-blob \
  --bundle connectors/connect-s7-1.0.0.so.sigstore.json \
  --certificate-identity-regexp '^https://github.com/punk-one/iot-rule-plugins/.github/workflows/release.yml@refs/tags/v[0-9]+\\.[0-9]+\\.[0-9]+$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  connectors/connect-s7-1.0.0.so

cosign verify-blob \
  --bundle release-manifest.json.sigstore.json \
  --certificate-identity "https://github.com/punk-one/iot-rule-plugins/.github/workflows/release.yml@refs/tags/v1.0.0" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  release-manifest.json
```

FANUC FOCAS 库不包含在默认产物中。任何带 `-tags fanuc` 的构建必须在受控 runner
安装合法授权的头文件和动态库，并单独记录其版本、许可证和 SHA-256。
