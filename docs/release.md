# 插件发布流程

1. 设置 `GOWORK=off`，在受影响的独立 module 内整理依赖，确认没有本地 replace。
2. 运行 `./scripts/test-plugins.sh`、仓库级 manifest 测试和 `go vet`。
3. 使用已发布且不可移动的 `iot-rule-plugin-sdk v1.0.0` 在干净环境构建。
4. 受保护的 GitHub Release 工作流使用 OIDC 执行
   `PLUGIN_REQUIRE_SIGNATURE=1 PLUGIN_SIGNING_MODE=keyless ./build-plugins.sh`。
5. `release-plugins.txt` 选择本次发布的插件，不默认发布全量。原生 AMD64/ARM64 runner 分别测试和构建；`scripts/package-plugins.py --sign` 为每个插件生成单独的签名清单和平台压缩包，包含该插件的 `.so`、SBOM 与 Sigstore bundle。SBOM 摘要进入签名清单。
6. 校验 release manifest 中的 commit、SDK、module 和 SHA-256 后再发布。

本地验证可省略签名，但公开 release 不得省略。无密钥签名身份必须是受保护的
`punk-one/iot-rule-plugins/.github/workflows/release.yml`，签发者为 GitHub Actions OIDC。
`latest` 软链仅供部署选择，不能代替版本化 artifact 或发布清单。

插件目录内 `plugin.json` 与 `PluginInfo` 使用各自语义版本。新 buffer-node 为
v1.0.0，MQTT 连接身份、离线启动和确认修复分别为对应插件的 v1.0.1；SDK 保持 v1.0.0。
Git Release 的集合标签不替代插件自身版本。Hub 对同一 ID/版本/平台拒绝不同摘要。

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
