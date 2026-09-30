# 官方插件使用 SDK v1 的约定

- `go.mod` 固定依赖 `github.com/punk-one/iot-rule-plugin-sdk v1.0.0`；同级开发由
  父目录 `go.work` 替换到本地 SDK。
- 插件入口只调用 `sdk.Serve` 或 `sdk.ServeConnector`，不要读取或修改
  `sdk.HandshakeConfig`。
- Event ID 是不透明标识，不能解析其中的时间或依赖数值连续性。
- 插件收到的 Event 由当前调用独占；交给多个异步任务时，每个任务使用 `Clone()`。
- 没有 Connector 绑定时，Connector 调用会返回 `sdk.ErrNotImplemented`，必须进入
  明确的配置失败或降级路径，不能把空结果视为成功。
- SDK StreamConsumer 支持并发调用和幂等 Close，但插件 Stop 仍必须等待自己的
  消费协程结束。
- 关键停止流程可调用 `sdk.FlushHealthReporter` 排空已接受的最终健康状态。

官方插件仓的根级契约测试用于及时发现 SDK module、握手和 Event 所有权回归。
