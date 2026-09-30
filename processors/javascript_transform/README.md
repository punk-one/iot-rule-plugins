# JavaScript Transform

独立 JavaScript 事件处理插件。配置中的 `code` 必须定义主函数 `main(input)`；引擎负责
调用主函数，唯一输入为只包含 JSON 数据的 `input`：

```text
input.payload       object
input.metadata      object<string,string>
input.timestamp_ms  number
input.event_id      string
input.trace_id      string
input.rule_id       string
input.source        string
```

标准示例：

```javascript
function main(input) {
  console.log("JavaScript Transform input:", JSON.stringify(input));

  // ===== 在这里编写业务处理代码 =====
  var output = { temperature: Number(input.payload.temperature).toFixed(2) };

  return {
    payload: output,
    metadata: input.metadata,        // 可选；省略则保留原 metadata
    timestamp_ms: input.timestamp_ms // 可选；省略则保留原时间
  };
}
```

返回对象只允许 `payload`、`metadata`、`timestamp_ms`；`payload` 必须是对象，全部值必须
可序列化为 JSON。运行时不注入 `require`、文件系统、网络、环境变量或宿主进程对象；每个
事件使用全新的 JS Runtime，并受 `timeout_ms`（默认 50ms，最大 1000ms）限制，因此脚本
不能依赖跨事件的全局状态。

## 内置能力与第三方库

v1 稳定契约保证 ECMAScript 5.1 标准对象，例如 `JSON`、`Math`、`Date`、`RegExp`、
`Array`、`Object`、`String`、`Number`、`Boolean`、`parseInt` 和 `parseFloat`，另提供
`console.log/info/warn/error`，输出进入当前规则日志。

不提供浏览器或 Node.js 宿主能力，包括 `window`、`document`、`fetch`、`WebSocket`、
`require`、`process`、`Buffer`、文件系统和环境变量。v1 不允许在管理页面执行 `npm install`
或从公网动态加载代码。这一限制用于保证离线部署、依赖可复现和插件进程安全。

需要第三方 JavaScript 库时，采用以下发布流程：

1. 将库和业务代码封装为新的独立插件；
2. 固定依赖版本并生成 SBOM、校验和与签名 manifest；
3. 在 CI 中完成许可证、安全和稳定性测试；
4. 通过离线插件发布流程安装，不在运行中的规则里下载依赖。

后续若引入共享 JS 模块包，也只能加载管理员预装、签名、白名单且固定版本的模块，不能开放
任意 npm 包在线安装。
