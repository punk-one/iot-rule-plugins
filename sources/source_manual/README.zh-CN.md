# source-manual

1.0.0 开发预览。默认关闭，只接受 Engine 管理的管理员手动触发。请求必须明确 confirm_live=true；会执行已部署规则及下游 Sink，HTTP 202 仅确认调度接收。物理命令仍须正常 command-guard、有效期和 Connector 白名单。完整配置、输入输出、权限、UI、错误与测试见 [README](README.md)。
