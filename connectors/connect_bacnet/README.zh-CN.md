# connect-bacnet

BACnet/IP 1.0.0 开发预览，完整字段和输入输出见 [README](README.md)。只支持直连 IPv4 UDP 的标量 ReadProperty/WriteProperty；不承诺 COV、路由、BBMD、分段或真实设备验收。逐点失败为 null/坏质量，整批通信失败不输出半份数据。写入必须固定地址/优先级、白名单、command_id 和有效期；不能自动重试、持久重放或把属性 ACK 当成设备动作完成。参见 [交付台账](../../docs/PLUGIN_DEVELOPMENT_STATUS.zh-CN.md)。
