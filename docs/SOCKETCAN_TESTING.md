# SocketCAN 本地验证

本仓库测试不会配置真实 can0。需要 Linux 内核提供 CAN_RAW/vcan，以及 go、C 编译器（Go race runtime）、iproute2、unshare。内核不支持时应记录未验收，不把跳过当成通过。

```bash
./scripts/test-can-vcan.sh
TEST_ENGINE_BINARY=/absolute/path/to/iot-rule-engine ./scripts/test-can-engine.sh
```

脚本创建新的匿名 user/network namespace，在其中创建 testvcan0；退出后自动销毁，不更改宿主网络接口。Engine 闭环同时启用隔离空间内的回环接口。对端测试帮助程序仅允许显式标记的 testvcan 接口；产物不进入正式插件包。

第一组验证 Linux 原生经典帧、扩展帧、RTR、CAN FD64字节、发送接收及无需求关闭。默认 go test 执行编解码和模拟生命周期测试，并明确跳过需要虚拟内核设备的项。第二组使用真实 Engine：两个 Source 共用一个资源，采集质量和字节保真；定时重复命令经 command-guard 后只向虚拟总线写入一次；停止一条规则不影响另一条，每个 artifact 一个进程。

这不证明真实 ECU 支持该 ID/报文，也不证明物理完成、CANopen/J1939/DBC 或实机高负载性能。写入许可、位速率/终端电阻、设备协议和故障恢复应单独验收。跨编译不等于 ARM 实际运行。
