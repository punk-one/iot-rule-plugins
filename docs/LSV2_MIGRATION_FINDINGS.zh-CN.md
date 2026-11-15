# LSV2 迁移审查发现

日期：2026-11-14。对照 pyLSV2 主线实现发现旧本地应用的三个关键不一致：帧 length 被定义为命令+payload，而参考实现只计 payload；R_VR selector 是单字节而非 uint16；执行/程序状态等 R_RI 子码存在错位。新 Connector 采用参考实现的帧语义、固定命令白名单和常量向量测试，没有复制旧应用的猜测写入命令。原目录保持不变。

复杂 runtime 数据先保留 raw_base64，不把未验证字段解释成轴坐标/倍率/刀号。DNC login 显式启用且仅用于读取。厂商型号、选项和固件仍需真机对照。此发现说明旧应用“有代码”不能直接当作协议验收证据。

参考：[传输实现](https://github.com/drunsinn/pyLSV2/blob/master/pyLSV2/low_level_com.py)、[命令枚举](https://github.com/drunsinn/pyLSV2/blob/master/pyLSV2/const.py)、[读取实现](https://github.com/drunsinn/pyLSV2/blob/master/pyLSV2/client.py)。
