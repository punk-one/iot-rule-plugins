# PostgreSQL Connector

`connect-postgres` 1.0.3 继续使用 SDK v1.0.0。`metadata/info` 增加 `pool`，包含初始化状态、连接上限、当前打开/在用/空闲数、累计等待次数和毫秒数，以及因空闲数量或寿命被关闭的累计连接数。

查询该元数据不会打开新连接或创建连接池。未初始化时只有配置的连接上限，其他计数为零。`initialized` 只表示本地池句柄已建立，连通性仍通过 `metadata/ping` 或健康探针确认。
