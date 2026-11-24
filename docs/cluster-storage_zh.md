[English](cluster-storage.md) | [简体中文](cluster-storage_zh.md)

# 集群元数据存储

Registry 在 SQLite、PostgreSQL 和 FoundationDB 上使用同一套元数据实现，保存执行所有权和已完成发布的 Node Group support。Node 持有私有 Group 上下文；Registry 根据已发布目录鉴权和选点。Router 与可选 Placer 通过 Registry 协议工作，不直接打开数据库。首次部署从[最小双节点集群](cluster_zh.md#minimum-cluster)开始。

## 配置与部署

首次启动前选择后端：

| 后端 | 部署方式 | 必需的持久输入 |
|---|---|---|
| SQLite | 一个 Registry 服务多个计算 Node | 私有、持久的本地数据库文件；不能由多个 Registry 进程共享，也不要放到网络文件系统 |
| PostgreSQL | 一个或多个 Registry peer | 同一个数据库和 namespace，以及保存 DSN 的服务环境变量 |
| FoundationDB（默认） | 一个或多个 Registry peer | 可达的数据库、可读的 cluster file 和相同 namespace |

用 `run-cluster-ctl config registry --template` 生成完整角色配置，再选择后端。下面是**单 Registry** 配置的存储部分：

```yaml
storage:
  backend: sqlite                 # fdb（默认）、sqlite、postgres
  namespace: example/records-v2/   # Registry peer 必须相同
  sqlite_path: /var/lib/cluster-ctl/metadata.sqlite
  # postgres_dsn_env: CLUSTER_STORAGE_POSTGRES_DSN  # 仅 backend: postgres 使用
foundationdb:
  cluster_file: /etc/foundationdb/fdb.cluster
  prefix: example/                # 未填写 namespace 时用作旧配置的命名空间基底
```

只打开选中的后端。PostgreSQL 凭据来自配置中命名的环境变量，不进入 YAML 或 membership API。选择 `backend: postgres` 后，将实际 DSN 提供给 Registry 服务。使用交付的 systemd unit 时，创建 root 所有、权限 0600 的 `/etc/cluster-ctl/storage.env`，内容为 `CLUSTER_STORAGE_POSTGRES_DSN=postgresql://...`，填写实际数据库、用户、密码及 TLS 要求，再添加 drop-in：

```ini
# /etc/systemd/system/cluster-registry.service.d/storage.conf
[Service]
EnvironmentFile=/etc/cluster-ctl/storage.env
```

执行 `systemctl daemon-reload`，再按正常维护流程启动或重启该 Registry。操作员 shell 中的 `export` 不会配置已由 systemd 管理的服务。诊断输出不得打印 DSN，也不要通过命令行参数传递它。所有 peer 必须连接同一数据库和 namespace。后端、namespace 和连接设置的变更需要重启。membership 中的 `https://` 通告必须配套 listener 的证书、私钥和 peer 信任；URL 本身不会启用 TLS。

SQLite 要求持久本地文件，每个已配置 membership version 都只能有一个 Registry member；使用 WAL、`synchronous=FULL` 和 immediate write transaction。PostgreSQL 使用 serializable transaction 与 `synchronous_commit=on`，启动时拒绝 `fsync=off`。数据库复制、故障切换、备份及一致历史恢复由部署方负责。FoundationDB 验证版本为 7.3.77，API 730。

SQLite 和 PostgreSQL 不需要 FDB headers、动态库、build tag 或 CGO：

```sh
CGO_ENABLED=0 go build -o cluster-ctl ./cmd/cluster-ctl
./cluster-ctl registry --config registry.yaml
```

原生发布包仍包含支持 FDB 的二进制和固定运行库，支持的构建/启动入口仍是 `make build-cluster-native` / `run-cluster-ctl`。在纯 Go 构建中选择 FDB 会明确报错。

## 共享实现与引擎契约

`internal/cluster/metadata` 定义业务记录与 repository 契约，`internal/cluster/metastore` 只实现一套业务逻辑。`internal/storage/record` 提供记录身份、revision、完整性、分块和条件写入。`internal/storage` 定义有序 byte key、`View`、`Update`、`Get`、`GetMany`、有界 `Scan`、`Put` 和 `Delete`；observation 是独立能力。`internal/storage/sqlengine` 和 `internal/fdbdriver` 只适配后端。架构测试禁止存储层导入业务包。Registry 接收 repository，不接收通用事务闭包。

引擎回调执行一次。读操作使用一致快照；写操作可串行化，覆盖不存在的 key、范围谓词，并能读到自身写入。`GetMany` 保持顺序、重复项以及“不存在”和“空 value”的区别。范围按 byte 字典序排列，cursor 为排他边界。跨事务分页不保留同一个快照。

只有确认已中止的事务返回 `ErrRetry`。记录层可在四秒操作期限内保留最初读取的 revision 重试；guard 改变返回 `Conflict`，不授权重新选点。重试中的范围扫描是一次新读取，需要固定候选或快照的调用方必须保留明确身份/版本或使用业务快照协议。提交结果不明时不自动重放；结果与 Node RPC 副作用只能在确认提交之后发布。

版本保持十字节 / 20 位十六进制形式，对同一 namespace 中同一个逻辑 key（包括删除和重建）单调有序；它不是原生 versionstamp、墙钟或全局提交顺序。原子记录变更使用 `max(被改记录的旧 revision)+1`。可见记录缺失仍以零表达期望不存在；隐藏 revision cell 保留原逻辑身份，删除后仍存在以避免 ABA。其规模随曾经变更的不同身份数量增长，不能用普通 TTL 回收。替换或恢复 namespace 需要丢弃旧进程缓存并明确新的历史边界。

长 equality-key 组件由公共 codec 哈希，原身份保留在 header/revision cell 并校验；scope prefix 保持不变。这不承诺哈希组件的用户字符串排序。需要排序的字段使用短定长编码；哈希规则由所有后端共享。

## 边界与观察

| 限制 | 契约 |
|---|---|
| 物理 key | 2,048 字节，不含 namespace |
| 物理 value | 100,000 字节 |
| 记录分块 | 64 KiB，校验长度、数量、身份及 SHA-256 |
| 逻辑事务预算 | 已计量字节小于 9,000,000；staging 也有界 |
| 原生事务预算 | 同时满足后端限制；FDB 的 namespace/conflict-range 开销也计入 10 MB |
| Scan 请求 | 1–1,024 行，1 字节–2 MiB 字节预算 |
| 首个大记录 | 可单独超出请求页字节预算以保证进展 |
| 批量点读 | 最多 1,024 个 key |
| 一次 observation | 1–256 个持久条件 |
| observation owner 预算 | 8,192 槽，其中 256 为 catalog 保留 |

route 读取返回持久 revision 条件。注册观察时再次比较条件，因此读取与注册之间发生的变化会立即使结果失效。FDB watch 持久 revision cell。SQL owner 合并重复 key，以 256 个为批次轮询，100 ms 兜底，本地写入立即唤醒。PostgreSQL LISTEN/NOTIFY 加速相同的重读，不携带业务数据；先建立 LISTEN 再复查，断线会使活动观察失效。通知丢失或合并不能替代持久 revision 检查。Cancel/Close 等待资源回收；reservation 在连续重新注册时保留。

## Schema 与升级边界

当前为 record schema v2。SQL 使用私有 `kuasar_storage_v2` KV 表和 namespace hash；FDB 在配置的 namespace 下使用私有记录 key。未指定 `storage.namespace` 时默认为 `foundationdb.prefix + "records-v2/"`，默认完整值为 `kuasar-sandbox/cluster/records-v2/`。

没有将旧实验 FDB schema 原地转换的自动迁移。不要让新 codec 读取旧记录字节，也不要将空的新 namespace 当成已迁移数据库。当前没有受支持的执行状态 export/import API，也没有从 Node DB、route events、Group 定义或可移植快照重建 Registry 所有权的工具。不得伪造旧 SID/Node/ready 记录。

对可丢弃的预发布状态：停止新准入，用旧版本完成或明确删除测试所有的工作，停止旧角色并保留必要证据；使用全新 Registry 数据库/namespace 和 Node 状态启动新版本，重新安装根凭据与 Group，再从兼容的 canonical artifacts 创建新 Sandbox。这是具有新执行身份的重建，不是原地迁移或同 ID 恢复。旧数据在保留决策完成前保持隔离；不删除无关状态。

对同 schema 数据库备份恢复：停止可能写入或缓存该历史的全部 Registry/Router/Placer，保留对应 Node 所有权证据，再用后端原生方法恢复一致备份，包括 SQLite WAL 一致性。数据库备份不能单独把正在运行的 Node 回滚到同一时间点。所有权对账得到验证前保持准入关闭；新空 namespace 不能修复错位。Node DB/加密密钥、租户根凭据、canonical artifacts 及完整依赖闭包均是独立备份输入。[恢复契约](cluster_zh.md#registry-state-ownership-and-disaster-recovery-boundaries)定义当前限制；缺失所需所有权状态时不能承诺保留身份恢复。

## 验证与元数据扩展

`storagetest` 在真实后端执行相同的事务、记录、ABA、重试、未知提交结果、观察及代表性 repository 测试。生产 Group-support repository 还验证 registration guard、完整发布、选点竞争、已有对象鉴权保留及有界清理。这些业务契约位于引擎之上，不向 driver 添加 Group/Create 方法。`MaxCheckpoints` 也保留在业务回归范围中。

```sh
go test -race ./internal/storage/... ./internal/cluster/metastore
# PostgreSQL 用例需设置指向隔离真实数据库的 STORAGE_TEST_POSTGRES_DSN。
STORAGE_TEST_BACKEND=postgres go test -race ./test/e2e/cluster_stub
python3 scripts/with-test-fdb.py -- python3 scripts/cluster-native.py test --arch x86_64 -- -race
```

PostgreSQL 测试未设置专用 DSN 时会跳过；原生 FDB 测试必须显式选择测试集群，不静默跳过。共享 benchmark 报告 resolution、intent/ownership、128 记录列表及 observation 唤醒的延迟分位数、吞吐和 Go 分配量。FDB 配对比较使用相同工作负载、依赖和持久设置。这些测试不能证明生产高可用或千万对象 SLO。
