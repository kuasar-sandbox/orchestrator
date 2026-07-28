# deployment — 部署拓扑与组件清单

kuasar-sandbox 平台由若干**独立部署的进程**组成,通过网络协议(gRPC / wire /
vsock / UDS)协作。本文档定义这些进程在生产部署中的归属、责任边界、配置入口与
启停依赖,供运维与 SRE 使用。

各模块的 CLI、配置 schema、内部设计在自己的文档里(`docs/<模块>.md`);本文档
**不**重复这些细节,只回答"东西在哪、彼此怎么找到对方、谁先起谁后起"。

## 1. 角色概览

资源分两个尺度:

**AZ 级集群**

| 角色 | 集群规模 | 职责 | 关键进程 |
|---|---|---|---|
| Compute Node | 每 AZ 一集群,~5,000 节点 | 承载客户沙箱(microVM),每节点 ~3K microVM;e2b 模板构建也在本节点的构建沙箱内进行(§5) | `node-ctl`(serve, 含 resource_listen)、`cache-ctl tiered`、`store-ctl`(sidecar)、`sandbox-ctl × N` |
| L2 Cache Cluster | 每 AZ 一集群,100-200 节点 | 分布式 EC 缓存(RS 4+1,Maglev 一致性哈希),吸收 L1 miss 把 L3 请求压到 < 0.1% | `cache-ctl shard` |
| Cluster Control Plane | 每 AZ 一组(小规模可单机)| e2b 兼容机群控制面:registry(自聚簇注册表 + 节点通道枢纽)/ router(统一入口 + 路由缓存)/ placer(放置调度 + group provider);显式创建沙箱后按 sandbox-group + route-key + 稳定 sandbox_id 路由请求,Router 在 node 边界替换为 NodeSandboxID,并按需激活已知的非 READY 沙箱 | `cluster-ctl registry`、`cluster-ctl router`、`cluster-ctl placer` |

**Region 级共享资源**(由各自的平台管理面运营,平台外)

| 资源 | 用途 |
|---|---|
| OBS 桶 | L3 chunk 与 Manifest 持久化(`store-ctl` 后端) |
| 平台管理面 | 沙箱实例调度、配置、租户管控、模板构建凭据(registry 拉取令牌 / 客户密钥);通过 **cluster 控制面**向 placer/provider 导入 sandbox-group 配置,或在单机部署中直接调用 `node-ctl` e2b API |
| 容器镜像仓库 | 租户镜像来源;构建沙箱内 `flatten-ctl` 按需拉取(OCI v1.1,支持 Referrers) |

## 2. Compute Node

### 2.1 常驻进程

| 进程 | 角色 | 数量 | 启停 | 归属 |
|---|---|---|---|---|
| `node-ctl`(`serve`)| 本机沙箱编排 + e2b 兼容控制面 + 数据面 proxy(反代 guest envd/floatingip)+ 节点级资源仲裁(`resource_listen`)+ node-link 集群接入客户端;经 run-id 模板单元 `sandbox-runner@<run-id>`/`sandbox-builder@<run-id>` 驱动 sandbox-ctl | 单实例 | systemd | 平台内,`orchestrator/docs/node.md`(资源协议见 node-resource.md)|
| `cache-ctl`(`mode: tiered`)| 节点本地数据入口:L1 RocksDB + EC 客户端(→ L2)+ L3 origin | 单实例 | systemd,先于 node-ctl | 平台内,`docs/cache.md` |
| `store-ctl` | 本机 OBS 读写代理(sidecar);**所有**远端 OBS 流量走这里 | 单实例 | systemd | 平台内,`docs/store.md` |
| `sandbox-ctl`(`run`) | 单个沙箱的控制平面(类 `runc run`);非 daemon | 每沙箱一个,~3K | 由 node-ctl 经 `sandbox-runner@<run-id>` 单元(`run-sandbox`)assignment 后启动 | 平台内,`docs/sandbox.md` |
| `cloud-hypervisor` | VMM(patched);`sandbox-ctl` 子进程 | 每沙箱一个 | `sandbox-ctl` 派生 | 平台内,`docs/cloud-hypervisor.md` |

### 2.2 端口与套接字

所有节点本机进程默认监听 loopback 或 UDS,不向集群外暴露:

| 进程 | 监听 | 协议 | 用途 |
|---|---|---|---|
| `store-ctl` | `127.0.0.1:7100` | gRPC | `Put` / `Get` / `GetSalt`(本机 cache-ctl + manifest-ctl 调用)|
| `store-ctl` | `127.0.0.1:7061` | gRPC health | 探针 |
| `cache-ctl tiered` | `127.0.0.1:7070` | wire(自定义二进制 TCP)| 数据面:`sandbox-ctl` / `manifest-ctl` 拉 chunk |
| `cache-ctl tiered` | `127.0.0.1:7071` | gRPC | health / `ping` / `info` |
| `node-ctl conductor serve(resource_listen)` | `/run/sandbox-resource.sock` | UDS,自定义协议 | 沙箱资源协议(`sandbox-ctl` 拨号目标)|
| `sandbox-ctl` | `/run/sandbox/<sid>/*.sock` | UDS | sandbox 内部:`ch.sock` / `blk{0,1}.sock` / `uffd.sock` / `ctl.sock` / `vsock.sock`(+ `_5000`);另写 `<sid>.pid`(config-socket 鉴别)、`<sid>.env`(`SANDBOX_ARGS`)|
| `node-ctl` | `:443`(可配) | HTTPS/h2 | **对外** e2b 控制面 API + 沙箱数据面 proxy(`<port>-<sid>.<domain>`;转发层设计见 `orchestrator/docs/node-proxy.md`)|
| `node-ctl` | `/run/sandbox/node-ctl.socket` | UDS,framed JSON | config-socket(task/admin/plugin/api 四平面):`run-sandbox`/`run-builder` 取 LaunchSpec / BuildSpec(密钥经 env);external proxy / 平台 agent 经 plugin 平面注册并同步路由(SO_PEERCRED + `<id>.pid` / pidfile 鉴别)|

`cache-ctl tiered` 的 EC 客户端通过节点对外网络拨号 L2 cluster 节点的 `7070`
端口(详见 §3)。**对外服务端口仅 `node-ctl` 一处**(e2b ingress);其余本机进程均 loopback/UDS。
`sandbox-ctl` 由 `node-ctl` 经 systemd **模板单元 `sandbox-runner@<run-id>.service`** 拉起
(`StartUnit`/预启动 → 单元内 `run-sandbox` WaitAssignment 后 `execve` 为 `sandbox-ctl run`,非自行 fork-exec)。e2b 模板构建
另走第二个模板单元 **`sandbox-builder@<run-id>.service`**(单元内 `run-builder` WaitAssignment 后
**驻留驱动构建沙箱内的三阶段流水线** import / steps / template,每阶段一台 microVM 作其直接子进程;镜像拉取与 step 执行
都在沙箱内,详见 §5 与 `orchestrator/docs/node.md` §12)。两个模板单元由
`node-ctl conductor serve` 启动时自动生成并安装(`install_units:false` 则交由运维带外管理),完整设计见
`orchestrator/docs/node.md` §5/§12。

运维侧:`/run/sandbox/<sid>/ctl.sock` 除了承载 snapshot,也是 `sandbox-ctl exec
--sandbox-id <sid> -- CMD` 的入口——在不打断应用的前提下进入一个运行中的
沙箱排障(命令跑在应用的命名空间内,如 `docker exec`)。完整规格见
`sandboxer/docs/sandbox.md` §2.4(发布包平铺名:`docs/sandbox.md`)。

### 2.3 持久化与运行时目录

```
/var/store/                          store-ctl 用 fs backend 时的本地数据(开发用;生产用 obs)
/var/cache/accel-l1/                 cache-ctl tiered 的 L1 RocksDB(典型 SSD 1 TiB)
/run/node-ctl/                       node-ctl audit + state(tmpfs)
/run/sandbox/<sid>/                  每沙箱运行时目录:socket + snap-stage/snap-state(CH 元数据中转,tmpfs)
/var/lib/sandbox/<sid>/              每沙箱磁盘目录:overlay 写层 <sid>.overlay.diff(本地 NVMe)
```

run 根(`/run/sandbox`,tmpfs)与 base 根(`/var/lib/sandbox`,磁盘)分离:可写
overlay 层必须落盘,不能用 tmpfs。两者分别由 `--run-root`/`SANDBOX_RUN_ROOT`、
`--base-root`/`SANDBOX_BASE_ROOT` 覆盖。快照**产物**另由 `--output` 指定目录(磁盘)。

### 2.4 节点共享资源目录

平台维护节点级共享文件,所有沙箱按名称引用,不复制:

```
/opt/sandbox/
  kernel/
    <ver>/vmlinux                    # 多版本并存,SANDBOX_CONFIG 按名引用
  runtime/
    <ver>/sandbox-runtime.bundle      # 多版本并存
  overlay-templates/
    overlay-1g.ext4                  # 预格式化空 ext4,大小不同的多份
    overlay-4g.ext4
    overlay-16g.ext4
```

引用方式(在 `SANDBOX_CONFIG` 里):

```yaml
boot:
  kernel:  file:///opt/sandbox/kernel/6.1.169-sandbox/vmlinux
  runtime: file:///opt/sandbox/runtime/v1/sandbox-runtime.bundle
  root:
    overlay:
      # diff 省略 → 自动落在 /var/lib/sandbox/<sid>/<sid>.overlay.diff(磁盘)
      diff_template: file:///opt/sandbox/overlay-templates/basic-1G.ext4  # 见下
```

**复制约定**:`sandbox-runtime.bundle` 与 `vmlinux` **不复制**——`sandbox-ctl`
让 CH 以只读 mmap / 直接打开方式使用(DAX 共享 host page cache,N 个沙箱共一份
RAM 工作集)。**overlay 写层**是沙箱独占、运行期被修改的可写盘:`diff` 省略时
`sandbox-ctl` 自动在 base 目录(磁盘)创建,并在 `diff_template` 给定时从模板
**稀疏复制**一份预格式化 ext4(无需 node-ctl 预先 `cp`)。显式给定 `diff`
则按该路径打开既有文件、不复制、不删除。

### 2.5 per-sandbox 配置下发

每个沙箱由 `node-ctl` 在启动单元前写一份 per-sandbox **`SANDBOX_CONFIG`**
(`<sid>.yaml`,非密),搭配**共享**的 **`MANIFEST_CONFIG`**(`manifest.key` 留空)+
per-沙箱 `MANIFEST_KEY` env;经 `run-sandbox`(单元)以 flag 传入 sandbox-ctl:

| 文件 | 内容 | 传入方式 | 文档 |
|---|---|---|---|
| `SANDBOX_CONFIG` | 该沙箱的资源 / 启动 / 网络 / launch 配置 | `sandbox-ctl run --config <path>`,等价 `SANDBOX_CONFIG` env | `docs/sandbox.md` §3 |
| `MANIFEST_CONFIG` | 客户密钥 + 本机 store-ctl + cache-ctl 端点 + 分块 / 加密参数 | `sandbox-ctl run --manifest-config <path>`,等价 `MANIFEST_CONFIG` env;`manifest-ctl` 也吃同一份格式 | `docs/manifest.md` §3 |

要点:

- **per-sandbox 密钥**:每沙箱用各自租户的客户密钥;node-ctl 经**共享**
  `MANIFEST_CONFIG`(`manifest.key` 留空)+ per-沙箱 `MANIFEST_KEY` env 注入(e2b 路径下
  `MANIFEST_KEY` 为该租户内容根密钥——node-ctl 从加密的 APISecret+ManifestKey
  凭据对中解出;**api_key 由 APISecret 签发**,见 `orchestrator/docs/node.md` §7)。
  外部管理面若选择直接对接单机
  `node-ctl`,也必须按同一 per-sandbox 生命周期落地 manifest 配置
- **共享格式**:`manifest-ctl` 与 `sandbox-ctl` 用**同一**配置格式;两者都
  **只**连本机 store-ctl(`127.0.0.1:7100`)+ 本机 cache-ctl(`127.0.0.1:7070`),
  yaml 里的 endpoint 写 loopback
- **不**走 env、不走全局默认:loader 要求显式 flag 指定路径(详见
  `docs/manifest.md` §3 loader 契约)

## 3. L2 Cache Cluster

### 3.1 集群规格

- **规模**:每 AZ 一集群,100-200 节点
- **编码**:RS 4+1(`data_shards: 4, parity_shards: 1`)→ 每个 chunk 编码为
  5 个 shard
- **放置**:Maglev 一致性哈希。每个 chunk 的 5 个 shard 由 chunk hash 通过
  `LocateN(key, 5)` 在全集群 100-200 peer 池中确定性选出。5 peer 是 RS 4+1
  的**最小集群规模**;集群可任意扩到 100-200,placement 算法不变。详见
  `docs/cache.md` §4.9
- **机型**:大盘 SSD(~5 TiB / 节点);RAM 占 8%(`mem_ratio: 0.08`)作 RocksDB
  BlockCache
- **网络**:同 AZ 内 10/25 GbE
- **隔离**:不同应用域(镜像 chunk / 快照 chunk)可独立部署集群实例,同一套
  软件配置不同 RocksDB path + 不同集群成员
- **持久化**:RocksDB on `/mnt/ssd/accel-l2`,daemon 进程崩溃可热重启不丢数据

### 3.2 端口

| 进程 | 监听 | 协议 | 用途 |
|---|---|---|---|
| `cache-ctl shard` | `0.0.0.0:7070` | wire | shard PUT/GET(由 compute node 上 tiered cache-ctl 发起)|
| `cache-ctl shard` | `0.0.0.0:7071` | gRPC | health / `info` |

`shard` 模式既不访问 L3 也不持有任何 origin 凭据,纯 KV——这是它能水平扩展、
彼此对等无主的前提。

### 3.3 成员变更

集群成员的 `endpoint` 列表写在每个 compute node 上 `cache-ctl tiered` 的
yaml 里(`tiers[].cluster.peers`)。增减节点是 compute node 端的**配置变更
+ SIGHUP**:Maglev 表重算后约 `1/M` 的 `(key, idx)` 迁移到新节点,其他 peer
命中正常。**操作规程:一次只动 1 个 peer**——同时换 ≥ 2 peer 单 key miss 数
可能超过 parity(RS 4+1 parity=1),读路径将 fallthrough origin,正确但慢。

shard 节点本身无须感知集群成员;它只是个 KV。

## 4. Region 级资源

### 4.1 OBS 桶

`store-ctl` 的最终持久化后端。一个 region 配一个或一组 OBS 桶,按 AZ 流量分
不分桶取决于运营策略(单桶跨 AZ dedup 最佳,多桶有故障域隔离收益)。桶内目录
结构由 `store-ctl` 维护:`__meta/generations/`、`chunk/<gen>/<hash[:2]>/...`、
`manifest/<gen>/<hash[:2]>/...`,详见 `docs/store.md`。

凭据获取顺序(`store-ctl` 配置):

```
yaml 显式 access_key/secret_key  →  ~/.obsconfig  →  AWS SDK 默认凭证链(IMDS)
```

每节点的 `store-ctl` sidecar 通过同一套凭据访问同一个桶——节点本身**无状态**,
重启不丢数据。

### 4.2 外部管理面接口

region 级、独立运营,平台外。与平台的接口:

- 多节点部署:平台管理面向 `cluster-ctl placer` 的 provider/importer 侧导入
  sandbox-group 配置、selector、客户密钥引用和模板构建凭据。
- 单节点部署:平台管理面可直接调用 `node-ctl` e2b API,并按 §2.2 的配置契约
  提供 per-sandbox manifest 配置。
- 节点侧桥接进程若由外部系统提供,不属于本发布件,也不改变本页列出的进程、
  配置和启动依赖。

构建在 compute 节点的构建沙箱内进行(§5),无独立展平管理面/数据面池。

## 5. 镜像构建(构建沙箱内三阶段)

e2b 模板构建在 compute 节点上进行,**无独立展平池**:每个构建执行绑定一个
`sandbox-builder@<run-id>` 单元(`run-builder` 驻留驱动),镜像拉取与 step 执行都在**构建沙箱(microVM)内**——租户网络
流量与镜像内容不触宿主用户态,宿主侧只做工件接力与收尾上传。完整语义见
`orchestrator/docs/node.md` §12。

### 5.1 三阶段流水

`run-builder` 依 BuildSpec 最多跑三阶段,每阶段一台 `sandbox-ctl run` + cloud-hypervisor
(都计入本单元 cgroup):

| 阶段 | 触发 | 做什么 |
|---|---|---|
| A import | 有 fromImage | 空单盘沙箱 + 单一 `sandbox-runtime.bundle` 内置的 flatten-ctl/mkfs.erofs;guest 内 `flatten-ctl export -` 以租户凭据拉取 + 确定性展平,tarstream 工件经 exec stdio 流回宿主 |
| B steps | 有 steps | base 镜像 + 单一 runtime + 大可写层;**envd 为 app**,RUN/ENV/ARG/WORKDIR/USER 经 envd `process.Start` 执行(与 e2b 同形);导出新镜像工件 |
| C template | 有 startCmd | 生产 e2b runtime 冷启最终镜像;startCmd 经 envd 启动、readyCmd 轮询;`sandbox-ctl snapshot` 出本地快照 bundle |

两类 guest 信道刻意分离:e2b 语义(steps/startCmd/readyCmd)走 **envd**,平台机制(flatten 拉取/
导出、配置注入、工件流回、就绪探针)走 **`sandbox-ctl exec`**。COPY 上下文经对象存储直传
(`builder.files_storage`,presigned PUT/GET,字节不过控制面),构建期 `flatten-ctl tar extract`
解包。

### 5.2 收尾上传(平台凭据唯一出现点)

阶段产物经宿主 workdir 顺序交接;终态:img ⇒ `manifest-ctl store image.img`(→ 64-hex manifest
key)、快照 ⇒ 一条 `sandbox-ctl upload-snapshot <bundle>`(自动上传 snapshot.cfg 引用的本地工件
并改写为 `manifest://`)。持久 id `e2b-<kind>-<key>`。本机 `store-ctl`(§2.1 sidecar)承载远端写。

### 5.3 凭据与隔离

租户 registry 拉取凭据仅 `FLATTEN_*` 经 exec env 进入 import 阶段 guest;**`MANIFEST_KEY`
永不入 guest**。凭据来源(任务级 pull token / SDK 明文 / 租户默认 `registry_auth_enc`)由
node-ctl 解析,见 node.md §12。构建池上限由 `sandbox-builder.slice` 的
`CPUQuota`/`MemoryMax` 施加,并发由 `builder.max_concurrent` 准入。

## 6. Cluster Control Plane (cluster-ctl)

大规模(多 compute 节点)部署时,机群之上由 **cluster-ctl** 三角色控制面聚合:**registry**
(shardkv 状态集群 + 节点通道枢纽)、**router**(e2b 兼容统一入口:控制面 + 数据面,
按 sandbox-group + route-key + 稳定 sandbox_id 路由,在 node 边界使用 NodeSandboxID)、**placer**(group provider/importer、WATCH_LIST 消费方与
放置调度器)。详见 `orchestrator/docs/cluster.md`。单 compute 节点独立部署(直供 e2b SDK)
时**不需要** cluster 层。

```text
client / SDK
    │
    ▼
cluster-ctl router
    │ route_link Reserve/Resolve
    ▼
cluster-ctl registry  ◄──── node_link ────► node-ctl conductor serve × N
    ▲
    │ placer_link Place / verify-key
    ▼
cluster-ctl placer
```

### 6.1 进程

| 进程 | 角色 | 数量 | 启停 | 归属 |
|---|---|---|---|---|
| `cluster-ctl registry` | registry 自聚簇成员;复制 `route_link` / `node_link` / `node_list` / `placer_link` 执行态,承载 node 长连接和 route/node owner RPC | 1 或 N 副本;每个 group/node 由 LocateN 选 owner set | systemd | 平台内,`cluster.md` |
| `cluster-ctl router` | e2b 兼容统一入口(`api.<domain>` 控制面 + 数据面),持近期 route cache;数据面 miss 时 Resolve 并对已知非 READY route 做 data Reserve,create/connect 使用对应 Reserve operation | N 副本(LB 后,无状态)| systemd | 平台内,`cluster-router.md` |
| `cluster-ctl placer` | group provider/importer、WATCH_LIST 消费方与放置调度器;向 registry 提供 PlaceSandbox / PlaceBuild / verify-key | N 副本;按 placer memberlist ready 视图和 group 确定性 failover | systemd | 平台内,`cluster-placer.md` |

小规模可三角色同机共置;大规模按 registry 成员表、router 入口副本和 placer 副本分别扩展。

### 6.2 端口

| 进程 | 监听 | 协议 | 用途 |
|---|---|---|---|
| `cluster-ctl router` | `:443` | HTTPS/h2 | **对外** e2b 控制面 + 数据面入口(机群唯一北向面)|
| `cluster-ctl registry` | `member.listen`,如 `:7700` | JSONRPC over HTTP/h2c 或 HTTPS | 统一控制面;按 path 承载 `/node-link/*`、`/route-link/*`、`/placer-link/*`、`/cluster/membership`、`/internal/registry-member/*`、`/internal/memberlist/*` |
| `cluster-ctl registry` | `node_link.listen`(可选) | JSONRPC over HTTP/h2c 或 HTTPS | 可选独立 node 长连接监听;为空时复用 `member.listen` |
| `cluster-ctl placer` | `placer.listen`,如 `:7800` | JSONRPC over HTTP/h2c 或 HTTPS | placer Place / verify-key API;memberlist HTTP transport 复用同一监听 |

### 6.3 与节点 / 平台管理面的关系

- **节点接入**:每 compute 节点 `node-ctl conductor serve` 配 registry node_link endpoint,拨入 node-link。接入成员
  可以 redirect 到 node owner,或 relay 到首个可用 owner。`node_link` owner 复制完整节点视图;
  `route_link` owner 下发 create/connect/delete/build/key 命令时,通过 node-owner RPC 转给当前
  `link_owner`。
- **平台管理面(平台外)**:向 placer/provider 侧导入 sandbox-group 配置(租户 `manifest_key`、
  `api_secret`、沙箱初始化配置、镜像仓库、模板、nodeSelectors)。registry 不实现 group provider,
  只在 Reserve/Place 冷路径把请求转给 ready placer。凭据对分发是 create/build 前置条件;
  drop 或租约过期不修改已经复制到现有 sandbox/build 记录的凭据对。
- **成员关系**:registry 成员表由版本化配置分发,通过信号或 API reload。`memberlist` 复用 HTTP 控制面,
  只做 failure detection 和 meta 传播,不维护成员清单,不参与 `LocateN` 分片计算。
- **成员变更**:registry 可同时持有 active / next membership。受影响的 group/node 逻辑 owner set 为
  old/new 并集,提交要求 old quorum + new quorum;node 上报、node_list 投影、group 请求和 placer import/source
  驱动数据自然复制到新 owner set。router/node/placer 通过 `/cluster/membership` 刷新 active/next 视图。

### 6.4 故障域

| 故障 | 影响 | 自愈 |
|---|---|---|
| 单个 `registry` 成员崩溃 | 其参与的逻辑分片降一格;quorum 仍满足时继续服务,不足时该分片停写 | 成员恢复后通过 quorum read / read-repair catch-up;router 本地 cache 使**已建立会话热路径不受影响** |
| registry 整集群完全下电但 shard 数据保留 | 下电期间 Reserve/Place 不可用 | registry quorum 恢复后读取原 shard 数据,node-link 重连并继续收敛 |
| registry 执行 shard 不可恢复地丢失 | 不得从备份构造 sandbox/build 节点执行态 | provider 数据按其持久化流程恢复;存活 node 的执行投影恢复和 migration-token 持久 route 分别由 [orchestrator #34](https://github.com/kuasar-sandbox/orchestrator/issues/34)/[#33](https://github.com/kuasar-sandbox/orchestrator/issues/33) 跟踪,完成前须明确报告不可恢复 |
| `router` 崩溃 | 该副本连接断 | 无状态,LB 改路由其余副本 |
| `placer` 崩溃 | 该 placer 不再作为 ready 候选;冷放置 failover 到同 group 的其他 placer | 热路径不受影响;Place 超时后 registry 换下一个候选 |
| 单 compute 节点 node-link 失联 | registry 暂失该节点视图 | 节点重连重报;node_dead_after 后 node_list 失效,placer 不再放置到该节点;孤儿 sandbox 按 group+sandbox_id 清理 |

## 7. 全景拓扑

按节点角色分三张子图。每张图自闭合:外部端点用 `(...)` 标注,实体在其他
子图或 region 级。

### 7.1 Compute Node

```
   ┌─ Compute Node  ( × ~5,000 per AZ ) ──────────────────────────────────────────────────────────────┐
   │                                                                                                  │
   │   ── process tree ──                                                                             │
   │                                                                                                  │
   │   (Platform Mgmt Plane, region)                                                                  │
   │           │  attach + per-sandbox SANDBOX_CONFIG / MANIFEST_CONFIG                               │
   │           ▼                                                                                      │
   │   node-ctl            ── StartUnit ──►  sandbox-ctl × ~3K  ── spawns ──►  cloud-hypervisor       │
   │                                                │                                  │              │
   │                                                │                                  ▼              │
   │                                                │                              guest VM           │
   │                                                │                                                 │
   │                                                └── UDS  /run/sandbox-resource.sock ──► node-ctl  │
   │                                                                                                  │
   │   ── data path ──                                                                                │
   │                                                                                                  │
   │   sandbox-ctl  ── wire ObjectGet :7070 ──►  cache-ctl  tiered                                    │
   │                                                  │   L1 RocksDB                                  │
   │                                                  ├── wire EC fan-out (5 shards) ──► (L2 cluster) │
   │                                                  └── origin gRPC :7100 ──► store-ctl  (sidecar)  │
   │                                                                                       │          │
   │                                                                                       ▼  HTTPS   │
   │                                                                                   (OBS bucket)   │
   │                                                                                                  │
   └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 L2 Cache Cluster

```
                            wire EC fan-out  (5 shards / chunk)
                            from every Compute Node's cache-ctl tiered
                                                │
                                                ▼
   ┌─ L2 Cache Cluster  ( 100-200 nodes per AZ ) ─────────────────────────────────────────────────────┐
   │                                                                                                  │
   │      cache-ctl  shard      wire :7070   /   gRPC :7071                                           │
   │      RocksDB on SSD                                                                              │
   │      Maglev placement:   LocateN( chunk_hash, 5 )  over full peer pool   (RS 4+1)                │
   │                                                                                                  │
   │      no L3 / no OBS from here — L3 origin is each Compute Node's local store-ctl                 │
   │                                                                                                  │
   └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 7.3 Image Build (in-sandbox, on Compute Node)

```
   ┌─ Compute Node ── e2b template build  (§5) ───────────────────────────────────────────────────────┐
   │                                                                                                  │
   │   node-ctl          ── assign ──►  sandbox-builder@<run-id>  ( run-builder, resident )            │
   │                                              │  drives 3 stage VMs (sandbox-ctl run + CH)         │
   │                                              ▼                                                    │
   │     A import ──► B steps ──► C template      ( guest: flatten-ctl / envd; tenant net stays in VM )│
   │                                              │  image.img / snapshot bundle  (host workdir)       │
   │                                              ▼                                                    │
   │   manifest-ctl store  /  sandbox-ctl upload-snapshot  ──►  store-ctl  (sidecar)  ──► (OBS bucket) │
   │                                                                                                  │
   └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

构建复用 compute 节点既有的 `store-ctl`(收尾上传)与 vswitch 网络槽(guest 拉取出网);
无独立池、无额外常驻进程(§5)。

## 8. 启停依赖

### 8.1 启动顺序

**Region 级(一次性)**

1. OBS 桶就绪;`store-ctl` 凭据可达

**L2 Cache Cluster(在 compute 之前)**

2. `cache-ctl shard` × 100-200 全部启动并健康
3. 集群成员清单(`tiers[].cluster.peers`)落到 compute node 配置仓

**Compute Node(每节点独立)**

4. `store-ctl`(sidecar)→ active generation 已 init,gRPC 健康
5. `cache-ctl tiered` → 与 L2 peer 拨号、与本机 `store-ctl` origin 拨号成功
6. `node-ctl conductor serve`(`resource_listen`)→ state 恢复或冷启;e2b SDK
   直连或 cluster registry 接入后开始接受新沙箱

注:`cache-ctl tiered` 启动**不需要**等 L2 全员在线——tier chain 把瞬时
故障层视作 miss 下穿(详见 `docs/cache.md` §错误模型)。**写**路径
(`manifest-ctl store` 直连 `store-ctl`)在 OBS / store-ctl 不可达时会失败。

模板构建复用 compute 节点的常驻进程(`node-ctl` + `store-ctl`),无独立启停依赖;
`node-ctl` 就绪后即可经 e2b API 接受构建(§5)。

### 8.2 关闭顺序(自顶向下)

1. 平台管理面 / cluster / 客户端停止向该节点 `node-ctl` 调度新沙箱
2. `node-ctl` 等待存量沙箱自然退出 / 主动 snapshot,然后 SIGTERM,persist 状态(含资源预算)后退出
3. `cache-ctl tiered` → SIGTERM,等 in-flight 请求结束,RocksDB flush
4. `store-ctl` → 同上

L2 cluster 的关闭与 compute node 关闭无强序——每个 compute node 的 `cache-ctl
tiered` 自己处理 L2 不可达。

## 9. 故障域

| 故障 | 直接影响 | 自愈 |
|---|---|---|
| 单 compute node `store-ctl` 崩溃 | 本机 L3 read/write 停;L1+L2 命中不受影响 | systemd 重启;无状态前端,秒级恢复 |
| 单 compute node `cache-ctl tiered` 崩溃 | 本机沙箱新 fault 卡 wire dial | systemd 重启;RocksDB 持久化 ⇒ L1 命中不丢 |
| 单 `cache-ctl shard` 节点崩溃 | RS 4+1 容 1 节点故障;L2 仍服务 | systemd 重启;tiered 端 Maglev 表在该 peer 不可达期间把请求路由到其余 4 + 1 parity |
| 同 RS 组中 ≥ 2 `cache-ctl shard` 同时崩溃 | 部分 `(chunk, idx)` 落到 ≥ 2 故障 peer 上,该 chunk L2 miss | 读路径 fallthrough origin(慢但正确);避免方式:成员变更**一次只动 1 peer** |
| `node-ctl` 崩溃 | 北向 API 中断,新沙箱无法拉起 / admit 失败;存量沙箱保持上次 grant 继续跑(资源仲裁随进程在本机) | systemd 重启;状态在 sqlite(`db_path`,默认 `<base_root>/node-ctl.db`)持久化 + 资源 `state.json` 在 tmpfs(扫 cgroup 重建),以 `ListUnitsByPatterns("sandbox-runner@*.service")` 的存活单元对账 sqlite `sandboxes` 表重挂(active+running⇒adopt 重武装 TTL;running 无单元⇒标 dead;`paused`/snapshot 记录保留可被 connect/auto-resume 拉起) |
| OBS 区域不可达 | 整 region L3 不可达 | 已 L1/L2 命中的沙箱继续跑;依赖新 L3 的写路径 / cold-image fault / 展平上传失败 |
| compute node 整机故障 | 该节点全部沙箱失效 | 平台管理面调度走 |
| L2 cluster > parity 同时故障 | L2 整体不可用 | tiered fallthrough origin(slow path 持续);恢复后自然恢复 |

## 10. 部署规模示例

### 10.1 开发 / PoC(单机)

```
单机:  store-ctl       (fs backend, /var/store)
       cache-ctl       (mode: local,无 L2)
       sandbox-ctl × N (node-ctl 可省,手工 run)
```

无 L2 cluster、无 OBS、无独立资源控制器(资源仲裁随 `node-ctl conductor serve` 内置,`resource_listen`
未配则静态 cgroup);`manifest-ctl` 走本机 `store-ctl` + `cache-ctl`。对应 `docs/cache.md`
§3.2 (local 模式)。**单机直供 e2b SDK,无需 cluster 层(`node-ctl conductor serve` 即北向面)**。

### 10.2 生产单 AZ

```
Compute:           ~5,000 节点(每节点 ~3K microVM;e2b 模板构建在构建沙箱内)
L2 Cache Cluster:  100-200 节点(RS 4+1,Maglev 全池放置)
Region 级:         OBS 桶 + 平台管理面
```

每 compute 节点跑:`store-ctl` + `cache-ctl tiered` + `node-ctl`(serve,含
`resource_listen`)+ `sandbox-ctl × ~3K`。
Cluster Control Plane:  registry 自聚簇(N 副本,按 group/node 逻辑分片)+ router(N 副本 LB 后)+ placer(N 副本)。

### 10.3 多 AZ

每 AZ 独立 compute 集群 + L2 cluster;region 级共享同一组 OBS 桶。Compute
节点的 `cache-ctl tiered` 配置只列本 AZ L2 peer,最小化跨 AZ 流量。
跨 AZ 去重在 OBS 桶级别天然达成(同 manifest → 同 chunk 密文哈希)。

## 11. 配置入口速查

每个模块的完整 yaml schema 在自身文档里,本节只给入口指针。

| 进程 | 配置位置 | 部署惯例 | Schema 文档 |
|---|---|---|---|
| `store-ctl` | `--config <path>` | `listen: 127.0.0.1:7100`(节点本机)| 源仓 `accelerator/docs/store.md` §3;发布包 `docs/store.md` |
| `cache-ctl tiered` | `--config <path>` | `listen: 127.0.0.1:7070`(节点本机);`tiers[].cluster.peers` 写本 AZ L2 全集群 | 源仓 `accelerator/docs/cache.md` §3.4;发布包 `docs/cache.md` |
| `cache-ctl shard` | `--config <path>` | `listen: 0.0.0.0:7070`(对外服务)| 源仓 `accelerator/docs/cache.md` §3.3;发布包 `docs/cache.md` |
| `node-ctl conductor serve(resource_listen)` | `/etc/node-ctl/conductor.yaml` 的内联 `resource_listen` 块 | `socket: /run/sandbox-resource.sock` | 源仓 `orchestrator/docs/node-resource.md` §3;发布包 `docs/node-resource.md` |
| `cluster-ctl registry` | `--config /etc/cluster-ctl/registry.yaml` | `member.id/listen`;`membership.active/versions[].members[].advertise/node_advertise/owners`;`node_link`、`route_link`、`node_list`、`placer_link` | 源仓 `orchestrator/docs/cluster.md`;发布包 `docs/cluster.md` |
| `cluster-ctl router` | `--config /etc/cluster-ctl/router.yaml` | `registry.bootstrap` 指向 registry 控制面;router `:443`(LB 后 N 副本);请求必须带 `X-Kuasar-Sandbox-Group` | 源仓 `orchestrator/docs/cluster-router.md`;发布包 `docs/cluster-router.md` |
| `cluster-ctl placer` | `--config /etc/cluster-ctl/placer.yaml` | `placer.id/listen/advertise/memberlist_label`;`registry.bootstrap`;`import_groups[]`;`placement` | 源仓 `orchestrator/docs/cluster-placer.md`;发布包 `docs/cluster-placer.md` |
| `sandbox-ctl run` | `--config <path>`(`SANDBOX_CONFIG`)+ `--manifest-config <path>`(`MANIFEST_CONFIG`)| **per-sandbox**,由 `node-ctl` 生成,落在 `/run/sandbox/<sid>/` | 源仓 `sandboxer/docs/sandbox.md` §3;发布包 `docs/sandbox.md` |
| `manifest-ctl` | `--manifest-config <path>`(`MANIFEST_CONFIG`)| 与 `sandbox-ctl` 共享格式;只连本机 store-ctl + cache-ctl | 源仓 `accelerator/docs/manifest.md` §3;发布包 `docs/manifest.md` |
| `flatten-ctl` | CLI flag + `--manifest-config`(`MANIFEST_CONFIG`,`--upload` 时)+ `--config`(`FLATTEN_CONFIG`,registry 源时);凭据走 `FLATTEN_REGISTRY_*` env | 经单一 guest runtime 在构建沙箱 guest 内运行(`run-builder` 驱动,§5)| 源仓 `guest-runtime/docs/flatten.md` §2;发布包 `docs/flatten.md` |

构建产物路径、跨架构、release 打包见 `orchestrator/release-builder/README.md`
和 `orchestrator/release-builder/scripts/release.sh`;发布包解压与测试入口见
`test/QUICKSTART.md`;性能基线、回归 checklist 见 [`perf.md`](perf.md)。

## 12. See Also

- [`docs/kuasar-sandbox.md`](kuasar-sandbox.md) — 系统设计总览:业务目标、子系统分工、端到端数据流
- `sandboxer/docs/sandbox.md`(发布包:`docs/sandbox.md`) — compute node 上 `sandbox-ctl` 的完整生命周期
- `accelerator/docs/cache.md`(发布包:`docs/cache.md`) §3.1 — `local` / `shard` / `tiered` 三形态选择;§4.9 Maglev 一致性哈希
- `accelerator/docs/store.md`(发布包:`docs/store.md`) — 后端选择(fs / obs)与代轮转
- `orchestrator/docs/node-resource.md`(发布包:`docs/node-resource.md`) — 节点资源控制协议
- `orchestrator/docs/node.md` — e2b 兼容控制面与节点主机;`cluster.md` — 集群级注册表 / 路由 / 放置
- `accelerator/docs/manifest.md`(发布包:`docs/manifest.md`) — `MANIFEST_CONFIG` 格式与 loader 契约
