# deployment — 部署拓扑与组件清单

Mass Sandbox 由若干**独立部署的进程**组成,通过网络协议(gRPC / wire / vsock /
UDS)协作。本文档定义这些进程在生产部署中的归属、责任边界、配置入口与启停
依赖,供运维与 SRE 使用。

各模块的 CLI、配置 schema、内部设计在自己的文档里(`docs/<模块>.md`);本文档
**不**重复这些细节,只回答"东西在哪、彼此怎么找到对方、谁先起谁后起"。

## 1. 角色概览

资源分两个尺度:

**AZ 级集群**

| 角色 | 集群规模 | 职责 | 关键进程 |
|---|---|---|---|
| Compute Node | 每 AZ 一集群,~5,000 节点 | 承载客户沙箱(microVM),每节点 ~3K microVM | `orchestrator-agent`、`node-ctl`、`cache-ctl tiered`、`store-ctl`(sidecar)、`sandbox-ctl × N` |
| L2 Cache Cluster | 每 AZ 一集群,100-200 节点 | 分布式 EC 缓存(RS 4+1,Maglev 一致性哈希),吸收 L1 miss 把 L3 请求压到 < 0.1% | `cache-ctl shard` |

**Region 级共享资源**(由各自的平台管理面运营,本方案不展开)

| 资源 | 用途 |
|---|---|
| OBS 桶 | L3 chunk 与 Manifest 持久化(`store-ctl` 后端) |
| 平台管理面 | 沙箱实例调度、配置、租户管控 ↔ 与 compute 节点 `orchestrator-agent` 对话 |
| 镜像展平数据面节点(独立池) | 容器镜像拉取 + 确定性展平 + 写入 store(详见 §5) |
| 镜像展平管理面 | 调度展平任务,向数据面下发凭据 |

## 2. Compute Node

### 2.1 常驻进程

| 进程 | 角色 | 数量 | 启停 | 本方案归属 |
|---|---|---|---|---|
| `orchestrator-agent` | 本机沙箱编排代理:与平台管理面对话,拉取沙箱实例配置 / 客户密钥,生成 per-sandbox `SANDBOX_CONFIG` + `MANIFEST_CONFIG`,fork-exec `sandbox-ctl run`,处理生命周期事件回传 | 单实例 | systemd | **本方案外**,接口契约见 §2.4 |
| `node-ctl`(`daemon`)| 节点级资源仲裁:沙箱准入、内存预算分配、密度控制 | 单实例 | systemd | 本方案,`docs/node.md` |
| `cache-ctl`(`mode: tiered`)| 节点本地数据入口:L1 RocksDB + EC 客户端(→ L2)+ L3 origin | 单实例 | systemd,先于 orchestrator-agent | 本方案,`docs/cache.md` |
| `store-ctl` | 本机 OBS 读写代理(sidecar);**所有**远端 OBS 流量走这里 | 单实例 | systemd | 本方案,`docs/store.md` |
| `sandbox-ctl`(`run`) | 单个沙箱的控制平面(类 `runc run`);非 daemon | 每沙箱一个,~3K | 由 orchestrator-agent fork-exec | 本方案,`docs/sandbox.md` |
| `cloud-hypervisor` | VMM(patched);`sandbox-ctl` 子进程 | 每沙箱一个 | `sandbox-ctl` 派生 | 本方案,`docs/cloud-hypervisor.md` |

### 2.2 端口与套接字

所有节点本机进程默认监听 loopback 或 UDS,不向集群外暴露:

| 进程 | 监听 | 协议 | 用途 |
|---|---|---|---|
| `store-ctl` | `127.0.0.1:7060` | gRPC | `Put` / `Get` / `GetSalt`(本机 cache-ctl + manifest-ctl 调用)|
| `store-ctl` | `127.0.0.1:7061` | gRPC health | 探针 |
| `cache-ctl tiered` | `127.0.0.1:7070` | wire(自定义二进制 TCP)| 数据面:`sandbox-ctl` / `manifest-ctl` 拉 chunk |
| `cache-ctl tiered` | `127.0.0.1:7071` | gRPC | health / `ping` / `info` |
| `node-ctl daemon` | `/run/sandbox-resource.sock` | UDS,自定义协议 | 沙箱资源协议(`sandbox-ctl` 拨号目标)|
| `sandbox-ctl` | `/run/<sid>/*.sock` | UDS | sandbox 内部:`ch.sock` / `blk{0,1}.sock` / `uffd.sock` / `ctl.sock` / `vsock.sock`(+ `_5000`)|

`cache-ctl tiered` 的 EC 客户端通过节点对外网络拨号 L2 cluster 节点的 `7070`
端口(详见 §3)。除此之外 compute 节点上没有任何**对外**的服务端口。

### 2.3 持久化与运行时目录

```
/var/store/                          store-ctl 用 fs backend 时的本地数据(开发用;生产用 obs)
/var/cache/accel-l1/                 cache-ctl tiered 的 L1 RocksDB(典型 SSD 1 TiB)
/run/node-ctl/                       node-ctl audit + state(tmpfs)
/run/<sid>/                          每沙箱运行时目录:socket、blk1.diff、配置(tmpfs)
/var/lib/sandbox/<sid>/snap/         snapshot 中转(本地 NVMe)
```

### 2.4 节点共享资源目录

平台维护节点级共享文件,所有沙箱按名称引用,不复制:

```
/opt/sandbox/
  kernel/
    <ver>/vmlinux                    # 多版本并存,SANDBOX_CONFIG 按名引用
  runtime/
    <ver>/sandbox-runtime.erofs      # 多版本并存
  overlay-templates/
    overlay-1g.ext4                  # 预格式化空 ext4,大小不同的多份
    overlay-4g.ext4
    overlay-16g.ext4
```

引用方式(在 `SANDBOX_CONFIG` 里):

```yaml
boot:
  kernel:  file:///opt/sandbox/kernel/6.1.169-sandbox/vmlinux
  runtime: file:///opt/sandbox/runtime/v1/sandbox-runtime.erofs
  root:
    overlay:
      diff: file:///run/<sid>/blk1.diff   # 沙箱独占,见下
```

**复制约定**:`sandbox-runtime.erofs` 与 `vmlinux` **不复制**——`sandbox-ctl`
让 CH 以只读 mmap / 直接打开方式使用(DAX 共享 host page cache,N 个沙箱共一份
RAM 工作集)。**overlay-ext4 模板必须复制一份**——它是沙箱写层,运行期会被
修改。复制由 `orchestrator-agent` 在 `sandbox-ctl run` 之前完成(典型用
`cp --reflink=auto` 走 reflink 落 NVMe);`sandbox-ctl` 不负责选模板、不负责
复制,只按 `boot.root.overlay.diff` URL 打开既有文件。

### 2.5 per-sandbox 配置下发

每个沙箱有**独立的两份 yaml**,由 `orchestrator-agent` 在 fork-exec 之前生成
并通过 flag 传入:

| 文件 | 内容 | 传入方式 | 文档 |
|---|---|---|---|
| `SANDBOX_CONFIG` | 该沙箱的资源 / 启动 / 网络 / launch 配置 | `sandbox-ctl run --config <path>`,等价 `SANDBOX_CONFIG` env | `docs/sandbox.md` §3 |
| `MANIFEST_CONFIG` | 客户密钥 + 本机 store-ctl + cache-ctl 端点 + 分块 / 加密参数 | `sandbox-ctl run --manifest-config <path>`,等价 `MANIFEST_CONFIG` env;`manifest-ctl` 也吃同一份格式 | `docs/manifest.md` §3 |

要点:

- **per-sandbox MANIFEST_CONFIG**:每沙箱用各自租户的客户密钥;orchestrator-agent
  从平台管理面取密钥,落地为 `/run/<sid>/manifest.yaml`,生命周期跟沙箱走
- **共享格式**:`manifest-ctl` 与 `sandbox-ctl` 用**同一**配置格式;两者都
  **只**连本机 store-ctl(`127.0.0.1:7060`)+ 本机 cache-ctl(`127.0.0.1:7070`),
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

### 4.2 平台管理面 / 镜像展平管理面

均为 region 级、独立运营,本方案外。与本方案的接口:

- 平台管理面 ↔ 各 compute 节点 `orchestrator-agent`:沙箱实例配置 / 客户密钥 /
  生命周期事件
- 镜像展平管理面 ↔ 镜像展平数据面节点(§5):租户镜像拉取凭据、客户加密
  凭据下发

## 5. 镜像展平数据面节点

### 5.1 角色定位

独立池,与 compute node 不重叠。**纯写路径**——拉镜像、展平、写 OBS,**不读**
已存 manifest,因此**不部署 cache-ctl**。

| 进程 | 类型 | 用途 |
|---|---|---|
| `flatten-ctl` | CLI(一次性)| OCI / docker bundle → 确定性 EROFS,逐字节可重现 |
| `manifest-ctl` | CLI(一次性)| `store --put-manifest`:分块 + 加密 + 写远端 |
| `store-ctl` | daemon(sidecar)| 把 manifest-ctl 的 gRPC `Put` 写到 region OBS |

### 5.2 流水

```
   Flatten Mgmt Plane  (region)
            │
            │  tenant image pull credentials + customer encryption keys
            ▼
   flatten-ctl  (CLI)
            │  stdout:  deterministic EROFS
            ▼
   manifest-ctl  store --put-manifest
            │  gRPC 127.0.0.1:7060
            ▼
   store-ctl  (sidecar)
            │  HTTPS
            ▼
   OBS bucket  (region)
```

### 5.3 不部署的组件

- **cache-ctl**:无读路径,装了空跑
- **node-ctl**:不跑沙箱,不需要资源仲裁
- **sandbox-ctl** / **cloud-hypervisor**:不跑沙箱
- **orchestrator-agent**:这里由镜像展平管理面直接驱动

数据面节点上常驻只有 `store-ctl`;`flatten-ctl` + `manifest-ctl` 是按任务拉
起的 CLI。

## 6. 全景拓扑

按节点角色分三张子图。每张图自闭合:外部端点用 `(...)` 标注,实体在其他
子图或 region 级。

### 6.1 Compute Node

```
   ┌─ Compute Node  ( × ~5,000 per AZ ) ──────────────────────────────────────────────────────────────┐
   │                                                                                                  │
   │   ── process tree ──                                                                             │
   │                                                                                                  │
   │   (Platform Mgmt Plane, region)                                                                  │
   │           │  attach + per-sandbox SANDBOX_CONFIG / MANIFEST_CONFIG                               │
   │           ▼                                                                                      │
   │   orchestrator-agent  ── fork-exec ──►  sandbox-ctl × ~3K  ── spawns ──►  cloud-hypervisor       │
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
   │                                                  └── origin gRPC :7060 ──► store-ctl  (sidecar)  │
   │                                                                                       │          │
   │                                                                                       ▼  HTTPS   │
   │                                                                                   (OBS bucket)   │
   │                                                                                                  │
   └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 L2 Cache Cluster

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

### 6.3 Image Flatten Data-Plane Node

```
   ┌─ Image Flatten Data-Plane Node  ( separate pool ) ───────────────────────────────────────────────┐
   │                                                                                                  │
   │   (Flatten Mgmt Plane, region)                                                                   │
   │           │  tenant pull credentials + customer encryption keys                                  │
   │           ▼                                                                                      │
   │   flatten-ctl  (CLI)  ── stdout EROFS ──►  manifest-ctl  (CLI)                                   │
   │                                                  │  gRPC 127.0.0.1:7060                          │
   │                                                  ▼                                               │
   │                                             store-ctl  (sidecar)                                 │
   │                                                  │                                               │
   │                                                  ▼  HTTPS                                        │
   │                                             (OBS bucket)                                         │
   │                                                                                                  │
   └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

无 `cache-ctl`、无 `node-ctl`、无 `sandbox-ctl`——纯写路径不需要(§5.3)。

## 7. 启停依赖

### 7.1 启动顺序

**Region 级(一次性)**

1. OBS 桶就绪;`store-ctl` 凭据可达

**L2 Cache Cluster(在 compute 之前)**

2. `cache-ctl shard` × 100-200 全部启动并健康
3. 集群成员清单(`tiers[].cluster.peers`)落到 compute node 配置仓

**Compute Node(每节点独立)**

4. `store-ctl`(sidecar)→ active generation 已 init,gRPC 健康
5. `cache-ctl tiered` → 与 L2 peer 拨号、与本机 `store-ctl` origin 拨号成功
6. `node-ctl daemon` → state 恢复或冷启
7. `orchestrator-agent` → 与平台管理面 attach 后开始接受新沙箱

注:`cache-ctl tiered` 启动**不需要**等 L2 全员在线——tier chain 把瞬时
故障层视作 miss 下穿(详见 `docs/cache.md` §错误模型)。**写**路径
(`manifest-ctl store` 直连 `store-ctl`)在 OBS / store-ctl 不可达时会失败。

**镜像展平数据面节点(独立)**

- `store-ctl`(sidecar)起来后,展平管理面随时可调度任务,无其他常驻依赖。

### 7.2 关闭顺序(自顶向下)

1. 平台管理面停止向该节点 `orchestrator-agent` 调度新沙箱
2. `orchestrator-agent` 等待存量沙箱自然退出 / 主动 snapshot
3. `node-ctl` → SIGTERM,persist 状态后退出
4. `cache-ctl tiered` → SIGTERM,等 in-flight 请求结束,RocksDB flush
5. `store-ctl` → 同上

L2 cluster 的关闭与 compute node 关闭无强序——每个 compute node 的 `cache-ctl
tiered` 自己处理 L2 不可达。

## 8. 故障域

| 故障 | 直接影响 | 自愈 |
|---|---|---|
| 单 compute node `store-ctl` 崩溃 | 本机 L3 read/write 停;L1+L2 命中不受影响 | systemd 重启;无状态前端,秒级恢复 |
| 单 compute node `cache-ctl tiered` 崩溃 | 本机沙箱新 fault 卡 wire dial | systemd 重启;RocksDB 持久化 ⇒ L1 命中不丢 |
| 单 `cache-ctl shard` 节点崩溃 | RS 4+1 容 1 节点故障;L2 仍服务 | systemd 重启;tiered 端 Maglev 表在该 peer 不可达期间把请求路由到其余 4 + 1 parity |
| 同 RS 组中 ≥ 2 `cache-ctl shard` 同时崩溃 | 部分 `(chunk, idx)` 落到 ≥ 2 故障 peer 上,该 chunk L2 miss | 读路径 fallthrough origin(慢但正确);避免方式:成员变更**一次只动 1 peer** |
| `node-ctl` 崩溃 | 长连断,沙箱保持上次 grant 继续跑;新沙箱 admit 失败 | systemd 重启;`state.json` 在 tmpfs,扫 cgroup 重建 |
| `orchestrator-agent` 崩溃 | 与平台管理面失联,新沙箱无法拉起;存量沙箱不受影响 | systemd 重启,重新 attach |
| OBS 区域不可达 | 整 region L3 不可达 | 已 L1/L2 命中的沙箱继续跑;依赖新 L3 的写路径 / cold-image fault / 展平上传失败 |
| compute node 整机故障 | 该节点全部沙箱失效 | 平台管理面调度走 |
| L2 cluster > parity 同时故障 | L2 整体不可用 | tiered fallthrough origin(slow path 持续);恢复后自然恢复 |

## 9. 部署规模示例

### 9.1 开发 / PoC(单机)

```
单机:  store-ctl       (fs backend, /var/store)
       cache-ctl       (mode: local,无 L2)
       sandbox-ctl × N (orchestrator-agent 可省,手工 run)
```

无 L2 cluster、无 OBS、无 node-ctl;`manifest-ctl` 走本机 `store-ctl` + `cache-ctl`。
对应 `docs/cache.md` §3.2 (local 模式)。

### 9.2 生产单 AZ

```
Compute:           ~5,000 节点(每节点 ~3K microVM)
L2 Cache Cluster:  100-200 节点(RS 4+1,Maglev 全池放置)
镜像展平数据面:    独立池,按吞吐扩
Region 级:         OBS 桶 + 平台 / 展平管理面
```

每 compute 节点跑:`store-ctl` + `cache-ctl tiered` + `node-ctl` +
`orchestrator-agent` + `sandbox-ctl × ~3K`。

### 9.3 多 AZ

每 AZ 独立 compute 集群 + L2 cluster;region 级共享同一组 OBS 桶。Compute
节点的 `cache-ctl tiered` 配置只列本 AZ L2 peer,minimize cross-AZ 流量。
跨 AZ 去重在 OBS 桶级别天然达成(同 manifest → 同 chunk 密文哈希)。

## 10. 配置入口速查

每个模块的完整 yaml schema 在自身文档里,本节只给入口指针。

| 进程 | 配置位置 | 部署惯例 | Schema 文档 |
|---|---|---|---|
| `store-ctl` | `--config <path>` | `listen: 127.0.0.1:7060`(节点本机)| [`docs/store.md`](store.md) §3 |
| `cache-ctl tiered` | `--config <path>` | `listen: 127.0.0.1:7070`(节点本机);`tiers[].cluster.peers` 写本 AZ L2 全集群 | [`docs/cache.md`](cache.md) §3.4 |
| `cache-ctl shard` | `--config <path>` | `listen: 0.0.0.0:7070`(对外服务)| [`docs/cache.md`](cache.md) §3.3 |
| `node-ctl daemon` | `/etc/node-ctl/node-ctl.yaml` | `listen: /run/sandbox-resource.sock` | [`docs/node.md`](node.md) §3 |
| `sandbox-ctl run` | `--config <path>`(`SANDBOX_CONFIG`)+ `--manifest-config <path>`(`MANIFEST_CONFIG`)| **per-sandbox**,由 `orchestrator-agent` 生成,落在 `/run/<sid>/` | [`docs/sandbox.md`](sandbox.md) §3 |
| `manifest-ctl` | `--manifest-config <path>`(`MANIFEST_CONFIG`)| 与 `sandbox-ctl` 共享格式;只连本机 store-ctl + cache-ctl | [`docs/manifest.md`](manifest.md) §3 |
| `flatten-ctl` | 仅 CLI flag(无 yaml)| 仅在镜像展平数据面节点使用 | [`docs/flatten.md`](flatten.md) §2 |

构建产物路径、跨架构、release 打包见 [`docs/build.md`](build.md);性能基线、
回归 checklist 见 [`docs/perf.md`](perf.md)。

## 11. See Also

- [`docs/PROPOSAL.md`](PROPOSAL.md) — 系统级方案与业务模型
- [`docs/sandbox.md`](sandbox.md) — compute node 上 `sandbox-ctl` 的完整生命周期
- [`docs/cache.md`](cache.md) §3.1 — `local` / `shard` / `tiered` 三形态选择;§4.9 Maglev 一致性哈希
- [`docs/store.md`](store.md) — 后端选择(fs / obs)与代轮转
- [`docs/node.md`](node.md) — 资源控制协议
- [`docs/manifest.md`](manifest.md) — `MANIFEST_CONFIG` 格式与 loader 契约
