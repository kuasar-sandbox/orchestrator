# deployment — 部署拓扑与组件清单

Mass Sandbox 由若干**独立部署的进程**组成,通过网络协议(gRPC / wire / vsock /
UDS)协作。本文档定义这些进程在生产部署中的归属、责任边界、配置入口与启停
依赖,供运维与 SRE 使用。

各模块的 CLI、配置 schema、内部设计在自己的文档里(`docs/<模块>.md`);本文档
**不**重复这些细节,只回答"东西在哪、彼此怎么找到对方、谁先起谁后起"。

## 1. 角色概览

四类节点角色,职责正交,独立扩缩:

| 角色 | 数量级 | 职责 | 关键进程 |
|---|---|---|---|
| Compute Node | 集群规模 × 数千 | 承载客户沙箱(microVM);每节点常驻 ~3K microVM | `cache-ctl`(tiered)、`sandbox-ctl × N`、可选 `node-ctl` |
| L2 Cache Cluster | 每 AZ × 5 节点起步,RS k=4 m=1 一组 | 分布式 EC 缓存,吸收 L1 miss 把 L3 命中压到 < 0.1% | `cache-ctl`(shard)|
| Storage Layer | 每 AZ × 数节点 + OBS | 内容寻址持久化,代管理与 GC | `store-ctl`、OBS 桶 |
| Image Build / Admin | 按需 | 镜像确定性展平、清单写入与运维巡检 | `flatten-ctl`、`manifest-ctl` CLI |

```
                       ┌──────────────────────────────┐
                       │  Storage Layer (per AZ)      │
                       │  ┌────────────────────────┐  │
                       │  │ store-ctl × M          │  │
                       │  │   fs / obs backend     │  │
                       │  │   __meta/generations   │  │
                       │  └────────┬───────────────┘  │
                       │           │                  │
                       │           ▼                  │
                       │  ┌────────────────────────┐  │
                       │  │ OBS buckets            │  │
                       │  │   chunk / manifest /   │  │
                       │  │   __meta               │  │
                       │  └────────────────────────┘  │
                       └──────────────▲───────────────┘
                                      │ gRPC Get/Put/GetSalt
                                      │ (L3 origin)
                       ┌──────────────┴───────────────┐
                       │  L2 Cache Cluster (per AZ)   │
                       │  ┌─ cache-ctl shard ──────┐  │
                       │  │   wire server (TCP)    │  │ × 5 (RS 4+1)
                       │  │   RocksDB on SSD       │  │
                       │  └────────▲───────────────┘  │
                       └───────────┼──────────────────┘
                                   │ wire ShardGet/Put × 5
                                   │ (EC client fan-out)
   ┌───────────────────────────────┴──────────────────┐
   │  Compute Node (× 1000s)                          │
   │                                                  │
   │  cache-ctl tiered ───── wire ObjectGet ◄─── sandbox-ctl × N
   │    L1 RocksDB                                    │   │
   │    EC client ──► L2 cluster                      │   │ vhost-user-blk
   │    origin    ──► store-ctl                       │   │ + uffd handler
   │                                                  │   ▼
   │  node-ctl (opt) ◄── UDS ResAlloc ──── sandbox-ctl ─►  cloud-hypervisor
   │                                                          │
   │                                                          ▼
   │                                                       guest VM
   │                                                       (sandbox-init,
   │                                                        vmlinux,
   │                                                        runtime.erofs)
   └──────────────────────────────────────────────────┘

   ┌──── Image Build / Admin (按需) ───────────────────┐
   │  flatten-ctl  | stdin OCI → stdout EROFS         │
   │  manifest-ctl | store / load / put / get / verify│
   │                  ─── 走远端 cache-ctl + store-ctl │
   └──────────────────────────────────────────────────┘
```

## 2. Compute Node

### 2.1 常驻进程

| 进程 | 角色 | 数量 | 启停 |
|---|---|---|---|
| `cache-ctl`(`mode: tiered`)| 节点本地数据入口:L1 RocksDB + EC 客户端 + L3 origin | 单实例 | systemd,先于 sandbox-ctl |
| `node-ctl`(`daemon`)| 节点级资源控制器(可选,见 §2.4) | 单实例 | systemd |
| `sandbox-ctl`(`run`) | 单个沙箱的控制平面(类 runc) | 每沙箱一个,~3K | 由上层调度器 fork-exec |
| `cloud-hypervisor` | VMM(patched) | 每沙箱一个,作为 `sandbox-ctl` 子进程 | sandbox-ctl 派生 |

`sandbox-ctl` 的进程模型是**前台运行直到 VM 退出**(类 `runc run`),不是
daemon。上层调度器(K8s pod、custom orchestrator 等)负责 fork-exec、收
exit code、清理 `/run/<sid>/`。

### 2.2 持久化与运行时目录

```
/var/cache/accel-l1/             cache-ctl 的 RocksDB(L1,典型 SSD 1 TiB)
/var/store/                      store-ctl 用 fs backend 时的数据根(通常不在 compute node)
/run/<sid>/                      sandbox-ctl 运行时 socket + blk1.diff(tmpfs / 节点本地 NVMe)
/var/lib/sandbox/<sid>/snap/     snapshot 中转目录
/run/node-ctl/                   node-ctl audit + state(tmpfs)
/opt/sandbox/vmlinux             guest kernel(节点级共享)
/opt/sandbox/sandbox-runtime.erofs  guest 运行时 image(节点级共享,DAX 共享 host page cache)
```

### 2.3 端口

| 进程 | 监听 | 协议 | 用途 |
|---|---|---|---|
| `cache-ctl tiered` | `0.0.0.0:7070` | wire(自定义二进制 TCP)| 数据面:`sandbox-ctl` / `manifest-ctl` 拉 chunk |
| `cache-ctl tiered` | `0.0.0.0:7071` | gRPC | health / `ping` / `info` |
| `node-ctl daemon` | `/run/sandbox-resource.sock` | UDS,自定义协议 | 沙箱资源协议 |
| `sandbox-ctl` | `/run/<sid>/*.sock` | UDS | 沙箱 per-instance(ch.sock / blk0.sock / blk1.sock / uffd.sock / ctl.sock / vsock.sock + `_5000`)|

端口都可在 yaml 改;表中是模块文档里的默认值。

### 2.4 node-ctl 何时部署

`node-ctl` 是**可选**组件,只在沙箱配置启用动态资源控制模式
(`control.controller` 非空)时需要。三种部署形态对应的依赖:

| 部署形态 | `sandbox.yaml` `control.cgroup_path` | `sandbox.yaml` `control.controller` | 需要 `node-ctl`? |
|---|---|---|---|
| 无 cgroup | 空 | 空 | 否 |
| 静态 cgroup | 非空 | 空 | 否 |
| 动态控制 | 非空 | 非空(指向 `node-ctl` UDS) | **是** |

不启用动态控制的节点上不要起 `node-ctl`——空跑无收益。

## 3. L2 Cache Cluster

### 3.1 集群规格

- **成员数**:RS 4+1(`data_shards: 4, parity_shards: 1`)→ 每个 chunk 编码为
  5 个 shard,分别写到 5 个不同的 `cache-ctl shard` 节点。可扩展到更高
  m(更高容错代价更大冗余)
- **机型**:大盘 SSD(~5 TiB / 节点);RAM 占 8% (`mem_ratio: 0.08`)作 RocksDB
  BlockCache
- **网络**:同 AZ 内 10/25 GbE
- **隔离**:不同应用域(镜像 chunk / 快照 chunk)可独立部署集群实例,同一套
  软件配置不同 RocksDB path + 不同集群成员
- **持久化**:RocksDB on `/mnt/ssd/accel-l2`,daemon 进程崩溃可热重启不丢数据

### 3.2 端口

| 进程 | 监听 | 协议 | 用途 |
|---|---|---|---|
| `cache-ctl shard` | `0.0.0.0:7070` | wire | shard PUT/GET(由 Compute Node 上 tiered cache-ctl 发起)|
| `cache-ctl shard` | `0.0.0.0:7071` | gRPC | health / `info` |

`shard` 模式既不访问 L3 也不持有任何 origin 凭据,纯 KV——这是它能水平扩展、
彼此对等无主的前提。

### 3.3 成员变更

集群成员的 `endpoint` 列表写在每个 Compute Node 上 `cache-ctl tiered` 的
yaml 里(`tiers[].cluster.peers`)。增删节点是 Compute Node 端的**重启配置变更**;
shard 节点本身无须感知集群成员。详细 rebalance 与一致性窗口见 `docs/cache.md`。

## 4. Storage Layer

### 4.1 store-ctl 部署形态

```yaml
listen: 0.0.0.0:7060
backend: obs              # 或 fs(本地文件系统,典型用于开发/小集群)
obs:
  bucket: container-accelerator-prod
  prefix: store/
```

- 数量:每 AZ 数节点(横向扩 Get/Put 吞吐),共用同一个 OBS 桶
- 数据本身在 OBS 桶里;`store-ctl` 进程是**无状态前端**(除 `__meta/generations`
  的轻量 active gen 状态外),崩了直接重启,不丢数据
- 凭据来源:yaml 显式(`access_key`/`secret_key`)→ `~/.obsconfig` → AWS SDK
  默认凭证链(IMDS),按优先级取
- `fs` backend 用于开发 / 单节点 / NAS 测试环境;生产用 `obs`

### 4.2 端口

| 进程 | 监听 | 协议 | 用途 |
|---|---|---|---|
| `store-ctl` | `0.0.0.0:7060` | gRPC | Put / Get / GetSalt |
| `store-ctl` | `0.0.0.0:7061` | gRPC health | 探针 |

### 4.3 代生命周期与运维

代轮转(Active → Retired → Expired → Deleted)走 `store-ctl admin` 子命令,
**直接操作后端,绕过 gRPC**,与运行中的 daemon 共存(共享 `__meta/generations`)。
日常 GC、salt 旋转、迁移均不影响 daemon 在线读写。详见 `docs/store.md`。

## 5. Image Build / Admin

### 5.1 镜像展平

```
docker save app:v1 | flatten-ctl > app.erofs           # 本地落盘
flatten-ctl --image oci:./app | manifest-ctl store \
    --put-manifest --manifest-config build-host.yaml   # 直接入 store
```

- `flatten-ctl` 是**一次性 CLI**:OCI/Docker bundle → 确定性 EROFS,逐字节
  可重现。无 daemon
- 数据面节点上同时部署 `manifest-ctl`,凭 `MANIFEST_CONFIG` yaml 找到远端
  `cache-ctl` + `store-ctl` 端点;`manifest-ctl store --put-manifest` 把镜像
  分块加密上链,返回 manifest key

### 5.2 运维客户端

| 操作 | 命令 | 备注 |
|---|---|---|
| 写入 | `manifest-ctl store [--put-manifest]` | 走 store-ctl gRPC `Put` |
| 读取 | `manifest-ctl load [--get-manifest <hex>]` | 走 cache-ctl tiered wire `ObjectGet` |
| 校验 | `manifest-ctl verify --manifest <ref>` | 拉所有 chunk 验完整性 |
| 去重对比 | `manifest-ctl diff <a> <b>` | 客户端纯计算 |
| store-ctl 代管理 | `store-ctl admin gen ...` | 直接打开后端 |
| cache-ctl 状态 | `cache-ctl info --endpoint <health_listen>` | gRPC,实时 stats |
| node-ctl 状态 | `node-ctl status` / `node-ctl list` | 本机 UDS |
| sandbox-ctl 快照 | `sandbox-ctl snapshot --sandbox-id <sid>` | 联系 `/run/<sid>/ctl.sock` |

`manifest-ctl` 与 `flatten-ctl` 都是 CLI,不留长进程;放在镜像构建节点 / 跳板
机 / CI runner 上按需调用。

## 6. 全景拓扑

```
                                            ┌─ Storage Layer (per AZ) ──────────┐
                                            │                                   │
                                            │   ┌── store-ctl ──┐               │
                                            │   │  gRPC :7060   │   × M ───►    │  OBS / NAS
                                            │   │  __meta gens  │               │
                                            │   └──────▲────────┘               │
                                            │          │                        │
                                            └──────────┼────────────────────────┘
                                                       │ origin (gRPC)
                                            ┌──────────┴────────────────────────┐
                                            │  L2 Cache Cluster (per AZ)        │
                                            │                                   │
                                            │   ┌── cache-ctl shard ──┐         │
                                            │   │  wire   :7070       │ × 5    │
                                            │   │  RocksDB on SSD     │  (RS   │
                                            │   └─────────▲───────────┘   4+1) │
                                            └─────────────┼─────────────────────┘
                                                          │ wire (EC fan-out)
   ┌──────────────────── Compute Node × thousands ────────┴──────────────────┐
   │                                                                          │
   │  ┌── cache-ctl tiered ──────┐                                            │
   │  │   wire :7070  L1 RocksDB │  ─── ObjectGet ───►  L2 cluster            │
   │  │   tiered chain           │  ─── origin   ───►  store-ctl              │
   │  └───────────▲──────────────┘                                            │
   │              │ wire ObjectGet                                            │
   │              │                                                           │
   │  ┌── sandbox-ctl × N (per-VM) ───────────────────┐                       │
   │  │   vhost-user-blk × 2 (blk0 ro / blk1 cow)     │                       │
   │  │   uffd handler (memfd-backed RAM)             │                       │
   │  │   BalloonController (vm.resize, §sandbox.md)  │                       │
   │  │   launch / ping / snapshot / restore vsock    │                       │
   │  │   ───────────────────────────────────────────  │                      │
   │  │   spawns: cloud-hypervisor (patched)          │  ── KVM ──► guest VM  │
   │  │                                               │              │       │
   │  │                                               │              ▼       │
   │  │                                               │       sandbox-init   │
   │  │                                               │       (PID 1)        │
   │  │                                               │       + user app     │
   │  └───────────────────▲───────────────────────────┘                      │
   │                      │ UDS /run/sandbox-resource.sock                   │
   │                      │ (optional, 动态控制模式)                          │
   │  ┌── node-ctl daemon ┴──┐                                               │
   │  │   admission / alloc  │                                               │
   │  │   sensor heartbeat   │                                               │
   │  └──────────────────────┘                                               │
   └──────────────────────────────────────────────────────────────────────────┘

   ┌──── Image Build / Admin nodes (按需) ────────────────────────────────────┐
   │  flatten-ctl  (CLI)  → 确定性 OCI → EROFS                                │
   │  manifest-ctl (CLI)  → store / load / verify / diff,走远端 cache+store  │
   └──────────────────────────────────────────────────────────────────────────┘
```

## 7. 启停依赖

### 7.1 启动顺序(自底向上)

1. **OBS 桶就绪**(若 `store-ctl` 用 obs backend);凭据可达
2. **`store-ctl` × M**:启动 daemon,确认 active generation 已 init
3. **L2 cluster `cache-ctl shard` × 5**:启动 5 个 shard 节点
4. **Compute Node `cache-ctl tiered`**:配置里 `tiers[].cluster.peers` 与
   `origin.store.endpoint` 已可达
5. **Compute Node `node-ctl daemon`**(若启用动态控制)
6. **Compute Node 接收沙箱调度**:每个新沙箱由上层 orchestrator
   `sandbox-ctl run --config ...` 拉起

注意:`cache-ctl tiered` 启动**不需要**等 L2 / L3 在线——tier chain 的容错
模型把瞬时故障层视作 miss 下穿(详见 `docs/cache.md` §错误模型)。但 L3
不可达时**写入**会失败(写路径 manifest-ctl → store-ctl 直连,不经 cache-ctl)。

### 7.2 关闭顺序(自顶向下)

1. orchestrator 停止调度新沙箱,等待存量沙箱自然退出或主动 snapshot
2. `node-ctl daemon`:发送 SIGTERM,daemon persist 状态后退出
3. `cache-ctl tiered`:SIGTERM 后等 in-flight 请求结束,RocksDB flush
4. `cache-ctl shard` × 5
5. `store-ctl` × M

不需要严格"先关沙箱"——sandbox-ctl 失去 cache-ctl 后,新发起的 fault
拿不到 chunk,沙箱内 page fault 卡住;关闭语义由上层 orchestrator 控制。

## 8. 故障域

| 故障 | 直接影响 | 自愈 |
|---|---|---|
| 单 Compute Node 上 `cache-ctl` 崩溃 | 本机沙箱新 fault 卡 wire dial | systemd 重启;RocksDB 持久化 ⇒ 重启后 L1 命中不丢 |
| 单 `cache-ctl shard` 节点崩溃 | RS 4+1 容 1 节点故障;L2 仍可服务 | systemd 重启;tiered 客户端在节点恢复前把请求路由到余下 4 个 + 1 parity |
| `store-ctl` daemon 崩溃 | 该节点 L3 read/write 停;L1+L2 命中不受影响 | systemd 重启;无状态前端,秒级恢复 |
| OBS 区域不可达 | 整 AZ L3 不可达 | 沙箱已 L1/L2 命中部分继续跑;依赖新 L3 的写路径 / cold-image fault 失败 |
| `node-ctl` 崩溃 | 长连断,沙箱保持上次 grant 继续跑;新沙箱 admit 失败 | systemd 重启;`state.json` 在 tmpfs 上,扫 cgroup 重建 |
| Compute Node 整机故障 | 该节点上沙箱全部失效;其他节点不受影响 | 上层 orchestrator 调度走 |

## 9. 部署规模示例

下表是参考起点,实际取决于工作负载形态(密度 × 镜像大小 × 快照频率)。

### 9.1 开发 / 小规模 PoC

```
单机:  store-ctl (fs backend, /var/store)
       cache-ctl (mode: local)        # 替代 tiered,无 L2
       sandbox-ctl × N(直接拉起)
```

无 L2 cluster,无 node-ctl;manifest-ctl 走本机 store-ctl + cache-ctl。

### 9.2 生产单 AZ

```
Storage:   3 × store-ctl(同 OBS 桶)
L2 cache:  5 × cache-ctl shard(RS 4+1)
Compute:   100-1000 × Compute Node(每节点 ~3K microVM)
Admin:     1-2 × build/CI 跳板,装 flatten-ctl + manifest-ctl
```

### 9.3 多 AZ

每 AZ 独立部署 L2 cluster + store-ctl 前端,共享同一个 OBS 桶。Compute Node
默认连本 AZ 的 L2 + store-ctl,minimize cross-AZ 流量;跨 AZ 的去重在 OBS
桶级别天然达成(同 manifest → 同 chunk 密文哈希)。

## 10. 配置入口速查

每个模块的完整 yaml schema 在自身文档里,本节只给入口指针:

| 进程 | 配置位置 | Schema 文档 |
|---|---|---|
| `store-ctl` | `--config <path>`,无 env | [`docs/store.md`](store.md) §3 |
| `cache-ctl` | `--config <path>` | [`docs/cache.md`](cache.md) §3 |
| `node-ctl daemon` | `/etc/node-ctl/node-ctl.yaml` 默认 | [`docs/node.md`](node.md) §3 |
| `sandbox-ctl run` | `--config <path>`(`SANDBOX_CONFIG` env)+ `--manifest-config`(`MANIFEST_CONFIG` env)| [`docs/sandbox.md`](sandbox.md) §3 |
| `manifest-ctl` | `--manifest-config <path>`(`MANIFEST_CONFIG` env)| [`docs/manifest.md`](manifest.md) §3 |
| `flatten-ctl` | 仅 CLI flag(无 yaml) | [`docs/flatten.md`](flatten.md) §2 |

构建产物路径、跨架构、release 打包见 [`docs/build.md`](build.md);性能基线、
回归 checklist 见 [`docs/perf.md`](perf.md)。

## 11. See Also

- [`docs/PROPOSAL.md`](PROPOSAL.md) — 系统级方案与业务模型
- [`docs/sandbox.md`](sandbox.md) — Compute Node 上 `sandbox-ctl` 的完整生命周期
- [`docs/cache.md`](cache.md) §3.1 — `local` / `shard` / `tiered` 三形态选择
- [`docs/store.md`](store.md) — 后端选择(fs / obs)与代轮转
- [`docs/node.md`](node.md) — 动态资源控制协议
