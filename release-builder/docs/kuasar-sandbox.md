# kuasar-sandbox — 大规模 microVM 沙箱平台

面向大规模 Agent 沙箱、传统 Serverless 与 RL 训练场景的 microVM 沙箱平台:亚秒级
冷启动与快照恢复、跨镜像/快照内容级去重的存储加速、单节点数千沙箱的密度与资源管控。
平台覆盖从 VMM/Guest 环境到数据按需加载的完整路径,支持两条运行时路径(块设备
vhost-user-blk、内存快照 userfaultfd)与三种启动模式(镜像冷启动、SnapStart、
Warm Pool),共享同一套基础设施:内容定义分块、收敛加密、内容寻址存储、分层缓存
与二进制清单(Manifest)。

实现按职责拆分为五个独立演进的子仓(§2.2),边界只暴露薄的、命名具体的纯 Go
导入面;系统级文档、跨仓 e2e/perf 套件与发布聚合由
`orchestrator/release-builder` 这个 umbrella 目录承载。本文是系统设计总览:
平台解决什么问题、子系统如何分工协作、数据如何端到端流动、关键机制与取舍。
模块级设计在各子仓 `docs/`,部署拓扑见
[`deployment.md`](deployment.md),实测性能基线见 [`perf.md`](perf.md)。

## 1. 概述

### 1.1 业务问题

三类负载在同一组诉求上收敛——按需加载 + 内容去重 + 安全加密 + 分层缓存:

| 诉求 | 传统 Serverless | Agent 在线服务 | RL 训练 |
|------|----------------|---------------|---------|
| 安全隔离 | 多租户隔离 | Agent 行为不可预测 | 沙箱间完全隔离 |
| 快速启动 | 亚秒冷启动 | 亚秒会话创建 | 毫秒级批量创建 |
| 大规模并发 | 万级/秒 | 千级同时在线 | 万级并发沙箱 |
| 成本可控 | 镜像存储 | 快照存储 | 海量镜像+快照 |

**启动延迟分解**。端到端启动 = VM 启动 + 数据加载 + 应用就绪。VM 启动(VMM +
内核 + init)本地基线约 125ms;数据加载是其后的首要瓶颈——Agent 镜像 1-5 GiB,
传统方式全量下载需秒级,且万级并发同时拉取的聚合带宽(PB/s 量级)远超网络承载;
应用就绪(服务预热、连接建立、模型加载)可达数秒,存储加速无法消除,只能提前
完成并冻结为内存快照、需要时恢复。按需加载解决数据加载延迟,快照恢复解决应用
就绪延迟,二者组合出三种启动模式(§3.4)。

**按需加载为什么成立**:容器启动实际访问的数据仅占镜像总量 ~6.4%,内存快照
首次运行访问率 ~20-40%——全量下载意味着大部分传输没有价值。

**存储成本**。SWE 场景 10 万+ 镜像(每 PR 一镜像)、单镜像 1-5 GiB,原始存储
100 TiB 级;层级去重在"每 PR 独立构建"模式下失效(层重复率低、层内文件重复率
高),需要内容级去重(§4.2)。

**隔离模型**。Agent 在线服务中每个用户会话独占一个 VM,会话间完全隔离——这决定
快照恢复必须支持各实例独立预热的 N:N 模式(§3.4),也是平台以 microVM 而非容器
承载负载的根本原因。

**RL 训练的极限压力**(规模锚点):单次 Rollout 60s 内滚动创建 30,000 沙箱,
峰值同时存活 8,000-10,000(生命周期 ~20s),沙箱创建慢即 GPU 空闲。

### 1.2 设计原则

1. **microVM 直载**:沙箱直接以 microVM 承载应用,VM 内无容器运行时;镜像离线
   展平为块设备镜像直接作 rootfs,定制 init 直接拉起应用进程。
2. **按需加载**:块设备与内存页都只在实际访问时加载(vhost-user-blk 拦截 I/O、
   userfaultfd 拦截缺页),配合三层缓存把绝大多数请求留在近端。
3. **内容寻址 + 收敛加密**:相同内容产生相同密文,密文层面跨租户去重;存储与
   缓存全程零明文。
4. **分代组织**:salt 源于 Generation,同代去重、跨代隔离,O(1) 整代回收,
   兼带缓存热点分散。
5. **确定性**:镜像展平逐字节可重现,Guest 配置最大化跨实例内存/磁盘一致性——
   确定性是去重率的前提。
6. **两环资源控制**:每沙箱 balloon 环回收闲置内存,节点环做准入/额度/回收,
   在固定虚拟规格下最大化物理密度。

### 1.3 关键指标

| 关键结果 | 目标 | 核心手段 |
|----------|------|----------|
| 启动加速 | 镜像冷启动端到端 P99 <500ms;快照恢复 P50 <80ms | 块设备/内存按需加载 + 分层缓存 + 内存统一持有 |
| 存储效率 | 跨镜像去重 >85%;确定性配置跨实例去重 >90% | 内容定义分块 + 收敛加密 + 分代组织 |
| 运行时访问 | L2 命中 >99.9%;L2 读取尾延迟 P99.9 <4ms | 三层缓存 + 纠删码降尾延迟 |
| 高密度隔离 | 单节点密度 >3000;单沙箱底噪 <60MiB;内存实际上浮 <15% | 轻量 VMM + 定制 Guest + 内存按需加载 + 气球柔性回收 |

定量目标与规模推算集中在 §7;实测基线(含测试环境口径)在 [`perf.md`](perf.md)。

### 1.4 边界

```
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │ Platform mgmt plane  (out of scope)                                                      │
  │   sandbox mgmt platform   /   image registry                                             │
  ├──────────────────────────────────────────────────────────────────────────────────────────┤
  │ This platform                                                                            │
  │   Sandbox & VMM       sandbox control / VMM / Guest runtime / on-demand block/snapshot   │
  │                       / density & resource control                                       │
  │   Data acceleration   ingest / chunk / encrypt / manifest / content-addressed store      │
  │                       / tiered cache                                                     │
  ├──────────────────────────────────────────────────────────────────────────────────────────┤
  │ Infrastructure                                                                           │
  │   compute / KVM / object storage / network                                               │
  └──────────────────────────────────────────────────────────────────────────────────────────┘
```

- 平台覆盖沙箱运行与数据按需加载/访问加速两层,向下消费 KVM、对象存储与网络。
- 向上对接平台管理面(沙箱管理平台、镜像仓库,平台外);二者由部署在每个计算
  节点上的 orchestrator 衔接——对外提供 e2b 兼容 API、驱动沙箱生命周期。
  与平台管理面的桥接代理(platform-agent)在平台外。
- 平台不要求管理面了解分块、加密、缓存与 VMM 实现细节。

## 2. 系统架构

### 2.1 总体拓扑

按节点角色组织:计算节点承载沙箱运行与本地缓存;cluster 控制面提供跨节点
入口、放置与 registry 状态复制;可用区内部署 L2 缓存集群;区域内是对象存储与
代管理;镜像展平在计算节点的构建沙箱内完成(§8)。

```
        ┌── Platform mgmt plane  (out of scope) ───────────────────────────────────────────────────────────┐
        │   sandbox mgmt platform      /      image registry                                              │
        └──────────────────────────────────────────────────────────────────────────────────────────────────┘
      e2b API / build API / group config         │
                                                ▼
  ┌── Cluster control plane ───────────────────────────────────────────────────────────┐
  │  cluster-ctl router ── reserve/query ──► cluster-ctl registry                     │
  │       data ingress                 shardkv state cluster                           │
  │                                    ▲                                               │
  │                                    └── place/import ── cluster-ctl placer          │
  │                                                    WATCH_LIST + provider/importer  │
  └──────────────────────────────────────────────┬────────────────────────────────────┘
      node_link / route target / key refresh      │
                                                  ▼
  ┌── Compute node  (×N per AZ) ───────────────────────────────────────────────────────┐
  │                                                                                    │
  │  node-ctl          (e2b-compatible ingress)                                        │
  │    lands per-sandbox config & keys / invokes run / drives in-sandbox build         │
  │        │ run                                  │ build  (sandbox-builder@<bid>)     │
  │        ▼                                      ▼                                    │
  │  sandbox control (one per sandbox)   ◄─ proto ─►   node resource control           │
  │    block dev / snapshot / unified memory /      admission / quota /                │
  │    balloon reclaim / handshake                  reclaim / persist  (node-ctl)      │
  │        │ drive VM                        │ fetch / store                           │
  │        ▼                                 ▼                                         │
  │  sandbox ×N: VMM + Guest runtime     tiered cache L1 (local) + CA store ──┐        │
  │  (build: 3-stage build-sandbox →     flatten / chunk / encrypt in guest)  │        │
  └──────────────────────────────────────────────────────────────────────────┼────────┘
                                        L1 miss   │                           │ chunks /
                                                  │                           │ manifest
                                                  ▼                           ▼
      ┌── AZ ──────────────────────────────────────────┐    ┌── Region ──────────────────────────────────────────────┐
      │                                                │miss│                                                        │
      │   L2 cache cluster                             │──► │  object store (L3)                                     │
      │   EC 4-of-5 / consistent hash                  │    │  encrypted chunks & manifests / generational layout    │
      └────────────────────────────────────────────────┘    │  GC & generation mgmt  (control plane):                │
                                                            │  rotation / migration / reclaim / ops                  │
                                                            └────────────────────────────────────────────────────────┘
```

cluster 控制面由 `cluster-ctl` 的 registry/router/placer 三个角色组成。
registry 是可靠状态集群,以 group/node 等逻辑键分片,分片内全复制并提供 CAS
与 WATCH;router 只处理数据面入口与活动连接缓存;placer 通过 node_list 与
sandbox group provider/importer 做放置决策。分层缓存(L1/L2)是可替换的访问
加速层:延迟与吞吐达标时可由托管 NAS 加速服务(如 SFS Turbo,对 OBS 提供近端
加速)承担,对上层提供相同访问语义。GC 与代管理作用于对象存储,处于控制平面,
不在读写热路径上。进程归属、端口与启停依赖见
[`deployment.md`](deployment.md)。

### 2.2 子系统分工

| 仓 | 角色 | 关键进程/产物 | 导出面 | 详设 |
|---|---|---|---|---|
| **orchestrator/release-builder** | 系统文档 + 发布聚合 + 跨仓 e2e/perf | `release.sh` 下载即用包 | — | 本文 + `deployment.md`/`perf.md` |
| **sandboxer** | microVM 生命周期引擎:一沙箱一进程的沙箱控制(块设备/快照代理、内存统一持有、balloon 环)+ Guest 一号进程源码 | `sandbox-ctl`、`sandbox-init` | `pkg/resource`(资源控制协议+Client) | `sandboxer/docs/sandbox.md`、`sandboxer/docs/sandbox-runtime.md` |
| **accelerator** | 存储加速 + 镜像构建:分块/收敛加密/清单库 + 内容寻址存储 + 分层缓存 + OCI → EROFS 确定性展平(远程拉取 + Referrers 幂等) | `manifest-ctl`、`store-ctl`、`cache-ctl`、`flatten-ctl` | `pkg/manifest`、`pkg/image`、`pkg/{cache,store}/client` | `accelerator/docs/{manifest,store,cache,flatten}.md` |
| **connector** | eBPF/TC 虚拟交换机:单节点 4096 端口隔离网络 + tapfd 交接 | `connector-ctl vswitch`、`connector-ctl tapfd get` | `pkg/tapfd`(fd 交接规约) | `connector/docs/{vswitch,tapfd}.md` |
| **orchestrator** | 单机沙箱编排 + e2b 兼容 ingress:控制面 REST、envd-in-guest 反代、模板构建(沙箱内三阶段)、密钥派生 + 节点级资源守护(准入/额度分配/主动回收)+ 集群控制面(registry/router/placer)与 stub e2e 节点 | `node-ctl`、`cluster-ctl`、`node-stub-ctl`、`e2b-key-ctl` | — | `orchestrator/docs/{node,node-proxy,node-resource,cluster,cluster-router,cluster-placer}.md` |
| **guest-runtime** | Guest runtime 镜像与原生依赖:打包 `sandbox-init`,构建定制 Guest 内核、VMM 补丁、erofs 工具 | `sandbox-runtime.erofs`、`vmlinux`、`cloud-hypervisor`、`mkfs.erofs` | 构建脚本 + patches + configs | `guest-runtime/docs/sandbox-runtime.md`、`guest-runtime/native-deps/docs/{cloud-hypervisor,sandbox-kernel,build}.md` |

### 2.3 依赖关系

```
 accelerator   connector   guest-runtime/native-deps        (T0: no internal Go deps)
   (storage + flatten)       ▲
        ▲                    │ pkg/tapfd
        │ pkg/manifest+image │
        └────────────────────┤
 sandboxer ─────────────────┘                              (T1: sandbox engine)
        ▲
        │ pkg/resource  (orchestrator CLI: run-sandbox→sandbox-ctl + connector-ctl vswitch;run-builder→沙箱内 flatten-ctl)
 orchestrator   (e2b ingress + node-ctl)            (T2)

 guest-runtime consumes sandboxer/bin/<arch>/sandbox-init and native-deps/mkfs.erofs
 to build sandbox-runtime.erofs; it is a release artifact dependency, not a Go import edge.
```

实线是 Go 导入边。`orchestrator` 不 import 任何兄弟仓(`CGO_ENABLED=0`
叶子),在计算节点上经 `run-sandbox`/`run-builder` 驱动 `sandbox-ctl`(拉起/快照沙箱)、
`connector-ctl vswitch`(编排网络);模板构建在构建沙箱内跑三阶段(`flatten-ctl` 经 builder runtime
flavor 投影进 guest 执行,详见 `orchestrator/docs/node.md` §12)。各 Go 导出面均为纯 Go、
无 CGO;重后端(rocksdb/对象存储 SDK/eBPF)隔离在各仓 `server`/`rocks`/
`internal` 内,不进入下游导入闭包。唯一 CGO 二进制是 accelerator 的
`cache-ctl`(静态链 librocksdb)。

### 2.4 沙箱运行形态

**microVM 直载**。沙箱直接以 microVM 承载客户应用,不使用容器运行时:容器镜像
在上传阶段展平为块设备镜像,运行时直接作为 VM rootfs 按需加载;定制 init 直接
拉起客户应用进程(容器 entrypoint)。消除容器运行时的启动开销与非确定性,冷启动
延迟截止到应用进程启动。

**VMM**。选定 Cloud Hypervisor(Rust/rust-vmm,KVM),其 vhost-user-blk 及含该
设备的快照恢复成熟,契合冷启动—预热—快照—恢复全流程。在其上定制三处(补丁化
维护,不启用定制能力时与开源版本一致):

1. **外部缺页处理器**:VMM 进程内创建缺页通道并把句柄交给宿主侧处理器,使内存
   按需加载成立(§3.3);
2. **外部传入的内存后端**:VMM 复用宿主侧创建并持有的可封印内存对象,而非自建;
3. **快照跳过外部托管内存区**:保存时不导出宿主托管内存区,恢复时也不填充,
   留给缺页机制按需触发。

**内存统一持有**。VM 内存由宿主侧沙箱控制进程创建并持有为可封印内存对象,VMM
复用同一块内存——冷启动与快照恢复走同一套内存机制;保存快照时宿主直接扫描该
内存的驻留页,无需经 VMM 中转。

**Guest 启动模型**。只读 EROFS 与可写层分别作为两个 vhost-user-blk 设备挂入,
Guest 内经 overlayfs 组装为完整 rootfs。定制 init(Guest 一号进程,单一静态
二进制)顺序执行:环境准备(挂载、根组装、网络)→ 启动握手取回启动配置 →
拉起客户应用并监督其生命周期。该进程同时承载 vsock 控制面,响应快照前清理、
内存上报等指令——无独立的 Guest Agent 进程。Guest 内核从最小配置起步,只开
沙箱必需的子系统与驱动(`guest-runtime/native-deps/docs/sandbox-kernel.md`)。

## 3. 端到端数据流

镜像与快照统一存放在远端对象存储:任何节点可恢复任何快照(无调度亲和性约束),
存储容量独立于计算节点扩缩,快照不因节点故障丢失。代价是远端延迟进入启动关键
路径——本章的按需加载与预取、§4.5 的分层缓存都围绕吸收这一延迟展开。

### 3.1 写入路径

三类输入汇入同一条分块管线:

```
Container Image      Memory Snapshot      Block Device Snapshot
       │                  │                      │
       ▼                  │                      │
Deterministic Flatten     │               Incremental Merge
(layers → EROFS image)    │               (base + CoW overlay)
       │                  ▼                      │
       └────────────► Data Stream ◄──────────────┘
                          │
                          ▼
                    Chunking (§4.2) ─► Convergent Encrypt ─► Dedup Check ─► Store.Put
                          ▲                    ▲                 │            │
                          │                    │ct               ▼            ▼
                 Pluggable strategy     key=SHA256(salt||pt)   Exists?     Atomic write
                  (FastCDC / fixed)     ct=AES-CTR(key,pt)   skip if yes  (idempotent)
                                               │                              │
                                               │key                           ▼
                                               └────────► Manifest (per chunk: offset, size, hash, key)
                                                                              │
                                                                              ▼
                                                                Seal key table with customer key
                                                                       (AES-256-GCM)
```

- **容器镜像**先确定性展平为单一 EROFS 块设备镜像(`flatten.md`):展平顺序
  固定,相同基础层总是产生相同块内容——块级去重的前提。
- **内存快照**由 VMM 暂停后导出,内存区域直接作为数据流进入管线。
- **磁盘快照**由块设备代理导出 CoW overlay,与按需加载层增量合并为完整磁盘
  状态后进入管线;全新可写层(按需加载层为空)可跳过合并。
- 写入幂等(内容寻址 PUT-if-not-exists)、流式处理(内存占用与数据大小无关)。

### 3.2 读取路径

读取层从 Manifest + Store 按需取回任意字节范围:二分定位 chunk(O(log n),
变长 chunk)→ 逐层查缓存 L1 → L2 → L3 → 以 Manifest 中的 per-chunk key 解密
(AES-256-CTR)→ 返回明文。解密只发生在最靠近 VM 的代理层(§6.1)。

冷启动的块设备路径由沙箱控制内的**块设备代理**承载:

```
         Guest VM (virtio-blk driver)
                    │
                    ▼
       ┌───────────────────────────┐
       │       vhost-user-blk      │
       │ (shared memory + eventfd) │
       └────────────┬──────────────┘
                    │
              ┌─────▼───────┐    miss    ┌───────────────┐
              │ CoW Overlay │───────────►│  Fetch Layer  │
              │ dirty page  │            │  (manifest +  │
              │ → overlay   │            │  tiered cache)│
              │ clean page  │            └───────────────┘
              │ → fetch     │
              └─────────────┘
```

- vhost-user-blk 为纯用户空间路径,避免内核穿越,对齐 I/O 端到端 P50 <500µs
  预算;代理独立于 VMM 进程,可独立部署升级。
- 写入标记脏页并写入稀疏 CoW overlay,底层存储保持不可变;overlay 可独立导出,
  支撑磁盘快照(§3.1)。
- 只读挂载(EROFS 根)下根镜像代理简化为纯读路径;可写层是独立的 ext4 块设备,
  由另一个代理实例以常规 CoW 模式服务,Guest 经 overlayfs 组装(§2.4)。
- 预取可插拔:顺序预取适合块设备流式读;chunk 对齐预取适合快照缺页。

### 3.3 快照与恢复

**捕获**:应用就绪后,Guest 一号进程执行快照前清理(清缓存、丢弃 buffer、清
/tmp、断连接)→ VMM 暂停 VM → 导出内存(宿主直扫可封印内存的驻留页)与磁盘
状态(CoW overlay 导出)→ 经写入路径(§3.1)分块入库。快照保存时 VMM 仅保存
内存与 CPU 状态,磁盘状态由块设备代理独立管理;恢复时块设备代理先就绪,VMM
恢复后重建 vhost-user-blk 连接——Guest 内 virtio-blk 驱动状态已在内存快照中,
对 Guest 透明。

**恢复**(快照代理,userfaultfd):

```
VM Resume ─► Memory Access ─► Page Fault
                                  │
                                  ▼
                           userfaultfd handler
                                  │
                        ┌─────────▼───────────┐
                        │  page in bitmap?    │
                        └──┬──────────────┬───┘
                        yes│              │no
                           ▼              ▼               ┌───────────────┐
                         skip         fetch chunk ───────►│  Fetch Layer  │
                                  (64K-1M, 4K aligned)    └───────────────┘
                                          │
                                          ▼
                              load all pages in this chunk (prefetch)
                                          │
                                          ▼
                                     UFFDIO_COPY
```

- 快照内存仅 20-40% 在首次运行期间实际访问,按需加载使物理页面仅在访问时分配。
- 缺页以 4KiB 触发,加载以 chunk 粒度(64K-1M):单次网络请求有固定开销,同
  chunk 页面空间局部性强,首次缺页后同 chunk 后续页面不再触发网络请求;bitmap
  跟踪已加载页避免重复请求。

### 3.4 三种启动模式

| 模式 | 机制 | 端到端延迟目标 | 适用 |
|------|------|-----------|------|
| 镜像冷启动 | VM 冷启动 + 镜像按需加载 | P99 <500ms(应用初始化另计) | 冷启动延迟可接受 |
| SnapStart | 快照恢复 + 快照按需加载(1:N) | 快照恢复 P50 <80ms | 同一信任域内弹性扩缩 |
| Warm Pool | 快照恢复 + 快照按需加载(N:N) | 快照恢复 P50 <80ms | 跨用户会话的隔离实例 |

镜像冷启动是基线模式,也是黄金快照创建与 Warm Pool 预热的基础步骤。两种快照
模式的区别在隔离模型:

- **SnapStart(1:N)**:一份黄金快照恢复 N 次,N 个实例共享同一初始状态——init
  阶段生成的 TLS 私钥、会话密钥等在所有克隆中相同。适用于同一应用弹性扩缩,
  实例处于同一信任域;平台提供 post-restore hook 供应用重新初始化敏感状态。
- **Warm Pool(N:N)**:每个实例独立预热并各自快照、各恢复一次,实例间无共享
  状态。Agent 在线服务中不同用户的 VM 持有相同密钥材料是不可接受的安全边界
  破坏,无法在应用层消减,必须 N:N。

快照模式把持续的计算成本转化为一次性存储成本:运行中 VM 池 = O(池大小) ×
内存+CPU;快照池 = O(池大小) × 存储,且经确定性去重后存储成本极低(§7.3)。
Warm Pool 生命周期:预热(冷启动 → 应用就绪 → 清理 → 捕获 → 入库)离线完成,
恢复(§3.3)按需服务,实例用毕销毁、Manifest 标记待回收。

## 4. 关键概念与机制

### 4.1 Manifest

二进制清单,把虚拟镜像/快照映射到加密 chunk 集合,含按需重建任意部分所需的
全部元数据:

```
┌───────────────────────────────────────┐
│           Header (64 bytes)           │
│  Magic, Version, ImageSize,           │
│  ChunkCount, ChunkMode, ...           │
├───────────────────────────────────────┤
│       Chunk Entries (56B x N)         │
│  Offset, Size, Flags, CiphertextHash  │
├───────────────────────────────────────┤
│     Encrypted Key Table               │
│  GCM Nonce + Encrypted Keys + Tag     │
│  (sealed with customer key,           │
│   Header + Entries as AAD)            │
└───────────────────────────────────────┘
```

10 GiB 镜像 ÷ 512 KiB 均值 chunk ≈ 20,000 条目 × 56 B ≈ 1.1 MiB + 密钥表
~0.7 MiB ≈ 1.8 MiB(0.018%)。变长 chunk 二分定位,20,000 chunk 约 15 次比较。
Header 的 ChunkMode 指定分块策略,读取层据此选择索引方式。格式与读写管线见
`accelerator/docs/manifest.md`。

### 4.2 内容定义分块(FastCDC)

固定大小分块在文件中间插入/删除数据时,后续所有块边界偏移、全部变为"不同"块,
去重率从 85% 跌到 ~0%。FastCDC 基于内容特征(Gear 滚动哈希)确定边界,本地修改
只影响修改处附近的 chunk:

```
Original:  [────A────][────B────][────C────][────D────]
Insert X:  [────A────][X][────B────][────C────][────D────]

Fixed-size:  boundaries shift → ALL chunks after X change → 0% dedup
FastCDC:     content-defined boundaries stable → only 1-2 chunks change → 85%+ dedup
```

参数:最小 64 KiB、平均 512 KiB、最大 1 MiB;两阶段归一化使 chunk 尺寸向均值
聚拢。策略可插拔(FastCDC / 固定大小,由 Manifest ChunkMode 指定):固定大小
适合内容不偏移的内存快照。所有策略产生的 chunk 4K 对齐,与页面粒度兼容。

块级去重率按场景量化:

| 场景 | 固定分块 | 内容定义分块 |
|------|---------|-------------|
| 相同镜像 | 100% | 100% |
| 文件修改(无偏移) | 62.5% | 85-90% |
| 新增小文件(偏移发生) | ~0% | 80-90% |
| 修改已有文件 | ~0% | 70-85% |

**块级而非文件级去重**:文件系统在 Guest 内部,Host 不暴露文件语义,块级操作
保持 Host 对 Guest 内容零感知(安全);且无需理解文件系统格式与 overlay 语义,
一条管线处理所有数据(简单)。

### 4.3 收敛加密

多租户共享存储与缓存,且要求存储/缓存被攻破时不暴露明文——但跨租户去重要求
系统能识别相同内容。每租户独立密钥会使相同内容产生不同密文,放弃去重。收敛
加密从内容本身派生密钥:

```
key        = SHA256(salt || plaintext)
ciphertext = AES-256-CTR(key, iv=0, plaintext)
chunk_name = SHA256(ciphertext)

same content → same key → same ciphertext → dedup on ciphertext
```

- 存储/缓存只见密文,解密仅在靠近 VM 的代理层发生;
- 密钥表存入 Manifest,用客户管理的密钥(AES-256-GCM)加密,Header + Entries
  作为 AAD 防篡改;
- salt 控制去重域:同 salt 内去重,不同 salt 间隔离(§4.4)。

代价:对已知明文攻击有理论弱点(可确认某内容是否存在)。对本场景可接受——
存储的是系统级数据(OS、运行时),非用户敏感数据。

### 4.4 Salt、Generation 与分代 GC

salt 源于 Generation ID,一个机制同时回答三个问题:

**去重域**。同代内相同内容 → 相同 chunk_name → 去重;跨代无共享。

**热点分散**。基础层(libc、Python 运行时)被大量镜像共享,其哈希固定 → 一致性
哈希永远映射到相同缓存节点,单节点故障影响面大。不同代不同 salt → 相同内容产生
不同密文/哈希 → 映射到不同节点;3 个活跃代把热点流量分散到 3 倍节点。

**空间回收**。百万级 Manifest 共享十亿级 chunk,引用计数(逐 chunk 原子操作)
与标记-清除(全量遍历 + 一致性窗口)都不可行。代是自包含的回收单元:

```
┌───────────┐     ┌───────────┐     ┌───────────┐     ┌───────────┐
│ Active    │────►│ Retired   │────►│ Expired   │────►│ Deleted   │
│ (R/W,     │     │ (R only,  │     │ (alarm on │     │ (gone)    │
│  new      │     │  migrate  │     │  read,    │     │           │
│  writes)  │     │  active   │     │  auto-    │     │           │
│           │     │  manifests│     │  pause    │     │           │
│           │     │  to new   │     │  delete)  │     │           │
│           │     │  gen)     │     │           │     │           │
└───────────┘     └───────────┘     └───────────┘     └───────────┘
```

整代删除 = 删一个目录,O(1);Expired 态被读触发告警并自动暂停删除(安全网)。
退役代中仍被引用的 Manifest 解密后用新代 salt 重加密迁移,走后台路径;高去重率
使每代唯一 chunk 占比小,且应用频繁重部署使旧 Manifest 多在退役前已被替代。

内容寻址存储按代组织目录(两级哈希前缀防单目录过大),PUT 幂等:

```
{store_root}/
├── G1/  a1/b2/a1b2c3...        ← retired (salt_1)
├── G2/  a1/b2/a1b2...          ← active  (salt_2)
└── G3/  ...                    ← active  (salt_3)
```

代价:同一内容在活跃代间各存一份(3 代 ≈ 3× 单代去重后大小)——相比无去重的
原始数据仍节省数量级。后端与代轮转见 `store.md`。

### 4.5 分层缓存

远端对象存储延迟(P50 36ms, P99.9 175ms)是基础设施固有特性,必须在近端吸收
绝大多数请求;单节点 SSD 容量有限(工作集超出单节点),引入 AZ 级共享 L2:

| 层 | 位置 | 延迟 | 命中率 |
|----|------|------|--------|
| L1 | 节点本地 SSD(1-2 TiB) | <500µs(SSD),热数据 <100µs(page cache) | >65% |
| L2 | AZ 级分布式集群 | P50 550µs, P99.9 3.7ms | >99.9% |
| L3 | 远端对象存储 | P50 36ms, P99.9 175ms | 兜底 |

到达远端的请求 ≈ L1 miss 35% × L2 miss 0.1% ≈ 0.035%。

- **L1 抗扫描**:强制准入(Put 不做 admission 拒绝)+ Count-Min Sketch 频率统计,
  冷数据经存储引擎 CompactionFilter 后台物理删;无应用层 LRU/SLRU 队列,RocksDB
  BlockCache 护住热数据,避免大批量启动冲刷常驻热数据。
- **L2 纠删码降尾延迟**:数百 chunk 的启动场景下至少一次命中 L2 尾延迟的概率
  显著(§7.1)。每 chunk 编码为 5 分片(RS 4-of-5)分布到 5 节点,读取并行请求
  5 取最快 4 重建——最慢节点被自然丢弃,全延迟分布改善 ~20%,存储开销 25%
  (对比三副本 200%)。
- **L2 冷热分层**:内存层(~10% 容量,<1ms)+ SSD 层(~90%,低毫秒),容量
  10×、成本 ~1/5 于纯内存。
- **客户端旁路填充(fill-aside)**:L2 miss 时客户端从 L3 取完整 chunk、本地
  RS 编码、并行 PUT 5 分片(fire-and-forget)并写 L1。L2 节点是纯 KV——不访问
  L3、不持凭证、不编解码,EC 的 CPU 分散到数千客户端;写入幂等,淘汰各节点独立
  LRU,无需协调。分片缺失读时回填:缺 1 从余 4 重建回填,缺 ≥2 回退 L3 重新
  编码回填。
- **稳态保护**:99.9% 命中率意味着缓存故障时流量放大 1000×。三重防护:L3 并发
  信号量、断路器(错误率 >50% 快速失败)、对等预热(新节点从健康节点而非 L3
  加载)。

缓存仅存密文——无密钥管理,泄露仅暴露密文,可跨租户共享。L2 部署限定 AZ 内
(跨 AZ RTT 1-5ms 会突破 P99.9 <4ms 预算);镜像与快照业务特征不同,可分独立
集群部署(规模推算见 §7.3,集群规格见 `deployment.md` §3)。

### 4.6 确定性

去重率的前提是"相同的东西真的产生相同的字节",两处保障:

**展平确定性**(镜像)。多层 OCI 镜像按固定顺序合并、归零时间戳、确定性遍历,
相同输入逐字节相同输出(`flatten.md` §3-4)。

**Guest 确定性配置**(快照)。KASLR/ASLR、并行初始化、文件系统缓存使相同配置的
VM 内存布局不同,跨实例去重率从 >90% 跌至 50-70%;磁盘可写层同理分化:

```
┌────────────────────────────────────────────────────────┐
│              Deterministic Guest Config                │
│                                                        │
│  Kernel:   nokaslr norandmaps (fixed address base)     │
│  Root FS:  read-only (no state accumulation)           │
│  Init:     sequential (deterministic process order)    │
│  Pre-snap: guest PID1 clears caches, drops buffers,    │
│            closes connections, flushes /tmp            │
│  Post-restore: re-seed entropy pool                    │
└────────────────────────────────────────────────────────┘
```

| 内存一致性 | 去重率 | 场景 |
|-----------|--------|------|
| 确定性初始化 | >90% | SnapStart / Warm Pool |
| 非确定性 | 50-70% | 标准部署 |
| 不同应用 | 30-50% | 共享基础设施 |

商业影响:单应用 200 实例 × 512 MiB,确定性下实际存储 ~1.5 GiB(含 3 代),
非确定性 ~50 GiB——约 33× 差异,直接决定 Warm Pool 是否经济可行。代价:禁用
KASLR/ASLR 降低安全性,对短生命周期、网络隔离的 VM 可接受;SnapStart 恢复后
重新播种熵池。

### 4.7 密度与资源控制

单节点(128 Core / 512G)承载数千并发沙箱。沙箱按客户最大消耗提供固定虚拟规格
(如 2C8G),平台在不影响 Guest 感知的前提下动态管理实际物理资源:

```
┌───────────────────────────────────────────────────────────────────────┐
│                     Density & resource control                        │
│                                                                       │
│  Per-sandbox loop (balloon, host-driven)                              │
│    boot: balloon=0 → guest sees full capacity                         │
│    guest reports MemAvailable periodically (vsock)                    │
│    host drives inflate target → reclaim idle pages                    │
│                                                                       │
│  Node loop (resource arbitration, node-ctl)                           │
│    admission & concurrency control (rate + in-flight cap)             │
│    quota by watermark + rate limit; emergency pool                    │
│    active reclaim: converge quota toward actual usage                 │
│    state persistence & scan recovery                                  │
│                                                                       │
│  Cross-cutting                                                        │
│    KSM cross-VM identical-page merge / cgroup watermark backpressure  │
│    / uffd page-load backpressure                                      │
│                                                                       │
│  Modes: no-cgroup / static / dynamic;  CPU: cgroup weight + quota     │
└───────────────────────────────────────────────────────────────────────┘
```

- **每沙箱环**:启动时 balloon=0(Guest 看到完整声明容量),settled 后宿主按
  Guest 周期上报的 MemAvailable 推 inflate target,按实际需求保留物理内存。
- **节点环**:令牌桶限制创建速率与并发、按高/低/应急三档水位发放内存额度、把
  已稳定沙箱的额度向实际用量收敛(留冷却窗口)、状态持久化于内存盘 + 扫描恢复。
  两环经资源协议对话(`sandboxer/pkg/resource`),断连降级为按当前额度
  继续运行。
- KSM 对确定性配置下 >90% 内存一致的实例尤为有效;CPU 经 cgroup 权重与配额表达,
  无需热插拔。
- 快照恢复路径下内存按需加载,初始工作集约 25%,天然减少物理占用;单沙箱底噪
  (VMM 进程 + 已驻留 Guest 页)<60 MiB,其中 VMM 进程开销 ~10 MiB 量级。

密度实测与调优杠杆见 [`perf.md`](perf.md) §3,协议与算法见 `orchestrator/docs/node-resource.md`。

## 5. 设计取舍

| 决策 | 选择 | 备选 | 为什么 | 代价 |
|------|------|------|--------|------|
| 分块策略 | FastCDC 内容定义 | 固定大小 | 偏移场景去重 85% vs 0% | 写入 CPU、变长索引 |
| 去重粒度 | 块级 | 文件级 | 安全(Host 零文件感知)+ 简单(无需理解 FS) | 元数据稍大 |
| 加密方式 | 收敛加密 | 每租户独立密钥 | 密文层去重;已知明文仅暴露系统级公开内容 | 已知明文理论弱点 |
| L2 缓存冗余 | 4-of-5 纠删码 | 三副本 | 25% vs 200% 开销,且降尾延迟 | EC 编解码 CPU |
| L2 缓存填充 | 客户端旁路填充 | 缓存穿透填充 | L2 无需 L3 凭证,零明文暴露面;EC CPU 分散 | 客户端逻辑略复杂 |
| L2 存储 | 内存+SSD 冷热两层 | 纯内存 | 10x 容量、成本 ~1/5 | 冷数据延迟略高 |
| 块设备协议 | vhost-user-blk | FUSE | 零内核穿越,对齐 <500µs 目标 | 实现复杂度 |
| 页面加载 | userfaultfd 按需 | 全量预加载 | 仅 6.4%-40% 数据被实际使用 | 运行时缺页延迟 |
| GC 策略 | 分代 GC + salt | 引用计数 | O(1) 整代删除 vs 十亿级原子操作 | 迁移成本、多代存储 |
| 展平格式 | EROFS | ext4 | 只读布局确定性强,利于去重;匹配不可变存储 | Guest 需额外可写层 |
| 写入覆盖 | CoW overlay | 直接修改 | 底层不可变,overlay 可独立捕获/恢复 | 稀疏文件管理 |
| Manifest 格式 | 二进制定长条目 | JSON/Protobuf | 0.018% 开销 vs ~1% | 调试可读性 |
| 压缩 | 不压缩 | zstd | 确定性密文 + 随机访问 vs 压缩侧信道 | — |
| 沙箱运行 | microVM 直载 | containerd + microVM | 消除容器运行时开销与非确定性 | 定制 init 维护 |
| VMM 选型 | Cloud Hypervisor | Firecracker | vhost-user-blk 与含该设备的快照恢复成熟;外部缺页经定制补齐 | 跟踪上游、补丁维护 |
| 缓存层 | 自建分层缓存 | 托管 NAS 加速(SFS Turbo) | 自建在密文 KV 角色上实现 EC 与抗扫描淘汰 | 运维复杂度(可切换托管) |
| VM 内存 | 内存统一持有 | VMM 自建内存 | 冷启动与快照恢复同一套内存机制,快照直扫驻留页 | VMM 定制注入内存后端 |

三个核心平衡:

- **安全 vs 去重** → 收敛加密:相同内容相同密文,在密文层去重,存储层零明文。
- **热点分散 vs 去重** → Salt 分代:代内去重完整保留,热点在代间自然分散。
- **按需加载 vs 低延迟** → 三层缓存:99.9% 请求在 L1/L2 完成,按需 ≠ 每次远端。

## 6. 安全模型

### 6.1 信任边界

```
┌──────────────────────────────────────────────────────────────┐
│  ┌─────────────────┐  virtio  ┌─────────────────────────┐    │
│  │ Guest VM        │◄────────►│ Agent (sandbox control) │    │
│  │ (untrusted)     │  blk/    │ plaintext boundary —    │    │
│  │                 │  uffd    │ decrypt here only       │    │
│  └─────────────────┘          └───────────┬─────────────┘    │
│                                           │ ciphertext       │
│                               ┌───────────▼─────────────┐    │
│                               │ Cache + Store           │    │
│                               │ ciphertext only,        │    │
│                               │ zero plaintext exposure │    │
│                               └─────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

解密仅发生在沙箱控制的代理层——最靠近 VM 的位置。存储与缓存全程只处理密文,
被攻破也不暴露明文;密文相同 = 内容相同,缓存可跨租户共享,无需每租户隔离。
沙箱间的网络隔离由 connector 提供(无 port→port 转发路径的结构性隔离,
`vswitch.md` §7)。

### 6.2 威胁缓解

| 威胁 | 缓解手段 |
|------|----------|
| 存储泄露 | 仅存密文,密钥不在存储中 |
| 缓存泄露 | 仅缓存密文,无密钥管理 |
| chunk 篡改 | 密文哈希校验(SHA256),地址即校验 |
| Manifest 篡改 | AES-GCM 认证标签,Header + Entries 作为 AAD |
| 已知明文攻击 | 可确认内容存在性——对系统级数据(OS、运行时)可接受 |
| KASLR/ASLR 禁用 | 短生命周期 + 网络隔离 + 恢复后重新播种熵池 |
| 跨代数据残留 | Expired 态告警 + 自动暂停删除的安全网 |

## 7. 性能目标与规模

本节是设计目标与规模推算;实测基线(嵌套 KVM 开发环境口径)在
[`perf.md`](perf.md)。

### 7.1 规模基线

| 维度 | 基准值 | 增长目标 |
|------|--------|----------|
| 容器镜像总量 | 10 万 | 100 万 |
| 镜像均值大小 | 1 GiB(250 MiB - 10 GiB) | — |
| 快照均值大小 | 512 MiB(= Guest 内存分配) | — |
| 物理节点 | 5,000 / AZ | — |
| 每节点并发实例 | 3,000 | — |
| 每节点并发创建 | ~150 VMs/sec(VMM 能力;实际突发基线按 ~100) | — |
| 实例创建速率 | 10 万/分钟持续,50 万/分钟峰值 | — |
| 节点规格 | 128+ Core, 512G+ Mem, 5T+ SSD, 25/40G NIC | — |

每次启动需取回的 chunk 数与尾延迟暴露概率(遭遇 ≥1 次 P99.9):

| 场景 | 计算 | chunk 数 | 暴露概率 |
|------|------|----------|----------|
| 镜像冷启动(1 GiB) | 1 GiB × 6.4% ÷ 512 KiB | ~128 | 12% |
| 镜像冷启动(5 GiB) | 5 GiB × 6.4% ÷ 512 KiB | ~640 | 47% |
| 快照恢复(512 MiB, 25%) | 512 MiB × 25% ÷ 512 KiB | ~256 | 23% |

这是 L2 必须用纠删码压尾延迟(§4.5)的量化依据。

### 7.2 延迟目标

VMM + 内核层基线(本地镜像/快照,不含按需加载):VM 启动 ~125ms,VMM 快照恢复
~5ms。端到端目标(不含调度时延):

| 指标 | P50 | P99 |
|------|-----|-----|
| 冷启动(VMM 启动 + 镜像按需加载,不含应用就绪) | <250ms | <500ms |
| 快照拉起(VMM 恢复 + 快照按需页面加载) | <80ms | <200ms |

延迟预算分解(P99,均预留大镜像/冷 L1/偶发 L3 回退的余量):

```
Cold boot (1 GiB image):                 Snapshot restore (512 MiB):
  VMM + kernel boot          125 ms        VMM snapshot restore          5 ms
  Manifest fetch (L2 P99)      3 ms        Manifest fetch (L2 P99)       3 ms
  Chunk serial chain         ~25 ms        Page-fault serial chain     ~34 ms
  (~15 x L2 P99 ~1.7ms)                    (~20 x L2 P99 ~1.7ms)
  ──────────────────────────────────       ─────────────────────────────────
  Total                     ~153 ms        Total                       ~42 ms
```

串行链长度取决于启动/恢复的 I/O 关键路径——顺序预取(块设备)与 chunk 级预取
(快照)把大部分获取隐藏在执行过程中,串行链仅为无法被预取覆盖的依赖性访问。

分层指标:

| 指标 | P50 目标 | P99/P99.9 目标 |
|------|----------|---------------|
| 页面错误延迟 | <500µs | <5ms |
| L1 命中延迟 | <500µs(SSD),热数据 <100µs(page cache) | — |
| L2 命中延迟 | <1ms | P99.9 <4ms |
| L3 远端延迟 | <50ms | <200ms |
| 快照创建吞吐 | >500 MB/s | — |
| 跨镜像去重 | >85% | — |
| L1 / L2 命中率 | >65% / >99.9% | — |

### 7.3 存储与带宽推算

**存储**(行业数据:80% 上传零唯一 chunk,其余均值 4.3% 唯一,等效节约 ~23×):

| 数据 | 原始 | 去重后 |
|------|------|--------|
| 镜像 10 万 × 1 GiB | 100 TiB | ~4 TiB |
| 镜像 100 万 × 1 GiB | 1 PiB | ~43 TiB |
| Warm Pool 单应用(200 实例 × 512 MiB) | 100 GiB | <1.5 GiB(>90% 去重 × 3 代) |
| Warm Pool 平台(10% 应用启用,1 万应用 × 均值 50 实例) | ~250 TiB | ~7.5 TiB(应用内 >90% + 跨应用 ~50% 重叠 × 3 代) |

**带宽**(镜像路径,单节点 100 VMs/sec 突发):总需求 ~6.4 GiB/s 由本地 SSD
承担;L1→L2 按 35% miss ~2.24 GiB/s(40G NIC 利用率 ~45%);L2→L3 按 0.1% miss
~2.24 MiB/s。集群口径:200 节点同时突发 → L1→L2 聚合 ~448 GiB/s → L2-镜像集群
~90 节点(40G,吞吐驱动);L2-快照集群服务 RL Rollout 峰值 ~22 GiB/s → 20-30
节点。两集群合计 ~110-130 节点/AZ(部署规格见 `deployment.md` §3)。

**RL Rollout**(30K 沙箱快照恢复 / 60s):读需求 ~3.75 TiB,经 L1 >65% 命中后
到 L2 ~22 GiB/s,经 L2 >99.9% 命中后到 L3 仅 ~22 MiB/s——按需加载 + 分层缓存
把"对象存储不可承受的并发"压缩为常规流量。

## 8. 部署形态

按节点角色部署(进程清单、端口、配置入口、启停依赖与故障域见
[`deployment.md`](deployment.md)):

- **计算节点**(~5,000/AZ):`node-ctl` + `cache-ctl tiered`
  + `store-ctl`(sidecar)+ `sandbox-ctl × ~3K`(每沙箱一进程,派生
  `cloud-hypervisor`);e2b 模板构建在本节点的构建沙箱内进行(`sandbox-builder@<bid>`
  → 三阶段,见 `deployment.md` §5),无独立展平池。
- **Cluster 控制面**(AZ 级或 Region 级):`cluster-ctl registry` 按
  membership 配置形成可靠状态集群;`cluster-ctl router` 提供 group-scoped
  数据入口与活动连接缓存;`cluster-ctl placer` 订阅 node_list、导入 group
  配置并执行放置。registry 与 placer 各自通过 memberlist 健康检测隔离成员
  label,成员清单由配置和注册路径提供。
- **L2 缓存集群**(AZ 级,100-200 节点):`cache-ctl shard`,RS 4+1 + Maglev
  一致性哈希,纯密文 KV。
- **Region 级**:对象存储桶 + GC 与代管理(控制平面)+ 平台管理面(平台外)。

开发/PoC 可单机运行:`store-ctl`(fs 后端)+ `cache-ctl local` + 手工
`sandbox-ctl run`,无 L2/OBS/node-ctl(`deployment.md` §9.1)。

## 9. See Also

- [`deployment.md`](deployment.md) — 部署拓扑与组件清单:进程归属、端口、启停
  依赖、故障域。
- [`perf.md`](perf.md) — 实测性能基线、回归 checklist 与调优杠杆。
- `sandboxer/docs/sandbox.md` — 沙箱控制(`sandbox-ctl`)完整生命周期与
  配置 schema;`sandboxer/docs/sandbox-runtime.md` — Guest 一号进程协议与实现。
- `guest-runtime/docs/sandbox-runtime.md` — Guest runtime 镜像打包与发布边界。
- `accelerator/docs/{manifest,store,cache}.md` — 清单格式与读写管线、
  内容寻址存储与代轮转、分层缓存三形态。
- `accelerator/docs/flatten.md` — 确定性展平、远程拉取与 Referrers 幂等。
- `connector/docs/vswitch.md` — eBPF 虚拟交换机;`tapfd.md` — tap fd 交接
  协议。
- `orchestrator/docs/node-resource.md` — 节点资源仲裁协议与算法。
- `orchestrator/docs/node.md` — e2b 兼容控制面、模板构建、密钥与归属模型、
  集群接入(node-link)。
- `orchestrator/docs/cluster.md` — 集群级 registry 自聚簇 / Reserve 状态机 /
  placer 放置与密钥分发。
- `guest-runtime/native-deps/docs/{cloud-hypervisor,sandbox-kernel,build}.md` — VMM 补丁集、
  Guest 内核契约、原生依赖构建。
