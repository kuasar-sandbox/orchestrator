# Container Accelerator 方案建议书 v1.4

## §1 概述

Container Accelerator 是面向大规模 Agent 沙箱平台的存储与缓存加速引擎，同时服务传统 Serverless 与 RL 训练等同栈场景。它解决三类核心问题：

1. **启动加速**：容器镜像和内存快照的按需加载，配合 VMM 与内核优化，实现端到端亚秒级启动——镜像冷启动 P99 <500ms，快照恢复 P50 <80ms
2. **存储效率**：跨镜像/快照内容级去重，将 PB 级原始数据压缩到 TB 级实际存储
3. **高密度隔离**：在单节点数千并发沙箱规模下，通过轻量 VMM、定制 Guest 环境和动态资源控制，实现 VM 级安全隔离与高密度的兼得

加速引擎以 Agent 沙箱平台为主线提供统一基础设施，同时覆盖传统 Serverless 与 RL 训练，覆盖从 VMM/Guest 环境到数据按需加载的完整路径，支持两条运行时路径（块设备、内存快照）与三种启动模式（镜像冷启动、SnapStart、Warm Pool）。

四个关键结果：

| 关键结果 | 目标 | 核心手段 |
|----------|------|----------|
| 启动加速 | 镜像冷启动端到端 P99 <500ms；快照恢复 P50 <80ms | 块设备/内存按需加载 + 分层缓存 + 内存统一持有 |
| 存储效率 | 跨镜像去重 >85%；确定性配置跨实例去重 >90% | 内容定义分块 + 收敛加密 + 分代组织 |
| 运行时访问 | L2 命中 >99.9%；L2 读取尾延迟 P99.9 <4ms | 三层缓存 + 纠删码降尾延迟 |
| 高密度隔离 | 单节点密度 >3000；单沙箱底噪 <60MiB；内存实际上浮 <15% | 轻量 VMM + 定制 Guest + 内存按需加载 + 气球柔性回收 |

---

## §2 业务场景与目标用户

### §2.1 Agent 应用与沙箱

**Agent 演进**：

```
┌──────────────────────┐         ┌──────────────────────┐         ┌──────────────────────┐
│       ChatBot        │         │    Workflow Agent    │         │    Agentic Agent     │
│                      │         │                      │         │                      │
│  LLM + RAG           │────────►│  Multi-step          │────────►│  Autonomous plan     │
│  Stateless           │         │  Partial isolation   │         │  Full sandbox        │
│  No env deps         │         │  Limited tools       │         │  Code, browser, FS   │
└──────────────────────┘         └──────────────────────┘         └──────────────────────┘
```

**环境复杂度**：Agent 应用的运行环境比传统 Serverless 更复杂——依赖更多、镜像更大（1-5 GiB）。依赖安装和数据加载在镜像构建阶段完成，运行时的启动过程本身不一定慢，但从启动到应用就绪可能需要数秒或更长（服务预热、连接建立、模型加载到内存等）。

**两个层次的冷启动问题**：

1. **镜像加载延迟**：大镜像（1-5 GiB）的拉取/加载耗时——存储加速解决
2. **应用就绪延迟**：启动到就绪的时间——Warm Pool 通过提前预热解决

**会话隔离**：Agent 在线服务中每个用户会话独占一个 VM，会话之间要求完全隔离——这是沙箱平台的核心约束，也决定了快照恢复采用各实例独立预热的隔离模型。

### §2.2 传统 Serverless 冷启动

**现状**：Serverless 应用的冷启动延迟范围从 100ms 到 10s+，取决于运行时和包大小。虽然冷启动仅占总调用的不到 1%，但对用户体验影响显著——触发冷启动时延迟可增加 100 倍。

**趋势**：容器镜像支持使部署包从 250MB 增长到 10GiB+。传统"全量下载后启动"模式需要的带宽随规模线性增长——数千台节点每秒可创建数百容器（微 VM 级约 150 VMs/sec/host），若每个全量下载数 GiB 镜像，聚合带宽需求达到 PB/s 级别，远超任何网络基础设施的承载能力。

**SnapStart**：对 Java 等重运行时应用，平台提供"黄金快照 → 克隆恢复"能力（SnapStart），将冷启动从数秒降至亚秒。快照的存储和按需加载是这一能力的核心基础设施。

### §2.3 RL 训练的极限压力

Agent RL 训练需要与真实环境交互——环境即沙箱。行业典型规模：

| 维度 | 规模 |
|------|------|
| 单次 Rollout 创建总量 | 30,000（60s 内滚动创建） |
| 峰值同时存活 | 8,000-10,000（沙箱生命周期 ~20s） |
| SWE 场景镜像数 | 100,000+（每个 PR 对应一个镜像） |
| 单镜像大小 | 1-5 GiB |
| 每分钟启动量 | 数万~数十万 |
| 训练对沙箱延迟的容忍 | 沙箱创建慢 = GPU 空闲 = 资源浪费 |

**带宽瓶颈**：每轮 Rollout 创建 30,000 沙箱，若每个需要全量拉取数 GiB 镜像，单轮需要数十 TiB 级带宽。传统全量下载模式在此规模下完全不可行。

### §2.4 共同诉求

| 诉求 | 传统 Serverless | Agent 在线服务 | RL 训练 |
|------|----------------|---------------|---------|
| 安全隔离 | 多租户隔离 | Agent 行为不可预测 | 沙箱间完全隔离 |
| 快速启动 | 亚秒冷启动 | 亚秒会话创建 | 毫秒级批量创建 |
| 大规模并发 | 万级/秒 | 千级同时在线 | 万级并发沙箱 |
| 成本可控 | 镜像存储 | 快照存储 | 海量镜像+快照 |

三个场景的需求在此收敛：**按需加载 + 内容去重 + 安全加密 + 分层缓存**。

---

## §3 需求分析

### §3.1 启动延迟分解

端到端启动延迟可分解为三个层次：

```
端到端启动 = VM 启动 + 数据加载 + 应用就绪
```

**VM 启动**。沙箱直接以 microVM 承载客户应用，不使用容器运行时。容器镜像在上传阶段展平为块设备镜像，运行时直接作为 VM rootfs 按需加载，消除容器运行时引入的启动开销和非确定性。定制 init 直接拉起客户应用进程（容器 entrypoint），冷启动延迟截止到应用进程启动。VM 启动（VMM + 内核 + init）的本地基线约 125ms，叠加数据按需加载后端到端冷启动 P99 目标 <500ms。

**数据加载**。真实场景中镜像和快照存储在远端（参见 §3.3），远端数据加载延迟成为 VM 启动优化之后的首要瓶颈——传统 Serverless 大镜像（最大 10 GiB）全量拉取需秒级，Agent / RL 训练场景镜像 1-5 GiB 且万级并发同时拉取，聚合带宽远超网络承载。平台通过按需加载与分层缓存，最小化数据加载延迟。

**应用就绪**。部分应用从进程启动到服务就绪需数秒（服务预热、连接建立、模型加载等），存储加速无法消除这一延迟。平台的应对方式是提前完成就绪过程，将就绪状态冻结为内存快照，需要时直接恢复。

按需加载解决数据加载延迟，快照恢复解决应用就绪延迟，平台据此提供三种启动模式（§3.2）。

### §3.2 三种启动模式

| 模式 | 机制 | 端到端延迟 | 适用 |
|------|------|-----------|------|
| 镜像冷启动 | VM 冷启动 + 镜像按需加载 | 端到端 P99 <500ms（含应用初始化另计） | 冷启动延迟可接受 |
| SnapStart | 快照恢复 + 快照按需加载 (1:N) | 快照恢复 P50 <80ms | 同一信任域内弹性扩缩 |
| Warm Pool | 快照恢复 + 快照按需加载 (N:N) | 快照恢复 P50 <80ms | 跨用户会话的隔离实例 |

镜像冷启动是基线模式。当应用就绪延迟不可接受时引入快照恢复——两种快照模式的区别在于隔离模型：

- **SnapStart (1:N)**：一份黄金快照恢复 N 次。N 个实例共享同一初始状态——init 阶段生成的 TLS 私钥、会话密钥等在所有克隆中相同。适用于同一应用弹性扩缩（传统 Serverless），单个应用服务多用户请求，实例处于同一信任域，平台提供 post-restore hook 供应用重新初始化敏感状态。
- **Warm Pool (N:N)**：每个实例独立预热并各自快照，各恢复一次。实例间无共享状态，天然满足隔离要求。Agent 在线服务中每个用户会话独占一个 VM——不同用户的 VM 持有相同密钥材料是不可接受的安全边界破坏，无法通过应用层消减。

快照模式将持续的计算成本转化为一次性存储成本——运行中 VM 池 = O(池大小) × 内存+CPU，快照池 = O(池大小) × 存储。单应用 200 实例 × 512 MiB：运行中 = 100 GiB 内存常驻；快照 = 存储（经去重后 <1 GiB）。恢复延迟 <100ms 对脉冲式访问模式可接受。

三种模式共享同一基础设施（按需加载、分块、加密、缓存、GC）。镜像冷启动也是 SnapStart 黄金快照创建和 Warm Pool 预热的基础步骤。

以 Warm Pool 为例，展示从预热到恢复的端到端生命周期：

```{.small}
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                            WARM POOL END-TO-END LIFECYCLE                                          │
│                                                                                                                    │
│  ── Pre-warm Phase (offline, ahead of demand) ───────────────────────────────────────────────────────────────────  │
│                                                                                                                    │
│  ┌───────────────────┐      ┌──────────────────────┐      ┌──────────────────┐      ┌──────────────────────┐       │
│  │ 1. Boot VM        │      │ 2. App Ready         │      │ 3. Prepare       │      │ 4. Capture           │       │
│  │                   │      │                      │      │                  │      │                      │       │
│  │ vhost-user-blk    │─────►│ Dependencies loaded  │─────►│ Guest Agent:     │─────►│ Pause VM             │       │
│  │ on-demand block   │      │ Application warmed   │      │ clear caches,    │      │ Dump memory +        │       │
│  │ device loading    │      │ Health check passed  │      │ drop buffers,    │      │  capture disk state  │       │
│  │                   │      │                      │      │ flush /tmp       │      │ Destroy pre-warm VM  │       │
│  └───────────────────┘      └──────────────────────┘      └──────────────────┘      └───────────┬──────────┘       │
│                                                                                                 ▼                  │
│           ┌─────────────────────────────┐      ┌──────────────────────────────────────────────────────────────┐    │
│           │ 6. Pool                     │      │ 5. Ingest (write path, §6.1)                                 │    │
│           │                             │◄─────│                                                              │    │
│           │ Manifest + encrypted chunks │      │ Chunk → Encrypt → Dedup → Store                              │    │
│           │ persisted in remote store   │      │ Seal Manifest with customer key (AES-256-GCM)                │    │
│           │ Available for restore       │      │ Deterministic init ensures >90% cross-instance dedup         │    │
│           └─────────────────────────┬───┘      └──────────────────────────────────────────────────────────────┘    │
│                                     │                                                                              │
│  ── Serve Phase (on demand, ~80ms) ─┼────────────────────────────────────────────────────────────────────────────  │
│                                     ▼                                                                              │
│  ┌──────────────────────────────────────────────────┐      ┌──────────────┐      ┌───────────────────────┐         │
│  │ 7. Restore (read path, §6.2)                     │      │ 8. Running   │      │ 9. Reclaim            │         │
│  │                                                  │      │              │      │                       │         │
│  │ New VM + userfaultfd memory region               │─────►│ User session │─────►│ Destroy VM            │         │
│  │ Manifest lookup → tiered cache (L1 → L2 → L3)    │      │ active       │      │ Snapshot consumed     │         │
│  │ On-demand page loading with chunk-level prefetch │      │              │      │ Mark Manifest for GC  │         │
│  └──────────────────────────────────────────────────┘      └──────────────┘      └───────────────────────┘         │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### §3.3 远端存储

快照和镜像统一存储在远端，核心收益：

- **调度自由度**：任何主机恢复任何快照，无亲和性约束
- **弹性**：存储容量独立于计算节点扩缩
- **持久性**：快照不因节点故障丢失

### §3.4 关键问题

1. **恢复延迟**：远端存储延迟远大于本地——每次页面错误/块读取都需网络往返
2. **存储成本**：N 镜像 × M GiB 线性增长——无去重则 PB 级存储
3. **并发带宽**：万级沙箱同时恢复——TB 级带宽需求超过网络容量

---

## §4 核心挑战

### §4.1 单节点高密度沙箱

**业务问题**：单节点需支持数千并发沙箱，每个沙箱提供完整 VM 级隔离。节点资源有限（128 Core, 512G Mem），每 VM 的固有开销直接限制并发上限。为简化客户应用适配，沙箱按客户最大消耗提供固定规格（如 2C8G），客户应用面对确定性的资源环境。平台侧需在不影响 Guest 感知的前提下动态管理实际物理资源，最大化密度。

**方案**：从 VMM 精简、内存机制、动态资源控制、按需加载多个层面压缩开销。

使用 microVM 级 VMM，最小化设备集和每 VM 进程开销（约 10 MiB 量级）；VM 内不使用容器运行时，展平镜像直接作为 VM rootfs，定制 init 直接拉起客户应用进程。单沙箱底噪（VMM 进程 + 已驻留 Guest 页）控制在 <60 MiB。

**内存统一持有**：VM 内存由宿主侧控制进程创建并持有为可封印内存对象，VMM 复用同一块内存——冷启动与快照恢复走同一套内存机制，保存快照时宿主直接扫描该内存的驻留页，无需经 VMM 中转。

**两环动态资源控制**：每沙箱一环——VM 启动时 balloon=0（Guest 看到声明规格的完整容量），settled 后宿主侧控制器按 Guest 周期上报的 MemAvailable 推 balloon 的 inflate target，把闲置物理内存回收回宿主；节点一环——节点资源仲裁负责准入与并发管控、按水位与限速分配内存额度、把已稳定沙箱的额度向实际用量收敛、状态持久化与扫描恢复。资源约束支持三态模式：无限制、静态限制、动态调控。叠加 KSM 跨 VM 相同页合并、cgroup 水位反压、userfaultfd 页加载背压综合控制节点级内存密度。CPU 通过 cgroup 权重与配额表达，竞争时按权重分配，无需热插拔。

快照恢复路径下 VM 内存按需加载（userfaultfd），物理页面仅在访问时分配，初始工作集约 25%——天然减少物理页面占用。

**代价**：VMM 定制开发与持续跟踪上游；定制 Guest 内核与 init 维护；宿主侧控制器复杂度。

### §4.2 沙箱启动延迟

**业务问题**：沙箱启动需要镜像或快照数据。传统方式全量下载后启动——单个容器需秒级，平台规模下聚合带宽不可行（见 §8.7）。

**为什么可以不全量下载？** 容器启动时实际访问的数据仅占镜像总量的 ~6.4%——大量文件在启动路径中从未被打开。内存快照的首次运行访问率更高（~20-40%），但仍显著低于 100%。全量下载意味着 >60% 的数据传输没有价值。

**方案：按需加载**。只在实际访问时才加载对应数据块/页面：

- **块设备路径**：vhost-user-blk 协议拦截 Guest VM 的 I/O 请求，按需获取 chunk
- **快照路径**：userfaultfd 拦截页面错误，按需加载内存页

```
┌─────────────┐        ┌──────────────┐
│  Guest VM   │        │  Guest VM    │
│  virtio-blk │        │  Memory      │
└──────┬──────┘        └──────┬───────┘
       │ I/O request          │ Page fault
       ▼                      ▼
┌───────────────┐      ┌────────────────┐
│ Block Device  │      │  Snapshot      │
│ Agent         │      │  Agent         │
│ vhost-user-blk│      │  userfaultfd   │
└──────┬────────┘      └──────┬─────────┘
       │                      │
       ▼                      ▼
┌───────────────────────────────────────┐
│         Tiered Cache + Fetch          │
│     On-demand: load only what's used  │
└───────────────────────────────────────┘
```

结合三层缓存（详见 §4.4），绝大多数按需请求在本地或 L2 完成（延迟预算见 §8.2）。

**代价**：运行时每次未缓存的访问增加网络延迟（由缓存吸收）。

### §4.3 海量镜像的存储成本

**业务问题**：SWE 场景 10 万+ 镜像，每个 1-5 GiB（均值 ~1 GiB），原始存储 ~100 TiB。RL 训练场景镜像持续增长。

**层级去重为什么不够？** 容器镜像按层（layer）组织。层级去重在共享基础层时有效（如 Alpine 基础层被广泛共享）。但在 SWE 场景中，每个 PR 构建独立镜像，层重复率低，而文件级重复率高。层级去重无法捕捉层内文件重复。

**固定大小分块为什么不行？** 将镜像切成固定 512KiB 块可以跨层去重。但当文件中间插入/删除数据时，所有后续块的边界偏移 → 全部变为"不同"块 → 去重率从 85% 降至 ~0%。

**方案：内容定义分块（FastCDC）**。基于内容特征（而非固定位置）确定分块边界：

```
Original:  [────A────][────B────][────C────][────D────]
Insert X:  [────A────][X][────B────][────C────][────D────]

Fixed-size:  chunk boundaries shift → ALL chunks after X change → 0% dedup
FastCDC:     content-defined boundaries stable → only 1-2 chunks change → 85%+ dedup
```

- 使用 Gear 滚动哈希检测内容边界
- 参数：最小 64 KiB、平均 512 KiB、最大 1 MiB
- 本地修改只影响修改处附近的 chunk，其余 chunk 不受影响

**块级而非文件级去重**：选择块级去重而非文件级去重有两个关键原因。其一，安全——文件系统在 Guest VM 内部，Host 不应暴露文件语义，块级操作保持 Host 对 Guest 内容的零感知。其二，简单——无需理解各种文件系统格式和 overlay 语义，一条统一管线处理所有数据。

**镜像展平**：容器镜像写入前需确定性展平（deterministic flatten）为块设备镜像。确定性展平使相同基础层产生相同块内容，是块级去重的前提。展平顺序固定，相同输入总是产生相同输出。

**量化证据**：

| 场景 | 固定分块去重率 | 内容定义分块去重率 |
|------|---------------|-------------------|
| 相同镜像 | 100% | 100% |
| 文件修改（无偏移） | 62.5% | 85-90% |
| 新增小文件（偏移发生） | ~0% | 80-90% |
| 修改已有文件 | ~0% | 70-85% |

行业大规模生产数据：80% 的新上传应用产生零唯一 chunk（纯重新部署）；剩余 20% 中均值仅 4.3% 唯一 chunk，中位数 2.5%。总存储节约可达 ~23x。

**代价**：写入路径 CPU 开销（滚动哈希计算），变长 chunk 需要二分查找索引。

### §4.4 运行时数据访问延迟与尾延迟

**业务问题**：按需加载将"启动等待"转化为"运行时关键路径访问"。每次缓存未命中 = 一次远端往返（P50 36ms, P99.9 175ms）。一次启动需获取的 chunk 数因场景而异（见 §8.1），即使最轻量场景（128 chunk），仍有 12% 的启动遭遇至少一次 P99.9 尾延迟；大镜像场景升至 47%。这包含两个递进的问题：基础延迟——每次远端访问 P50 36ms，直接决定启动速度；尾延迟放大——数百 chunk 场景下 P99.9 事件几乎必然发生，决定启动体验的下限。

**为什么需要多层缓存？** 远端对象存储延迟是基础设施固有特性，不可优化——必须在近端吸收绝大多数请求。但单节点本地 SSD 容量有限（1-2 TiB），命中率 ~67%，仍有 33% 请求落到远端。增大本地缓存无法根本解决：节点服务数百种镜像，生命周期访问率 30-50%，工作集超出单节点容量。

解决方案是引入 AZ 级共享缓存层（L2）。L2 容量覆盖全量热数据——100 万镜像去重后 ~43 TiB，在 L2 集群容量范围内（详见 §8.5）；任一计算节点拉取的 chunk 进入 L2 后所有节点都能命中，命中率 >99.9%。最终到达远端的请求约 0.035%（L1 miss ~35% × L2 miss ~0.1%）。

**方案：三层缓存**

| 层 | 位置 | 延迟 | 命中率 |
|----|------|------|--------|
| L1 | 本地 SSD | <500µs（SSD），热数据 <100µs（page cache） | >65% |
| L2 | 可用区分布式集群 | P50 550µs, P99.9 3.7ms | >99.9% |
| L3 | 远端对象存储 | P50 36ms, P99.9 175ms | 兜底 |

**降尾延迟**：三层缓存解决基础延迟，但 L2 本身也有尾延迟——节点拥塞或故障时单次请求可能超时。数百 chunk 的启动场景下，至少一次命中 L2 尾延迟的概率显著。L2 使用 4-of-5 纠删码：并行请求 5 个分片取最快 4 个重建数据，最慢节点被自然丢弃，尾延迟和全延迟分布均改善约 20%，存储开销仅 25%（详见 §6.8）。

**代价**：分布式缓存运维复杂度（L2 集群 ~110-130 节点/AZ），纠删码编解码 CPU 开销。

### §4.5 多租户共享基础设施的数据安全

**业务问题**：多租户共享存储和缓存。存储/缓存被攻破时不能暴露任何租户明文。但跨租户去重要求系统能识别相同内容——安全和去重看似矛盾。

**传统加密为什么不行？** 每租户独立密钥 → 相同内容产生不同密文 → 无法跨租户去重。等于放弃核心存储优势。

**方案：收敛加密**。从内容本身派生加密密钥：

```
┌───────────────────────────────────────────────────┐
│              Convergent Encryption                │
│                                                   │
│  plaintext ──► key = SHA256(salt || plaintext)    │
│                  │                                │
│                  ▼                                │
│  ciphertext = AES-256-CTR(key, iv=0, plaintext)   │
│                  │                                │
│                  ▼                                │
│  chunk_name = SHA256(ciphertext)                  │
│                                                   │
│  Same content → same key → same ciphertext → dedup│
└───────────────────────────────────────────────────┘
```

- 相同内容 → 相同密钥 → 相同密文 → 可去重
- 存储/缓存只见密文，解密仅在靠近 VM 的 Agent 层发生
- 密钥表存入 Manifest，用客户管理的密钥（AES-256-GCM）加密
- Manifest 头部和 chunk 条目作为附加认证数据（AAD），防篡改

**代价**：对已知明文攻击有理论弱点（可确认某内容是否存在）。对本场景可接受——存储的是系统级数据（OS、运行时），非用户敏感数据。

### §4.6 高共享数据的访问集中与连锁故障

**业务问题**：基础层（Alpine、libc、Python 运行时）被大量镜像共享。这些 chunk 的哈希固定 → 一致性哈希永远映射到相同缓存节点 → 单节点故障影响面极大。

**一致性哈希为什么不够？** 一致性哈希解决节点变更时的迁移问题，不解决热点集中。高共享数据哈希固定 → 永远在相同节点。

**方案：Salt 分代旋转**。不同代的数据使用不同 salt → 相同内容产生不同密文/哈希 → 映射到不同缓存节点。

```
Generation G1 (salt_1): Alpine chunk → hash_A → Nodes {3, 7, 12}
Generation G2 (salt_2): Alpine chunk → hash_B → Nodes {1, 9, 14}
Generation G3 (salt_3): Alpine chunk → hash_C → Nodes {5, 11, 19}
```

3 个活跃代时，1000 请求/秒从集中在 5 个节点 → 分散到 15 个节点。

**代价**：同一内容在不同代中存储多份。但 80% 上传是纯重复上传（零唯一 chunk），实际存储增量 = 去重后基准 × 活跃代数。3 代意味着约 3x 于单代去重后的大小——相比无去重的原始数据仍节省数量级。

### §4.7 大规模共享存储的空间回收

**业务问题**：百万级 Manifest 共享的 chunk 如何回收？传统引用计数需对每个 chunk 维护原子计数器——十亿级 chunk × 原子操作 = 不可行。标记-清除需遍历所有 Manifest——一致性窗口期间可能误删。

**方案：分代 GC**。Salt 天然将数据分为自包含的代（Generation）。每代使用不同 salt → 不同代的相同内容产生不同 chunk_name → 无跨代引用。

```
┌───────────┐     ┌───────────┐     ┌───────────┐     ┌───────────┐
│ Active    │────►│ Retired   │────►│ Expired   │────►│ Deleted   │
│ (R/W)     │     │ (R only,  │     │ (alarm on │     │ (gone)    │
│           │     │  migrate  │     │  read,    │     │           │
│           │     │  data to  │     │  auto-    │     │           │
│           │     │  new gen) │     │  pause    │     │           │
│           │     │           │     │  delete)  │     │           │
└───────────┘     └───────────┘     └───────────┘     └───────────┘
```

- **Active**：新写入进入此代
- **Retired**：停止写入，将仍在使用的 Manifest 迁移到新代
- **Expired**：数据仍可读但触发告警——安全网，自动暂停删除流程
- **Deleted**：整代目录删除 = O(1) 操作

**代价**：迁移成本（解密 + 用新 salt 重加密）。实际迁移量可控：高去重率意味着每代新增的唯一 chunk 占比小（80% 新上传应用产生零唯一 chunk），且应用频繁重部署使旧 Manifest 在代退役前已被新版本替代，无需迁移。

### §4.8 快照去重率对 Guest 运行时状态敏感

**业务问题**：KASLR（内核地址空间随机化）、ASLR（用户空间随机化）、并行初始化、文件系统缓存使相同配置的 VM 内存布局不同。去重率从 >90% 降至 50-70%。磁盘状态同样受运行时影响——文件系统缓存、临时文件、日志使不同实例的可写层内容分化，降低磁盘快照去重率。

**商业影响**：单应用 200 实例 × 512 MiB = 100 GiB 原始数据。确定性初始化下（>90% 跨实例去重），实际存储 ~512 MiB × 3 代 ≈ 1.5 GiB；非确定性时各实例内存布局不同，跨实例去重失效（50% 去重），存储接近 ~50 GiB——约 33x 差异直接决定 Warm Pool 是否经济可行。

**方案：确定性 Guest 配置**

- 内核参数：`nokaslr norandmaps`（固定内核/用户空间基址）
- 只读根文件系统（无状态积累）
- 顺序初始化（无并行启动的时序依赖）
- 快照前清理（清除 /tmp、丢弃缓存、关闭连接）

**量化**：

| 内存一致性 | 去重率 | 场景 |
|-----------|--------|------|
| 确定性初始化 | >90% | SnapStart / Warm Pool |
| 非确定性 | 50-70% | 标准部署 |
| 不同应用 | 30-50% | 共享基础设施 |

上表为内存快照数据。磁盘可写层遵循相同规律：确定性清理（只读 rootfs + Guest Agent 清除缓存和临时文件）使跨实例磁盘状态高度一致，非确定性时可写层内容分化导致去重失效。

**代价**：禁用 KASLR/ASLR 降低安全性。对短生命周期、网络隔离的 VM 可接受。SnapStart 模式下快照恢复后需重新播种熵池。

---

## §5 系统架构

### §5.1 系统定位

```
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │ Platform mgmt plane  (out of scope)                                                      │
  │   sandbox mgmt platform   /   image flatten mgmt   /   image registry                    │
  ├──────────────────────────────────────────────────────────────────────────────────────────┤
  │ This solution                                                                            │
  │   Sandbox & VMM       sandbox control / VMM / Guest runtime / on-demand block/snapshot   │
  │                       / density & resource control                                       │
  │   Data acceleration   ingest / chunk / encrypt / manifest / content-addressed store      │
  │                       / tiered cache                                                     │
  ├──────────────────────────────────────────────────────────────────────────────────────────┤
  │ Infrastructure                                                                           │
  │   compute / KVM / object storage / network                                               │
  └──────────────────────────────────────────────────────────────────────────────────────────┘
```

本方案覆盖沙箱运行与数据按需加载与访问加速两层。向上对接平台管理面（沙箱管理平台、容器镜像仓库，本方案外）；二者由部署在每个节点上的**沙箱编排（sandbox-orchestrator / orchestrator-ctl，本方案内，§10.13）**衔接：它对外提供 e2b 兼容 API、调用 run 拉起沙箱、回传生命周期；与平台管理面的对接由其上层 platform-agent（本方案外/未来）承担。向下消费 KVM、对象存储与网络。本方案不要求平台侧了解分块、加密、缓存与 VMM 实现细节。

本方案支持两条运行时路径（块设备路径 vhost-user-blk、快照路径 userfaultfd）和三种启动模式（镜像冷启动、SnapStart、Warm Pool），共享同一套基础设施——内容分块、收敛加密、内容寻址存储、分层缓存和清单。

### §5.2 总体架构

总体架构按节点角色组织。计算节点承载沙箱运行与本地缓存；可用区内部署二级缓存集群；区域内部署对象存储与代管理；镜像展平在独立的数据面节点池完成。

```{.small}
        ┌── Platform mgmt plane  (out of scope) ───────────────────────────────────────────────────────────┐
        │   sandbox mgmt platform      /      image flatten mgmt      /      image registry                │
        └──────────────────────────────────────────────────────────────────────────────────────────────────┘
      per-sandbox config & keys             │                                         flatten task    │
                                            ▼                                                         ▼
  ┌── Compute node  (×N per AZ) ───────────────────────────────────────────────────────┐  ┌── Image flatten  (pool) ─┐
  │                                                                                    │  │                          │
  │  orchestrator-ctl  (in-scope; platform-agent ext.)                                 │  │  image flatten           │
  │    lands per-sandbox config & keys / prepares write-layer / invokes run            │  │        │                 │
  │        │ run                                                                       │  │        ▼                 │
  │        ▼                                                                           │  │  write path              │
  │  sandbox control (one per sandbox)   ◄─ proto ─►   node resource control           │  │  chunk / encrypt / dedup │
  │    block dev / snapshot / unified memory /      admission / quota /                │  └──────────────────────────┘
  │    balloon reclaim / handshake                  reclaim / persist  (node-ctl)      │              │
  │        │ drive VM                        │ fetch / store                           │              │
  │        ▼                                 ▼                                         │              │
  │  sandbox ×N: VMM + Guest runtime     tiered cache L1 (local) + CA store            │              │
  │                                                                                    │              │
  └────────────────────────────────────────────────────────────────────────────────────┘              │
                                        L1 miss   │                                                   │   chunks /
                                                  │                                                   │   manifest
                                                  │                                                   │
                                                  ▼                                                   ▼
      ┌── AZ ──────────────────────────────────────────┐    ┌── Region ──────────────────────────────────────────────┐
      │                                                │miss│                                                        │
      │   L2 cache cluster                             │──► │  object store (L3)                                     │
      │   EC 4-of-5 / consistent hash                  │    │  encrypted chunks & manifests / generational layout    │
      └────────────────────────────────────────────────┘    │  GC & generation mgmt service  (control plane):        │
                                                            │  rotation / migration / reclaim / ops                  │
                                                            └────────────────────────────────────────────────────────┘
```

沙箱编排（sandbox-orchestrator / orchestrator-ctl，本方案内，§10.13）部署在每个计算节点上、与沙箱控制同处一节点；它对外提供 e2b 兼容 API、调用 run 拉起沙箱、回传生命周期。与平台管理面的对接由 platform-agent（本方案外/未来）承担，不属于平台层。

分层缓存（L1/L2）是可替换的访问加速层：可由 SFS Turbo（支持 OBS 访问加速的托管 NAS 服务）承担，由托管服务提供对对象存储的近端加速，无需自建缓存集群。GC 与代管理服务作用于对象存储，处于控制平面，不在读写数据热路径上。

---

## §6 技术组件设计

### §6.1 写入层（Ingest）

**职责**：将原始数据（磁盘镜像或内存快照）转化为去重、加密、内容寻址的存储。

**镜像展平**：容器镜像在进入分块管线前，需确定性展平为块设备镜像。展平过程将多层 overlay 合并为单一块设备镜像，且展平顺序固定——相同基础层总是产生相同块内容，这是块级去重的前提。

**展平目标格式**：展平的目标文件系统格式影响去重效果和下游读写设计。EROFS（只读文件系统）布局确定性强，利于跨镜像去重；只读特性与内容寻址不可变存储天然匹配。只读挂载场景下（如 EROFS），块设备代理无需 CoW overlay，读写支持通过 Guest 侧 overlayfs 或独立的可抛弃稀疏镜像（由另一块设备代理实例以 CoW 模式服务）实现。

**内存快照**：VM 暂停后，VMM 导出 Guest 内存。内存区域直接作为数据流进入分块管线，无需展平等预处理。

**磁盘快照**：VM 暂停后，块设备代理导出 CoW overlay 内容。overlay 与按需加载层内容增量合并为完整磁盘状态后，作为数据流进入分块管线。按需加载层为空的全新磁盘（如与只读块设备镜像配合使用的独立可写层）可跳过合并步骤。

**流水线**：

```{.small}
Container Image      Memory Snapshot      Block Device Snapshot
       │                  │                      │
       ▼                  │                      │
Deterministic Flatten     │               Incremental Merge
(layers → EROFS image)    │               (base + CoW overlay)
       │                  ▼                      │
       └────────────► Data Stream ◄──────────────┘
                          │
                          ▼
                    Chunking (§6.3) ─► Convergent Encrypt ─► Dedup Check ─► Store.Put
                          ▲                    ▲                 │            │
                          │                    │ct               ▼            ▼
                 Pluggable strategy     key=SHA256(salt||pt)   Exists?     Atomic write
                  (FastCDC / fixed)     ct=AES-CTR(key,pt)   skip if yes  (Idempotent)
                                               │                              │
                                               │key                           ▼
                                               │                           Manifest
                                               └────────► (record per chunk: offset, size, hash, key)
                                                                              │
                                                                              ▼
                                                                Seal key table with customer key
                                                                       (AES-256-GCM)
```

关键属性：

- **写入幂等**：内容寻址 PUT-if-not-exists，重复写入无副作用
- **流式处理**：无需全文件缓冲，内存占用与数据大小无关
- **统一管线**：容器镜像、内存快照和磁盘快照共享同一分块管线。展平和增量合并分别是容器镜像和磁盘快照特有的上游预处理

### §6.2 读取层（Fetch）

**职责**：从 Manifest + Store 按需读取和解密任意字节范围。

```
Byte Range Request (offset, length)
       │
       ▼
Binary Search in Manifest
(O(log n), variable-size chunks)
       │
       ▼
Cache Lookup: L1 ─► L2 ─► L3
       │
       ▼
Decrypt: AES-256-CTR with per-chunk key from Manifest
       │
       ▼
Plaintext ─► Caller
```

关键属性：

- **可插拔预取**：顺序预取适合块设备流式读，chunk 对齐预取适合快照页面错误（一次错误加载整个 chunk，避免同 chunk 后续页面再次触发网络请求）
- **缓存回填**：L2 未命中时从 L3 获取的 chunk 回填 L1 和 L2（详见 §6.8）

### §6.3 内容定义分块

**职责**：将连续数据流切分为 chunk。支持多种分块策略（由 Manifest 指定），不硬编码单一方式。

**策略框架**：

| 策略 | 适用场景 | 去重特性 |
|------|----------|----------|
| FastCDC（内容定义） | 镜像分发、跨版本去重 | 局部修改 85-90% 去重 |
| 固定大小 | 内存快照、确定性场景 | 内容不偏移时 100% 去重 |
| 未来扩展 | 按业务特征选择 | — |

Manifest Header 中的 ChunkMode 字段指定当前使用的分块策略，读取层据此选择正确的索引方式。所有策略产生的 chunk 均为 4K 对齐，确保与页面粒度兼容。

**FastCDC（默认策略）工作原理**：

```{.small}
FastCDC Two-Phase Normalization

Bytes scanned since last cut point →

├── (no cut) ───┼────────── strict mask ───────────┼────────── relaxed mask ──────────┤ ← (forced cut)
0               Min 64K                            Avg 512K                           Max 1M

                        low P(cut)                        high P(cut)
                        hard-to-match mask                easy-to-match mask
                        push toward Avg →                 ← pull toward Avg

                                           ◄── normalize ──►

Effect: chunk sizes cluster around Avg, bounded by [Min, Max]
```

### §6.4 收敛加密

**职责**：加密 chunk 使相同内容总是产生相同密文，支持密文层面去重，同时保持存储层零明文。

```
                    Ingest Path
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
    Key Derivation   Encryption    Naming
    key=SHA256       ct=AES-CTR    name=SHA256
    (salt||pt)       (key,0,pt)    (ciphertext)
          │             │             │
          ▼             ▼             ▼
    Stored in        Stored in      Used as
    Manifest         Store          address
    (encrypted       (ciphertext    in Store
     with customer    only)
     key)
```

Salt 参数控制去重域：同一 salt 内去重，不同 salt 间隔离。Salt 来源于当前活跃代（Generation），既控制去重范围，又为 GC 提供自然边界。

### §6.5 清单（Manifest）

**职责**：将虚拟镜像/快照映射到加密 chunk 集合。包含按需重建任意部分所需的全部元数据。

```
┌───────────────────────────────────────┐
│           Header (64 bytes)           │
│  Magic, Version, ImageSize,           │
│  ChunkCount, ChunkMode, ...           │
├───────────────────────────────────────┤
│       Chunk Entries (56B x N)         │
│  Offset, Size, Flags, CiphertextHash  │
│  ... repeated N times ...             │
├───────────────────────────────────────┤
│     Encrypted Key Table               │
│  GCM Nonce + Encrypted Keys + Tag     │
│  (sealed with customer key)           │
│  Header + Entries as AAD              │
└───────────────────────────────────────┘
```

开销分析：10 GiB 镜像 ÷ 512 KiB 平均 chunk = ~20,000 chunk × 56 字节 = ~1.1 MiB + 密钥表 ~0.7 MiB ≈ **1.8 MiB（原始数据的 0.018%）**。

变长 chunk 使用二分查找 O(log n) 定位，20,000 chunk 仅需 ~15 次比较。

### §6.6 内容寻址存储

**职责**：持久化存储加密 chunk，以密文哈希为键。

```
{store_root}/
├── G1/                    ← retired generation (salt_1)
│   ├── a1/b2/a1b2c3...
│   └── f0/e1/f0e1d2...
├── G2/                    ← active generation (salt_2)
│   ├── a1/b2/a1b2...
│   └── c3/d4/c3d4...
└── G3/                    ← active generation (salt_3)
    └── ...
```

- 两级哈希前缀（每级 ~256 条目）防止单目录文件过多
- PUT 幂等：相同 chunk 重复写入无副作用
- Generation 之间无共享 chunk（salt 保证），整代删除 = O(1)

### §6.7 Salt 与分代 GC

**职责**：控制去重域边界，实现安全的存储空间回收。

```
┌───────────┐     ┌───────────┐     ┌───────────┐     ┌───────────┐
│ Active    │────►│ Retired   │────►│ Expired   │────►│ Deleted   │
│ (R/W,     │     │ (R only)  │     │ (alarm on │     │ (gone)    │
│  new      │     │ migrate   │     │  read,    │     │           │
│  writes)  │     │ active    │     │  auto-    │     │           │
│           │     │ Manifests │     │  pause    │     │           │
│           │     │           │     │  delete)  │     │           │
└───────────┘     └───────────┘     └───────────┘     └───────────┘
```

- Salt = Generation ID → 不同代的相同内容 → 不同 chunk_name → 无跨代引用
- 整代删除 = O(1)：删除一个目录，无需引用计数
- 安全网：Expired 态访问触发告警并自动暂停删除，防止误删仍在使用的数据

### §6.8 分层缓存

**职责**：将频繁访问的密文 chunk 保持在靠近 Agent 的位置。

```
┌───────────────────────────────────────────────────────────────────┐
│                      Cache Architecture                           │
│                                                                   │
│  ┌───────────────┐ miss  ┌──────────────────────┐ miss  ┌──────┐  │
│  │ L1 Local SSD  │──────►│ L2 Distributed       │──────►│  L3  │  │
│  │ LRU-k         │       │ Cluster              │       │  OBS │  │
│  │ ~1-2 TiB      │       │ ┌────────┐ ┌───────┐ │       │      │  │
│  │ <500us (SSD)  │       │ │Hot     │ │Cold   │ │       │      │  │
│  │ page cache:   │       │ │Memory  │ │SSD    │ │       │      │  │
│  │  <100us       │       │ │~200GiB │ │~3TiB  │ │       │      │  │
│  │ hit >65%      │       │ │<1ms    │ │low-ms │ │       │      │  │
│  │               │       │ └────────┘ └───────┘ │       │      │  │
│  │               │       │ EC 4-of-5            │       │      │  │
│  │               │       │ hit >99.9%           │       │      │  │
│  └───────────────┘       └──────────────────────┘       └──────┘  │
│                                                                   │
│  Erasure Coding Read Path:                                        │
│  ┌──────┐                                                         │
│  │Client│──► Request 5 shards in parallel                         │
│  │      │◄── Take first 4 responses                               │
│  │      │    Reconstruct chunk (Reed-Solomon)                     │
│  │      │    Slowest shard dropped automatically                  │
│  └──────┘                                                         │
│                                                                   │
│  Cache Fill Path (on L2 miss):                                    │
│  ┌──────┐                                                         │
│  │Client│──► Fetch complete chunk from L3 (OBS)                   │
│  │      │    Reed-Solomon 4-of-5 encode (client-side)             │
│  │      │──► Parallel PUT 5 shards to L2 (fire-and-forget)        │
│  │      │──► Write complete chunk to L1 (local SSD)               │
│  └──────┘                                                         │
│                                                                   │
│  Stability Protection:                                            │
│  ● L3 concurrency semaphore (limit in-flight requests)            │
│  ● Circuit breaker (>50% error rate → fail fast)                  │
│  ● Peer warming (new nodes load from peers, not L3)               │
└───────────────────────────────────────────────────────────────────┘
```

**L1 设计**：节点对镜像/快照的访问语义均为文件级，本地 SSD 是自然缓存介质。节点 5T+ SSD，分配 1-2 TiB 给 L1。工作集不限于启动阶段的 6.4%——实例运行期间逐渐访问更多数据（生命周期访问率 30-50%）。500-1000 种镜像 × 1 GiB × 40% = 200-400 GiB（去重前），基础层 >75% 重叠，唯一工作集 ~50-100 GiB，均在 1-2 TiB 容量范围内。命中率主要受冷启动 miss 约束（新容器首次启动的 chunk 不在本地），行业参考 per-worker 67%。SSD 相比内存小缓存的优势在于容量充足、可持久化，减少因驱逐导致的重复拉取。OS page cache 对热数据提供隐式内存加速。

**L1 抗扫描**：使用 LRU-k (k=2) 算法。顺序扫描的 chunk 留在"冷链表"快速驱逐，频繁访问的 chunk 提升到"热链表"受保护。避免大批量启动冲刷常驻热数据。

**L2 集群规划**：L2 部署限定在可用区内——跨可用区网络延迟通常在 1-5ms 量级，会突破 L2 P99.9 <4ms 的延迟预算；AZ 内部网络往返 P99 <1ms，满足 P50 550µs 的目标。

镜像和快照虽共享技术实现层（分块、去重、加密、EC），但业务特征不同，L2 部署可分为独立集群：

> **L2-镜像**：服务所有容器块设备路径。容量需求：100 万镜像去重后 ~43 TiB（EC 4/5 → 54 TiB 原始）。吞吐需求：200 个计算节点同时突发创建沙箱（各 ~100 VMs/sec），聚合 ~448 GiB/s。每节点 40G NIC (~5 GiB/s) → 需 ~90 节点（吞吐驱动）。
>
> **L2-快照**：服务 Warm Pool / SnapStart 快照恢复。容量需求：~7.5 TiB（10% 应用启用 Warm Pool）。吞吐需求：RL Rollout 峰值 ~22 GiB/s → 需 ~20-30 节点（含突发余量）。
>
> 两个集群合计 ~110-130 节点/AZ。

**纠删码降尾延迟**：L2 使用 4-of-5 纠删码方案：

- 每个 chunk 编码为 5 个分片，分布到 5 个缓存节点
- 读取时并行请求 5 个分片，取最快返回的 4 个即可重建数据
- 最慢的 1 个节点（可能是故障/拥塞节点）被自然丢弃
- 效果：不仅降 P99 尾延迟，全延迟分布改善 ~20%（含 P50）
- 存储开销仅 25%（vs 三副本 200%）

**L2 冷热分层**：内存层（~10% 容量）服务热数据，延迟 <1ms。SSD 层（~90% 容量）服务温数据，延迟 <4ms。10x 容量扩展，存储成本降至纯内存方案的 ~1/5。

**缓存填充与淘汰**：L2 采用客户端旁路填充（fill-aside）。L2 未命中时，客户端从 L3 获取完整密文 chunk，本地 Reed-Solomon 4-of-5 编码为 5 个分片（每个 = chunk_size/4），通过 consistent hashing with bounded loads 映射到 5 个不同 L2 节点，并行 PUT（fire-and-forget），同时写入 L1。L2 节点仅作为简单 KV 存储——不访问 L3、不持有凭证、不执行编解码；EC 编码 CPU 分散到数千客户端节点。写入幂等，并发 miss 的重复填充无副作用。淘汰由各节点独立执行（SSD 层满时 LRU 淘汰最冷分片），无需节点间协调。读取时分片缺失触发回填：1 分片缺失从剩余 4 分片重建并回填；2+ 分片缺失回退 L3 获取完整 chunk，重新编码回填所有分片。

**稳态保护**：99.9% 缓存命中率意味着缓存故障时流量放大 1000x。三重保护：

1. **L3 并发信号量**：限制同时飞行的远端请求数
2. **断路器**：错误率 >50% 时快速失败，避免雪崩
3. **对等预热**：新节点从健康节点而非 L3 加载数据

缓存仅存密文——无需密钥管理，泄露仅暴露密文，可跨租户共享。

**可替换层**：分层缓存（L1/L2）在架构上是可替换的访问加速层。当延迟与吞吐满足目标时，L1/L2 可由 SFS Turbo（支持 OBS 访问加速的托管 NAS 服务）承担——由托管服务提供对对象存储的近端加速；自建实现与托管方案对上层提供相同的访问语义。

### §6.9 块设备代理

**职责**：向 Guest VM 呈现按需加载的虚拟块设备，拦截 I/O 请求并按需获取数据。

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
              │             │            │               │
              │ dirty page? │            │               │
              │ → overlay   │            │               │
              │ clean page? │            │               │
              │ → fetch     │            │               │
              └─────────────┘            └───────────────┘
```

**vhost-user-blk 选型**：块设备代理作为独立用户空间进程，通过 vhost-user-blk 协议与 VMM 交互。纯用户空间路径避免内核穿越开销，对齐 I/O 请求端到端延迟预算（见 §8.2）。独立进程使 Agent 可独立于 VMM 部署和升级。

**读写与 CoW overlay**：读取时先查 CoW overlay——脏页从 overlay 读取，干净页通过 Fetch 层按需获取。写入标记页面为脏并写入稀疏 overlay 文件，底层存储保持不可变。CoW overlay 内容可独立导出，支持快照捕获。

**磁盘快照捕获与恢复**：块设备代理支持将 CoW overlay 独立导出。导出的 overlay 与按需加载层增量合并后通过写入层（§6.1）分块保存。写层导出按内容寻址命名——先计算指纹再以指纹命名落盘，同一沙箱重复保存自动落到同名文件。恢复时，已保存的磁盘状态成为新的按需加载层，CoW overlay 以空文件重新开始。

**只读挂载优化**：块设备镜像以只读方式挂载时（如 EROFS），根镜像块设备为只读，CoW overlay 不需要，块设备代理简化为纯读取路径。Guest 通过 overlayfs 挂载完整文件系统，可写层为独立的 EXT4 块设备，由另一个块设备代理实例以常规 CoW 模式服务。

关键属性：I/O 请求端到端 P50 <500µs（见 §8.2）。

### §6.10 快照代理

**职责**：处理 VM 内存快照的按需恢复。内存快照的捕获由 VMM 直接完成——Guest 内存是 VMM 管理的私有内存，无需 Agent 参与。

```
VM Resume ─► Memory Access ─► Page Fault
                                  │
                                  ▼
                           userfaultfd handler
                                  │
                        ┌─────────▼───────────┐
                        │  Page in bitmap?    │
                        │  (already loaded?)  │
                        └──┬──────────────┬───┘
                        yes│              │no
                           ▼              ▼               ┌───────────────┐
                         skip         Fetch chunk ───────►│  Fetch Layer  │
                                  (64K-1M, 4K aligned)    └───────────────┘
                                          │
                                          ▼
                              Load all pages in this chunk
                                     (prefetch)
                                          │
                                          ▼
                                     UFFDIO_COPY
                             (zero-copy to VM address space)
```

**按需加载**：快照内存仅 20-40% 在首次运行期间实际访问，全量预加载传输 >60% 无用数据。userfaultfd 使 VM 恢复后的内存访问透明地按需从远端加载，Guest 无感知。

**chunk 级预取**：页面错误以 4KiB 为触发单位，但网络加载以 chunk 粒度进行（64K-1M，4K 对齐）。单次网络请求有固定开销（调度、Manifest 查找、解密），chunk 粒度加载的边际成本远低于多次独立 4KiB 请求。同 chunk 页面空间局部性强，首次错误后同 chunk 后续页面不再触发网络请求。

SnapStart：同一 Manifest，恢复 N 次。Warm Pool：每个 Manifest 恢复一次后回收。快照的去重率依赖创建阶段的确定性配置（见 §6.11）。

关键属性：恢复端到端 P50 <80ms、P99 <200ms（见 §8.2）。Bitmap 跟踪已加载页面，避免重复请求。

### §6.11 VMM 与 Guest 环境

**职责**：提供轻量 VM 运行环境，包括 VMM 选型与定制、Guest 启动模型和确定性配置。是块设备代理（§6.9）、快照代理（§6.10）和密度与资源控制（§6.12）的运行基础。

**VMM 选型**：选定 Cloud Hypervisor（基于 Rust / rust-vmm，KVM 硬件虚拟化）。理由是其 vhost-user-blk 以及含该设备的快照恢复成熟，契合冷启动—预热—快照—恢复的完整流程。需在其上定制三处：

1. **外部缺页处理器**：在 VMM 进程内创建缺页通道并将句柄交给宿主侧缺页处理器，使内存按需加载成立（§6.10）。
2. **外部传入的内存后端**：VMM 复用宿主侧创建并持有的可封印内存对象作为内存后端，而非自建（内存统一持有，见下）。
3. **快照跳过外部托管内存区**：保存快照时不导出由宿主托管的内存区，恢复时也不填充，留给外部缺页机制按需触发。

**内存统一持有**：VM 内存由宿主侧控制进程创建并持有为可封印内存对象，VMM 复用同一块内存。冷启动与快照恢复因此走同一套内存机制；保存快照时宿主直接扫描该内存的驻留页，无需经 VMM 中转。

**vhost-user-blk 与快照协同**：预热流程（§3.2）要求 VM 先通过 vhost-user-blk 冷启动再创建快照。快照保存时 VMM 仅保存内存与 CPU 状态，Block Device Agent 独立管理磁盘状态（§6.9 CoW overlay 导出）；恢复时 Block Device Agent 先就绪，VMM 恢复内存快照后建立新的 vhost-user-blk 连接——Guest 内 virtio-blk 驱动状态已在内存快照中恢复，对 Guest 透明。

**Guest 启动模型**：VM 内无容器运行时。只读 EROFS 和可写层分别作为两个 vhost-user-blk 设备挂入，Guest 内通过 overlayfs 组装为完整 rootfs（§6.9）。定制 init（Guest 一号进程）取代通用 init 系统，顺序执行：

1. 环境准备（挂载文件系统、根文件系统组装、网络配置）
2. 启动握手：从宿主取回启动配置
3. 拉起客户应用进程（容器 entrypoint）并监督其生命周期

该 Guest 一号进程同时承载 vsock 控制面，响应快照前清理、内存上报等外部指令——无独立的 Guest Agent 进程。

**确定性配置**：最大化跨 VM 实例的内存和磁盘状态一致性，提升快照去重率。

```
┌────────────────────────────────────────────────────────┐
│              Deterministic Guest Config                │
│                                                        │
│  Kernel:   nokaslr norandmaps                          │
│            (fixed kernel/user address base)            │
│                                                        │
│  Root FS:  read-only (no state accumulation)           │
│                                                        │
│  Init:     sequential (deterministic process order)    │
│                                                        │
│  Pre-snap: guest PID1 clears caches, drops buffers,    │
│            closes connections, flushes /tmp            │
│                                                        │
│  Post-restore: re-seed entropy pool                    │
└────────────────────────────────────────────────────────┘
```

量化影响：

| 配置 | 去重率 | 200 实例 × 512 MiB 存储 |
|------|--------|--------------------------|
| 确定性 | >90% | ~1.5 GiB (含 3 代 salt) |
| 非确定性 | 50-70% | ~50 GiB |
| 差异 | — | ~33x |

确定性配置在快照创建流程中落地：VM 完成应用就绪后，Guest 一号进程执行清理（Pre-snap），随后 VMM 暂停 VM，导出内存快照并捕获磁盘状态，经写入层（§6.1）分块保存。配置的确定性直接决定快照的跨实例一致性。

### §6.12 密度与资源控制

**职责**：在固定虚拟硬件规格下，通过每沙箱与节点两个控制环动态管理各 VM 实际物理内存等资源，最大化单节点并发密度。

```{.small}
┌───────────────────────────────────────────────────────────────────────┐
│                     Density & resource control                        │
│                                                                       │
│  Per-sandbox loop (balloon, host-driven)                              │
│    Boot: balloon=0 → guest sees full capacity                         │
│    Guest reports MemAvailable periodically (vsock)                    │
│    Host drives inflate target → reclaim idle pages                    │
│                                                                       │
│  Node loop (resource arbitration)                                     │
│    Admission & concurrency control (rate + in-flight cap)             │
│    Quota by watermark + rate limit; emergency pool                    │
│    Active reclaim: converge quota toward actual usage                 │
│    State persistence & scan recovery                                  │
│                                                                       │
│  Cross-cutting                                                        │
│    KSM: cross-VM identical-page merge (host kernel)                   │
│    Cgroup watermark backpressure / uffd page-load backpressure        │
│                                                                       │
│  Modes: no-cgroup / static / dynamic                                  │
│  CPU: cgroup weight + quota, no hotplug                               │
└───────────────────────────────────────────────────────────────────────┘
```

每沙箱环：VM 启动时 balloon=0，settled 后宿主侧控制器按 Guest 周期上报的 MemAvailable（短连接 vsock）推 balloon 的 inflate target——按实际需求保留物理内存、回收闲置部分，而非按固定规格预分配。

节点环：节点资源仲裁统一管理本机资源——以令牌桶限制创建速率与并发、按高/低/应急三档水位与限速发放内存额度、把已稳定沙箱的额度向实际用量收敛（留冷却窗口避免抖动）、状态持久化于内存盘并支持扫描恢复。两环通过资源协议对话：沙箱启动时申请准入与额度，运行期上报用量、按压力申请扩额，断连时降级为按当前额度继续运行。

资源约束支持三态：无控制组（不设限）、静态限制（固定上限）、动态调控（额度随仲裁结果变化）。叠加 KSM 跨 VM 相同页合并（对确定性配置下 >90% 内存一致的实例尤为有效）、cgroup 水位反压、userfaultfd 页加载背压。CPU 通过 cgroup 权重与配额表达，竞争时按权重分配，Guest 无感知，无需热插拔。

---

## §7 安全模型

### §7.1 信任边界

```
┌──────────────────────────────────────────────────────────────┐
│                     Trust Boundaries                         │
│                                                              │
│  ┌─────────────────┐  virtio  ┌─────────────────────────┐    │
│  │ Guest VM        │◄────────►│ Agent                   │    │
│  │ (untrusted)     │  blk/    │ (plaintext boundary)    │    │
│  │                 │  uffd    │ Decrypt here only       │    │
│  └─────────────────┘          └───────────┬─────────────┘    │
│                                           │ ciphertext       │
│                               ┌───────────▼─────────────┐    │
│                               │ Cache + Store           │    │
│                               │ (ciphertext only)       │    │
│                               │ Zero plaintext exposure │    │
│                               └─────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

解密仅发生在 Agent 层——最靠近 VM 的位置。存储和缓存全程只处理密文，即使被攻破也不暴露明文。缓存可跨租户共享（密文相同 = 内容相同），无需每租户隔离缓存。

### §7.2 威胁缓解

| 威胁 | 缓解手段 |
|------|----------|
| 存储泄露 | 仅存密文，密钥不在存储中 |
| 缓存泄露 | 仅缓存密文，无密钥管理 |
| chunk 篡改 | 密文哈希校验（SHA256），地址即校验 |
| Manifest 篡改 | AES-GCM 认证标签，Header + Entries 作为 AAD |
| 已知明文攻击 | 可确认内容存在性——对系统级数据（OS、运行时）可接受 |
| KASLR/ASLR 禁用 | 短生命周期 + 网络隔离 + 快照后重新播种熵池 |
| 跨代数据残留 | Expired 态告警 + 自动暂停删除的安全网机制 |

---

## §8 性能目标与规模分析

### §8.1 规模基线

本节汇总全文使用的规模参数与派生数据。各章节引用的容量、带宽、命中率数据以此为统一参考。

**平台参数**：

| 维度 | 基准值 | 增长目标 |
|------|--------|----------|
| 容器镜像总量 | 10 万 | 100 万 |
| 镜像均值大小 | 1 GiB（250 MiB - 10 GiB） | — |
| 快照均值大小 | 512 MiB（= Guest 内存分配） | — |
| 物理节点 | 5,000 / AZ | — |
| 每节点并发实例 | 3,000 | — |
| 单沙箱底噪 | <60 MiB | — |
| 内存实际上浮 | <15% | — |
| 每节点镜像种类 | 500-1,000 | — |
| 单 AZ 并发 | ~1500 万（5,000 节点 × 3,000） | — |
| 每节点并发创建 | ~150 VMs/sec | — |
| 实例创建速率 | 10 万/分钟持续，50 万/分钟峰值 | — |
| 节点规格 | 128+ Core, 512G+ Mem, 5T+ SSD, 25/40G NIC | — |
| 应用数 | ≈ 镜像数（10 万~100 万），典型实例数 0-200，上限 1K | — |

**派生存储**：

| 数据 | 原始 | 去重后（23x） |
|------|------|--------------|
| 镜像（10 万 × 1 GiB） | 100 TiB | ~4 TiB |
| 镜像（100 万 × 1 GiB） | 1 PiB | ~43 TiB |
| Warm Pool 单应用（200 实例 × 512 MiB） | 100 GiB | <1 GiB（>90% 去重 + 3 代） |
| Warm Pool 平台（10% 应用启用，1 万应用 × 均值 50 实例） | ~250 TiB raw | ~7.5 TiB（应用内 >90% 去重 → 5 TiB，跨应用 ~50% 重叠 → 2.5 TiB，3 代） |

**每次启动 chunk 数**：

| 场景 | 计算 | chunk 数 |
|------|------|----------|
| 镜像冷启动（均值 1 GiB） | 1 GiB × 6.4% ÷ 512 KiB | ~128 |
| 镜像冷启动（大镜像 5 GiB） | 5 GiB × 6.4% ÷ 512 KiB | ~640 |
| 快照恢复（512 MiB, 25%） | 512 MiB × 25% ÷ 512 KiB | ~256 |

尾延迟暴露概率（遭遇 ≥1 次 P99.9）：128 chunks → 12%；256 chunks → 23%；640 chunks → 47%。

### §8.2 性能指标

**VMM + 内核层基线**（本地镜像/快照，不含按需加载，作为端到端目标的下沉基线）：

| 指标 | 数值 | 说明 |
|------|------|------|
| VM 启动（VMM + 内核 + init） | 125ms | 本地镜像，不含按需加载开销 |
| 快照恢复（VMM） | 5ms | 本地快照，不含按需页面加载 |

**平台端到端目标**（不含调度时延，加速引擎为主要贡献方）：

| 指标 | P50 | P99 | 说明 |
|------|-----|-----|------|
| 冷启动（VMM 启动 + 镜像按需加载，不含应用就绪） | <250ms | <500ms | 125ms 基线 + Manifest 加载 + chunk 串行链 |
| 快照拉起（VMM 恢复 + 快照按需页面加载） | <80ms | <200ms | 5ms 基线 + Manifest 加载 + page fault 串行链 |

延迟预算分解：

```
Cold Boot Latency Budget (1 GiB image, P99):
  VMM + kernel boot                              125 ms
  Manifest fetch (L2 P99)                          3 ms
  Chunk serial chain   ~15 x L2 P99 ~1.7ms       ~25 ms
                                                ───────
  Total                                          ~153 ms  < 500 ms

  Margin: large images (5 GiB, more chunks),
          cold L1, occasional L3 fallback (P99.9 ~379ms)

Snapshot Restore Latency Budget (512 MiB, typical app, P99):
  VMM snapshot restore                              5 ms
  Manifest fetch (L2 P99)                           3 ms
  Page fault serial chain   ~20 x L2 P99 ~1.7ms   ~34 ms
                                                  ───────
  Total                                            ~42 ms  < 200 ms

  Margin: heavy apps (30+ serial fetches),
          tail L2 latency, occasional L3 fallback
```

chunk 串行链长度取决于启动/恢复的 I/O 关键路径——顺序预取（块设备路径）和 chunk 级预取（快照路径）将大部分 chunk 获取隐藏在计算/执行过程中，串行链仅为无法被预取覆盖的依赖性访问。

**加速引擎核心指标**：

| 指标 | P50 目标 | P99/P99.9 目标 | 行业参考 |
|------|----------|---------------|----------|
| 页面错误延迟 | <500µs | <5ms | — |
| L1 命中延迟 | <500µs（SSD），热数据 <100µs（page cache） | — | — |
| L2 命中延迟 | <1ms | P99.9 <4ms | P50 550µs, P99.9 3.7ms |
| L3 远端延迟 | <50ms | <200ms | P50 36ms, P99.9 175ms |
| 快照创建吞吐 | >500 MB/s | — | — |
| 端到端冷启动 | — | P99 <500ms | 相对传统全量下载秒级 >10x |
| 跨镜像去重 | >85% | — | 行业 80% 零唯一 chunk |
| L1 缓存命中 | >65% | — | 行业 per-worker 67% |
| L2 缓存命中 | >99.9% | — | 行业 99.9% |

### §8.3 镜像存储规模

**去重效率**：10 万镜像 × 1 GiB 均值 = 100 TiB 原始。行业数据 80% 上传零唯一 chunk，剩余 20% 均值 4.3% 唯一——等效存储节约 ~23x，实际存储 ~4 TiB。增长到 100 万镜像时原始 ~1 PiB，去重后 ~43 TiB。内容定义分块在偏移场景保持 85% 去重率（固定分块 ~0%）。

### §8.4 Warm Pool 存储估算

**单应用示例**（典型 Agent 应用，200 实例 × 512 MiB）：

```
200 instances x 512 MiB = 100 GiB raw
    │
    ▼ deterministic dedup (>90%)
~512 MiB base
    │
    ▼ 3 active generations (salt x3)
~1.5 GiB total = 1.5% of raw
```

**平台级估算**（估 10% 应用启用 Warm Pool，1 万应用 × 均值 50 实例）：

```
500K instances x 512 MiB = 250 TiB raw
    │
    ▼ intra-app >90% dedup → 10K x 512 MiB ≈ 5 TiB
    ▼ cross-app base layer ~50% overlap → ~2.5 TiB unique
    ▼ 3 active generations
~7.5 TiB total
```

并非所有应用使用 Warm Pool——传统 Serverless 中轻量应用通常通过冷启动+镜像按需加载即可满足延迟要求，Warm Pool 主要服务重初始化的 Agent 应用和 RL 训练场景。

### §8.5 RL 训练带宽

```
Single Rollout (30K sandbox snapshot restore, 60s):
30K x 512 MiB x 25% on-demand = ~3.75 TiB read demand
    │
    ▼ L1 >65% hit (local SSD)
~1.3 TiB reaches L2 (~22 GiB/s)
    │
    ▼ L2 >99.9% hit
~1.3 GiB reaches L3 (~22 MiB/s)
```

### §8.6 纠删码尾延迟

4-of-5 纠删码：全延迟分布改善 ~20%。在大镜像/快照场景（256-640 chunk，见 §8.1）中，23-47% 的启动会遭遇至少一次 P99.9 尾延迟。纠删码使 L2 P99.9 控制在 <4ms，避免请求落入 L3（P99.9 175ms）。存储开销 25%（vs 三副本 200%）。

### §8.7 网络带宽分析

单节点 VMM 能力为 ~150 VMs/sec（见 §8.1），考虑 I/O 路径和调度开销，以 ~100 VMs/sec 作为实际突发基线。

**每节点峰值**（100 VMs/sec 突发创建沙箱，镜像场景）：

| 路径 | 计算 | 带宽 |
|------|------|------|
| 总 chunk 需求 | 100 × 128 chunks × 512 KiB | ~6.4 GiB/s（本地 SSD 读取） |
| L1→L2 | 35% L1 miss | ~2.24 GiB/s（40G NIC 容量 ~5 GiB/s，利用率 ~45%） |
| L2→L3 | 0.1% L2 miss | ~2.24 MiB/s |

**集群峰值**（200 个计算节点同时突发创建沙箱，各 ~100 VMs/sec，镜像路径）：

| 路径 | 聚合带宽 | L2-镜像集群（~90 节点 × 40G） |
|------|----------|------------------------------|
| L1→L2 | ~448 GiB/s | 集群带宽 ~450 GiB/s，接近满载 |
| L2→L3 | ~448 MiB/s | — |

90 个 L2 节点是从 200 个计算节点同时突发推导的最低配置。常态下（50-100 个计算节点突发创建）利用率 ~50%。

**RL Rollout**（30K 沙箱快照恢复 / 60s，快照路径）：

| 路径 | 计算 | 带宽 |
|------|------|------|
| 总 chunk 需求 | 30K × 256 chunks / 60s | 128K chunks/sec |
| L1→L2 | 35% miss × 512 KiB | ~22 GiB/s |
| L2→L3 | 0.1% miss | ~22 MiB/s |

L2-快照集群 20-30 节点 × 5 GiB/s = 100-150 GiB/s，22 GiB/s 利用率 ~15-22%。

---

## §9 设计决策总览

### §9.1 决策矩阵

| 决策 | 选择 | 备选 | 为什么 | 代价 |
|------|------|------|--------|------|
| 分块策略 | FastCDC 内容定义 | 固定大小 | 偏移场景去重 85% vs 0% | 写入 CPU、变长索引 |
| 去重粒度 | 块级 | 文件级 | 安全（Host 零文件感知）+ 简单（无需理解 FS） | 元数据稍大 |
| 加密方式 | 收敛加密 | 每租户独立密钥 | 密文层去重；已知明文仅暴露系统级公开内容，风险可接受 | 已知明文理论弱点 |
| L2 缓存冗余 | 4-of-5 纠删码 | 三副本 | 25% vs 200% 开销，且降尾延迟 | EC 编解码 CPU |
| L2 缓存填充 | 客户端旁路填充 | 缓存穿透填充 | L2 无需 L3 凭证，零明文暴露面；EC 编码 CPU 分散到数千 Agent | 客户端逻辑略复杂 |
| L2 存储 | 内存+SSD 冷热两层 | 纯内存 | 10x 容量、成本降 ~1/5 | 冷数据延迟略高 |
| 块设备协议 | vhost-user-blk | FUSE | 零内核穿越，I/O 路径延迟显著降低，对齐 <500µs 目标 | 实现复杂度 |
| 页面加载 | userfaultfd 按需 | 全量预加载 | 按需 vs 全量（仅 6.4%-40% 实际使用） | 运行时页面错误延迟 |
| GC 策略 | 分代 GC + salt | 引用计数 | O(1) 整代删除 vs 十亿级原子操作 | 迁移成本、多份存储 |
| 展平格式 | EROFS | EXT4 | 只读布局确定性强，利于去重；匹配不可变存储 | Guest 需额外可写层 |
| 写入覆盖 | CoW overlay | 直接修改 | 底层存储不可变，overlay 可独立捕获/恢复 | 稀疏文件管理 |
| Manifest 格式 | 二进制定长条目 | JSON/Protobuf | 0.018% 开销 vs ~1% | 调试可读性 |
| 压缩 | 不压缩 | zstd | 确定性密文 + 随机访问 vs 压缩侧信道 | — |
| 沙箱运行 | microVM 直载 | containerd + microVM | 消除容器运行时开销和非确定性；展平镜像直接作为 rootfs | 定制 init 维护 |
| VMM 选型 | Cloud Hypervisor | Firecracker | vhost-user-blk 与含该设备的快照恢复成熟，契合预热—快照—恢复全流程；外部缺页经定制补齐 | 跟踪上游、补丁维护 |
| 缓存层 | 自建分层缓存 | SFS Turbo（托管 NAS，OBS 加速） | 自建在密文 KV 角色上实现 EC 与抗扫描淘汰，控制延迟与成本 | 自建运维复杂度（可切换 SFS Turbo） |
| VM 内存 | 内存统一持有 | VMM 自建内存 | 冷启动与快照恢复同一套内存机制，快照直扫驻留页 | VMM 定制注入内存后端 |

### §9.2 核心设计平衡

系统中三个关键的设计平衡：

**安全 vs 去重**：收敛加密——从内容派生密钥。相同内容产生相同密文，可在密文层去重。存储层只见密文，安全不依赖去重边界。

**热点分散 vs 去重**：Salt 分代——代内去重，跨代分散。同一个 Alpine 基础层在不同代产生不同哈希、映射到不同缓存节点。去重在代内完整保留，热点在代间自然分散。

**按需加载 vs 低延迟**：三层缓存——99.9% 请求在 L1/L2 完成（<4ms），不到 0.1% 到达远端。按需加载不等于每次远端获取。

---

## §10 工程交付设计

§6 从技术职责出发分解了各逻辑组件的设计。本章从交付视角把这些逻辑组件落到具体工程模块——计算节点上的常驻进程、Guest 内的运行环境、数据写入侧的工具，以及共享的公共库。

§6 逻辑组件 → 工程模块对应关系：

| §6 逻辑组件 | 工程模块 |
|-------------|----------|
| §6.1 写入层（分块管线）、§6.2 读取层、§6.3 分块、§6.4 加密、§6.5 清单 | 公共库 + 清单服务客户端（manifest-ctl） |
| §6.1 写入层（镜像展平） | 容器镜像展平（flatten-ctl） |
| §6.6 内容寻址存储 | 存储代理（store-ctl） |
| §6.7 Salt 与分代 GC | GC 与代管理服务 |
| §6.8 分层缓存 | 缓存代理（cache-ctl） |
| §6.9 块设备代理、§6.10 快照代理 | 沙箱控制（sandbox-ctl）+ 公共库（虚拟磁盘后端库） |
| §6.11 VMM | VMM 定制（cloud-hypervisor） |
| §6.11 Guest 运行时 | Guest 控制（sandbox-init）+ Guest 内核（vmlinux） |
| §6.11 内存统一持有、§6.12 密度与资源控制（每沙箱环） | 沙箱控制（sandbox-ctl） |
| §6.12 密度与资源控制（节点环） | 节点资源控制器（node-ctl） |
| §5.1 系统定位（节点北向入口，e2b 兼容 ingress） | 沙箱编排（orchestrator-ctl，§10.13） |

### §10.1 部署形态

按节点角色部署，逻辑拓扑见 §5.2：

- **计算节点**（5000/AZ）：节点资源控制器、沙箱控制（每沙箱一进程）、缓存代理（L1 本地）、存储代理（边车）；每节点承载数千沙箱，每沙箱由 VMM 定制 + Guest 控制 + Guest 内核组成。
- **二级缓存集群**（AZ 级，约 100-200 节点）：缓存代理 shard 形态，纠删码 + 一致性哈希。
- **镜像展平数据面节点**（独立池）：容器镜像展平 + 清单服务客户端 + 存储代理。

区域级为对象存储与 GC 与代管理服务。数据通路：客户端（沙箱控制 / 清单服务客户端 / 容器镜像展平）→ 缓存代理 → 存储代理 → 对象存储；缓存全部未命中时经存储代理回退到对象存储。沙箱编排（sandbox-orchestrator / orchestrator-ctl，本方案内，§10.13）也部署在计算节点上、与沙箱控制同节点，对外提供 e2b 兼容 API 并驱动沙箱生命周期；与平台管理面的对接由 platform-agent（本方案外）承担。沙箱管理平台与容器镜像仓库为平台侧远端服务，本方案外。

### §10.2 沙箱控制（sandbox-ctl）

**定位**：计算节点上的沙箱全生命周期管理，一进程一沙箱——故障域、资源记账、生命周期天然对齐，不依赖常驻守护进程。承载块设备代理（§6.9）、快照代理（§6.10）、内存统一持有与每沙箱气球控制环（§6.11、§6.12）。

**功能要点**：

- 运行与快照命令；冷启动与快照恢复两种模式的配置校验
- 块设备代理：只读镜像盘 + 写时复制写层盘两个 vhost-user-blk 后端
- 快照代理：缺页按需填充、脏页管理、稀疏导出与按需恢复
- 内存统一持有：创建并持有可封印内存对象供 VMM 复用
- 每沙箱气球控制：按 Guest 上报推 inflate target 回收闲置内存
- 启动握手与心跳；与节点资源控制器的资源对话

**关键指标**：I/O 请求端到端 P50 <500µs；快照恢复端到端 P50 <80ms、P99 <200ms（详见 §8.2）

### §10.3 节点资源控制器（node-ctl）

**定位**：节点级 VM 资源密度管理（§6.12 节点环），每台计算节点一个守护进程，统一仲裁本机资源。

**功能要点**：

- 准入与并发管控（令牌桶限速 + 在制数上限 + 排队/超时回收）
- 内存额度分配（高/低/应急三档水位 + 限速 + 应急池）
- 主动回收（已稳定沙箱额度向实际用量收敛，留冷却窗口）
- 状态持久化于内存盘 + 扫描恢复
- 与沙箱的资源协议：准入、就绪、扩额、心跳、释放、重连

**关键指标**：单节点密度 >3000；内存实际上浮 <15%（详见 §8.1）

### §10.4 缓存代理（cache-ctl）

**定位**：数据访问加速（§6.8）。部署在需要数据读写的节点上，对上游屏蔽缓存层级；以 local/shard/tiered 三形态分别承担本地缓存、二级缓存集群成员、多级查找链。

**功能要点**：

- L1 本地磁盘缓存（抗扫描频率淘汰、命中回填）
- L2 二级缓存集群成员（纠删码分片 + 一致性哈希，对等无主）
- 查找链：逐层查找、命中回填、单层瞬时故障降级为未命中
- 远端回退经存储代理，缓存进程本身无持久化权限

**可替换**：L1/L2 可由 SFS Turbo（支持 OBS 访问加速的托管 NAS 服务）替代，由托管服务承担访问加速。

**关键指标**：L2 命中 >99.9%，读取尾延迟 P99.9 <4ms，L1 命中 >65%（详见 §8.2）

### §10.5 存储代理（store-ctl）

**定位**：内容寻址存储的数据面入口（§6.6）。边车进程，部署在需要访问对象存储的节点上，对上层屏蔽后端是本地文件还是对象存储。

**功能要点**：

- 流式收发数据块与清单，上传校验内容指纹、先临时后原子改名
- 按指纹读取的流式接口；附带客户端库
- 本地文件后端（开发/测试）与对象存储后端（生产，无状态）
- 按 GC 与代管理服务设定的当前活跃代落盘，并向写入侧提供活跃代盐

**关键指标**：写路径 >500 MB/s（详见 §8.2）

### §10.6 GC 与代管理服务

**定位**：存储生命周期的控制平面（§6.7），含服务端功能，与仅做数据面的存储代理分离。直接操作对象存储后端，与在线读写路径资源隔离。当前 generation 的 init/rollout/purge 控制操作由存储代理的 admin 子命令承载、活跃 Manifest 迁移与过期告警为基础实现；本节给出其作为独立控制平面的目标职责。

**实现要点**：

- 分代生命周期状态机：Active → Retired → Expired → Deleted（控制平面操作，频率月级）
- 活跃 Manifest 迁移：退役代中仍被引用的 Manifest，其数据块解密后用新代盐重加密、写入新代目录，走后台路径
- Salt 分配与旋转：维护活跃代、设定去重域边界与缓存热点分散
- 整代回收：整代目录删除，常数时间完成
- Expired 安全网：过期代被读触发告警并自动暂停删除
- 管理运维接口：容量查询、代管理、迁移触发

**关键指标**：整代删除 O(1)，迁移期间不影响在线读写路径

### §10.7 清单服务客户端（manifest-ctl）

**定位**：数据读写的入口工具（§6.1、§6.2、§6.5），调用公共库完成分块/加密/去重/组装，经存储代理入库、经缓存代理读取。

**功能要点**：

- 写入：切块 → 收敛加密 → 去重入库 → 组装并封装清单，产出清单键
- 读取：解析清单、按偏移定位、穿多级缓存取回、解密、按序写出
- 清单存取：清单本身按数据块方式存储，凭清单键取回
- 查看、校验、对比去重率

**关键指标**：读路径 P99.9 <4ms，写路径 >500 MB/s（详见 §8.2）

### §10.8 容器镜像展平（flatten-ctl）

**定位**：容器镜像到块设备镜像的确定性转换工具（§6.1 展平），在镜像构建或发布阶段运行，独立于运行时数据路径。确定性是块级去重的前提。

**功能要点**：

- 多层 overlay 按固定顺序合并为只读块设备镜像，处理删除标记、归零时间戳
- 镜像运行配置追加到镜像尾部
- 输入支持远程 registry 镜像（直接拉取，本地 OCI-layout 缓存：并发预取、内容寻址、LRU 上限回收）与 docker-archive；两条源逐字节一致
- 管道友好的输入处理；确定性校验（两遍展平比对）
- 展平后经本地清单服务客户端入库；可经 OCI Referrers 回写 manifest 标识、对已展平镜像幂等跳过

**关键指标**：相同输入 → 逐字节一致输出

### §10.9 Guest 控制（sandbox-init）

**定位**：Guest 内的一号进程（§6.11），单一静态二进制，全程仅用系统调用，不依赖任何外部程序。

**功能要点**：

- 早期挂载与根文件系统组装（只读镜像盘 + 读写写层盘叠加为新根）
- 启动握手取回启动配置、配置网络、在新命名空间拉起客户应用
- 进程监督（按重启策略处理退出、回收孤儿、信号转发）
- 快照前清理（同步磁盘、清空页缓存）、内存上报、快照恢复响应

**关键指标**：确定性配置下跨实例去重率 >90%（详见 §4.8）

### §10.10 Guest 内核（vmlinux）

**定位**：定制 Guest 内核（§6.11），从最小配置起步，只开沙箱必需的子系统与驱动。

**功能要点**：

- 最小化配置（虚拟设备、只读/可写/叠加文件系统、低节拍等）
- 确定性配置（关闭各类运行时随机化、固定版本号、收敛日志）
- 跨架构构建（两种处理器架构各自的启动协议）
- 平台能力边界（Guest 内关掉控制组与缺页机制，只留必要命名空间）

**关键指标**：镜像小、启动快、底噪低（计入单沙箱底噪 <60 MiB）

### §10.11 VMM 定制（cloud-hypervisor）

**定位**：VM 虚拟化层（§6.11），基于 Cloud Hypervisor 开源版本定制，是沙箱运行的集成点。

**功能要点**：

- 外部传入的内存后端（复用宿主持有的内存，不自建）
- 快照跳过外部托管内存区
- 外部缺页处理（在 VMM 进程内建缺页通道并把句柄交给宿主侧处理器）
- 内外通道代理与冷启动设备拓扑
- 不启用定制能力时与开源版本一致；补丁化维护以跟踪上游

**关键指标**：单 VM 进程开销约 10 MiB（详见 §8.1）

### §10.12 公共库

**定位**：被清单服务客户端、沙箱控制等复用的库（§6.1–§6.5、§6.9）。

**功能要点**：

- 分块（内容定义 / 固定大小）、收敛加密（含不加密对照模式，生产拒绝）
- 清单二进制格式（读写 + 密钥表封装 + 稀疏空洞编码）
- 写入管线、读取管线（顺序预取 / 按块对齐预取）
- 虚拟磁盘后端库（运行时由沙箱控制承载）
- 配置加载（显式指定，无自动查找）

### §10.13 沙箱编排（sandbox-orchestrator，本方案内）

**定位**：计算节点上的单实例编排 daemon，对外提供一套 **e2b 兼容 API**，把节点沙箱以 e2b 协议暴露给客户端（未改造的 e2b SDK 可直连本机）。一身兼 e2b 的 api + orchestrator + proxy 三角色：控制面 REST 管沙箱生命周期、驱动沙箱控制（§10.2）拉起/快照/销毁、调 vswitch 编排网络、把数据面流量反代到 guest（envd 数据面经 vsock，用户服务经 floatingip）。另经 **e2b 模板构建路径**（`flatten-ctl` 经 run-task）把客户镜像/快照入库为模板，e2b profile 复用注入 envd 的 `sandbox-runtime-e2b.erofs` 运行时制品。产物两个二进制：`orchestrator-ctl`（`serve` 启动服务）+ `e2b-key-ctl`（从 manifest_key 派生 api_key / 指纹）。完整设计见 `sandbox-orchestrator/docs/orchestrator.md`。

**两类沙箱（profile）**：**e2b**（guest 内跑原版 envd，支持 fs/process/pty/runCode 数据面）与 **bare**（无 envd，仅经 floatingip 网络提供服务——即基础沙箱接上北向 API）。

**功能要点**：

- e2b 兼容控制面（create/connect/timeout/pause/kill/list/metrics），鉴权 `X-API-KEY`
- 准入**不感知**：经 run-task（systemd 模板单元 `sandbox-runner@<sid>`）启动 `sandbox-ctl run` 即可，准入/配额在沙箱控制内部（§10.2/§10.3）
- per-沙箱生成 `SANDBOX_CONFIG`；共享 `MANIFEST_CONFIG`，客户密钥由 API key 派生经 `MANIFEST_KEY` 注入
- 经 vswitch `attach/detach` 编排网络，floatingip 记入状态
- guest 内原版 envd 经沙箱控制 `--connect`（vsock）反代；启动后调 envd `/init` 装载
- 状态 sqlite 持久化 + 多信号重启对账；TTL 回收

**与平台管理面的衔接（platform-agent，本方案外/未来）**：多节点平台编排由 `platform-agent`（本方案外/未来）承担，作为平台管理面与 orchestrator 之间的桥接，与 e2b SDK 并列为 orchestrator 的北向客户。本方案不实现 platform-agent，仅约定其经 orchestrator 北向接口驱动沙箱生命周期。

### §10.14 交付节奏

各模块按依赖关系排序交付，运行时通过接口协作：

```
存储代理（对象存储后端 + 分代落盘）
└─ GC 与代管理服务（分代生命周期、迁移、salt 旋转）
公共库（分块 / 加密 / 清单 / 写入 / 读取 / 虚拟磁盘后端 / 配置）
├─ 清单服务客户端（写入 + 读取）
└─ 容器镜像展平
缓存代理（L1 本地 → L2 集群 → 查找链与回填）
VMM 定制 → Guest 内核 → Guest 控制 → 沙箱控制（块设备 + 快照 + 内存持有 + 气球）
节点资源控制器（准入 / 额度 / 回收 / 持久化）
沙箱编排（e2b 兼容控制面 + envd-in-guest 反代 + 模板构建；经 run-task 驱动沙箱控制 / 容器镜像展平）
```

关键路径：存储代理 → 公共库与清单服务客户端 → 缓存代理 → 沙箱控制（块设备/快照代理）。

---

## 修订历史

| 版本 | 日期 | 修改人 | 说明 |
|------|------|--------|------|
| v1.0 | 2026-03-28 | chenxiaohui | 初稿 |
| v1.1 | 2026-03-31 | chenxiaohui | 补充缓存填充路径、块设备快照能力、展平格式选型 |
| v1.2 | 2026-04-03 | chenxiaohui | 补充 VMM 选型、高密度沙箱、密度控制器、Guest 环境设计 |
| v1.3 | 2026-05-20 | chenxiaohui | 对齐系统架构与工程模块划分；性能改为端到端口径；§5/§10 按进程模块重构，逻辑组件与工程模块分离表述 |
| v1.4 | 2026-06-04 | chenxiaohui | 新增节点沙箱编排（sandbox-orchestrator / orchestrator-ctl + e2b-key-ctl）：§5 系统定位/总体架构补 e2b 兼容 ingress；§10.13 沙箱编排（含 e2b 模板构建路径 + sandbox-runtime-e2b.erofs）；§10 逻辑→工程映射表与 §10.14 交付节奏补沙箱编排 |
