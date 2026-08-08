# node-resource — 节点资源控制协议与控制器

节点级**资源控制器**跟节点上所有动态控制模式 sandbox-ctl 通过沙箱资源控制
协议对话,完成跨沙箱仲裁、burst 申请、settled 收回、admission 控制。它由
`node-ctl conductor serve` 经 `resource_listen` 内置(node.md §3 / §10),配置内联在
conductor.yaml 里,无独立 daemon 入口、无单独配置文件;本文档统称其为"控制器"。

控制器是协议的**对端角色**,不是某个具体进程:任何遵循 §5 定义的实现都可
作为 sandbox-ctl 的对端,`node-ctl` 的内置控制器是参考实现。本文档同时定义
协议规范与控制器的内部组织。

## 1. 概述

### 1.1 业务问题

平台目标是单节点高密度承载 microVM 沙箱。Agent 应用的运行剖面有显著间歇性:

- **稳态**:绝大多数时间占用极少资源(典型 0.1 核 / 128 MiB),用于心跳、
  长连接保活、轻量后台任务
- **突发**:处理用户请求时短时拉到声明上限(2 核 / 8 GiB),持续秒到分钟级
- **回归**:请求处理完后回到稳态

矛盾的根源是:**capacity 之和远大于物理内存(超分),但 burst 窗口期需要
兑现 capacity 承诺**。静态分配解决不了这个问题——必须有动态调度,根据节点
资源预算把资源按需分给突发中的沙箱、从空闲沙箱回收。

控制器解决三个矛盾:
1. **超分必要性**:让 allocatable 贴近真实工作集,密度从 `physical_mem /
   capacity` 提升到 `physical_mem / typical_floor`(典型提升 60×)
2. **突发不能丢状态**:burst 中沙箱被 OOM kill = 业务感知失败。控制器仲裁
   保证有预算的沙箱可以拉到 capacity
3. **节点不能炸**:多沙箱同时 burst 时 host 级 OOM 是灾难。控制器水位机制
   保证 `node_allocated ≤ allocatable_pool`

### 1.2 设计原则

1. **优先确定性闭环**:首选内核态自动机制(cgroup memory.high PSI、cpu.max
   throttling、cpu.weight 比例公平、deflate_on_oom),用户态控制环路只补内核
   机制不到位的部分
2. **CPU 静态、内存动态**:CPU 通过 `cpu.max + cpu.weight` 静态实现,不引入
   CPU 维度的动态控制环;只有内存有 burst/recover 状态机
3. **per-sandbox-ctl 进程模型不破**:sandbox-ctl 仍是 runc-style 一次启动
   一个沙箱;控制器是它的协调对端,不是监督者
4. **状态全部在 /run**:控制器状态进 tmpfs,host 下电即清空——下电后所有
   沙箱都不存在,无任何有效信息需要跨重启留存
5. **故障域边界清晰**:控制器状态意外丢失后能从 cgroup 与 sandbox-ctl 重建;
   sandbox-ctl 崩溃自动通知控制器释放预留;两者无需强一致
6. **事件驱动而非定时**:settled 等关键状态转换以 launch protocol 消息为
   触发,不依赖定时器估计

### 1.3 故障域

| 故障 | 直接影响 | 规避 / 自愈 |
|------|---------|-------------|
| controller 崩溃 | sandbox-ctl 长连断开;新沙箱 admit 失败 | systemd 重启;sandbox-ctl 退化无控制器继续跑 |
| controller 状态文件损坏 | 重启时无法恢复 reservation 记账 | 从空状态起,预算按 yaml 重算,等 sandbox-ctl 重连(§8.2) |
| sandbox-ctl 崩溃 | 沙箱按既有规则销毁 | controller 检测连接断 → 保留 reservation 等 Reattach,心跳静默 90 s 后释放 |
| controller ↔ sandbox-ctl 网络抖动 | RPC 超时 | 退避重连;期间 sandbox-ctl 用最近一次 grant 继续跑 |
| 单沙箱 OOM | guest 内进程被 kill;deflate_on_oom 释放 balloon | 非平台级故障;controller 计 OOM 事件 |
| host 物理 OOM | 内核 OOM killer 选目标 | 水位机制保证 node_allocated 始终 ≤ 物理可用,正常情况不应触发 |

## 2. 命令行接口

控制器由 `node-ctl conductor serve` 内置(配 `resource_listen`,node.md §3 / §10);只读巡检
与运维动词在 `node-ctl resource` 子命令组下。

### 2.1 子命令总览

| 子命令 | 用途 |
|---|---|
| `serve`(配 `resource_listen`) | 在该 UDS 起控制器:RPC server + admission + allocator + reclaimer + persister(node.md §2.2) |
| `resource status` | 节点预算快照(只读) |
| `resource list` | 列举当前 reservation(只读) |
| `resource drain` | 进入排空状态(运维) |
| `resource grant` | 强制下发 budget(调试) |
| `resource reclaim` | 强制收回(运维) |

### 2.2 控制器启动(`node-ctl conductor serve` 内置)

控制器随 `node-ctl conductor serve` 起:配 `resource_listen`(§3)即在该 UDS 起 RPC server、
admission worker、memory allocator、active reclaimer、idle sweeper 与 state
persister(§7)。`resource_listen` 的子字段(`socket` / `state_path` /
`cgroup_scan_paths` / `resources` / `watermarks` / …)是内联在 conductor.yaml 里的控制器调参(§3)。
集群下,控制器上报的节点水位(zone / allocated / pool)经 serve 的 node-link 心跳喂
集群 P2C 放置(node.md §10、cluster.md / cluster-placer.md)。

### 2.3 `node-ctl resource status`

```
node-ctl resource status [--state /run/node-ctl/state.json]
```

读 state 文件,打印水位区、节点预算、host 预留、运维容差、已分配量、利用率与
reservation 数。只读,可与运行中的控制器共存。

### 2.4 `node-ctl resource list`

```
node-ctl resource list [--state /run/node-ctl/state.json]
```

把 reservation 表导出为 JSON。只读。

### 2.5 `node-ctl resource drain`

```
node-ctl resource drain [--socket /run/sandbox-resource.sock] [--disable]
```

通知控制器不再接受新沙箱准入;`--disable` 取消排空。运维命令——典型用于
节点维护前清空沙箱。**集群下节点排空仍是节点侧动作**(本命令 / 本机深空闲自提升上送远程快照),
集群侧仅停止向其分配——**不下发 drain 命令**(cluster.md、node.md §10)。

### 2.6 `node-ctl resource grant`

```
node-ctl resource grant <sid> --memory <size> [--socket /run/sandbox-resource.sock]
```

绕过水位与限速,直接给某沙箱发放内存 budget(上限 clamp 到 capacity),用于排查
或紧急调度;sandbox-ctl 经下次 Heartbeat 取得新值落地。

### 2.7 `node-ctl resource reclaim`

```
node-ctl resource reclaim <sid> --memory <target> [--socket /run/sandbox-resource.sock]
```

直接将某沙箱 allocatable 收缩到目标值(shrink-only,clamp 到 floor;目标高于
当前值时报错),用于人工干预异常场景;同样经 Heartbeat 传播。

## 3. 配置

### 3.1 resource_listen 配置块

控制器配置是 `node-ctl conductor serve` 配置(node.md §3)的 `resource_listen` 块,内联在
conductor.yaml 里(无独立配置文件);`enabled: true` 即在其 `socket` 起控制器。字段
(yaml 形态,均挂在 `resource_listen:` 下):

```yaml
enabled: true
socket: /run/sandbox-resource.sock     # "" = pkg/resource 默认(与 sandbox-ctl 一致)
state_path: /run/node-ctl/state.json   # tmpfs
audit_path: /run/node-ctl/audit.log    # tmpfs;高频审计不写磁盘
cgroup_scan_paths:                      # (预留)重启对账扫描根
  - /sys/fs/cgroup/sandboxes

resources:
  physical_memory: auto                # auto = 读 /proc/meminfo MemTotal
  physical_cpu: auto                   # auto = 读 nproc
  host_reserved:
    memory: 16GiB
    cpu: 1.5

watermarks:
  operational_margin_factor: 0.10
  high_factor: 0.85
  low_factor: 0.70
  emergency_factor: 0.05
  startup_factor: 0.50                 # startup_pool = allocatable_pool × 此值

rate_limits:
  memory_grant_per_sec_factor: 0.05    # × allocatable_pool_mem

admission:
  rate: 4                              # /s — admission token bucket 填充
  burst: 16                            # admission token bucket 容量
  startup_ttl: 30s                     # admit → settled 上限
  queue_ttl: 30s                       # 短期阻塞排队上限
  queue_max_depth: 256                 # 队列容量,超即立 reject queue_full

dampening:                              # 振荡阻尼,不进 sandbox.yaml
  recover_duration: 60s                # burst → settled 观察期
  cooldown_periods: 10                 # × 100ms,burst → recover 判定门槛
```

### 3.2 字段语义

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 置 `true` 才在 serve 内起控制器;否则沙箱用静态 cgroup |
| `socket` | `pkg/resource` 默认 | UDS,sandbox-ctl 拨号目标;`""` = 协议默认(与 sandbox-ctl 一致) |
| `state_path` | `/run/node-ctl/state.json` | tmpfs,控制器重启快速恢复用 |
| `audit_path` | `/run/node-ctl/audit.log` | tmpfs;高频审计不写磁盘 |
| `cgroup_scan_paths` | `[/sys/fs/cgroup/sandboxes]` | (预留)重启对账扫描根 |
| `resources.physical_memory` | `auto` | 节点物理内存(`/proc/meminfo`)|
| `resources.physical_cpu` | `auto` | 节点物理核数(`nproc`) |
| `resources.host_reserved.memory` | `16GiB` | host 自身预留(kernel + cache-ctl + store-ctl + monitoring),按节点实测覆盖(§10.2) |
| `resources.host_reserved.cpu` | `1.5` | 同上,CPU 维度 |
| `watermarks.operational_margin_factor` | 0.10 | 节点预算的 10%,容差,不参与分配 |
| `watermarks.high_factor` | 0.85 | red 区起点 |
| `watermarks.low_factor` | 0.70 | yellow 区起点 |
| `watermarks.emergency_factor` | 0.05 | 紧急池,仅 urgency=high 申请可触 |
| `watermarks.startup_factor` | 0.50 | startup_pool = allocatable_pool × 此值;admission 阶段累计 effective_startup_budget 的上界,为 steady-state grant 留余量(约束:emergency_factor < startup_factor ≤ 1.0) |
| `rate_limits.memory_grant_per_sec_factor` | 0.05 | 内存仲裁限速:每秒总扩展量 ≤ allocatable_pool × 此值 |
| `admission.rate` | 4 | Admit 速率,token bucket 装填速率(/s) |
| `admission.burst` | 16 | Admit token bucket 容量 |
| `admission.startup_ttl` | 30s | sandbox-ctl 必须在此时间内进入 settled,否则 IdleSweeper 释放 reservation |
| `admission.queue_ttl` | 30s | 短期阻塞排队最长等待,超时 → reject `queue_canceled` |
| `admission.queue_max_depth` | 256 | 队列容量,超即立 reject `queue_full` |
| `dampening.recover_duration` | 60s | burst → settled 观察期 |
| `dampening.cooldown_periods` | 10 | × 100ms,burst → recover 判定门槛 |

## 4. 资源预算与水位

### 4.1 预算结构

```
physical_total            = 节点物理内存(或 CPU 总核数)
host_reserved             = kernel + controller + cache-ctl + store-ctl + monitoring
                            内存典型 5-10%,CPU 典型 5%
node_budget               = physical_total − host_reserved
operational_margin        = node_budget × 10%       # 容差,不参与分配
allocatable_pool          = node_budget − operational_margin
```

`operational_margin = 10%` 是固定容差,留给:

- 控制器重启时的状态重建延迟(短暂超额)
- cgroup 计数误差 + kernel 内部分配波动
- host 进程偶发内存峰值

operational_margin 即使在 critical 区也不被触动——节点的"绝对底线"。

### 4.2 水位线

```
high_watermark   = allocatable_pool × 85%
low_watermark    = allocatable_pool × 70%
emergency_pool   = allocatable_pool × 5%
startup_pool     = allocatable_pool × 50%   # admission 阶段累计上界
```

`emergency_pool` 仅响应 urgency=high(oom 类紧急申请),正常仲裁动用前 95%
预算。

`startup_pool` 是 admission 单独跟踪的 sub-budget:所有 pre-settled 阶段
reservation 的 `effective_startup_budget` 累计不能超过 startup_pool。创建并发
以"内存硬限"而非"个数硬限"表达——同样的 startup_pool 容下若干个小 sandbox 或
单个大 sandbox,自然 throttle。

水位区间行为:

| 区间 | node_allocated 位置 | Admit | settled 收缩 margin | burst 申请 |
|------|---------------------|-------|---------------------|-----------|
| **green** | < low_watermark | 接受 | ×1.25 | 限速批 |
| **yellow** | [low, high) | 接受 | ×1.10 | 限速批 |
| **red** | [high, pool − emergency_pool) | **拒绝**(`zone_critical`) | ×1.05 | 仅 urgency=high |
| **critical** | ≥ pool − emergency_pool | 拒绝 | ×1.00 | 仅 urgency=high |

settled 收缩由 Active Reclaimer 执行:每 10 s 扫一遍 settled reservation,把
allocatable_now 收向 `max(floor, last_reported_rss × margin)`,margin 随水位区
收紧;新值经 Heartbeat ack 的 `new_allocatable` 传到 sandbox-ctl 落地(§5.2)。

注: zone red/critical 是系统保护性 reject——即使其他短期资源可缓解
(token bucket / startup_pool),进入 red/critical 仍然立即拒绝,给系统
recovery 留空间。

CPU 不参与水位决策:admission 与运行期仲裁均仅内存维度;控制器对 CPU 只做
记账(reservation 按 floor.cpu 累计,`status` 可见)——CPU 不动态分配。

### 4.3 仲裁策略

`RequestBudget` 同步处理(仅内存维度),没有服务端 grant 队列——未获批的请求
拿到 `cooldown_ms` 退避重试,期间沙箱在 cgroup memory.high PSI 反压下等待:

- **urgency**:high(oom)绕过 token bucket 限速,且可动用 emergency_pool;
  normal/low 在 red/critical 区直接得 0 + cooldown
- **限速**:token bucket,每秒总扩展量 ≤ allocatable_pool ×
  `memory_grant_per_sec_factor`;token 不足时按补给 ETA 回 cooldown_ms
  (下限 50 ms)
- **步长**:单次 grant clamp 到 [4 MiB, 512 MiB],且 ≤ capacity 余量与
  zone headroom
- CPU:无运行时仲裁

限速是防惊群核心:即使 100 个沙箱同时 burst,每秒放出的总量有界,未获批的
沙箱继续在 PSI 反压下退避重试,而不是同时全 grant 撞墙。

## 5. 沙箱资源控制协议

### 5.1 协议形态

UDS,长度前缀(4 字节 LE uint32)+ JSON 消息——简单、调试友好、无外部依赖。

每个 sandbox-ctl 启动时 `Admit` 后建立长连,直到沙箱退出。连接生命周期与
沙箱生命周期对齐。wire 格式与 `Client` 的唯一定义点是
`sandboxer/pkg/resource`,client/server 共用。

### 5.2 消息类型

请求/响应对(sandbox-ctl 发,控制器答):

```
Admit            (sandbox_id, capacity, floor, startup_budget_memory,
                  allocatable_at_snapshot?, cgroup_path)
                 → AdmitResponse (status, token, granted_initial_alloc,
                                  reason?, queued_for_ms, queue_pos_at_in)
                   status ∈ {admitted, rejected}
                   (queued 保留不用:短期阻塞时 server 持连不回应,§5.3)
                   granted_initial_alloc = max(startup_budget_memory, floor,
                                               allocatable_at_snapshot)
                   reason:rejected 时分类(§6.4)
                   queued_for_ms / queue_pos_at_in:命中队列时回填的诊断元数据
                 # sandbox_id 用作 admin 动词(grant/reclaim)的索引键;
                 # cgroup_path 存入 reservation 并随 state.json 持久化

Settled          (token, current_rss, current_cpu_usec)        # 进入 settled 通知
                 → Ack
                 # 触发时机:冷启动收到 launch hello 之后;
                 #            恢复收到 guest 的 restore_ack 之后

RequestBudget    (token, current_alloc, requested_delta, urgency, reason)
                 → BudgetResponse (granted_delta, new_allocatable, cooldown_ms)

OOMReport        (token, oom_count, killed_pid, killed_rss)    # 紧急上报
                 → Ack

Heartbeat        (token, current_rss, current_cpu_usec, recent_high_count,
                  cpu_throttled_periods)                       # 30s 周期
                 → Ack (new_allocatable)
                   # Ack 回带权威 new_allocatable:active reclaimer 或 admin
                   # 动词改过 allocatable_now 时,sandbox-ctl 在此取新值并落地
                   # (cgroup memory.high + balloon resize)——reclaim/admin 的
                   # 传播通道

Release          (token, reason)                                # reason ∈ {normal, error, ...}
                 → Ack

Reattach         (token)                                        # 断线重连后重新绑定(§5.3)
                 → Ack (new_allocatable)                        # 成功:回带当前 allocatable_now
                 → Error "unknown token"                        # token 不存在
```

admin 动词(运维 CLI 发,控制器答;以 sandbox_id 而非 token 定位目标):

```
AdminStatus      ()                                             # node-ctl 经 RPC 查实时态
                 → Ack (zone, node_allocated, allocatable_pool,
                        reservation_count, drained)
                   # state.json 不可直接访问时(如跨主机巡检)用

AdminDrain       (drain)                                        # 背后 node-ctl resource drain
                 → Ack (drained)

AdminGrant       (sandbox_id, requested_delta)                  # 背后 node-ctl resource grant
                 → Ack (granted_delta, new_allocatable)         # 绕过水位/限速

AdminReclaim     (sandbox_id, target_allocatable)               # 背后 node-ctl resource reclaim
                 → Ack (new_allocatable)                        # shrink-only,clamp 到 floor
```

`token`:Admit 时由控制器生成的随机字符串,作为后续所有 RPC 的认证 + 索引。
重连时 sandbox-ctl 重发 token,控制器验证后绑定到现有 reservation。

`reason` 字段是字符串枚举,用于审计与调试。

**注意 RequestBudget 没有 dim 字段**——CPU 不参与运行时仲裁。

### 5.3 连接生命周期

**首次建连**:

1. sandbox-ctl 拨 UDS,发 `Admit`
2. 控制器评估请求:
   - 通过 → 立即回 `status=admitted`
   - 长期失败(drain / zone red/critical / 超 pool / 超 startup_pool)→ `status=rejected`
   - 短期阻塞(token / main_headroom / startup_headroom)→ **不回应**,
     conn 入服务端 FIFO queue;worker 在条件满足时回 `admitted`,
     queue_ttl 超时回 `rejected`
3. status=admitted:sandbox-ctl 持有 token,继续启动流程
4. status=rejected:sandbox-ctl 退出非零(上层调度决定 retry / 换节点)

**长连维持**:

- 沙箱进入 startup 后,连接保持活跃;sandbox-ctl 在此连接上发后续 RPC,并经
  Heartbeat ack 的 `new_allocatable` 接收 reclaim/admin 的 allocatable 调整
- 30s 周期 Heartbeat;控制器 90s(3 个周期)未收到 → 视为掉线,触发故障
  处理(详见 §故障域处理)

**重连**:

- 连接断开,sandbox-ctl 退避重试(1s, 2s, 5s, 10s, 10s, ...)
- 重连成功后发 `Reattach(token)`,控制器验证 token 找到 reservation 后回
  `Ack(new_allocatable)`,重新绑定连接;token 不存在则回 `Error "unknown token"`
- 重连期间 sandbox-ctl 用最近一次 grant 状态继续运行(cgroup 不变、balloon
  不变),不主动调整资源

**断连降级**:

- 持续 60s 重连失败,sandbox-ctl 切到无控制器模式继续——保持当前
  allocatable 不变,接管 cgroup memory.high 设置,不再申请 burst
- 此时即使有新压力,只能靠 cgroup PSI 反压 + deflate_on_oom 兜底
- 重连成功后自动恢复联动

**沙箱退出**:

- sandbox-ctl 退出前发 `Release`;控制器收到后释放 reservation
- sandbox-ctl 异常退出(无 Release):控制器检测连接 EOF/RST → 视沙箱可能
  死亡;通过 cgroup 路径检查交叉验证

## 6. 创建期管控

### 6.1 admission 流程

`sandbox-ctl run` 启动后第一动作不是 cgroup join,而是判断是否需要连控制器:

```
T0  sandbox-ctl run --config xx.yaml
T1  parse config, compute (capacity, floor, startup.memory)
    若 control.controller 为空 → 跳过 admit,跳到 T5
T2  dial controller, send Admit
T3  block read AdmitResponse(可能立即,也可能在 queue 中等待)
T4  if rejected: exit 2(上层调度决定 retry / 换节点)
T4' if admitted: continue(可能伴随 queued_for_ms > 0,仅作 log 用)
T5  cgroup join(静态 cgroup / 动态控制模式写限制;无 cgroup 模式跳过)
T6  ... 启动序列继续(详见 `sandboxer/docs/sandbox.md` §冷启动数据流)
```

sandbox.yaml `resources.startup.memory` 是 sandbox 期望的 startup 阶段
budget;默认 = allocatable.memory。admission 实际给出 `granted_initial_alloc
= max(startup.memory, allocatable.memory, allocatable_at_snapshot)`,即
三者最大,无降级。

### 6.2 决策矩阵

每个 Admit 在 server 内部归一化:

```
effective_startup_budget = max(
    startup_budget_memory,      # 来自 sandbox.yaml resources.startup.memory
    floor_memory_bytes,         # 来自 sandbox.yaml resources.allocatable.memory
    allocatable_at_snapshot     # 仅 restore 路径,cold = 0
)
```

冷启动与恢复**走完全相同的决策路径**,差异由公式吸收。

预检(立即 reject,不入队):

| 条件 | reject 原因 |
|---|---|
| `effective_startup_budget == 0` | `invalid_burst` |
| `effective_startup_budget > allocatable_pool` | `exceeds_node_capacity`(永不可能) |
| `effective_startup_budget > startup_pool` | `exceeds_startup_pool`(永不进 startup_pool) |

主决策:

| 失败原因 | 决策 | 类别 | 唤醒源 |
|---|---|---|---|
| drained | reject | long | — |
| zone Red/Critical | reject | sys-protect | — |
| `> main_headroom`(可装 pool 但暂缺) | **queue** | short | Settled / Release / Reclaim |
| `> startup_headroom`(可装 startup_pool 但暂缺) | **queue** | short | Settled / pre-settled Release |
| token bucket 空 | **queue** | short | one-shot token-refill timer |
| `queue.depth ≥ queue_max_depth` | reject `queue_full` | overload | — |
| 等待时间 > `queue_ttl` | reject `queue_ttl` | timeout | per-entry TTL timer |
| 默认(全部通过) | admitted | — | — |

并发不再由 hardcoded `max_concurrent_creating` 限,而是由 `startup_pool`
budget 自然 throttle:每个 sandbox admission 占 `effective_startup_budget`
在 startup_pool 内,Settled 时立即归还。

### 6.3 队列协调机制

服务端 FIFO 队列 + 持有连接 + 事件驱动 worker(单 goroutine)。

**worker 主循环**只 select `wakeCh + shutdownCh`,无周期 timer:

| Wake 来源 | 触发时机 |
|---|---|
| 入队 | server.handleAdmit 决策为 short-term block 时 |
| Settled | sandbox-ctl 发 Settled message(main pool 收 burst→max(rss,floor);startup pool 收 effective_startup_budget→0)|
| Release | sandbox-ctl 发 Release(pre-settled 时 startup pool 同步释放)|
| Reclaim | 强制收回完成 |
| **per-entry TTL** | 入队时 `time.AfterFunc(queue_ttl, pushWake)` |
| **per-entry conn EOF** | 入队时启 reader goroutine,client 断连时 `pushWake` |
| **one-shot token refill** | head 阻于 token bucket 时,worker 末尾 `AfterFunc(eta, pushWake)` |

worker 处理:严格 FIFO。head 不通过即停,直到 wake 重试。worker 入口先取
`queueMu`,再取 `State.Lock`(锁顺序固定避免死锁)。

### 6.4 reject reason 分类

| reason | 含义 | sandbox-ctl 行为 |
|---|---|---|
| `drained` | 控制器进入 drain 模式 | exit 非零;上层调度知节点不再接客 |
| `zone_critical` | main pool 在 red/critical 区 | exit;上层换节点或等其他 sandbox release |
| `invalid_burst` | 请求 effective_burst 计算为 0 | 配置错(yaml 全空)|
| `exceeds_node_capacity` | effective_startup_budget > allocatable_pool | 配置错(永远塞不进这个节点)|
| `exceeds_startup_pool` | effective_burst > startup_pool 上限 | startup_factor 配过小 / sandbox burst 配过大 |
| `queue_full` | queue.depth ≥ queue_max_depth | 节点 admission 压力极大,上层换节点 |
| `queue_ttl` | 等待时间 > queue_ttl | 短期资源紧但 30s 内未缓解,上层调度决定 |
| `queue_canceled` | 客户端断连或自身 TTL 触发 | 通常 client-side 已退出 |
| `daemon_shutting_down` | 控制器停机时排空在队 admit | 上层换节点 |

### 6.5 reservation 生命周期与 token TTL

```
Admit (admitted)
 │  reservation 占用 effective_startup_budget,token 生成
 │
 ▼
sandbox-ctl 启动 CH(cgroup join → memfd → ...)
 │  若 sandbox-ctl 在 startup_ttl(默认 30s)内不发 Settled
 │   → 控制器视为创建失败,自动 release reservation
 ▼
launch hello(冷启动)/ restore_ack(恢复)
 │
 ▼
sandbox-ctl 发 Settled(token, current_rss, current_cpu_usec)
 │  reservation 进入 settled;allocatable_now 从 effective_startup_budget
 │  收缩到 max(current_rss, floor),startup_pool 同步归还该 budget
 │
 ▼
... 后续 RequestBudget / Heartbeat / OOMReport ...
 │
 ▼
sandbox-ctl 发 Release 或连接断开
 │  控制器释放 reservation
 ▼
end
```

**TTL 设计依据**:

- `startup_ttl = 30s`:覆盖典型冷启动(大镜像 + 慢网络下 manifest 恢复)。
  超时即视为创建失败,释放 reservation
- `queue_ttl = 30s`:Admit 排队太久无意义,上层调度器宁可换节点

## 7. node-ctl 内部组织

`node-ctl` 的内置控制器是协议的参考实现,随 `node-ctl conductor serve` 起(配 `resource_listen`)。
它内部组织成四个角色,共享 in-memory state 与 /run 持久化:

```
controller (node-ctl conductor serve resource_listen)
├── RPC server               处理协议消息
├── Admission Controller     §6 准入与速率限制
├── Memory Allocator         §4.3 仲裁与 grant
├── Reclaim Scheduler        §4.2 settled 期主动收回
└── State Persister          §8 /run/node-ctl/state.json 写
```

这些角色在协议规范中是隐式的——其他实现可以选择不同的内部组织。本文档其余
部分(§可靠性、§admission)以该控制器行为为准描述。

## 8. 可靠性

### 8.1 状态全部在 /run

控制器状态分两类,**全部 tmpfs,无任何磁盘持久化**:

**进程内**:

- 当前 RPC 连接表
- 速率限制 token bucket 状态
- 短期统计(1 分钟窗口的 grant 次数等)

**`/run/node-ctl/state.json`**:

```
{
  "version": 1,
  "node_budget":         { "memory_bytes": ..., "cpu_milli": ... },
  "host_reserved":       { "memory_bytes": ..., "cpu_milli": ... },
  "operational_margin":  { "memory_bytes": ..., "cpu_milli": ... },
  "watermarks":          { "high_factor": 0.85, "low_factor": 0.70, "emergency_factor": 0.05 },
  "reservations": [
    {
      "token":              "...",
      "sandbox_id":         "...",
      "sandbox_ctl_pid":    12345,
      "cgroup_path":        "/sys/fs/cgroup/sandboxes/sb-001",
      "capacity":           { "memory_bytes": ..., "cpu_milli": ... },
      "floor":              { "memory_bytes": ..., "cpu_milli": ... },
      "allocatable_now_mem":  ...,    # 仅内存有运行时值
      "stage":              "settled",
      "stage_entered_at":   "2026-..-..T..",
      "last_heartbeat_at":  "2026-..-..T..",
      "oom_count":          0
    },
    ...
  ]
}
```

写入策略:

- **关键事件即时写**:Admit grant、Release、Reclaim 完成等改变 reservations
  数组的事件
- **状态变化批量写**:Heartbeat 更新 last_heartbeat_at 等高频但非关键的更新
  按 5 秒批量 flush
- **原子写**:`write to state.json.tmp + fsync + rename` 防截断

`/run` 是 tmpfs,host 重启即丢——这正是想要的:host 重启时所有沙箱的 CH
进程都已死,没有任何活沙箱需要恢复 reservation。

### 8.2 控制器进程重启恢复

控制器进程重启(systemd 自动 restart 或运维主动)但 host 未重启时:

1. **读 /run/node-ctl/state.json**:成功则得到上次写入时的 reservations 快照;
   失败或不存在则 reservations = []
2. **扫描已知父 cgroup**:控制器 yaml 配置一个或多个 `cgroup_scan_paths`
   (典型 `/sys/fs/cgroup/sandboxes/`),遍历得到当前 host 上活的沙箱 cgroup
   列表
3. **交叉对账**:对每个 cgroup:
   - 在 state.json 找到对应 reservation:状态有效,标记待重连
   - 不在 state.json:**孤儿 cgroup**——通过 cgroup.procs 找 sandbox-ctl pid,
     通过 `/proc/<pid>/cmdline` 验证是 sandbox-ctl,通过启动参数读到
     sandbox.yaml 路径取得 sandbox sid + capacity,**临时收纳**为新
     reservation,等其重连
   - state.json 有但 cgroup 不存在:沙箱已死,丢弃 reservation
4. **等待重连**:已知的活 reservation 暂无连接(`Conn=nil`),收到
   sandbox-ctl 的 `Reattach(token)` 后回 `Ack(new_allocatable)` 重新绑定;
   超过心跳过期阈值仍无重连的由 IdleSweeper 视为死亡丢弃
5. **服务恢复**:期间控制器接受新 Admit 申请,但水位计算包含已知活沙箱的
   reservation(可能短暂超估,优先保守)

**关键不变量**:**cgroup 是真相之源,state.json 是性能优化**。state.json
损坏不导致功能失败,只是恢复期 + 30 s 等所有沙箱重连。

### 8.3 故障域处理

**sandbox-ctl 崩溃**:

- 控制器通过 RPC 连接 EOF 检测
- 立即将 reservation 标记 `pending_release`,5 s 内交叉检查 cgroup:
  - cgroup 还在 → 沙箱可能仍在跑(sandbox-ctl 异常但 CH 还活):pending,
    最长等 60 s,期间不接受该 sid 重连(必须新 Admit)
  - cgroup 已消失 → 真死,释放 reservation

**sandbox-ctl 网络断 + 自身仍活**:

- 同上,但 60 s 后 cgroup 仍在
- 此时 sandbox-ctl 已经走降级流程,继续无控制器运行
- 控制器把 reservation 转 `presumed_no_controller`:仍计入 node_allocated,
  但不再接受其 RPC(token 失效,需要 sandbox-ctl 主动重 Admit)

**控制器自身崩溃**:

- systemd 重启
- 重启过程中所有 sandbox-ctl 退化无控制器
- 控制器起来后走重建流程,sandbox-ctl 重连后恢复联动

**全节点 OOM(host 级)**:

- 不应发生(水位机制保证)。若发生,kernel OOM killer 选目标。controller 与
  cache-ctl 等 host 进程优先级低于 sandbox(由 systemd OOM score 设置),
  先被杀
- 控制器重启后走重建流程,期间沙箱继续运行(降级)

**host 重启**:

- 所有沙箱与控制器同时消失
- /run/ 清空,无任何状态留存
- 重新启动后从空状态开始,等上层调度器重新发沙箱

## 9. 工作负载模型与基线

### 9.1 agent-intermittent 模型

agent 应用的运行剖面输入规范(实测在
`platform/docs/perf.md`):

```
节点参数:
  N_sandboxes               沙箱总数(扫描变量,32 / 64 / 128 / 256 / 512)
  capacity_each             (memory=8 GiB, cpu=2)
  floor_each                (memory=128 MiB, cpu=0.1)
  startup_budget_memory     1 GiB

每沙箱状态机:
  state ∈ {idle, active}
  转换:
    idle → active:    指数到达,平均间隔 1/λ_a 秒
    active → idle:    持续时间 D ~ Pareto(α=1.5, x_min=5)秒

每沙箱内存行为:
  idle:    RSS_target = 128 MiB,cpu 使用 ≈ 0.05 核
  active:  RSS 在 G ~ Uniform[1, 5]秒内线性升至 R_max ~ Uniform[1, 6]GiB
           cpu 使用 ≈ Uniform[0.5, 2]核
           保持到 active 期结束
           active 结束后 5 秒内 RSS 通过 madvise 回到 128 MiB
```

baseline 参数集:

```
λ_a = 0.01 events/s/sandbox       # 平均每沙箱 100s burst 一次
α = 1.5, x_min = 5                # Pareto 形状
correlation = independent          # 沙箱独立 burst,可改 50% 同步
```

### 9.2 衡量指标

| 指标 | 目标 | 测量方式 |
|------|------|---------|
| guest OOM 频次 | 0 / 节点 / 小时 | cgroup memory.events.oom 累计 |
| host OOM 频次 | 0 / 节点 / 小时 | dmesg + /proc/pressure 异常 |
| burst P99 应用延迟 | < 100 ms 或基线值的 1.2 倍 | 应用内打点,vsock 协议回传 |
| 单节点最大 N | 越大越好 | 调高 N 直到 OOM 频次 > 阈值 |
| budget-grant 延迟 P99 | < 50 ms | 控制器内 RPC 计时 |
| Admit 拒绝率 | < 5%(green/yellow 区) | 控制器计数 |
| 节点预算利用率 | > 75%(memory) | (node_allocated_mem / allocatable_pool_mem) 时间均值 || CPU 公平偏差 | < 10%(节点饱和时) | 单沙箱实测 cpu / (allocatable.cpu × physical_cpu / Σ allocatable.cpu) |

## 10. 跟其他子系统的关系

### 10.1 与 sandbox-ctl

- **协议对端**:sandbox-ctl 是协议本侧执行器(沙箱级),node-ctl 是对端
  控制器(节点级)
- **通过 sandbox.yaml 关联**:sandbox.yaml `control.controller` 字段是 UDS
  路径,sandbox-ctl 启动时拨号
- **解耦**:控制器不直接管 CH 进程,不直接读 guest 状态;只跟 sandbox-ctl
  对话,sandbox-ctl 负责把决策落到 CH/cgroup

详见 `sandboxer/docs/sandbox.md` §与 node-ctl 的资源协议。

### 10.2 与 cache-ctl / store-ctl

cache-ctl / store-ctl 的资源占用是 host 预留的一部分,计入
`resources.host_reserved.memory`。典型:
- store-ctl(fs backend):~50-100 MiB(含 page cache 缓冲)
- cache-ctl(rocksdb L1):由 yaml `wal_size_limit_mb` + `block_cache_size`
  决定,典型 2-8 GiB
- 监控 / 日志:~500 MiB

实际 host_reserved 应在节点上跑空载基线后实测,加 ~20% 余量。

### 10.3 不感知 VMM

控制器只跟 sandbox-ctl 对话,不依赖 cloud-hypervisor 任何接口。这层间接是为了
让控制器跟 CH 解耦,未来 VMM 替换或控制器实现替换都不影响另一侧。

## 11. See Also

- [node.md](node.md) §10 —— 内置控制器(`resource_listen`)的宿主 `node-ctl conductor serve` 与
  node-link 集群接入(节点水位经心跳喂集群 P2C)
- `sandboxer/docs/sandbox.md` §资源模型 / §与 node-ctl 的资源协议 —— sandbox-ctl
  侧的执行器行为(本协议本侧)
- `sandboxer/docs/sandbox.md` §恢复时的资源衔接 —— 恢复路径下的 Admit 字段
  `allocatable_at_snapshot` 派生
- `accelerator/docs/cache.md` / `accelerator/docs/store.md` —— 资源预留计入 host_reserved
  的两个组件
- `platform/docs/perf.md` —— agent-intermittent 工作负载模型实测、密度调优
- `platform/docs/kuasar-sandbox.md` §1.1 / §4.7 —— 高密度承载与超分场景的业务定位
