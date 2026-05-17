# node — 节点资源控制器

`node-ctl` 是节点级控制器的参考实现,作为 systemd-managed daemon 运行,跟
节点上所有动态控制模式 sandbox-ctl 通过沙箱资源控制协议对话,完成跨沙箱
仲裁、burst 申请、settled 收回、admission 控制。

控制器是协议的**对端角色**,不是某个具体进程。任何遵循协议的实现都可作为
sandbox-ctl 的对端;`node-ctl` 是参考实现。本文档同时定义协议规范与 node-ctl
的内部组织。

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
| controller 状态文件损坏 | 重启时无法 fast-recovery | fallback:扫描 cgroup + 询问每个 sandbox-ctl 重建 |
| sandbox-ctl 崩溃 | 沙箱按既有规则销毁 | controller 检测连接断 → 自动 release 该沙箱预留 |
| controller ↔ sandbox-ctl 网络抖动 | RPC 超时 | 退避重连;期间 sandbox-ctl 用最近一次 grant 继续跑 |
| 单沙箱 OOM | guest 内进程被 kill;deflate_on_oom 释放 balloon | 非平台级故障;controller 计 OOM 事件 |
| host 物理 OOM | 内核 OOM killer 选目标 | 水位机制保证 node_allocated 始终 ≤ 物理可用,正常情况不应触发 |

## 2. 命令行接口

### 2.1 子命令总览

| 子命令 | 用途 |
|---|---|
| `daemon` | 启动 RPC server + admission + allocator + reclaimer + persister |
| `status` | 节点预算快照(只读) |
| `list` | 列举当前 reservation(只读) |
| `drain` | 进入排空状态(运维) |
| `grant` | 强制下发 budget(调试) |
| `reclaim` | 强制收回(运维) |

### 2.2 `node-ctl daemon`

```
node-ctl daemon [--config /etc/node-ctl/node-ctl.yaml]
```

启动 RPC server、admission controller、memory allocator、reclaim scheduler、
state persister(`/run/node-ctl/state.json`)。systemd 单元入口。

### 2.3 `node-ctl status`

```
node-ctl status [--state /run/node-ctl/state.json]
```

读 state 文件,打印节点物理资源、已 reserve、burst 中的沙箱数量。只读,
可与运行中的 daemon 共存。

### 2.4 `node-ctl list`

```
node-ctl list [--state /run/node-ctl/state.json]
```

把 reservation 表导出为 JSON。只读。

### 2.5 `node-ctl drain`

```
node-ctl drain [--socket /run/sandbox-resource.sock] [--disable]
```

通知 daemon 不再接受新沙箱准入;`--disable` 取消排空。运维命令——典型用于
节点维护前清空沙箱。

### 2.6 `node-ctl grant`

```
node-ctl grant <sid> --memory <size> [--socket /run/sandbox-resource.sock]
```

绕过仲裁直接给某沙箱发放内存 budget,用于排查或紧急调度。

### 2.7 `node-ctl reclaim`

```
node-ctl reclaim <sid> --memory <target> [--socket /run/sandbox-resource.sock]
```

直接将某沙箱内存收缩到目标值,用于人工干预异常场景。

## 3. 配置

### 3.1 node-ctl.yaml

`/etc/node-ctl/node-ctl.yaml`:

```yaml
listen: /run/sandbox-resource.sock
state_path: /run/node-ctl/state.json   # tmpfs
cgroup_scan_paths:                      # 重启恢复扫描路径
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

rate_limits:
  memory_grant_per_sec_factor: 0.05    # × allocatable_pool_mem

admission:
  rate: 4                              # /s
  burst: 16
  max_concurrent_creating: 16
  startup_ttl: 300s
  queue_ttl: 60s

dampening:                              # 振荡阻尼,不进 sandbox.yaml
  recover_duration: 60s                # burst → settled 观察期
  cooldown_periods: 10                 # × 100ms,burst → recover 判定门槛

logging:
  level: info
  audit_path: /run/node-ctl/audit.log  # tmpfs;高频审计不写磁盘
```

### 3.2 字段语义

| 字段 | 默认 | 说明 |
|---|---|---|
| `listen` | `/run/sandbox-resource.sock` | UDS,sandbox-ctl 拨号目标 |
| `state_path` | `/run/node-ctl/state.json` | tmpfs,daemon 重启快速恢复用 |
| `cgroup_scan_paths` | `[/sys/fs/cgroup/sandboxes]` | 重启时扫已知活沙箱 |
| `resources.physical_memory` | `auto` | 节点物理内存(`/proc/meminfo`)|
| `resources.physical_cpu` | `auto` | 节点物理核数(`nproc`) |
| `resources.host_reserved.memory` | (必填) | host 自身预留(kernel + cache-ctl + store-ctl + monitoring) |
| `resources.host_reserved.cpu` | (必填) | 同上,CPU 维度 |
| `watermarks.operational_margin_factor` | 0.10 | 节点预算的 10%,容差,不参与分配 |
| `watermarks.high_factor` | 0.85 | red 区起点 |
| `watermarks.low_factor` | 0.70 | yellow 区起点 |
| `watermarks.emergency_factor` | 0.05 | 紧急池,仅 urgency=high 申请可触 |
| `rate_limits.memory_grant_per_sec_factor` | 0.05 | 内存仲裁限速:每秒总扩展量 ≤ allocatable_pool × 此值 |
| `admission.rate` | 4 | Admit 速率,token bucket 装填速率(/s) |
| `admission.burst` | 16 | Admit 速率,token bucket 容量 |
| `admission.max_concurrent_creating` | 16 | 已 admit 但未到 settled 的沙箱数上限 |
| `admission.startup_ttl` | 300s | sandbox-ctl 必须在此时间内进入 settled |
| `admission.queue_ttl` | 60s | Admit 排队上限 |
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
```

`emergency_pool` 仅响应 urgency=high(oom 类紧急申请),正常仲裁动用前 95%
预算。

水位区间行为:

| 区间 | node_allocated 位置 | Admit | settled 主动收回 | burst 申请 |
|------|---------------------|-------|-------------------|-----------|
| **green** | < low_watermark | 接受 | 不主动 | 直接批 |
| **yellow** | [low, high] | 接受 | 渐进收回(10s 周期) | FIFO 限速批 |
| **red** | (high, allocatable_pool − emergency_pool] | **拒绝新沙箱** | 强制收回最旧 settled 沙箱超额 | 仅紧急批,普通申请排队 |
| **critical** | 进入 emergency_pool | 拒绝 | 强制 + 最旧优先 | 仅 oom urgency 批 |

CPU 维度水位**仅用于 admission**(`Σ allocatable.cpu_i ≤ allocatable_pool_cpu`),
**不参与运行期 grant 排队与限速**——CPU 不动态分配。

### 4.3 仲裁策略

`RequestBudget` 队列(仅内存维度)按 `(urgency, fairness_score)` 二元组排序:

- **urgency**:high(oom)> normal(throttled / high event)> low(预测)
- **fairness_score**:本沙箱过去 60s 内 grant 总量,越多越靠后(防饥饿)

仲裁限速:

- 内存:每秒总扩展量 ≤ allocatable_pool × 5%
- CPU:无运行时仲裁

限速是防惊群核心:即使 100 个沙箱同时 burst,grant 按队列序拉,被排队的沙箱
继续在 cgroup memory.high PSI 反压下等额度,而不是同时全 grant 撞墙。

## 5. 沙箱资源控制协议

### 5.1 协议形态

UDS,长度前缀(4 字节 BE uint32)+ JSON 消息——简单、调试友好、无外部依赖。

每个 sandbox-ctl 启动时 `Admit` 后建立长连,直到沙箱退出。连接生命周期与
沙箱生命周期对齐。**控制器实现是协议的对端,任何遵循本节定义的实现都可作为
sandbox-ctl 的对端**;`node-ctl` 是参考实现。

### 5.2 消息类型

请求/响应对(sandbox-ctl 发,控制器答):

```
Admit            (capacity, floor, startup_budget_memory, allocatable_at_snapshot?)
                 → AdmitResponse (status, reservation_token, granted_initial_alloc, queued_eta_ms)
                   status ∈ {admitted, queued, rejected}
                   granted_initial_alloc:冷启动 = startup_budget_memory;
                                          快照恢复 = allocatable_at_snapshot 或降级值

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
                 → Ack

Release          (token, reason)                                # reason ∈ {normal, error, ...}
                 → Ack
```

通知(控制器推,sandbox-ctl 接):

```
ReclaimRequest   (token, target_allocatable, deadline_ms)      # red 区强制收回
                 sandbox-ctl 必须在 deadline 前到达 target

UpdateConfig     (token, new_watermark_ratio, ...)             # 运维动态调
```

`token`:Admit 时由控制器生成的随机字符串,作为后续所有 RPC 的认证 + 索引。
重连时 sandbox-ctl 重发 token,控制器验证后绑定到现有 reservation。

`reason` 字段是字符串枚举,用于审计与调试。

**注意 RequestBudget 没有 dim 字段**——CPU 不参与运行时仲裁。

### 5.3 连接生命周期

**首次建连**:

1. sandbox-ctl 拨 UDS,发 `Admit`
2. 控制器检查水位 + concurrency + token bucket,返回 AdmitResponse
3. status=admitted:sandbox-ctl 持有 token,继续启动流程
4. status=queued:sandbox-ctl 等到 queued_eta_ms 后重试
5. status=rejected:sandbox-ctl 退出非零

**长连维持**:

- 沙箱进入 startup 后,连接保持活跃;sandbox-ctl 在此连接上发后续 RPC、
  收 ReclaimRequest/UpdateConfig 推送
- 30s 周期 Heartbeat;控制器 90s(3 个周期)未收到 → 视为掉线,触发故障
  处理(详见 §故障域处理)

**重连**:

- 连接断开,sandbox-ctl 退避重试(1s, 2s, 5s, 10s, 10s, ...)
- 重连成功后发 `Reattach(token, current_state)`,控制器验证 token 找到
  reservation,以 sandbox-ctl 上报为准同步状态
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
T1  parse config, compute (capacity, floor, startup_burst.memory)
    若 control.controller 为空 → 跳过 admit,跳到 T5
T2  dial controller, send Admit
T3  receive AdmitResponse
T4  if rejected: exit 2(节点已满)
T4' if queued:   sleep queued_eta_ms, retry from T2
T4'' if admitted: continue
T5  cgroup join(静态 cgroup / 动态控制模式写限制;无 cgroup 模式跳过)
T6  ... 启动序列继续(详见 sandbox.md §冷启动数据流)
```

`startup_burst.memory` 取自 sandbox.yaml(默认 = allocatable.memory),仅
动态控制模式有意义。

### 6.2 速率限制与并发限制

控制器内部对 Admit 维护:

- **token bucket**:rate `R_admit`(默认 4 个/s),burst `B_admit`(默认 16)。
  每 Admit 消耗 1 token;无 token 时排队
- **concurrent counter**:`creating_count`(已 admit 但未到 settled 的沙箱数);
  上限 `max_concurrent_creating`(默认 16)。超限的 Admit 排队

排队策略:FIFO 队列,带 TTL(60 s)。超 TTL 的回 status=rejected。

### 6.3 reservation 生命周期与 token TTL

```
Admit (admitted)
 │  reservation 占用 startup_burst.memory,token 生成
 │
 ▼
sandbox-ctl 启动 CH(cgroup join → memfd → ...)
 │  若 sandbox-ctl 在 startup_ttl(默认 5 分钟)内不发 Settled
 │   → 控制器视为创建失败,自动 release reservation
 ▼
launch hello(冷启动)/ restore_ack(恢复)
 │
 ▼
sandbox-ctl 发 Settled(token, current_rss, current_cpu_usec)
 │  reservation 进入 settled,startup_burst 转 allocatable_now
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

- `startup_ttl = 5min`:覆盖最坏冷启动(大镜像 + 慢网络下 manifest 恢复)。
  生产典型冷启动 < 30 s
- `queue_ttl = 60s`:Admit 排队太久无意义,上层调度器宁可换节点

## 7. node-ctl 内部组织

`node-ctl` 是协议的参考实现,作为 per-node daemon 由 systemd 管理。它内部
组织成四个角色,共享 in-memory state 与 /run 持久化:

```
node-ctl daemon
├── RPC server               处理协议消息
├── Admission Controller     §6 准入与速率限制
├── Memory Allocator         §4.3 仲裁与 grant
├── Reclaim Scheduler        §4.2 settled 期主动收回
└── State Persister          §8 /run/node-ctl/state.json 写
```

这些角色在协议规范中是隐式的——其他实现可以选择不同的内部组织。本文档其余
部分(§可靠性、§admission)以 node-ctl 行为为准描述。

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
4. **等待重连**:已知的活 reservation 各自标记 `awaiting_reattach`,从
   sandbox-ctl 收到 Reattach 后转回正常状态。30 s 内未重连的视为死亡,丢弃
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

agent 应用的运行剖面输入规范(实测在 [`perf.md`](perf.md)):

```
节点参数:
  N_sandboxes               沙箱总数(扫描变量,32 / 64 / 128 / 256 / 512)
  capacity_each             (memory=8 GiB, cpu=2)
  floor_each                (memory=128 MiB, cpu=0.1)
  startup_burst_memory      1 GiB

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
| 节点预算利用率 | > 75%(memory) | (node_allocated_mem / allocatable_pool_mem) 时间均值 |
| Reclaim push 准时率 | > 95% | reclaim deadline 内 sandbox-ctl 完成的比例 |
| CPU 公平偏差 | < 10%(节点饱和时) | 单沙箱实测 cpu / (allocatable.cpu × physical_cpu / Σ allocatable.cpu) |

## 10. 跟其他子系统的关系

### 10.1 与 sandbox-ctl

- **协议对端**:sandbox-ctl 是协议本侧执行器(沙箱级),node-ctl 是对端
  控制器(节点级)
- **通过 sandbox.yaml 关联**:sandbox.yaml `control.controller` 字段是 UDS
  路径,sandbox-ctl 启动时拨号
- **解耦**:控制器不直接管 CH 进程,不直接读 guest 状态;只跟 sandbox-ctl
  对话,sandbox-ctl 负责把决策落到 CH/cgroup

详见 [`sandbox.md`](sandbox.md) §与 node-ctl 的资源协议。

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

- [`sandbox.md`](sandbox.md) §资源模型 / §与 node-ctl 的资源协议 —— sandbox-ctl
  侧的执行器行为(本协议本侧)
- [`sandbox.md`](sandbox.md) §恢复时的资源衔接 —— 恢复路径下的 Admit 字段
  `allocatable_at_snapshot` 派生
- [`cache.md`](cache.md) / [`store.md`](store.md) —— 资源预留计入 host_reserved
  的两个组件
- [`perf.md`](perf.md) —— agent-intermittent 工作负载模型实测、密度调优
- `PROPOSAL.md` §1 / §6.11 —— 高密度承载与超分场景的业务定位
