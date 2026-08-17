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
4. **State 纯内存且可重建**:控制器不写 checkpoint/WAL;`/run` 中只有每个
   sandbox 生命周期写一次的 immutable lease。host 下电后进程、lock、cgroup
   一并消失,不跨 host reboot 恢复
5. **恢复先取安全上界**:控制器重启先从 live lease、managed pidfile/YAML 和
   populated cgroup 建 provisional reservation,再由 sandbox-ctl 的 StateSync
   原子替换成精确状态。恢复可短暂多记,绝不能漏记存活消费者
6. **事件驱动而非定时**:settled 等关键状态转换以 launch protocol 消息为
   触发,不依赖定时器估计

### 1.3 故障域

| 故障 | 直接影响 | 规避 / 自愈 |
|------|---------|-------------|
| controller 崩溃 | sandbox-ctl 长连断开;新沙箱 admit 暂停 | systemd 重启;sandbox-ctl 保持最后已应用额度并持续退避重连;controller 在 listen 前保守重建(§8.2) |
| 旧 `state.json` 缺失/损坏 | 无影响 | `state_path` 已弃用且忽略;恢复不读取共享 State 文件 |
| sandbox-ctl 崩溃 | 沙箱按既有规则销毁 | lease lock 随进程自动释放;仅在 lease 不 live 且 cgroup 不 populated 后释放记账 |
| controller ↔ sandbox-ctl 网络抖动 | RPC 失败 | 单一 jitter 指数退避 reconnect loop;期间保持最后已应用额度、不回退 floor、不销毁正常 VM |
| 单沙箱 OOM | guest 内进程被 kill;deflate_on_oom 释放 balloon | 非平台级故障;controller 计 OOM 事件 |
| host 物理 OOM | 内核 OOM killer 选目标 | 水位机制保证 node_allocated 始终 ≤ 物理可用,正常情况不应触发 |

## 2. 命令行接口

控制器由 `node-ctl conductor serve` 内置(配 `resource_listen`,node.md §3 / §10);只读巡检
与运维动词在 `node-ctl resource` 子命令组下。

### 2.1 子命令总览

| 子命令 | 用途 |
|---|---|
| `serve`(配 `resource_listen`) | 在该 UDS 起控制器:RPC server + inventory recovery + admission + allocator + reclaimer(node.md §2.2) |
| `resource status` | 节点预算快照(只读) |
| `resource list` | 列举当前 reservation(只读) |
| `resource drain` | 进入排空状态(运维) |
| `resource grant` | 强制下发 budget(调试) |
| `resource reclaim` | 强制收回(运维) |

### 2.2 控制器启动(`node-ctl conductor serve` 内置)

控制器随 `node-ctl conductor serve` 起:配 `resource_listen`(§3)即在该 UDS 起 RPC server、
inventory recovery、admission worker、memory allocator、active reclaimer 与 idle
sweeper(§7)。`resource_listen` 的子字段(`socket` / deprecated `state_path` /
`cgroup_scan_paths` / `resources` / `watermarks` / …)是内联在 conductor.yaml 里的控制器调参(§3)。
集群下,控制器上报的节点水位(zone / allocated / pool)经 serve 的 node-link 心跳喂
集群 P2C 放置(node.md §10、cluster.md / cluster-placer.md)。

### 2.3 `node-ctl resource status`

```
node-ctl resource status [--socket /run/sandbox-resource.sock]
```

经 live UDS 读取一次加锁的一致快照,打印水位区、节点预算、host 预留、运维
容差、已分配量、startup、reservation/provisional/unknown 数。控制器不在线时
明确失败,不返回陈旧文件。

### 2.4 `node-ctl resource list`

```
node-ctl resource list [--socket /run/sandbox-resource.sock]
```

经 live UDS 把 token-free reservation 视图导出为 JSON,包含 `provisional`、
`connected`、`recovery_source`、cgroup、额度和报告字段。客户端通过 SID 游标自动
聚合受 64 KiB protocol frame 限制的有界分页,查询不读取恢复文件。旧客户端的
单帧请求仅在完整结果可容纳时继续工作;超限时控制器明确要求升级,不会静默截断。

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

### 3.1 managed sandbox resource policy

Conductor 对所有 managed sandbox 使用以下 node-owned policy。Builder 的 A/B/C phase 也逐个
走同一 resolver/controller 路径;Build resources 是外层工作流准入与 systemd 限制,不在本池记账:

```yaml
sandbox:
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable:
      # cpu: 2                  # 省略 => 跟随最终 capacity.cpu
      memory: 256MiB
    # startup: { memory: 512MiB } # 仅 dynamic;省略 => 最终 capacity.memory
    overhead: { memory: 32MiB }
```

- `capacity` 是 guest VM 上限/SKU;Sandbox Create 的 E2B `cpuCount`/`memoryMB` 表示它。
  Template Register 上的同名字段只表示 Build resources,二者不关联。
- `allocatable` 是 steady floor。默认场景因此是 `memoryMB=2048`,floor=256MiB;
  dynamic 当前 grant 可在 `256MiB..2GiB` 变化。
- `startup` 是 admission/pre-settled 启动预算。dynamic 中 request/node 都未显式设置
  时取每个 sandbox 的最终 capacity,所以新的 256MiB floor 不会把启动预算一同降为
  256MiB。node 显式值按最终 floor/capacity 夹住;request 显式值越界直接拒绝。
- `overhead` 只来自 node,缺省 32MiB。`deflate_on_oom` 由 node-ctl 在实际有 balloon
  时固定 true。request/template/group/header/migration 都不能配置这两项。
- `watermark_high` 与 `control.sensor` 不渲染,继续使用 sandboxer 当前默认。

request/template 的 `kuasar-sandbox.resource` 只允许 capacity/allocatable/startup 的
五个 leaf,严格 JSON 解析并逐 leaf 合并。优先级是 node < template/group < create/reserve
body < resource header < E2B capacity leaf < restore snapshot capacity。任何低层非法 JSON
都会失败,不能由合法高层隐藏。request 不能携 control/cgroup/controller/deflate/
overhead/watermark/sensor。

static(`resource_listen` absent/disabled) 最终资源形态:

```yaml
resources:
  capacity: { cpu: 2, memory: 2GiB }
  allocatable: { cpu: 2, memory: 256MiB, deflate_on_oom: true }
  overhead: { memory: 32MiB }
```

它不渲染 startup/controller;request 显式 startup 与 node policy startup 都在产生
运行副作用前失败。dynamic 另外渲染:

```yaml
resources:
  startup: { memory: 2GiB }
  control:
    controller: /canonical/parent/sandbox-resource.sock
```

`resource_listen.socket` 是 endpoint 的唯一配置源。node-ctl 用绝对 `Listen` 路径 bind;
父目录 symlink 由 sandboxer `CanonicalSocketPath` 规范化成 `SocketIdentity`,controller
owner lock、lease inventory 和 sandbox YAML/client dial 都使用这一 identity 并指向同一
socket inode。最终 socket symlink、
dangling/ambiguous alias fail closed。`control.cgroup_path/CgroupFD` 不进 YAML,仍由
run-sandbox 通过继承 FD 注入。

img cold boot capacity 可由 portable patch 覆盖。snp create、paused resume、migration
restore 必须先读出 snapshot capacity;request 同值可作 assertion,不同即拒绝。probe
失败绝不回退 node defaults,且发生在 network Attach、runner Assign、resource Admit/cgroup/VM
之前。snapshot balloon 的 `allocatable_at_snapshot` 仍按既有 wire protocol参与 initial grant。

旧 `sandbox.resources.vcpu/memory/control_socket` schema 不再接受,没有兼容别名或第二
controller 优先级。trigger-time 通用 metadata/config header 同样已废弃并返回 400;
模板 Build trigger 暂保留的 `cpuCount/memoryMB` 只可断言等于注册时 Build resources,
不得覆盖本节 capacity 或 phase patch。

### 3.2 resource_listen 配置块

控制器配置是 `node-ctl conductor serve` 配置(node.md §3)的 `resource_listen` 块,内联在
conductor.yaml 里(无独立配置文件);`enabled: true` 即在其 `socket` 起控制器。字段
(yaml 形态,均挂在 `resource_listen:` 下):

```yaml
enabled: true
socket: /run/sandbox-resource.sock     # "" = pkg/resource 默认(与 sandbox-ctl 一致)
state_path: /run/node-ctl/state.json   # deprecated/ignored;仅兼容旧 YAML 解析
audit_path: /run/node-ctl/audit.log    # tmpfs;异步 best-effort 审计,不阻塞 RPC
cgroup_scan_paths:                     # 重启时扫描 populated sandbox cgroup
  - /sys/fs/cgroup/sandbox.slice/sandbox-runner.slice
  - /sys/fs/cgroup/sandbox.slice/sandbox-builder.slice

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

### 3.3 字段语义

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 置 `true` 才在 serve 内起控制器;否则沙箱用静态 cgroup |
| `socket` | `pkg/resource` 默认 | 文件系统 UDS 的 bind/dial 路径;`""` = 协议默认。保留配置中的绝对路径作为 endpoint,另把父目录 symlink 解析成 owner/lease canonical identity,因此短 alias 仍可规避 AF_UNIX 路径长度限制 |
| `state_path` | 无 | **deprecated/ignored**;仅保留旧 YAML 可解析,不会打开、读取或写入 |
| `audit_path` | `/run/node-ctl/audit.log` | tmpfs;后台 goroutine 异步 best-effort 写,资源 RPC 不等待文件 I/O |
| `cgroup_scan_paths` | `[/sys/fs/cgroup/sandbox.slice/sandbox-runner.slice, /sys/fs/cgroup/sandbox.slice/sandbox-builder.slice]` | lease/cgroup 身份边界和重启对账扫描根；默认覆盖普通 sandbox runner 和 Builder A/B/C phase 的 `vmm` cgroup。直接运行 `sandbox-ctl run` 时需显式加入其 cgroup 根。 |
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
| `admission.startup_ttl` | 30s | sandbox-ctl 应在此时间内进入 settled;超时标记诊断状态,但 live lease 或 populated cgroup 仍在时不释放记账 |
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
                  allocatable_at_snapshot?, cgroup_path, client_features?)
                 → AdmitResponse (status, token, granted_initial_alloc,
                                  reason?, queued_for_ms, queue_pos_at_in)
                   status ∈ {admitted, rejected}
                   (queued 保留不用:短期阻塞时 server 持连不回应,§5.3)
                   granted_initial_alloc = max(startup_budget_memory, floor,
                                               allocatable_at_snapshot)
                   reason:rejected 时分类(§6.4)
                   queued_for_ms / queue_pos_at_in:命中队列时回填的诊断元数据
                 # sandbox_id 用作 admin 动词(grant/reclaim)的 O(1) 索引键;
                 # 新客户端发出的不可变字段必须等于其 Admit 前写入的 lease

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

StateSync        (sandbox_id, applied_allocatable_memory, settled,
                  current_rss, previous_token?)
                 → Ack (token=new_session_token,
                        new_allocatable=applied_allocatable_memory)
                 # previous_token 仅兼容/诊断;认证来自 SO_PEERCRED + live lease lock
                 # provisional charge 以同一个 State 临界区原子替换为精确 reservation
```

admin 动词(运维 CLI 发,控制器答;以 sandbox_id 而非 token 定位目标):

```
AdminStatus      ()                                             # node-ctl 经 RPC 查实时态
                 → Ack (zone, node_allocated, allocatable_pool,
                        reservation_count, provisional_count,
                        unknown_count, startup_in_flight, drained)

AdminList        (list_after?, list_limit?)                     # SID exclusive cursor
                 → Ack (reservations=[... connected, provisional,
                                      recovery_source ...], list_next?)
                 # 新客户端 list_limit=128 并自动聚合;server 可为 64 KiB frame
                 # 返回更少条目。list_limit=0 是旧客户端单帧请求,超限明确 Error

AdminDrain       (drain)                                        # 背后 node-ctl resource drain
                 → Ack (drained)

AdminGrant       (sandbox_id, requested_delta)                  # 背后 node-ctl resource grant
                 → Ack (granted_delta, new_allocatable)         # 绕过水位/限速

AdminReclaim     (sandbox_id, target_allocatable)               # 背后 node-ctl resource reclaim
                 → Ack (new_allocatable)                        # shrink-only,clamp 到 floor
```

`token`:Admit 或 StateSync 时由控制器生成的 session 随机字符串,作为后续 RPC
的认证 + 索引。controller 重启后 token 可失效;新客户端以 lease 身份执行
StateSync 并取得新 token。Reattach 仅为旧 server/client 的滚动升级兼容。

`reason` 字段是字符串枚举,用于审计与调试。

**注意 RequestBudget 没有 dim 字段**——CPU 不参与运行时仲裁。

### 5.3 连接生命周期

**首次建连**:

1. 动态模式 sandbox-ctl 在第一次 Admit 前创建 immutable lease 并持有 POSIX
   write lock;文件名是 SID 的 SHA-256,内容只写一次
2. sandbox-ctl 拨 UDS,发 `Admit`
3. 控制器在 accept 时取得 `SO_PEERCRED`,并以内存中的 pool/State 与配置好的
   cgroup root 评估请求;Admit 热路径不 reopen/解析 lease、pidfile 或 YAML:
   - 通过 → 立即回 `status=admitted`
   - 长期失败(drain / zone red/critical / 超 pool / 超 startup_pool)→ `status=rejected`
   - 短期阻塞(token / main_headroom / startup_headroom)→ **不回应**,
     conn 入服务端 FIFO queue;worker 在条件满足时回 `admitted`,
     queue_ttl 超时回 `rejected`
4. status=admitted:sandbox-ctl 持有 token,继续启动流程
5. status=rejected:sandbox-ctl 退出非零(上层调度决定 retry / 换节点)

**长连维持**:

- 沙箱进入 startup 后,连接保持活跃;sandbox-ctl 在此连接上发后续 RPC,并经
  Heartbeat ack 的 `new_allocatable` 接收 reclaim/admin 的 allocatable 调整
- 30s 周期 Heartbeat;控制器 90s(3 个周期)未收到 → 视为掉线,触发故障
  处理(详见 §故障域处理)

**重连**:

- 任意 EOF/reset/broken pipe 后保留最后成功落地到 `memory.high` 和 balloon 的
  `applied_allocatable`,暂停正向 budget 请求,由唯一 reconnect loop 按带 jitter
  的指数退避重新 Connect
- 新 server:发送 `StateSync(sid, applied, settled, rss, previous_token?)`。controller
  验证 lease lock owner = `SO_PEERCRED` peer、managed 元数据和
  `floor ≤ applied ≤ capacity`,再原子替换
  provisional 并签发新 token
- 不支持 StateSync 的旧 server:回退 `Reattach(previous_token)`;旧 server 回带的
  allocatable 仍须成功落地后才成为新的 `applied_allocatable`
- 所有 RPC、连接替换和同步共享一个 session 串行化边界,每个 sandbox 最多一个
  outstanding request

**controller 不可用期间**:

- sandbox-ctl 保持最后已经实际应用的额度,不回退 floor,不销毁正常运行的 VM
- sensor 暂停新的正向 budget 请求;Heartbeat 和 budget 在同步成功后恢复
- cgroup 与 balloon 保持原值;不能因重连耗时切换成另一套无 controller 状态机

**沙箱退出**:

- sandbox-ctl 正常退出前发 `Release`,再在仍持 lock 时 unlink lease并关闭 FD
- sandbox-ctl 异常退出无 Release时 lock 自动释放;控制器只有确认 lease 不 live
  且 cgroup 不 populated 后才能移除 reservation

## 6. 创建期管控

### 6.1 admission 流程

`sandbox-ctl run` 启动后第一动作不是 cgroup join,而是判断是否需要连控制器:

```
T0  sandbox-ctl run --config xx.yaml
T1  parse config, compute (capacity, floor, startup.memory)
    若 control.controller 为空 → 跳过 lease/admit,跳到 T5
T2  创建并锁定 immutable lifecycle lease(每生命周期只写一次)
T3  dial controller,send Admit;block read response(可能在 queue 中等待)
T4  if rejected:unlink lease,exit 2(上层调度决定 retry / 换节点)
T4' if admitted:保存 session token,continue(queued_for_ms 仅诊断)
T5  cgroup setup(静态/动态模式写限制;CH 通过 CLONE_INTO_CGROUP 原子出生在目标中)
T6  ... 启动序列继续(详见 `sandboxer/docs/sandbox.md` §冷启动数据流)
```

sandbox.yaml `resources.startup.memory` 是 sandbox 期望的 startup 阶段
budget。sandboxer 裸配置的内部 fallback 仍是 allocatable,但 orchestrator-managed
dynamic sandbox 总是按 §3.1 显式解析并渲染:request/node 都未配置时为最终 capacity,
不会因默认 256MiB floor 降低启动预算。admission 实际给出 `granted_initial_alloc
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
`queueMu`,再调用内部加锁的 State 具名转换(锁顺序固定避免死锁;调用方不接触 State mutex)。

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
 │  lease 已锁定;reservation 占用 effective_startup_budget,token 生成
 │
 ▼
sandbox-ctl 启动 CH(cgroup join → memfd → ...)
 │  若 sandbox-ctl 在 startup_ttl(默认 30s)内不发 Settled
 │   → 标记 startup_expired;live lease/populated cgroup 仍在则保留安全上界记账
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
 │  Release 正常释放;仅断连则保留并等待 StateSync/Reattach
 │  正常退出 unlink lease;异常退出由 lock 自动释放
 ▼
end
```

**TTL 设计依据**:

- `startup_ttl = 30s`:覆盖典型冷启动(大镜像 + 慢网络下 manifest 恢复)。
  超时用于诊断和清理已消失消费者;live lease/populated cgroup 永远优先于 TTL
- `queue_ttl = 30s`:Admit 排队太久无意义,上层调度器宁可换节点

### 6.6 per-sandbox resource stats

`GET /sandboxes/{sid}/stats/resource` 由 conductor 直接读取本进程 controller state,不发
resource RPC、不访问 envd 或 guest `/metrics`,也不触发生命周期动作。controller 以 SID
为主索引,另维护 token/cgroup 派生索引;Admit、StateSync、Release、IdleSweeper 和
所有其它删除路径统一维护该索引,因此查询不需要线性扫描 reservation 表。

Settled/Heartbeat 仅在 `CurrentRSS>0` 时同时更新:

```text
last_reported_rss = CurrentRSS
last_report_at    = controller receive time
```

`last_heartbeat_at` 仍只表示 reservation liveness,不能冒充资源采样时间。公开 sparse 字段为:

| 字段 | controller 来源 |
|---|---|
| `timestampUnix` | `last_report_at` |
| `cpuCount` | `capacity.cpu_milli / 1000` |
| `cpuAllocatable` | 当前 CPU budget/floor,单位 core |
| `memUsed` | `last_reported_rss`(sandboxer cgroup `memory.current`) |
| `memTotal` | `capacity.memory_bytes` |
| `memAllocatable` | `allocatable_now_mem` |

任何未采集字段都省略,不用零填充。controller disabled 返回 501;starting 有 reservation 可返回
sparse 200;paused 无 live reservation 返回 409;running 缺 reservation 返回 503;reservation
存在但尚无 RSS report 时省略 `timestampUnix`/`memUsed`。API ownership 与完整 JSON 例见
[node.md](node.md) §4.1.1。

## 7. node-ctl 内部组织

`node-ctl` 的内置控制器是协议的参考实现,随 `node-ctl conductor serve` 起(配 `resource_listen`)。
它内部角色共享一个纯内存、可重建的 State:

```
controller (node-ctl conductor serve resource_listen)
├── Owner lock               <controller-socket>.owner 生命周期独占
├── Inventory                live lease + managed metadata + populated cgroup
├── RPC server               SO_PEERCRED、StateSync 与协议消息
├── Admission Controller     §6 准入与速率限制
├── Memory Allocator         §4.3 仲裁与 grant
├── Reclaim Scheduler        §4.2 settled 期主动收回
└── State                    SID 主索引 + token/cgroup 索引 + O(1) aggregates
```

State 的 `allocatedMem`、`allocatedCPU`、`startupInFlight`、provisional/unknown
计数随具名转换方法同步维护。Admit/Settled/Heartbeat/Grant/Reclaim/Release/OOM
不做 State 快照、JSON 编码、`open/write/fsync/rename` 或等待持久化锁。
`ResourceSnapshot` 在一次 State 加锁内同时读取水位所需的全部聚合值,不会无锁
遍历 map 或拼接跨时刻字段。周期 reclaimer/sweeper 可以扫描诊断视图,但正常
RPC 的聚合与水位判断是 O(1)。`queueMu` 涉及 State 时顺序固定为先 queue、再
调用内部自行加锁的 State 转换;调用方不直接 Lock/Unlock 或改 map。

这些角色在协议规范中是隐式的——其他实现可以选择不同的内部组织。本文档其余
部分(§可靠性、§admission)以该控制器行为为准描述。

## 8. 可靠性

### 8.1 真相来源与 inventory

控制器不保存共享 checkpoint、WAL、append log 或每沙箱动态状态文件。真相按
职责拆分:

| 信息 | 权威来源 |
|---|---|
| 消费者是否仍活 | lease 的 POSIX write lock;managed pidfile lock;populated cgroup |
| SID/controller/cgroup/capacity/floor/startup/features | `<controller-socket>.leases/<sha256(sid)>.json` immutable lease |
| 当前实际应用内存额度、settled、RSS | sandbox-ctl 内存状态,重连经 StateSync 上报 |
| session token、heartbeat、OOM/cooldown | 当前 controller 进程内短期状态 |
| 节点聚合 | State 具名转换维护的 O(1) 计数器 |

lease 由 sandbox-ctl 在第一次 Admit 前创建、写一次并锁定,正常退出时 unlink;
SIGKILL 时内核释放 lock,残留文件下次扫描清理。文件名只使用 SID SHA-256,
tenant SID 不进入目录拼接。`F_GETLK` 返回锁 owner PID;JSON 中自报 PID 不能
单独证明存活。lease 只在 sandbox 生命周期边界产生文件 I/O,不在任何资源 RPC
热路径更新。

controller 在删除旧 UDS 或 bind 前先非阻塞取得稳定
`<controller-socket>.owner` 的 POSIX write lock。第二实例在动到 live UDS 前
失败,owner FD 持有到 server 完整退出。这里的 `<controller-socket>` 是 canonical
inventory identity,不是必须用于 AF_UNIX bind/dial 的原始 spelling:父目录 symlink
会解析;endpoint 最终组件 symlink、dangling parent 和多 hard-link socket 因无法
得到唯一 identity 而 fail closed。

### 8.2 控制器进程重启恢复

控制器进程重启(systemd 自动 restart 或运维主动)但 host 未重启时:

1. 取得 owner lock,此时尚未删除或 bind UDS。
2. 扫描 `<controller-socket>.leases/` 并用 `F_GETLK` 判活:
   - 正常 live lease:memory/startup 按 capacity、CPU 按 immutable floor 保守计费;
   - live 但 JSON/身份损坏:安装 unknown provisional,至少占满整个 allocatable pool;
   - stale unlocked lease:删除残留,不计费。
3. 扫描 managed run root。没有新 lease 的旧 managed sandbox 由 locked pidfile、
   YAML 合同和 `/proc/<pid>/cgroup` 推出 VMM cgroup,按 capacity provisional 计费;
   若 live pidfile 对应的 YAML/cgroup 尚不能可信恢复(包括 Admit→cgroup 窗口),
   以该 SID 按整个 pool 建 unknown provisional,绝不跳过。
4. 扫描 `cgroup_scan_paths`。无 lease/managed 记录但 populated 且有消费者进程的
   cgroup 按 `memory.max` 计费;无法读、为 `max` 或超过 pool 时占满 pool。
   State 的规范化 cgroup 索引保证 lease/managed/cgroup 同一消费者不重复计费。
5. inventory 全部安装到 State 后才删除 stale UDS、bind 并对外服务。因此任何
   新 Admit 都先看到全部 provisional 安全上界。
6. sandbox-ctl 重连 StateSync。controller 用 `SO_PEERCRED` 验证 peer PID 等于
   lease lock owner,验证 controller socket/cgroup root、额度范围;managed 模式
   再交叉核对 pidfile lock/PID 与 YAML 的 capacity/floor/startup/controller。
   managed YAML 不包含最终 cgroup path——runner 在 `exec` 时通过 node-owned FD
   注入——因此还从 `/proc/<peer-pid>/cgroup` 推导 sibling `vmm` 并与 lease 比较。
   成功后在一个 State 临界区以实际 applied reservation 替换 provisional、更新
   全部聚合差值并签发新 session token。

旧 sandbox-ctl 在新 controller 中由 managed/cgroup provisional 保守计费;
新 sandbox-ctl 对不认识 StateSync 的旧 controller 回退 Reattach。滚动升级期间
controller 会先规范化旧 lease 内保存的等价 parent-alias socket identity,因此可
继续精确 StateSync;其它旧客户端则允许暂时多记,任何混用路径都不能少记。

### 8.3 故障域处理

- **连接断开**:只清当前 connection contribution,不删 reservation。live lease、
  locked managed pidfile 或 populated cgroup 任一成立都保留记账。
- **heartbeat timeout/startup TTL**:同样先验证 inventory liveness;live 消费者只
  标记 `startup_expired` 等诊断字段,不释放额度。
- **sandbox-ctl 崩溃**:lease lock 自动释放;若 CH/cgroup 也已消失则 sweeper
  才移除。若 cgroup 仍 populated,继续按现有 reservation或 orphan 上界计费。

**控制器自身崩溃**:

- owner lock 随进程退出释放,systemd 可启动 replacement
- sandbox-ctl/VM 不重启,保持最后已应用额度并持续 reconnect
- replacement 先建安全上界、再 listen;StateSync 后恢复精确联动
- `state.json` 缺失、损坏或旧配置指向任意路径均不影响该流程

**全节点 OOM(host 级)**:

- 不应发生(水位机制保证)。若发生,kernel OOM killer 选目标。controller 与
  cache-ctl 等 host 进程优先级低于 sandbox(由 systemd OOM score 设置),
  先被杀
- 控制器重启后走重建流程,期间沙箱保持最后已应用额度继续运行

**host 重启**:

- 所有沙箱与控制器同时消失
- `/run`、lease、进程和 cgroup 同时清空
- 重新启动后从空状态开始,等上层调度器重新发沙箱

该方案只保证**无 host reboot 的 controller 异常重启恢复**。它不是共享
checkpoint/WAL,也不提供跨节点 HA/共识;host reboot 后没有存活消费者需要恢复。

## 9. 工作负载模型与基线

### 9.1 agent-intermittent 模型

agent 应用的运行剖面输入规范(实测在
`kuasar-sandbox/docs/perf.md`):

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
- `kuasar-sandbox/docs/perf.md` —— agent-intermittent 工作负载模型实测、密度调优
- `kuasar-sandbox/docs/kuasar-sandbox.md` §1.1 / §4.7 —— 高密度承载与超分场景的业务定位
