[English](node-resource.md) | [简体中文](node-resource_zh.md)

# node-resource — 节点 reservation 控制器

`node-ctl conductor serve` 可选地内置 node resource controller。它只处理节点侧
reservation、admission、pool、水位、恢复 inventory、压力 episode 和统计投影。压力 worker 通过 orchestrator 协调现有完整内存 Pause/Resume。sandbox 的
guest memory observation、Cloud Hypervisor balloon、`memory.high`、cold/restore/
snapshot 生命周期均由 sandboxer 自闭环管理,不属于 node controller。

## 1. 概述

### 1.1 责任边界

内存控制分成两个独立流程:

```text
sandbox-local loop
  guest MemAvailable + CH target/current
    -> RequestedBudget
    -> optional RequestBudget reservation RPC
    -> memory.high / vm.resize / convergence

node reservation loop
  Admit / RequestBudget / StateSync / Release
    -> NodeReservation
    -> node aggregate / pool / zone / recovery
```

正常 reservation loop 不采样或写入 sandbox cgroup、不调用 CH API、不接收 Guest MemReport,
也不推送 balloon target。恢复 inventory 会只读进程/cgroup identity、存活与保守上界
(§8.1),但不据此实现 Guest Budget loop。Controller 原子处理 sandbox 主动请求;
Heartbeat 返回 reservation echo,不是执行命令。

Builder registration/execution 两本账与逐 Sandbox reservation 分开. 每个实际 Build 阶段
沿用普通 sandbox-ctl Admit/Release. Build.Resources 配置 A/B 规格,目标 Sandbox 独立解析.
Builder service/slice 只承担归属, 委托和整组回收,orchestrator 不增加 CPU/内存限额;
现役 VMM 叶子策略和保守只读 recovery inventory 保持不变.

### 1.2 内存复用设计模型

该控制器面向的节点负载具有明显的不均衡性:大量 sandbox 长时间空闲或低负载,只有较少
sandbox 在较短阶段密集使用内存。设计目标不是持续按每个 sandbox 的 Capacity 驻留或预留
内存,而是把空闲 sandbox 释放的内存重新用于节点上的其他 sandbox;同时保留足够的反应余量,
使突发负载能够在 guest OOM 导致工作丢失之前完成扩容。

机制刻意把三个职责分开:

- **Balloon/Budget 是单 sandbox 的内存大小执行器。** Balloon inflate 降低 guest Budget,
  是空闲 sandbox 实际交还 guest 内存的操作;grow 获得授权后通过 balloon deflate 扩大 Budget。
- **NodeReservation 是跨 sandbox 的分配总账。** 只有本地 shrink 已安全完成并提交更小
  reservation 后,这部分额度才可供其他 sandbox 重新使用。Host RSS 下降或单独降低
  `memory.high` 都不等于释放节点额度。
- **`memory.high` 主要是 host 侧节流边界,不是回收所有者。** VMM host charge 超过
  `memory.high` 后,Linux 对 cgroup 执行 reclaim 和节流;产生的 stall 与
  `memory.events.local` 提供压力反馈。仅仅接近阈值不会触发它的节流或 `high` 事件。
  减缓后续增长是为了给 Budget grow 闭环争取取得 reservation 和 deflate balloon 的时间。
  此路径上的内核 reclaim 不是节点的记账事件,也不能替代 balloon shrink + reservation
  release。`memory.high` 不是硬上限,实际占用可以超过它;参见
  [Linux cgroup v2 内存接口](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files)。

因此 grow 和 shrink 有意采用不对称策略。在正常受控路径中,grow 以安全为优先:
先预留节点额度,再放宽 `memory.high`,最后 deflate balloon。Shrink 则保守执行:
先 inflate balloon,观察到安全的 current 进展,再降低 `memory.high`,最后释放
reservation。空闲内存回收可以滞后。Guest 应急 `deflate_on_oom` 是 §1.3 描述的显式
例外,不是正常提高密度的机制,也不等于 node 已经 grant。

`Headroom` 是每个 sandbox 本地的反应余量,node emergency pool 是共享安全余量。
Per-sandbox headroom 帮助覆盖发现压力到完成控制的延迟;共享 pool 保留给 high-urgency
grow（§4.3）,不供普通 grow 使用。它不能替代本地 headroom,也不保证每次突发都能及时
取得内存。OOM/high-urgency 是最后的安全路径,不是正常 grow 的主要信号。

大量 sandbox 并发 grow 时,sandbox 之间不直接转移内存。每个 sandbox 保留自己的 grow
objective,并向共同 node pool 请求 reservation。State 串行化 aggregate reservation 更新,
所以并发请求不能重复消费同一份 headroom。普通新增 grant 还受 node-wide runtime grant
token bucket 整形;red/critical zone 继续保护 node safety 和 emergency pool。Runtime grow
没有 node 侧 FIFO 等待队列:收到 partial/zero grant 的 sandbox 保留本地 objective,在本地
控制器处理 observation、pressure event 和未完成事务时,遵守返回的 cooldown 继续重试。

稳态反馈模型为:

```text
idle sandbox
  guest MemAvailable 上升
    -> balloon inflate
    -> 观察到安全的较小 Budget
    -> memory.high down
    -> release NodeReservation
    -> node pool 可被其他 sandbox 复用

bursting sandbox
  guest report 或 host pressure
    -> RequestBudget
    -> node 原子记账 grant
    -> memory.high up
    -> balloon deflate
    -> guest 获得更多 Budget

host pressure fast path
  VMM charge 超过 memory.high
    -> 节流（伴随内核 reclaim）+ PSI/events 反馈
    -> sandbox-local grow request
    -> reservation grant/cooldown
```

该设计不承诺所有 sandbox 可以同时达到 Capacity。Capacity 是单 sandbox 的上限;
NodeReservation 才是共享 node pool 当前已经承诺给该 sandbox 的额度。避免 guest OOM
是设计目标,不是在容量、可授予额度或控制闭环响应时间不足时仍能兑现的无条件保证。

### 1.3 术语

| 名称 | 定义 | 所有者 |
|---|---|---|
| Capacity | VM 固定最大内存 | sandbox config/snapshot |
| Headroom | `resources.allocatable.memory`,settled guest headroom | sandbox policy |
| StartupHeadroom | `resources.startup.memory`,cold 首份可信 report 前的 headroom | sandbox policy |
| BudgetAtSnapshot | `Capacity - min(snapshot target,snapshot current)` | sandbox snapshot |
| NodeReservation | node 已为某 sandbox 保留的绝对内存额度 | node controller |
| HostMemoryCurrent | host VMM cgroup `memory.current`,仅诊断 | sandbox 上报,node 记录 |
| reservedMemory | 所有 live `NodeReservation` 的和 | node state |

`Headroom` 不是 total Budget. CPU `allocatable` 表示映射到 `cpu.weight` 的相对调度规格,
不表示 fractional-core 硬 quota 或无条件性能保证. 其含义与 memory headroom 不同.

sandbox 内部另有 `TargetBudget`、`CurrentBudget`、`ObservedBudget` 和
`DemandMemory`;node 不需要也不保存这些状态。正常受控路径中 sandbox 先取得足够
`NodeReservation` 才扩大 Budget,并在 shrink 收敛后才释放 reservation。
`deflate_on_oom` 的 guest 应急 deflate 是阶段化软保证的例外:它不改变 target 或
reservation,原有 `memory.high` 继续对超过阈值的 host VMM charge 施加节流,sandbox 把 target/current
标记为 unstable 并禁止 shrink。snapshot 仍按两侧安全上界计算
`BudgetAtSnapshot`。

### 1.4 核心不变量

- `0 < NodeReservation <= Capacity`。
- `reservedMemory = sum(live NodeReservation)`。
- pool、RawZone、ResourceProbe allocated 与 recovery replacement 只聚合 NodeReservation；有效 Zone 另记录合格压力与尚未完成的恢复义务。
- grow 只由 sandbox 发起;node 先记账再返回 grant。
- shrink 只由 sandbox 在 balloon current 收敛且 `memory.high` 已按顺序处理后提交。
- `Settled` 是生命周期事实,不从 `memory.current` 推导或改写 reservation。
- controller restart 先按 Capacity provisional charge,StateSync 后原子替换。
- stale heartbeat、host charge 或 node 管理命令不能改变某个 sandbox 的
  reservation。

## 2. 命令行接口

controller 由 `node-ctl conductor serve` 的 `resource_listen` 启动,没有独立 daemon
子命令。管理命令只提供观察和 admission drain:

```text
node-ctl resource status [--socket PATH]
node-ctl resource list   [--socket PATH]
node-ctl resource drain  [--socket PATH] [--disable]
node-ctl resource pressure [--socket CONTROL_SOCKET]
```

`status` 展示 node budget、host reserved、operational margin、allocatable pool、
reserved memory、startup in-flight、zone 和 recovery 数量。`list` 输出逐 sandbox
reservation。`drain` 只禁止新 admission,不改变任何 live reservation。

`pressure` 通过 conductor control socket（或 NODE_CTL_SOCKET）的 `GET /internal/admin/resource-pressure` 查询，沿用本地 admin 认证。返回有效/原始水位、进入原因/版本/时间、R/P/E、剩余 hold、保护额度、按阶段统计的 Q、清理屏障、最老等待、逐沙箱暂停原因/运行起点和有界最近操作结果。现役 resource reservation 协议不变。不再输出原 `host_safety_blocked` 诊断字段；该查询不测量整机内存安全状态。

不存在 `resource grant` 或 `resource reclaim`。Headroom 属于 sandbox policy,应通过受支持的 sandbox 配置/生命周期输入选择,
再由 sandbox 既有闭环根据 observation 收敛。这不引入 node 侧 live policy reload
或直接 balloon/cgroup 调整命令。

`worker_blocked=no_eligible_running_sandbox` 表示当前没有可安全换出的 running 对象；节点继续保守等待，不释放他人占账。

## 3. 配置

### 3.1 sandbox resource policy

```yaml
sandbox:
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable:
      memory: 256MiB
    startup: { memory: 2GiB }
    overhead: { memory: 32MiB }
    watermark_high: { ratio: 0.875 }
```

- `capacity.memory` 是 Capacity。
- `allocatable.memory` 是 settled headroom,缺省 `256MiB`;若继承缺省值大于最终
  Capacity,resolver 收敛到 Capacity。request 显式越界则拒绝。
- `startup.memory` 是 cold startup headroom,缺省为最终 Capacity,同时适用于 static
  和 dynamic 模式。它与 settled headroom 独立,不要求更大或更小。
- `overhead.memory` 是 node-owned host VMM overhead;sandbox-ctl 位于独立 control
  cgroup,不消费该额度。`watermark_high.ratio` 同样由 node policy 拥有。两者都不允许
  request/template 设置。
- `0 < allocatable.memory <= capacity.memory`。
- `0 < startup.memory <= capacity.memory`。
- `0 < watermark_high.ratio < 1`,缺省 `0.875`。

restore 的 Capacity 来自 snapshot。`startup.memory` 不参与 restore admission;
sandboxer 上报的 `BudgetAtSnapshot` 是唯一 initial reservation。

restore 的同步请求只校验 portable patch 结构。runner 绑定 exact run-id 后,task 在进程内
读取根 `snapshot.cfg`;Manifest Bundle根会先只读metadata prefix,从平面 `bundle/refs`补全
located来源mapping,但不会打开或递归扫描refs Bundle。task把 Capacity 作为非秘密 summary
交给唯一 launch worker;conductor不打开 snapshot。request 显式 Capacity 可作为 assertion,不一致或 snapshot 读取失败成为
异步 `resource_resolve` failure。失败发生在 network Attach、sandbox YAML、controller Admit
和 VM 启动前,并且绝不回退 node default。`BudgetAtSnapshot` 仍由 sandboxer 从 CH
snapshot target/current 计算,不由 orchestrator probe 或推导。

### 3.2 resource_listen

```yaml
resource_listen:
  enabled: true
  socket: /run/sandbox-resource.sock
  cgroup_scan_paths:
    - /sys/fs/cgroup/sandbox.slice/sandbox-runner.slice
    - /sys/fs/cgroup/sandbox.slice/sandbox-builder.slice
  resources:
    physical_memory: auto
    physical_cpu: auto
    host_reserved: { memory: 16GiB, cpu: 1.5 }
  watermarks:
    operational_margin_factor: 0.10
    high_factor: 0.85
    low_factor: 0.70
    emergency_factor: 0.05
    startup_factor: 0.50
  rate_limits:
    memory_grant_per_sec_factor: 0.05
  admission:
    rate: 4
    burst: 16
    startup_ttl: 30s
    queue_ttl: 30s
    queue_max_depth: 256
  pressure:
    interval: 1s
    failure_interval: 500ms
    critical_after_rounds: 3
    pause_after_rounds: 3
    critical_exit_hold: 5s
    red_to_yellow_hold: 30s
    yellow_to_green_hold: 30s
    minimum_run_time: 30s
```

`operational_margin_factor` 从扣除 HostReserved 后的预算中保留 node safety margin。
`emergency_factor` 为 high-urgency grow 保留 pool。`startup_factor` 限制创建/恢复中
reservation 的并发总量。admission `rate`/`burst` 是请求 token bucket,不是内存
Budget。

controller preflight 要求 `host_reserved.memory < physical_memory`,并验证
`0 <= operational_margin_factor < 1`、`0 <= low_factor < high_factor < 1 - emergency_factor <= 1`、
`0 <= emergency_factor < startup_factor <= 1` 和
`0 < memory_grant_per_sec_factor <= 1`。最终 pool 必须为正，字节取整后要求 Ty < Tr < Tc。pressure 周期与 hold 为正，失败轮数为正，轮间隔至少 250ms，最短运行窗口非负。非法值在服务启动前失败。这些缺省值是调优起点，应在目标节点验证 capture/restore 时延及宿主净释放。

`resource_listen.socket` 是 endpoint 的唯一配置源。node-ctl 以绝对路径 bind,并把父目录
symlink 规范化为 owner lock、lease inventory 和 sandbox client 共用的 canonical identity;
最终 socket symlink、dangling 或 ambiguous alias fail closed。`control.cgroup_path` 不写入
sandbox YAML,runner 仍通过继承 cgroup FD 注入该 host capability。
`resource_listen.state_path` 已弃用且被忽略,不启用 state.json 恢复(§8.1)。

### 3.3 request/template 所有权

tenant resource patch 只允许 `capacity`、`allocatable`、`startup`。`control`、
`overhead`、`watermark_high`、sensor 和 `deflate_on_oom` 均由 node resolver 管理。
parser 对未知字段和这些越权字段直接报错,不会静默忽略。

## 4. Node reservation 模型

### 4.1 pool

```text
Physical           = NodeBudget status field
PostHostBudget      = saturating_sub(Physical, HostReserved)
OperationalMargin  = PostHostBudget * operational_margin_factor
AllocatablePool    = saturating_sub(PostHostBudget, OperationalMargin)
Reserved           = sum(NodeReservation)
MainHeadroom        = saturating_sub(AllocatablePool, Reserved + EmergencyPool)
StartupPool         = AllocatablePool * startup_factor
```

`NodeBudget` 是现役 status/wire 名称,其值为配置或探测到的物理资源总量;不能在公式
中再次把它当作已经扣除 `HostReserved` 的结果。

共享宿主上应通过 `resources.physical_memory` 和 `host_reserved` 明确本控制器获分配的
预算，并为其他服务留出资源。`physical_memory: auto` 仅在配置解析时读取宿主 `MemTotal`；
探测到整机容量不代表控制器独占整机。OperationalMargin 仍作为静态操作余量从 P 中扣除，
既不授予沙箱，也不作为整机可用内存的阈值。运行期节点闭环不采样宿主 `MemAvailable`，
不根据其他服务暂时闲置的内存推导自身资源池。Guest 观测及 sandbox-local Budget 闭环
保持独立，不作修改。

减法使用不下溢的资源运算。所有 reservation 更新与 aggregate 更新位于同一 State
临界区。插入或 recovery replacement 在修改索引前验证 aggregate 加法不会溢出。

### 4.2 RawZone 与有效 Zone

令 R=Reserved、P=AllocatablePool、E=floor(P*emergency_factor)，
Ty=floor(P*low_factor)、Tr=floor(P*high_factor)、Tc=P-E。
RawZone 在 R<Ty 时为 green，Ty<=R<Tr 为 yellow，Tr<=R<Tc 为 red，
R>=Tc 为 critical。零池在 preflight 拒绝，运行期防御性处理为 critical。

有效 Zone 是唯一节点水位权威。reservation 越界立即升级；合格持续内存不足也能从
 green/yellow/red 直接进入 critical。请求来源不参与规则：

| 有效水位 | 新建（含 Snapshot 模板） | 普通恢复 | 资源暂停恢复 | 资源 Pause |
|---|---|---|---|---|
| green | 有资格 | 有资格 | 有资格 | 不执行 |
| yellow | 有资格 | 有资格 | 有资格 | 不执行 |
| red | 不允许 | 有资格 | 有资格 | 不执行 |
| critical | 不允许 | 不允许 | 有资格 | 继续合格不足后执行 |

资格仍受完整 initial memory、启动协调、rate token 和既有认证/归属约束。
直接 API、Proxy、内部路径、节点命令共用规则。Cluster 的选址偏好属于 #46；
yellow 不是节点拒绝 cluster-create 的模式。NodeList 不新增动态水位或别名。

失败轮次按同一有效需求及 launch/token 身份在时间推进后复核。并发重复、认证/参数错误、
不可能满足的容量、drain、传输错误以及单纯启动槽/token 等待不计入。
缺省三轮进入 critical，再三轮授权一次 Pause；下一次 Pause 需要新轮次。
未增加 sandboxer 可执行 Budget 的 reservation grant，保留仍阻塞的需求及其下一可执行步长的额度保护。
需求有效期独立于轮询，为 30 秒、三倍失败间隔、两倍扫描间隔中的最大值。
因此来宾每五秒上报/重试的历史不会在两次请求之间被观察操作清空；观察本身不增加失败轮次。
过期后新请求重新累计，未使用的过期保护即使尚未扫描也不能阻塞准入；过期不清除 reservation
或恢复义务。本地 pressure 查询显示各需求的非敏感年龄、额度及失败/critical 轮数。
只有受控资源池内因内存不足而阻塞的有效需求才推进压力轮次。整机可用内存不产生
demand，也不授权资源 Pause；仅有 critical 水位、没有合格需求时不会自动 Pause。

Q 包含已受理的 capture intent、resource-pressure paused 及其 starting 恢复。
即使 RawZone green，只要 Q>0 仍保持 critical。退出同时要求 Q=0、相关未核实清理结束、
RawZone 低于 critical、无活跃合格内存阻塞，并连续满足 exit hold；只退出到 red。
red 要求 RawZone 连续低于 red 达到自身 hold 后到 yellow；yellow 要求 RawZone 连续
 green 达到自身 hold 后到 green。反弹重置计时，一次评估至多下降一级。
重启在准入前装载 journal/义务，单调 hold 时钟重新开始。
`host_operational_margin` 等历史转换原因仅保留为标签，不据此重建需求。策略变更不清空
既有 Q、清理屏障或 reservation；它们继续按原有恢复和逐级退出规则收敛。

### 4.3 runtime grant

现役 `RequestBudget` 是绝对 baseline 加 delta 的 reservation transaction:

```text
request:  CurrentReservation, RequestedDelta, Urgency
response: GrantedDelta, NewReservation, Cooldown

NewReservation = CurrentReservation + GrantedDelta
0 <= GrantedDelta <= RequestedDelta
```

普通新增 grow 按实际 P-R-E 再扣除其他受益者未兑现保护；high 可用 E，但总量不超过 P。有效 red/critical 本身不禁止 runtime grow。已占账 replay 不重复计费或扣 token。选中的一个 resume/grow 需求保护释放空间，直到 Admit、可执行进展、取消或未兑现 hold 到期；resume 优先于 grow。取消保护不释放 live reservation。

开始 capture 或提交 paused 行不代表已有可授予额度。只有安全 shrink 或确认 Release
才减少 R；释放出的额度可供合格 grow 使用，即使 Q 或 exit hold 仍让有效水位保持 critical。

sandbox 可收到 partial grant。sandboxer 会先累积 reservation，只有当额度足以表示一个
balloon target 按 64 MiB 对齐的更大 Budget 时才执行 balloon deflate，因此取整不会制造未保留内存。
Budget 边界相对于 Capacity：Capacity 为 1056 MiB 时，边界是 32/96/160/224/... MiB。
从 160 授予到 192 MiB 会保留需求并保护剩余 32 MiB；达到 224 MiB 才构成可执行进展并清除该需求。
Capacity 本身无需对齐。

shrink 使用同一消息,但 `RequestedDelta=0`:sandbox 在本地完成 balloon inflate、
current convergence 和 `memory.high` 顺序后,以较小的绝对 baseline 提交释放。node
不轮询 CH,也不判断 shrink 是否完成。

## 5. Reservation 协议

### 5.1 传输与认证

协议由 sandboxer `pkg/resource` 定义,使用 Unix socket 上的 length-prefixed JSON。
每个 sandbox 使用长连接和 token。controller restart/reconnect 使用 lifecycle lease、
`SO_PEERCRED`、managed pidfile/cgroup identity 和 `StateSync` 重建 session。

### 5.2 现役消息

| 消息 | 方向 | node 作用 |
|---|---|---|
| Admit | sandbox→node | 完整 initial reservation admission |
| Settled | sandbox→node | 只切换生命周期 stage |
| RequestBudget | sandbox→node | reconcile absolute baseline,可选 grow grant |
| OOMReport | sandbox→node | 诊断计数,不直接修改 reservation |
| Heartbeat | sandbox→node | liveness/host charge,返回 reservation echo |
| StateSync | sandbox→node | 以 sandbox 的安全绝对 baseline 替换 provisional state |
| Release | sandbox→node | 删除 reservation 和 aggregate charge |
| AdminDrain/Status/List | admin→node | admission drain 与观察 |

没有 node→sandbox 的 balloon/cgroup 命令,也没有 admin grant/reclaim。

### 5.3 现有 wire 字段语义

当前实现保持现役 reservation 报文形状,没有新增 Budget 协议。因而代码中的 wire 名称
按以下方式解释:

| wire/Go 名称 | 当前含义 |
|---|---|
| `FloorMemoryBytes` | settled `HeadroomMemoryBytes` |
| `StartupBudgetMemory` | 已按 target 对齐的 cold InitialBudget |
| `AllocatableAtSnapshot` | restore `BudgetAtSnapshot` |
| `GrantedInitialAlloc` | 完整 InitialBudget |
| `CurrentAlloc` | sandbox 的安全绝对 NodeReservation baseline |
| `NewAllocatable` | node 事务后的 NodeReservation/heartbeat echo |
| `AppliedAllocatableMemory` | StateSync 的安全 NodeReservation baseline |
| `CurrentRSS` | HostMemoryCurrent 诊断值,即 host VMM cgroup charge |

这些字段不表示 guest RSS、guest demand、balloon current 或 `memory.high` target。
保留报文形状是已确认的架构边界,不是新旧语义双路径;实现中没有 negotiation、版本
gate、alias decoder 或 mixed-version 分支;这不构成任意跨版本兼容性保证。

## 6. 生命周期

### 6.1 cold admission

```text
sandbox resolve H/Hs/C
  -> InitialBudget = aligned Hs
  -> Admit exact InitialBudget
  -> node atomically charges it or queue/reject
  -> sandbox starts CH with command-line balloon target
  -> launch_ack -> Settled
  -> fresh report drives sandbox-local steady policy
```

node 不能 partial-admit。首份 report 前不从 host `memory.current` 推导 Budget。
`Settled` 清除 startup-in-flight charge,但不改变 `NodeReservation`。

### 6.2 restore admission

```text
sandbox reads Capacity,target,current from snapshot
  -> BudgetAtSnapshot = Capacity - min(target,current)
  -> Admit exact BudgetAtSnapshot
  -> CH spawn/resume -> restore ACK -> MUX
  -> sandbox-local normalization/new epoch/steady loop
```

node 不消费 startup headroom,不读取 snapshot balloon state,也不在 ACK/MUX 前触发
调整。它只看到 sandbox 提交的 exact initial reservation。

### 6.3 runtime shrink/grow

grow 的跨边界顺序是:

```text
sandbox RequestBudget -> node charge/grant -> response
sandbox memory.high up -> balloon deflate
```

shrink 的跨边界顺序是:

```text
sandbox balloon inflate -> wait current -> memory.high down
sandbox RequestBudget(smaller baseline, delta=0) -> node release
```

node 与 sandbox 流程没有共享状态机;reservation 请求/响应是唯一协调边界。

### 6.4 资源 Pause、恢复与显式接管

单 capture worker 按当前连续运行时间最长优先，不使用 CreatedUnix；running 提交重置
起点。它复用现有内存 Pause：native exec quiesce/cleanup、Snapshot、旧 VM 退出和
runner/network/resources 清理。不引入保留 VMM 的 pause、page swapper、exec 保留或新
snapshot 格式。Checkpoint 必须落到磁盘，拒绝 tmpfs/ramfs；只有实际 Release/reconcile
才能复用额度。Capture 失败退避，不丢弃已有保存源。

单后台恢复 worker 按最老义务服务，与外部 Wake 共用 SID launch group。在 worker 启动及其
自身 capture/recovery 完成后，用一个有界间隔控制准备恢复的节奏。无关沙箱的 reservation
变化不重置该间隔；繁忙节点不能要求全节点停止分配，才恢复无人访问的资源暂停沙箱。
`headroom_stable` 仅用于诊断，不是恢复准入前提。随后 sandboxer 给出精确 I，最终资源准入
仍等待完整预算才启动 VM；节奏计时不授予内存。
Starting 在实际 running 提交前仍属于 Q。最短运行窗口抑制刚恢复就换出；如果所有
其他条件合格的对象仍在窗口内，选择过程等待窗口到期，不根据整机可用内存绕过配置的
保护。有效需求可在 critical 中协调进一步 Pause，不等待被 Q 自身阻挡的
降级。制品缺失/损坏、磁盘满/慢和容量失败保留源与义务并进入操作诊断。既有 Deadline
终结、显式 Delete 通过同一生命周期所有者解除意图。

已资源暂停的沙箱收到授权的缺省内存 Pause 时，直接接管现有 Snapshot 为普通显式暂停。
Hook 仍执行，重取 lifecycle lock 后复核完整记录前置条件。窄 durable CAS 只改原因、义务、
版本，保留身份、保存源、凭据和未完成清理；不重做 capture、不启动 VM。一次性取消未兑现
资源恢复 hold 和 critical 豁免，State 仍为 paused 也发布更新。显式 capture 选项（包括显式
 false）及 filesystem-only capture 均拒绝：保留源无法证明新的 capture 动作已经满足。
Hook 也不能悄悄移除该动作后接管。普通重复 Pause 仍 409，starting 仍冲突。
后续真实 Wake 按普通恢复规则执行，不增加永久用户暂停锁。接管不是 Release，也不能跳过
critical 的完整退出条件。

物理释放检查区分旧 VMM 的共享内存占用与 Snapshot 文件缓存。旧进程退出后，磁盘上的
Snapshot 页面仍可能驻留缓存或处于 dirty 状态，`MemFree` 不保证按旧占用等量增加。
隔离验证时分别观察旧进程/cgroup 身份、已确认的 reservation 释放、宿主 `MemAvailable`
与存储回写。这些整机测量仅用于测试诊断，不是节点控制输入。节点不将 Snapshot 文件
长度或预测的来宾工作集当作已释放余量。

Sandbox 字段与小型 transition journal 共用现有 SQLite owner，在服务前结合 inventory
重建。资源热路径不在 State.mu 内 capture、访问文件/网络或 SQLite；admission queue 先于
State，State 不回调生命周期。只持久化低频生命周期/水位转换，不在 grant/heartbeat 重写
全状态。节点 routesync、SHM、extension 投影只传递原因、义务、版本和运行起点等事实。

## 7. Admission 与调度投影

### 7.1 admission

Cold 使用 StartupBudgetMemory；内存 restore 使用现役 AllocatableAtSnapshot 字段，
值仍为 sandboxer 的权威 BudgetAtSnapshot。RSS、压缩大小、guest 峰值、预计业务完成都
不能替代它。Initial memory 必须在与 aggregate/grow 相同的 State 临界区内完整兑现。

Orchestrator 从最终 durable source 和解析后的 launch mode 分类；进程内 lookup 将
resource 连接 peer PID 与当前 runner pidfile 核对。RPC 字段、metadata、模板 kind、
urgency、Origin 均不能赋予恢复特权。Snapshot 模板创建仍是 Create。已 durable starting
的操作在水位改变后保留同一 accepted 身份和异步生命周期；后续资源等待不伪装为无副作用拒绝。

普通新建要求 I<=StartupPool、P-R-E 足够、启动预算和 rate token 可用。
合法内存恢复使用串行完整预算通道：无其他启动占用，实际 I 不截断记账，到 Settled 或
确认 Release 前不并发新启动。允许 I>StartupPool，必要时允许 I>P-E，但扣除其他保护后
必须 R+I<=P。宿主/操作保留区在 P 之外，永不授予。完整保存源能力在显式 Pause 接管后仍
保留；critical 豁免只属于当前资源恢复义务。I>P 明确失败并保留源。

有界 admission queue 按 FIFO 年龄逐项复核，不被新近不再合格的 Create 或内存不足的
队首阻挡。每次重查当前 launch 身份，超时/迟到工作只能取消同一身份的未兑现保护。
启动槽/token 等待保留 resume 保护，但不累计内存失败轮次。Replay/StateSync 原子替换
已有占账；断连、TTL 或不明清理不能把已占账消费者变成可用空间。


显式 cold 恢复按普通恢复水位规则，不享有资源内存恢复的 critical 豁免或完整保存内存预算例外。
可信 launch lookup 同时核对当前 runner pidfile 内容及同一已打开文件上的活跃 POSIX 锁持有者。

### 7.2 ResourceProbe 与 cluster load

`ResourceProbe.Allocated` 和 cluster projected memory load 使用
`reservedMemory`,不是 host charge。E2B `memoryMB` 继续表示 Capacity/SKU。

per-sandbox resource stats 读取当前 sandboxer lifecycle owner,独立于 controller heartbeat/reservation 路径:

- `cpuCapacity`: 生效 capacity,单位为核;`cpuAllocatable`: 映射到 `cpu.weight` 的相对调度规格,不表示 fractional-core 硬 quota 或性能保证.
- `memoryCapacity`: 生效 Capacity 字节数;`memoryHeadroom`: 最终 `resources.allocatable.memory`,表示气球 headroom,不是 Budget、guest free memory 或 NodeReservation.
- `memoryReserved`: 动态 controller 有观测时的当前 NodeReservation,否则省略.
- `memoryUsed`: 宿主 VMM `memory.current`;`cpuSeconds`: 同一个 cgroup 的 `cpu.stat.usage_usec / 1e6`. 不额外增加 ctl/guest CPU,不扣 inactive-file/balloon,不按 guest capacity 截断.
- `timestampUnix`: 实际观测时间. 缺失的宿主字段分别省略,合法零值仍可见. 来源重建时 CPU seconds 可以重置;native usage 负责生命周期累计.

静态/动态模式以及 usage 或 telemetry 关闭时均可读取. 读取不修改 RequestBudget、admission、recovery charge、heartbeat 采样和已有 build-phase reservation 释放 fence. 原生 API 删除 `cpuCount`/`memTotal`/`memAllocatable`/`memUsed`,reservation 和 headroom 使用各自明确名称. [Node §4.1.1](node_zh.md#411-即时-resource--traffic-stats) 定义响应有效性、错误和数值精度. E2B 兼容 metrics 保持自己的字段名和含义.

## 8. 可靠性

### 8.1 inventory 与 controller restart

lifecycle lease 保存 sandbox identity、Capacity、headroom、cold startup headroom、cgroup
identity 和 feature 信息。controller 启动扫描 lease、managed pidfile 与 configured
cgroup roots:

1. 已知 lease 按 Capacity provisional charge。
2. identity 不完整的 live cgroup 按可证明的保守上界 charge。
3. sandbox 重新连接后发送 StateSync。
4. State 在一个临界区中删除 provisional charge 并插入 sandbox 上报的
   NodeReservation。

controller 不读取旧持久化 state schema,也不从 memory.current 重建 Guest Budget。
恢复的只读检查包括 cgroup.events、cgroup.procs、进程 cgroup identity 与 memory.max,
用于存活/保守记账,不是第二个 Guest 内存控制器。

### 8.2 response loss 与 reconnect

- grow 响应丢失时,sandbox 尚未据此执行 high/deflate,仍保留本地旧 baseline;
  StateSync 原子替换 node provisional charge,不会少记。
- shrink commit 响应丢失时,balloon current 已收敛且 `memory.high` 已降低;
  sandbox 因而使用已完成的较小 baseline。node 已提交时双方相等,node 未提交时
  StateSync 完成释放。继续使用旧的大 baseline 反而可能让 sandbox 复用 node
  已释放并重新分配的 headroom。
- Heartbeat mismatch 触发 reconnect/StateSync,不会把 echo 当作执行命令。
- stale heartbeat 和 host charge 只能更新诊断时间/数值,不能改变 reservation。

### 8.3 reservation 生命周期

startup TTL 只清理未达到 Settled 的失败创建。heartbeat timeout、release 和 inventory
清理均按 reservation identity/token 原子删除索引与 aggregate。未知或 provisional
reservation 不接受普通 grow transaction。

## 9. 性能与可观测性

admission token bucket 限制创建请求洪峰;startup pool 限制创建/恢复中的内存总额;
runtime grant token bucket 限制普通 grow 的节点总速率。high urgency 可使用 emergency
pool。allocator 可返回 cooldown,由 sandbox 后续 observation/pressure event 重试。

当前 reservation 与恢复状态通过 `resource status`、`resource list`、本地 `resource pressure` 和既有节点状态生产端观察。ResourceProbe 返回有效 Zone 与同一 reservation R/P；消费者不能由 R/P 重新推导有效水位。异常 queue 诊断进入 conductor 标准日志出口,由部署环境统一采集与保留。重点口径包括:

- reserved memory / pool / zone。
- startup in-flight。
- provisional/unknown reservation 数量。
- per-sandbox HostMemoryCurrent 及最后上报时间。
- grant/cooldown 和 admission queue 时延。

这些指标不能混成一个“内存使用量”:reservation、host VMM charge 和 guest demand
分别属于不同口径。

生命周期累计用量通过[原生 stats/usage](node_zh.md#原生-usage)读取, 节点策略下发到全部三条启动路径. 当前 VMM counter、生命周期累计量与历史 E2B Gauge 保持各自语义.

## 10. See Also

- [node](node_zh.md)
- [sandboxer sandbox](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md)
- [cluster](cluster_zh.md)
- `sandboxer/pkg/resource`
- `internal/nodectl`
- `internal/sandboxcfg/resource.go`
