[English](node-resource.md) | [简体中文](node-resource_zh.md)

# node-resource — 节点 reservation 控制器

`node-ctl conductor serve` 可选地内置 node resource controller。它只处理节点侧
reservation、admission、pool、水位、恢复 inventory 和统计投影。sandbox 的
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

### 1.2 术语

| 名称 | 定义 | 所有者 |
|---|---|---|
| Capacity | VM 固定最大内存 | sandbox config/snapshot |
| Headroom | `resources.allocatable.memory`,settled guest headroom | sandbox policy |
| StartupHeadroom | `resources.startup.memory`,cold 首份可信 report 前的 headroom | sandbox policy |
| BudgetAtSnapshot | `Capacity - min(snapshot target,snapshot current)` | sandbox snapshot |
| NodeReservation | node 已为某 sandbox 保留的绝对内存额度 | node controller |
| HostMemoryCurrent | host VMM cgroup `memory.current`,仅诊断 | sandbox 上报,node 记录 |
| reservedMemory | 所有 live `NodeReservation` 的和 | node state |

`Headroom` 不是 total Budget。CPU `allocatable` 仍表示调度权重/保证,与 memory
headroom 不完全同构。

sandbox 内部另有 `TargetBudget`、`CurrentBudget`、`ObservedBudget` 和
`DemandMemory`;node 不需要也不保存这些状态。正常受控路径中 sandbox 先取得足够
`NodeReservation` 才扩大 Budget,并在 shrink 收敛后才释放 reservation。
`deflate_on_oom` 的 guest 应急 deflate 是阶段化软保证的例外:它不改变 target 或
reservation,原有 `memory.high` 继续限制 host VMM charge,sandbox 把 target/current
标记为 unstable 并禁止 shrink。snapshot 仍按两侧安全上界计算
`BudgetAtSnapshot`。

### 1.3 核心不变量

- `0 < NodeReservation <= Capacity`。
- `reservedMemory = sum(live NodeReservation)`。
- admission、pool、zone、ResourceProbe、cluster projected load 和 recovery
  replacement 只使用 `NodeReservation` 聚合。
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
```

`status` 展示 node budget、host reserved、operational margin、allocatable pool、
reserved memory、startup in-flight、zone 和 recovery 数量。`list` 输出逐 sandbox
reservation。`drain` 只禁止新 admission,不改变任何 live reservation。

不存在 `resource grant` 或 `resource reclaim`。Headroom 属于 sandbox policy,应通过受支持的 sandbox 配置/生命周期输入选择,
再由 sandbox 既有闭环根据 observation 收敛。这不引入 node 侧 live policy reload
或直接 balloon/cgroup 调整命令。

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
```

`operational_margin_factor` 从扣除 HostReserved 后的预算中保留 node safety margin。
`emergency_factor` 为 high-urgency grow 保留 pool。`startup_factor` 限制创建/恢复中
reservation 的并发总量。admission `rate`/`burst` 是请求 token bucket,不是内存
Budget。

controller preflight 要求 `host_reserved.memory < physical_memory`,并验证
`0 <= operational_margin_factor < 1`、`0 <= low_factor < high_factor < 1`、
`0 <= emergency_factor < startup_factor <= 1` 和
`0 < memory_grant_per_sec_factor <= 1`。非法值在计算 pool 前失败,不会进入无符号减法
或 cluster load 投影。

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

减法使用不下溢的资源运算。所有 reservation 更新与 aggregate 更新位于同一 State
临界区。插入或 recovery replacement 在修改索引前验证 aggregate 加法不会溢出。

### 4.2 zone

zone 仅由 `Reserved / AllocatablePool` 推导:

| zone | 含义 |
|---|---|
| green | 正常 admission 和 grow |
| yellow | 保守运行,仍可按 policy grant |
| red | 拒绝新 admission;非 high urgency grow 暂缓 |
| critical | 只保留安全/高紧急请求路径 |

zone 不读取 `MemAvailable`、balloon current、`memory.current` 或 sandbox lifecycle
细节。

### 4.3 runtime grant

现役 `RequestBudget` 是绝对 baseline 加 delta 的 reservation transaction:

```text
request:  CurrentReservation, RequestedDelta, Urgency
response: GrantedDelta, NewReservation, Cooldown

NewReservation = CurrentReservation + GrantedDelta
0 <= GrantedDelta <= RequestedDelta
```

sandbox 可收到 partial grant。sandboxer 会先累积 reservation,只有当额度足以表示一个
64MiB 对齐 Budget 时才执行 balloon deflate,因此取整不会制造未保留内存。

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

## 7. Admission 与调度投影

### 7.1 admission

Cold 使用 `StartupBudgetMemory`,restore 使用 `AllocatableAtSnapshot`。选择后的
InitialBudget 必须同时满足:

- 不超过 sandbox Capacity。
- 不超过 `AllocatablePool` 和 startup pool 的单请求上界。
- 当前 main headroom 和 startup headroom 足够。
- node 未 drain,zone 未进入 red/critical。
- admission token bucket 可用。

短期不足进入 FIFO queue;请求本身超过节点上界或 node protection 条件不允许时拒绝。
任何路径都不会用较小 initial grant 继续启动/恢复。

### 7.2 ResourceProbe 与 cluster load

`ResourceProbe.Allocated` 和 cluster projected memory load 使用
`reservedMemory`,不是 host charge。E2B `memoryMB` 继续表示 Capacity/SKU。

per-sandbox resource stats 中:

- `memTotal` 是 Capacity。
- `memAllocatable` 是现有 API 名称,值为 NodeReservation。
- `memUsed` 是 HostMemoryCurrent/VMM cgroup charge。

`memUsed` 不是 DemandMemory,也不参与 RequestBudget、admission 或 recovery charge。

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

当前 reservation 与恢复状态通过 `resource status`、`resource list` 和 cluster heartbeat
观察。异常 queue 诊断进入 conductor 标准日志出口,由部署环境统一采集与保留。重点口径包括:

- reserved memory / pool / zone。
- startup in-flight。
- provisional/unknown reservation 数量。
- per-sandbox HostMemoryCurrent 及最后上报时间。
- grant/cooldown 和 admission queue 时延。

这些指标不能混成一个“内存使用量”:reservation、host VMM charge 和 guest demand
分别属于不同口径。

## 10. See Also

- [node](node_zh.md)
- [sandboxer sandbox](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md)
- [cluster](cluster_zh.md)
- `sandboxer/pkg/resource`
- `internal/nodectl`
- `internal/sandboxcfg/resource.go`
