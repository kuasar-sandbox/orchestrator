[English](node-resource.md) | [简体中文](node-resource_zh.md)

<a id="overview"></a>

# 节点资源与准入

`node-ctl conductor serve` 可选地内置 node resource controller。它只处理节点侧
reservation、admission、pool、水位、恢复 inventory、压力 episode 和统计投影。压力 worker 通过 orchestrator 协调现有完整内存 Pause/Resume。sandbox 的
guest memory observation、Cloud Hypervisor balloon、`memory.high`、cold/restore/
snapshot 生命周期均由 sandboxer 自闭环管理,不属于 node controller。

先读单位与配置，再读请求 patch。capacity、allocatable memory、startup target 与 host reservation 各有不同的职责；后续模型、线协议及恢复部分维护扩展和诊断 controller 所需的完整实现契约。

**阅读路径：** [术语](#units-and-terms) · [配置](#resource-policy) · [请求资源](#request-resources) · [资源池](#resource-pools) · [模型](#resource-model) · [协议](#controller-protocol) · [准入](#admission-and-pressure) · [恢复](#recovery)

<a id="units-and-terms"></a>

## 术语

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

<a id="resource-command"></a>

## 命令行接口

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

<a id="resource-policy"></a>

## 配置

<a id="sandbox-resource-policy"></a>

### sandbox resource policy

```yaml
sandbox:
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable:
      memory: 256MiB
    startup: { memory: 2GiB }
    overhead: { memory: 32MiB }
    watermark_high: { ratio: 0.875 }
    diff_cow: { cache_size: 64MiB }
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

restore 的 Capacity 来自 Snapshot S 引用的配对 Sandbox E 的 portable 配置。`startup.memory` 不参与 restore admission;
sandboxer 上报的 `BudgetAtSnapshot` 是唯一 initial reservation。

restore 的同步请求只校验 portable patch 结构。runner 绑定 exact run-id 后,task 在进程内
读取 Snapshot S 并沿 `sandbox_ref` 读取配对 E。snapshot reader 本身就会打开 E 以派生配置视图，
prepare 从 E 的 portable 配置取得 Capacity。该 metadata/config 准备可打开所需 sibling/ref
载体中的 E，并从平面 `bundle/refs` 元数据补全 located 来源 mapping，但不递归扫描无关 Bundle。
task 把 Capacity 作为非秘密 summary
交给唯一 launch worker;conductor 不打开制品。request 显式 Capacity 可作为 assertion,不一致或来源制品读取失败成为
异步 `resource_resolve` failure。失败发生在 network Attach、sandbox YAML、controller Admit
和 VM 启动前,并且绝不回退 node default。`BudgetAtSnapshot` 仍由 sandboxer 从 CH
snapshot target/current 计算,不由 orchestrator probe 或推导。

公共配置 `ResourceAllocatable.Memory` 以 `*string` 保留存在性：nil 继承内部 resolver 的
`256MiB` 默认值；非 nil（包括显式 `256MiB`）属于 operator/custom Configure 策略，
必须适配最终 Capacity。`SetMemory`/`InheritMemory` 是便捷方法，直接赋指针具有相同
语义。Clone 与 component bootstrap JSON 均保留这种存在性。

<a id="resource_listen"></a>

### resource_listen

以下示例使用隐式 `default` 单池和旧物理资源公式。独立净预算应采用显式
[资源池配置](#pool-configuration)，多池不能同时携带下例中的物理资源或
operational margin 字段。

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
`resource_listen.state_path` 已弃用且被忽略,不启用 state.json 恢复([inventory 与 controller restart](#inventory-and-controller-restart))。

<a id="requesttemplate-ownership"></a>

### request/template 所有权

同一 Sandbox 的可写盘共享一份宿主机 DiffCOW 缓存。`cache_size` 缺省为 32 MiB，省略的 `max_dirty_size` 按最终缓存的一半向下对齐到 4 KiB，至少为 4 KiB。上例得到 64 MiB 缓存与 32 MiB 脏数据上限。显式大小须为 4 KiB 的正整数倍，脏数据上限不得超过缓存。

tenant resource patch 支持 `capacity`、`allocatable`、`startup`，以及 `diff_cow` 的缓存限制与冷启动 discard 策略。`control`、
`overhead`、`watermark_high`、sensor 和 `deflate_on_oom` 均由 node resolver 管理。
parser 对未知字段和这些越权字段直接报错,不会静默忽略。

<a id="conductor-resource-configuration"></a>

## Conductor 资源配置

以下字段属于 Conductor 配置；共享加载、路径与环境变量规则见[节点运行](node_zh.md#conductor-configuration)。

| 字段 | 默认 | 说明 |
|---|---|---|
| `sandbox.resources.capacity.cpu` / `.memory` | `2` / `2GiB` | guest 可见的 VM 上限/SKU;E2B `cpuCount`/`memoryMB` 继续表示 Capacity。img 冷启可由 create/group 覆盖;restore Capacity 由 snapshot 固定 |
| `sandbox.resources.allocatable.cpu` / `.memory` | 最终 capacity CPU / 省略时继承 `256MiB` | CPU 是相对调度权重，不是硬性小数核保证；memory 是 settled guest headroom,不是 total Budget。conductor 配置省略 memory 时可随最终 Capacity 收敛；operator/custom 显式值（即使等于 `256MiB`）不得静默收敛，越界直接拒绝。request patch 的 pointer schema 与规则不变 |
| `sandbox.resources.startup.memory` | 最终 `capacity.memory` | cold 首份可信 report 前的 headroom,static/dynamic 均有效,与 settled headroom 独立;restore 不使用 |
| `sandbox.resources.overhead.memory` | `32MiB` | node-owned host VMM overhead;sandbox-ctl 不在 VMM cgroup 内;request/template 不可覆盖 |
| `sandbox.resources.watermark_high.ratio` | `0.875` | node-owned sandbox `memory.high` pressure ratio,必须在 `(0,1)`;request/template 不可覆盖 |
| `sandbox.resources.diff_cow.cache_size` | `32MiB` | 同一 Sandbox 的可写磁盘共享的宿主机缓存 |
| `sandbox.resources.diff_cow.max_dirty_size` | 最终缓存的一半，向下对齐到 4 KiB，至少 4 KiB | 显式脏数据上限不得超过缓存 |
| `sandbox.resources.diff_cow.discard` | `auto` | 可写 ext4 的冷启动挂载策略：`auto` 使用 `discard` 挂载；`manual` 使用 `nodiscard`，同时保留显式 trim 能力 |
| `resource_listen` | 缺省(不内置) | controller endpoint 的唯一配置源:`socket` 解析为 bind 用的绝对 `Listen` 与 owner/inventory/lease/sandbox.yaml 使用的 canonical `SocketIdentity`;sandbox client 经 canonical path 连接同一 socket inode。`enabled` 开关及其余调参见 node-resource.md [resource_listen](#resource_listen)。省略或 disabled = 静态 cgroup |

<a id="request-resources"></a>

## 请求资源 schema 与优先级

`resource` 与 `traffic` 按 leaf 合并,而不是整段 namespace 覆盖。`resource` 的公开 JSON 只允许:

```json
{
  "capacity": {"cpu": 2, "memory": "8GiB"},
  "allocatable": {"cpu": 0.5, "memory": "256MiB"},
  "startup": {"memory": "1GiB"},
  "diff_cow": {"cache_size": "64MiB", "max_dirty_size": "16MiB", "discard": "auto"}
}
```

整个值必须是单个 JSON object。unknown、`null`、数组/scalar、trailing JSON、显式
零/负 CPU、空/非法/非正 memory 均返回 400,错误带完整
`kuasar-sandbox.resource.<path>`。request/template/group/header/migration token 都不能
携带 `allocatable.deflate_on_oom`、`overhead`、`watermark_high`、`control` 或
`sensor`:这些字段属于 node/runtime,其中 `watermark_high.ratio` 由 node policy 注入。
合法 patch 持久化为 compact canonical JSON。

固定的 leaf priority 是:

```text
node resource policy
  < cluster group defaults
  < create / reserve body resource
  < X-Kuasar-Sandbox-Resource
  < E2B first-class cpuCount / memoryMB (只覆盖 capacity 对应 leaf)
  < portable artifact capacity constraint
```

各个叶子独立合并。group 只设 `capacity.memory`、create 只设
`allocatable.memory` 时两者同时保留。Build row metadata 不是 Create overlay 层。其它
namespace 遵循[Sandbox 配置透传](sandbox-lifecycle_zh.md#request-configuration)中的各自规则，多数采用 whole-namespace 覆盖。每一层都先
严格解析,所以合法高层不能隐藏非法低层。group/reserve、standalone/cluster 与 build
registration 共用同一 helper。

例如节点省略脏数据上限时，请求头 `X-Kuasar-Sandbox-Resource: {"diff_cow":{"cache_size":"64MiB"}}` 会得到 32 MiB 的脏数据上限。在 `diff_cow` 内提供 `"max_dirty_size":"16MiB"` 可显式设置上限。两项大小均须为 4 KiB 的正整数倍。原始策略、请求 metadata 和迁移 token 只保存显式叶子，恢复时由目标节点重新解析缓存限制，独立于制品 capacity。Build 执行继承节点策略，目标 Sandbox 再叠加其请求配置。

若希望新冷启动延后文件系统 discard，可在节点设置
`sandbox.resources.diff_cow.discard: manual`，或为实例发送
`X-Kuasar-Sandbox-Resource: {"diff_cow":{"discard":"manual"}}`。
`auto` 保持默认的自动 discard 挂载；`manual` 对可写 ext4 根盘与数据盘（包括
overlay upper 文件系统）明确使用 `nodiscard`。值只能是精确的 `auto` 或 `manual`。
两种模式均保留块设备的 DISCARD 能力。manual 不安排 trim 任务；显式 trim 需要访问
底层 ext4 文件系统，不能直接针对可见的 overlayfs 挂载。块在 trim 或复用前，已删除
payload 仍可能占用本地空间并进入新导出。挂载范围与 runtime 兼容要求见
[sandboxer 挂载策略契约](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/configuration_zh.md#diff-cow-policy)：
manual 冷启动需要 Guest runtime 声明相应能力，旧 runtime 会明确报错。

该叶子用于 image 启动和从 Sandbox E 冷启动。从 Snapshot S 恢复内存时保留捕获的挂载：
renderer 在 restore host 文档中省略 `discard`，同时保留两项缓存限制。修改节点策略或
保留的资源 patch 不会重新挂载恢复后的 Guest。请求 metadata 和迁移 token 为后续冷启动
保留显式请求的模式；节点默认策略来自执行该次冷启动的节点。该叶子不会作为资源字段
进入 Portable E。运行时缓存预算及其所有权不变。

<a id="resource-resolution"></a>

### 解析 capacity、startup 与策略

最终 resolver 先确定 capacity,再解析 allocatable/startup,最后添加 node-only
overhead/watermark/deflate/controller。node policy 中省略的 allocatable.memory 超过最终
capacity 可安全收敛；node policy 或 request 的显式值越界拒绝。startup 与 allocatable 独立,request 显式值只需位于
`(0,capacity.memory]`;node 显式 startup 超过最终 capacity 时收敛到 capacity,双方都未
显式设置则取最终 capacity。static/dynamic 都渲染 startup,static 只省略 controller。
settled policy 需要 balloon(`capacity.memory > allocatable.memory`)时最终 YAML 显式
写入 `deflate_on_oom: true`;仅 cold startup target 非零时也会创建 balloon device,
并使用 sandboxer 的同一 effective true 缺省。

<a id="resource-pools"></a>

## 资源池与放置

资源池扩展现有进程内 resource controller。每个 runner/builder 池固定关联一个资源池；
一个资源池可以关联多个逻辑池，并允许这些池共享 unit 模板。同一模板不能跨资源池使用。
所有池沿用共享 `paths.base_root`，没有新增存储池。

<a id="pool-configuration"></a>

### 配置与容量

两层 pool 使用不同配置和记账口径：

| 配置 | 默认/存在性 | 契约 |
| --- | --- | --- |
| `resource_listen.pools[].name` | 必填且唯一 | 非空，匹配 `[a-zA-Z0-9][a-zA-Z0-9_.-]*`；标识一个资源预算 |
| `resource_listen.pools[].capacity.{cpu,memory}` | 缺省维度无限 | 正值的声明 Capacity 调度上限；CPU 支持小数核，memory 为正大小 |
| `resource_listen.pools[].allocatable.memory` | 多池必填 | 正值净运行内存；单池可沿用旧推导公式 |
| `resource_listen.pools[].allocatable.cpu` | 继承 capacity CPU；两者均省略则无有限分母 | CPU 使用率/水位记账分母，不新增运行 CPU 硬准入 |
| `resource_listen.pools[].watermarks.{cpu,memory}` | 按叶子继承 | low/high/emergency factor 可分别覆盖节点策略；其他维度拒绝 |
| `resource_listen.pools[].unit_dropins` | 无 | 外部 unit 放置片段的绝对路径，复制到托管命名空间 |
| `units.{runner,builder}_pools[].name` | 从零起 `runner-N` / `builder-N` | 同种类内唯一；重排前应使用稳定显式名称 |
| `units.{runner,builder}_pools[].resource_pool` | `default` | 必须引用已有资源池；共用模板不能跨资源池 |
| `units.{runner,builder}_pools[].unit` / `size` | 显式项必填 | 模板与非负 idle 目标；共享安装和旧字段规则见 [Node 配置](node_zh.md#conductor-configuration) |
| `units.{runner,builder}_pools[].capacity` | 缺省无限 | 非负实例/启动预留总上限，要求 `size <= capacity`；零不允许新增供给 |
| `units.{runner,builder}_pools[].priority` | `0` | 所选资源池内数值越高越优先，同级随机 |

显式空/null `resource_listen.pools`、重复/非法名称、不存在的池引用，以及模板跨资源池
均拒绝。显式 CPU/内存额度须有限、为正且可表示。各维度阈值满足
`0 <= low < high < 1-emergency <= 1`；最终 resolver 还检查正净内存、按字节取整的
阈值及非零增长速率。

以下片段假设两个实际带内存的 NUMA 节点。按主机调整预算、CPU 集合和路径；
allocatable.memory 是净运行预算。

```yaml
resource_listen:
  enabled: true
  pools:
    - name: numa0
      capacity: {cpu: 32, memory: 128GiB}
      allocatable: {cpu: 8, memory: 24GiB}
      watermarks:
        cpu: {low_factor: 0.60, high_factor: 0.80}
      unit_dropins: [/etc/kuasar/pools/numa0.conf]
    - name: numa1
      capacity: {cpu: 32, memory: 128GiB}
      allocatable: {cpu: 8, memory: 24GiB}
      unit_dropins: [/etc/kuasar/pools/numa1.conf]
units:
  install: true
  runner_pools:
    - {name: interactive0, resource_pool: numa0, unit: runner-numa0@.service, size: 2, capacity: 32, priority: 100}
    - {name: batch0, resource_pool: numa0, unit: runner-numa0@.service, size: 0, capacity: 16}
    - {name: interactive1, resource_pool: numa1, unit: runner-numa1@.service, size: 2, capacity: 32, priority: 100}
  builder_pools:
    - {name: build0, resource_pool: numa0, unit: builder-numa0@.service, size: 0, capacity: 4}
    - {name: build1, resource_pool: numa1, unit: builder-numa1@.service, size: 0, capacity: 4}
```

`resource_listen.pools[]` 包含 `name`、`capacity`、`allocatable`、`watermarks`、
`unit_dropins`。`units.runner_pools[]` 和 `units.builder_pools[]` 均支持
`name/resource_pool/unit/size/capacity/priority`。

- 调度 `capacity` 按沙箱声明 Capacity 扣账，包含已接受的启动预留；未配置维度无限。
- 运行 `allocatable.memory` 是净预算，按实际 reservation（含启动 headroom）扣账。
  CPU 按沙箱 allocatable.CPU 计量，缺省使用 Capacity.CPU，不新增运行 CPU 硬准入。
  CPU 不在启动/Settled 阶段切换；StartupPool 和沙箱 startup 只处理内存。
- 运行 CPU 上限缺省继承调度 capacity.CPU，两者均缺省时使用率为 0，绝对用量仍可查询。
- `watermarks.memory` / `watermarks.cpu` 支持分别覆盖 low/high/emergency_factor；
  未覆盖项继承全局 watermarks。池使用率和原始水位取各维度最大值。
- 节点任一池 critical 则节点 critical，否则报告最低池水位。本机准入检查目标池，
  Create 允许 green/yellow，普通 Resume 还允许 red，资源 Recovery 允许全部水位。
- runner/builder 的 `size` 仍是 idle 预热目标；可选 `capacity` 限制 starting、idle、
  bound、stopping、重启恢复实例及有效启动预留总量，缺省无限，0 禁止新增供给。
  停止失败不会提前释放槽位。资源池内 priority 越高越优先，同级随机。

调度先过滤声明容量、操作权限和 runner 供给，再选择运行使用率最低的资源池，同使用率随机。
Builder 原有业务配额独立保留：启动前预留潜在串行阶段的最大容量；实际阶段开始后，
以该阶段 VM 的 Capacity 替换预留。下一阶段必须等待上一阶段的运行资源释放。

<a id="pool-growth-limits"></a>

### 增长限速与数值校验

普通/低优先级内存 grow 在各资源池独立限速。每秒额度 `G` 直接取该池有效运行内存乘
`rate_limits.memory_grant_per_sec_factor` 并向下取整；取整为零的配置拒绝启动。
桶容量为 `max(G, 4 MiB)`，初始 tokens 为 `G`，每次授额同时受桶容量、请求尾额、
实际 headroom 和既有 512 MiB 上限约束。小速率可以积累到最小部分授额，完整小尾额
仍精确授予。纯令牌等待不构成内存压力，high 保留既有应急语义。

请求级 admission 令牌桶仍为节点级。`admission.rate/burst` 缺省或显式零沿用默认值，
负值由内置 conductor、公共 App 和 `config conductor --resolve` 共用的最终 resolver 拒绝。
旧 `physical_cpu` 仅接受 `auto` 或可安全转换为 millicores 的完整十进制整数核数；
`host_reserved.cpu` 接受有限非负小数，保留向下截断和显式零默认值语义。
新池 CPU 字段仍接受小数并向上取整到 millicores，缺省调度容量仍为无限。

<a id="pool-systemd-placement"></a>

### Systemd 放置

`unit_dropins` 使用外部文件绝对路径。Conductor 将其复制到关联 runner/builder 模板的
`.service.d/50-kuasar-resource-pool-NNNN.conf`，统一 reload。它只清理该托管命名空间的
过期文件，保留其他管理员文件。请勿手工占用此命名空间。
`units.install: false` 时不读取引用文件、不安装也不 reload，继续由外部管理。

检查 `lscpu -e` 与 `/sys/devices/system/node/node0/cpulist` 后，节点 0 的文件可采用：

```ini
[Service]
CPUAffinity=
CPUAffinity=0-7
AllowedCPUs=
AllowedCPUs=0-7
NUMAPolicy=bind
NUMAMask=
NUMAMask=0
AllowedMemoryNodes=
AllowedMemoryNodes=0
```

使用主机实际拓扑配置 CPUAffinity、AllowedCPUs、NUMAPolicy、NUMAMask 和
AllowedMemoryNodes，并确认祖先 cgroup 允许对应集合。配置在预热前作用于模板；
reload 不会搬迁已启动进程。Builder 外层不增加 CPUQuota/MemoryMax/MemoryHigh，
沙箱资源约束仍在 sandbox-ctl 的 VMM cgroup 内执行。

<a id="pool-lifecycle-and-pressure"></a>

### 生命周期、压力与主动均衡

沙箱持有运行资源期间不改变池归属。Pause 确认旧执行退出并释放后，Resume 可以重新选池。
制品准备可能早于获知权威 Capacity；需要换池时，先停止准备实例，再解除绑定，
由目标池的新实例重新准备并比较结果，最后发布运行配置。准备实例不会启动 VM。

连续真实内存需求失败升级、保护额度、最短运行时间和退避均在资源池内生效。
一轮暂停扫描按当前连续运行时间从老到新尝试 `Pause(ifIdle=true)`；明确 busy 才继续
寻找 idle 候选。没有 idle 成功时，普通 Pause 回退到最老的合格 busy 候选。
Proxy 错误、状态改变和管理冲突不是 busy。握手接受后才消耗压力暂停资格，
capture 前必须事务提交持久恢复义务和源/目的池记录。

定时主动均衡只从原始水位 critical 的池发起，并要求存在可行的非 critical 目的池。
其持久记录在恢复时排除源池，重启后也生效；目的池失效时可继续 paused。
原始压力已经下降、仅因未完成恢复义务维持 critical 的池不会被继续换空。
全部池 critical 时只停止新增均衡，普通压力处理及真实预算允许的恢复继续。

未完成义务只阻止已 critical 的池降级，不把健康池提升到 critical。
后台恢复检查完整预算、实际空闲内存、目标池竞争和保护需求以及启动并发；
不再要求恢复后水位低于 red，也不会为自身等待预算触发新的换出。
没有预检预算的旧快照仍须通过运行协议的精确 Admit 才能启动 VM。

<a id="pool-compatibility-and-recovery"></a>

### 兼容与恢复

未配置 `resource_listen.pools` 时使用 `default` 单池；单池未设置运行内存时沿用
原物理内存减 host_reserved、再减 operational margin 的公式。
多池必须明确设置每池净运行内存，禁止显式旧节点物理资源及 margin 字段。
默认值、Clone、YAML/JSON 往返和 Configure Hook 不会把自动填充值误判为显式旧配置。

runner 名称缺省为 `runner-0`、`runner-1` 等，builder 为 `builder-0`、`builder-1` 等；
需要重排配置时应先使用稳定显式名称。列表缺省时仍兼容旧 unit/size 配置。
实例启动前持久化逻辑池、资源池和实际 unit 身份。重启先恢复实例槽位及调度账本，
再沿用 lease inventory/Sync 恢复运行预留，最后开放准入。
降低配置上限不会漏记已存在的超限实例。活跃执行的模板和资源池身份必须继续保留。

旧版本遗留实例若使用多个逻辑池共享的模板，无法可靠恢复原逻辑池身份；
无限 runner 容量时显示为 unattributed，启用有限容量则拒绝启动，避免猜测造成漏账。
应先清理这类旧实例再启用严格上限。无法确认声明容量的受管消费者保守占用所属池的
运行内存和有限调度容量，直到完成验证或确认退出。

`node-ctl resource pressure` 增加 pools、runner_pools、未归属实例数、各维度使用率、
调度/启动用量及每个恢复义务的源/目的池。运行资源协议和可移植制品不携带主机池身份。

控制器缺省、显式禁用或启用时均支持显式内存 startup。IMG 和 SBX cold 使用 startup
headroom；SNP memory 和 memory Resume 使用 sandboxer 的权威快照预算。
取得制品最终 Capacity 后仍检查 startup 的合法范围。

<a id="pool-regression"></a>

### 真实环境回归

真实环境回归使用已有 prepared E2E 的 `BIN`、`E2E_LIB`、`WORK`、`OUT`、execute image
及 Zot 输入。在具备 KVM、systemd、cgroup v2 和磁盘临时目录的隔离 workbench 中，设置
`RESOURCE_POOL_NUMA_NODES="0 1"` 运行 `test/e2e/manual/resource-pools.sh`，验证双 NUMA
VMM 放置、Builder 释放、账本重建、主动均衡和保留进程/磁盘的跨池恢复。
该手动场景要求两个实际带内存的 NUMA 节点；source gate 的 systemd fixture 也支持单节点。
`test/e2e/cases/orchestrator.static-startup.sh` 覆盖控制器缺省/禁用的 IMG、SNP、SBX
以及显式 startup 的 memory/cold Resume。每秒一字节等 allocator 边界使用虚拟时钟，
并通过实际 sandboxer 预算适配器与 node 协议 Server 验证。

<a id="controller-design"></a>

## 概述

<a id="responsibility-boundaries"></a>

### 责任边界

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
([inventory 与 controller restart](#inventory-and-controller-restart)),但不据此实现 Guest Budget loop。Controller 原子处理 sandbox 主动请求;
Heartbeat 返回 reservation echo,不是执行命令。

Builder registration/execution 两本账与逐 Sandbox reservation 分开. 每个实际 Build 阶段
沿用普通 sandbox-ctl Admit/Release. Build.Resources 配置 A/B 规格,目标 Sandbox 独立解析.
生成的 Builder service/slice 承担归属、委托和整组回收，不从 Build 业务配额派生外层
CPU/内存限额；显式池放置 drop-in 由 [systemd 放置](#pool-systemd-placement) 定义；
现役 VMM 叶子策略和保守只读 recovery inventory 保持不变.

<a id="memory-multiplexing-model"></a>

### 内存复用设计模型

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
reservation。空闲内存回收可以滞后。Guest 应急 `deflate_on_oom` 是 [术语](#units-and-terms) 描述的显式
例外,不是正常提高密度的机制,也不等于 node 已经 grant。

`Headroom` 是每个 sandbox 本地的反应余量,node emergency pool 是共享安全余量。
Per-sandbox headroom 帮助覆盖发现压力到完成控制的延迟;共享 pool 保留给 high-urgency
grow（[runtime grant](#runtime-grants)）,不供普通 grow 使用。它不能替代本地 headroom,也不保证每次突发都能及时
取得内存。OOM/high-urgency 是最后的安全路径,不是正常 grow 的主要信号。

大量 sandbox 并发 grow 时,sandbox 之间不直接转移内存。每个 sandbox 保留自己的 grow
objective,并向共同 node pool 请求 reservation。State 串行化 aggregate reservation 更新,
所以并发请求不能重复消费同一份 headroom。普通新增 grant 还受所属资源池的 runtime grant
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

<a id="core-invariants"></a>

### 核心不变量

- `0 < NodeReservation <= Capacity`。
- `reservedMemory = sum(live NodeReservation)`。
- 内存 pool 用量、内存 RawZone、ResourceProbe allocated memory 与 recovery replacement 只聚合 NodeReservation；CPU 使用 allocatable CPU，缺省为 Capacity。池 RawZone 取各维度最大值；有效 Zone 另记录合格压力与未完成恢复义务。
- grow 只由 sandbox 发起;node 先记账再返回 grant。
- shrink 只由 sandbox 在 balloon current 收敛且 `memory.high` 已按顺序处理后提交。
- `Settled` 是生命周期事实,不从 `memory.current` 推导或改写 reservation。
- controller restart 先按 Capacity provisional charge,StateSync 后原子替换。
- stale heartbeat、host charge 或 node 管理命令不能改变某个 sandbox 的
  reservation。

<a id="resource-model"></a>

## Node reservation 模型

<a id="pool"></a>

### pool

单池且未显式配置净运行内存时，沿用以下内存公式：

```text
Physical           = NodeBudget status field
PostHostBudget      = saturating_sub(Physical, HostReserved)
OperationalMargin  = PostHostBudget * operational_margin_factor
AllocatablePool    = saturating_sub(PostHostBudget, OperationalMargin)
Reserved           = sum(NodeReservation)
MainHeadroom        = saturating_sub(AllocatablePool, Reserved + EmergencyPool)
StartupPool         = AllocatablePool * startup_factor
```

旧单池模式中，`NodeBudget` 是配置或探测到的物理资源总量，不能再当作已扣除
`HostReserved` 的结果。显式 pool 的 `allocatable.memory` 已是净预算，不重复扣 host
reservation 或 operational margin；多池节点汇总这些净预算，不能套用旧物理公式。

共享宿主采用旧单池模式时，通过 `resources.physical_memory` 和 `host_reserved` 明确本控制器获分配的
预算，并为其他服务留出资源。`physical_memory: auto` 仅在配置解析时读取宿主 `MemTotal`；
探测到整机容量不代表控制器独占整机。OperationalMargin 仍作为静态操作余量从 P 中扣除，
既不授予沙箱，也不作为整机可用内存的阈值。运行期节点闭环不采样宿主 `MemAvailable`，
不根据其他服务暂时闲置的内存推导自身资源池。Guest 观测及 sandbox-local Budget 闭环
保持独立，不作修改。

减法使用不下溢的资源运算。所有 reservation 更新与 aggregate 更新位于同一 State
临界区。插入或 recovery replacement 在修改索引前验证 aggregate 加法不会溢出。

<a id="rawzone-and-effective-zone"></a>

### RawZone 与有效 Zone

对所选资源池的内存维度，令 R=Reserved、P=AllocatablePool、E=floor(P*emergency_factor)，
Ty=floor(P*low_factor)、Tr=floor(P*high_factor)、Tc=P-E。
RawZone 在 R<Ty 时为 green，Ty<=R<Tr 为 yellow，Tr<=R<Tc 为 red，
R>=Tc 为 critical。零池在 preflight 拒绝，运行期防御性处理为 critical。

池原始水位取内存与 CPU 维度的最大值；CPU 使用其运行记账分母和最终维度阈值。
任一池 critical 时节点投影为 critical，否则取最低池水位。本机操作检查所选池，
某个池不健康不能阻断其他健康池的准入。

有效 Zone 是所选资源池的有状态权威。reservation 越界立即升级；合格持续内存不足也能从
 green/yellow/red 直接进入 critical。请求来源不参与规则：

| 有效水位 | 新建（含 Snapshot 模板） | 普通恢复 | 资源暂停恢复 | 资源 Pause |
|---|---|---|---|---|
| green | 有资格 | 有资格 | 有资格 | 不执行 |
| yellow | 有资格 | 有资格 | 有资格 | 不执行 |
| red | 不允许 | 有资格 | 有资格 | 不执行 |
| critical | 不允许 | 不允许 | 有资格 | 继续合格不足，或符合原始 critical 主动均衡条件 |

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
demand，也不授权资源 Pause；仅有有效 critical 水位不授权压力 Pause。
[定时主动均衡](#pool-lifecycle-and-pressure) 另外要求源池原始水位 critical 且有可行的非 critical 目的池。

Q 包含已受理的 capture intent、resource-pressure paused 及其 starting 恢复。
Q>0 使已 critical 的池在 RawZone green 时仍保持 critical，但不把健康池提升到 critical。
退出同时要求 Q=0、相关未核实清理结束、
RawZone 低于 critical、无活跃合格内存阻塞，并连续满足 exit hold；只退出到 red。
red 要求 RawZone 连续低于 red 达到自身 hold 后到 yellow；yellow 要求 RawZone 连续
 green 达到自身 hold 后到 green。反弹重置计时，一次评估至多下降一级。
重启在准入前装载 journal/义务，单调 hold 时钟重新开始。
`host_operational_margin` 等历史转换原因仅保留为标签，不据此重建需求。策略变更不清空
既有 Q、清理屏障或 reservation；它们继续按原有恢复和逐级退出规则收敛。

<a id="runtime-grants"></a>

### runtime grant

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

<a id="controller-protocol"></a>

## Reservation 协议

<a id="transport-and-authentication"></a>

### 传输与认证

协议由 sandboxer `pkg/resource` 定义,使用 Unix socket 上的 length-prefixed JSON。
每个 sandbox 使用长连接和 token。controller restart/reconnect 使用 lifecycle lease、
`SO_PEERCRED`、managed pidfile/cgroup identity 和 `StateSync` 重建 session。

<a id="current-messages"></a>

### 现役消息

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

<a id="existing-wire-field-semantics"></a>

### 现有 wire 字段语义

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

<a id="lifecycle"></a>

## 生命周期

<a id="cold-admission"></a>

### cold admission

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

<a id="restore-admission"></a>

### restore admission

```text
sandbox reads Capacity,target,current from snapshot
  -> BudgetAtSnapshot = Capacity - min(target,current)
  -> Admit exact BudgetAtSnapshot
  -> CH spawn/resume -> restore ACK -> MUX
  -> sandbox-local normalization/new epoch/steady loop
```

node 不消费 startup headroom,不读取 snapshot balloon state,也不在 ACK/MUX 前触发
调整。它只看到 sandbox 提交的 exact initial reservation。

<a id="runtime-shrinkgrow"></a>

### runtime shrink/grow

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

<a id="resource-pause-recovery-and-explicit-adoption"></a>

### 资源 Pause、恢复与显式接管

单 capture worker 按当前连续运行时间从老到新尝试 Pause(ifIdle=true)，不使用 CreatedUnix。
明确 busy 时继续扫描，没有 idle 成功才普通 Pause 最老合格 busy 候选；错误和冲突不是 busy。
running 提交重置起点。它复用现有内存 Pause：native exec quiesce/cleanup、Snapshot、旧 VM 退出和
runner/network/resources 清理。不引入保留 VMM 的 pause、page swapper、exec 保留或新
snapshot 格式。Checkpoint 必须落到磁盘，拒绝 tmpfs/ramfs；只有实际 Release/reconcile
才能复用额度。Capture 失败退避，不丢弃已有保存源。

单后台恢复 worker 按最老义务服务，与外部 Wake 共用 SID launch group。在 worker 启动及其
自身 capture/recovery 完成后，用一个有界间隔控制准备恢复的节奏。无关沙箱的 reservation
变化不重置该间隔；繁忙节点不能要求全节点停止分配，才恢复无人访问的资源暂停沙箱。
`headroom_stable` 仅用于诊断，不是恢复准入前提。随后 sandboxer 给出精确 I，最终资源准入
仍等待完整预算才启动 VM；节奏计时不授予内存。
后台恢复仍检查实际可用内存、目标池竞争和保护需求及启动并发，不要求恢复后原始水位
低于 red，也不会为自身预算不足触发新的换出。多维水位、配置及跨池均衡见[资源池](#resource-pools)。
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

<a id="admission-and-pressure"></a>

## Admission 与调度投影

<a id="admission"></a>

### admission

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

内存预算、启动并发、竞争需求和保护额度均属于所选资源池；请求级 admission 令牌桶
仍为节点级。声明容量预留和可用 worker 槽位不能代替精确运行内存 Admit。

<a id="resourceprobe-and-cluster-load"></a>

### ResourceProbe 与 cluster load

`ResourceProbe.Allocated` 和 cluster projected memory load 使用
`reservedMemory`,不是 host charge。E2B `memoryMB` 继续表示 Capacity/SKU。

per-sandbox resource stats 读取当前 sandbox-ctl owner,独立于 controller heartbeat/reservation 路径:

- `cpuCapacity`: 生效 capacity,单位为核;`cpuAllocatable`: 映射到 `cpu.weight` 的相对调度规格,不表示 fractional-core 硬 quota 或性能保证.
- `memoryCapacity`: 生效 Capacity 字节数;`memoryHeadroom`: 最终 `resources.allocatable.memory`,表示气球 headroom,不是 Budget、guest free memory 或 NodeReservation.
- `memoryReserved`: 动态 controller 有观测时的当前 NodeReservation,否则省略.
- `memoryUsed`: 宿主 VMM `memory.current`;`cpuSeconds`: 同一个 cgroup 的 `cpu.stat.usage_usec / 1e6`. 不额外增加 ctl/guest CPU,不扣 inactive-file/balloon,不按 guest capacity 截断.
- `timestampUnix`: 实际观测时间. 缺失的宿主字段分别省略,合法零值仍可见. 来源重建时 CPU seconds 可以重置;native usage 负责生命周期累计.

静态/动态模式以及 usage 或 telemetry 关闭时均可读取. 读取不修改 RequestBudget、admission、recovery charge、heartbeat 采样和已有 build-phase reservation 释放 fence. 原生 API 删除 `cpuCount`/`memTotal`/`memAllocatable`/`memUsed`,reservation 和 headroom 使用各自明确名称. [即时 resource / traffic stats](observability_zh.md#native-stats-api) 定义响应有效性、错误和数值精度. E2B 兼容 metrics 保持自己的字段名和含义.

<a id="recovery"></a>

## 可靠性

<a id="inventory-and-controller-restart"></a>

### inventory 与 controller restart

lifecycle lease 保存 sandbox identity、Capacity、headroom、cold startup headroom、cgroup
identity 和 feature 信息。controller 启动扫描 lease、managed pidfile 与 configured
cgroup roots。此前 Conductor 已恢复持久逻辑池/资源池身份、worker 槽位和声明容量账本；
inventory 再计入对应运行池，无法确认的受管消费者不能被当作空闲。完整兼容规则见
[池恢复](#pool-compatibility-and-recovery)。

1. 已知 lease 按 Capacity provisional charge。
2. identity 不完整的 live cgroup 按可证明的保守上界 charge。
3. sandbox 重新连接后发送 StateSync。
4. State 在一个临界区中删除 provisional charge 并插入 sandbox 上报的
   NodeReservation。

controller 不读取旧持久化 state schema,也不从 memory.current 重建 Guest Budget。
恢复的只读检查包括 cgroup.events、cgroup.procs、进程 cgroup identity 与 memory.max,
用于存活/保守记账,不是第二个 Guest 内存控制器。

<a id="lost-responses-and-reconnects"></a>

### response loss 与 reconnect

- grow 响应丢失时,sandbox 尚未据此执行 high/deflate,仍保留本地旧 baseline;
  StateSync 原子替换 node provisional charge,不会少记。
- shrink commit 响应丢失时,balloon current 已收敛且 `memory.high` 已降低;
  sandbox 因而使用已完成的较小 baseline。node 已提交时双方相等,node 未提交时
  StateSync 完成释放。继续使用旧的大 baseline 反而可能让 sandbox 复用 node
  已释放并重新分配的 headroom。
- Heartbeat mismatch 触发 reconnect/StateSync,不会把 echo 当作执行命令。
- stale heartbeat 和 host charge 只能更新诊断时间/数值,不能改变 reservation。

<a id="reservation-lifecycle"></a>

### reservation 生命周期

startup TTL 将未完成启动标记为 `startup_expired`，不授权释放存活消费者的占账。
只要连接仍活跃，或 inventory 发现 locked lease/managed pidfile、populated cgroup，
sweeper 就保留 reservation。检查出错且不能确认为对象已消失时，也保守保留占账。
只有启动已过期或断连 heartbeat 已陈旧、且消费者确认消失，才可被 sweeper 清理。
已确认的 Release 与符合条件的清理按 reservation identity/token 原子删除索引和
aggregate charge。未知或 provisional reservation 不接受普通 grow transaction。

<a id="performance"></a>

## 性能与可观测性

admission token bucket 限制创建请求洪峰;startup pool 限制创建/恢复中的内存总额;
runtime grant token bucket 限制各资源池普通 grow 的总速率。high urgency 可使用 emergency
pool。allocator 可返回 cooldown,由 sandbox 后续 observation/pressure event 重试。

当前 reservation 与恢复状态通过 `resource status`、`resource list`、本地 `resource pressure` 和既有节点状态生产端观察。`pressure` 另含 `pools`、`runner_pools`、未归属 runner 数、各维度使用率、调度/启动用量，以及恢复义务的源/目的池，见[资源池](#resource-pools)。ResourceProbe 返回有效 Zone 与同一 reservation R/P；消费者不能由 R/P 重新推导有效水位。异常 queue 诊断进入 conductor 标准日志出口,由部署环境统一采集与保留。重点口径包括:

- reserved memory / pool / zone。
- startup in-flight。
- provisional/unknown reservation 数量。
- per-sandbox HostMemoryCurrent 及最后上报时间。
- grant/cooldown 和 admission queue 时延。

这些指标不能混成一个“内存使用量”:reservation、host VMM charge 和 guest demand
分别属于不同口径。

生命周期累计用量通过[原生 stats/usage](observability_zh.md#native-usage)读取, 节点策略下发到全部三条启动路径. 当前 VMM counter、生命周期累计量与历史 E2B Gauge 保持各自语义.

<a id="related-contracts"></a>

## See Also

- [node](node_zh.md)
- [sandboxer sandbox](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/runtime_zh.md)
- [cluster](cluster_zh.md)
- `sandboxer/pkg/resource`
- `internal/nodectl`
- `internal/sandboxcfg/resource.go`
