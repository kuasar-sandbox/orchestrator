# 资源池

[English](resource-pools.md) · [资源协议](node-resource_zh.md) · [运行单元](node_zh.md)

资源池扩展现有进程内 resource controller。每个 runner/builder 池固定关联一个资源池；
一个资源池可以关联多个逻辑池，并允许这些池共享 unit 模板。同一模板不能跨资源池使用。
所有池沿用共享 `paths.base_root`，没有新增存储池。

## 配置和容量

完整的双 NUMA 配置及外部 drop-in 示例见 [配置示例](resource-pools.md#configuration)。
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

## systemd

`unit_dropins` 使用外部文件绝对路径。Conductor 将其复制到关联 runner/builder 模板的
`.service.d/50-kuasar-resource-pool-NNNN.conf`，统一 reload。它只清理该托管命名空间的
过期文件，保留其他管理员文件。请勿手工占用此命名空间。
`units.install: false` 时不读取引用文件、不安装也不 reload，继续由外部管理。

使用主机实际拓扑配置 CPUAffinity、AllowedCPUs、NUMAPolicy、NUMAMask 和
AllowedMemoryNodes，并确认祖先 cgroup 允许对应集合。配置在预热前作用于模板；
reload 不会搬迁已启动进程。Builder 外层不增加 CPUQuota/MemoryMax/MemoryHigh，
沙箱资源约束仍在 sandbox-ctl 的 VMM cgroup 内执行。

## 压力、恢复和主动均衡

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

## 兼容和重启

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

## 增长限速与数值校验

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

控制器缺省、显式禁用或启用时均支持显式内存 startup。IMG 和 SBX cold 使用 startup
headroom；SNP memory 和 memory Resume 使用 sandboxer 的权威快照预算。
取得制品最终 Capacity 后仍检查 startup 的合法范围。

## 真实环境回归

真实环境回归使用已有 prepared E2E 的 `BIN`、`E2E_LIB`、`WORK`、`OUT`、execute image
及 Zot 输入。在具备 KVM、systemd、cgroup v2 和磁盘临时目录的隔离 workbench 中，设置
`RESOURCE_POOL_NUMA_NODES="0 1"` 运行 `test/e2e/manual/resource-pools.sh`，验证双 NUMA
VMM 放置、Builder 释放、账本重建、主动均衡和保留进程/磁盘的跨池恢复。
该手动场景要求两个实际带内存的 NUMA 节点；source gate 的 systemd fixture 也支持单节点。
`test/e2e/cases/orchestrator.static-startup.sh` 覆盖控制器缺省/禁用的 IMG、SNP、SBX
以及显式 startup 的 memory/cold Resume。每秒一字节等 allocator 边界使用虚拟时钟，
并通过实际 sandboxer 预算适配器与 node 协议 Server 验证。
