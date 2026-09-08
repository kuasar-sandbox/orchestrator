[English](proxy-admission-generations.md) | [简体中文](proxy-admission-generations_zh.md)

# worker 自有的 admission 世代计数


## 用所有权消除跨进程锁恢复

admission arena 与 master 写、worker 只读的 route SHM 分离。route record 携带有效 `traffic.max_inflight` 和 `{slot,generation}`。arena v2 只保存 master 写的 slot 当前 generation，以及每个 worker 自有的世代标签与四类服务绝对计数；不再重复保存限额、identity hash、draining 状态或共享 row guard。

存活 worker 独占写自己的列。进程本地锁必须按 **slot** 组织，不能只按 SandboxID：acquire、世代初始化和旧 release 都在同一 slot 锁内串行。master 从不取得这些锁，也不清零存活 worker 的行。Delete 撤销当前 generation，slot 复用发布新 generation，均不等待 worker 前进；worker 即使停在临界区也不会阻塞 master。worker 首次使用新 generation 时初始化自己的行，先清计数再发布世代标签。汇总时在读取四个计数的前后读取标签，只累加匹配当前 generation 的行。

迟到 release 要么在本 worker 初始化新 generation 前完成，要么观察到新标签后不作修改，不会减少新 Sandbox 的计数。与撤销并发的 acquire 在发布增量前后重验 generation，失效则在同一本地 slot 锁内撤销增量。既有 RouteBinding/ExecIdentity 完整相等检查继续在激活边界保护凭据与策略；arena Valid 只检查 generation 是否仍有效。

master 分配事务继续保留一个 spare slot。身份替换先撤销旧 binding、分配新 binding，再发布 route；提交后回收旧 slot，失败则回收尚未发布的新 slot，并恢复旧 generation，不清空旧连接计数。正常 starting/running/paused 转换和相同身份的 full-sync 重放保持 generation 与计数。

## worker 退出

supervisor 必须确认指定 worker epoch 已被回收，才清理其计数列，并在同一 index 启动 replacement。进程退出本身不会清零共享计数；提前清零会漏记仍存活连接。ClearWorker 不需要共享锁，因为旧写者已退出、新写者尚未启动。master 路由更新只写 slot generation，所以退出清理与 route apply 不再形成 worker guard 等待环。不新增 watchdog、锁超时、争锁即杀进程策略、每 flow master RPC 或全局限流。

## 有界近似

对当前 generation 和稳定正上限 M，每个 worker 在同一本地 slot 锁中串行所有服务的 acquire，并在返回 grant 前发布增量。无 release 的历史中，首次达到 M 的增量发布后，只有其他 worker 已开始的至多一个 acquire 仍可能基于旧读数发布，共最多 N-1；之后才开始的扫描至少看到 M。因此 total 和目标 service 各自满足 M+N-1。

对任意观察时刻 T，删去 T 前已释放 flow 的完整正计数区间，只会降低其他 acquire 的观察值，保留所有 T 时仍活跃的成功 acquire 及其 worker 内串行关系。所得无 release 历史恰好包含 T 时活跃 flow，因此适用同一上界。已回收 worker 的关闭连接等价于 release；replacement 在清理后启动，不与旧写者重叠。新 generation 只计匹配标签的行。这不承诺降低策略时驱逐已有 flow，也不把新旧 Sandbox 身份合成一个 quota。

unlimited 路径继续不扫描、不 IPC。worker 落点不是 quota：单个 worker 可使用当前全部 M，而非静态 M/N；多个 worker 也不能各自独立消耗 M。

## 布局与验证

route_capacity=65536、两个 worker 时，arena v2 的共享 mmap 为 5,767,320 字节，原为 8,388,800 字节；计数主体仍为 4,194,304 字节。每个 worker 另有 65,537 个进程本地 mutex（sync.Mutex 为 8 字节时共 524,296 字节），不是跨进程 Guard。映射大小、版本、stride、index 和原子对齐仍严格校验。只把 admission arena 从 v1 升级到 v2，不修改用户配置或 routesync；master/worker 必须使用相同可执行文件及 bootstrap source set。

保留 1/2/4/8 worker 阈值交错、service/total、偏斜、release 波次、回滚和真实子进程退出清理测试；新增 SIGSTOP 把真实子进程停在 acquire 内的测试，要求 master 路由变更和 surviving worker 无需先唤醒或杀死它即可前进。
