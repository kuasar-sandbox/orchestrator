[English](node-proxy.md) | [简体中文](node-proxy_zh.md)

# node-proxy — 节点数据面转发层

## 1. 概述

数据面 proxy 是沙箱流量的 L7 转发层:把外部 e2b SDK/CLI、端口转发或
cluster-router 进入本节点的请求按 `(sid, target)` 路由到 guest envd/CI UDS,
sandbox floatingip 用户端口或 native exec `ctl.sock`.普通 HTTP 的 target 仍是 legacy
port;CONNECT 可以用 `E2b-Sandbox-Service` 显式选择逻辑服务.控制面 API,生命周期,密钥,构建由
`node-ctl conductor serve` 承载,见 [node.md](node.md);本文只描述数据面转发层。

```text
client / cluster-router
  │ Host: <port>-<sid>.<domain>
  │ or E2b-Sandbox-Id + E2b-Sandbox-Port
  │ CONNECT may add E2b-Sandbox-Service + X-Access-Token
  ▼
node proxy worker
  │ shared route view (read-only mmap)
  ├─ e2b legacy 49983/49999 ─► envd / ci UDS
  ├─ forward:port ───────────► floatingip:port
  └─ exec ──────────────────► <run_root>/sandboxes/<sid>/ctl.sock
```

### 1.1 设计原则

- **转发层与控制面分离**:proxy 只做路由判定、鉴权和字节转发;沙箱生命周期权威在
  conductor。
- **单订阅 master,多 worker 数据面**:只有 proxy master 注册
  config-socket plugin;worker 不连接 conductor,不持独立 routesync 订阅。
- **分离路由视图**:固定长度的数据面字段写共享内存,worker mmap 只读;可变长的
  `mmds_routes` 与 `mmds_route_secret_values` 只放 master 有界 heap,worker 经继承的
  本机 socketpair RPC 按 exact path 查询。secret plaintext 不进入 mmap。
- **listener fd 继承**:master 绑定 data/MMDS listener,把同一个 fd 传给所有
  worker;worker 执行 accept 和转发。后续可把 master bind 替换为 systemd socket
  activation,worker 模型不变。
- **转发 netns 可配置**:`proxy_netns` 指向 connector 管理平面 netns 时,worker 进程
  在该 netns 内运行,proxy 到 `floatingip:port` 的访问和 MMDS listener 都位于其中.
- **无上游连接池**:普通 HTTP 每请求拨一次后端并关闭;CONNECT 是一条请求绑定一条
  TCP/UDS 连接。不同 sandbox/port 不复用上游连接。
- **鉴权先于生命周期副作用**:普通 HTTP 与 non-exec CONNECT 固定执行
  `LookupRoute → authorize(RouteBinding) → TryBeginParking → ActivateRoute → fresh Route → dial`。
  Lookup 不 Wake/Resume/park/dial;Activate 在生命周期副作用前后重验 binding。无效
  credential 在节点有效策略为 `enforce` 时不能唤醒或占用 traffic parking/admission；
  节点 `log`/`off` 对普通请求的放行语义见 §6。
- **逻辑服务只影响 CONNECT**:普通 HTTP 不解析 `E2b-Sandbox-Service`,应用层
  Header 保持不变；唯一显式例外是值精确为 `exec` 时在激活前返回 405。
  CONNECT 中显式 service 是 backend 选择的权威输入。
- **Exec 先鉴权后激活**:`service=exec` 始终验证绑定 `StableID` 的 KAT;
  CONNECT 200 后还必须授权完整首个 ExecRequest.失败请求不得触发 parking、Wake/resume
  或 backend dial.最终 node proxy 只将双重 gate 通过的请求交给 ctl tunnel helper,
  不向租户开放任意 UDS 或其它 ctl capability.
- **确定性 MMDS 密钥**:`MmdsSecret = HMAC-SHA256(manifest_key, "kuasar-mmds-v1:" + sid)`（先将十六进制 manifest key 解码为字节）,PUT 和 GET 即使落到
  不同 worker 也一致。

## 2. CLI

节点数据面由一个 proxy master 进程启动:

```bash
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

`node-ctl` 运行内置 master，或按 `paths.proxy_executable` 原地 exec 静态定制 master；master
始终 reexec 自己当前的 executable 启动 worker。内部 worker 模式不作为运维接口。

`proxy.yaml` 字段:

| 字段 | 默认 | 说明 |
|---|---|---|
| `config_socket` | `/run/sandbox/node-ctl.socket` | conductor config-socket;master 在 plugin 平面注册并同步路由 |
| `paths.proxy_executable` | 空 | 静态定制 Proxy master 的绝对 executable;空使用内置实现.静态 config 诊断检查 regular/executable,非 group/world-writable 和 same-file,不按诊断 EUID 判断 owner;实际 root dispatch 只接受 root-owned,非 root dispatch 接受 root-owned 或本 EUID-owned.只用于 node-ctl → master,不用于选择 worker executable |
| `paths.run_root` | (必填) | 节点 RunRoot;worker 本地构造 `<run_root>/sandboxes/<NodeSandboxID>/ctl.sock`,该路径不经 routesync `Policy` 或共享路由记录传递 |
| `data_listen` | (必填) | 节点唯一 sandbox 数据入口;master 绑定一次并把同一 listener FD 交给 worker |
| `proxy_netns` | 空 | 转发平面 netns;空 = 当前 netns.非空时 worker 在该 netns 内运行,conductor 下发的 MMDS listen 也在该 netns 绑定;`data_listen` 仍在 master 当前 netns |
| `stats_socket` | `<dir(config_socket)>/proxy-stats.sock` | master 独占监听并注册给 conductor 的 traffic stats UDS;必须是绝对路径且不得与 config/SHM 路径冲突,权限 0600 |
| `shm_path` | `<dir(config_socket)>/proxy-routes.shm` | 共享路由表 mmap 文件 |
| `route_capacity` | `65536` | 固定路由槽位数;满时 Upsert 失败并终止当前 routesync session,等待该 route 的 Create 返回 503 |
| `workers` | `1` | worker 进程数 |
| `tls` | 空 | 数据面 TLS `{cert,key}`;空 = h2c |
| `auth` | `enforce` | routesync policy 到达前的数据面鉴权回退值 |
| `park_timeout` | `30s` | routesync policy 到达前的 park 回退值 |
| `metrics_listen` | 空 | master Prometheus 文本端点,聚合 worker 数据面计数 |
| `traffic.max_inflight.total` | `0` | 每个 Sandbox 全部适用 service 的 Proxy 级 inflight 上限;`0` = unlimited |
| `traffic.max_inflight.forward` | `0` | 每个 Sandbox 的 `forward` inflight 上限;`0` = unlimited |
| `traffic.max_inflight."e2b:envd"` | `0` | 每个 e2b Sandbox 的 envd inflight 上限;`0` = unlimited |
| `traffic.max_inflight."e2b:code-interpreter"` | `0` | 每个 e2b Sandbox 的 code-interpreter inflight 上限;`0` = unlimited |
| `traffic.max_inflight.exec` | `0` | 每个 Sandbox 的 native exec inflight 上限;`0` = unlimited |

`proxy.yaml` 不含 `mmds_listen` 或 `services`:两者唯一来源是 conductor
`mmds.listen` / `mmds.services`,经可信 plugin registration 的 `Hello{Policy}` 下发。

### 2.1 静态定制 Proxy

运维入口仍只有 `node-ctl proxy serve --config ...`。公共 Load/Decode 只做环境无关的 strict
decode、defaults 与 provided-value validation；内置路径由 node-ctl 显式 final validate，custom
路径则延后到 master `Configure` 后。`paths.proxy_executable` 非空时，node-ctl 校验 protected
absolute executable 的 runtime owner/mode/identity，再把公共 `config.Proxy` 的
bootstrap snapshot 写入有大小上限且禁止 write/grow/shrink 的 sealed memfd；环境变量只传
FD 编号，配置正文与 TLS 材料不进入 argv 或环境。node-ctl 从已验证的同一打开文件原地 exec
xproxy，失败不回退内置实现。xproxy 直接运行、bootstrap 缺失/损坏或 component/file identity
不匹配均 fail closed；这是进程组织和防误用，不是抵抗同 UID 恶意进程的密码学认证。

可编译示例见 [examples/custom-proxy](../examples/custom-proxy/README.md)：

```go
app := proxy.New(proxy.Hooks{
    Configure: func(ctx context.Context, cfg *proxy.Config) error {
        // master-only declarative override
        return nil
    },
    BindRuntime: func(ctx context.Context, process proxy.Process, rt *proxy.Runtime) error {
        // bind process-local logger / TLS provider
        if process.Role == proxy.RoleMaster {
            rt.MasterExtension = newMasterExtension()
        } else if process.Role == proxy.RoleWorker {
            // Construct a fresh instance for this worker epoch.
            rt.WorkerExtension = newWorkerExtension()
        }
        return nil
    },
})
if err := app.Run(); err != nil {
    log.Fatal(err)
}
```

`New` 无副作用；`Run` one-shot、处理 SIGINT/SIGTERM且不调用 `os.Exit`，上层托管可用
`RunContext`。零值/nil App 在 signal、component bootstrap 或 worker bootstrap 处理前返回
必须由 `proxy.New` 构造的明确错误。master 固定执行 bootstrap decode → clone → `Configure` exactly once → 校验
`paths.proxy_executable` 未改变 → final validation → 再 deep-clone/canonical serialize/digest
冻结 EffectiveConfig → `BindRuntime(master)` → 启动 core。Configure Hook、provider 或 final
validation 失败时尚未创建 SHM、listener、routesync session 或 worker。若设置
`MasterExtension`，core 创建共享路由表与进程内 traffic aggregate 后调用其
`Start(ctx, MasterHost)` 恰好一次；Start 失败时尚未绑定 listener、启动 routesync 或 worker，
已建 SHM 会清理。Start 成功后同一对象的可选 `ManagementWrapper` 能包装
`stats_socket` handler；能力只检查一次并冻结，返回 nil handler 会中止启动。

worker 固定由 master 的 `/proc/self/exe` reexec：内置 master 得到 node-ctl worker，custom master
得到 xproxy worker，配置中的 executable 不参与选择。master 通过另一 sealed memfd 传递 frozen
EffectiveConfig、digest、worker id/epoch、FD protocol/mapping 与当前 executable identity；listener、
wake/notify、stats、MMDS RPC、独立 admission arena 等作为继承 FD 传入,并同时校验 worker
index/epoch、arena version/size/layout。worker 严格验证后调用
`BindRuntime(worker)`。每个 worker epoch 都得到新的 `Runtime`，不得复用上一 epoch 的
`WorkerExtension` 实例。worker 完成 stats hello/ready、构造 Host 并等待初始 route-table sync，
随后调用 `WorkerExtension.Start` 恰好一次、冻结同一对象的可选 `IngressWrapper`，成功后才
Serve.Start 错误或 nil wrapper 不开放 data listener,由既有 master supervisor 重启
worker。worker 从不读取 `proxy.yaml`，也不调用 `Configure`；因此配置文件被替换或删除不影响
replacement worker。

Worker 是专用、单次运行的子进程：内置与定制入口在 `Run` 返回后必须退出 worker 进程。成功建立的 route SHM 与 admission 映射存活到进程退出；`PreparedWorker.Close` 只关闭继承描述符，不卸载仍可能被 handler/extension/traffic GC 引用的映射。Supervisor 仍须确认进程退出后才清理共享计数。普通 HTTP 与 HTTP/2 CONNECT 的取消会关闭 backend，与有效流量限制无关；HTTP/1 hijacked CONNECT 保留半关闭语义。详见 [worker 生命周期与转发取消](proxy-worker-lifetime_zh.md)。

公共 `Config` 仅含可序列化声明。`Runtime` 是拒绝 JSON 编解码的进程对象，开放 logger、
启动期 TLS material provider，以及按 process role 使用的可信、静态编译
`MasterExtension`/`WorkerExtension`。provider 返回
certificate chain、`crypto.Signer` 与可选 client CA pool，不能替换任意 `*tls.Config`。
provider 非 nil 即为权威来源，错误不回退 cert/key 文件；最低 TLS version、HTTP/2 ALPN 与
client-auth 策略仍由 core 固定。V1 不支持配置、材料或 Extension 热更新，custom component
与 node-ctl 必须来自兼容版本。

`MasterHost.Routes()` 提供 applied route 的 `Get`、generation-based `Watch` 与
`SyncState(initializing|syncing|synced|stale)`。完整 generation 是
`sync_begin → snapshot upsert* → sync_end`，之后按发布顺序发送 live upsert/delete；断线发送
`sync_lost`。慢 watcher 只使自身 generation 失效并自动 full resync；允许重复、不保证观察到
每个中间变化，也不是 durable audit。View 复制身份、profile/template/state/RunID、当前 endpoint、
artifact location、fingerprint 与 route revision，不复制原始 secret/token，也不增加 route metadata
或 SHM schema。observer 只在 core SHM apply 成功后非阻塞发布，绝不影响 routesync、barrier ACK、
Wake 或 worker notification。

`MasterHost.Traffic().Get` 以当前 route identity 直接读取 master 的进程内 worker aggregate，
不经 stats UDS 回环，返回 map/pointer 副本；V1 没有 Traffic Watch。断线期间保留 route 仍可查询，
要求新鲜度的调用者同时检查 Route `SyncState`。Management wrapper 可添加、覆盖或透传任意本地
route；框架不保留 namespace、不做 route conflict 检测，也不规定认证。

`WorkerHost.Process()` 返回当前 worker id/epoch；`GetRoute(sid)` 只做当前 SHM 点查并返回
独立的非秘密 `RouteView` 副本，不暴露 raw record/Router/可变指针，也不提供 worker Route
Watch.`IngressWrapper` 在 canonical Host/Header 与 CONNECT parser 之前接收 raw request;
wrapped handler 只服务节点 `data_listen`,MMDS listener 不使用它.Extension
可自行定义 Header/path/auth、覆盖或本地响应；未匹配请求调用 `next` 即保留 core token 与
native exec 语义。

私有认证完成后，`WorkerHost.ForwardAuthorized` 可复用 core 的
`LookupRoute → TryBeginParking → ActivateRoute/Wake/binding revalidation → optional Revalidate →
dial → ordinary HTTP/CONNECT → traffic close`。该 helper 拥有 `ResponseWriter`，返回后调用方
不得再写错误；它不校验 Kuasar `X-Access-Token`。`Revalidate` 在 activation 后、dial 前执行；
普通 HTTP 的 `Rewrite` 只收到 guest-facing clone，失败时不写任何 guest request bytes；CONNECT
不调用 Rewrite。generic helper 拒绝 native exec，后者继续经 `next` 走 KAT + per-command CEL。
本接口不增加 WebSocket transport；WebSocket 仍由 [#269](https://github.com/kuasar-sandbox/orchestrator/issues/269) 独立跟踪。

该 API 只对应独立 Proxy,不为 conductor 增加数据面 factory;也不开放原始 Router,
SHM、listener、routesync、stats、dial target 或 credential records。除同一 master Extension 的
可选 management wrapper 和同一 worker Extension 的可选 ingress wrapper 外，不引入 Go plugin、
运行时发现、多 Extension registry、通用 lifecycle hook、secret resolver 或 DI container。
`node-ctl config proxy` 只做 declarative/bootstrap 与
executable metadata 诊断，绝不执行 xproxy、调用 Runtime provider，或用诊断命令 EUID 代替实际
启动的 runtime owner 校验。完整 Extension 合同见 [extensions.md](extensions.md)。

## 3. 部署拓扑

每个可承载 sandbox 的节点同时运行 conductor 与 Proxy.conductor 的 API listener 只承载
控制面;Proxy 的必填 `data_listen` 是该节点唯一 sandbox 数据入口.两个 advertised endpoint
分别指向这两个 listener,也不存在跨平面回退.

```text
client / cluster-router ── control ─► conductor APIEndpoint
                              │
                              └─ config_socket plugin stream
                                 Wake / BarrierAck ▲ │ route/policy/MMDS
                                                   │ ▼
client / cluster-router ── data ───► Proxy DataEndpoint
                                      master ─► shared route mmap
                                        │       shared admission mmap
                                        │       MMDS bounded heap
                                        │       stats UDS
                                        └─ inherited data/MMDS listener
                                                   ▼
                                             worker[0..N)
```

要点:

- conductor 只看到一个固定 plugin id `proxy`;registration 的 `Proxy` 字段是可信 Proxy
  marker,`StatsSocket` 是独立可选的 traffic stats 地址.
- `stats_socket` 只由 master 监听;conductor 的公开 traffic GET 经该 UDS 读 master
  聚合缓存,不会查询时扇出 worker.缺少 `StatsSocket` 不影响 route barrier participant.
- worker 不注册 plugin,不保存独立全量路由表;route mmap 仍是 master 单写/worker 只读,
  admission mmap 则由每个 worker 只写自己的 absolute-counter column。崩溃后由 master
  重启,replacement 直接读取当前 route/admission view。
- master 退出会带走其 worker;systemd 重启 master 后重新注册并重建共享表。
- plugin id 必须精确为 `proxy`,且 registration 同时满足
  `subscribe.kind=route_wake`、`proxy!=nil`、`mmds=true`,conductor 才投影 MMDS policy、
  routes 和 secret values;普通 observer 与 node-link 均收不到这些 confidential values。

## 4. routesync 与共享路由视图

routesync 仍是帧化 JSON over h2c,由 proxy master 拨 conductor:

```text
master → conductor : register{subscribe: route_wake, proxy{stats_socket?}, mmds}
master → conductor : wake{sid} / route_barrier_ack{barrier_id}
conductor → master : hello{policy}
conductor → master : upsert* → bookmark → upsert/delete/route_barrier...
```

master 把下行路由流投影到共享内存:

- `BeginSync` 开启新同步世代;
- `Upsert` 写入或更新 `sid` 槽位;完全相同的重放只刷新同步世代,不推进生命周期
  revision;
- `Delete` 用 backshift 删除回收 live 槽位,并在独立的有界终态 cache 中记录无凭据的
  `(sid, revision)`;
- `Bookmark` 清理本世代未出现的旧记录,并标记首轮同步完成;
- `Policy` 写入共享头部,worker 每请求读取当前 `auth_mode` / `park_timeout_ms`。

每个 Create 都使用同一有序 stream 建立 route-applied barrier:

```text
conductor: Upsert(initial starting) → route_barrier{id}
master:    traffic patch validate/merge → admission apply → route SHM Upsert
           → notify workers → route_barrier_ack{id}
```

`ApplyUpsert` 把 admission 与 route 作为一个可回滚事务处理:任一校验、arena allocation、MMDS
projection 或 route SHM Upsert 失败都会恢复旧 admission/route view。master 的 ACK 只表示 parking
所需的 `starting RouteBinding` 及其 effective limit 已进入 master-owned serving view,不表示 sandbox
已 running、backend 可拨、envd 已完成 `/init` 或任何 worker healthy/ready。serving worker 数为 0、worker
重启或 stats stream fault 都不进入 Create 条件。Wake 与 BarrierAck 由一个上行 writer 串行
写帧。任一先行 Upsert 写表失败时,subscriber 不发送 ACK并终止 session;conductor 将等待中的
Create 回滚并返回 503。barrier id 只存在于本次内存协调,不进入 route changelog、共享表、
Sandbox schema 或日志字段。

当前只有固定 plugin id `proxy` 的一个 master participant。Create 在 ACK 后再次核验该
registration epoch 仍为当前租约;断连、同 id replacement、迟到或旧 session ACK 都不能完成
barrier。完成规则内部按 all-of participant set 实现,不采用 quorum;若以后显式配置多个
traffic-serving master,必须全部 ACK 后才能返回 201。

MMDS 扩展不写固定表。master 对每个 active sandbox 在一个锁内替换 routes + values;
heap entry 总数受 `route_capacity` 限制。`BeginSync` 与 routesync 断开
都会立即清空 heap 并标记 unavailable；新 master 也从空视图开始，只有完整 `Bookmark` 后才
重新开放查询。仅重启 worker 不会清空仍存活 master 的 heap；replacement 通过继承 RPC 接回
当前 master，并在共享视图同步后才开始服务。service
registry 由每次 `Hello{Policy}` 原子整表替换。running/starting Upsert 先更新 heap 再公开
SHM route;paused/Delete 先撤销 heap 再更新 SHM,让已采样旧 active row 的 worker 也 fail closed,
避免把新生命周期与旧 secret
组合。`RunID` 进入固定表用于 MMDSv2 token 的 incarnation 绑定;routes/value plaintext
绝不进入固定记录、metrics 或日志。

本文中 `RouteEntry.SandboxID`、`sid` 和共享表 key 均是 node-local SandboxID.集群路径下,它们是
Registry 分配的 NodeSandboxID;cluster Router 已在进入 node 之前把公开稳定 SandboxID 转换为该值.
`RouteEntry.StableID` 是跨 NodeSandboxID 变化保持的 sandbox identity，用于 KAT/credential
binding，不参与共享表 lookup。routesync V3 将 identity 一次性切换为 `stable_id`；V4 将
Snapshot-specific location 改为与 E/S kind 正交的 `artifact_location`;V5 将节点注册拆成
`api_endpoint` 与 `data_endpoint` 并收缩 Proxy registration;subscriber
和 node-link client 在首个 Hello 校验版本，不匹配时在处理 route/command 前终止 session。

共享表是固定容量开放寻址 hash 表。master 单写;每条记录带 seqlock,worker 读取时若遇到
写中状态或版本变化会重试,不会看到半条路由。worker 只依赖共享表本地读取:

```text
sid hash ─► record slot ─► RouteEntry (Lookup 只读)
                         ├─ running → auth 后 Activate 重验并转发
                         ├─ starting → auth 后 park,不发 Wake;回滚即结束
                         └─ paused → auth 后才发 wake pipe 并 park
```

普通 route Lookup 对 missing/paused/starting 都不写 wake pipe;只有请求已按返回的
`RouteBinding` 完成鉴权后,Activate 才可对 paused sid 写 wake pipe。starting 已由 conductor
launch owner 推进,Activate 只等待 running/delete/paused 更新,不得再发 Wake;后两种回滚更新
立即结束 starting 请求。master 去重后通过 routesync
上行 `Wake`。master 每次写共享表后通过 notify pipe 唤醒 worker 本地 park waiters。
全局 revision/notify 只负责唤醒检查;worker 以该 SID 的 live 或终态 revision 判断 Wake
是否已收到终态回应。live 路由、终态 cache 和 revision 在同一次 table seqlock snapshot
中读取,waiter 不会把旧 missing/paused 路由与新 revision 混合为假终态。重复的相同 paused
Upsert 不推进 per-SID revision,因此订阅重放不能伪装成 Wake 的完成响应。live hash 的删除
会在同一 table seqlock 下 backshift 并立即回收槽位;终态 cache 固定最多 4096 条且不含任何
凭据。极端 churn 下 cache 碰撞只会淘汰较旧的终态相关性,对应 waiter 保守地继续 park 到
后续状态或 timeout,不会错误路由或把无关 SID 当作 Wake 结果。
正常的单次 Wake 路径中,即使异步共享表收敛把中间 starting 与随后 paused/Delete 合并,
终态 revision 仍会让 waiter 及时观察 rollback;只有前述极端 cache 淘汰才退化为保守 timeout。
一个请求只允许在初始 missing/paused 发一次 Wake;观察过 starting 后回到 paused 不得再次
Wake。

共享视图是异步收敛的路由缓存。默认创建使用 UUID,集群 NodeSandboxID 使用
`<stableSandboxID>-g<SandboxGeneration>`,正常流程不会让不同逻辑沙箱复用同一个
node-local ID。若外部系统显式把刚删除的 NodeSandboxID 立即分配给另一个逻辑沙箱,
在 Delete/新 Upsert 尚未到达 Proxy 的极短窗口内,worker 仍可能持有旧实例的
凭据投影和同名运行目录.节点不得主动执行这种跨逻辑沙箱的即时 ID 复用;
为不同逻辑沙箱显式指定迁移 target 时应使用新的 NodeSandboxID,或先确认路由视图已经收敛。

受保护 `RouteEntry` 的 state 为 `starting|running|paused|dead`,并显式携带
`StableID`、`APISecret`、`APISecretFingerprint`、
`ManifestKeyFingerprint`、`ServiceSecret`、`EnvdAccessToken`、`TrafficAccessToken` 和
`ForwardAccessToken`。
ManifestKey 原文不进入路由。节点 proxy 转发时只按目标选择 EnvdAccessToken 或
ForwardAccessToken;TrafficAccessToken 仅随受保护视图投影给外部网关及 e2b 数据面组件,
不由 node 平台层消费.`StableID + ServiceSecret` 用于验证 exec KAT,
其中共享表 key 和本地运行目录仍只使用 NodeSandboxID.既有 `MmdsSecret` 独立服务于
MMDS token 签名,不等于 route secret values;后者仅经上述可信投影进入 master heap。
`MaxInflightPatch` 只在 routesync wire 上携带 Sandbox 显式叶子;master 将目标节点
`proxy.yaml` 默认值与该 patch 合并后,把 fixed effective value 及
`{admission slot,generation}` 写入 route SHM,不会把 pointer 写入 SHM 或用于 route equality。
当前内部兼容边界为 routesync version 7、route SHM schema 7、worker bootstrap/config/FD
protocol version 2，以及 admission arena version 1。这些都是内部 hard cut；协议版本不匹配的
conductor/proxy/registry/router 或旧 SHM/bootstrap 不兼容且 fail closed。

初始 durable starting upsert 可以没有 FloatingIP、UDS 或其它 backend endpoint;worker 按
state park,绝不尝试使用这些空字段。node 持久化 network ownership 并完成 YAML/ready.sock
后会发布 enriched starting,此时 MMDS 才能按 FloatingIP 反查身份。ordinary data plane 仍
须等 running。

## 5. 转发路径

普通 HTTP 按 `Host: <port>-<sid>.<domain>` 或 `E2b-Sandbox-Id` /
`E2b-Sandbox-Port` 解析 `(sid, port)`.它不解析 `E2b-Sandbox-Service`;该 Header 作为
应用层 Header 原样转发，不改变 backend；唯一例外是值精确为 `exec` 时在激活前返回 405。CONNECT 解析 `(sid, service?, port?)`.
cluster 第二跳的 `sid` 必须是当前 NodeSandboxID.

未显式携带 service 时,Node 从本地受信 profile 应用 legacy 映射:

```text
profile=e2b  and port ∈ {49983,49999} → envd / ci UDS
otherwise                              → floatingip:port
unknown or not running before timeout   → 404
```

因此 bare 的 49983/49999 与其它合法端口一样转发到 `floatingip:port`,不具有
envd/CI 逻辑含义,也不返回 501.

CONNECT 显式携带 `E2b-Sandbox-Service` 时,service 取代 legacy 端口推导:

| Service | 支持 profile | Backend | Port 语义 |
|---|---|---|---|
| `forward` | e2b / bare | `floatingip:port` | 必须由 `E2b-Sandbox-Port`,legacy Host 或 CONNECT authority 之一提供 |
| `e2b:envd` | e2b | envd UDS | 可携带,但不参与 backend 选择 |
| `e2b:code-interpreter` | e2b | CI UDS | 可携带,但不参与 backend 选择 |
| `exec` | e2b / bare | `<run_root>/sandboxes/<NodeSandboxID>/ctl.sock` | 可携带,但不参与 backend 选择 |

bare 显式请求 `e2b:envd` 或 `e2b:code-interpreter` 返回 501.unknown/空 service 返回 400.
service 与 port 并存不是冲突;Node 不会用 49983/49999 反向覆盖显式 service.

普通 HTTP:

1. worker 只读共享表,得到不含 backend 的 `RouteBinding`;
2. 按目标选择 EnvdAccessToken 或 ForwardAccessToken,按节点有效鉴权策略检查 `X-Access-Token`；符合条件的
   `/files` 请求也可使用 EnvdAccessToken 验证 signature，各模式的强制范围见 §6；
3. 按该策略获准后执行 `TryBeginParking`:同时检查 Sandbox total 与目标 service 上限,成功后
   发布共享计数并进入 parking;达到上限返回 429,不 Wake、不 Activate、不 dial;
4. 进入 `ActivateRoute`:在 Wake/等待前重验包含 admission generation/effective policy 的
   binding,完成生命周期动作后再
   重验一次,并从最新 running route 构造最终 backend;binding 改变时 fail closed;
5. 拨一次 envd UDS 或 `floatingip:port`。配置 `proxy_netns` 时,`floatingip:port` 在该
   netns 内拨号;
6. 写入一条 HTTP 请求,流式复制响应,响应或完整 relay 结束后一次性关闭 flow并释放额度。

CONNECT:

- sandbox id 来自 `E2b-Sandbox-Id` 或 legacy authority label;
- legacy/`forward` 可从 CONNECT authority 取实际 port;无端口逻辑服务的 authority
  只是 transport 占位,不会生成 `E2b-Sandbox-Port`;
- 普通 forward/envd/CI 目标通过有效鉴权策略后,把客户端连接与后端连接双向 splice;
- `service=exec` 只接受 CONNECT;普通 HTTP 携带该 service 返回 405,且不触发恢复.

node proxy 的 exec 路径分为三个有序阶段:

1. CONNECT 200 前只做无副作用本地 `LookupExec`,以 route 中的
   `StableID + ServiceSecret` 严格验证 `X-Access-Token` KAT,并在 HMAC 验证成功后
   编译/读取有界缓存中的 CEL programs.该阶段不 parking、不 activation、不拨
   `ctl.sock`;token/identity/expiry/conditions 失败以 HTTP 400/401/404/501 结束.
2. 返回并 flush CONNECT 200 后,在固定 10 秒 first-request timeout 内读取完整首个 ctl
   frame.解析严格覆盖顶层 `exec_request`、`ExecSpec` 和 `StdioSpec`,保留客户端原始
   4-byte little-endian length + JSON bytes,重新检查 expiry,构造规范化 request view 并
   以 AND 执行全部 conditions.false、error、unknown、cost exceeded 或 cancel 都 fail closed.
3. 只有 request admission 成功后才 `TryBeginParking(exec) → ActivateExec → exact identity recheck →
   ctl.sock dial → AttachBackend`,然后把首帧 Raw 原样写入一次并进入双向 relay.首帧一旦
   写入 backend 就不 retry、reroute 或 replay.

CEL view 将 nil argv/env 规范化为 `[]`/`{}`,空 cwd 与 `/` 规范化为 `/`,user 保留请求原值.
TTY 模式中 stdin/stdout/stderr flags 沿用 ctl wire 的 ignored 语义,view 暴露规范化后的有效
语义,不会因 flags 同时出现而拒绝合法请求.条件或结构 gate 失败不改变 parking/activity,
不启动 guest child;因此也不会使 paused sandbox 恢复.

worker 从 master 冻结的 EffectiveConfig 取得 `proxy.yaml` 中必填的 `paths.run_root`,不重新
读取文件.该值应与同节点 conductor 的 `paths.run_root` 一致.routesync `Policy` 和共享路由
视图只提供路由,凭据及鉴权策略,不投影 `ctl.sock` 路径.

共享的 sandboxer tunnel helper 不理解 KAT、CEL、route 或 lifecycle;它只冻结 callback 顺序、
严格首帧读取、Raw 单次转发和 half-close relay.H1 从 Hijack 返回的 buffered reader 继续读,
H2 从 request body 读并及时 flush response;两者都保留 half-close,等待双向 relay 结束.
CONNECT 200 后,已完整识别的 request denial 或 backend failure 返回统一脱敏 ctl frame
`{"type":"error","msg":"exec request rejected"}`;framing 无法恢复时直接关闭 tunnel.
`max_inflight.exec` 也只在首帧与 CEL 通过后检查;达到上限时 HTTP 200 已提交,因此使用同一
generic ctl error frame并关闭,记录 `data_requests_total{result="max_inflight_reached"}`,不伪造
HTTP 429 或 `X-Kuasar-Proxy-Error`,也不 Activate 或拨 `ctl.sock`。

KAT 在 CONNECT admission 时校验,并在首帧授权时重新检查 expiry;进入 backend relay 后
过期不强制断开已建立 tunnel,有效期内同一 KAT
可以建立多条独立 CONNECT.每条 tunnel 只承载一个 ctl exec session,不复用 backend 连接;
新 CONNECT 在 route 切换后自动进入当前 NodeSandboxID,已建立 tunnel 不迁移.

worker 在本进程执行上述完整 token + request + backend gate,用 frozen EffectiveConfig 的
`paths.run_root` 构造 `sandboxes/<NodeSandboxID>/ctl.sock` 路径.路径和 CEL programs 不经 routesync `Policy` 或 SHM
记录.conductor 不解析,不选择也不转发 ordinary HTTP,CONNECT 或 exec 字节;误发到
APIEndpoint 的数据请求只得到 API handler 的自然响应.cluster-router 的 canonical chained
CONNECT 只是中继,traffic 统计只发生在建立最终 sandbox backend 的 node worker.

## 6. 数据面鉴权

数据面请求头统一为 `X-Access-Token`,但期望值按转发目标选择:

- e2b legacy 49983/49999 以及显式 `e2b:envd`/`e2b:code-interpreter` 使用 create
  响应中的 `envdAccessToken`;
- bare 的任意 legacy 端口,e2b 的其它 legacy 端口和显式 `forward` 使用
  `forwardAccessToken`;
- `trafficAccessToken` 只供外部网关及 e2b 数据面组件验证,node proxy 不消费;
- `exec` 只接受以 ServiceSecret 直接 HMAC 签名,绑定 `StableID` 且
  `aud=exec` 的 `kat1` ExecAccessToken.Envd/Forward/Traffic token 不能代替它.

opaque Envd/Forward token 按各自线格式校验;exec KAT 执行严格格式,签名,SID,audience
和可选过期时间校验.

`auth` / policy `auth_mode`:

| 模式 | 行为 |
|---|---|
| `enforce` | 不匹配返回 401 |
| `log` | 记录但放行 |
| `off` | 不校验 |

上表只适用普通数据面。Exec 始终 enforce，不受 `auth_mode` 影响。节点独立使用自身有效
策略，Router 的 `enforce` 不会改变该策略。在节点 `log`/`off` 模式下，无效普通凭据也可能
进入 parking、激活和后端拨号。

e2b legacy 49983 上的 `GET/POST /files` 在未携带 `X-Access-Token` 时，可用
EnvdAccessToken 验证 envd signature query；`enforce` 下 proxy 先验签再转发，envd 收到原始请求
后再次验证同一 signature。如果请求携带非空但错误的 `X-Access-Token`，不回退 signature。
这些检查仍遵循节点普通数据面策略：`log` 可放行不匹配请求，`off` 跳过验证。显式逻辑 service
与 bare 49983 不继承 legacy 签名文件例外。

## 7. MMDS

`mmds.enabled=true` 时,envd 在 FC 模式下通过 Firecracker MMDS v2 获取当前身份的
access-token hash;`mmds.routes.enabled=true` 还开放显式声明的 static/secret/service
exact route.HTTP 只由 Proxy worker 承载,master 提供有界 route view.配置
`proxy_netns` 时,master 在该 netns 绑定 conductor 下发的 `mmds.listen`,并把同一个
listener fd 传给所有 worker;worker 不读取任何 proxy/MMDS YAML.

```text
guest envd
  │ 169.254.169.254:80
  ▼
connector mgmt-extract
  │ conductor mmds.listen
  ▼
proxy worker ─┬─► shared route view(token identity + RunID)
              └─► master socketpair RPC(routes + values + service socket)
```

两段式协议先签发 token，再用于 root 或声明路径的 GET：

1. `PUT /latest/api/token`:要求恰好一个 `X-metadata-token-ttl-seconds`,值为
   `1..21600`;按请求源 IP 查 enriched starting/running route 的 floatingip。初始
   starting 尚无 FloatingIP时仅此 token mint 路径可按 `park_timeout` 等待。返回的
   HMAC token 绑定 sid、来源 IP、当前 `RunID`、`aud=mmds` 和 expiry。
2. `GET /`:重新校验签名、来源、expiry、audience 与当前 `RunID`,返回
   `{instanceID, envID, address, accessTokenHash}`（当前 `address` 为空字符串）。pause/resume 改变 incarnation,旧 token 立即失效。
3. `GET <declared-path>`:完成相同认证后 exact lookup;未声明或声明但未配置的 secret
   都返回 404,store/sync 不可用返回 503。static/secret 缺省 Content-Type 在响应时才取
   `text/plain`,不写回配置。

MMDS session token 使用每沙箱确定性 `mmds_secret`,因此 PUT 和 GET 落到不同 worker
仍能互相验证。请求 path 不清理、不重定向:query、fragment、percent escape、需
percent-encode 的字符、空/dot segment、backslash、wildcard 和非 root trailing slash 均拒绝。GET 的非零/未知
Content-Length、任意 Transfer-Encoding 或未知 body 直接 400,handler 不读取一个 byte
来探测 body。内置 root 只精确匹配 `/`;所有 guest 响应统一带
`Cache-Control: no-store` 与 `X-Content-Type-Options: nosniff`。

三类自定义 route:

- `static`:直接返回声明的 UTF-8 `data`。
- `secret`:按 route 的 `secret` 名查当前 opaque bytes;PUT 完整替换,DELETE 后立即 404,
  不等待、不设 TTL,Content-Type 始终来自 route。
- `service`:master 从唯一 registry 解析本机 Unix socket,worker/handler
  构造全新 `GET <exact-path> HTTP/1.1`,`Host: mmds-service`,仅增加
  `E2b-Sandbox-Id: <sid>` 与 `E2b-Sandbox-Service: <service>`。不发送 port,不透传 guest
  Host/query/body/token/Authorization/Cookie 或任何 guest header。V1 只透传合法 status、
  有界 body 和合法 Content-Type(缺省 `text/plain`),不跟随 redirect;超时/过大/非法响应
  映射 504/502,service 缺失或 socket 不可达为 503。

安全边界:routes 是 portable declaration,secret values 则只存在 sqlite ciphertext 或受信
Proxy master 的有界 heap.它们不写普通 metadata,共享 mmap,
日志、metrics、migration token、template 或构建产物,也不发送给 observer/node-link。
但 guest 主动 GET 后,value 已进入 guest/application memory;随后执行包含内存的 Pause/snapshot
可能把该副本作为普通 guest working set 捕获。平台不能在宿主侧从任意 guest 内存中擦除它,
调用方应在应用侧缩短驻留时间,并把包含已消费 secret 的 snapshot 按敏感制品保护。

## 8. per-Sandbox traffic admission 与 stats

### 8.1 配置与合并

`traffic.max_inflight` 是每个 Sandbox 在整个 node Proxy 上可接纳的 logical inflight 规格,
不是 QPS、带宽、worker capacity 或 Proxy global capacity。配置值 `M` 不会按 worker 各自
应用,也不会静态切成 `ceil(M/N)`:

```yaml
traffic:
  max_inflight:
    total: 128
    forward: 96
    "e2b:envd": 16
    "e2b:code-interpreter": 8
    exec: 8
```

`0` 表示该维度 unlimited。`total` 与目标 service 在同一次临界区中独立检查,不要求 service
之和小于 `total`;`forward` 也不按 port 拆分。一个 Sandbox 达限不占用或拒绝其它 Sandbox
的额度,因此不会形成 global failure amplification。

Sandbox 只在 metadata key `kuasar-sandbox.traffic` 保存显式 patch;create/build 也可用恰好
一次的 `X-Kuasar-Sandbox-Traffic` 传入同形 JSON。合并优先级为:

```text
template/group/default explicit metadata
  < create metadata
  < X-Kuasar-Sandbox-Traffic
  < target Proxy resolution only for absent leaves
```

前三层按 `max_inflight` 叶子合并并 canonical marshal。叶子 absent 继承低优先级显式 patch,
最终继承当前目标节点 `proxy.yaml`;explicit `0` 则清除低优先级或节点默认限制。空 Header、重复
Header、`null`、unknown、负数、非整数和 uint32 overflow 均拒绝。bare Sandbox 显式声明
`e2b:envd` 或 `e2b:code-interpreter` 也拒绝;节点默认可包含全部 service,bare 只消费
`total/forward/exec`。

目标节点默认值不写入 Sandbox metadata/struct/SQLite、Registry record 或 MigrationToken。
MigrationToken 沿用已有 Metadata:absent traffic 在迁移后仍 absent,由目标 Proxy 使用自己的
默认值;显式 patch 原样迁移并覆盖目标默认。V1 不支持运行时修改 metadata,降低 limit 也不
驱逐已有连接,只影响后续 acquire。

### 8.2 共享 admission arena 与误差证明

route mmap 与 mutable admission arena 分离。master 为所有 effective limit 全为 0 的 route
发布零 binding,worker 直接执行既有 `BeginParking`,不扫描 arena、不 IPC。其它 route 得到
稳定的 `{slot,generation,effective limits}`。每个 entry 含 identity/state、generation、fixed
limits 以及 `counters[worker][forward/envd/CI/exec]`;worker 只写自己的 absolute cell。

acquire 在已有 worker-local per-Sandbox entry lock 内执行:

```text
verify active generation and effective limits
→ atomically load every worker's four cells exactly once
→ check total and target service together
→ increment this worker's target-service cell
→ enter local parking
→ unlock and return success
```

同一 worker、同一 Sandbox 的所有 service 共用该锁,所以每个 worker 同时最多有一个 acquire
临界区。先考虑没有 release 的单调执行。把使已发布值首次到达 `M` 的成功原子增量作为边界
发布:边界时其它每个 worker 最多各有一个已经开始但尚未发布的 acquire,共至多 `N-1` 个;
边界 worker 在解锁后才能开始下一次检查。边界之后才开始的检查逐列读取的都是不小于边界
时刻的值,即使不是原子 snapshot,其和也至少为 `M`,必须拒绝。增量在解锁和返回 grant 前已经
发布,所以只有边界时正在进行的其它 `N-1` 个 acquire 还可能成功,峰值至多为 `M + N - 1`。

concurrent release 不扩大该上界。固定任意观察时刻 `T`,从执行历史中删除所有在 `T` 前已经
release 的 flow 的 acquire→release 完整区间。删除这种正计数区间只会让其余 acquire 的逐列
读取值保持不变或降低,所以原执行中成功且在 `T` 仍 active 的每个 acquire 在缩减历史中仍会
通过;同 worker 的串行关系也不变。缩减历史到 `T` 为止没有 release,其已发布计数恰好等于原
执行在 `T` 的 actual active 数,因此适用上一段单调执行的 `M + N - 1` 上界。该论证不要求
逐列读取构成原子 snapshot,并分别适用于同一次临界区内检查的 `total` 和目标 service。所以
配置 `M`、worker 数 `N` 的合同是:

```text
actual admitted inflight <= M + N - 1
```

parking→egress 不改 shared count。activation、dial、HTTP forward、context cancel、ordinary
response 及完整 CONNECT/exec relay 的最终 Close 都由同一个 flow/lease exactly once 释放;
half-close 不释放。

Delete 或 identity replacement 先把旧 generation 置为不可 acquire,再清零/复用 slot。旧 flow
保留旧 generation,release mismatch 时不得减少新 route cell;starting/running/paused 的同一
Sandbox lifecycle 更新保持 generation。route/policy 发布前后的 Activate 都重读 route identity
与完整 binding;对已经 drain 的 limited generation 还直接重验 arena state,因此旧 lookup 不能在
新 route 发布后绕过新 binding。

worker stats stream fault 会先终止 worker。只有 supervisor 的 `cmd.Wait` 证明旧进程已退出、
kernel 已关闭其连接后,master 才接管可能遗留的 row guard并清该 index;此前 stale-high 只能
保守拒绝,不能漏计。replacement 复用 index 但使用新 epoch。master 退出会终止全部 child 和
连接;新 master 重建 arena，不继承旧计数。
仍存活 master 的完整 routesync 则保留重放且未改变 binding 的计数，只在 Bookmark 淘汰
本轮未出现的 binding；它不会清零仍活跃 flow 的计数。

在 `route_capacity=65536,workers=2` 配置下,counter 主体为
`65536 × 2 × 4 × 8 = 4194304` bytes;连同 entry headers、row guards 和一个 transaction spare
entry,实际 mmap 为 `8388800` bytes。size、stride、worker count/index 和 8-byte atomic alignment
均在 master/worker 映射时检查;当前仅支持项目 Linux `amd64`/`arm64` 范围。

普通 HTTP 与 non-exec CONNECT 达限返回 429、
`X-Kuasar-Proxy-Error: max_inflight_reached` 和固定 body,不设置 `Retry-After`、不 Wake/Activate/dial、
不逐次打印日志,并增加低基数 `data_requests_total{result="max_inflight_reached"}`。exec 的
CONNECT 200 后差异见 §5。

### 8.3 Traffic stats 与统一 worker stream

公开接口为 `GET /sandboxes/{sid}/stats/traffic`。统计的是最终 node proxy 按有效鉴权策略接纳的
逻辑 ingress,不是客户端物理 TCP 数:

```text
ingress = parking + egress

parking: 有效鉴权策略和 ExecRequest admission 通过后,ActivateRoute/ActivateExec 与最终 backend dial 尚未完成
egress:  最终 node proxy → sandbox backend 已建立且尚未最终 Close
```

service 固定为 `forward`、`e2b:envd`、`e2b:code-interpreter`、`exec`。e2b 返回四项,
bare 只返回 forward/exec。普通 HTTP 和每条 CONNECT/exec 各是一条逻辑 ingress。dial
成功时在同一 worker-local entry lock 中原子执行 `parking--/egress++`;activation 或 dial
失败只结束 parking。`CloseWrite` 只传播 half-close,不结束 egress;只有 tracked backend
的最终 `Close` 以 `sync.Once` 结束 egress。token 或 ExecRequest admission 失败不进入
parking/egress,也不刷新 sandbox activity/`idleSince`。

bare Sandbox 空闲响应示例（不适用的 e2b 上限为零）：

```json
{
  "state": "running",
  "maxInflight": {
    "total": 128,
    "forward": 96,
    "e2b:envd": 0,
    "e2b:code-interpreter": 0,
    "exec": 8
  },
  "inflight": {
    "parking": 0,
    "egress": 0
  },
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "forward": {
      "parking": 0,
      "egress": 0,
      "idleSince": "2026-08-12T14:03:21.123456789Z"
    },
    "exec": {
      "parking": 0,
      "egress": 0,
      "idleSince": "2026-08-12T14:00:00Z"
    }
  }
}
```

`maxInflight` 由 Proxy master 从当前 applied route 的 effective policy 注入,不从 conductor
Sandbox row 推导;全零对象明确表示 unlimited。worker stats unavailable 时该 API 仍可返回 503,
但不影响 master route/admission authority 或 Create barrier。

顶层 `idleSince` 仅在 state=running 且所有 inflight 为零时返回;starting/paused 即使零连接
也不返回顶层时间。service 的 `idleSince` 也只在该 service 两项为零时出现。接口不返回
`idle`、`idleForSeconds`、last-open/close、累计连接数、bytes、延迟、端口明细或 worker
身份;`Cache-Control: no-store`.Proxy route 未完成同步,
RunID/profile/state 不匹配或 worker 集不可信时返回 503。state 参与 conductor→master
查询身份,避免 Pause 已提交但异步 route view 仍为 running 时返回旧的顶层 `idleSince`。

每个 worker 使用一条 Unix socketpair 上报:

```text
worker hot path
  update sharded local absolute state + revision
  mark SID/counter dirty + nonblocking notify
        │
        ▼ async single sender
[4-byte LE length][JSON hello/ready/update/remove/goodbye]
        │ absolute Prometheus counters + absolute per-SID traffic
        ▼
master: workerID/epoch/sequence/contribution → per-SID aggregate cache
        ├─ counter absolute delta → existing metrics.M
        └─ stats UDS batchGet → conductor public GET
```

热路径不写 socket。sender 可合并任意中间变化,写成功后只在 revision 未再次变化时清 dirty;
因此 notify 合并和背压不会丢最终绝对状态。frame 有 1 MiB、每帧 SID/counter 数和标识长度
上限;同 epoch 的 sequence 回退/跳号/异内容重用、counter 回退/遗漏或 malformed frame 都是
协议错误。

master 查询只读持续维护的聚合 cache,不在 GET 时扇出 worker。它用 Linux boottime 比较
`idleSince`,对外只输出 UTC wall time;有效值取 worker idle、当前 master/worker 集合可信起点
和当前 RunID 首次被观察为 running 的时间的最大值。worker stats stream 断开后立即进入 503 窗口并终止
worker;只有 `Wait` 确认进程退出、内核已关闭其 backend FD 后才删除该 worker 的全部贡献。
replacement 以新 epoch 发 hello+ready,在 ready 前不开放 stats,并且 worker 也是在 stats ready
后才开始 Serve 数据 listener。

worker 经 socketpair 把绝对状态交给 master,conductor 只经当前注册的 `stats_socket` 查询.
route SHM 仍是 master 单写,worker 只读,没有 stats 区或 worker 写入;worker 的 mutable admission
column 只存在于独立 arena,不进入会因 backshift 移动的 route record。

## 9. 可靠性

- **worker 崩溃**:stats fault先触发旧 worker 终止;master 等 `cmd.Wait` 后才清其 admission
  column并以新 epoch重启同 index。等待期间 stale-high 只会保守拒绝;其他 worker 继续
  accept 同一 listener fd,route/Sandbox state和Create barrier均不改变。崩溃 worker 上的已有
  连接由 kernel 关闭。
- **master 崩溃**:plugin 租约断开,新 Create 无法通过 barrier,DataEndpoint 也不可用;
  systemd 重启 master 后重新注册,重建共享表并启动 worker.已运行沙箱本身不受影响.
- **routesync 断开**:master 指数退避重连;固定数据面共享表沿用原有保留/Bookmark
  收敛语义,但 MMDS routes/value/service authority 立即清空并返回 503,完整同步 Bookmark
  前不服务旧 secret 或执行旧 service route。断连同时使尚未返回的 Create barrier 失败;
  已 ACK并完成 201 commit 后的断连按正常运行期 availability failure 处理。
- **park / wake**:Lookup 不发送 Wake;通过有效鉴权策略的 Activate 才能对 paused sid 发 Wake 并等待
  共享表更新。starting 只 park、不 Wake,变为 paused/Delete 时立即结束;resume ownership 和
  当前 launch 的状态推进仍由 conductor 执行。
- **stats stream**:任一 worker stream EOF、超时或协议错误都会停止该 worker;确认退出前
  traffic GET 返回 503,确认后删除其贡献并等待 replacement ready。Prometheus counter 在
  master 生命周期内保持单调,worker epoch 更换不会回退。
- **失败码**:Create 无可用 proxy route stream,barrier 超时/断连或 route apply
  失败 = 503;非法 target = 400;exec 的非 CONNECT method = 405;未知/已删除 sid = 404;
  鉴权失败 = 401;已识别但 profile 不支持的 service = 501;
  后端/proxy 未注册或不可达 = 502;已授权的 exec 恢复失败 = 503;ordinary admission 达限 =
  429 + `max_inflight_reached`,exec 达限 = CONNECT 200 后 generic ctl error。

## 10. 性能

- 普通数据面 route lookup 是 worker 本地 mmap hash 查找,不进 conductor,不跨进程 RPC;
  只有 guest 自定义 MMDS path 走同机 worker→master socketpair。
- unlimited traffic fast path 不访问 admission arena;limited flow 只扫描固定 `N × 4` absolute
  cells,不做 per-flow master RPC。不同 Sandbox 使用不同 worker-local mutex,不争用一把进程级
  global lock;同 SID并发只在本 worker entry和本 worker row上串行。
- master 单写 route共享表;worker 只读。admission worker只写自己的 column,master不在
  per-flow热路径中。
- 普通 HTTP 和 CONNECT 都不使用上游连接池,避免跨 sandbox/port 连接复用。
- `route_capacity` 是固定容量保护阈值;容量不足时应调大配置并重启 proxy master。
- worker 数据面 metrics 与 traffic 经统一 stats socketpair 异步上报绝对快照;
  `metrics_listen` 由 master 对 counter 绝对值求差后继续输出既有
  `data_requests_total{result=...}`。背压只合并中间 snapshot,不会永久丢失计数或当前 traffic。
- MMDS 来源 IP 反查使用按 IPv4 对 connector `MaxPorts` 取模定位的可重建固定槽位，再对主路由表
  中的候选 SID、active state 和精确 IP 进行权威验证，已不是线性扫描。提示缺失、不匹配或过时
  均 fail closed；取模冲突会替换旧来源的提示，不会将旧来源授权为新来源。提示不含凭据，
  也不是普通数据面的路由权威。token 签发使用来源反查，已认证 GET 校验 token 的来源 IP 与
  incarnation 绑定，自定义 GET 还会查询 master RPC。MMDS 不仅用于 envd 初始化。

这里的 running 只证明 orchestrator readiness wire 与 mandatory e2b `/init` 已成功,不保证
code interpreter、forward 业务端口或用户应用 health 已监听;业务 backend readiness 仍由
[#125](https://github.com/kuasar-sandbox/orchestrator/issues/125) 独立跟踪,proxy 不在本阶段
增加通用 dial retry。

## 11. See Also

- [node.md](node.md) — conductor 控制面,Proxy 部署,生命周期与密钥模型.
- [cluster-router_zh.md](cluster-router_zh.md) — 集群入口如何转发到本节点数据面。
- [Connector vSwitch](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch_zh.md) — mgmt-extract / MMDS VIP 转换。
- [部署文档](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment_zh.md) — 部署拓扑、端口与故障域。
