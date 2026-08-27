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
  └─ exec ──────────────────► <run_root>/<sid>/ctl.sock
```

### 1.1 设计原则

- **转发层与控制面分离**:proxy 只做路由判定、鉴权和字节转发;沙箱生命周期权威在
  conductor。
- **单订阅 master,多 worker 数据面**:external 模式只有 proxy master 注册
  config-socket plugin;worker 不连接 conductor,不持独立 routesync 订阅。
- **分离路由视图**:固定长度的数据面字段写共享内存,worker mmap 只读;可变长的
  `mmds_routes` 与 `mmds_route_secret_values` 只放 master 有界 heap,worker 经继承的
  本机 socketpair RPC 按 exact path 查询。secret plaintext 不进入 mmap。
- **listener fd 继承**:master 绑定 data/proxy/MMDS listener,把同一个 fd 传给所有
  worker;worker 执行 accept 和转发。后续可把 master bind 替换为 systemd socket
  activation,worker 模型不变。
- **转发 netns 可配置**:`proxy_netns` 指向 connector 管理平面 netns 时,proxy 到
  `floatingip:port` 的访问和 MMDS listener 都在该 netns。internal 模式用 per-dial
  netns dialer;external 模式让 worker 进程直接在该 netns 内运行。
- **无上游连接池**:普通 HTTP 每请求拨一次后端并关闭;CONNECT 是一条请求绑定一条
  TCP/UDS 连接。不同 sandbox/port 不复用上游连接。
- **鉴权先于生命周期副作用**:普通 HTTP 与 non-exec CONNECT 固定执行
  `LookupRoute → authorize(RouteBinding) → ActivateRoute → fresh Route → dial`。
  Lookup 不 Wake/Resume/park/dial;Activate 在生命周期副作用前后重验 binding。无效
  credential 不能唤醒或占用 traffic parking。
- **逻辑服务只影响 CONNECT**:普通 HTTP 不解析 `E2b-Sandbox-Service`,应用层
  Header 保持不变;CONNECT 中显式 service 是 backend 选择的权威输入.
- **Exec 先鉴权后激活**:`service=exec` 始终验证绑定 `AuthSandboxID` 的 KAT;
  CONNECT 200 后还必须授权完整首个 ExecRequest.失败请求不得触发 parking、Wake/resume
  或 backend dial.最终 node proxy 只将双重 gate 通过的请求交给 ctl tunnel helper,
  不向租户开放任意 UDS 或其它 ctl capability.
- **确定性 MMDS 密钥**:`MmdsSecret = MAC(manifest_key, sid)`,PUT 和 GET 即使落到
  不同 worker 也一致。

## 2. CLI

external 模式由一个 proxy master 进程启动:

```bash
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

master 内部 reexec 当前 `node-ctl` 二进制启动 worker;内部 worker 模式不作为运维接口。

`proxy.yaml` 字段:

| 字段 | 默认 | 说明 |
|---|---|---|
| `config_socket` | `/run/sandbox/node-ctl.socket` | conductor config-socket;master 在 plugin 平面注册并同步路由 |
| `paths.run_root` | (必填) | 本机 sandbox 运行目录根;external worker 本地构造 `<run_root>/<NodeSandboxID>/ctl.sock`,该路径不经 routesync `Policy` 或共享路由记录传递 |
| `data_listen` | 空 | 数据面入口;空 = 只接受 conductor proxyForwarder 兜底 UDS |
| `proxy_netns` | 空 | 转发平面 netns;空 = 当前 netns。非空时 external worker 在该 netns 内运行,conductor 下发的 MMDS listen 也在该 netns 绑定;`data_listen` 仍在 master 当前 netns |
| `proxy_socket` | `<dir(config_socket)>/proxy.sock` | master 注册给 conductor proxyForwarder 的 UDS |
| `stats_socket` | `<dir(config_socket)>/proxy-stats.sock` | master 独占监听并注册给 conductor 的 traffic stats UDS;必须是绝对路径且不得与 config/proxy/SHM 路径冲突,权限 0600 |
| `shm_path` | `<dir(config_socket)>/proxy-routes.shm` | 共享路由表 mmap 文件 |
| `route_capacity` | `65536` | 固定路由槽位数;满时 Upsert 失败并终止当前 routesync session,等待该 route 的 Create 返回 503 |
| `workers` | `1` | worker 进程数 |
| `tls` | 空 | 数据面 TLS `{cert,key}`;空 = h2c |
| `auth` | `enforce` | routesync policy 到达前的数据面鉴权回退值 |
| `park_timeout` | `30s` | routesync policy 到达前的 park 回退值 |
| `metrics_listen` | 空 | master Prometheus 文本端点,聚合 worker 数据面计数 |

`proxy.yaml` 不含 `mmds_listen` 或 `services`:两者唯一来源是 conductor
`mmds.listen` / `mmds.services`,经可信 plugin registration 的 `Hello{Policy}` 下发。

## 3. 部署模式

`proxy.mode` 选择 conductor 如何装配数据面:

| 模式 | 数据面承载 | 进程 |
|---|---|---|
| `internal` | conductor 进程内 proxy | `node-ctl conductor serve` |
| `external` | proxy master + worker | `node-ctl conductor serve` + `node-ctl proxy serve` |
| `off` | 拒绝数据面请求 | conductor 返回 501 |

external 拓扑:

```text
                    config-socket plugin stream
                    register(proxy_socket, stats_socket, route_wake)
         Wake / BarrierAck(sid/id) ▲ │ Hello / Upsert / Delete / Bookmark / Barrier
                              │     ▼
node-ctl conductor serve ─────┴── node-ctl proxy master
        ▲ fallback CONNECT          ├─ fixed route writer ─► shared route mmap
        │ traffic GET ──────────────► cached aggregate
        │ via proxy_socket          ├─ MMDS routes/values ─► bounded heap
        │                           ├─ stats UDS (master only)
        │                           └─ one stats socketpair / worker ◄────┐
client ─┴────────► inherited data/MMDS listener ─► proxy worker[0..N)
                                                     ├─ mmap + MMDS RPC
                                                     └─ absolute counters + traffic ─┘
```

要点:

- conductor 只看到一个 proxy plugin id,当前实现固定为 `proxy`。
- `proxy_socket` 是 conductor fallback 的唯一注册目标;data-plane 请求误打到
  conductor 监听口时,proxyForwarder 通过这个 UDS 发 chained CONNECT。
- `stats_socket` 只由 master 监听;conductor 的公开 traffic GET 经该 UDS 读 master
  聚合缓存,不会查询时扇出 worker。
- worker 不注册 plugin,不保存独立全量路由表;崩溃后由 master 重启,重启后直接读取
  当前共享表。
- master 退出会带走其 worker;systemd 重启 master 后重新注册并重建共享表。
- plugin id 必须精确为 `proxy`,且 registration 同时满足
  `subscribe.kind=route_wake`、`proxy!=nil`、`mmds=true`,conductor 才投影 MMDS policy、
  routes 和 secret values;普通 observer 与 node-link 均收不到这些 confidential values。

## 4. routesync 与共享路由视图

routesync 仍是帧化 JSON over h2c,由 proxy master 拨 conductor:

```text
master → conductor : register{subscribe: route_wake, proxy{socket,stats_socket}, mmds}
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

external Create 使用同一有序 stream 建立 route-applied barrier:

```text
conductor: Upsert(initial starting) → route_barrier{id}
master:    ApplyUpsert succeeds → notify workers → route_barrier_ack{id}
```

master 的 ACK 只表示 parking 所需的 `starting RouteBinding` 已进入共享表,不表示 sandbox
已 running、backend 可拨或 envd 已完成 `/init`。Wake 与 BarrierAck 由一个上行 writer 串行
写帧。任一先行 Upsert 写表失败时,subscriber 不发送 ACK并终止 session;conductor 将等待中的
Create 回滚并返回 503。barrier id 只存在于本次内存协调,不进入 route changelog、共享表、
Sandbox schema 或日志字段。

当前只有固定 plugin id `proxy` 的一个 master participant。Create 在 ACK 后再次核验该
registration epoch 仍为当前租约;断连、同 id replacement、迟到或旧 session ACK 都不能完成
barrier。完成规则内部按 all-of participant set 实现,不采用 quorum;若以后显式配置多个
traffic-serving master,必须全部 ACK 后才能返回 201。

MMDS 扩展不写固定表。master 对每个 active sandbox 在一个锁内替换 routes + values;
heap entry 总数受 `route_capacity` 限制。`BeginSync`、routesync 断开和 worker/master 重启
都会立即清空 heap 并标记 unavailable,只有完整 `Bookmark` 后才重新开放查询。service
registry 由每次 `Hello{Policy}` 原子整表替换。running/starting Upsert 先更新 heap 再公开
SHM route;paused/Delete 先撤销 heap 再更新 SHM,让已采样旧 active row 的 worker 也 fail closed,
避免把新生命周期与旧 secret
组合。`RunID` 进入固定表用于 MMDSv2 token 的 incarnation 绑定;routes/value plaintext
绝不进入固定记录、metrics 或日志。

本文中 `RouteEntry.SandboxID`、`sid` 和共享表 key 均是 node-local SandboxID.集群路径下,它们是
Registry 分配的 NodeSandboxID;cluster Router 已在进入 node 之前把公开稳定 SandboxID 转换为该值.

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
在 Delete/新 Upsert 尚未到达 external proxy 的极短窗口内,worker 仍可能持有旧实例的
凭据投影和同名运行目录。external 模式不得主动执行这种跨逻辑沙箱的即时 ID 复用;
为不同逻辑沙箱显式指定迁移 target 时应使用新的 NodeSandboxID,或先确认路由视图已经收敛。

受保护 `RouteEntry` 的 state 为 `starting|running|paused|dead`,并显式携带
`AuthSandboxID`、`APISecret`、`APISecretFingerprint`、
`ManifestKeyFingerprint`、`ServiceSecret`、`EnvdAccessToken`、`TrafficAccessToken` 和
`ForwardAccessToken`。
ManifestKey 原文不进入路由。节点 proxy 转发时只按目标选择 EnvdAccessToken 或
ForwardAccessToken;TrafficAccessToken 仅随受保护视图投影给外部网关及 e2b 数据面组件,
不由 node 平台层消费.`AuthSandboxID + ServiceSecret` 用于验证 exec KAT,
其中共享表 key 和本地运行目录仍只使用 NodeSandboxID.既有 `MmdsSecret` 独立服务于
MMDS token 签名,不等于 route secret values;后者仅经上述可信投影进入 master heap。

初始 durable starting upsert 可以没有 FloatingIP、UDS 或其它 backend endpoint;worker 按
state park,绝不尝试使用这些空字段。node 持久化 network ownership 并完成 YAML/ready.sock
后会发布 enriched starting,此时 MMDS 才能按 FloatingIP 反查身份。ordinary data plane 仍
须等 running。

## 5. 转发路径

普通 HTTP 按 `Host: <port>-<sid>.<domain>` 或 `E2b-Sandbox-Id` /
`E2b-Sandbox-Port` 解析 `(sid, port)`.它不解析 `E2b-Sandbox-Service`;该 Header 作为
应用层 Header 原样转发,不改变 backend.CONNECT 解析 `(sid, service?, port?)`.
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
| `exec` | e2b / bare | `<run_root>/<NodeSandboxID>/ctl.sock` | 可携带,但不参与 backend 选择 |

bare 显式请求 `e2b:envd` 或 `e2b:code-interpreter` 返回 501.unknown/空 service 返回 400.
service 与 port 并存不是冲突;Node 不会用 49983/49999 反向覆盖显式 service.

普通 HTTP:

1. worker 只读共享表,得到不含 backend 的 `RouteBinding`;
2. 按目标选择 EnvdAccessToken 或 ForwardAccessToken,校验 `X-Access-Token`;符合条件的
   `/files` 请求也可使用 EnvdAccessToken 验证 signature;
3. 鉴权成功后进入 `ActivateRoute`:在 Wake/等待前重验 binding,完成生命周期动作后再
   重验一次,并从最新 running route 构造最终 backend;binding 改变时 fail closed;
4. 拨一次 envd UDS 或 `floatingip:port`。配置 `proxy_netns` 时,`floatingip:port` 在该
   netns 内拨号;
5. 写入一条 HTTP 请求,流式复制响应,响应结束关闭后端连接。

CONNECT:

- sandbox id 来自 `E2b-Sandbox-Id` 或 legacy authority label;
- legacy/`forward` 可从 CONNECT authority 取实际 port;无端口逻辑服务的 authority
  只是 transport 占位,不会生成 `E2b-Sandbox-Port`;
- 普通 forward/envd/CI 目标鉴权成功后,把客户端连接与后端连接双向 splice;
- `service=exec` 只接受 CONNECT;普通 HTTP 携带该 service 返回 405,且不触发恢复.

node proxy 的 exec 路径分为三个有序阶段:

1. CONNECT 200 前只做无副作用本地 `LookupExec`,以 route 中的
   `AuthSandboxID + ServiceSecret` 严格验证 `X-Access-Token` KAT,并在 HMAC 验证成功后
   编译/读取有界缓存中的 CEL programs.该阶段不 parking、不 activation、不拨
   `ctl.sock`;token/identity/expiry/conditions 失败以 HTTP 400/401/404/501 结束.
2. 返回并 flush CONNECT 200 后,在固定 10 秒 first-request timeout 内读取完整首个 ctl
   frame.解析严格覆盖顶层 `exec_request`、`ExecSpec` 和 `StdioSpec`,保留客户端原始
   4-byte little-endian length + JSON bytes,重新检查 expiry,构造规范化 request view 并
   以 AND 执行全部 conditions.false、error、unknown、cost exceeded 或 cancel 都 fail closed.
3. 只有 request admission 成功后才 `BeginParking → ActivateExec → exact identity recheck →
   ctl.sock dial → AttachBackend`,然后把首帧 Raw 原样写入一次并进入双向 relay.首帧一旦
   写入 backend 就不 retry、reroute 或 replay.

CEL view 将 nil argv/env 规范化为 `[]`/`{}`,空 cwd 与 `/` 规范化为 `/`,user 保留请求原值.
TTY 模式中 stdin/stdout/stderr flags 沿用 ctl wire 的 ignored 语义,view 暴露规范化后的有效
语义,不会因 flags 同时出现而拒绝合法请求.条件或结构 gate 失败不改变 parking/activity,
不启动 guest child;因此也不会使 paused sandbox 恢复.

internal 模式的 `run_root` 取自 conductor `paths.run_root`;external 模式的
worker 直接读取自身 `proxy.yaml` 必填的 `paths.run_root`.该值应与同节点
conductor 的 `paths.run_root` 一致.routesync `Policy` 和共享路由视图只提供
路由,凭据及鉴权策略,不投影 `ctl.sock` 路径.

共享的 sandboxer tunnel helper 不理解 KAT、CEL、route 或 lifecycle;它只冻结 callback 顺序、
严格首帧读取、Raw 单次转发和 half-close relay.H1 从 Hijack 返回的 buffered reader 继续读,
H2 从 request body 读并及时 flush response;两者都保留 half-close,等待双向 relay 结束.
CONNECT 200 后,已完整识别的 request denial 或 backend failure 返回统一脱敏 ctl frame
`{"type":"error","msg":"exec request rejected"}`;framing 无法恢复时直接关闭 tunnel.

KAT 在 CONNECT admission 时校验,并在首帧授权时重新检查 expiry;进入 backend relay 后
过期不强制断开已建立 tunnel,有效期内同一 KAT
可以建立多条独立 CONNECT.每条 tunnel 只承载一个 ctl exec session,不复用 backend 连接;
新 CONNECT 在 route 切换后自动进入当前 NodeSandboxID,已建立 tunnel 不迁移.

external worker 同样在本进程执行上述完整 token + request + backend gate,用
`proxy.yaml` 的 `paths.run_root` 构造 `ctl.sock` 路径.路径和 CEL programs 不经 routesync
`Policy`,SHM 记录或 conductor proxyForwarder 投影;proxyForwarder 仅透传同一 exec target、
客户端 KAT 和字节流,不解析 KAT/CEL/ExecRequest.

proxyForwarder:

- conductor 收到数据面请求但处于 external 模式时,不会自己查路由;
- 它向 `proxy_socket` 发 chained CONNECT,显式携带 sid,可选 service/port,并原样携带
  客户端的 `X-Access-Token`;
- 普通 HTTP 在该 CONNECT 隧道里发送一条请求;CONNECT 则继续隧道化到沙箱。

proxyForwarder 和 cluster-router 的 chained CONNECT 都只是中继,不重复计数;traffic
统计只发生在建立最终 sandbox backend 的 node worker。

## 6. 数据面鉴权

数据面请求头统一为 `X-Access-Token`,但期望值按转发目标选择:

- e2b legacy 49983/49999 以及显式 `e2b:envd`/`e2b:code-interpreter` 使用 create
  响应中的 `envdAccessToken`;
- bare 的任意 legacy 端口,e2b 的其它 legacy 端口和显式 `forward` 使用
  `forwardAccessToken`;
- `trafficAccessToken` 只供外部网关及 e2b 数据面组件验证,node proxy 不消费;
- `exec` 只接受以 ServiceSecret 直接 HMAC 签名,绑定 `AuthSandboxID` 且
  `aud=exec` 的 `kat1` ExecAccessToken.Envd/Forward/Traffic token 不能代替它.

opaque Envd/Forward token 按各自线格式校验;exec KAT 执行严格格式,签名,SID,audience
和可选过期时间校验.

`auth` / policy `auth_mode`:

| 模式 | 行为 |
|---|---|
| `enforce` | 不匹配返回 401 |
| `log` | 记录但放行 |
| `off` | 不校验 |

上表只适用普通数据面.Exec 始终 enforce,不受 `auth_mode` 影响.

e2b 49983 上的 `GET/POST /files` 在未携带 `X-Access-Token` 时,可用
EnvdAccessToken 验证 envd signature query;proxy 先验签再转发,envd 收到原始请求后再次
验证同一 signature。如果请求携带非空但错误的 `X-Access-Token`,不回退 signature。

## 7. MMDS

`mmds.enabled=true` 时,envd 在 FC 模式下通过 Firecracker MMDS v2 获取当前身份的
access-token hash;`mmds.routes.enabled=true` 还开放显式声明的 static/secret/service
exact route。internal 模式由 conductor 进程内 handler 直接读取 sqlite/service registry;
external 模式由 worker 承载 HTTP,master 提供有界 route view。配置 `proxy_netns` 时,
conductor 的 `mmds.listen` 在该 netns 绑定;external master 把同一个 listener fd 传给
所有 worker,worker 不读取第二份 MMDS YAML。

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

两段式协议:

1. `PUT /latest/api/token`:要求恰好一个 `X-metadata-token-ttl-seconds`,值为
   `1..21600`;按请求源 IP 查 enriched starting/running route 的 floatingip。初始
   starting 尚无 FloatingIP时仅此 token mint 路径可按 `park_timeout` 等待。返回的
   HMAC token 绑定 sid、来源 IP、当前 `RunID`、`aud=mmds` 和 expiry。
2. `GET /`:重新校验签名、来源、expiry、audience 与当前 `RunID`,返回
   `{instanceID, envID, accessTokenHash}`。pause/resume 改变 incarnation,旧 token 立即失效。
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
- `service`:master/internal conductor 从唯一 registry 解析本机 Unix socket,worker/handler
  构造全新 `GET <exact-path> HTTP/1.1`,`Host: mmds-service`,仅增加
  `E2b-Sandbox-Id: <sid>` 与 `E2b-Sandbox-Service: <service>`。不发送 port,不透传 guest
  Host/query/body/token/Authorization/Cookie 或任何 guest header。V1 只透传合法 status、
  有界 body 和合法 Content-Type(缺省 `text/plain`),不跟随 redirect;超时/过大/非法响应
  映射 504/502,service 缺失或 socket 不可达为 503。

安全边界:routes 是 portable declaration,secret values 则只存在 sqlite ciphertext、internal
conductor 内存或受信 external master 的有界 heap。它们不写普通 metadata、共享 mmap、
日志、metrics、migration token、template 或构建产物,也不发送给 observer/node-link。
但 guest 主动 GET 后,value 已进入 guest/application memory;随后执行包含内存的 Pause/snapshot
可能把该副本作为普通 guest working set 捕获。平台不能在宿主侧从任意 guest 内存中擦除它,
调用方应在应用侧缩短驻留时间,并把包含已消费 secret 的 snapshot 按敏感制品保护。

## 8. per-sandbox traffic stats 与统一 worker stream

公开接口为 `GET /sandboxes/{sid}/stats/traffic`。统计的是最终 node proxy 已鉴权接纳的
逻辑 ingress,不是客户端物理 TCP 数:

```text
ingress = parking + egress

parking: token 和 ExecRequest admission 成功后,ActivateRoute/ActivateExec 与最终 backend dial 尚未完成
egress:  最终 node proxy → sandbox backend 已建立且尚未最终 Close
```

service 固定为 `forward`、`e2b:envd`、`e2b:code-interpreter`、`exec`。e2b 返回四项,
bare 只返回 forward/exec。普通 HTTP 和每条 CONNECT/exec 各是一条逻辑 ingress。dial
成功时在同一 worker-local entry lock 中原子执行 `parking--/egress++`;activation 或 dial
失败只结束 parking。`CloseWrite` 只传播 half-close,不结束 egress;只有 tracked backend
的最终 `Close` 以 `sync.Once` 结束 egress。token 或 ExecRequest admission 失败不进入
parking/egress,也不刷新 sandbox activity/`idleSince`。

空闲响应示例:

```json
{
  "state": "running",
  "inflight": {"parking": 0, "egress": 0},
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "forward": {"parking": 0, "egress": 0, "idleSince": "2026-08-12T14:03:21.123456789Z"},
    "exec": {"parking": 0, "egress": 0, "idleSince": "2026-08-12T14:00:00Z"}
  }
}
```

顶层 `idleSince` 仅在 state=running 且所有 inflight 为零时返回;starting/paused 即使零连接
也不返回顶层时间。service 的 `idleSince` 也只在该 service 两项为零时出现。接口不返回
`idle`、`idleForSeconds`、last-open/close、累计连接数、bytes、延迟、端口明细或 worker
身份;`Cache-Control: no-store`。`proxy.mode=off` 返回 501;external route 未完成同步、
RunID/profile/state 不匹配或 worker 集不可信时返回 503。state 参与 conductor→master
查询身份,避免 Pause 已提交但异步 route view 仍为 running 时返回旧的顶层 `idleSince`。

external 模式用每 worker 一条 Unix socketpair 统一替换旧 lossy metrics pipe:

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

internal 模式复用同一 `WorkerStats → MasterStats` 状态机,只是 frame 在进程内应用;external
模式经 socketpair 和 `stats_socket`;off 不提供统计。route SHM 仍是 master 单写、worker
只读,没有 stats 区或 worker 写入。

## 9. 可靠性

- **worker 崩溃**:master 发现子进程退出并重启;其他 worker 继续 accept 同一 listener
  fd。崩溃 worker 上的已有连接断开。
- **master 崩溃**:plugin 租约断开,conductor proxyForwarder 失去目标;systemd 重启
  master 后重新注册、重建共享表并启动 worker。已运行沙箱不受影响。
- **routesync 断开**:master 指数退避重连;固定数据面共享表沿用原有保留/Bookmark
  收敛语义,但 MMDS routes/value/service authority 立即清空并返回 503,完整同步 Bookmark
  前不服务旧 secret 或执行旧 service route。断连同时使尚未返回的 Create barrier 失败;
  已 ACK并完成 201 commit 后的断连按正常运行期 availability failure 处理。
- **park / wake**:Lookup 不发送 Wake;已鉴权 Activate 才能对 paused sid 发 Wake 并等待
  共享表更新。starting 只 park、不 Wake,变为 paused/Delete 时立即结束;resume ownership 和
  当前 launch 的状态推进仍由 conductor 执行。
- **stats stream**:任一 worker stream EOF、超时或协议错误都会停止该 worker;确认退出前
  traffic GET 返回 503,确认后删除其贡献并等待 replacement ready。Prometheus counter 在
  master 生命周期内保持单调,worker epoch 更换不会回退。
- **失败码**:external Create 无可用 proxy route stream、barrier 超时/断连或 route apply
  失败 = 503;非法 target = 400;exec 的非 CONNECT method = 405;未知/已删除 sid = 404;
  鉴权失败 = 401;已识别但 profile/当前 proxy 模式不支持的 service 或 off = 501;
  后端/proxy 未注册或不可达 = 502;已授权的 exec 恢复失败 = 503.

## 10. 性能

- 普通数据面 route lookup 是 worker 本地 mmap hash 查找,不进 conductor,不跨进程 RPC;
  只有 guest 自定义 MMDS path 走同机 worker→master socketpair。
- master 单写共享表;worker 只读,无 worker 间锁竞争。
- 普通 HTTP 和 CONNECT 都不使用上游连接池,避免跨 sandbox/port 连接复用。
- `route_capacity` 是固定容量保护阈值;容量不足时应调大配置并重启 proxy master。
- worker 数据面 metrics 与 traffic 经统一 stats socketpair 异步上报绝对快照;
  `metrics_listen` 由 master 对 counter 绝对值求差后继续输出既有
  `data_requests_total{result=...}`。背压只合并中间 snapshot,不会永久丢失计数或当前 traffic。
- MMDS 按 floatingip 反查当前实现为共享表线性扫描,该路径只在 envd 初始化时使用,
  不在高 QPS 数据面热路径。

这里的 running 只证明 orchestrator readiness wire 与 mandatory e2b `/init` 已成功,不保证
code interpreter、forward 业务端口或用户应用 health 已监听;业务 backend readiness 仍由
[#125](https://github.com/kuasar-sandbox/orchestrator/issues/125) 独立跟踪,proxy 不在本阶段
增加通用 dial retry。

## 11. See Also

- [node.md](node.md) — conductor 控制面、`proxy.mode` 装配、生命周期与密钥模型。
- [cluster-router.md](cluster-router.md) — 集群入口如何转发到本节点数据面。
- `connector/docs/vswitch.md` — mgmt-extract / MMDS VIP 转换。
- `kuasar-sandbox/docs/deployment.md` — 部署拓扑、端口与故障域。
