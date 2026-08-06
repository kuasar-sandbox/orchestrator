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
- **共享只读路由视图**:master 是唯一写者,把 routesync 更新写入共享内存;worker
  mmap 只读,数据面请求零 RPC 查路由。
- **listener fd 继承**:master 绑定 data/proxy/MMDS listener,把同一个 fd 传给所有
  worker;worker 执行 accept 和转发。后续可把 master bind 替换为 systemd socket
  activation,worker 模型不变。
- **转发 netns 可配置**:`proxy_netns` 指向 connector 管理平面 netns 时,proxy 到
  `floatingip:port` 的访问和 MMDS listener 都在该 netns。internal 模式用 per-dial
  netns dialer;external 模式让 worker 进程直接在该 netns 内运行。
- **无上游连接池**:普通 HTTP 每请求拨一次后端并关闭;CONNECT 是一条请求绑定一条
  TCP/UDS 连接。不同 sandbox/port 不复用上游连接。
- **逻辑服务只影响 CONNECT**:普通 HTTP 不解析 `E2b-Sandbox-Service`,应用层
  Header 保持不变;CONNECT 中显式 service 是 backend 选择的权威输入.
- **Exec 先鉴权后激活**:`service=exec` 始终验证绑定 `AuthSandboxID` 的 KAT;
  失败请求不得触发 Wake/resume.最终 node proxy 只将已授权连接交给
  `ctl.ProxyExec`,不向租户开放任意 UDS 或其它 ctl capability.
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
| `proxy_netns` | 空 | 转发平面 netns;空 = 当前 netns。非空时 external worker 在该 netns 内运行,`mmds_listen` 也在该 netns 绑定;`data_listen` 仍在 master 当前 netns |
| `proxy_socket` | `<dir(config_socket)>/proxy.sock` | master 注册给 conductor proxyForwarder 的 UDS |
| `shm_path` | `<dir(config_socket)>/proxy-routes.shm` | 共享路由表 mmap 文件 |
| `route_capacity` | `65536` | 固定路由槽位数;满时新路由写入失败并告警 |
| `workers` | `1` | worker 进程数 |
| `tls` | 空 | 数据面 TLS `{cert,key}`;空 = h2c |
| `auth` | `enforce` | routesync policy 到达前的数据面鉴权回退值 |
| `park_timeout` | `30s` | routesync policy 到达前的 park 回退值 |
| `mmds_listen` | 空 | external MMDS 监听;空 = 不启动 MMDS |
| `metrics_listen` | 空 | master Prometheus 文本端点,聚合 worker 数据面计数 |

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
                    register(proxy_socket, route_wake)
                    Wake(sid) ▲     │ Hello / Upsert / Delete / Bookmark
                              │     ▼
node-ctl conductor serve ─────┴── node-ctl proxy master
        ▲ fallback CONNECT          │ single writer
        │ via proxy_socket          ▼
        │                   shared route mmap
        │                          ▲
        │                          │ read-only mmap
client ─┴────────► inherited data listener ─► proxy worker[0..N)
```

要点:

- conductor 只看到一个 proxy plugin id,当前实现固定为 `proxy`。
- `proxy_socket` 是 conductor fallback 的唯一注册目标;data-plane 请求误打到
  conductor 监听口时,proxyForwarder 通过这个 UDS 发 chained CONNECT。
- worker 不注册 plugin,不保存独立全量路由表;崩溃后由 master 重启,重启后直接读取
  当前共享表。
- master 退出会带走其 worker;systemd 重启 master 后重新注册并重建共享表。

## 4. routesync 与共享路由视图

routesync 仍是帧化 JSON over h2c,由 proxy master 拨 conductor:

```text
master → conductor : register{subscribe: route_wake, proxy{socket}, mmds}
master → conductor : wake{sid}
conductor → master : hello{policy}
conductor → master : upsert* → bookmark → upsert/delete...
```

master 把下行路由流投影到共享内存:

- `BeginSync` 开启新同步世代;
- `Upsert` 写入或更新 `sid` 槽位;
- `Delete` 把槽位置为 tombstone,保持开放寻址探测链;
- `Bookmark` 清理本世代未出现的旧记录,并标记首轮同步完成;
- `Policy` 写入共享头部,worker 每请求读取当前 `auth_mode` / `park_timeout_ms`。

本文中 `RouteEntry.SandboxID`、`sid` 和共享表 key 均是 node-local SandboxID.集群路径下,它们是
Registry 分配的 NodeSandboxID;cluster Router 已在进入 node 之前把公开稳定 SandboxID 转换为该值.

共享表是固定容量开放寻址 hash 表。master 单写;每条记录带 seqlock,worker 读取时若遇到
写中状态或版本变化会重试,不会看到半条路由。worker 只依赖共享表本地读取:

```text
sid hash ─► record slot ─► RouteEntry
                         ├─ running → 立即转发
                         ├─ starting → MMDS 可见;数据面 park,不发 Wake;回滚即结束
                         └─ missing/paused → wake pipe → park 等待共享表更新
```

worker 对 missing/paused sid 写 wake pipe 给 master;starting 已由 conductor launch owner
推进,worker 只等待 running/delete/paused 更新,不得再发 Wake;后两种回滚更新立即结束
starting 请求。master 去重后通过 routesync
上行 `Wake`。master 每次写共享表后通过 notify pipe 唤醒 worker 本地 park waiters。
全局 revision/notify 只负责唤醒检查;worker 以该 SID 槽位(含 Delete tombstone)的 revision
判断 Wake 是否已收到终态回应。因此即使异步共享表收敛把中间 starting 与随后
paused/Delete 合并,也不会漏掉 rollback 后继续消耗完整 park timeout。一个请求只允许在
初始 missing/paused 发一次 Wake;观察过 starting 后回到 paused 不得再次 Wake。

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
MMDS,不充当上述任一 token.

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

1. worker 从共享表解析 route;
2. 按目标选择 EnvdAccessToken 或 ForwardAccessToken,校验 `X-Access-Token`;符合条件的
   `/files` 请求也可使用 EnvdAccessToken 验证 signature;
3. 拨一次 envd UDS 或 `floatingip:port`。配置 `proxy_netns` 时,`floatingip:port` 在该
   netns 内拨号;
4. 写入一条 HTTP 请求,流式复制响应,响应结束关闭后端连接。

CONNECT:

- sandbox id 来自 `E2b-Sandbox-Id` 或 legacy authority label;
- legacy/`forward` 可从 CONNECT authority 取实际 port;无端口逻辑服务的 authority
  只是 transport 占位,不会生成 `E2b-Sandbox-Port`;
- 普通 forward/envd/CI 目标鉴权成功后,把客户端连接与后端连接双向 splice;
- `service=exec` 只接受 CONNECT;普通 HTTP 携带该 service 返回 405,且不触发恢复.

node proxy 的 exec 路径先做无副作用本地查找,以 route 中的
`AuthSandboxID + ServiceSecret` 严格验证 `X-Access-Token` KAT.仅验证成功后才可以
resume paused sandbox;若 route 已是 starting,则不再 Wake,只等当前 launch 完成。恢复后
重读 NodeSandboxID 和 credential identity,二者必须与鉴权时
一致.然后拨 `<run_root>/<NodeSandboxID>/ctl.sock`,发送并 flush CONNECT 200,将两个 stream
的所有权交给 `sandboxer/pkg/ctl.ProxyExec`.

internal 模式的 `run_root` 取自 conductor `paths.run_root`;external 模式的
worker 直接读取自身 `proxy.yaml` 必填的 `paths.run_root`.该值应与同节点
conductor 的 `paths.run_root` 一致.routesync `Policy` 和共享路由视图只提供
路由,凭据及鉴权策略,不投影 `ctl.sock` 路径.

`ProxyExec` 只接受第一个 ctl frame 为 `exec_request`,复用 `ctl.MaxMessageBytes`,保留原始
4-byte little-endian length + JSON bytes,然后透明中继 ctl/MUX 流.H1 从 Hijack 返回的 buffered reader 继续读,
H2 从 request body 读并及时 flush response;两者都保留 half-close,等待双向 relay 结束.
CONNECT 200 后发现 malformed,oversized,truncated 或非 exec 首帧时只关闭 tunnel,
不再合成 HTTP/ctl error.

KAT 只在 CONNECT admission 时校验;过期不强制断开已建立 tunnel,有效期内同一 KAT
可以建立多条独立 CONNECT.每条 tunnel 只承载一个 ctl exec session,不复用 backend 连接;
新 CONNECT 在 route 切换后自动进入当前 NodeSandboxID,已建立 tunnel 不迁移.

external worker 同样在本进程完成 KAT gate,用 `proxy.yaml` 的 `paths.run_root`
构造 `ctl.sock` 路径并进入 `ProxyExec`.路径不经 routesync `Policy`,SHM 记录或
conductor proxyForwarder 投影;proxyForwarder 仅透传同一 exec target 和客户端 KAT.

proxyForwarder:

- conductor 收到数据面请求但处于 external 模式时,不会自己查路由;
- 它向 `proxy_socket` 发 chained CONNECT,显式携带 sid,可选 service/port,并原样携带
  客户端的 `X-Access-Token`;
- 普通 HTTP 在该 CONNECT 隧道里发送一条请求;CONNECT 则继续隧道化到沙箱。

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
access-token hash。internal 模式由 conductor 进程内服务;external 模式由 proxy
worker 层服务。配置 `proxy_netns` 时,`mmds.listen` / `mmds_listen` 在该 netns 绑定;
external 模式由 master 绑定后把同一个 listener fd 传给所有 worker。

```text
guest envd
  │ 169.254.169.254:80
  ▼
connector mgmt-extract
  │ mmds_listen
  ▼
proxy worker ─► shared route view
```

两段式协议:

1. `PUT /latest/api/token`:按请求源 IP 查 enriched starting/running route 的 floatingip;
   初始 starting 尚无 FloatingIP,按 `park_timeout` 等待后续 route,超时 503。命中后返回
   `<sid>.<hmac>` session token。
2. `GET /`:校验 `X-metadata-token`,返回 `{instanceID, envID, accessTokenHash}`。

MMDS session token 使用每沙箱确定性 `mmds_secret`,因此 PUT 和 GET 落到不同 worker
仍能互相验证。

## 8. 可靠性

- **worker 崩溃**:master 发现子进程退出并重启;其他 worker 继续 accept 同一 listener
  fd。崩溃 worker 上的已有连接断开。
- **master 崩溃**:plugin 租约断开,conductor proxyForwarder 失去目标;systemd 重启
  master 后重新注册、重建共享表并启动 worker。已运行沙箱不受影响。
- **routesync 断开**:master 指数退避重连;重连后重新同步。共享表在重同步期间保留旧
  路由,Bookmark 后清除断连期间删除的记录。
- **park / wake**:worker 对 missing/paused sid 发送 wake 并等待共享表更新;starting
  只 park、不 Wake,变为 paused/Delete 时立即结束;resume ownership 和当前 launch 的状态推进
  仍由 conductor 执行。
- **失败码**:非法 target = 400;exec 的非 CONNECT method = 405;未知/未就绪 sid = 404;
  鉴权失败 = 401;已识别但 profile/当前 proxy 模式不支持的 service 或 off = 501;
  后端/proxy 未注册或不可达 = 502;已授权的 exec 恢复失败 = 503.

## 9. 性能

- 数据面 route lookup 是 worker 本地 mmap hash 查找,不进 conductor,不跨进程 RPC。
- master 单写共享表;worker 只读,无 worker 间锁竞争。
- 普通 HTTP 和 CONNECT 都不使用上游连接池,避免跨 sandbox/port 连接复用。
- `route_capacity` 是固定容量保护阈值;容量不足时应调大配置并重启 proxy master。
- worker 数据面 metrics 经继承 pipe 上报给 master,`metrics_listen` 输出聚合后的
  `data_requests_total{result=...}`;队列饱和时优先保护数据面,可能丢弃个别 metrics 增量。
- MMDS 按 floatingip 反查当前实现为共享表线性扫描,该路径只在 envd 初始化时使用,
  不在高 QPS 数据面热路径。

这里的 running 只证明 orchestrator readiness wire 与 mandatory e2b `/init` 已成功,不保证
code interpreter、forward 业务端口或用户应用 health 已监听;业务 backend readiness 仍由
[#125](https://github.com/kuasar-sandbox/orchestrator/issues/125) 独立跟踪,proxy 不在本阶段
增加通用 dial retry。

## 10. See Also

- [node.md](node.md) — conductor 控制面、`proxy.mode` 装配、生命周期与密钥模型。
- [cluster-router.md](cluster-router.md) — 集群入口如何转发到本节点数据面。
- `connector/docs/vswitch.md` — mgmt-extract / MMDS VIP 转换。
- `orchestrator/release-builder/docs/deployment.md` — 部署拓扑、端口与故障域。
