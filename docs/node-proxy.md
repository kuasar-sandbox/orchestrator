# node-proxy — 节点数据面转发层

## 1. 概述

数据面 proxy 是沙箱流量的 **L7 反代**:把外部(e2b SDK/CLI 或端口转发)请求按
`(sid, port)` 路由到目标沙箱的 guest envd 或 floatingip 用户端口。控制面(e2b API、
生命周期、密钥、模板构建)由 `node-ctl conductor serve` 承载,见
[node.md](node.md);本文档只讲数据面**转发层**:路由判定、部署形态、
路由分发协议(routesync)、数据面鉴权、envd 鉴权姿态与 CONNECT 隧道。

数据面与控制面共用同一对外端口(默认 443)与同一张通配 TLS 证书,按 `Host` /
`E2b-Sandbox-*` 头区分控制面(`api.<domain>`)与数据面(`<port>-<sid>.<domain>`)。
集群下,cluster-ctl router 把机群数据面转发进本节点(注入 `E2b-Sandbox-Id` +
`X-Access-Token`),proxy 照常按 sid 寻址,**无逻辑改动**(cluster-router.md)。

```text
cluster router
  │ E2b-Sandbox-Id + X-Access-Token
  ▼
node proxy
  │ local route table only
  ├─ e2b 49983/49999 ─► envd UDS
  └─ user port ───────► floatingip:port
```

### 1.1 设计原则

- **转发层与控制面分离**:proxy 只做"路由判定 + 鉴权 + 转发",不持久化、不调度;
  本节点路由权威在 serve,proxy 持只读缓存。
- **拓扑反转,worker 主动注册**:外置 worker 拨 serve 注册并拉路由,serve
  从不主动拨 worker——数据面字节流不经 serve。
- **零 gRPC**:路由分发是帧化 JSON over h2c(§6),无 protobuf/gRPC 依赖。
- **确定性密钥多实例一致**:MMDS 会话密钥由 `manifest_key + sid` 派生(§8),N 个对等
  worker 与节点内故障转移天然一致,无主从 / 共享内存。
- **即时刷流透传**:`ReverseProxy{FlushInterval:-1}`,流式接口(`process.Start` /
  `WatchDir` / `/files` / Connect 流)零缓冲。

### 1.2 边界与依赖

- 依赖 serve 的 **config-socket plugin 平面**(注册 + routesync,见
  node.md §6)。
- 依赖 **connector** 的 mgmt-extract 把 MMDS VIP 直译到本进程(§8)。
- 上游是 guest **envd**(经 host-UDS)或沙箱 **floatingip**;envd 协议见 node.md §4.3。
- 须与 serve 同节点(本地拨 envd-UDS / floatingip)。集群下的 cluster-ctl router 是更上一级
  入口,经本节点数据端点转发进来(cluster-router.md),仍落到本节点 proxy。

## 2. 命令行:`node-ctl proxy`

外置数据面 worker(部署模式见 §5)。运维带外起(`deploy/node-proxy@.service`),
与 serve 同节点。

```
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml --id <name>
               [--socket <uds>] [--metrics-listen <addr>] [--mmds]
```

worker 读 `proxy.yaml` 取共享策略与端点,命令行只给每实例身份。多个实例
(`node-proxy@1`、`@2`…)共用同一份 `proxy.yaml`,仅 `--id` 不同。

每实例命令行参数:

| 参数 | 默认 | 说明 |
|---|---|---|
| `--config` | `/etc/node-ctl/proxy.yaml` | worker 配置文件 |
| `--id` | (必填) | 本 worker 的 plugin id,每 worker 唯一(同 id 二次注册顶掉前者) |
| `--socket` | `<dir(config_socket)>/<id>.sock` | 本 worker 服务兜底网关转发的 UDS(注册时上报给 serve) |
| `--metrics-listen` | 空 | Prometheus 文本端点(`/metrics`),每实例独立(同机多 worker 端口须异) |
| `--mmds` | 关 | 在本实例起 FC MMDS 元数据服务(地址取 `mmds_listen`);serve 配 `mmds.enabled` 时挑一个 worker 开(§8) |

`proxy.yaml` 字段:

| 字段 | 默认 | 说明 |
|---|---|---|
| `config_socket` | `/run/sandbox/node-ctl.socket` | serve 的 config-socket UDS:worker 在其 plugin 平面注册并同步路由(node.md §6、本文 §6) |
| `data_listen` | `:443` | 数据面入口,**SO_REUSEPORT**(多 worker 共享同一端口);空 = 仅 UDS 兜底服务 |
| `tls` | 空 | 数据面 TLS `{cert,key}`(与 serve 同一张通配证书);空 = h2c |
| `auth` | `enforce` | 数据面鉴权回退值,仅在 serve 策略到达前生效(§7) |
| `park_timeout` | `30s` | 请求挂起预算回退值,同上 |
| `mmds_listen` | `127.0.0.1:19254` | `--mmds` 实例绑定的 MMDS 地址(§8) |

## 3. 配置

数据面相关字段(完整配置表见 node.md §3):

| 字段 | 默认 | 说明 |
|---|---|---|
| `proxy.mode` | `internal` | 数据面承载:`internal` / `external` / `off`(§5) |
| `proxy.park_timeout` | `30s` | 数据面请求挂起预算:等路由同步 / paused 沙箱 resume 的上限(§5) |
| `proxy.auth` | `enforce` | 数据面鉴权:`off` / `log` / `enforce`,校验 `X-Access-Token` 或 envd `/files` signature(§7) |
| `proxy.metrics_listen` | 空 | serve 内置 proxy 的 Prometheus 端点 |
| `mmds.enabled` | `false` | envd 鉴权姿态(§8):false = proxy 单闸门;true = FC MMDS v2 + envd re-key |

external 模式下 `proxy.auth` / `park_timeout` 经握手 `Policy` 下推到 worker;worker 的
`auth` / `park_timeout`(proxy.yaml)仅为策略到达前回退。

## 4. 转发数据通路

按 `Host`(`<port>-<sid>.<domain>`)或 `E2b-Sandbox-Id` / `E2b-Sandbox-Port` 头解析
`(sid, port)` → 路由判定 → 校验 `X-Access-Token` 或 envd `/files` signature(§7)→
转发。来源可以是客户端直连,也可以是 cluster-ctl router 转发进来的机群流量;普通 token
流量会携带 `X-Access-Token`,envd 预签名文件 URL 不携带该头。两种部署形态共用同一路由判定函数:

```
profile=e2b  且 port ∈ {49983, 49999}  → dial UDS (envd.sock / ci.sock)
profile=bare 且 port ∈ {49983, 49999}  → 501 (no data plane)
其余任意用户端口                        → dial floatingip:port
未知 sid / 挂起超时仍未 running         → 404
```

普通 HTTP 经 `httputil.ReverseProxy{FlushInterval:-1}` 即时刷流反代(上游走 HTTP/1.1,
envd 双栈,Connect 流在 h1 上承载,透传不缓冲);`CONNECT` 请求另走原始 TCP 隧道(§9)。
envd 的两个控制端口(49983/49999)仅经 proxy 的 host-UDS 可达(sandbox-ctl `--connect`
映射,非 floatingip),沙箱间无通路。

## 5. 部署模式(internal / external / off)

`proxy.mode` 选数据面如何承载(控制面 `api.<domain>` 始终由 serve 的 `api.listen`
提供;serve 如何按模式装配数据面见 node.md §9):

| 模式 | 数据面承载 | 进程 | 适用 |
|---|---|---|---|
| **internal**(默认) | serve 进程内 proxy | 单二进制 | 简单部署,无额外组件 |
| **external** | 独立 `node-ctl proxy` worker(≥1),**SO_REUSEPORT** 共享数据面端口 | serve + N×proxy | 数据面 / 控制面进程隔离、独立扩缩 |
| **off** | 拒绝(501) | – | 该节点不提供数据面 |

external 模式拓扑(数据面字节流不经 serve;**worker 主动注册,serve 不拨
任何人**):

```
 client / cluster-router ──► node-ctl proxy  :443 (SO_REUSEPORT, N workers share the port)
                 │ local route table (synced) + per-sid request park
                 │ running   → check X-Access-Token → dial envd-UDS / floatingip:port
                 │ paused /  → Wake (upstream) + park → unpark on Upsert(running)
                 │  missing     park timeout → 404
                 └─ dials serve's config-socket: PUT /internal/plugin/{id}/register
                       worker → serve : register(caps) → Wake{sid}
                       serve → worker : Hello(policy) → Upsert* → Bookmark → Upsert/Delete
 client / cluster-router ──► node-ctl conductor serve  :443 (fallback: data-plane request hits the
                 │                        control listener)
                 └─ forwards to one REGISTERED worker over its UDS (picked by sid hash;
                    CONNECT → chained CONNECT relay, §9)
```

- **注册即订阅**:worker 经 `config_socket`(proxy.yaml)/ `--id` 在 plugin 平面(node.md §6)
  注册 `subscribe=route_wake` + `proxy{socket}`,持挂连接 = 租约 + 路由流。serve
  不再拨 worker——拓扑反转后它只是连接的应答方与路由权威。断连即反注册;同 id 二次注册
  顶掉(断链)前者。同一平面也接受**非 proxy 观察者**(如平台 agent,`subscribe=route`),
  借此感知沙箱状态(含暂停态本地 / 远程,node.md §8.1)而不参与数据面转发。
- **路由权威在 serve,worker 是缓存**:create/resume/pause/kill 实时广播
  Upsert/Delete 给所有订阅者;worker 重启 → 自动重连重注册 + 重新同步;订阅滞后 →
  serve 断开该订阅,worker 重连重同步(有界内存、最终一致)。初始全量不再是单帧
  快照,而是**逐条 Upsert + 末尾 Bookmark**(§6),高密度下发端内存有界。
- **park**:对 missing/paused sid 的请求先上行 `Wake`(同 sid 去重),再挂起等
  `Upsert(running)` 回灌解挂;`park_timeout`(策略下推,默认 30s)内未就绪回 404。
  首个 Bookmark 到达前的请求同样先等同步完成。
- **运营策略集中下推**:握手 Hello 携带 `Policy{domain, auth_mode, park_timeout_ms}`;
  worker 的 `auth` / `park_timeout`(proxy.yaml)仅为策略到达前的回退。
- **兜底网关**:数据面请求误达 serve 监听口时,serve 按 sid 的 FNV 哈希在**活跃注册的
  worker 集**上挑一个,经其 `--socket` UDS 反代过去(连接亲和),由 worker 照常处理(含
  鉴权);`CONNECT` 经链式 CONNECT relay 转发(§9)。无 worker 注册时回 502。
- **多 worker**:plugin 注册 + SO_REUSEPORT + 确定性 MMDS 密钥(§8)使 N 个对等 worker
  进程各自独立注册、各持本地路由表、共享数据口即可工作,无需主从 / 共享内存。
- worker 由运维带外管理(`deploy/node-proxy@.service`),serve 不自动安装;须与
  serve 同节点(本地拨 envd-UDS / floatingip)。
- **可观测**:`proxy.metrics_listen`(serve)/ `--metrics-listen`(worker)暴露
  Prometheus 文本:`data_requests_total{result=ok|unauthorized|notfound|denied|…}`、
  `gateway_forward_total{result=ok|error|no_worker}` 等。

## 6. routesync 协议

订阅者 ↔ serve 的路由分发协议,**帧化 JSON over h2c**(零 gRPC/protobuf)。
**订阅者是拨号方,serve 是应答方 + 路由权威**:

- 传输:订阅者拨 serve 的 config-socket,发 `PUT /internal/plugin/{id}/register`
  (plugin 平面,node.md §6,`plugin_pidfile` / socket 0600 鉴权);单 HTTP/2
  请求全双工——请求体上行(首帧 `register`,之后 `wake`),响应体下行(`hello` /
  `upsert` / `bookmark` / `delete`)。持挂该连接即注册租约,断连即反注册;同 id 二次
  注册顶掉前者。
- 帧:`[4 字节 LE 长度][JSON]`,单帧上限 1 MiB(每帧仅一条路由 / 一个 wake,无全量帧)。
- 消息(`type` 字段判别):

  | 方向 | 消息 | 载荷 |
  |---|---|---|
  | 订阅者 → serve | `register` | `{subscribe{kind: route\|route_wake\|registry}, proxy{socket{path}}, mmds, resume_from?}`(能力 + 可选断点,首帧;断点续传见下)|
  | serve → 订阅者 | `hello` | `{version:1, policy{domain, auth_mode, park_timeout_ms}}` |
  | serve → 订阅者 | `upsert` / `delete` | `route: RouteEntry` / `sid` |
  | serve → 订阅者 | `bookmark` | —(初始全量结束;订阅者据此判定已同步) |
  | 订阅者 → serve | `wake` | `sid`(请求 resume;仅 `route_wake`) |

- **初始同步用 bookmark 取代全量快照**:握手后 serve 逐条流式 `upsert`(直接由
  store 流式扫描喂出,不物化整表 / 巨帧),末尾发一个 `bookmark` 表示"初始集已发完"。
  订阅者按一个**同步世代**应用本轮流,收到 bookmark 时清掉本轮未见过的条目——由此
  无缝回收断连期间发生的删除,且重同步全程旧表仍在服务(无路由空窗)。
- `RouteEntry = {sid, profile, template_id, state(running|paused|dead), envd_uds,
  ci_uds, floatingip, access_token, snap_loc, mmds_secret}`——订阅者据此独立服务数据
  面,无每请求回调。两个字段对所有订阅者一致下发(不做按角色裁剪):
  - `snap_loc`:running/dead 为空,否则 `local`(节点绑定的本机快照)或 `remote`
    (已上传、可移植)。观察者据此判定迁移(node.md §8.1);迁移 token 仍由
    export-sandbox 按需铸造,不随路由广播。
  - `mmds_secret`:该沙箱的 MMDS 会话签名密钥(hex),由 `manifest_key + sid` 确定性派生
    (§8),不服务 MMDS 的订阅者忽略即可。
- **断点续传(opt-in)**:每条 `upsert`/`delete` 带**分片单调版本号 `rev`**;订阅者记最后应用的
	  `rev`,重连 `register` 带 `resume_from=<rev>`——serve 在留存窗口内只重放增量 + `bookmark`,否则退回
	  逐条全量。避免高密度下重连发全量;node-link 集群通道复用同一机制(cluster.md)。
- 容错:连接断 → 订阅者指数退避重连重注册(0.2s 起、5s 封顶),重连即 `register`(带 `resume_from`)
  + 增量 / 全量重同步;订阅积压 → serve 掐掉该订阅、订阅者重连重同步。Wake 的 resume 由 serve 端
  单飞去重。

注:routesync 同一引擎服务两类订阅者——**本机** worker / 观察者(`kind=route` / `route_wake`,订阅者
拨 serve)与**集群 registry**(`kind=registry`)。集群接入**复用此权威侧**:node 拨 registry 后
**反向注册**,registry 在该连接以 `kind=registry` 订阅本节点路由 + `build` 事件、并上行下发命令——
故 node-link 不是另一套协议,而是 routesync 的一个订阅 kind(node.md §10、cluster.md)。

## 7. 数据面鉴权(X-Access-Token)

- 数据面 token = `envdAccessToken`,头 **`X-Access-Token`**(与原版 e2b 一致;secure
  沙箱自 SDK v2.0.0 默认开,SDK 每次数据面调用携带)。create 铸造(派生与落盘见
  node.md §7)→ 回 SDK → 随路由分发;**proxy 逐请求校验**其与该沙箱 token
  一致(常数时间比较)。集群下,token 由 cluster-ctl router 从 Reserve 结果注入再转发,
  proxy 校验不变(cluster-router.md)。
- `proxy.auth ∈ {off | log | enforce}`,默认 **enforce**(不符回 401);`log` 告警但
  放行;external 模式校验在 worker(策略下推,兜底转发路径上 serve 不校验、由 worker
  校验)。
- **signature 凭证**:仅 `port=49983`、`GET/POST /files`、且带非空 `signature`
  query 时可替代 `X-Access-Token`。proxy 按 envd 算法复算
  `v1_ + base64raw(sha256(path:operation:username:accessToken[:expiration]))`;
  `GET` 对应 `read`,`POST` 对应 `write`,`signature_expiration` 参与签名并按 Unix
  秒过期校验。该路径不补 `X-Access-Token`,envd 最后一跳继续自验。同一请求若带了
  非空但错误的 `X-Access-Token`,不回退到 signature。
- **例外放行**:`auth=off`;路由无 token(bare)。
- envd 是否**另行**自校验此 token 由 `mmds.enabled` 决定(§8)。无论哪种姿态,envd
  仅经 proxy 的 host-UDS 可达(49983/49999 走 `--connect` UDS,非 floatingip),沙箱
  间无通路。
- create 响应同时返回 `trafficAccessToken`(SDK 兼容字段,非强制头)。

## 8. envd 鉴权姿态与快照扇出(mmds.enabled)

快照扇出的子沙箱(`export-sandbox --to-template` 或任何 snp 模板 create)由内存恢复,
其 envd 持**源**沙箱的 token;envd 在 `-isnotfc` 下不允许把 token 改成新身份的值
(`/init` 仅允许首设 / 重确认同 token / 匹配 MMDS hash),会拒绝子沙箱数据面。单开关
`mmds.enabled` 选两种姿态,二者都使扇出沙箱数据面可用:

| | `enabled=false`(默认) | `enabled=true` |
|---|---|---|
| envd 启动 | `-isnotfc` | FC 模式(去 `-isnotfc`) |
| envd token | `/init` 省略 `accessToken` → envd 非 secure | 经 MMDS 授权后 `/init` 重置为每身份新 token |
| 数据面闸门 | 仅 proxy(配置强制 `proxy.auth=enforce`) | proxy + envd(纵深防御) |
| 扇出 fork | 可用(envd 无 token,故不错配) | 可用(envd 重置为新 token) |

**MMDS 服务**(`enabled=true`):serve 在 **proxy 组件**内起 Firecracker MMDS
v2 兼容服务(internal:serve 绑 `mmds.listen`;external:worker `--mmds`(地址取 `mmds_listen`),数据源
是同步来的路由表)。两段式:

1. **`PUT /latest/api/token`**:按请求源 IP(= 沙箱 floatingip,vswitch mgmt-extract
   已 SNAT)查路由表解析沙箱 id——未注册则 **park 等待**(复用数据面挂起预算,故
   external 模式无"先注册后轮询"的时序约束),超时回 503(envd 的轮询会重试);命中则回
   HMAC 签名、编码了沙箱 id 的 session token(`<sid>.<hmac>`)。
2. **`GET /`**(头 `X-metadata-token`):校验并解码 session token 取沙箱 id(guest 内
   代码不可信,不复读源 IP),回 `{instanceID, envID, accessTokenHash}`,
   `accessTokenHash = hex(sha512(token))`(= envd `keys.HashAccessTokenBytes`)。

session token 的 HMAC 密钥是**每沙箱确定性派生**的 `MmdsSecret = HMAC-SHA256(manifest_key,
"kuasar-mmds-v1:"+sid)`(`keys.MmdsSecret`),非进程随机:internal 模式 serve 直接
派生,external 模式经 `RouteEntry.mmds_secret` 同步给 worker(§6)。因此任一 worker 铸造
的 token 在任一 worker 都能校验——多 worker(及节点内故障转移)下 PUT 与 GET 落到不同
worker 也一致,这正是确定性密钥相对进程随机密钥的关键。

envd 硬编码访问 `169.254.169.254:80`;部署侧用 vswitch
`--mgmt-service 169.254.169.254:80:<mmds.listen>` 在 eBPF 数据面把该 VIP 直译到
`mmds.listen`(无 iptables,自动改写回程;loopback target 需 mgmt 设备
`route_localnet=1`),故本进程不占特权端口、不需 root。配置校验强制
`proxy.mode != off`(MMDS 寄宿 proxy)。

## 9. CONNECT 隧道

数据面除反代普通 HTTP 外,还支持 `CONNECT` 开原始 TCP 隧道(端口转发、非 HTTP 协议)。
按数据面模型,**CONNECT 目标主机被忽略**(统一是沙箱 floatingip),仅取其端口;沙箱 id
仍来自 `E2b-Sandbox-Id` 头(或 authority 的 `<port>-<sid>` 标签)。路由解析与
`X-Access-Token` 校验同普通请求(§4/§7),随后把客户端连接对接到后端(envd-control UDS
或 floatingip:port)。

一个共享隧道原语承载两种传输:HTTP/1.1 经 `Hijack` + `200 Connection established`,
HTTP/2 经 `WriteHeader(200)` + 请求 / 响应流对拷。它覆盖每条数据面路径,即 proxy 直收
的数据面与控制面转发的数据面都支持 CONNECT:

- **proxy 直收**(及 internal 模式):直接隧道到沙箱;
- **控制面兜底网关**(external):`httputil.ReverseProxy` 不能隧道 CONNECT,故网关向选中
  的 worker 经其 UDS 发**链式 CONNECT**(带上 sandbox id + access token),收到 200 后
  对接——client → 网关 → worker → 沙箱(无环,沿用 h2mux 式链式隧道)。集群下 cluster-ctl
  router 的端口转发同样经链式 CONNECT 转发进本节点(cluster-router.md)。

## 10. 可靠性

- **worker 崩溃**:持挂连接断 → serve 反注册该 worker;SO_REUSEPORT 下其余
  worker 继续收数据口;serve 兜底网关只在活跃注册集上选 worker(§5),无 worker 回 502。
  worker 重启自动重连重注册重同步。
- **routesync 重同步**:连接断 → 指数退避重连(0.2s 起、5s 封顶)→ 重新 register +
  逐条 upsert + bookmark;重同步全程旧路由表仍在服务,无路由空窗(§6)。
- **resume 单飞**:park / Wake 触发的 resume 由 serve 端单飞去重(internal 直接、
  external 经 routesync `Wake` 上行),并发请求只触发一次恢复(node.md §8)。集群级
	  会话亲和的单飞在 registry 端(cluster.md)。
- **语义化失败**:未知 / 未就绪 sid → 404;鉴权失败 → 401;无数据面(bare / off)→ 501;
  兜底无 worker → 502。

## 11. 性能

- **水平扩**:external + SO_REUSEPORT,N 个对等 worker 共享数据口,由内核分发连接,无锁
  无主从。
- **内存有界**:routesync 逐条 upsert + bookmark(无全量巨帧),高密度下发端与订阅端
  内存有界;路由表是本地缓存,O(沙箱数)。
- **零缓冲转发**:即时刷流(§4),流式接口(process.Start / WatchDir / /files)不堆积。

## 12. See Also

- [node.md](node.md) — 控制面:e2b API、生命周期与状态机、密钥模型
  (`envdAccessToken`/`MmdsSecret` 派生,§7)、模板构建,serve 如何按 `proxy.mode`
  装配数据面(§9),以及 node-link 集群接入(§10)。
- [node-resource.md](node-resource.md) — 节点资源控制协议(与数据面正交)。
- [cluster-router.md](cluster-router.md) — 集群级数据面入口:经本节点数据端点转发进 proxy,
  注入 `E2b-Sandbox-Id` + `X-Access-Token`。
- `connector/docs/vswitch.md` — mgmt-extract 把 MMDS VIP 直译到本进程(§8)。
- `kuasar-sandbox/docs/deployment.md` — 部署拓扑、端口与故障域。
