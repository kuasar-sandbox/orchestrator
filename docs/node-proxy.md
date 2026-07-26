# node-proxy — 节点数据面转发层

## 1. 概述

数据面 proxy 是沙箱流量的 L7 转发层:把外部 e2b SDK/CLI、端口转发或
cluster-router 进入本节点的请求按 `(sid, port)` 路由到 guest envd UDS 或沙箱
floatingip 用户端口。控制面 API、生命周期、密钥、构建由
`node-ctl conductor serve` 承载,见 [node.md](node.md);本文只描述数据面转发层。

```text
client / cluster-router
  │ Host: <port>-<sid>.<domain>
  │ or E2b-Sandbox-Id + E2b-Sandbox-Port + X-Access-Token
  ▼
node proxy worker
  │ shared route view (read-only mmap)
  ├─ e2b 49983/49999 ─► envd / ci UDS
  └─ user port ───────► floatingip:port
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

共享表是固定容量开放寻址 hash 表。master 单写;每条记录带 seqlock,worker 读取时若遇到
写中状态或版本变化会重试,不会看到半条路由。worker 只依赖共享表本地读取:

```text
sid hash ─► record slot ─► RouteEntry
                         ├─ running → 立即转发
                         └─ missing/paused → wake pipe → park 等待共享表更新
```

worker 对 missing/paused sid 写 wake pipe 给 master;master 去重后通过 routesync
上行 `Wake`。master 每次写共享表后通过 notify pipe 唤醒 worker 本地 park waiters。

受保护 `RouteEntry` 显式携带 `AuthSandboxID`、`APISecret`、`APISecretFingerprint`、
`ManifestKeyFingerprint`、`ServiceSecret`、`EnvdAccessToken`、`TrafficAccessToken` 和
`ForwardAccessToken`。
ManifestKey 原文不进入路由。节点 proxy 转发时只按目标选择 EnvdAccessToken 或
ForwardAccessToken;TrafficAccessToken 仅随受保护视图投影给外部网关及 e2b 数据面组件,
不由 node 平台层消费。既有 `MmdsSecret` 独立服务于 MMDS,不充当上述任一 token。

## 5. 转发路径

请求按 `Host: <port>-<sid>.<domain>` 或 `E2b-Sandbox-Id` /
`E2b-Sandbox-Port` 解析 `(sid, port)`。

```text
profile=e2b  and port ∈ {49983,49999} → envd / ci UDS
profile=bare and port ∈ {49983,49999} → 501
otherwise                              → floatingip:port
unknown or not running before timeout   → 404
```

普通 HTTP:

1. worker 从共享表解析 route;
2. 按目标选择 EnvdAccessToken 或 ForwardAccessToken,校验 `X-Access-Token`;符合条件的
   `/files` 请求也可使用 EnvdAccessToken 验证 signature;
3. 拨一次 envd UDS 或 `floatingip:port`。配置 `proxy_netns` 时,`floatingip:port` 在该
   netns 内拨号;
4. 写入一条 HTTP 请求,流式复制响应,响应结束关闭后端连接。

CONNECT:

- CONNECT 目标 host 被忽略,只取端口;
- sandbox id 来自 `E2b-Sandbox-Id` 或 authority label;
- 认证和路由判定同普通 HTTP;
- 成功后把客户端连接与后端连接双向 splice。

proxyForwarder:

- conductor 收到数据面请求但处于 external 模式时,不会自己查路由;
- 它向 `proxy_socket` 发 chained CONNECT,显式携带 sid、port,并原样携带客户端的
  `X-Access-Token`;
- 普通 HTTP 在该 CONNECT 隧道里发送一条请求;CONNECT 则继续隧道化到沙箱。

## 6. 数据面鉴权

数据面请求头统一为 `X-Access-Token`,但期望值按转发目标选择:

- e2b 49983/49999 使用 create 响应中的 `envdAccessToken`;
- e2b/bare 的其他允许转发端口使用 `forwardAccessToken`;
- `trafficAccessToken` 只供外部网关及 e2b 数据面组件验证,node proxy 不消费;
- bare 的 49983/49999 不进入鉴权或转发,直接按不支持的控制端口处理。

proxy 逐请求以常数时间比较请求 token 与选中的显式字段。

`auth` / policy `auth_mode`:

| 模式 | 行为 |
|---|---|
| `enforce` | 不匹配返回 401 |
| `log` | 记录但放行 |
| `off` | 不校验 |

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

1. `PUT /latest/api/token`:按请求源 IP 查 running route 的 floatingip;未同步时按
   `park_timeout` 等待,超时 503。命中后返回 `<sid>.<hmac>` session token。
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
- **park / wake**:worker 对 missing/paused sid 发送 wake 并等待共享表更新;resume
  单飞仍由 conductor 执行。
- **失败码**:未知/未就绪 sid = 404;鉴权失败 = 401;bare 控制端口或 off = 501;
  proxy 未注册/不可达 = 502。

## 9. 性能

- 数据面 route lookup 是 worker 本地 mmap hash 查找,不进 conductor,不跨进程 RPC。
- master 单写共享表;worker 只读,无 worker 间锁竞争。
- 普通 HTTP 和 CONNECT 都不使用上游连接池,避免跨 sandbox/port 连接复用。
- `route_capacity` 是固定容量保护阈值;容量不足时应调大配置并重启 proxy master。
- worker 数据面 metrics 经继承 pipe 上报给 master,`metrics_listen` 输出聚合后的
  `data_requests_total{result=...}`;队列饱和时优先保护数据面,可能丢弃个别 metrics 增量。
- MMDS 按 floatingip 反查当前实现为共享表线性扫描,该路径只在 envd 初始化时使用,
  不在高 QPS 数据面热路径。

## 10. See Also

- [node.md](node.md) — conductor 控制面、`proxy.mode` 装配、生命周期与密钥模型。
- [cluster-router.md](cluster-router.md) — 集群入口如何转发到本节点数据面。
- `connector/docs/vswitch.md` — mgmt-extract / MMDS VIP 转换。
- `orchestrator/release-builder/docs/deployment.md` — 部署拓扑、端口与故障域。
