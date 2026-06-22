# cluster-router — 集群级 e2b 兼容统一入口

router 是 cluster(cluster.md)的统一北向入口,在一个对外端点上同时承载 **e2b 兼容控制面**
(`api.<domain>`)与**数据面**(业务流量 / `<port>-<sid>`),按 Host 分流——与 node-ctl serve
同形(node.md §2.2),只是上移一级。N 个无状态副本置于 LB 后。

**为最小化启动延迟,router 持本地路由缓存**(订阅 registry 路由流):**热路径**(已建立会话)缓存
命中直转、**零控制面往返**;仅未命中 / 控制操作 / 冷启才走 registry(cluster.md §9)。

- **控制面**:e2b 沙箱生命周期 + 模板构建,**限定单个 sandbox-group**(`X-Kuasar-Sandbox-Group`,
  无跨 group);校验 `api_key ↔ group` 后,create/connect 走 `ReserveSandbox`,pause/kill/timeout 转发
  持有沙箱的节点,list/get 从该 group 注册表分片读,build 走 `ReserveBuild` + 转发(cluster.md §8)。
- **数据面**:把业务请求按身份路由到正确沙箱并转发——身份可以是 create 后的 **sid**(e2b 原生
  SDK 流),或业务流量直带的 **(group, route-key)** 头(无 create 直触发 `Reserve`)。

router 与节点 proxy(node-proxy.md)构成**两级转发**:router 选**哪个节点 + 哪个沙箱**(跨节点、
会话亲和),节点 proxy 做最后一跳 `(sid, port) → guest envd / floatingip` 并校验 `X-Access-Token`。
**节点侧零改动**;`bare` profile 同样可路由(其用户端口转发到 floatingip、无 token,§6)。

## 1. 概述

### 1.1 设计原则

1. **无状态 + 本地缓存、可横向扩**:router 不持路由权威(在 registry),但持**只读路由缓存**(订阅
   同步);热路径本地命中直转(§5)。N 副本置 LB 后,任一副本服务任一请求。
2. **统一 e2b 入口、操作不跨 group**:控制面 + 数据面共用一对外端口与一张通配证书,按 Host 分流;
   每请求带 `X-Kuasar-Sandbox-Group`(SDK 经 api_headers 注入),操作限定该 group(cluster.md §8)。
3. **校验后转发、节点零改**:控制面校验 `api_key↔group` 后转发节点 e2b 控制面 / 经 Reserve 走节点
   命令;数据面注入 `E2b-Sandbox-Id` + `X-Access-Token` 转发节点 proxy(node-proxy.md §4 / §7)。
4. **服务端持有 token**:per-sandbox `access_token` 服务端铸造 / 注入,client 不持有;调用方鉴权另靠
   api_key ↔ group 租户(§7,带缓存)。

### 1.2 边界与依赖

- 上游:e2b SDK / CLI(控制面)与业务客户端(数据面),均带 `X-API-KEY` + `X-Kuasar-Sandbox-Group`。
- 下游:registry 的异步 op / watch 接口(`ReserveSandbox`/`ReserveBuild`/控制 + 路由订阅,cluster.md
  §4.3 / §5.2);节点的 **e2b 控制面**(转发 pause/kill/build)与**数据端点**(node-proxy.md 数据面)。
- 数据面字节经 router → node → guest 两跳,不经 registry。

## 2. 命令行接口

```
cluster-ctl router [--config /etc/cluster-ctl/config.yaml] [--registry <addr>] [--listen :443]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--config` | `/etc/cluster-ctl/config.yaml` | 配置文件 |
| `--registry` | 空 | 覆盖 registry op 接口(UDS 本机 / mTLS 远端,cluster.md §4.2);省略则用 `router.registry`,再省则同机 `op.listen` |
| `--listen` | 空 | 覆盖 `router.listen` |

TLS(`router.tls`)、数据面鉴权(`router.data_plane_auth`)等经配置文件(§3),无对应命令行旗标。

## 3. 配置

`router` 配置组(完整表见 cluster.md §3):

| 字段 | 默认 | 说明 |
|---|---|---|
| `router.registry` | 空 | registry op 接口;空 = 同机 `op.listen`(cluster.md §4.2) |
| `router.listen` | `:443` | 统一入口(控制面 `api.<domain>` + 数据面,按 Host 分流);多副本同机 SO_REUSEPORT |
| `router.tls` | 空 | 通配证书(`*.<domain>` + `api.<domain>`);空 = h2c(dev) |
| `router.auth` | `enforce` | 调用方 api_key 鉴权(§8):`enforce` / `log`(告警放行）/ `off`(前置外部 mTLS/JWT 网关)|
| `router.data_plane_auth` | `enforce` | 数据面 access-token 校验(§7):`enforce` 要求调用方携带沙箱 token(否则 401)/ `log` 仅告警 / `off` 跳过 |
| `router.auth_cache_ttl` | `60s` | 调用方 api_key↔group 鉴权结果缓存时长(§8) |
| `router.metrics_listen` | 空 | Prometheus 端点(`router_requests_total{plane,result}`)|

sandbox-group / route-key 头为固定常量(`X-Kuasar-Sandbox-Group` / `X-Kuasar-Route-Key`,§4),非配置项。

`Reserve` 等待预算用 registry 的 `reserve.park_timeout`(cluster.md §3)。

## 4. 入口与寻址

按 Host 分流,**每请求必带 `X-Kuasar-Sandbox-Group`**(SDK / 客户端经 api_headers 注入;缺失 → `400`):

| Host | 平面 | 寻址 |
|---|---|---|
| `api.<domain>` | 控制面(§6) | 操作 + `X-Kuasar-Sandbox-Group`;create/connect 另带 route_key;sid 在路径 |
| `<port>-<sid>.<domain>` / `E2b-Sandbox-*` 头 | 数据面(§7) | **by sid**:create 后 SDK 流,`(group, sid)` 在 group 分片缓存 / 反查 |
| 业务流量(`X-Kuasar-Sandbox-Group` + `X-Kuasar-Route-Key`)| 数据面(§7) | **by (group, route-key)**:无 create,直触发 `Reserve` |

- **group 局部寻址**:`X-Kuasar-Sandbox-Group` 把每个操作(控制 + 数据)定位到唯一 group 分片;sid
  反查在该分片内——守住"操作不跨 group"。
- **port**:`E2b-Sandbox-Port` 头(缺省 49983)。group / route-key 是 opaque 字符串,router 不解释
  层级语义(分区 / 调度语义在 registry / scaler)。

## 5. 本地路由缓存与热路径

router 经 registry 的 watch 接口订阅路由流(`kind=router`,cluster.md §5.2 / §4.3),维护本地缓存
`(group,route-key)/sid → {node_id, sid, access_token, state}`——复用节点 routetable 同款世代 / 版本号
机制(node-proxy.md §6,断线增量重放 cluster.md §5.3)。

```
热路径(已建立会话):
  parse(group, sid | route-key)
  → 本地缓存命中且 state=READY → 注入 token → 转发 node.data_endpoint(§7)。零控制面往返。
冷/未命中/非 READY:
  → registry.ReserveSandbox(异步) → 等结果事件 → 回填缓存 → 转发。
```

缓存可滞后:沙箱 paused/moved → 缓存项失效经路由流近实时更新;偶发陈旧 → 转发 502/404 → 退回
`Reserve`。**首触 / 冷启**本就经 registry(放置 + 快照恢复主导延迟,cluster.md §9),缓存只优化
**已建立会话**的高频请求(零控制往返)。

## 6. 控制面转发(api.<domain>)

校验 `api_key ↔ group`(§7)后按操作分派(契约见 cluster.md §8):

| 操作 | router 动作 |
|---|---|
| create / connect | `ReserveSandbox(group, route_key, 合并配置)`(cluster.md §7)→ 回 `{sandboxID, tokens}`;connect 可携 `X-Kuasar-Migration-Token` + route_key 一步迁移 |
| pause / kill / timeout | registry 解析 `(group, sid) → node_id` → **转发该节点 e2b 控制面**执行(node.md §4);kill 另触发记录删除 + `delete` |
| get / list | 从该 group sandbox 分片读(registry);**list 仅本 group** |
| build register/trigger/status/files | register 走 `ReserveBuild`(cluster.md §7.5)→ 其余转发被 pin 节点 e2b 构建 API(node.md §12) |

create/connect 是 `Reserve` 的发起方,**等节点上报 running 才回**;其余写操作转发节点执行,只读 /
聚合从 store 服务。转发到节点 e2b 控制面经节点 `api.<domain>`(节点须对 router 可达,mTLS / 内网)。

## 7. 数据面转发

```
身份解析(§4):by sid  → 缓存 / 记录;若 paused/saved → Reserve 恢复 / 迁移
              by (group, route-key) → 缓存命中→直转;未命中→ Reserve(cluster.md §7)
  → (node_id, sid, access_token)
  → 转发 node_id.data_endpoint, 注入:
        E2b-Sandbox-Id: <sid>
        E2b-Sandbox-Port: <port>
        X-Access-Token: <access_token>       // 服务端注入(每沙箱 token 节点铸,含 bare)
```

- **普通 HTTP**:`httputil.ReverseProxy` 反代到节点数据端点(流式响应自动刷新;TLS / h2c);
  **按节点数据端点池化连接**(per-host idle 上限高于 stdlib 默认,避免高密度连接抖动)。节点 proxy
  按 `(sid, port)` 路由 guest envd / floatingip 并校验 token(node-proxy.md §4 / §7)。命中缓存的路由
  转发失败(502)即**淘汰该缓存项**,下次经 watch / op 重解析。
- **CONNECT 隧道**(端口转发 / 原始 TCP):router 向节点发**链式 CONNECT**(带 `E2b-Sandbox-Id` +
  端口 + `X-Access-Token`),节点照其链式 CONNECT 路径对接 guest(node-proxy.md §9)——
  client → router → node → guest,无环。
- **bare profile**:经集群 router 创建的沙箱(含 bare)一律由节点控制面铸 token(不存在空 token 绕过);
  router 对 bare 与 e2b **一视同仁**(注入 token + enforce)。bare 特有的只是数据面按 `<port>-<sid>` 转发到
  节点后路由到 **`floatingip:port`**(而非 envd UDS)、e2b 控制端口(49983/49999)回 501——均在节点 proxy
  (node-proxy.md §4/§7)。
- **by-sid 的 paused/saved**:数据面打到已暂停 / 已上送的 sid,经其记录的 (group, route-key) 触发
  `Reserve` 恢复 / 迁移(单飞在 registry),再转发。
- **数据面鉴权**(`router.data_plane_auth`,§3):`enforce`(默认)转发前校验调用方所带 `X-Access-Token`
  与该 sandbox token 一致(否则 `401`),`log` 仅告警,`off` 跳过;各模式转发给节点时均注入正确 token
  (节点 proxy 再校验,node-proxy.md §7)。

## 8. 调用方鉴权

per-sandbox `X-Access-Token` 由 router 注入(§7),故客户端侧数据面鉴权前移为**调用方对该
sandbox-group 的授权**——控制面同理:

- 客户端带 `X-API-KEY`;router 经 registry 以该 **group 租户 manifest_key** 校验(指纹匹配 + HMAC,
  复用 `apikey.Verify`,node.md §7)——api_key 解析出的租户须等于 group 的 `project_id` / manifest_key,
  否则拒(`403`;不泄露 group 存在性回 `404`)。
- **鉴权缓存**:校验结果按 `router.auth_cache_ttl`(默认 60s)缓存,避免每请求回 `GroupKeyProvider`
  (尤其 external provider)——降延迟(cluster.md §9)。
- 把鉴权折进 registry 调用使**租户密钥只在 registry / provider**(router 不持密钥)。
- 调用方鉴权 `router.auth`:`enforce`(默认,op 不可达回 `503`、密钥不符回 `403`)/ `log`(告警放行)/
  `off`(跳过——前置外部 mTLS / JWT 网关替换鉴权钩子)。与数据面 per-sandbox token 的 `router.data_plane_auth`
  (§7)分开命名、各自三档。

服务端内部 per-sandbox token 校验仍在节点 proxy(node-proxy.md §7),与此处调用方授权是**两道独立
闸门**。

## 9. 可靠性

| 故障 | 影响 | 自愈 / 语义 |
|---|---|---|
| router 崩溃 | 该副本连接断 | 无状态,LB 改路由;重启重连 registry 增量重同步缓存(cluster.md §5.3) |
| `Reserve` 超时(park) | 该请求 | `503` / `504`(registry 回滚 RESERVED,cluster.md §7.4) |
| 调用方未授权 / group 不存在 | 该请求 | `403` / `404`(§8) |
| group 无沙箱配置(数据面直 Reserve)| 该请求 | `409`:无可恢复 / 创建的配置(cluster.md §8) |
| 无可放置节点 | 该请求 | `503`(scaler 建议无 eligible,cluster-scaler.md §4.2) |
| 缓存陈旧 / 节点失联 | 转发失败 | `502`;退回 `Reserve`(死节点清扫使记录失效触发 re-Place,cluster.md §11) |
| 节点 proxy / 控制面拒 | 透传节点语义 | `401` / `404`(node-proxy.md §10) |

## 10. 性能

- **热路径**:本地缓存命中 = 一次查表 + 即时刷流反代,**零控制面往返**(§5 / cluster.md §9)。
  `FlushInterval:-1` 零缓冲,流式接口不堆积。
- **冷路径**:`ReserveSandbox` 异步(共置 UDS,cluster.md §9);单飞下沉 registry,router 无需自管。
- **连接池**:按节点数据端点池化,跨请求复用。
- **横向扩**:router 无状态,N 副本置 LB(或同机 SO_REUSEPORT)。缓存断线增量重放(cluster.md §5.3),
  高密度不发全量。

## 11. See Also

- [cluster.md](cluster.md) §8 —— 控制面 API 契约;§7 —— Reserve / ReserveBuild;§5 —— 统一通道(路由
  订阅);§9 —— 启动延迟与热 / 冷路径(本地缓存)。
- [cluster-scaler.md](cluster-scaler.md) —— Reserve 冷路径的放置建议。
- [node-proxy.md](node-proxy.md) §4 / §6 / §7 / §9 —— 节点数据面:路由判定、routetable / 版本号、
  `X-Access-Token` 校验(含 bare 无 token 放行)、链式 CONNECT。
- [node.md](node.md) §4 —— 节点直供的 e2b 契约(控制面转发目标 / 数据面头 / 端口);§7 —— api_key ↔
  manifest_key 校验(鉴权复用);§12 —— 构建(build 转发目标)。
