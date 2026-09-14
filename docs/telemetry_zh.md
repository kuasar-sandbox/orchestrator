[English](telemetry.md) | [简体中文](telemetry_zh.md)

# Telemetry — 沙箱指标与历史查询

## 1. 组件与权威边界

Telemetry 是独立节点进程，与 Conductor、Proxy 并列：

```sh
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
```

每条命令使用独立服务。Conductor 拥有生命周期、API 鉴权、ownership 和 Plugin registry。
Proxy 负责沙箱应用数据转发、MMDS 与即时 traffic observation。Telemetry 拥有自身的
RouteEntry view、envd scrape、sandbox-facing OTLP metrics ingress、可信身份 enrichment、
Collector pipeline、primary storage、exporter、历史查询及 E2B 转换。它不扩大 resource
controller，不改变 checkpoint 或生命周期策略。

```text
Conductor Plugin Plane -> full RouteEntry -> Telemetry view
Conductor config_socket -> native stats -> sandboxstats receiver
Envd UDS -> envd receiver
Guest -> connector management -> sandboxotlp receiver
                |
      source acceptance + trusted pdata identity
                |
Collector native processors / connectors / pipelines
                |
       configured exporters -> selected destinations

GET /sandboxes/{SandboxID}/metrics
  -> Conductor auth + ownership -> registered HTTP query UDS
  -> Telemetry handler -> selected Reader
```

所有采集写入都经过原生 OpenTelemetry Collector service 和配置的 exporter.
Receiver 不持有 storage handle. 原生 Collector 配置选择全部 pipeline 边和已链接
signal; 发行包不嵌入整个 contrib 仓库或 Prometheus server.

## 2. Plugin lease 与查询通道

固定 Plugin ID 为 `telemetry`，通过已有受鉴权的本地
`PUT /internal/plugin/telemetry/register` 注册，复用 routesync version 8：

```json
{
  "subscribe": {"kind": "route"},
  "telemetry": {"api": {"path": "/run/sandbox/telemetry.sock"}}
}
```

这是 registration capability body，不是另一套 target protocol。Telemetry 接收已有完整
`RouteEntry`，view 只保留 SandboxID、StableID、Profile、State、EnvdUDS、EnvdAccessToken、
FloatingIP；没有 telemetry 专有 projection/权限系统、lifecycle socket 或 Create/Resume
barrier。长连接承载 registration、lease 与 route stream；注册的 API UDS 是独立 HTTP
通道，查询不复用 routesync frame。仅 forwarding 的注册省略 `telemetry.api`。

同 ID 替换在发布 successor 前同步撤销旧 lease，断线也撤销 lease。旧 registration 的
迟到 Remove 不会删除 successor。每个转发查询绑定一个 lease 和独立本地 UDS transport，
连接池不跨 registration。替换/断线取消 in-flight 请求，新请求不会使用旧 endpoint。
若响应字节已经开始发送，取消会终止响应流，而不是伪造新的 HTTP status。Query socket
要求 canonical absolute local path、0600 权限和独占 sidecar lock；不会覆盖仍在监听的
socket 或无关普通文件。与 Proxy 注册一样，服务账号及受保护 Plugin socket 是信任边界。

Conductor 使用正常 API key 鉴权，精确查找 SandboxID 并检查 ownership，然后原样转发
method/path/raw query、body、status 与 relevant headers。它过滤 hop-by-hop headers，
查询通道不支持 protocol upgrade。它不解释 metric 名、DTO、范围、step 或 storage 结果。
不存在/不属于调用方的 sandbox 返回 404；无效鉴权沿用已有 API 行为。没有 live readable
telemetry endpoint、UDS 请求失败或 primary reader 不可用返回 503。客户端取消传播到
HTTP UDS 和 backend query。查询不调用 Wake、Resume、import、reservation 或 guest
control API；读取 paused 历史不会接触沙箱。

## 3. envd 与指标身份

完整同步到达 Bookmark 后，只采集 `profile=e2b`、`state=running` 且 EnvdUDS 非空的
target，同时要求可信 StableID 非空。receiver 调用 `GET /metrics`，携带
`X-Access-Token: <EnvdAccessToken>`，默认 interval 5s、timeout 1s。指标来自 guest，
不是 host quota、`memory.current`、balloon grant 或 image-diff 大小。源数据契约参照
[上游 envd metrics](https://github.com/e2b-dev/infra/blob/main/packages/envd/internal/host/metrics.go)。

| envd JSON | OTel gauge | Unit | E2B 字段 |
|---|---|---|---|
| `cpu_count` | `sandbox.cpu.count` | `{cpu}` | `cpuCount` |
| `cpu_used_pct` | `sandbox.cpu.used` | `%` | `cpuUsedPct` |
| `mem_total` | `sandbox.memory.total` | `By` | `memTotal` |
| `mem_used` | `sandbox.memory.used` | `By` | `memUsed` |
| `mem_cache` | `sandbox.memory.cache` | `By` | `memCache` |
| `disk_total` | `sandbox.disk.total` | `By` | `diskTotal` |
| `disk_used` | `sandbox.disk.used` | `By` | `diskUsed` |

`ts` 提供 guest Unix-second timestamp。缺失、null、负数、格式错误或非有限值会拒绝
该次 scrape；不使用旧 MiB 字段替代。CPU 百分比保留 envd 的原始汇总口径，不按 host
quota 再归一化。内建指标不使用 `kuasar.sandbox.*` 命名。

Pause 移除 scrape target；resume 创建新本地 generation；delete 移除 target。
Endpoint/token 变化取消旧请求；即使 transport 忽略取消，也丢弃 stale response。
断线使整个 view 失效，直到新一轮 full sync + Bookmark。每个 `envd/name` 实例独立维护 heap scheduler、有界
worker/queue 和 idle pool, 避免为每个 target 创建 timer goroutine。连接池 key 包含
UDS、本地 generation 与 SandboxID，不能把一个 sandbox 的 UDS connection 复用于另一个。
活跃 scrape 最多 `concurrency` 个，空闲 scrape connection 最多 `concurrency` 个。

### 原生 Collector 配置

`collector` 是原样交给 Collector 的配置 map, 由原生 confmap provider 和组件校验解析.
支持 `receivers`、`processors`、`exporters`、`connectors`、`extensions`、
`service.extensions`、`service.pipelines`、`service.telemetry`. 实际运行的就是解析后
的 graph. Factory 每种 type 只注册一次; 配置通过 `envd/fast`、`envd/slow` 等实例名
分别定义周期和生命周期. 无效或未使用选项不会被悄悄转换成能力更少的路由语言.

| 类别 | 发行包内组件 type | 版本 |
|---|---|---|
| Receiver | `envd`、`sandboxstats`、`sandboxotlp` | 当前 orchestrator 源码 |
| 基础设施 receiver | `otlp` | Collector 0.145.0 |
| Processor | `batch`、`memory_limiter` | Collector 0.145.0 |
| Processor | `filter`、`transform` | contrib 0.145.0 |
| Exporter | `otlp`、`otlp_http`、`debug` | Collector 0.145.0 |
| Exporter | `prometheusremotewrite`、`clickhouse` | contrib 0.145.0 |
| 主存储适配 exporter | `sandboxstorage` | 当前 orchestrator 源码 |
| Connector | `routing` | contrib 0.145.0 |
| Collector extension | `health_check` | contrib 0.145.0 |
| Config provider | `env`、`file`、`yaml` | confmap 1.51.0 |

静态构建可通过 `app/telemetry/otel.Components` 增加全部 factory 类别、provider 和
converter. Collector 子配置支持原生 `${env:NAME}`、`${file:/path}` 解析, 组件自行
校验选项. 标准基础设施 `otlp` 与已链接 exporter 可运行不含 SandboxID 的 logs/traces
pipeline; 只有沙箱业务 receiver 专门处理 metrics. 不增加 logs/traces 业务查询 API.
`proxy_netns` 只约束配置的各个 `sandboxotlp` 实例.

旧 `telemetry.scrape`、`telemetry.otlp` 和列表式 `telemetry.exporters` 被严格拒绝.
应迁移到原生 receiver/exporter 实例, 并在 `service.pipelines` 显式连接. Envd 选项
改为 `collection_interval`、`timeout`、`concurrency`; sandboxotlp 保留 `http_listen`、
`grpc_listen`、`max_connections`、`max_requests`、`max_request_bytes`, 不再有 `enabled`.
是否将 receiver 接入 pipeline 决定是否运行. 参见完整[本地部署](../deploy/telemetry.example.yaml)
和[双 sink 配置](../deploy/telemetry-fanout.example.yaml).

### 原生 sandboxstats receiver

`sandboxstats` 从同一 RouteEntry view 发现精确 SandboxID, 调用 conductor 现有
`config_socket` 的批量适配. 当前 telemetry lease 和真实 peer PID 授权该读取;
连接 UDS 不等于取得普通租户 API 权限. 它不收集租户 API key, 不持有 sandbox ctl
client、不读取 usage 文件、不维护第二套 port 绑定. Conductor 组织原生来源, 保留
ownership/binding 校验及失败规则.

配置项包括 `resource_interval` (默认 5s)、`traffic_interval` (10s)、`usage_interval`
(1m)、`timeout` (上限 5s)、`concurrency` (默认 4, 范围 1..8). 周期零表示关闭对应
section; 启用周期为 1s..1h, 至少启用一项. 每实例每次请求最多 64 个 SandboxID,
响应遵守 conductor 的 4 MiB 上限. 同一 section 的轮次不重叠, 完成后开始下一周期.
取消约束请求等待及投递. 配置来源不可用会记录错误并丢弃本次读取, 不导出零值或
旧的完整响应. Resource 选择当前 starting/running 对象; traffic 和 saved usage
也读取 paused 对象. Conductor 接纳的数据不因后续路由变化而撤销.

以下投影均为 Gauge, 可信来源为 `sandbox.telemetry.source=sandboxstats`. 数值表示
当前 counter 或原生累计端点, 不承诺永不重置. 原生 HTTP/usage JSON 保持无损;
float64 投影会舍入大于 2^53 的 uint64 以及 128-bit 积分. 遥测 scalar 历史不能反推
无损 native usage 账本.

| 原生 section | Metric 前缀及后缀 | 观测语义 |
|---|---|---|
| resource | `sandbox.resource.cpu.capacity`、`cpu.allocatable`; `memory.capacity`、`memory.headroom`、`memory.reserved`、`memory.used`; `cpu.seconds` | 单位为核、字节、秒. 配置/reservation 是当前读取; 宿主 Used/CPU 保留原生 `timestampUnix`. 各字段分别省略缺测并保留合法零. Headroom 是气球控制生效的 allocatable memory, 不是节点 reservation; CPU allocatable 表示相对 weight, 不是 quota |
| traffic | `sandbox.traffic.state`、`max_inflight`、`idle_since`、`inflight.parking`、`inflight.connected`; `service.parking`、`service.connected`、`service.max_inflight`、`service.idle_since` | 当前 Proxy ingress 读取, 带 state/service 属性. Admission limit 为零表示不限. 可选 idle 时间戳保持可选, 只描述已接纳 ingress, 不代表全沙箱空闲 |
| traffic | `sandbox.traffic.platform` / `sandbox.traffic.transit` 加 `.rx.packets`、`.rx.bytes`、`.tx.packets`、`.tx.bytes` | 当前 connector counter 读取, 方向为沙箱视角, 不跨观测点相加. 缺失平面不发布 counter; `egress:{}` 不产生伪造 egress series |
| usage | `sandbox.usage.cpu.seconds`、`memory.integral`、`memory.span`、`memory.covered` | 只投影 saved 原生端点. CPU 为 native known ns / 1e9; 积分为 byte-ns / 1e9, span/coverage 为 ns / 1e9. `usage.name`、`usage.status` 和 CPU 的 `usage.complete` 保留既有类别/有效性. Source identity 和 run_epoch 仍是 native metadata, 不作为 label |
| usage | `sandbox.usage.saved.available`、`enabled`、`saving`、`unknown_tail`、`save_error`、`read_error` | 当前读取状态的独立 Boolean Gauge. 不重标 saved 数量时间, 不把不确定尾部变成可靠零消耗 |

Usage 采集请求 `view=saved`. 累计数量保留 Record 的 `saved_utc_ns`; 重复读取只发布
相同端点, 不相加、不重新对 memory 积分. 未观测的数量被省略. Pause 或替换来源
会使当前位置失效, 但不会抹去已保存累计量; 已知贡献及 coverage 保留原始 record 时间
继续发布. `usage.source_known`、`usage.position_known`、`usage.value_known` 用于
区分保存的贡献和当前来源观测.
Live 可以领先 saved; crash 或保存失败可能丢失未保存尾部. Receiver 不强制 sample、
save、fsync 或 export ACK, 不改变原生 usage 策略. Current/live/history、精确整数、
raw counter、128-bit 积分、coverage 和详细错误仍由 [`stats/usage`](node-usage_zh.md)
提供. 启用 telemetry 不会启用 usage.

## 4. 直接面向沙箱的 OTLP

Telemetry 使用与 Proxy/MMDS 相同的 netns primitive, 自己在 `proxy_netns` 内绑定两种
listener; 空值表示当前进程 namespace.
`proxy_netns` 是唯一目标 YAML/JSON key, Go 字段为 `ProxyNetNS`.
早期 Preview 中使用 `sandbox_netns` 的配置必须迁移 key; strict decode 拒绝旧字段,
也拒绝同时提供两个字段. Telemetry 未进入现有 Stable 配置合同.
未知 namespace 名称/路径, setns 或 bind 失败均终止启动, 关闭已创建 listener 及应用资源,
不回退宿主 namespace. 仅创建 listener 时进入指定 namespace, 随后调用线程恢复原 namespace.
配置不移动整个进程, 不改变 conductor/query/envd UDS, 远端 exporter 或 query client;
这些通道继续使用正常的进程网络环境.
`sandboxotlp` receiver 支持 metrics: 4317 上的 OTLP/gRPC
MetricsService，以及 4318 上的 OTLP/HTTP `POST /v1/metrics`（protobuf/JSON，可选 gzip）。
不增加 OTLP token 身份协议；本 metrics 组件不接收应用 traces/logs。不要把 listener
放到应用 Proxy 或另一层 L7 reverse proxy 后面。

使用已有 connector management path。例如 Proxy/MMDS 与 telemetry 同处 management
namespace、监听 loopback 时，保留现有 MMDS mapping，并在部署的 vswitch 命令中增加：

```text
--mgmt-extract=sandbox-proxy:sw0m0:169.254.169.254/32
--mgmt-service=169.254.169.254:4317:127.0.0.1:4317
--mgmt-service=169.254.169.254:4318:127.0.0.1:4318
```

外层配置 `proxy_netns: sandbox-proxy`, 在 `sandboxotlp` receiver 中配置
`grpc_listen: 127.0.0.1:4317`、`http_listen: 127.0.0.1:4318`.Guest exporter 使用 `http://169.254.169.254:4318`
或对应 gRPC 端口。Connector 已有的 slot-derived SNAT 把共享 guest inner IP 转为分配
给该 sandbox 的 FloatingIP；service DNAT 选择 telemetry listener，management 回程
恢复 guest tuple。这是数据包直接送达 telemetry，不是转发身份 header。已有 loopback
management 部署要求 management interface 的 `route_localnet=1`；也可在可路由的
namespace interface 地址监听，把 mapping target 换成该地址，并沿用 FloatingIP return
route。参见 [connector management network](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch_zh.md#23-数据包流向)。
使用节点实际的 management namespace/device 名，不另建第二套交换机。
[部署模板](../deploy/telemetry.example.yaml) 包含对应 management-service 映射.
组件真实 guest E2E 通过这条已有路径验证两个协议. 特权 Collector 回归同时占用宿主相同端口,
在隔离 namespace 内发送 HTTP/gRPC, 核验来源身份及向宿主独有目的地导出.
无权限环境的 skip 不构成网络语义的通过证据.

Route view 同时索引 SandboxID 与 FloatingIP，复用 MMDS 的 IPv4 解析、active-state 与
反向查找一致性规则。HTTP/gRPC 身份都来自 accepted transport peer；`Forwarded`、
`X-Forwarded-For` 及 guest resource attributes 不授予身份。未知/paused/deleted peer
在 TCP accept 时 fail closed，HTTP/gRPC decode 前即关闭连接。Full sync 前连接的
客户端可以在 view ready 后重连，匿名 keep-alive connection 不会变成可信连接。
身份固定在 accepted connection；
remap、pause 或 stream invalidation 会关闭连接，不会把旧连接变成 successor 的连接。

来源接纳时, envd 校验当前 target, sandbox OTLP 校验已固定身份的连接.
Core 在每个 pdata resource 上覆盖 `sandbox.id`、`sandbox.stable_id` 和
`sandbox.telemetry.source`, 移除 point/scope 中冲突身份. 规范化为相同标准 exporter
label 的拼写(如 `sandbox_id` 或 `sandbox-telemetry-source`)也属于保留身份属性,
在写入可信身份前移除, 避免 Guest 属性在导出时混入可信 SID、StableID 或 source
值. Envd 使用 `envd` 来源,
guest OTLP 使用 `otlp`; 全局不将来源限制为这两个名称. Guest 即使复制 envd 指标名,
也不能伪造 envd 来源. 入站移除旧平台精确 key `sandbox.run_id`、`run_id`、`runId`、
`RunID`; 保留 `application.run_id` 等用户属性.

已接纳 resource 自身携带身份, 可以通过标准 batch、queue 和 retry. 一个 batch 可以
包含多个 SandboxID. 后续 pause/delete 或原请求 context 丢失不撤销已接纳历史.
调用下游 processor/exporter 时不持有 view 锁. 尚未结束的 envd fetch 或尚未接纳的
OTLP 请求在 target 失效时仍然失败. 普通基础设施 receiver 使用自身配置, 不要求
SandboxID. 可信部署 processor 和静态扩展仍属于原信任域, 可以转换 pdata; 异步
pipeline 边界不存在最终 sandbox-only guard.

尽可能在 transport decode 前施加上限：默认每 listener 256 条连接，全局 32 个并发
请求，HTTP 压缩体/解压体与 gRPC message 各 4 MiB。Resource/point attributes 最多
32 个，key 128 bytes、value 256 bytes；batch 最多 128 resources、16,384 points，每
scope 最多 4,096 metrics。超限返回错误，不悄悄截断。
gRPC server 的 10s deadline 在读取 message body 前就开始，停滞客户端不能无限期
占用全局请求槽位。

## 5. Primary storage 与 extra exporters

`telemetry.storage` 选择唯一主存储，必须同时实现 Collector write 和
`extension.Reader` history read. `collector.exporters` 与 `service.pipelines` 声明
全部写入目的地及 fan-out 边, 不隐式成为 primary reader.`storage.type: none` 必须有
exporter；telemetry 继续 collect/enrich/forward，但不注册 query API，公开 metrics 为 503。

### Local (显式启用)

Core `sandboxstorage` Collector exporter 指向 embedded Prometheus TSDB；`Local`
backend 同时提供写入与同一 DB 的直接 reader。不启动 Prometheus web server、scrape manager、rule
manager 或 Alertmanager。配置：

```yaml
telemetry:
  storage:
    type: local
    path: /var/lib/sandbox/telemetry
    retention: 168h
    max_size: 10GiB
    max_series: 1000000
```

DB 管理 WAL、replay、compaction 和 retention。Canonical batch 以事务追加，失败回滚；
存储时间精度为毫秒，截断不足毫秒的部分。完全相同的重复 sample 幂等；同 timestamp
不同值返回错误，不覆盖历史。接受 min(5m, retention) 内的乱序 sample，更旧数据失败。
早于 wall-clock retention 的数据被省略，超过当前时间一分钟的数据被拒绝，避免污染
head。即使没有新 sample，paused/deleted 沙箱的历史也继续过期。后台 head/block
expiration 回收空闲数据，query 立即执行 retention 过滤。

`max_size` 是 Prometheus TSDB retention-size 目标，不是文件系统硬配额：head/WAL、
compaction 临时数据可以使占用超过该值。须预留磁盘余量并监控文件系统。
`max_series` 限制 live head series（含 labels）；replay 后 head 已超限时启动失败。
Scrape/ingress/request 上限限制单次操作增长。Storage health error、corruption/WAL
recovery warning 和启动错误会传播为组件失败，不会悄悄开放不完整 reader。保留故障 DB
用于离线诊断/备份恢复，不自动删除。进程重启 replay WAL；不额外承诺断电持久性。

Canonical mapping 保留 UTF-8 OTel metric 名。可信身份使用直接 label，其他属性使用
不发生命名碰撞的 `resource.`、`scope.`、`point.` label namespace，并包含 scope
name/version、unit、metric kind。Sum 显式保留 temporality/monotonicity，不隐式把 delta
转为 cumulative。Histogram/exponential histogram 转为 count/sum/cumulative-bucket
scalar series；summary 转为 count/sum/quantile series。展开有界：histogram bounds/
quantiles 最多 160 个，每次 write 最多 65,536 scalar samples。Scalar 主存储不索引
exemplar 与 histogram min/max；extra OTLP exporter 保留 Collector pdata 表达。
Float64 对大于 2^53 的整数存在通常的精度限制。E2B 只读取可信 envd gauge。

### Prometheus-compatible readable primary

```yaml
telemetry:
  storage:
    type: prometheus
    retention: 168h
    prometheus:
      endpoint: https://metrics.example.com/prometheus
      headers: {Authorization: "Bearer REPLACE_WITH_PROTECTED_MATERIAL"}
```

Adapter 使用标准 Snappy/protobuf remote-write v1 写入 `<endpoint>/api/v1/write`，
通过 `<endpoint>/api/v1/read` 协商 `STREAMED_XOR_CHUNKS`，再执行相同的 field-wise MAX。
每次历史边界查找和数据查询各使用一次请求，包括长 retention 下的空历史和稀疏历史。
Frame 校验 checksum，大小限制为 32 MiB，每次仅保留一个 frame；10s HTTP deadline
覆盖整个 response body。完整 edge chunk 中的 sample 按精确、包含端点的范围过滤。
只支持 `SAMPLES` 的后端可回退到单个 Snappy response，压缩前后均限制为 32 MiB；
更大的历史需要 streaming。
Backend 必须同时启用两个 API 并接受 Prometheus 3 UTF-8 metric/label 名；只有 query
server 或 write-only exporter 不够。Remote read 保留精确观测，不引入 PromQL 的
lookback/step interpolation。参见 [remote read API](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/)。
配置 retention 定义 read lookback/write age policy；后端 retention、容量与 cardinality
控制需独立配置。

### ClickHouse readable primary

```yaml
telemetry:
  storage:
    type: clickhouse
    retention: 168h
    clickhouse:
      endpoint: https://clickhouse.example.com:8443
      database: default
      table: sandbox_metrics
      headers: {X-ClickHouse-User: telemetry, X-ClickHouse-Key: REPLACE_WITH_PROTECTED_MATERIAL}
```

Database 必须已存在；telemetry 创建自身专用 MergeTree table 并按 retention 设置 TTL。
服务账号只需该表的 CREATE/ALTER/INSERT/SELECT 权限，不需要无关表权限。改变 retention
会更新 TTL。JSONEachRow 写入保留 canonical labels 与 DateTime64(3, UTC) timestamp。
并发小写入使用有界 server-side async insert batching，并等待 flush 完成，传播错误
与 backpressure。参数化 SQL 精确过滤 SandboxID、可信 envd source，整数 epoch 时间桶
对每个 metric 独立 group/MAX。重复/乱序 row 保持该 MAX 结果。读取有 time、row、byte、
thread、memory 上限，HTTP client disconnect 会取消 readonly query。不会把 HTTP 200
中的错误正文当作数据。参见 [ClickHouse HTTP](https://clickhouse.com/docs/interfaces/http)
与 [async inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)。

两种 external client 都限制连接池和响应大小、传播取消、拒绝 redirect，只访问操作方
配置的 HTTP(S) destination。凭据也可通过 Runtime provider 提供。外部存储自己负责
disk-size/cardinality enforcement；local 的 `max_size`/`max_series` 不配置外部 server。
没有刻意延期的 external adapter。

### 原生 exporter 与 custom primary

在 `collector.exporters` 声明 exporter, 再由 `collector.service.pipelines` 引用完整 ID.
[Collector fan-out 示例](../deploy/telemetry-fanout.example.yaml) 使用原生 batch、filter、
transform、routing、两个 OTLP HTTP sink、queue 和 retry. Queue 大小、consumer、retry
及 timeout 由原生 exporter 选项控制, 不会被生成的 graph 覆盖. 除非组件明确提供,
queue 不代表持久历史. Export 失败不会暂停 sandbox. 静态集成遵循
[扩展契约](extensions_zh.md#telemetry-bootstrap-与扩展).

## 6. E2B 历史查询

`GET /sandboxes/{SandboxID}/metrics?start=<unix-seconds>&end=<unix-seconds>`
只使用已有 SandboxID。查找 miss 绝不 fallback StableID，包括 cluster 模式。
Cluster Router 现有 public SandboxID → 当前 node SandboxID 路由保持不变；节点 telemetry
只读该精确 node SandboxID，不按 StableID 合并迁移历史。StableID 仅用于外部 observability
correlation。RunID 继续用于 runner/run-plane、systemd/pidfile、MMDSv2、logging/debug；
不作为 telemetry label、resource attribute、TSDB identity、query key 或 pause/resume
series discriminator。

Telemetry handler 解析非负整数 Unix-second boundary（最大至 UTC 2299-12-31），
从第一条/最后一条保留历史补齐省略的 start/end，验证 start <= end，计算 step，调用
Reader，再按 epoch 对齐桶对七个字段分别取 MAX：

| Range | Step |
|---|---|
| `< 1h` | 5s |
| `< 6h` | 30s |
| `< 12h` | 1m |
| `< 24h` | 2m |
| `< 7d` | 5m |
| 其余 | 15m |

起止边界均包含。不对齐的 start 可导致 bucket timestamp 早于 start，但参与聚合的
sample 仍必须位于请求范围内。不做平均或 last-sample selection。只有七个字段齐全
的 resource bucket 才输出 E2B 对象，不把缺失字段编造成零。合法空历史/范围返回 `[]`。
无效/重复 boundary 或 start > end 返回 400。与
[E2B 的范围解析](https://github.com/e2b-dev/infra/blob/87968fc1e1fa57d896378249ae14d09916382d75/packages/api/internal/clusters/resources_local.go)
一致，验证发生在补齐省略边界之后：历史存在时，仅提供晚于最后 sample 的 start，
或仅提供早于第一条 sample 的 end，返回 400；同时提供两端、合法但不相交的范围返回 `[]`。
Reader 失败返回脱敏 503。不插值、不填零、
不 Wake。每个查询有 15s context budget，最多八个并发 reader、100,000 output buckets；
超过查询容量返回带 Retry-After 的 503。

九个必需字段遵循 [E2B API schema](https://github.com/e2b-dev/infra/blob/main/spec/openapi.yml)：

```json
[{"timestamp":"2026-09-13T00:00:00Z","timestampUnix":1789257600,
  "cpuCount":2,"cpuUsedPct":12.5,"memTotal":1073741824,
  "memUsed":536870912,"memCache":134217728,
  "diskTotal":4294967296,"diskUsed":1073741824}]
```

平台已有 `/stats/resource` 与 `/stats/traffic` 保持独立只读即时接口，不替代 guest
metric history。

原生 resource、traffic 和 usage 读取由 conductor 负责, telemetry 停止时仍可使用. 可信 telemetry lease 经现有 config socket 消费选定 section, 不访问沙箱 ctl socket 或 usage 文件. Paused saved usage 不要求 active guest. 参见[原生 usage 与本机读取](node-usage_zh.md).

原生 traffic 使用平铺的 `state`、`maxInflight`、`inflight:{parking,connected}`、`idleSince`、`services`、`platform`、`transit` 和 `egress` 结构. Platform/transit 以沙箱视角保留 connector Mgmt/Transit 计数; `egress:{}` 表示尚无可发布统计. 它们不改变 Proxy ingress idle,也不把 management 监控包视为已接纳的应用流. Conductor 按当前交换机绑定经 connector 原生 Go API 批量读取. 参见[原生 traffic](node-proxy_zh.md#83-traffic-stats-与统一-worker-stream).

## 7. 配置、关闭与验证

通过 `node-ctl config telemetry --template` 或
`node-ctl config telemetry --config FILE [--resolve]` 检查声明。`--resolve` 对 telemetry
没有额外作用。YAML 限一个严格解码 document、最多 4 MiB；未知/重复字段失败。默认值
只应用一次，不在 custom hook 后重新填充。`paths.telemetry_executable` 沿用 sealed
bootstrap 选择受信静态 App，不使用 Go runtime plugin。参见
[部署 YAML](../deploy/telemetry.example.yaml)、[service unit](../deploy/node-telemetry.service)、
[custom telemetry](../examples/custom-telemetry/README_zh.md)。

关闭时先停止 route subscription、撤销 query lease，drain query/OTLP ingress，再关闭
Collector receivers/processors/exporters，然后 extension，最后 primary storage。
启动失败也按相同资源 ownership 回收，即使启动失败也取消 extension 保留的后台工作。
Storage path 应持久化；不要让 conductor/Proxy 的 systemd dependency require telemetry，
telemetry 故障不得改变 sandbox lifecycle。

可重复验证（源码目录需有 sibling dependencies）：

```sh
GOWORK=off go test ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api ./app/telemetry ./config ./cmd/node-ctl
GOWORK=off go test -race ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api
GOWORK=off go test ./internal/telemetry -run '^$' -bench BenchmarkEnvdDensity -benchtime=2x -benchmem
GOWORK=off go test ./internal/telemetry -run '^$' -bench 'Benchmark(Local|Scrape)' -benchtime=100x -benchmem
TELEMETRY_CLICKHOUSE_TEST_URL=http://127.0.0.1:8123 GOWORK=off go test ./internal/telemetry -run TestClickHouseIntegration -count=1
TELEMETRY_PROMETHEUS_TEST_URL=http://127.0.0.1:9090 GOWORK=off go test ./internal/telemetry -run TestPrometheusIntegration -count=1
make test vet build
make test-e2e # 要求组装的项目 BIN 与真实 KVM host
```

可选 live-engine test 应使用可丢弃 endpoint。ClickHouse test 只创建/删除唯一命名测试表。
Prometheus test 写入唯一命名 sandbox series（由后端 retention 回收），要求启用 remote
write、配置至少 5m 的 out-of-order window，验证 streaming、UTF-8 label、重复/乱序 sample、
field MAX 和精确时间边界。
Density benchmark 对 1k/10k/50k synthetic targets 测真实 5s 周期，报告 scrape count、
goroutine/FD 峰值（含 fixture server）、allocation、TSDB batch write 与 local query
成本。Receiver 与 TSDB 分开测量，不声称是端到端生产容量。时间数字不作为普通 CI gate，
应在代表性 host/workload 重复测量。已有真实 Proxy E2E fixture 同时覆盖 envd → Collector
→ local DB、guest 经 connector mgmt-extract 的 OTLP、auth、paused query、telemetry
不可用、lease 撤销和 TSDB restart，不另建第二套 VM 生命周期。
