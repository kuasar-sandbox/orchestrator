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
Collector pipeline、可选本地 TSDB、独立 exporter 与查询 backend、历史查询及 E2B 转换。它不扩大 resource
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
telemetry endpoint、UDS 请求失败或 query backend 不可用返回 503。客户端取消传播到
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
| 可选本地 exporter | `sandboxlocal` | 当前 orchestrator 源码 |
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
启动或重连期间 discovery 尚未同步时, 到期轮次等待路由 bookmark, 不把未同步
视为完成空轮次而消耗整个周期. 取消约束该等待、请求等待及投递.
配置来源不可用会记录错误并丢弃本次读取, 不导出零值或
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

## 5. 写入与查询独立装配

`collector` 是原生写入 graph. `query.backend` 独立选择 `none`、`local`、
`prometheus`、`clickhouse` 或 `custom`; `query.handler` 选择 `none`、`e2b` 或
`custom`. 只有 Reader 和 handler 都可用时才注册 query UDS. 没有 handler 时,
公开 `/metrics` 返回 503. Conductor 的三个原生 stats API 在所有模式中都保留原始来源.

| 部署方式 | 配置 |
|---|---|
| 远端写入与查询 | 标准远端 exporter, 加上匹配的 `query.backend`/endpoint/schema/labels |
| Write-only | Collector exporters, `query: {backend: none, handler: none}`; 不注册 query UDS |
| Query-only | 选择 backend 和 handler, 完全省略 `collector`; 不需要 dummy receiver |
| 本地写入与查询 | `local.enabled: true`, `sandboxlocal` exporter, `query.backend: local` |
| 本地加远端 fan-out | 启用 local, 同时引用 `sandboxlocal` 与远端 exporter; 明确选择一个 query backend |
| 定制查询合同 | 静态绑定 `Runtime.MetricsHandler`, 配置 `query.handler: custom` |

未发布的 `telemetry.storage`、组合接口 `extension.Storage`、`Runtime.Storage` 和
`Runtime.StorageHeaders` 直接被替换. 使用 `local`、`query`、`Runtime.QueryBackend`
及 `Runtime.QueryHeaders`; 本地 Collector exporter 改为 `sandboxlocal`. 不保留解析
alias 或第二条运行路径. 写 exporter 与读 backend 分别配置和验证: OTLP destination
不自动成为 query backend. 远端错误不会改选本地数据库, 也不会变成成功的空历史.

### 本地 TSDB

只有 `local.enabled: true` 才创建本地存储. 选择远端 query backend 或启动 Collector
本身都不创建本地 DB. Core `sandboxlocal` exporter 写入已有嵌入式 Prometheus TSDB;
不运行 Prometheus server、scrape manager、rule manager 或 Alertmanager. 本地查询
`query.backend: local` 同样要求显式启用 local. 完整配置见
[本地部署示例](../deploy/telemetry.example.yaml).

```yaml
query: {backend: local, handler: e2b}
local:
  enabled: true
  path: /var/lib/sandbox/telemetry
  retention: 168h
  max_size: 10GiB
  max_series: 1000000
```

TSDB 保留已有 WAL、replay、compaction 和 retention. WAL 合同独立于 native usage
的 no-sync 合同. 每次 Collector delivery 是一个 transaction, 失败会 rollback.
时间戳按毫秒保存, 截去不足毫秒部分. 完全相同的重复 sample 幂等; 同一时间戳存在
冲突值则失败. 接受 min(5m, retention) 内的乱序 sample, 更旧数据失败. Wall-clock
retention 排除过期观测, 即使所有沙箱均已暂停也继续回收空闲 head/block 数据.
超过当前时间一分钟的写入失败. 缓存观测不会被盖上新时间.

`max_size` 是 retention 目标, 不是文件系统配额; head/WAL 和 compaction 临时数据
可能超过它. `max_series` 限制 live head series, 并在 replay 后检查. 后台错误与
corruption/WAL recovery warning 会停止组件、撤销 query lease. 启动不会删除损坏 DB.
重启 replay WAL, 不新增断电持久性保证.

Scalar 表达保留 OTel metric 名. 已接纳身份使用 `sandbox.id`、`sandbox.stable_id`
和 `sandbox.telemetry.source` label. 其他属性带 `resource.`、`scope.`、`point.`
前缀; scope name/version、unit、kind 使用 `otel.*`. Sum 保留 temporality/monotonicity.
Histogram 与 exponential histogram 的 count/sum/cumulative bucket 保留原 metric
名, 通过 `otel.part`/`otel.bound` 区分; summary 使用 count/sum/quantile parts.
未记录的可选 Histogram sum 保持缺失. 每次写入最多展开 160 个 bounds/quantiles 和
65,536 个 scalar samples. Exemplar 与 Histogram min/max 不属于此 scalar query
表达. 其他 Collector exporter 保留各自支持的 pdata 表达. Float64 不能无损表示所有
超过 2^53 的整数, 也不能替代 native usage 的无损账本.

### Prometheus remote read 与标准 remote write

[Prometheus 部署示例](../deploy/telemetry-prometheus.example.yaml) 分别选择
`prometheusremotewrite` exporter 和 `prometheus` query backend. Exporter 使用自身
原生 queue、retry、batching 和 resource conversion 选项. Reader 只调用
`<query.prometheus.endpoint>/api/v1/read`, 不实现 Write.
[Query-only 部署示例](../deploy/telemetry-query-only.example.yaml) 可以直接查询已有
远端数据库, 不启动 Collector.

已链接 exporter 将 name/label 中的标点转换为下划线.
`resource_to_telemetry_conversion.enabled: true` 将已接纳身份带到每个 metric.
默认查询映射将逻辑 `sandbox.id`、`sandbox.stable_id`、`sandbox.telemetry.source`
对应到物理 `sandbox_id`、`sandbox_stable_id`、`sandbox_telemetry_source`.
`query.prometheus.labels` 可显式声明其他映射, 包括为保留原 UTF-8 名称的后端声明
同名映射. 重复或相互覆盖的映射验证失败. 结果属性使用配置的逻辑名; 其他远端 label
保持存储名称, 包括 point labels. 物理身份 alias 不能替换必需的精确 SandboxID 匹配.

通用查询使用后端物理 metric 名. 示例设置 `add_metric_suffixes: false`, 并将七个
E2B 字段映射到 `sandbox_memory_used` 等名称. 启用后缀时, 标准 unit/type 转换可能
产生 `task_payload_bytes`, 或以 `_total` 结尾的 cumulative Sum 名称; 如果这些指标
用于兼容 API, 应同步更新 E2B 映射. 查询代码不猜测 writer 的 namespace、名称规范化
或 unit 后缀. Server retention、remote-write 接收和乱序策略须在 server 独立配置.
Query 凭据须具有 remote-read 权限, 与 exporter 凭据无关.

Reader 协商 `STREAMED_XOR_CHUNKS`, 验证每个 frame 的 CRC32C, 限制 frame 为 32 MiB,
每次仅保留一个 frame. 提供 `SAMPLES` 的 server 使用单个 Snappy response, 压缩前后
均限 32 MiB. Bounds 和 Query 各用一次请求, 包括空历史和长期稀疏历史. 10s HTTP
期限覆盖 body. 完整 edge chunk 中的 sample 在聚合前按精确、包含端点的原始范围过滤.
此 API 不引入 PromQL lookback、插值或最后值延长. 参见
[remote read API](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/).
`query.lookback` 限制远端读取窗口, 默认 168h, 至少一分钟; 它不修改 exporter 写入
或 server retention.

Native histogram chunk 和 histogram sample 使用所链接的 Prometheus model 解码.
结果保留实际 metric 名, `otel.part` 使用 `count`、`sum`、`native_bucket`;
`otel.bound` 记录每个 native bucket 的实际开闭区间. Native bucket count 是独立
bucket 的绝对计数, 不是累计 bucket. Reader 支持选择这些派生属性. Classic
histogram 的 `_bucket`/`_sum`/`_count` series 和 summary quantile label 保留标准
exporter 的实际表示. Stale marker 不变成观测, 也不延长之前的值. 两种 unit suffix
配置和五类指标均通过真实 server 验证.

### ClickHouse 标准 schema 与只读查询

[ClickHouse 部署示例](../deploy/telemetry-clickhouse.example.yaml) 使用已链接的标准
`clickhouse` exporter 及其五张原生 metrics 表: `otel_metrics_gauge`、
`otel_metrics_sum`、`otel_metrics_summary`、`otel_metrics_histogram` 和
`otel_metrics_exponential_histogram`. Exporter 控制 database 创建、`create_schema`、
TTL、queue/retry 和写入权限. Reader 经独立配置的 HTTP(S) endpoint、凭据、database
与 `query.clickhouse.tables` 执行 SELECT. 它不建表、不改 TTL、不要求 Write.
旧 `sandbox_metrics` canonical 表不属于此 schema. 缺表、schema 不匹配或远端故障
都会报错, 不会变成空数据.

Reader 先匹配 `ResourceAttributes['sandbox.id']`, 再读取 row. 它使用标准 exporter
schema 中的 `MetricName`、`TimeUnix`、`Value`、resource/scope/point maps 与 metric
kind 列. 排除 `Flags.NoRecordedValue` row. Gauge/Sum 保留原 metric name/unit,
属性使用本地 scalar namespace. Summary parts 包括 count、sum 和 quantile;
显式 Histogram parts 包括 count 和 cumulative bucket. 标准 schema 未保留
Histogram `HasSum`, 因此不会把无法确定有效性的 sum 列发布为观测到零. Schema 也
缺少 exponential `ZeroThreshold`: exponential 结果发布 count/zero_count 与已存储
的 positive/negative bucket count, 使用 `otel.scale`, 并将原始 bucket index 放入
`otel.bound`, 不编造数值边界. 这些 schema 限制不删除 Collector 或其他 exporter 的
数据; 它们定义此 backend 的 scalar 读取映射.

SandboxID、metric 名、相等属性、时间和 step 都通过 SQL 参数传递. Client 不提供
SQL. 时间先投影到毫秒, 再做包含端点的过滤, 与 local/Prometheus 精度一致. Raw 查询
保留观测时间. MAX 先过滤原始 row, 再按完整属性集和 epoch 对齐 bucket 独立分组.
相同的重复观测去重; 同一毫秒的冲突 raw 值报错, 不任意挑选. 乱序/重复插入不改变
MAX. 缺失 Gauge 不延长到后续 bucket.

读请求限制 server 执行九秒、两个线程、256 MiB server memory、700,000 rows 与
32 MiB response; HTTP client 断连会取消 readonly query, HTTP-200 错误 body 也会
被拒绝. 参见 [ClickHouse HTTP](https://clickhouse.com/docs/interfaces/http).
两个远端 client 均限制连接池、传播取消、拒绝 redirect, 只访问受保护的操作方配置
目标. `Runtime.QueryHeaders` 替换读凭据; exporter 凭据通过原生 Collector config
provider 读取. 查询或认证错误都不回退本地 DB. 外部 size/cardinality 控制仍属于
外部 server.

### Fan-out 与静态扩展

每种 type 注册一次 factory, 由原生 Collector 配置声明各个实例与连接.
[Fan-out 示例](../deploy/telemetry-fanout.example.yaml) 使用 batch/filter/transform/
routing 和两个带 queue/retry 的 OTLP HTTP sink. 添加 `local.enabled` 和
`sandboxlocal` destination 即可启用本地 fan-out; Reader 仍由 `query.backend`
显式选择. Queue 持久性只遵循配置组件自身的合同. Export 故障不暂停或唤醒沙箱.
参见[静态扩展](extensions_zh.md#telemetry-bootstrap-与扩展).

`extension.Reader` 接受 `Selection{SandboxID, Metrics, Attributes}` 和
`Query{Selection, Start, End, Step, Aggregation}`, 返回包含 metric 名、完整属性及
时间/数值点的 `[]Series`. Metrics 使用精确名称, attributes 要求 key 存在且值相等
(空字符串不匹配缺失属性), 空 metric
列表选择该精确沙箱内所有名称. 不存在公共七字段 enum、来源白名单或公开 SQL/PromQL
输入. 省略的 aggregation 在调用任意 backend 前规范化为 `Raw`. `Raw` 的 Step
必须为零; `Max` 要求正数整毫秒 Step.
查询支持 1970–2299 年、最多 64 个 metric 名、110 个相等属性、100,000 条 series 与
700,000 个点. 缺失点保持缺失, 负 Gauge 合法. Bounds 对保留观测应用同一 Selection.

`Runtime.QueryBackend` 构造 Reader 加 Shutdown, 不要求 Write.
`Runtime.MetricsHandler` 按 `QueryScope` 提供标准 `http.Handler`. Scope Reader 永久
绑定 conductor 授权的精确 SandboxID, 拒绝通过其他 SID、属性 selector 或不同 context
替换它. 部署配置与静态扩展仍受信任. 定制 JSON 合同不等同于 E2B 兼容; E2B SDK 应
选择内建 E2B handler. 可构建的[定制 handler](../examples/custom-telemetry/query.go)
发布任意 raw series; 其 [query-only 配置](../examples/custom-telemetry/query.yaml)
不启动 Collector 或本地存储.

## 6. E2B 历史查询

`GET /sandboxes/{SandboxID}/metrics?start=<unix-seconds>&end=<unix-seconds>`
只使用已有 SandboxID。查找 miss 绝不 fallback StableID，包括 cluster 模式。
Cluster Router 现有 public SandboxID → 当前 node SandboxID 路由保持不变；节点 telemetry
只读该精确 node SandboxID，不按 StableID 合并迁移历史。StableID 仅用于外部 observability
correlation。RunID 继续用于 runner/run-plane、systemd/pidfile、MMDSv2、logging/debug；
不作为 telemetry label、resource attribute、TSDB identity、query key 或 pause/resume
series discriminator。

`query.handler: e2b` 选择独立 E2B adapter, 使用七项 `query.e2b.metrics`
映射与 `query.e2b.source` (默认 `envd`). 部署方可显式选择具有相同观测语义的其他
source. 通用 Reader 不限制这组映射或来源. 此 handler 解析非负整数 Unix-second boundary（最大至 UTC 2299-12-31），
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
Collector receivers/processors/exporters，然后 extension, 最后 query backend 与可选 local TSDB.
启动失败也按相同资源 ownership 回收，即使启动失败也取消 extension 保留的后台工作。
Storage path 应持久化；不要让 conductor/Proxy 的 systemd dependency require telemetry，
telemetry 故障不得改变 sandbox lifecycle。

可重复验证（源码目录需有 sibling dependencies）：

```sh
GOWORK=off go test ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api ./app/telemetry ./config ./cmd/node-ctl
GOWORK=off go test -race ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api
GOWORK=off go test ./internal/telemetry -run '^$' -bench BenchmarkEnvdDensity -benchtime=2x -benchmem
GOWORK=off go test ./internal/telemetry -run '^$' -bench 'Benchmark(Local|Scrape)' -benchtime=100x -benchmem
bash test/e2e/e2e_telemetry_backends.sh # source: Go + Docker; installed package: BIN + Docker
make test vet build
make test-e2e # 要求组装的项目 BIN 与真实 KVM host
```

组件自有 backend case 从固定 manifest digest 创建临时 Prometheus 3.5.0 和
ClickHouse 25.8 容器, 只向 loopback 发布端口, 记录实际版本与镜像身份, 并在退出时
删除自己创建的容器和 volume. 两个引擎与所有具名 case 均必须执行; 缺少前提条件、
skip 和失败均报错. Source CI 从组装的 `BIN` 目录定位精确 sibling checkout;
源码缺失仍报错. 仅在其他源码布局中设置 `TELEMETRY_SOURCE_ROOT`. Source 与
exact-assets 验证均通过 `test/e2e/run_all.sh` 执行同一 case. Exact-assets 使用
发布的 `BIN/node-ctl`, 不要求 Go, 不重新构建组件源码.
`TELEMETRY_BACKEND_OUT_DIR` 可指定保留配置、JSON test event、可执行文件 hash
和引擎日志的位置; CI 默认写入会上传的 metadata 目录. Fixture 表与 Prometheus
sample 只存在于临时容器内, 退出时删除其 volume.

两种布局均运行实际 node-ctl, 由可信基础设施 OTLP fixture 经原生 batch/queue、
标准远端 exporter 和匹配 Reader, 通过私有 E2B query UDS 验证读取. 断言覆盖
精确 SID/source 隔离、StableID 仅作 label、独立 MAX、包含端点的时间边界、
不完整 bucket、不延长 Gauge、不创建本地 DB 和正常退出清理. 此 backend case
不替代真实 guest/conductor 认证与 namespace 用例.

覆盖实际原生部署启动、名称/unit/resource label 转换、五类指标、重复/乱序 sample、
raw/MAX 边界、缺测、精确 SID、local/remote fan-out, 以及路由删除后已接纳 batch
仍可发布. Source 验证还从精确源码构建并执行 node-ctl 和 custom-telemetry, 经过 sealed
bootstrap, 沿真实 Plugin lease/query UDS 从真实 Prometheus 查询非 E2B 指标, 并
验证 query-only 无 Collector graph/本地 DB 的退出清理. 普通 unit 调用可以跳过
这些外部 fixture, 但 skip 不计为 backend 验收. Density benchmark 对 1k/10k/50k synthetic targets 测真实 5s 周期，报告 scrape count、
goroutine/FD 峰值（含 fixture server）、allocation、TSDB batch write 与 local query
成本。Receiver 与 TSDB 分开测量，不声称是端到端生产容量。时间数字不作为普通 CI gate，
应在代表性 host/workload 重复测量。

真实 guest fixture 还将 conductor 的 resource/traffic 观测经 sandboxstats、原生 batch
导出至宿主网络 HTTP sink. 已暂停沙箱通过同一 lease 授权读取面导出实际 saved usage;
断言与公开原生 API 比较 CPU/内存累计值及原始 saved 时间, 并确认沙箱保持 paused.

已有真实 Proxy E2E fixture 同时覆盖 envd → Collector
→ local DB、guest 经 connector mgmt-extract 的 OTLP、auth、paused query、telemetry
不可用、lease 撤销和 TSDB restart，不另建第二套 VM 生命周期。
