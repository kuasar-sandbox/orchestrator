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
Conductor Plugin Plane ── 完整 RouteEntry stream ──► Telemetry view
                                                       │
envd /metrics 经各 sandbox UDS ─► envd receiver ─────────┤
guest ─► connector mgmt-extract ─► OTLP HTTP/gRPC ───────┤
                                                       ▼
                             Collector: trusted identity enrichment
                                      → custom processors（可选）
                                      → final identity guard
                                      ├─ primary storage exporter → DB
                                      └─ extra exporters → 外部 Collector

GET /sandboxes/{SandboxID}/metrics
  → Conductor API 鉴权 + ownership → live registration 的 query UDS
  → Telemetry E2B handler → primary Reader → 同一个 DB
```

所有主存储写入都经过真正的 OpenTelemetry Collector service graph 和 exporter。
receiver 不持有 storage handle。内建分发只链接实际需要的 Collector component，
包括 OTLP/HTTP extra exporter；不嵌入整个 contrib distribution 或 Prometheus server。

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
断线使整个 view 失效，直到新一轮 full sync + Bookmark。heap scheduler、固定上限的
worker/queue 和全局 idle pool 避免为每个 target 创建 timer goroutine。连接池 key 包含
UDS、本地 generation 与 SandboxID，不能把一个 sandbox 的 UDS connection 复用于另一个。
活跃 scrape 最多 `concurrency` 个，空闲 scrape connection 最多 `concurrency` 个。

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
支持的 signal 是 metrics: 4317 上的 OTLP/gRPC
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

配置 `proxy_netns: sandbox-proxy`、`grpc_listen: 127.0.0.1:4317`、
`http_listen: 127.0.0.1:4318`。Guest exporter 使用 `http://169.254.169.254:4318`
或对应 gRPC 端口。Connector 已有的 slot-derived SNAT 把共享 guest inner IP 转为分配
给该 sandbox 的 FloatingIP；service DNAT 选择 telemetry listener，management 回程
恢复 guest tuple。这是数据包直接送达 telemetry，不是转发身份 header。已有 loopback
management 部署要求 management interface 的 `route_localnet=1`；也可在可路由的
namespace interface 地址监听，把 mapping target 换成该地址，并沿用 FloatingIP return
route。参见 [connector management network](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch.md)。
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

Core 在 resource 层用当前 RouteEntry 覆盖 `sandbox.id`、`sandbox.stable_id`，移除
point/scope 中冲突身份。可信 `sandbox.telemetry.source=envd|otlp` 也不能伪造；即使
guest OTLP gauge 复制内建 resource metric 名，也不能污染 E2B envd 历史。定制 processor
位于 enrichment 与最终 revalidation/overwrite guard 之间；丢失私有 ingress context
会 fail closed。Resource、scope、datapoint 的 RunID-like 属性均被移除，包括定制
processor 新添加的此类属性。

尽可能在 transport decode 前施加上限：默认每 listener 256 条连接，全局 32 个并发
请求，HTTP 压缩体/解压体与 gRPC message 各 4 MiB。Resource/point attributes 最多
32 个，key 128 bytes、value 256 bytes；batch 最多 128 resources、16,384 points，每
scope 最多 4,096 metrics。超限返回错误，不悄悄截断。
gRPC server 的 10s deadline 在读取 message body 前就开始，停滞客户端不能无限期
占用全局请求槽位。

## 5. Primary storage 与 extra exporters

`telemetry.storage` 选择唯一主存储，必须同时实现 Collector write 和
`extension.Reader` history read。`telemetry.exporters` 和 custom Collector exporters
是额外的 write-only fan-out，不隐式成为 primary reader。`storage.type: none` 必须有
exporter；telemetry 继续 collect/enrich/forward，但不注册 query API，公开 metrics 为 503。

### Local（默认）

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

### Extra exporter 与 custom primary

```yaml
telemetry:
  exporters:
    - name: observability
      type: otlphttp
      endpoint: https://collector.example.com
```

内建 OTLP/HTTP exporter 使用 16 MiB 有界内存 queue、两个 consumer、10s request timeout、
最多 30s retry elapsed time。Queue 不是持久历史，也不能让故障 primary query 成功。
临时 scrape/export 错误按组件行为丢弃或重试；telemetry 不会为保证投递而暂停 sandbox。
Custom primary 与 Collector integration 使用
[扩展契约](extensions_zh.md#telemetry-bootstrap-与扩展)。

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
