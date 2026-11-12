[English](observability.md) | [简体中文](observability_zh.md)

<a id="overview"></a>

# 观测

按职责和精度选择观测面。Journal 输出、native 统计与无损 usage 不同于采样的 Telemetry metrics；Conductor 保持生命周期权威，观测故障不形成新的生命周期 barrier。

**阅读路径：** [选择观测面](#observation-surfaces) · [当前统计](#native-stats-api) · [精确 usage](#native-usage) · [日志](#logs) · [Telemetry](#telemetry) · [Telemetry 配置](#telemetry-configuration)

<a id="observation-surfaces"></a>

## 观测面选择

| 需要 | 入口与保证 |
|---|---|
| 宿主、guest app 与 Build 输出 | [Journal 输出](#logs)，明确字段、target、查询与发送限制 |
| 当前资源/流量 | [Native stats API](#native-stats-api)，保留资源和服务维度 |
| 精确累计用量 | [Native usage](#native-usage)，整数与无损结构；不能用浮点 metrics 替代 |
| 历史指标与 OTLP metrics | [Telemetry](#telemetry)，显式 Collector/TSDB/查询后端，支持写入或只查询模式 |

当前面向 Sandbox 的 Telemetry 入口和查询契约是 metrics。基础设施 Collector 可配置自己的 logs/traces pipeline；这不等于实现跨组件 distributed tracing，也不增加 Sandbox logs/traces API。

<a id="native-stats-api"></a>

## 即时 resource / traffic stats

`GET /sandboxes/{SandboxID}/stats/resource` 与
`GET /sandboxes/{SandboxID}/stats/traffic` 沿用普通 API key 鉴权路径。缺失或格式
错误的 key 返回 401；Sandbox 业务记录不存在或 ownership 校验失败返回 404。
Stats handler 设置 `Cache-Control: no-store`，只读观察，不调用 Connect、Wake、
Resume、Pause 或 envd。

resource stats 经既有 ctl socket 读取当前 sandbox-ctl owner 的最终资源规格和宿主 VMM cgroup. conductor 在有观测时组合内置 resource controller 的 reservation:

```json
{
  "cpuCapacity": 2,
  "cpuAllocatable": 0.5,
  "memoryCapacity": 2147483648,
  "memoryHeadroom": 268435456,
  "memoryReserved": 1073741824,
  "memoryUsed": 536870912,
  "cpuSeconds": 12.345678,
  "timestampUnix": 1786482600
}
```

`cpuCapacity` 是 `capacity.cpu`,单位为核. `cpuAllocatable` 是映射到 `cpu.weight` 的既有相对调度规格,不是 fractional-core 硬 quota 或性能保证. `memoryCapacity` 是以字节表示的 `capacity.memory`. `memoryHeadroom` 是最终生效的 `resources.allocatable.memory`,表示气球控制的 headroom,与 Budget、guest free memory 和 NodeReservation 不同. `memoryReserved` 是节点实际承担的 reservation 观测;动态 controller 未启用或没有观测时省略.

`memoryUsed` 读取 `memory.current`,`cpuSeconds` 读取 `cpu.stat.usage_usec / 1e6`,两者来自同一个已 pin 的宿主 VMM cgroup. 不增加 ctl 进程或 guest CPU,不扣 inactive file/balloon,也不把宿主 memory 限制在 guest capacity 内. CPU seconds 是当前来源的累计值,来源重建可以重置;生命周期累计由 native usage 负责. 原生 JSON 保留整数字节,CPU seconds 以具有微秒精度的精确十进制输出. 消费端转为二进制浮点时可能损失精度.

各宿主字段分别保留有效性:合法零值正常返回,缺失的 memory 或 CPU 观测分别省略. `timestampUnix` 是实际读取时间;两个宿主字段都没有观测时省略,不把 heartbeat/cache 时间刷新为当前时间. 没有 live VMM 时仍可从 owner 读取最终规格,但不伪造当前宿主观测. starting/running/pausing 没有可达 owner 为 503;paused 和其它没有当前 runtime 的状态为 409. 与 runtime 或 binding replacement 并发的读取失效.

静态/动态资源控制、usage 关闭、telemetry 停止时均可独立使用. 读取不 Wake,不采样/保存 usage,不调用 guest 或 Cloud Hypervisor,不修改控制策略,也不创建 resource 历史. 原生 API 的 `cpuCount`、`memTotal`、`memAllocatable` 和 `memUsed` 已删除;旧的 reservation 值 `memAllocatable` 由 `memoryReserved` 替代,`memoryHeadroom` 单独表达 headroom. E2B `/metrics` 和 list/SKU 兼容字段保持原有含义.

`GET /sandboxes/{id}/stats/traffic` 返回最终 node proxy 已接纳的逻辑 ingress. 普通请求遵循实际生效的鉴权策略(`log`/`off` 放行不保证凭据有效);exec 始终要求有效 KAT 鉴权及 ExecRequest 准入:

```text
ingress = parking + connected
parking = 已接纳,但生命周期激活/最终 backend dial 尚未完成
connected  = 最终 node proxy→sandbox backend 已建立且尚未最终 Close
```

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
    "connected": 0
  },
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "hold": {
      "parking": 0,
      "connected": 0,
      "idleSince": "2026-08-12T14:00:00Z"
    },
    "forward": {
      "parking": 0,
      "connected": 0,
      "idleSince": "2026-08-12T14:03:21.123456789Z"
    },
    "exec": {
      "parking": 0,
      "connected": 0,
      "idleSince": "2026-08-12T14:00:00Z"
    }
  },
  "platform": {
    "rxPackets": 57,
    "rxBytes": 5108,
    "txPackets": 39,
    "txBytes": 8042
  },
  "transit": {
    "rxPackets": 2,
    "rxBytes": 196,
    "txPackets": 3,
    "txBytes": 294
  },
  "egress": {}
}
```

`maxInflight` 来自 Proxy master 当前 applied route 的目标节点 effective policy,不是从
conductor Sandbox row 推导;全零对象表示 unlimited。配置 `M` 是整个 node Proxy 对该
Sandbox 的近似上限,`N` 个 worker 的短暂理论上界为 `M+N-1`,不是每 worker 的 `M`。

e2b 返回 `forward/e2b:envd/e2b:code-interpreter/exec`，bare 返回 `forward/exec`。流量 Proxy 与 Conductor 协商支持 hold 时，两种 profile 均另含 hold。顶层 `inflight` 是各 service 求和。 `hold.connected` 计数单位为工作项；物理连接指标和 `maxInflight` 不包含它。每个 service 仅在两项为零时附
`idleSince`;顶层时间表示运行中且计数全零的沙箱内最后一个归零的适用 service；有启动时钟证据时按启动时钟确定顺序。
starting/pausing/paused 返回 inflight 但不返回顶层 idle。接口不返回 idle bool/duration、last
open/close、累计连接数、速率/延迟/端口明细或 worker 信息。
旧版 Telemetry 保留 Proxy 按启动时钟确定的物理空闲起点。若 Proxy 仅提供物理空闲的墙钟时间，且它与已记录的 hold 释放时间不同，原生 API 省略顶层 `idleSince`；各物理 service 的空闲时间和计数仍可读取。

conductor 经当前 trusted Proxy registration 的 `stats_socket` 读取 master cache,查询时不扇出 worker.master 未注册,
route 未完成同步、RunID/profile/state 不匹配、worker stream 故障或 replacement 未 ready 为 503。
stats 的 503 窗口不影响 Proxy master 的 route/admission authority 或 Create barrier。完整共享
admission算法、误差证明、worker-local状态机、绝对快照 stream 和故障窗口见
[node-proxy_zh.md](node-proxy_zh.md) [per-Sandbox traffic admission 与 stats](node-proxy_zh.md#admission)。

响应为平铺结构: `state`、`maxInflight`、`inflight`、`idleSince`、`services`、`platform`、`transit` 和 `egress`. Conductor 拥有该原生 API,按照当前 Sandbox 到 switch/port 的绑定组织既有 Proxy 观测与网络计数. `platform` 映射 connector 的 Mgmt RX/TX 包数和字节数,`transit` 映射 Transit RX/TX;两者都采用沙箱视角. 已配置且当前有效的端口返回全部四项无符号整数,包括合法的 0. 仅当 `VswitchPort`、`FloatingIP`、`InnerIP` 和 `PortMAC` 全部为空时,沙箱才返回空 `platform`/`transit` 对象,表示没有适用的当前观测. 部分网络绑定在查询任一来源前返回 503. `egress: {}` 始终表示尚无可发布的 egress 统计,不是观测到零流量. 不发布 per-service 包计数、来源分组或 network API 别名.

`connected` 替代原生 `inflight.egress` 和 `services[*].egress`,保持后端连接已建立至最终 Close 的原有含义. 顶层 `idleSince` 覆盖 Proxy 已接纳的 ingress 和业务登记的 hold;management 监控包不会刷新它,它不对沙箱计算或整个网络空闲作出结论. packets 是包数而非应用请求数;bytes 是观测帧字节而非吞吐速率. management 使用既有端口/management ingress 帧长度;transit 使用封装前或去除外层 GENEVE 头后的帧长度,保留以太网头. 不把不同观测点相加成总流量. 累计计数属于当前 attachment,端口复用后可以重置;API 不建立网络历史.

Conductor 每个 switch 最多批量读取 64 个当前端口,调用 connector Go `Stats(ports)`,不逐沙箱启动 CLI,不维护第二套生命周期权威. 它复用原有 allocation/detach fence;connector 复用 pin 目录共享锁、当前 pinned-map ID 和清零确认标记. 既有 Proxy stats socket 也支持有界批量. 已配置来源读取失败、清零未确认、控制锁占用、结果不完整或绑定变化时,整个读取返回 503,不会用 0 或旧样本伪装完整响应. 公开 API、可信本机 batch 与 conductor extension 共用相同领域读取. Stats 独立于 telemetry,不参与 Create/Resume readiness.

<a id="native-usage"></a>

### 原生 usage

Conductor 在 `/stats/resource` 和 `/stats/traffic` 之外发布
`GET /sandboxes/{SandboxID}/stats/usage`. Usage 读取 sandboxer 已有的原生生命周期
计量. Resource stats 描述当前生效规格和 VMM 观测; traffic stats 描述既有 Proxy
ingress 观测. 这些接口独立于 telemetry 及其
`/sandboxes/{SandboxID}/metrics` 历史查询. 原生账本格式和 runtime 查询合同由
[sandboxer usage](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/resources_zh.md#usage-cli)维护.

每次公开读取均认证 API key 并核对精确 SandboxID 的归属. StableID 只是关联标签,
不能作为查询别名. Conductor 定位当前对象, 调用与 `sandbox-ctl usage` 共用的
sandboxer [`pkg/usagereader.Read`](https://github.com/kuasar-sandbox/sandboxer/blob/main/pkg/usagereader/read.go).
它不复制原生 codec、恢复、锁或历史读取算法. Telemetry 只经 conductor 获取原生
section, 不打开沙箱 `ctl.sock` 或 `.usage` 文件.

```sh
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage"
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage?view=saved"
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage?view=history&cursor=0&limit=10"
```

| 参数 | 合同 |
|---|---|
| `view` | `current` (默认)、`saved` 或 `history` |
| `cursor` | 仅 history; 非负十进制字节位置, 默认 `0`. 原样保留返回的 `next_cursor` 字符串, 不经过浮点转换 |
| `limit` | 仅 history; 整数 `1`–`100`, 默认 `10`. 超出原生 1 MiB 页面上限时应减小 |

未知、重复、空值或格式错误的参数返回 400. 不提供 `/usage` 别名. 响应包含
`Cache-Control: no-store`. 普通认证错误保持不变; 对象不存在或 SandboxID 不属于
调用方时返回 404. 文件缺失、活动 writer 锁冲突、owner 不可用、owner 身份无效、
读取失败、超时和并发 runtime 替换返回 503. 合法 View 中的原生
`read_error`/`save_error` 仍完整保留, 不丢弃或转换为零.

`current` 返回原生 View; `saved` 返回省略 `live` 的同一 View. 例如 usage 关闭且
没有 saved record 的 owner 可以返回:

```json
{"enabled":false,"saved_end":"0","saving":false,"unknown_tail":false}
```

这不是测得零用量的记录, 也不能证明其他位置不存在历史用量. `history` 返回原生
Record 和 `next_cursor`; 验证为空的文件返回
`{"records":[],"next_cursor":"0"}`. 原生 counter、大小、时间戳、位置和 128-bit
积分保持十进制字符串表达. Coverage、completeness、status、`live`、`saved`、
`saving`、`unknown_tail`、错误和原生 `run_epoch` 元数据均保留. 不向 stats 模型
加入 orchestrator RunID.

运行中的 owner 对 live 状态、自己接纳的 saved 基线和已确认 history 具有权威.
当前对象没有 RunDir 时，Conductor 直接选择离线读取；否则仅 owner socket
不存在或拒绝连接时允许离线回退. 成功连接之后, 协议错误、EOF、
owner 错误、取消或超时均不能回退到看似可读的文件. 每个响应都证明精确 owner
SandboxID, 包括空/关闭的 View 和空 history. Reader 与 sandboxer owner 必须使用
该 ctl envelope 的兼容版本.

Paused 对象可以在不唤醒的情况下读取. 离线读取取得原生非阻塞共享锁、验证普通
文件, 然后使用 Recover 和 ReadHistory. 活动 writer 阻止调用方绕过其接纳的
saved 边界. 离线 recovery 找到完整存活记录, 不证明前一 writer 已确认该 append,
也不证明数据经历掉电后仍可靠保存.

读取不采样、不积分、不 append、不 flush、不通过写入进行 recovery、不 resize,
也不调用 guest. 它不会将尚未保存的 live 值保存, 或消除不确定尾部. 原生 memory
积分 coverage 与 CPU 来源 reset 保持原有语义. 重复读取同一累计记录得到的是同一
总量, 不是新增区间消耗. 浮点 telemetry 投影不能恢复无损原生账本, 也不能证明
不完整尾部的完整性. 不新增 usage 持久化、sync、ACK、finalizer、删除后保留或
生命周期行为.

这些值按需读取, 不进入 RouteEntry 订阅流. 公开读取、本机 batch 和进程内
extension 共用 conductor 领域实现及最终当前绑定校验, 包括 paused SandboxID
被复用时既有的插入绑定凭据和系统身份校验. Stats 和 telemetry 均不
创建 Create/Resume barrier.

<a id="node-observation-configuration"></a>

## 节点观测配置

以下字段属于 Conductor 配置；共享加载、路径与环境变量规则见[节点运行](node_zh.md#conductor-configuration)。

| 字段 | 默认 | 说明 |
|---|---|---|
| `proxy.metrics_listen` | 空(关) | conductor 进程的全局 Prometheus 文本端点;Proxy worker 数据面指标在 `proxy.yaml` 的 `metrics_listen` |
| `sandbox.usage.enabled` / `.sample_interval` / `.flush_interval` | `false` / `1s` / `5m` | image cold、`run --from`、`run --restore` 共用的原生生命周期计量策略; 严格校验, 不进入 portable artifact, 独立于 telemetry. 参见[原生 usage 策略](#usage-policy) |

<a id="usage-policy"></a>

## 原生 usage 策略

```yaml
sandbox:
  usage:
    enabled: true
    sample_interval: 1s
    flush_interval: 5m
```

默认值分别为 false、`1s` 和 `5m`. 周期必须是正的 Go duration, flush 不小于
sample, sample 的两倍不得超出原生有符号 duration 范围. 关闭时也执行校验.
显式 null、错误类型、未知/重复字段和 YAML merge key 均拒绝. Clone、配置输出、
外部 conductor bootstrap 和 Configure hook 后的最终校验保留同一策略.

Conductor 将节点策略下发到 image cold start、`run --from` 和 `run --restore`.
Usage 是宿主策略, 不进入 portable E/S artifact; 恢复使用当前节点策略. 启用
telemetry 不会启用 usage, 停止 telemetry 不会停止原生计量. `flush_interval`
调度原生 append, 不触发 fsync 或设备缓存 flush. 参见经实际解析器校验的
[conductor 部署示例](../deploy/conductor.example.yaml).

<a id="native-stats-batches"></a>

## 原生 stats batch

对应的内部 `POST /internal/plugin/telemetry/stats` 复用现有
`paths.config_socket`, 对完整 RouteEntry 流已发现的 SandboxID 显式选择原生
section:

```json
{"sandboxIDs":["exact-sid"],"sections":["usage"],"usage":{"view":"saved"}}
```

按请求顺序返回的结果包含 `sandboxID`、可选 `stableID` 和每个已选 section 的原生
body. 不返回凭据、路径或 RunID. 任一已选 section 缺失或读取失败都会使整批失败,
不会以旧样本或零补齐. Paused 对象无需 active guest 即可提供 usage; 若同批还请求
不可用的 resource section, 则该批可能失败.

能够连接 UDS 不自动获得此操作权限. 服务端验证真实 SO_PEERCRED PID、既有可选
`plugin_pidfile` 白名单, 以及固定 Plugin ID `telemetry` 当前已 ready 的有效注册.
Stats 连接必须属于已注册进程. Lease 替换或断开会取消正在执行的读取. 注册不要求
暴露 query UDS, 因而 write-only telemetry 仍能消费原生 stats. 未配置白名单时,
mode 0600 保持既有可信本机 plugin 边界; 普通 API 请求仍需要 API 认证和归属校验.
不收集租户 API key, 不引入新的权限模型或 socket.

每批包含 1–64 个不重复的非空 SandboxID, 并从 `resource`、`traffic`、`usage` 选择
1–3 个不重复 section. 仅选中 usage 时接受 usage 参数. 请求 body 上限 64 KiB,
响应上限 4 MiB, 来源读取超时 5 秒; config socket 响应另有一秒写入上限,
包括读取超时后的 503 响应. 本机 client 为两段操作保留时间; 调用方更早的截止
时间仍会取消请求. 底层原生 Reader 保留各自更小的上限. 所有 batch
调用之间最多并发读取八个对象. 父 context 取消和 lease 撤销会传递到底层读取,
并释放槽位和连接. 无效请求返回 400, 对象不存在返回 404, 生命周期状态冲突返回
409, 读取不可用返回 503, 与公开 API 共用同一领域错误体系.

<a id="logs"></a>

## Journal 输出与身份

Conductor 显式构造 journal target 与身份字段。Runtime target 语法由 [sandboxer journal 契约](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/configuration_zh.md#journal-output-targets)维护。

<a id="journal-streams"></a>

### 日志:journald 单汇 + 标签词表

Conductor 为 runtime component、app stdio 与 guest console 显式构造独立 journald targets;
普通 Sandbox 的每个 managed target 均携 `KUASAR_STABLE_ID`、`KUASAR_SANDBOX_ID`、`KUASAR_RUN_ID`,
Build phase target 携 `KUASAR_BUILD_ID`、`KUASAR_RUN_ID`。字段不从环境、unit 或制品推导,
也不在输出之间继承。完整身份规则、target 编码、查询与发送边界由
[journal 身份指南](#logs) 维护;运行时参数由
[sandboxer journal 指南](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/configuration_zh.md#journal-output-targets) 维护。

| 标签 | 写者 | 内容 | 去向 |
|---|---|---|---|
| `sandbox-ctl` | runner/Build phase 的 `--log-to` | runtime component 诊断 | 仅宿主排障 |
| `sandbox` | runner 的 `--stdout-to`/`--stderr-to` | 沙箱 app stdio | 仅宿主排障 |
| `build` | Build phase/run-builder | app stdio、里程碑、envd RUN 回放与 flatten 进度 | SDK 与宿主;完整读取/分页/失败语义见 [按目标执行与发布](node-build_zh.md#execution-and-publication) |
| `console` | runner/Build phase 的 `--console` | guest 内核 dmesg | 仅宿主;不进入 SDK Build 流 |

所有这些 native stream 直写 journal,不经临时日志文件。run-builder 里程碑与 envd RUN 回放使用
pure-Go `go-systemd/journal`。`--log-to` 不替换原始进程 stderr;早期 CLI、CH stderr、panic 与
发送失败 fallback 保留原有出口。Builder 单元设置 `LogRateLimitIntervalSec=0`,runner 保留默认限流;
这不保证 journal/storage 故障下零丢失、恰好一次持久化或零阻塞发送。

<a id="journal-ownership-and-output-targets"></a>

### 职责与输出目标

业务身份字段的含义及选择由 orchestrator 负责。sandboxer 接收相互独立的
`journald=TAG[,FIELD=VALUE...]` 输出目标，不从环境变量读取这些字段，也不从
unit 名称、目录、模板或快照推导它们。没有进程级 `--journal-field` 参数，
各输出之间也不继承字段。

每次托管沙箱运行时，Conductor 生成四个完整目标：

| 参数 | identifier | 身份字段 |
| --- | --- | --- |
| `--log-to` | `sandbox-ctl` | `KUASAR_STABLE_ID`、`KUASAR_SANDBOX_ID`、`KUASAR_RUN_ID` |
| `--stdout-to` | `sandbox` | 同样三个字段，分别显式传入 |
| `--stderr-to` | `sandbox` | 同样三个字段，分别显式传入 |
| `--console` | `console` | 同样三个字段，分别显式传入 |

字段值直接来自 `Sandbox.StableID()`、`Sandbox.ID` 与当前 `Sandbox.RunID`。
因此普通单机沙箱也有 StableID，缺省时回退到本地 ID。独立传入的 StableID 或
import 保留的 StableID 不要求沙箱属于集群。同一个 artifact 创建的新沙箱使用自身
身份，artifact 内容不是日志字段的来源。

同节点恢复保留逻辑身份，并使用新分配的 RunID。保持身份的 import/migration 可在
改变本地 SandboxID 和 RunID 时保留 StableID。StableID 是聚合键，不是本地唯一
lookup key：保持身份的复制可能让多个实例同时存在。

<a id="journal-build-output"></a>

### Build 输出

每个 Build phase 的 `run` 目标显式携带当前 `BuildSpec` 的 `KUASAR_BUILD_ID` 与
`KUASAR_RUN_ID`。组件 identifier 为 `sandbox-ctl`，应用为 `build`，console 为
`console`。Build 输出不合成沙箱 StableID，也不把 phase 沙箱 ID 当作 Build 身份。

四个原有写 journal 的 flatten/export/referrer `exec` 调用点同样接收完整 `build`
目标。artifact/文件目标和 stderr 捕获保持不变；`exec` 进程诊断仍可被现有错误尾部
捕获逻辑读取。SDK 仍只消费 `build` 流，不包含 console 和运行时组件诊断。
run-builder 自身里程碑及 Envd RUN 回放保留现有原生 Build 字段。

<a id="journal-encoding-and-queries"></a>

### 编码与查询

格式化函数按键排序，对每个不透明值使用 Go `url.PathEscape`。运行时先分割字段，
再且仅再执行一次 `url.PathUnescape`。逗号及百分号不能注入字段，`+` 保持为字面加号，
显式空值不表示从环境变量读取。例如 `worker,42%2C+east` 编码为
`worker%2C42%252C+east`。

```bash
# 此主机上同一稳定逻辑身份的全部沙箱原生日志流。
journalctl KUASAR_STABLE_ID=worker-42

# 一次本地运行，包括组件诊断。
journalctl KUASAR_SANDBOX_ID=worker-42-g1 KUASAR_RUN_ID=run-17

# 仅应用或仅组件视图。
journalctl SYSLOG_IDENTIFIER=sandbox KUASAR_STABLE_ID=worker-42
journalctl SYSLOG_IDENTIFIER=sandbox-ctl KUASAR_STABLE_ID=worker-42

# SDK 可见的 Build 日志流。
journalctl SYSLOG_IDENTIFIER=build KUASAR_BUILD_ID=build-17
```

查询只作用于正在读取的 journal；跨节点汇聚由平台运营方负责。本修改不增加
exporter 或中心日志存储。

<a id="journal-boundaries-and-verification"></a>

### 边界与验证

不改变 guest 环境、StdioSpec/MUX 协议、公共 API、metadata/header、快照格式或认证
身份。`MANIFEST_KEY` 保留现有秘密传递路径，绝不写入输出目标。
`--log-to` 只选择运行时组件诊断出口，不替换进程描述符，不接管 Cloud Hypervisor
stderr、runtime panic、node-ctl pre-exec 信息或 systemd 事件。

原生发送是同步且尽力而为的。成功后不再复制到 stderr；失败则回退到原始出口。
不承诺恰好一次持久化日志或零阻塞。

测试覆盖实际生成的 runner/Build 参数、StableID 回退及独立值、恢复/import/新对象
隔离、转义和文件/捕获行为保留。owner E2E runner 用 journal cursor 包围真实沙箱、
集群与 Build 用例，并按精确 sandbox-ctl 可执行文件筛选。每条实际观测到的原生
应用、console 或组件记录都必须具有正确、完整的身份，不能混入其他对象字段，
同次运行的 StableID 也不能互相矛盾。

确定产生输出的集群用例要求同次运行同时存在应用、console 和组件记录，并且
StableID 与本地 ID 不同。Build 用例同样要求一个 Build 执行实例具有三个日志流。
单机 Envd 用例可以静默，并单独捕获 exec 输出，因此改为要求原生组件与 console
记录证明缺省的 `StableID == SandboxID`；它实际产生的任何应用记录仍全部校验。
验证器有正反测试，覆盖静默应用、日志流或字段缺失、身份混用、原生传输，以及
错误地把不同运行实例的记录拼成完整覆盖。

源码工作区还执行完整模块单测、race 和 vet；二进制包不要求 Go 工具链。
sandboxer 配套测试覆盖 cold/restore/exec 原生输出、尾部半行、同 tag 独立字段，
以及启动失败时不主动重复写 stderr。

<a id="telemetry"></a>

## Telemetry

Telemetry 独立配置与部署；下文维护 source/identity、exporter graph 和查询服务，日志与 native usage 仍使用上面的独立契约。

<a id="telemetry-release-scope"></a>

### 源码与发布范围

本契约描述 orchestrator main，包括已合入的独立服务（#359）、namespace 契约
（#364）、原生 Collector 组装（#370）和查询/导出分离（#371）。它已实现，
不是早期系统级 OTel 提案。独立的控制面 tracing PR #219 不构成已交付 API
tracing 的证据。此处沙箱业务入口/查询仅支持指标；链接的基础设施 Collector
组件不代表提供沙箱日志或 traces 查询。

部署时固定一个 aggregate Release，核对所选 orchestrator 版本、打包指南和
该二进制的 `node-ctl telemetry --help`，用它校验配置并执行本文验证流程。
main 文档、Preview manifest 或未合入版本选择 PR 都不能证明 Stable 已包含。
不要推测旧版本支持此配置。单测和 namespace/OTLP 集成覆盖仅证明其已测场景，
不证明任意 exporter 目标或生产容量；须验证所选 exporter、查询后端、身份
隔离与保留策略在自身故障条件下的行为。

<a id="telemetry-components"></a>

### 组件与权威边界

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

<a id="telemetry-command"></a>

### `node-ctl telemetry`

```sh
node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
```

独立组件拥有自身 strict config schema 和 `paths.telemetry_executable` static bootstrap
入口。以固定 Plugin ID `telemetry` 注册，订阅 `Kind: route`；只有同时选定 Reader 和 HTTP handler
时才注册独立 query UDS. 写入与查询独立; query-only 不要求 Collector 或 local TSDB.它不加入 Create/Resume readiness，也不调用 Wake。
原生 Collector 配置选择 envd、sandboxstats、sandboxotlp receiver, 标准 processor、
exporter、connector、extension 及多 pipeline. Sandboxstats 只经 conductor config_socket
上的当前 telemetry lease 读取, 原生 stats 不依赖 telemetry. Local TSDB、Prometheus、ClickHouse reader、直接 sandbox OTLP 网络、E2B step/MAX 与
boundary 契约见 [Telemetry](#telemetry)。`deploy/node-telemetry.service` 与
conductor/Proxy 并列部署，不作为它们的 required dependency。

<a id="telemetry-storage-and-query"></a>

### 写入与查询独立装配

`collector` 是原生写入 graph. `query.backend` 独立选择 `none`、`local`、
`prometheus`、`clickhouse` 或 `custom`; `query.handler` 选择 `none`、`e2b` 或
`custom`. 只有 Reader 和 handler 都可用时才注册 query UDS. 没有 handler 时,
公开 `/metrics` 返回 503. Conductor 的三个原生 stats API 在所有模式中都保留原始来源.

省略 `query.backend` 时默认为 `none`。省略 `query.handler` 时，已选择 backend
则默认为 `e2b`，否则为 `none`；要保留 backend 但关闭查询端点，须显式设置
`handler: none`。仅启用 local storage 不会自动选择写入 pipeline 或 query backend。

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

<a id="local-tsdb"></a>

#### 本地 TSDB

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

<a id="prometheus-remote-read-and-standard-remote-write"></a>

#### Prometheus remote read 与标准 remote write

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

<a id="clickhouse-standard-schema-and-read-only-queries"></a>

#### ClickHouse 标准 schema 与只读查询

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

<a id="fan-out-and-static-extensions"></a>

#### Fan-out 与静态扩展

每种 type 注册一次 factory, 由原生 Collector 配置声明各个实例与连接.
[Fan-out 示例](../deploy/telemetry-fanout.example.yaml) 使用 batch/filter/transform/
routing 和两个带 queue/retry 的 OTLP HTTP sink. 添加 `local.enabled` 和
`sandboxlocal` destination 即可启用本地 fan-out; Reader 仍由 `query.backend`
显式选择. Queue 持久性只遵循配置组件自身的合同. Export 故障不暂停或唤醒沙箱.
参见[静态扩展](extensions_zh.md#telemetry-bootstrap-and-extension).

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

<a id="telemetry-configuration"></a>

### 配置、关闭与验证

通过 `node-ctl config telemetry --template` 或
`node-ctl config telemetry --config PATH[:PATH...] [--resolve]` 检查声明。serve 与配置
诊断共用[配置文件加载规则](node_zh.md#configuration-files)，每个文件仅一个 YAML 文档，
总输入与最终合并 YAML 均最多 4 MiB。`--resolve` 对 telemetry 没有额外作用。
最终配置中的未知字段及每个文件内的重复键会失败；默认值只对合并结果应用一次，
不在 custom hook 后重新填充。`paths.telemetry_executable` 沿用 sealed
bootstrap 选择受信静态 App，不使用 Go runtime plugin。参见
[部署 YAML](../deploy/telemetry.example.yaml)、[service unit](../deploy/node-telemetry.service)、
[custom telemetry](../examples/custom-telemetry/README_zh.md)。

关闭时先停止 route subscription、撤销 query lease，drain query server，再关闭
Collector（含其 OTLP receiver），然后 extension，最后 query backend 与可选 local TSDB。
启动失败也按相同资源 ownership 回收，即使启动失败也取消 extension 保留的后台工作。
Storage path 应持久化；不要让 conductor/Proxy 的 systemd dependency require telemetry，
telemetry 故障不得改变 sandbox lifecycle。

可重复验证（先按 [源码构建](../CONTRIBUTING_zh.md#build-and-test) 初始化选定兄弟仓的 workspace）：

```sh
go test ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api ./app/telemetry ./config ./cmd/node-ctl
go test -race ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api
go test ./internal/telemetry -run '^$' -bench BenchmarkEnvdDensity -benchtime=2x -benchmem
go test ./internal/telemetry -run '^$' -bench 'Benchmark(Local|Scrape)' -benchtime=100x -benchmem
bash test/source/telemetry_backends.sh # compiler-capable source contracts
make test vet build
ORG=/path/to/exact/sources make test-source
```

组件自有 backend case 从固定 manifest digest 创建临时 Prometheus 3.5.0 和
ClickHouse 25.8 容器, 只向 loopback 发布端口, 记录实际版本与镜像身份, 并独占一个
私有 Docker network. 退出时删除自己创建的容器、volume 和 network, 包括部分启动
失败的情况; 不依赖或修改共享的默认网桥. 两个引擎与所有具名合同均必须执行。产品用例 `telemetry.backends.sh` 通过
[统一 runner](../CONTRIBUTING_zh.md#node-validation) 消费已准备的 node-ctl 和 backend 镜像，
不发现源码、不编译、不自动拉取后备输入。每个 case 的输出保留版本、镜像/可执行文件
身份、查询结果和日志。独立源码门禁 `test/source/telemetry_backends.sh` 准备临时固定
backend，运行 Go exporter/Reader 集成测试，并编译 exact node-ctl/custom-telemetry
验证 sealed-bootstrap/query-only 扩展；缺少或跳过 source group 均失败。

产品用例运行已准备的 node-ctl, 由可信基础设施 OTLP fixture 经原生 batch/queue、
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

独立的 `telemetry.guest.sh` 用例使用自身真实 guest 生命周期，覆盖 envd → Collector
→ local DB、guest 经 connector mgmt-extract 的 OTLP、auth、paused query、telemetry
不可用、lease 撤销和 TSDB restart。

<a id="guest-otlp"></a>

### 直接面向沙箱的 OTLP

Telemetry 使用与 Proxy/MMDS 相同的 netns primitive, 自己在 `proxy_netns` 内绑定两种
listener; 空值表示当前进程 namespace.
`proxy_netns` 是唯一目标 YAML/JSON key, Go 字段为 `ProxyNetNS`.
早期 Preview 中使用 `sandbox_netns` 的配置必须迁移 key; strict decode 拒绝旧字段,
也拒绝同时提供两个字段.
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
route。参见 [connector management network](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch_zh.md#数据包流向)。
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

尽可能在 transport decode 前施加上限：默认每 listener 256 条连接，每个
`sandboxotlp` 实例的 HTTP/gRPC 共用 32 个并发请求槽位（不是进程级全局上限），
HTTP 压缩体/解压体与 gRPC message 各 4 MiB。移除保留属性后，resource/scope/point
attributes 各最多 32 个，key 128 bytes、value 256 bytes，随后添加可信 resource 身份。
Batch 最多 128 resources、16,384 points，每 scope 最多 4,096 metrics。超限返回错误，不悄悄截断。
gRPC server 的 10s deadline 在读取 message body 前就开始，停滞客户端不能无限期
占用实例的请求槽位。

<a id="e2b-metrics-history"></a>

### E2B 历史查询

`GET /sandboxes/{SandboxID}/metrics?start=<unix-seconds>&end=<unix-seconds>`
只使用已有 SandboxID。查找 miss 绝不 fallback StableID，包括 cluster 模式。
Cluster Router 现有 public SandboxID → 当前 node SandboxID 路由保持不变；节点 telemetry
只读该精确 node SandboxID，不按 StableID 合并迁移历史。StableID 仅用于外部 observability
correlation。RunID 继续用于 runner/run-plane、systemd/pidfile、MMDSv2、logging/debug；
不作为 telemetry label、resource attribute、TSDB identity、query key 或 pause/resume
series discriminator。
保留 metrics 不等于删除后仍可公开查询：Sandbox 业务记录删除后，即使 backend
仍保留其 sample，Conductor 也返回 404。Backend retention 与公开对象 lookup 是独立契约。

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

原生 resource、traffic 和 usage 读取由 conductor 负责, telemetry 停止时仍可使用. 可信 telemetry lease 经现有 config socket 消费选定 section, 不访问沙箱 ctl socket 或 usage 文件. Paused saved usage 不要求 active guest. 参见[原生 usage 与本机读取](#native-usage).

原生 traffic 使用平铺的 `state`、`maxInflight`、`inflight:{parking,connected}`、`idleSince`、`services`、`platform`、`transit` 和 `egress` 结构. Platform/transit 以沙箱视角保留 connector Mgmt/Transit 计数; `egress:{}` 表示尚无可发布统计. 它们不改变 Proxy ingress idle,也不把 management 监控包视为已接纳的应用流. Conductor 按当前交换机绑定经 connector 原生 Go API 批量读取. 参见[原生 traffic](node-proxy_zh.md#traffic-stats-and-the-unified-worker-stream).

<a id="metrics-identity"></a>

### envd 与指标身份

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

<a id="native-collector-configuration"></a>

#### 原生 Collector 配置

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

<a id="native-sandboxstats-receiver"></a>

#### 原生 sandboxstats receiver

`sandboxstats` 从同一 RouteEntry view 发现精确 SandboxID, 调用 conductor 现有
`config_socket` 的批量适配. 当前 telemetry lease 和真实 peer PID 授权该读取;
连接 UDS 不等于取得普通租户 API 权限. 它不收集租户 API key, 不持有 sandbox ctl
client、不读取 usage 文件、不维护第二套 port 绑定. Conductor 组织原生来源, 保留
ownership/binding 校验及失败规则.

内部请求为 `POST /internal/plugin/telemetry/stats`,使用固定 Plugin ID `telemetry` 的 ready、current、live 注册及实际 `SO_PEERCRED` PID,并遵守既有 `plugin_pidfile` 白名单. 注册替换或断开会取消 inflight 读取;write-only 注册无需 query UDS 也可消费原生 stats. 完整请求/响应示例、整批失败、参数与大小/并发/读取/写入时间上限由 [Node 本机 stats 合同](#native-stats-batches)维护.

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
raw counter、128-bit 积分、coverage 和详细错误仍由 [`stats/usage`](#native-usage)
提供. 启用 telemetry 不会启用 usage.

<a id="telemetry-registration"></a>

### Plugin lease 与查询通道

固定 Plugin ID 为 `telemetry`，通过已有受鉴权的本地
`PUT /internal/plugin/telemetry/register` 注册，复用 routesync version 11：

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
