[English](sandbox-lifecycle.md) | [简体中文](sandbox-lifecycle_zh.md)

<a id="overview"></a>

# Sandbox 生命周期

本篇定义 Sandbox 的创建、查询、暂停、恢复、重置、删除、导出与导入，维护公共受理语义、持久身份及数据保留。宿主服务配置见[节点运行](node_zh.md)，凭据与 guest 访问见[访问与代理](node-proxy_zh.md)，模板生产见 [Build](node-build_zh.md)。

**阅读路径：** [API](#sandbox-api) · [身份](#create-identity) · [Pause](#capture-and-pause) · [Resume](#resume) · [Reset 与保留](#reset-and-retention) · [发布与迁移](#publication-and-migration) · [请求输入](#request-configuration) · [状态所有权](#state-and-ownership)

<a id="reading-path"></a>

## 生命周期阅读顺序

调用 API 时，先读操作表与身份规则，再按需要阅读捕获/恢复和导出/导入。`running`、`starting`、`pausing`、`paused`、`dead`、`deleting` 是不同的管理状态；Create/Connect 成功受理可以早于 runtime 就绪。不保留内存的 Pause 保留磁盘制品 E，`memory=false` 的恢复从 E 冷启动。dead 的数据保留至显式 Delete 或 `sandbox.dead_ttl` 触发最终删除；`autoReset` 是下文单独定义的策略。公共契约之后给出实现所有权与启动准备细节。

<a id="public-api"></a>

## e2b API 契约

基址 `https://api.<domain>`;鉴权 **`X-API-KEY`**(SDK)或 **`Authorization: Bearer`**
(e2b CLI 构建面,两者同样解析)。api_key 由 APISecret 签发(`e2b-key-ctl
gen-apikey`),serve 经 MAC 校验解析出租户——无静态 api_keys 表([密钥与归属模型(APISecret + ManifestKey 凭据对,加密存 sqlite)](node-proxy_zh.md#credential-model))。

**归属校验**:按 id 的控制操作用 api_key 的 MAC 对该资源行的(解密)APISecret
校验,不符回 **404**(不泄露他租户存在性);create / build / import 另需对应的
APISecret+ManifestKey 凭据对在白名单,否则 **403**。

<a id="sandbox-api"></a>

## 控制面:沙箱生命周期

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| create | `POST /sandboxes` → 201 | body `{templateID, timeout, metadata, envVars, idleTimeout?, autoPauseMemory?, autoReset?, max_checkpoints?}` + 可选 `X-Kuasar-Sandbox-*` Header;`max_checkpoints` 仅接受省略或 1；`autoPauseMemory` omitted/null/true 使 TTL capture S,false 使 TTL capture E,且不改变显式 Pause 缺省;201 表示 durable starting acceptance,不等待 runner/runtime/envd;e2b 回 Envd/Traffic/Forward token,bare 只回 Forward token |
| get | `GET /sandboxes/{id}` | 附 `state`/`startedAt`/`endAt`/`metadata`、autoReset，以及可选 deadAt/executionResult/autoResetError（[终止、Reset 与数据保留](#reset-and-retention)） |
| usage stats | `GET /sandboxes/{id}/stats/usage` | 共用原生 current/saved/history Reader; 无损整数和 coverage, 在线 owner/离线锁, 不 Wake 或采样 |
| resource stats | `GET /sandboxes/{id}/stats/resource` | 只读最终资源规格和宿主 VMM counter,附可观测的节点 reservation;sparse JSON,不访问 guest |
| metrics history | `GET /sandboxes/{SandboxID}/metrics?start=...&end=...` | 精确 SandboxID ownership、opaque live telemetry UDS 转发；不可用为 503，不 Wake/Resume；[E2B 契约](observability_zh.md#e2b-metrics-history) |
| traffic stats | `GET /sandboxes/{id}/stats/traffic` | 最终 node proxy 当前 parking/connected 与保守 `idleSince`;不 Wake/Resume |
| list | `GET /v2/sandboxes` | 仅本租户;query `state`/`limit`/`nextToken`,省略 state 时列 running/pausing/paused,显式 state 可供内部故障诊断;分页头 `x-next-token`;每项含 `cpuCount`/`memoryMB`/`diskSizeMB`(`cpuCount`/`memoryMB` 的合同仍是 capacity/SKU,不改成 memory headroom)与 ISO-8601 `startedAt`/`endAt` |
| kill | `DELETE /sandboxes/{id}` → 204 | 非本租户 404；先持久接纳 deleting、撤销业务路由并发布管理 Upsert，finalizer 隔离 runner/来源使用者并清理全部父对象所属数据，最终移除父记录和管理定位；pending 重复调用幂等 |
| reset | `POST /sandboxes/{id}/reset` → 204 | E-only paused 已提交后成功，重复 E-only paused 幂等；其他非 dead 状态 409，归属不符 404，当前盘不可用 422 并保持 dead；不启动 VM（[终止、Reset 与数据保留](#reset-and-retention)） |
| resume | `POST /sandboxes/{id}/connect` | body `{timeout:秒, memory?:bool\|null}`;`memory` 在本实现中 nil=auto,true=memory,false=cold;paused 在返回前原子变为 `starting` 并持久化 `launch_mode`;目标缺失时可携 `X-Kuasar-Migration-Token` 同步 import paused 后执行同一受理;返回不等待异步 launch |
| exec session | `POST /sandboxes/{id}/exec-sessions` → 201 | 只接受 `X-API-KEY`;为 native exec 签发一个 `execAccessToken`,body 可含 `ttlSeconds` 和 CEL `conditions`,并可携 `X-Kuasar-Migration-Token`;不创建 guest process |
| pause | `POST /sandboxes/{id}/pause` → 204 或 200 | body 支持 `checkpoint`/`ifIdle` (默认 false, [Capture 与 Pause](#capture-and-pause)); `memory` omitted/null/true 保存 Snapshot S,false 保存 Sandbox E;false 与 snapshot-only merge/drop 字段组合返回 400;普通已暂停或正在 starting 回 409；缺省内存 Pause 可直接接管资源暂停，不再 capture |
| timeout | `POST /sandboxes/{id}/timeout` | body `{timeout:秒}`,重置 TTL;starting 允许窄字段更新 |

兼容范围以实际验证的上游 API/SDK 版本和测试为准。2026-09-07 核验的 E2B 官方
[Create](https://docs.e2b.dev/api-reference/sandboxes/create-sandbox)、
[Pause](https://docs.e2b.dev/api-reference/sandboxes/pause-sandbox) 与
[Connect](https://docs.e2b.dev/api-reference/sandboxes/connect-to-sandbox) 均记录相关 memory 选择，
Connect 已有 `memory=false`；该字段并非 Kuasar 独有扩展。Kuasar 的 nil→按 E/S source 自动选择、
以及所有 paused E/S 可被授权流量 Wake 是本地语义；上游对 filesystem-only snapshot 的流量触发恢复有明确限制。
本文不宣称完整实现 E2B `autoResume` policy。

create 的 `templateID` 接受三种引用:持久 id(`<profile>-<kind>-<base64url-ref>`,[模板 ID 与工件权威](node-build_zh.md#artifact-identity))、注册期
transient id、或已 ready 构建的 name/alias——后两者解析到持久 id 再走统一路径。
`envdVersion` 回 `0.6.1`(e2b)或 stub `0.1.0`(bare,≥0.1.0 否则 SDK 自毁)。bare 无
envd,不生成也不返回 Envd/Traffic token;两种 profile 都返回独立的
`forwardAccessToken`。该 token 在创建时签发并随 Sandbox 记录持久化。

Create 响应保持既有 body(不新增 `state` 字段),且与 cache 和后台 worker 使用不同的对象
副本。GET 可观察 `starting` 或内部 cleanup-pending `deleting`;默认 List 仍列 running/pausing/paused,
显式 `state=starting|deleting|dead`
用于诊断。Connect 已是 running 时保持幂等;已是 starting 时只应用显式 timeout 的窄更新,
不重复启动。paused Connect 在返回前完成 durable resume acceptance,因此成功响应可以对应
starting,但不会仍对应旧 paused 状态。

每次 standalone Connect 成功返回前,包括 running、starting 和迁入目标,都会等待当前
Proxy registration 应用先行路由更新并确认有序 barrier。它不等待 runtime 或 envd ready;
首个数据请求可以在 starting 上 parking。等待沿用既有 park timeout,并受调用方取消及
节点关闭约束。route barrier 失败返回 503,但不会撤销已经提交的 timeout 更新或 resume
acceptance;已受理的 launch 独立继续。此响应屏障不改变 cluster CmdConnect 的受理及归属协议。

paused/starting 上的显式 deadline intent 在同一
conductor 进程内的恢复失败回 paused 后继续保留,只在某次 exact-run 成功提交 running 后
消费。V1 没有持久 intent 字段,所以 Reconcile 可从仍为 starting 的中断 resume 推断并在
本次进程内保守恢复 intent,避免紧随其后的普通 Wake/Connect 被 node default 覆盖。如果该行
已经回到 paused 后 conductor 再次重启,数据库中已没有办法把它与普通 paused/default-rearm
区分;精确跨多次重启保留需要 #139 单独批准 schema discriminator,不在 #135 中用隐式编码或
sidecar 绕过。

Exec session 的完整请求、CEL 条件、凭据与失败语义见[访问与代理](node-proxy_zh.md#exec-session-api)。

<a id="create-identity"></a>

## Create 身份

Build 内部 MMDS 路由索引使用独立命名空间，因此合法的 `build-` 前缀用户 ID 仍然受支持。

直连 conductor 的 `POST /sandboxes` 可以指定节点本地 SandboxID，并可选指定独立的
StableID。这是创建时的配置输入，不是身份预约接口，也不提供幂等结果重放。

**两种等价输入**

使用现有 namespaced metadata 入口，其中值是一个 JSON **字符串**：

```json
{
  "templateID": "<canonical-template-id-or-alias>",
  "timeout": 300,
  "metadata": {
    "kuasar-sandbox.identity": "{\"id\":\"worker-42-instance-3\",\"stable_id\":\"worker-42\"}",
    "application": "worker"
  }
}
```

也可以通过配置 Header 提供相同的身份对象：

```http
X-Kuasar-Sandbox-Identity: {"id":"worker-42-instance-3","stable_id":"worker-42"}
```

不增加 Create body 顶层字段。SDK 调用方可以使用已有的 `metadata` 参数。Header
**整对象覆盖** metadata 中的身份配置，不逐字段合并。两个显式输入层都必须合法，
因此合法 Header 不能掩盖非法身份 metadata。

例如，metadata 为 `{"id":"body-instance","stable_id":"body-stable"}`，Header 为
`{"id":"header-instance"}` 时，本地 ID 和有效 StableID 都是 `header-instance`。
Header `{}` 清除两个低优先级选择，改用正常默认值。

**字段契约**

| 输入 | 节点本地 SandboxID | 有效 StableID |
|---|---|---|
| 未提供身份或 `{}` | 新生成的 UUIDv7 | 本地 ID |
| 仅 `id` | 指定的 `id` | 本地 ID |
| 仅 `stable_id` | 新生成的 UUIDv7 | 指定的 `stable_id` |
| 两个字段 | 指定的 `id` | 指定的 `stable_id` |

字段空字符串表示未指定。未指定 StableID 时，其可选持久化字段仍为空，
由 `Sandbox.StableID()` 回落为本地 ID。

本次新增 Create 配置的两个非空字段都使用现有 `ValidLocalSandboxID` 契约：
1..57 字节，只允许小写 ASCII 字母、数字和连字符，首尾必须是字母或数字。
精确模式是 `^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$`。大写、点、斜线、空白、NUL
和超长值直接拒绝，不做自动转换。迁移 token 编码层保留历史 StableID 契约；节点 Import
准入独立执行相同的 1..57 字节格式校验（[密钥与归属模型(APISecret + ManifestKey 凭据对,加密存 sqlite)](node-proxy_zh.md#credential-model)）。

对象只接受 `id` 和 `stable_id`。空 Header、非对象 JSON、null（包括字段值 null）、
重复 Header、重复或未知 JSON 字段、尾随 JSON、非法 ID 字符串和非字符串字段返回 400。

Create 响应的 `sandboxID` 仍是**本地 ID**。节点生命周期 URL、Proxy Host/Header
寻址、本地路径和路由表主键都使用该 ID。StableID 不是查询别名，也没有独立查询接口。
只指定 StableID 而不指定 `id`，不能按本地 ID 去重，也不能预知本地访问 URL。
两个 ID 不要求具有字符串上的派生关系；`-g0` 不是 conductor 对输入规定的格式。

**适用范围与所有权**

身份只针对本次 Create 解释一次，随后从传给 Hook 和存入 Sandbox 的 metadata 中移除。
之后以专用的 `Sandbox.ID`、`StableIDValue` 字段为事实源。普通应用 metadata 保留。
模板、组默认配置和 placement 默认配置不能提供继承身份；`MergeCreateMetadata`
只允许当前请求显式提供这一命名空间。

指定 ID 不授予权限，也不产生集群归属。即使 conductor 已连接 Registry，直连 Create
仍是 `OriginDirect`，且 `Cluster == nil`。现有凭据对 allowlist 与 APIKey 所有权检查不变。

Create Hook 从受保护的操作 envelope 看到最终本地 ID，不能修改它，也不能向可变
metadata 重新加入 `kuasar-sandbox.identity`。其他受支持请求字段的修改仍会重新校验。
最终 StableID 必须在生成沙箱凭据前确定。

默认 ServiceSecret 从 APISecret 和 StableID 派生，forward token 绑定该 StableID。
因此，使用相同默认凭据输入复用 StableID，不会自动使所有旧凭据失效。
StableID 是参与认证的稳定身份，不是可以随意重复的显示标签。

| 入口 | 身份配置处理 |
|---|---|
| 直连 conductor Create | 接受 metadata 和 Header |
| 单机或集群 Build 注册 | 显式身份返回 400；不保存到 Build 或模板 |
| 集群 Router 公共 Create | 拒绝两种输入；身份分配权仍属于 Registry |
| 可信 node-link Create | 使用已有 typed `SID` 和集群 StableID 字段；拒绝 `Config` 中的身份命名空间 |
| Import / Connect | 保持现有目标 ID 和迁移 token 语义；不增加身份覆盖能力 |

**冲突、重试和删除**

Create 保持 insert-only。本地 ID 已有任意状态的保留记录，或仍有活动 launch owner
时，都不允许第二次创建。请求到达身份冲突阶段后，两种冲突统一归类为 409，沿用现有
message 响应形态：

```json
{"message":"sandbox already exists"}
```

失败方不能替换既有记录、取消成功方、清理其资源或返回其凭据。请求仍可能更早因凭据、
配置错误或 Proxy 准入不可用而失败；冲突检测不改变这些检查顺序。

409 不是成功重放，也不说明既有对象与本次请求的定义相同或由本次请求创建。
响应丢失后，调用方可以使用已知本地 ID 和已有凭据查询对象并按状态处理；服务端不保存
Create 结果供重放。这是单节点冲突保护，不是跨节点的全局唯一性保证。

创建失败可能保留 `dead` 行，例如 Proxy 路由屏障在 durable starting 准入后失败。
此时 ID 仍被占用。发出删除请求不等于清理已完成；只有现有 finalizer/retention 真正
释放数据库行和活动 launch ownership 后才能复用。Create 不隐式恢复、替换或清理
既有对象。列表游标仍按本地 ID 排序，因此调用方指定的 ID 不保证按创建时间排列。

**共享实现**

直连 API 与可信 node-link 适配器保留各自的认证、模板所有权、timeout 和 MMDS 边界，
共用纯配置规范化、最终 Sandbox 构造和 `acceptFreshLaunch`。集群预检查返回独立的
解析结果，不再改写 `Command.Config`。单机模板别名规则与集群 canonical 模板限制
仍然不同；不增加公开 Hook 或 wire 协议。

共同准入仍按顺序取得 launch ownership、原子插入 Sandbox 与可选 MMDS 值、发布
starting、等待 Proxy 路由应用屏障，再调度异步启动。单机 HTTP 201 表示 durable
acceptance，不表示 guest 已就绪。node-link 在同一边界返回 accepted ACK；
`CreateCluster` 仍只是等待本次具体 attempt 的同步包装。

周边契约参见[集群 Router](cluster_zh.md#client-access)与
[扩展指南](extensions_zh.md)。

<a id="stable-identity"></a>

## 稳定身份与节点本地身份

- **StableID 与本地 ID**:`Sandbox.ID` 是当前 node-local store/runtime/route lookup key；
  `StableID()` 是跨 node-local ID 变化保持不变的 sandbox identity。standalone 普通 create
  未显式物化 `StableIDValue`，因此 helper 回退为本地 `Sandbox.ID`；standalone import
  指定不同 target ID 时只改变 `Sandbox.ID`，保留 source StableID 和全部 service
  credential。身份保持型 migration/copy 可以产生多个共享 StableID 的 node-local row，
  `stable_id` 没有 UNIQUE constraint，也不提供本地反向 lookup。cluster 中本地 ID 是
  NodeSandboxID，StableID 等于 Registry 的公开 SandboxID。同节点 resume 保持两者；跨节点
  migration/re-place 只更换 NodeSandboxID。Forward/Exec KAT 的 canonical `sid` claim 始终
  绑定 StableID；这不把 copy 定义为 independently authorized fork。
  发布位置也按 StableID 键控（[Publish、template 与 migration](#publication-and-migration)）：import 改变 target ID 后再次导出仍进入原目录，
  各个 cluster generation 也共享该目录。Import 准入使用 `types.ValidLocalSandboxID`
  校验 token 携带的 StableID，非法身份在导入时拒绝，避免延后到导出时才因 Resolve 失败。
- SQLite schema 直接使用 `stable_id`，不探测或迁移旧列，也没有双读/双写；不兼容的
  旧 preview database 必须按 schema 边界明确重建；这不是项目未曾发布 Stable Release 的声明。

<a id="create-acceptance"></a>

## Create 受理与异步启动

create 同步受理流程只做请求校验/纯解析、选定请求身份或生成默认身份、生成 token、进程内 launch ownership claim,
然后 insert `starting, run_id=""`(网络字段为空)并 cache/publish starting,在同一有序 routesync
下发 `route_barrier`,等待当前 proxy
master 成功应用先行 Upsert并 ACK,再次核验 registration lease 后才调度 launch并返回 HTTP
201.201 表示资源已被持久接受,且 proxy 已具备用该 starting identity 鉴权并
parking 的必要信息;不表示 runner 已分配、runtime 已 ready、backend 已可拨或 e2b `/init`
已完成。barrier 无 proxy、断连、apply 失败或超时时返回 503,在启动任何资源前删除该对象的
RunDir、保留 BaseDir 与已形成数据，再以 exact owner CAS 将 `starting,run_id=""` 收敛为 dead
并登记自动责任、发布不可业务路由的 dead 管理 Upsert。cold image 后台路径保持原有单阶段 fast path:先完成
resource/network/YAML,再从 runner pool 分配 run-id。artifact launch 路径先建目录和绑定
`ready.sock`,再由 pool commit callback 以 `starting AND run_id=''` CAS 绑定 exact run-id;
task 取得 assignment 后立即连接 readiness,使用父进程已锁定的 RunID pidfile 鉴权并取 bootstrap。认证通过后
ManifestKey 覆盖进程环境,task 按 E/S kind 与 LaunchMode 打开 root、选择 PreparedSource 并提交
capacity/network/ref closure 的非秘密 summary。唯一 launch worker随后 resolve resource/network→attach→以
`starting AND run_id=<exact>` CAS 持久化 ownership→写非密 YAML→返回最终 LaunchSpec。
runner 追加本地保留的 ref-location并以直接子进程启动 `sandbox-ctl run`。父进程的唯一 Wait
与 conductor 消费 runtime readiness、执行 e2b `POST /init` 及提交 running 并行；
runtime 严格完成 readiness wire→(e2b)直接 `POST /init` 置 env/默认用户→以 exact run-id CAS
为 `running`并开放数据面。只有子进程后来退出，父进程的 Wait 才返回并开始有界结果上报。

<a id="capture-and-pause"></a>

## Workspace 存储

一个 workspace 将一块具名数据盘映射到 Guest 的一个或多个目录。任务可写入这些目录，暂停沙箱 A，然后把盘交给沙箱 B。B 可以选择不同的挂载路径。每个使用该盘的实例均有独立的可写层。

通过现有的 `POST /sandboxes` 或 `POST /v3/templates` 声明 workspace。在 `X-Kuasar-Sandbox-Workspace` 请求头中传 JSON 数组，或将同一数组编码为 `metadata["kuasar-sandbox.workspace"]` 的字符串值。同时提供时以请求头为准。例如，以下单行数组为任务预留盘位和两个挂载目录：

```json
[{"name":"project","mounts":[{"path":"/data","subpath":"src"},{"path":"/work/results","subpath":"results"}],"slot":{}}]
```

`path` 是 Guest 内的绝对目录，`subpath` 是盘内相对目录，省略时挂载盘根。同一沙箱各盘的挂载路径不能重叠，同一块盘内的各个 `subpath` 也不能重叠。每个沙箱最多八块额外数据盘。公开的沙箱与模板 metadata 仅保留 workspace 名称和挂载拓扑。

| Create 输入 | 创建结果 |
|---|---|
| `"slot":{}` | 预留未接盘设备和挂载路径，容量由 attach 时的实际盘决定。`"slot":{"size":"8GiB"}` 则要求实际盘容量恰好为 8 GiB。 |
| `"disk":{}` | 使用节点配置的默认盘初始化私有数据盘。 |
| `"disk":{"migrationToken":"…"}` | 使用相同租户凭据导入其他沙箱导出的盘，为本实例新建可写层。 |
| `"disk":{…DiskV1…}` | 传入已授权的完整磁盘，`kind` 为 `disk`，`version` 为 `1`。用 `base` 表示最上层，`base_from_refs` 从近到远列出依赖层，也可以使用已授权的 `diff_template`。`size` 是以字节为单位的可选容量断言。 |
| 省略两者 | 从镜像创建时预留盘位，从 SBX 或 SNP 模板创建时继承捕获的设备和数据。 |

从 SBX 或 SNP 模板创建时，可按名字选择模板中的空盘位，例如 `[{"name":"project","disk":{}}]`。省略 `mounts` 即继承其挂载拓扑。conductor 收到绑定确认后才提交 `running`。已绑定的设备按捕获状态继承。构建声明或继承 workspace 的模板时，须在注册时显式指定 `builder.target`。例如，要生成 SBX，可将 `{"target":{"kind":"sandbox","memory":false}}` 放在 `X-Kuasar-Sandbox-Builder` 请求头中，或作为 `metadata["kuasar-sandbox.builder"]` 的字符串值传入。将 `memory` 设为 `true` 即可生成 SNP。即使存在 start 或 ready 命令，省略 `target` 也会报错。

| 操作 | API | node-ctl |
|---|---|---|
| 查看设备 | `GET /sandboxes/{id}/workspaces` | `node-ctl workspace list B --json` |
| 对运行中沙箱的空位接盘 | `POST /sandboxes/{id}/workspaces/attach?name=project`，正文为 `{"disk":{}}`、以 `disk` 包裹的完整磁盘，或 `{"disk":{"migrationToken":"…"}}` | `node-ctl workspace attach B --name project` 使用默认盘，也可用 `--disk-spec FILE` 或 `--token-file FILE` 选择来源 |
| 导出已暂停沙箱的一个已启用盘 | `POST /sandboxes/{id}/workspaces/export?name=project`，正文为 `{}`，返回 `{"token":"…"}` | `node-ctl workspace export A --name project`；加 `--json` 输出含 token 的 JSON 对象 |

命令使用沙箱所有者的 `E2B_API_KEY`。A 同步应用写入并暂停后，可通过标准输入将盘交给 B：

```bash
set -o pipefail
node-ctl workspace export "$A" --name project |
  node-ctl workspace attach "$B" --name project --token-file -
```

导出覆盖整盘，包括未映射的目录。A 保持暂停，之后可恢复或删除。暂停保存了内存时，导出取配对磁盘制品中的数据。attach 成功（204）后对应目录可访问，Guest 在首次访问时挂载文件系统。盘位未接入时访问会返回错误。

列表返回 `unattached`、`attaching` 或 `attached`，容量已知时附带 `sizeBytes`。运行中的沙箱还返回 `runGeneration`。重试可用 `--if-generation G` 或 `X-Kuasar-Run-Generation: G` 固定运行代。换代返回 412，盘源或沙箱状态冲突返回 409，暂时无法确认 owner 状态时返回 503。

## Capture 与 Pause

Create body 的 E2B 兼容字段 `autoPauseMemory` 使用三态解析:

```text
omitted/null -> true
true         -> true
false        -> false
```

解析结果作为独立 `auto_pause_memory` 业务字段持久化,只供 TTL reaper 决定 CaptureKind。它不进入
`kuasar-sandbox.checkpoint` 或 `snapshot.cfg`。standalone 和 cluster 使用同一 typed 字段链:
public Router -> route-link reserve -> Registry reserve state -> node-link command -> node CreateReq ->
durable Sandbox row,不通过 metadata 隧道传递。

Create 的 `idleTimeout` 是实例级可空整数秒策略。缺省或 `null` 关闭；`0` 在可信 idle
通知到达后立即调度；正整数从 Conductor 接收时刻起等待。负数、非整数或大于
9223372036 秒返回 400。例如：

```json
{"templateID":"<template-id>","idleTimeout":5}
```

Get/List 和扩展视图展示有效可空值。Store、集群 reserve/create、Resume、Reset 和迁移
完整保留该值，包括 0；不新增运行中修改策略 API。新 RunID 重新建立观察，从未收到
业务请求的新 running Sandbox 也会形成首次 idle 候选。

自动路径仅使用完整、正常的单 Proxy 观察。接收时记录 t0 及 BOOTTIME 锚，等待策略
时长后调用共同 Pause 路径，固定 `memory=true`、`checkpoint=false`、`ifIdle=true`、
`ifIdleSince=t0`。同一 SID 最多有一个自动 Pause 尝试等待生命周期或来源访问；新观察更新候选但不增加
等待任务，旧尝试结束后再评价当前候选。重复 idle 通知不延长 deadline；新 idle、来源变化、统计不完整和重启
使候选重建。计时期间不持有生命周期锁，仅读取不可变生命周期缓存；到期后在 SID 锁内
从 Store 重新读取权威策略和状态。最终 Worker 同步检查能够发现异步统计遗漏的
短请求。拒绝后即使没有新业务也能重新计时；持续失败使用内部有界退避。未收敛的
持久化 pausing 继续由原生命周期恢复机制处理。

下一请求沿既有 park/Wake 恢复。Worker 采样后到达的请求可等待 capture 与 Resume，
仍受原 `park_timeout` 约束；已交付业务不会重放。流式请求、WebSocket、Native Exec
直到完整 relay 关闭才释放 busy。Guest 后台工作和绕过 Proxy 的流量不属于该观察范围，
会随整机一起冻结。

`idleTimeout` 不刷新 `DeadlineUnix`、不改变 TTL 的 `autoPauseMemory`、不创建保留
checkpoint，也不设置资源恢复责任。running 实例若仍有 `ResourceObligation`，先等待
既有责任收敛，再等待一轮新的 idle 时长。TTL、资源压力和显式 Pause 保留各自语义。
启用前协调升级 Conductor、Proxy 和集群端点（routesync v11）。携非 null `idleTimeout`
的迁移 token 需要接收端支持该字段；关闭策略时保持原 token 结构。

显式 `POST /sandboxes/{id}/pause` 的 body 可为空或为:

```json
{"memory":false}
```

`memory` omitted/null/true 选择 `CaptureSnapshot`;false 选择 `CaptureSandbox`。显式 Pause 的缺省始终
是 Snapshot S,不继承 `autoPauseMemory`。因此 `Create(autoPauseMemory=false) + Pause({})` 保存 S,
而同一 Sandbox TTL 到期保存 E。`checkpoint_merge_ref` 和 `checkpoint_drop_caches` 只属于
`SnapshotPolicy`;若 `memory=false` 同时携任一字段（即使值为 `null`）,API 在 guest freeze、capture 或 lifecycle
副作用前返回 400。Pause Header 中显式提供 `merge_ref`/`drop_caches` 也受此限制。`X-Kuasar-Sandbox-Checkpoint` 与 `kuasar-sandbox.checkpoint` 仍只接受
`merge_ref`、`drop_caches`,不接受 `memory`。

`ifIdleSince` 可为 `ifIdle=true` 增加持续空闲时间阈值。格式为带时区的
RFC 3339 时间，保留亚秒精度；缺省/null 保持原行为。未指定 `ifIdle=true`、
格式非法或晚于最终 Conductor 接收时刻的时间，在安装 gate 前返回 400。
该条件只对 running 实例评价；starting/paused（包括资源暂停）返回 409，不接管。

同步 Worker 样本必须证明 parking/connected 均为零，且
`effectiveIdleSince <= ifIdleSince`。busy 或已知空闲基线较晚返回 409；
缺样本、空参与者、ACK 失败或历史时钟依据不可判定返回 503。失败撤回临时 gate，
不 capture、不持久化 pausing。扩展 hook 不能修改该条件；集群 Router 原样转发时间。

`checkpoint` 和 `ifIdle` 均默认为 false. `ifIdle=true` 要求握手参与 Proxy 的全部 worker
在安装 pausing 门禁后同步返回的 parking/connected 都为零, 否则 409. 普通 Pause 也握手并
收集相同统计, 但允许忙碌. 不持久化 Proxy 清单; 握手前已断链者不参与. 握手中断链、替换、
新参与者加入或未收到完整 ACK 时拒绝本次 Pause, 返回 503, 并发布当前权威状态撤销临时门禁.
因此 IfIdle 只认证本次在线参与者. 没有在线参与者时不等待离线 Proxy.

门禁通过可发布的 `pausing` 路由生效, 新订阅的全量同步也包含它. Proxy 在 pausing/paused
上按相同逻辑 park 已授权新请求并发送 Wake, 不再拨旧 backend. 已建立连接在握手期间保留,
避免把主动断开连接造成的零值误当空闲. conductor 持有同一 SID lifecycle lock 完成 capture、
暂停提交和清理, 再处理排队的 Wake. pausing→paused 是正常进度; starting→paused 才是恢复失败.
runner 退出与 Pause 完成是两个独立事实。当前 RunID 的退出报告立即持久化并 ACK，
包括 pausing 期间，以及 paused 清理仍持有该 RunID 时。报告不会把已接纳 Pause 的
路由改成 dead，也不否决成功 capture 后的 paused 提交。paused 行保留退出诊断；
finalizer 在 SID 锁内重读状态和 RunID，只完成剩余暂停清理。Resume 在存储和实时
视图中同时清除旧结果，有效 paused 来源仍可导出；已退休 RunID 的报告不能修改后继运行。
capture 成功但 paused 事务失败时，未提交操作保持 pausing，交由既有恢复路径处理，
不能宣称旧实例仍在 running。
崩溃后遗留的 durable pausing 没有已提交新来源, Reconcile fence 精确 runner 并按既有终止路径
收敛, 不仅凭进程存活重新发布 running.

`checkpoint=true` 保存一个长期保留的不可变 E 或 S/E 对, 与 paused/source 在同一事务提交,
返回 HTTP 200 `{"checkpointID":"<UUID-v7>"}`. 默认 Pause 仍返回 204. Sandbox 选项
`max_checkpoints` 当前固定为 1, Create 仅接受省略或 1; 没有节点级保留上限配置. 配额在
握手/capture 前检查, 已有未消费 checkpoint 时下一次指定 checkpoint 的 Pause 返回 409.
普通 Pause 不占此配额, 也不覆盖保留来源. 正在导出的 checkpoint 仍占配额.

Resume 只删除当前 ResumeSource 与用户 checkpoint 都不再依赖的版本；Resume 后新发起的不带 ID Export 仍返回 400.
保留 checkpoint 不随 Resume 删除. `POST /sandboxes/{id}/export` 可传
`{"checkpointID":"<id>","toTemplate":true}`, CLI 使用 `export-sandbox <sid> --checkpoint <id>`.
指定 ID 时选择该不可变来源, 不要求当前 runtime 为 paused; 并发 Resume 不抢占此导出.
成功后消费 checkpoint, 失败保留, `keepSource=true` 与指定 ID 互斥. 导出不会删除 sandbox.
调用方也可直接 `DELETE /sandboxes/{id}/checkpoints/{checkpointID}` (204, 不存在为 404).
消费释放配额; 当前 ResumeSource 仍需要的文件保留, 其余文件/DIFF 由 sandbox 的既有清理路径回收.

checkpoint 始终归属于 sandbox，父对象删除时一并删除，不建立独立生命周期。
终止保留当前磁盘、必要依赖、原 ResumeSource 和用户 checkpoint（[终止、Reset 与数据保留](#reset-and-retention)）。
既有 checkpoint schema 仍拒绝缺少 `max_checkpoints` 的数据库；本次 Reset 元数据采用 additive
升级，已有对象默认 autoReset=false（[状态存储(sqlite)](node_zh.md#sqlite-state)）。

`checkpoint.mode` 只接受 `local|bundle`,两种 CaptureKind 都支持两种 carrier:

```text
CaptureSnapshot:
  sandbox-ctl snapshot --json --path-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root> --checkpoint <UUID-v7> [--merge-ref=...] [--drop-caches=...]
  -> ResumeSource{kind:snapshot, ref:<actual S basename + identity>, sandboxRef:<actual E basename + identity>}

CaptureSandbox:
  sandbox-ctl export --json --path-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root> --checkpoint <UUID-v7>
  -> ResumeSource{kind:sandbox, ref:<actual E basename + identity>, sandboxRef:""}
```

capture 返回生产者的实际身份: `snapshot --json` 返回
`{snapshotRef,sandboxRef,removedRefs}`, `export --json` 返回
`{sandboxRef,removedRefs}`. capture 的差集为 `[]`. Snapshot Bundle 在同一物理文件
中携带 S 和 E 两个 selector. 本地引用保留身份限定符, 只公开 basename; checkpoint
目录属于执行上下文. 默认人类可读输出及 upload-key stdout 不变. 适配器先验证完整
结构化结果, 再原子提交 pausing→paused; Snapshot 保存实际 S/E 对, E-only capture
清除旧 Snapshot 及其关联. capture 失败绝不提交半对.

Pause 的顺序固定为 resolve request/quota -> accepted operation -> online Proxy handshake -> running→pausing -> runtime capture ->
`CommitPaused(id, exactRunID, ResumeSource, checkpointID)` -> stop/reset exact runner -> detach exact network ->
selective checkpoint cleanup -> RemoveAll RunDir -> publish paused route。commit 原子写 `state=paused` 与完整 source kind/S/E；之后非空
RunID、port 与 RunDir 共同表示 cleanup pending。Stop/Reset 成功后 exact-CAS 清 RunID，Detach 成功后
exact-CAS 清 network；任一步失败保留尚需重试的字段。checkpoint 或 RunDir 清理失败不允许新的 Resume/Wake/Exec
取得 runtime owner，当前进程的下一次 admission 与 startup Reconcile 都会重试。BaseDir 与当前 checkpoint 依赖始终保留，已知且无用的旧 checkpoint 文件会选择性清除。capture 失败仅在相同 RunID 仍持有 pausing 行且未接纳退出结果时恢复 running；
已接纳的退出沿既有终态路径清理。失败尝试保留旧 source，不创建成功 alias，也不从 S 降级为 E。

<a id="local-disk-checkpoints"></a>

### 本地磁盘 Checkpoint

每次托管业务沙箱的实际启动，包括冷/内存恢复、重启及空闲 runner 接到任务后的启动，都传入
`--enable-checkpoint`。可写 DIFF 保留在
`<BaseRoot>/sandboxes/<SandboxID>/checkpoint/diffs`，清理 RunDir 不删除它。
目录沿用既有 BaseRoot 与 PathID，不增加独立租户选项。Build 阶段不传此参数，使用单层可写盘，
见 [Build 执行](node-build_zh.md#execution-and-publication)。

每次实际命名捕获都生成新的 UUID v7，根盘和全部可写数据盘共用该 ID。
UUID 生成失败时尚不启动捕获命令；没有再次捕获的重复请求不创建新版本。
`checkpoint.mode` 仍选择 local 或 Bundle 载体，`memory`、`merge_ref`、`drop_caches`
保留各自的含义。E 在 `checkpoint.json` 中记录固定磁盘版本，本次捕获不复制普通磁盘 payload。

旧运行实例不原地转换。只有 sandbox-ctl 明确返回不支持（退出码 78）时，才允许一次
不带 `--checkpoint` 的普通捕获重试；重试使用独立参数和输出缓冲，并校验自己的完整报告。
I/O、空间不足、损坏或报告不完整都不能触发降级。新启动必须支持已启用的模式。
部署需使用包含 [sandboxer #336](https://github.com/kuasar-sandbox/sandboxer/pull/336)
的匹配源码和二进制（`7cd3ed4c5a8807b75b175bdb845867effb1c5018` 或后继版本）；
实际选定的 Sandboxer SDK 模块也必须匹配。单仓构建使用 `go.mod` 中提交的版本；
协同构建通过显式 Go workspace 选择本地模块（[构建与测试](../CONTRIBUTING_zh.md#构建与测试)）。

Task-local Open、Clone、Validate 保留 Checkpoint 布局。即使普通 active-base 字段为空，
根盘和数据盘也已有活动内容，因此预检不注入空白盘。普通直接 EROFS 来源仍需准备可写 upper。
Resume 原样传递已提交来源：目录、磁盘身份、容量、base 及最新活动版本匹配时，sandboxer
可以重开原 DIFF；历史版本、活动内容已变化或目标目录不同则需要物化。
来源制品相对其实际载体解析，与目标 RunDir/BaseDir 分开。

命名本地捕获去除了历史磁盘 payload 的复制，不代表整个 Pause 或 Resume 为常量时间。
Drain、内存捕获和业务收尾仍有成本。DIFF 重开先完成必要检查，再于首次访问查询 presence，
因此应把 guest ready、首批工作集与 Open 时间一并衡量。保留版本可能增加空间和共享写入成本；
普通活动 DIFF 导出仍需遍历索引。存储层测量不能替代平台 pause/resume 的实测结果。

平台验证使用公开 `orchestrator.checkpoint.sh` 用例，要求命名版本、同目录 DIFF 重用、
内存/冷恢复及可移植发布后的 guest 数据正确性、导出期间保留版本，以及释放后实际分配字节下降。
用例允许有界后台 GC 宽限，仅有 `List=[]` 不能判定空间已回收。Build 阶段必须使用单层盘，
业务沙箱启动必须启用 Checkpoint，最终模板捕获仍为普通产物。

可选性能对比应通过中央 E2E prepare 流程准备独立的普通复制基线和候选工作区，两者都包含
当前 `orchestrator.checkpoint.sh` 的依赖和测试 helper。基线和候选的 `node-ctl` 分别来自
对应源码，其余 sandboxer/产品、execute Image、架构和宿主资源相同。在 orchestrator
仓库中顺序运行，输出目录必须是尚不存在的新目录：

```sh
sudo python3 test/perf/run_checkpoint_platform.py --workspace /path/to/baseline-prepared \
  --mode baseline-copy --sizes 0,1 --out /path/to/baseline-observations
sudo python3 test/perf/run_checkpoint_platform.py --workspace /path/to/candidate-prepared \
  --mode checkpoint --sizes 0,1 --out /path/to/candidate-observations
```

独立测量入口调用 prepared 内的公开 `workspace.verify`、能力探测、`load_images` 和
`case_environment`，结束后再次校验输入。它不改写 prepared 产品，也不回退到宿主产品；
其结果不代表 E2E 验收通过。报告保留完整 prepared 来源、产品 SHA-256、Image 身份、
实际 argv 和捕获布局。基线必须没有 Checkpoint 启动/捕获参数和元数据，候选必须启用
新模式并使用不同的 UUID v7。两边均通过相同公开 UDS export API 发布，客户端预算为
1,200 秒；这样可避开旧 CLI 固定 30 秒的限制，不改变其服务端。

两边使用相同 Build 请求和文件系统容量（本轮最大数据量再加 2 GiB）。默认覆盖 0/1 GiB
密集数据，以及逻辑 1 GiB、实际写入 64 MiB 的稀疏文件；0 GiB 仍包含正常 guest/文件系统写入。
`--sizes 10` 显式选择更大样本，执行前检查可用内存和磁盘空间。串行测试把 controller 的
startup factor 显式设为 0.8，以容纳开发机上的 6 GiB Build；不改变生产默认值或物理内存探测。
启动元数据和加密可能导致模板身份不同，报告分别记录。

Build 准备独立计时，不混入 Create、Pause、热/冷恢复、发布及发布模板 Create 的耗时。
每次恢复校验数据，包括删除来源后恢复。完整 SHA-256 校验分别记录缓存准备、哈希和
宿主总耗时，预算按数据量设置并有上限。哈希校验后独立计时 sync/cache-drop，使捕获前的
文件缓存条件一致；正常 Pause 策略不变。ready 之后另测数据文件首次页和 256 页工作集的
直接读取。再次冷 Pause 前不显式写入业务文件，但 guest 后台工作仍可能写盘。这些端到端
计时没有单独隔离内部 Drain、版本登记或 DIFF Open。

先完整初始化 128 MiB，再比较真实 publisher 保留固定版本与发布完成、版本释放并观察到
物理回收后的同一 guest：顺序/随机 4 KiB 写、2 MiB 热集写、512 字节部分页写和混合读写，
使用 `O_DIRECT|O_DSYNC`、QD1。保留和释放阶段分别使用不同的 64 MiB 半区；每轮交换保留
半区的位置。每个半区内五种负载各用互不重叠的 12 MiB 区域。因此释放阶段首次改写此前未改写
的 payload 页，避免重复覆盖保留阶段已经私有化的页。结果记录实际偏移；该 `fresh-v3` 协议
不能与此前重复写相同地址的样本混算。

不支持直接 I/O 时明确失败。默认每种 1,024 次操作，保留原始系统调用延迟、读/写
p50/p95/p99 和吞吐，吞吐包含确定性缓冲生成成本。两阶段使用相同相对地址种子、不同内容
以确保真实改写。保留阶段总是先执行，缓存、索引、分配器、顺序及后台 GC 影响仍在，
不能把对照当作单独隔离了 Delete 的因果效应。`--rounds 3` 重复采样，
`--skip-lifecycle` 或 `--skip-workloads` 可单独运行一组。

`checkpoint-platform.jsonl` 分别记录 BaseDir/RunDir 与 DIFF 分配空间。负载期间每 250 ms
采样 runtime unit 的 RSS/cgroup 内存及 `/proc/PID/io`、cgroup `io.stat`，保留计数重置和
前后增量。采样最大值不是进程峰值；共享缓存、Store 输出和 node daemon 不在此口径内。
短负载 I/O 增量不等于物理介质写放大比率。比较结果应同时说明源码版本、文件系统、缓存条件、
原始样本及上述限制。

<a id="managed-checkpoint-history-and-selective-cleanup"></a>

### 托管 checkpoint 历史合并与选择性清理

`merge_ref=false` 独立记录本轮驻留工作集。已有 `S1 -> S0` 时，下一次捕获先流式生成新的
不可变历史 Snapshot `S1′ = S1(memory) 覆盖 S0(memory)`，再写出
`S2(当前工作集) -> S1′`。后续本地捕获重复该组合，最新 S 下最多保留一个本沙箱拥有的本地
内存 lower。`merge_ref=true` 将当前驻留内存与整个可合并本地前缀合并；false → true → false
切换同样收敛。reader 仍支持旧的多层输入。恢复执行状态和 `sandbox_ref` 只来自最新 S，
历史 S 只提供 memory。

前缀必须属于当前沙箱的规范 `BaseDir/checkpoint`。先按完整来源绑定和 Bundle 实际成员选择
物理载体，再比较路径。named location 或外部模板是边界，即使文件可在本机读取、甚至映射
到同一路径也不能越界合并。该边界及其后的 lower refs 保持原顺序。local tarstream、Bundle
和混合载体使用相同规则。普通复制式可写磁盘捕获吸收同设备的连续本地前缀，不受内存开关影响；
不可变 EROFS base 与 ext4 upper 保持分离。Data 和不透明 Zero 覆盖下层，Hole 向下穿透。
历史读取走宿主制品 stream，不读 guest memfd，因此不会扩大本轮工作集。

历史组合在 guest freeze 前准备并直接流入最终 sink，复用 `fetch.NewLayered` 和 sparse run，
不缓存整镜像，也不生成多余整镜像中间副本。local 输出对 checkpoint 已拥有的复用依赖保留物理
selector；Bundle 输出继续使用既有成员复制发布路径。历史合并生成新内容身份；sink commit/close 和数据库提交之前，
旧来源始终有效。捕获或数据库提交失败都不能授权删除旧文件。

托管 Pause 提交精确 S/E 对（或 E-only 根）后，生命周期 owner 先 fence 旧 runner 及全部
读写使用者，再执行选择性清理。通用 FileSink、任意 `--output`、独立 `snapshot --resume`
和共享 build 阶段输入都不因此取得清理权。sandboxer 制品库解释保留集合；conductor 只通过
短生命周期 `node-ctl checkpoint-cleanup` 工具提供目录归属、路径、已提交的来源对及既有
lifecycle fence。portable Export 继续只使用已存储的来源对，不读取制品。token、公开结果、
base 格式和持久化 cleanup schema 均不改变。

保留集合包含当前来源与显式保留 checkpoint 的全部 S/E、各自历史内存载体、磁盘/upper/不可变 base 载体及复用文件. 固定别名只由当前来源决定.历史 S
的旧 E 不是磁盘依赖。Bundle 任一成员仍被使用就保留整个物理载体。比较 basename 前先解析
source/location 绑定。选择只读取有界的当前小型元数据与 Bundle 索引，不读取旧候选 payload，
不计算整镜像摘要；此操作无需 Snapshot 的 CPU/state body。kernel/runtime 的
basename identity 绑定宿主提供的启动文件，不是 checkpoint payload 依赖。keep plan 或 reader Close 出错时
不删除任何文件。

只处理已验证专属 checkpoint 的直接目录项：64 位小写十六进制 digest/key 加 `.snapshot`、
`.sandbox`、`.overlay`、`.image`、`.bundle` 的成品必须是普通文件；捕获 partial 必须完整匹配
`<producer-SandboxID>.<kind>.<uint32十进制>.partial`（包括 bundle，除 `0` 外不得有前导零）；
固定 `<sid>.snapshot`/`<sid>.sandbox` 别名和 `.<sid>.<role>.<32位小写hex>.tmp` 必须是符合生产端
basename target 约定的符号链接。SandboxID 不等于 PathID 或 StableID。未知名字、其他 SID、
前缀碰撞、格式近似但非法的名字、目录和异常链接一律保留。未完成 partial 无需内容校验。
固定别名只有指向对应当前 S/E root 的载体时才保留；即使 E 成员仍使用同一 Bundle，
E-only 也会删除旧 S 别名。Snapshot 捕获只提交 S 别名，E 身份取自持久 S/E 对，不要求
存在 E 别名。清理只 unlink 可识别别名本身，不跟随链接，不递归删除目录；dirfd 操作保证路径发生竞态时
unlink 仍被限制在原目录内。

托管 checkpoint 清理要求 `paths.base_root` 使用实际规范路径，checkpoint 路径及其祖先
不得含符号链接。应直接配置解析后的目录，而非符号链接别名。这是既有清理前置条件；
Pause 成功不代表节点启动时已经完成该路径检查。外部发布制品的保留与删除仍由使用方负责，
与本沙箱 checkpoint 清理分别处理。

新来源已提交后，即使清理失败，Pause 仍成功。最后一个持久 RunDir ownership 标记保留到
checkpoint 清理和既有 paused 收尾均成功。既有 worker 在 SID lifecycle lock 下重新读取当前
来源重试，重启后也如此。active 或 detached export 的读取 fence 在其完成前持续有效；清理
不会持锁等待需要同一锁完成的 export。Resume/Wake/Exec 遵守既有 pending-cleanup admission
合同。候选已不存在视为完成；权限、I/O 和身份错误继续保留重试责任。普通 Kill 和 BuildBaseDir
终态删除仍由原 finalizer 负责；此步骤不回收任何共享或外部制品。

<a id="releasing-disk-versions-after-resume"></a>

### Resume 后释放磁盘版本

运行期磁盘版本释放与上述 paused 制品文件清理分别执行。成功提交 running、实际导出完成及
启动接管触发共同释放检查。检查重新读取当前状态、精确 runtime 与路径归属；只有 running、
没有终态执行结果、且没有仍在使用 checkpoint 的来源操作时才可释放。
starting、恢复失败回退 paused、尚未恢复的 `keepSource` 都保留版本。

检查使用 sandboxer 完整 List 结果与共享 SDK CheckpointVersions 图校验，排除
完整 ResumeSource 版本集合及长期 checkpointID 后，删除其余精确 `{DiffID, ID}` 引用。
部分或结果不确定的 Delete 重试保持同一集合，不把旧选择交给另一个运行实例。
若 List 明确返回 `checkpoint_unsupported`，本次检查正常结束，不发出 Delete 或重试。
这不会按 SID 缓存能力；后续或并发的独立触发仍会重新检查。其他 List 错误、不完整列表和
全部 Delete 错误仍会记录并通过有界调用和退避重试，不撤销已成功的启动或发布。
这一步不执行 `checkpoint-cleanup`、不删除 DIFF 目录，也不删除内存依赖。
Delete 使旧版本可回收，实际空间由 sandboxer 后续 GC 归还，而非同步回收。
running row 的 `ResumeSource` 及其所需全部版本继续保留，供恢复失败回退和终止保留使用；
图校验出错时不释放版本。

<a id="resume"></a>

## Resume admission、Connect 与 Wake

Connect body 是严格、限长 JSON object，接受 timeout 与可选 `memory?: boolean|null`。
2026-09-07 核验的上游 Connect 也有该字段；下列 source-dependent 规则是本地精确合同，
不等同于完整上游 autoResume 策略。
API 映射为 omitted/null -> `ResumeAuto`,true -> `ResumeMemory`,false -> `ResumeCold`,随后按 durable
source 解析:

| paused source | request | LaunchMode | sandbox-ctl |
|---|---|---|---|
| Snapshot S | omitted/null/auto | `memory` | `run --restore <S>` |
| Snapshot S | true/memory | `memory` | `run --restore <S>` |
| Snapshot S | false/cold | `cold` | 解析 `S.snapshot.cfg.sandbox_ref`,再 `run --from <E>` |
| Sandbox E | omitted/null/auto | `cold` | `run --from <E>` |
| Sandbox E | false/cold | `cold` | `run --from <E>` |
| Sandbox E | true/memory | conflict | 409 `memory unavailable` |

running Connect 保持幂等,显式 false 不会重启。starting resume 上,omitted/null 加入当前 attempt;
显式值与 durable `launch_mode` 一致时加入,冲突时返回 409,不会修改已经接受的模式。cluster
Connect 在 Router、route-link、Registry 和 node-link 间保留 `*bool` 的存在性。

`BeginResume(id, deadline, LaunchMode, RunDir, EnvdUDS, CiUDS)` 只接受 runner/network/RunDir 已完成
cleanup 的 paused row，并原子写 `state=starting, launch_mode` 与新一代 canonical RunDir/UDS。
`CommitStartingRunning`、`RollbackStartingPaused`、
`RollbackStartingDead` 清空 `launch_mode`。对本地已有 S，失败的 cold 尝试恢复原 paused S，
不会改写成 E，所以之后仍可选择 memory。conductor 重启遇到 starting resume 时从 durable
source + `launch_mode` 重建 attempt；例如 S+cold 不会因进程内缓存丢失而错误执行 memory restore。

从 E 冷启动使用目标节点配置的 kernel 和 runtime，保留来源工作负载/磁盘图，并派生记录本次
boot 身份的新 C0。对已有 S 选择 cold 时使用其关联 E，无需改写 S 或先导出 E-only。内存恢复
保留原 C0，要求 runtime 的 digest 类型和值匹配，不依赖外部 kernel 文件。

Connect 若需导入缺失的目标，同一个 memory 选项也决定导入后的持久来源：false 先保存 E-only，
再冷启动。后续启动失败仍为 paused E。之后从 paused 恢复时，auto 继续冷启动，true 返回
409，直到捕获新的 S。这与本地已有 paused S 的一次 cold 尝试不同。Import 不改写原 token，也不删除其 S/E 制品；
见 [Import 选择](#import-selection)。

Proxy 的 ordinary ingress 和 native exec activation 都经 `OnWake` 使用
`ResumeTriggerWake`;`ExecSession` 使用 `ResumeTriggerExecSession`.公开的
`ResumeTriggerRoute` / `ResumeTriggerExec` 常量继续保留扩展契约,但 conductor 不再承载
对应的数据面 adapter.这些入口全部只传 `ResumeAuto`:S 默认 memory,E 默认 cold.两种 source
都保留既有 Wake 能力;
proxy SHM 不携 source kind 或 cold gate。已鉴权的首个 E 请求保持 parked,直到 node
完成 cold launch、route 变为 running 并成功拨通 backend 后才继续转发。未来的 `autoResume=false`
若实现,必须是独立 traffic policy,不能从 E/S kind 推导。本变更不宣称完整实现 E2B autoResume。

<a id="reset-and-retention"></a>

## 终止、Reset 与数据保留

Create 接受布尔 `autoReset`，默认 false，作为父对象不可变设置沿后续运行保留；升级前对象也
默认 false。Get/List 返回 autoReset、可选 deadAt/executionResult/autoResetError。deadAt 是本轮
首次持久终态时间，executionResult 保留原已知执行结果；未知退出保持缺省，不合成退出码 0。
autoResetError 单独记录自动准备失败，endAt 仍表示运行期限。

正常/异常结束、首次 Create 失败及确认旧运行消失时，隔离精确 runner，释放网络、运行预留和
不再需要的 RunDir；保留父对象、当前磁盘、必要依赖、原 ResumeSource 与用户 checkpoint，再
提交 dead。终态收尾不以生成 E 为前提。正常 Pause 的退出归暂停收尾；Resume 失败回原 paused。
已接纳 Delete/Kill 继续删除且不触发 autoReset；迟到旧 Run 事件不能清理或覆盖新运行。

guest 使用磁盘前，共享 sandboxer 启动路径在 BaseDir/checkpoint 持久保存 portable C0、实际
完整磁盘图与来源绑定、根盘/具名盘和 active DIFF 身份及依赖，关联父实例和 RunID。每轮运行
均刷新关联，包括复用 DIFF。SDK 保留物化的本地 lower；未选择保留的独立调用方继续临时清理。

`POST /sandboxes/{id}/reset` 沿用鉴权和归属检查。仅 E-only paused 提交后返回 204，重复请求
幂等成功。带内存 paused、starting、running、pausing、deleting 返回 409，外部/不存在对象
返回 404。当前盘缺失或未就绪返回 422 并保持 dead；不补空盘、不回退模板/旧 checkpoint/上一轮
描述，也不等待已退出 runtime 初始化磁盘。请求取消只停止等待，不撤销已接纳的 Reset；
重试加入同一持久 operation ID，直到成功或明确失败已提交。

每轮自动机会的 Reset operation ID 与 dead 在同一事务持久提交。worker 通过共享 SDK 薄封装
`sandbox-ctl reset --checkpoint-dir ... --sandbox-id ... --instance-id ... --run-id ...
--operation-id ... --json` 取得独占来源归属，校验完整盘集身份、容量、就绪状态和依赖，以同一
操作身份固定多盘版本、核验并续作部分产物、生成标准 Sandbox E。精确父实例/终止实例/操作
提交才发布 E-only ResumeSource 与 paused。全部输出归父对象；不消费用户 checkpoint，不占
长期 checkpoint 配额。配置的 sandbox-ctl 必须来自声明的 sandboxer 依赖版本。

手动请求加入 pending 自动操作；明确失败持久 failed，保留原 deadAt、结果、数据；自动操作
另记 autoResetError。重复事件、扫描、重启不自动重试；后续手动请求可登记新操作。结果未知的中断
续作原操作，包括多盘部分完成及 E 已生成；已提交 paused 按成功恢复。成功清空 deadAt、当前
结果和自动错误；新运行再次终止才产生下一次自动机会。Reset 不启动 VM；Connect/Wake 复用
同一 SandboxID、新 RunID 冷启。E-only 显式内存恢复失败，冷启失败仍保留 E-only paused。

SQLite additive 升级增加不可变 instance_id、默认 false 的 auto_reset、dead_run_id/dead_token、
reset_id/state/automatic/error 和 auto_reset_error。旧 dead 补身份 token，不追补自动操作；
缺当前数据时明确失败，但仍能显式删除或到期回收。running 中 dead_run_id 可先记录已确认
结束事实，再完成收尾，不虚构 executionResult。事务/CAS 与继承的 source lock 在 conductor
进程重启期间共同隔离 Reset、checkpoint Export、Delete 和 TTL。

继续遵循既有 guest FLUSH/capture 持久性合同，不增加跨节点磁盘恢复、自动重启 VM 循环、
paused TTL、运行历史归档或远端 artifact 所有权。

<a id="publication-and-migration"></a>

## Publish、template 与 migration

本机 E/S 都通过同一命令发布:

```text
sandbox-ctl publish --json --quiet --manifest-config <cfg> [--to-ref-location <name>=<uri>] <artifact>
```

发布适配器读取并验证 JSON 报告，保持 `ResumeSource` 的 kind 不变。没有 named location 时发布到 Manifest Store;
配置 `checkpoint.remote.ref_location_parent` 时,publication name 为实体 id——sandbox 发布用
StableID、build 发布用 BuildID(见 `reflocation.PublicationName`),URI 为
`<parent>/<sha256(name)[0:2]>/<sha256(name)[2:4]>/<name>`。local tarstream 可得到 located
`.sandbox`/`.snapshot`:plaintext carrier 使用 `@digest:<digest>`,encrypted carrier 使用
`@hmac:<digest>`。Bundle 得到使用 `@manifest:<root-key>` 选择 root Manifest 的 located `.bundle`。
即使发布到 named location 也始终传 `--manifest-config`,因为 Bundle exact publication 仍须验证并
发布其 Manifest graph。Bundle->Store 验证 recorded admission、physical digest 和 salt domain,
根 Manifest 最后提交。

name 本身即目录键:同一逻辑 sandbox 的全部 publication——重试、跨进程重启、import 换 target id 后的
再导出、cluster 各 generation——收敛到同一个 StableID 目录;同一 Build 的 image/checkpoint 等
named publication 收敛到同一个 BuildID 目录。目录内内容寻址的 `<digest>.<role>` 文件累积成多个
版本，相同内容由 publisher 去重复用。SHA-256 两级扇出限制单目录条目。使用方跟踪 portable ref、
TemplateID 和 migration token，并负责发布制品的保留与删除策略。仅凭发布时间不能判断制品
是否已不再被引用。本项目提供引用及其变化信息，外部制品的保留与删除不属于节点生命周期。
conductor 与 task reader 共用 `internal/reflocation` 的 deterministic 解析，location name 自足，
恢复不依赖额外 side table。

需属主鉴权的 `POST /sandboxes/{id}/export` 请求仍为
`{"toTemplate":false,"keepSource":true}`。成功响应保留既有 `result`，恰好增加对应的
发布根引用与 `removedRefs`。Sandbox 响应有三个字段：

```json
{"result":"kmt1.…","sandboxRef":"manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","removedRefs":[]}
```

Snapshot 响应有四个字段：

```json
{"result":"kmt1.…","snapshotRef":"manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","sandboxRef":"manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","removedRefs":["file://old.snapshot"]}
```

`toTemplate:true` 仅把 `result` 的含义改为对应 `sbx`/`snp` TemplateID。Snapshot 始终
返回最终 S 实际引用的 E，包括选中的 located Bundle 成员。发布适配器严格解析单个
`sandbox-ctl publish --json` 文档，并在源 finalization **之前**检查期望 S/E 角色、
portable 最终根、必需的最终 E、有序唯一差集和安全文件 basename，不把诊断 stderr 当作
成功结果。Builder checkpoint publication 共用同一契约。Core 方法统一返回
`types.ExportResult`，HTTP 的 `result` 字段继续兼容既有客户端。采用新内部 CLI 契约时，
sandboxer 与 orchestrator 应部署来自相匹配来源集的版本。

本地发布的报告和 token 同时使用 S1/E1; keep-source 的 SQLite 与 cache 继续保存
S0/E0. capture 及其生产者结果由 [Capture 与 Pause](#capture-and-pause) 维护.

**配对 source schema 升级:** 先停止旧 conductor，排空或退役其管理的 sandbox，并预先保留需要的
工件和记录。启动本版本前，操作员必须明确清空旧本地 sandbox 数据库；此配对 schema
不提供 ALTER、迁移、回填或双读兼容。进程拒绝旧 schema，绝不为升级自动删除用户数据。
部署匹配版本的 sandboxer/orchestrator 后重新创建本地记录。缺少必填 `resumeSandboxRef`
字段的旧迁移 token（包括缺少 E 的 Snapshot token）无效；废弃过期 token，从具有完整
对的记录重新导出。不得用旧 token 或读取
S 元数据重建持久对。

已 portable 的源不再发布，返回 `removedRefs:[]`；portable Snapshot 直接返回 SQLite
中保存的精确 S/E 对。即使两份工件都离线，Export 也不打开工件、不配置存储、不检查
元数据、不启动元数据子进程。缺少 E 的 paused 对无效，绝不通过读取 S 修复。普通、keep-source、move 和已受理的 detached-template 成功分支
均携带同一套一致根引用与差集。Resume/preemption、属主检查、生命周期取消、幂等及并发
继续遵守下文原有顺序。

差集表示已知旧拓扑减去最终保留引用，不是字段改写事件或删除命令；包含确实退出的本地根
和内部引用，最终拓扑中其他位置仍使用的共享引用会排除。skip 模式中被替换旧引用即使
可读也按叶子处理，显式原生 lower 列表仍属于已知引用。JSON 报告不增加 payload 扫描、
摘要或 staging 文件。所有公开的无 location 文件引用都只有 basename，并保留既有
`@digest`、`@hmac` 或 `@manifest` 身份；已具名 location 保留合法表示。先按完整来源
上下文比较，再投影。调用方在外部提供 checkpoint 目录；响应不含路径、上下文、标识或
字段映射对象。Bundle selector 退出不代表应删除物理 carrier。即使报告差集，
`keepSource` 仍保留原源与 checkpoint。报告本身不触发工件删除或 GC；move 按下文执行源沙箱拥有的本地资源清理。

`TemplateID` 为 `<profile>-<kind>-<base64url(canonical-portable-ref)>`:

```text
img: manifest ref,或 located .image/.bundle
sbx: manifest ref,或 located .sandbox/.bundle
snp: manifest ref,或 located .snapshot/.bundle
```

paused E 转模板得到 `KindSbx`;paused S 得到 `KindSnp`。两者均可 publish,不会因 E 无内存而拒绝。
build pipeline 的 Snapshot 输出仍是 `snp`,但 publication 同样调用 `sandbox-ctl publish`,不依赖
单独的 Snapshot publication 命令。

启用 `autoReset` 的父对象会在经过认证的 KMT V1 payload 中携带该字段，Import 恢复此设置。已有 token 未携带该字段，导入时取默认值 false。旧版严格解析器会拒绝携带新字段的 token。

KMT V1 payload 直接携 `resumeSourceKind`、`resumeSourceRef`、`resumeSandboxRef` 和
`autoPauseMemory`。Snapshot 必须在 `resumeSandboxRef` 中携带关联 E；E-only 的该字段必须
为空。缺失字段、重复或未知 JSON 字段以及错误组合均被拒绝，包括缺少 E 的旧 Snapshot
token。Import 先认证原 token，并针对原 S/E 对校验目标 Expectations，再选择新记录的持久来源。
目标 node 从本地 trusted key table 取得租户根密钥，校验 fingerprints 和 profile，原子插入
选定来源、deadline、portable env/metadata 和既有 ServiceSecret/Envd/Traffic/Forward 凭据。
token 不携 raw tenant roots、host path、MMDS secret value、cluster Group/RouteKey 或 generation。

本地磁盘 Checkpoint 的发布由 sandboxer 将固定版本物化为普通可移植磁盘产物，并从发布 E
中移除 `checkpoint.json`，原本地来源不变。移往其他节点或回退到不支持 Checkpoint E 的
reader 前，先完成 publish；只复制元数据不会转移本地 DIFF 内容。

导出准入先完成待办 paused 清理，再登记 attempt 或启动发布。checkpoint 目录之外的私有
来源操作锁由捕获、清理、导出、磁盘版本释放和终态资源删除共同使用。
publisher、capture CLI 和 `checkpoint-cleanup` 继承其描述符，因此 conductor 取消或退出
不会在实际子进程结束前解除保护。锁文件随 BaseDir 保留到终态目录删除，不能单独 unlink
或替换。发生争用时，在 SID lifecycle lock 之外等待重试，重新读取当前状态和路径。
已清理干净的来源可在发布期间 resume，但版本释放和终态删除等待来源使用真正结束；
发出取消不代表已经完成。正常执行结束、确认运行实例失联及显式删除共同使用受来源保护的
资源收尾，保留的 dead 历史与执行结果继续保持原有业务含义。

Export 的 publish 阶段不持有长 lifecycle lock。结果校验通过后，在 per-SID lifecycle lock
内检查完整原始 S/E 对、源实例和导出 attempt。Resume 先成功 `BeginResume` 时，KMT
export 被取消并返回 409；template publish 可 detached 完成并返回 TemplateID，但不清理
source。Export 先赢得收尾权时，`keepSource=true` 在准入已完成待办 paused 清理并持久清空
相应归属后，保留原 ResumeSource 及必要 checkpoint 产物；发布收尾不改写保留的来源。
之后再对保留的源执行 move，仍使用同一删除流程。

`keepSource=false` 校验本地路径所有权后，调用正常的锁内删除接纳入口。成功表示
有效 token/template 结果和持久 `deleting` 行，物理清理可能尚未完成。Export 返回前
撤销 source runtime cache，并发布不可业务路由的 deleting 管理 Upsert；全量快照保留它直到硬删除。已有 finalizer
依次 fence runner、释放 network、删除 RunDir 和 BaseDir（包括全部所属 checkpoint 文件），
最后 hard-delete 行并通知 terminal observer。local/portable 的 S/E 源全部采用此流程，
portable 源遗留的本地 checkpoint 也会清理。发布目录与共享输出不属于源清理所有权；
finalizer 不删除 located/remote artifact。

删除接纳无法持久化时返回错误，paused source、S/E 对和 checkpoint 完整保留。接纳后
目录或 hard-delete 失败仍保留 `deleting` 所有权与有效导出结果，绝不恢复为 paused。
worker 独立于 HTTP 请求持续重试；服务关闭时未完成所有权留在同一行，由启动
reconciliation 接续。请求或 lifecycle 取消可在 Export 赢得收尾权前终止操作；获胜后
通过有时限的 `context.WithoutCancel` 临界区完成接纳。同 ID Create/import 在源行最终
hard-delete 前继续返回既有的 409 冲突；客户端可在清理完成后重试，或选择不同 node-local ID。

standalone import 省略 target 时复用 source NodeSandboxID;显式 target 只替换 node-local ID,保留
StableID 和 service credentials。insert 是原子的 insert-only,冲突返回 409。Connect 可携
`X-Kuasar-Migration-Token` 在目标缺失时同步 import 后接受同一 Resume;目标已存在时不解析 token。
paused route 只投影与 kind 正交的 `artifact_location=local|remote`,供可信迁移组件判断节点绑定;
proxy 不读取 kind。Migration token 是 sandbox 级敏感凭据,不会随 route 广播。

<a id="import-selection"></a>

### Import 选择与 runtime 兼容性

`POST /sandboxes/import` 在 `token` 和可选 `sandboxID` 之外，接受可选的
`memory: boolean|null`。Import 创建 paused 状态，不启动 VM：

| 已认证的 token 来源 | Import memory | 新记录的持久来源 | 后续恢复 |
|---|---|---|---|
| S + 关联 E | omitted/null 或 true | runtime 准入通过后保留原 S + E | auto/true 恢复内存；false 冷启动 |
| S + 关联 E | false | E-only，使用 token 的关联 E | auto/false 冷启动；true 返回 409 |
| E-only | omitted/null 或 false | 原 E-only | auto/false 冷启动 |
| E-only | true | 不插入记录；409 memory unavailable | 不接纳恢复 |

选择 E-only 只改变新记录的 ResumeSource，不新建替代快照，不修改 token，也不删除或改写
原 S/E。认证和 Expectations 仍针对原完整对。E-only Import 不打开制品或检查目标启动文件；
导入成功不保证 E 可读或能够冷启动，这些检查留给后续 launch。

平台只在两个入口新增 runtime 准入：从 snp 模板首次 Create，在既有异步租户准备阶段检查；
Import 最终选择 S，在插入前同步检查。Import 通过短生命周期的租户 helper 读取 S 与关联 E
元数据，检查该来源实际要求的 runtime 身份，不恢复内存、不扫描磁盘图，也不分配沙箱 runner。
Import 遇到确定的 runtime 不匹配返回 409；元数据、访问、I/O 和取消错误仍作为检查错误返回。已有本地
Connect/Resume/Wake 不新增平台预检查；真正恢复内存时，sandboxer 仍校验 runtime。

选择 S 的 Import 对元数据/helper 检查使用固定四秒的内部准入 context，若 caller deadline
更早则服从该时限，并将 context 传给插入。helper 正常退出后的输出读取仍使用该 context；
取消时关闭父进程的管道端点，进程取消收尾另有 250 ms 上限。已有同沙箱 lifecycle fence 不支持
取消，等待该锁可能超过准入预算；取得锁后，Import 再次检查 context，拒绝过期插入。
元数据检查失败或超时不插入记录。因此该预算不是整个 Import 请求的硬截止时间，也不是用户
配置项或沙箱 TTL；首次 snp Create 沿用既有 task deadline。这些检查不新增按组件兼容性调度
节点的机制。

Token 仍为 KMT V1。`RuntimeDigest` 保留签发时节点默认 runtime 整文件 SHA-256 的含义，
但 Import 不再将其与目标默认文件比较。因此 Export 签发 migration token 时仍依赖该默认
runtime 文件，并保留整文件 hash 成本。旧接收端能解析 V1，但仍执行旧兼容性规则。使用 Import memory
选项前，应升级接收端 Conductor 与配套工具；旧 HTTP handler 可能静默忽略未知的
`memory:false` 字段。设计依据见 [升级兼容性提案](https://github.com/kuasar-sandbox/orchestrator/issues/519)。

`running` 仅表示 runtime readiness 与 mandatory e2b `/init` 已完成,不保证用户应用端口健康。
通用 backend health/dial retry 仍由独立工作跟踪。cluster 的 create/connect 复用上述 node-local
原语;secrets-only CONNECT import 仍只属于 standalone,node-link 不传 MMDS secret plaintext。

<a id="export-import-command"></a>

## `node-ctl export-sandbox` / `import-sandbox`

serve daemon **api 平面**的客户端(经本机控制 socket 调 `POST /sandboxes/{id}/export`
与 `POST /sandboxes/import`),鉴权 `E2B_API_KEY` env(须属主)。语义见 [Artifact lifecycle、转模板与迁移](#capture-and-pause)。

```
node-ctl export-sandbox <sid> [--checkpoint ID] [--to-template] [--keep-source] [--json] [--timeout D] [--socket S]
node-ctl import-sandbox <token> [--memory=true|false] [--socket S]
```

- `--timeout` 限定完整导出请求时间，默认 `20m`，必须是正的 Go duration。发布需要物化磁盘内容，可能超过短管理请求的预算。超时会取消本次客户端请求，但不证明已接纳的服务端操作已经回滚；重试前应核对来源及发布结果。
- `export-sandbox <sid>`:打印单行 `kmt1.` opaque 迁移 token。默认成功表示结果有效且源删除
  已被持久接纳，物理清理可在响应之后完成；`--keep-source` 保留 paused source,且 ResumeSource 与
  local checkpoint 均保持原样——源沙箱仍从导出前的恢复位置恢复(#336).
- `export-sandbox <sid> --to-template`:发布 paused Sandbox E 或 Snapshot S 并打印对应的
  持久 `sbx` / `snp` templateID(扇出用).
  `--keep-source` 使用相同的 source retention 语义;未指定时同样接纳删除并最终完成物理清理。
- local artifact 上传期间统一允许 Resume,不增加额外开关。Resume 先受理时,KMT
  Export 取消上传并返回 409;Template Export 继续上传并返回 templateID。两者都放弃
  source finalizer,不更新/删除 source,也不删除 local artifact.
- `import-sandbox <token>`:缺省复用 token 中的 source NodeSandboxID,以 insert-only 方式
  写入 paused 行并打印 sid；目标已存在（包括仍在 `deleting` 的源）时返回 409。复用同一
  node-local ID 需要等待物理清理完成，import 本身不会等待。API body 可通过可选 `sandboxID` 指定另一
  个 node-local target,但不会改变逻辑认证主体或既有 service credential。随后调用
  `connect` 即可异步恢复。
- 省略 `--memory` 保留 token 的来源；`--memory=false` 永久只导入 E，包括来源为 S 的 token；
  `--memory=true` 要求 S。客户端仅在显式指定时发送该字段。兼容性与失败行为见
  [Import 选择表](#import-selection)；使用该选项需要服务端已支持。

`--json` 输出完整的成功 Export API 响应；默认仍只打印一行 `result`（token 或
TemplateID），发布进度不会进入 stdout。无效/尾随 JSON、被污染的 stdout、缺少 result
或根、错误根角色、不安全本地引用，以及 TemplateID 与根不一致都会明确失败。客户端也
检查响应读取错误，不会把空凭据打印为成功。完整响应契约见 [Publish、template 与 migration](#publication-and-migration)。

<a id="request-configuration"></a>

## 沙箱配置传递链

每实例沙箱配置经**命名空间化的 e2b metadata 保留键** `kuasar-sandbox.<ns>`(各值一个
JSON 对象字符串)注入,零 SDK/API 改动。这些独立的 typed 租户 schema 定义可配置子集，再由 renderer 生成 sandboxer 配置；
不能把完整 runtime YAML 当作租户策略直接导入：

| 命名空间 | 去向 |
|---|---|
| `resource` | 按叶子合并的严格资源 patch，支持 capacity、allocatable、startup、DiffCOW 缓存限制与冷启动 discard 策略（[资源 schema](node-resource_zh.md#request-resources)） |
| `traffic` | host-only 的 per-Sandbox `max_inflight.{total,forward,e2b:envd,e2b:code-interpreter,exec}` 显式 patch;不进入 guest |
| `network` | 拆分:`hostname`/`nexthop`→guest;`inner_ip`/`transit_*`→`vswitch.Attach`;`dns`→`/etc/resolv.conf` |
| `launch` | `launch.{exec,args,env,workdir,restart,user,stop_signal,plugin,cgroup_control}`——**仅 bare**;e2b profile 拒(envd 占用 launch) |
| `init` / `mounts` / `files` | 直透 `init[]` / `mounts[]` / `files[]` |
| `metadata` | `SANDBOX_CONFIG.metadata` 透传(如 `e2b.start_cmd`) |
| `restore` | 本次 host restore 的 `prefetch` 策略;可省略,显式值只允许 `off`/`memory` |
| `credentials` | 创建期 ServiceSecret、Envd/Traffic token override;解析后从普通 metadata 剥离,不进入 guest |
| `checkpoint` | host-only、仅本次 Create 的 local Pause 缺省:`merge_ref`/`drop_caches` 各自为 `true`/`false`/`null`;只存 sandbox row,不进入 runtime YAML 或 snapshot.cfg |
| `workspace` | 可移植的数据盘名称与挂载拓扑，创建时的盘源选择走私有通道。参见 [Workspace 存储](#workspace-存储)。 |
| `mmds` | portable exact `routes` + request-scoped initial `secrets`;持久化前拆分,metadata 最终只保留 routes |

字段定义随能力维护：[资源 patch 与优先级](node-resource_zh.md#request-resources)、[流量限制](node-proxy_zh.md#request-traffic)、[MMDS](node-proxy_zh.md#request-mmds)、[凭据覆盖](node-proxy_zh.md#request-credentials)、[集群系统上下文](cluster_zh.md#node-system-context)。下文定义共享输入装配、restore 与 checkpoint policy。

<a id="request-restore"></a>

### Restore policy 输入

单 sandbox 显式启用的两种等价请求形态:

```http
X-Kuasar-Sandbox-Restore: {"prefetch":"memory"}
```

```json
{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}"}}
```

`restore` 只接受严格 JSON object。未知字段、非字符串 `prefetch` 和非法枚举
均在生命周期副作用前拒绝。未提供时默认关闭;同一请求的 Header 覆盖
metadata。orchestrator 不判断本地/远程、单层/多层或底层 Prefetch 能力:显式
`memory` 在 restore 配置中原样表达,最终执行或跳过由 sandboxer 决定。

<a id="request-checkpoint"></a>

### Checkpoint policy 输入

Checkpoint policy 也支持 metadata 与 Create header 两个入口:

```http
X-Kuasar-Sandbox-Checkpoint: {"merge_ref":false,"drop_caches":null}
```

```json
{"metadata":{"kuasar-sandbox.checkpoint":"{\"merge_ref\":true,\"drop_caches\":false}"}}
```

两处共用严格 JSON object 解析:只允许 `merge_ref`、`drop_caches`,每个值只允许
`true`、`false`、`null`;unknown、错误类型、第二个 value 或尾随内容均返回 400。
Header 按字段覆盖 metadata;`null`/缺失表示不覆盖,而不是清除低层值。合并后全为
`null`/缺失则删除整个 namespace。该 namespace 不从 source template 继承；Build 的 checkpoint 输入见 [请求级 Builder 输入](node-build_zh.md#request-scoped-builder-input)。`local` 与 `bundle` 使用完全相同的 policy 解析、覆盖和副作用边界。

<a id="configuration-rendering"></a>

### 配置解析与持久化

请求级 `kuasar-sandbox.builder` 的完整输入和目标解析规则见 [Builder 输入](node-build_zh.md#request-scoped-builder-input)。下述规则继续定义共享 Sandbox 配置。

- **渲染**:cold image 仍在 network Attach/runner Assign 前用纯 resolver 生成完整
  `ResourcesConfig`。restore 在 runner task summary 到达后由唯一 launch worker解析
  snapshot capacity并生成同一 canonical `ResourcesConfig`;conductor 不打开 snapshot。
  renderer 直接安装该对象,不再逐字段重解释。YAML 不含
  `control.cgroup_path`;run-sandbox 最后以继承 cgroup FD 注入该 capability。
  dynamic `control.controller` 只取启动时解析的 `resource_listen.SocketIdentity`;
  `watermark_high.ratio` 由 node policy 注入,sensor 保持 omitted。其它 boot/tapfd/已解析 network 与租户子集再经
  config-socket 交 sandbox-ctl。
- **两个注入面**:e2b metadata 与 `X-Kuasar-Sandbox-<Ns>` 请求头在 API 边缘归一化。
  resource/traffic 按 leaf,MMDS 按顶层 key,checkpoint 按字段存在性,其余按各自
  whole-namespace 规则取 Header 优先。Create 的 runtime 输入存 `sandboxes.metadata_json`。
  Build 的共享 parser、目标准入与持久化由 [请求级 Builder 输入](node-build_zh.md#request-scoped-builder-input) 完整定义。
  集群 Sandbox 所有权使用独立 node-link 系统上下文([集群接入(node-link)](cluster_zh.md#node-attachment))。
- **优先级**:resource/traffic 使用上述各自 leaf chain；MMDS/checkpoint 使用各自专门规则，其余
  使用节点/cluster group 与本次 create 的 whole-namespace 行为。canonical TemplateID Create 不读取
  `builds.metadata_json`。Build resources 与最终 Sandbox resources 完全独立,不互相默认、比较或推导;
  trigger 的 `cpuCount`/`memoryMB` 只可断言不可变 Build resources。
- **capacity**:img create 自由(create/group/节点默认);sbx/snp create、paused resume 与迁移导入以
  Sandbox E 的 portable capacity 为权威。同步请求只校验 patch 结构;runner task prepare 后,request 显式相同
  Capacity 可作为 assertion,任一 leaf 不同则异步 `resource_resolve` failure。无法可靠读取
  artifact Capacity 时任务异步失败;runner 已经 Assign,但尚未 Attach network、写 YAML、
  创建 controller reservation 或启动 VM（runner 的委托 cgroup 可已存在）,且绝不回退 node defaults。restore initial
  reservation 严格等于 sandboxer 从 CH snapshot target/current 推导的
  `BudgetAtSnapshot`;startup headroom 不参与,也不接受 partial grant。
- **network 随制品**：普通 Sandbox 把已解析逻辑网络写入
  `SANDBOX_CONFIG.metadata["kuasar-sandbox.network"]`，由权威 Sandbox E 的
  `sandbox.runtime.cfg` 保存；S 通过 `sandbox_ref` 引用 E，而非把该 metadata 存在 `snapshot.cfg`。
  task-local reader 严格解析 metadata，拒绝 unknown/duplicate/malformed，只把 typed bounded network
  summary 交给 conductor；raw metadata 不跨 task 边界，也不作 best-effort 宽松回退。
  显式 Create 网络字段覆盖制品相同字段，最后补 profile/node 默认值。
  Build 的模板网络投影见 [请求级 Builder 输入](node-build_zh.md#request-scoped-builder-input)。MigrationToken 也携 metadata。
  除 host restore/checkpoint 策略外，其余配置在 cold launch 生效或已冻入 memory；network 还用于
  host 侧重建连接，因而保留 portable 网络声明。
- **host policy 不随快照或模板**:`kuasar-sandbox.restore` 与
  `kuasar-sandbox.checkpoint` 只由 create 请求写入 sandbox metadata。image cold boot
  不把它们渲染进运行 YAML;snp create、pause 后 resume 和 migration import 在存在
  restore ref 时重新渲染 restore policy;checkpoint 始终只由 host Pause 路径读取,
  不进入 `SANDBOX_CONFIG`/`snapshot.cfg`。connect/resume 不提供临时覆盖。
- **持久化**:`sandboxes.metadata_json` 是 Sandbox launch 配置；`builds.metadata_json` /
  `builds.builder_json` 是 retention-bounded Build 执行记录。后两者不会成为另一份模板权威。

<a id="node-defaults"></a>

## 节点生命周期默认值

以下字段属于 Conductor 配置；共享加载、路径与环境变量规则见[节点运行](node_zh.md#conductor-configuration)。

| 字段 | 默认 | 说明 |
|---|---|---|
| `sandbox.timeout_sec` | `300` | 沙箱默认 TTL(秒) |
| `sandbox.dead_ttl` | `24h` | dead 父对象及所属数据保留期，从本轮首次持久提交 dead 起算；必须为正 Go duration（[终止、Reset 与数据保留](#reset-and-retention)） |
| `sandbox.network.switch` | `sw0` | vswitch 交换机名 |
| `sandbox.network.hostname` | `sandbox` | guest 主机名:sethostname + `/etc/hosts` 条目([guest profile:envd 与工具链](node_zh.md#guest-runtime)) |
| `sandbox.network.dns` | `[169.254.169.253]` | 注入 guest `/etc/resolv.conf` 的 nameserver;该地址需部署侧路由到真实 DNS |
| `sandbox.network.e2b` / `.bare` | `169.254.0.21/30`+`169.254.0.22` / `169.254.1.1/31`+`169.254.1.0` | 按 profile 的 guest 内 `{inner_ip, nexthop}`:每 profile 复用同一对,沙箱唯一身份是 floatingip;e2b 的 /30 + 网关让 envd 端口转发可用 |
| `checkpoint.mode` | `local` | 暂停态本机 capture:`local` = 现有 tarstream,`bundle` = multi-Manifest ZIP Bundle；输出固定在 Sandbox `BaseDir/checkpoint`([节点目录与身份](node_zh.md#node-directories-and-identities)、[Artifact lifecycle、转模板与迁移](#capture-and-pause)) |
| `checkpoint.merge_ref` / `.drop_caches` | 未设置 | Pause 的节点级三态策略:`true`/`false` 显式传给 `sandbox-ctl snapshot`;省略或 YAML `null` 则交给 sandbox-ctl 缺省 |
| `checkpoint.remote.ref_location_parent` | 空 | 可选 absolute hostless `file://` URI；Builder checkpoint 类 graph 和 `export-sandbox` 的 named-location parent，也用于解析 located Build image Bundle；不改变 Pause capture mode |
| `checkpoint.remote.manifest` | `false` | `false`：Build image 类进入 Manifest store；`true`：image 类不写 Manifest store，而以 single-root Manifest Bundle 物化到上述 named location，且 parent 必填。它不改变 `checkpoint.mode` 或 checkpoint 类 policy（[按目标执行与发布](node-build_zh.md#execution-and-publication)） |

<a id="state-and-ownership"></a>

## 生命周期与状态机

生命周期遵循四条固定规则:

```text
Pause decides what is saved.
Resume decides what is used.
Wake only triggers Resume.
LaunchMode records what will actually run.
```

`CaptureKind`、`ResumeSource`、`ResumeMode`、`LaunchMode` 和 `ResumeTrigger` 是彼此独立的概念:

| 概念 | 值 | 职责 |
|---|---|---|
| `CaptureKind` | `snapshot` / `sandbox` | 本次 Pause 保存 Snapshot S 还是 Sandbox E |
| `ResumeSource` | `kind/ref`, Snapshot 还包含 `SandboxRef` | 行持有的精确 S/E 对或 E-only 根 |
| `ResumeMode` | `auto` / `memory` / `cold` | 调用方对本次 Resume 的选择 |
| `LaunchMode` | `image` / `memory` / `cold` | 解析完成并将在本次 starting 中实际执行的模式 |
| `ResumeTrigger` | `connect` / `wake` / `route` / `exec` / `exec-session` | 低基数日志与指标来源,不参与模式或权限判断 |

`ResumeSource` 保留 `Kind` 和 `Ref`. Snapshot 还必须包含 `SandboxRef`, 即 S 实际选择
的 E; E-only source 将 E 存在 `Ref` 中, `SandboxRef` 为空. 完整对作为一个原子持久
身份参与 SQLite, cache, 生命周期 CAS 比较, 重启恢复及加密迁移 token, 不是尽力填充
的缓存. `removedRefs` 仅属于操作报告, 不进入 SQLite 或 token.

Image 是只读镜像根;Sandbox E 是不含 guest 内存、但包含 portable `sandbox.runtime.cfg` 和磁盘
状态的执行制品;Snapshot S 是内存制品,其 `snapshot.cfg.sandbox_ref` 指向权威 Sandbox E。
Snapshot S 始终携有效内存状态,其 cfg 不使用 memory 开关表达 E。模板 kind 与启动映射为:

```text
img -> LaunchImage  -> sandbox-ctl run
sbx -> LaunchCold   -> sandbox-ctl run --from <E>
snp -> LaunchMemory -> sandbox-ctl run --restore <S>
```

状态机如下:

```text
Create(img|sbx|snp) --> starting --success--> running
                         +--fresh failure--> dead

running --Proxy ACK--> pausing --capture + commit--> paused(source=E|S)
paused --Resume(auto|memory|cold)--> starting(launch_mode=cold|memory)
                                      +--success--> running
                                      +--failure--> paused(original E|S)

running --termination + cleanup--> dead --Reset + commit--> paused(source=E)
Import --source selection + insert--> paused(source=E|S)

running|starting|paused|dead --Kill/Delete acceptance--> deleting
deleting --finalizer success--> absent
(dead retention expiry also requests deletion)
```

`starting` 是持久业务状态,覆盖 admission、artifact prepare、resource/network、runner pool、
runtime readiness 和 mandatory e2b `/init`。fresh Create 必须持久化 `launch_mode=image|cold|memory`;
paused Resume 必须在接受 `paused -> starting` 的同一原子更新中持久化 `cold|memory`。running、paused、
deleting、dead 均清空 `launch_mode`。running 可保留最近的 `resume_source` 用于本机制品 ownership;下一次成功
Pause 原子覆盖它。

同一 SID 的 Create、Connect、Wake、route activation、native exec、exec-session 和 migration import
共用 launch group。每次 admission 先在 per-SID lifecycle lock 内重读 durable row,再执行授权、
模式解析和 Store CAS,最后创建或加入 launch attempt。attempt 中的 mode 只是 durable
`launch_mode` 的缓存.Kill/Delete 在 lifecycle lock 内先 exact-CAS 到 `deleting`,保留 RunID,
unit identity,port,RunDir,BaseDir 与制品 owner，取消 attempt 并立即撤销 runtime cache，
在 live/full 管理投影中发布 deleting。业务路由撤销不证明本地资源已完成清理。
请求在 durable acceptance 后返回;节点 finalizer 等待 late launch owner 退出,再按
Stop/Reset + inactive readback,allocation fence 内 Detach + full-owner exact CAS 清空 network tuple,
RemoveAll RunDir,RemoveAll BaseDir,exact hard-delete 收敛.只有 durable network clear 成功才释放
detached-port fence;其后的目录或 hard-delete 故障继续保留 path/runner owner,但不阻塞新的 Attach.
hard-delete 后才发送 Extension/object terminal observation 和最终 route Delete。失败不清尚未
完成的 ownership,当前进程持续重试,崩溃后由 startup Reconcile 重试.每次 fresh Create 必须在启动资源前通过
route-applied barrier；失败清理本轮 runtime 归属、保留已形成数据，再以 exact owner CAS
提交 dead 与自动责任并发布 dead 管理投影。

Pause 的 `CommitPaused` 继续原子提交 state 与 source。其后 RunID、port、RunDir 分别只在
Stop/Reset fence、Detach、checkpoint 选择性清理及 RemoveAll 成功后 exact-clear；RunDir CAS 同时清除该目录拥有的 envd/ci
UDS path。任一步失败都保留尚未完成的字段供当前进程或 startup Reconcile 重试。fully-cleaned paused
row 保留含 checkpoint 数据及来源操作锁的 BaseDir 与 source；Resume/Wake/Exec 在取得新 runtime owner 前完成 backlog，
并在 `paused -> starting` acceptance 中原子恢复 canonical RunDir/UDS。这样 paused BaseDir 永不被
runtime cleanup 删除，显式 Delete 仍可从 fully-cleaned paused row 删除 BaseDir 后 hard-delete。

启用 `resource_listen` 时，本机准入检查所选资源池的[来源无关有效水位矩阵](node-resource_zh.md#rawzone-and-effective-zone)：
green/yellow 允许新建和普通恢复，red 允许普通恢复，critical 保留当前资源暂停恢复资格。
Snapshot 模板创建仍属于 Create。全部入口分类最终解析后的操作，已 accepted starting
在水位改变后仍保留原异步身份。

资源压力调用现有完整内存 Pause，包括 native exec quiesce/cleanup。既有 SQLite 行记录
resource-pressure 原因、当前恢复义务、版本及连续运行起点。单后台恢复 worker 与真实 Proxy
Wake 共用 launch singleflight；实际 running 提交才解除义务并重置运行起点，runner 创建或
打开 snapshot 均不代表履约。清理 ownership 与资源 Release 仍是不同事实。

对授权的 resource-paused 沙箱，缺省内存 Pause 在 Hook 及完整记录 CAS 复核后，原子接管
保存源为普通显式暂停。身份/source/清理保持不变，不重新 capture 或启动 VM。取消资源后台
恢复及未兑现 hold，State 同为 paused 也发布原因更新。显式 capture 选项或 filesystem-only
Pause 返回 400，因为无法认证新的 capture 动作已满足；普通重复 Pause 与 starting 仍为 409。
接管重启后仍有效。后续真实 Wake 按普通资格执行，合法保存 Snapshot 仍有完整预算恢复能力。
不引入永久用户暂停锁，不恢复被清理的 exec 子进程，也不重放已投递旧 VM 的请求。
Q、critical 退出、存储和记账规则见[资源生命周期](node-resource_zh.md#resource-pause-recovery-and-explicit-adoption)。

<a id="task-local-preparation"></a>

## Task-local artifact prepare 与三种 config

首次分配 worker 后，制品准备才可能获知权威 Capacity。需要更换资源池时，Conductor
先停止仅用于准备的旧 worker，再解除 assignment；由替代 worker 重新准备、核对
summary，最后发布运行配置。旧准备 worker 不启动 VM。[资源池](node-resource_zh.md#pool-lifecycle-and-pressure)
定义放置、持久恢复意图及容量记账。

artifact launch 的 tenant task 先执行 `internal/taskartifact`。`ArtifactPrepareSpec` v5 携 RunID、root
source kind/ref、已受理的 E（若存在）、durable launch mode、manifest config、ref-location parent、relative dir、max refs
和 absolute deadline。task 持有 `MANIFEST_KEY`,由官方 sandboxer reader 实际打开/解密制品;
conductor 不读取、解密或解析 tenant artifact。

prepare 路径为:

```text
E + cold   -> open E, parse sandbox.runtime.cfg, compute disk closure, prepared=E
S + memory -> open S, open S.sandbox_ref E, compute memory+disk closure, prepared=S
S + cold   -> open S, resolve/open S.sandbox_ref E, compute disk closure, prepared=E
E + memory -> reject during request mode admission; invalid task spec fails before VM launch
```

remote Manifest 的 S+cold 选择 `manifest://E`;local tarstream 解析相对、content-identified E file ref;
Manifest Bundle preparation 保留 reader 实际选择的 current/sibling carrier 与 E selector，
或实际远端 Manifest fallback；不假定 E 位于 S carrier 中。Result 中的 `PreparedSource`、ref-location URI、carrier/Bundle binding 和 cfg 只留在
task 进程。task 在本地严格解析 artifact network metadata,拒绝 duplicate/unknown/malformed 字段,
只把完整根 S/E 对、typed network topology、capacity、disk topology、required ref count、
`resolution_digest` 组成 bounded summary 交给 conductor；原始 metadata 不跨 task 边界。
digest 覆盖 RunID、完整根对、launch mode、selected source、
closure、locations、carrier binding、capacity/network summary。相同 runID + digest replay 返回同一结果;
冲突 replay fail closed。
外部 S-only 模板仅是初次 starting 阶段的 preparation 输入, 此时持久 source 为空,
不是有效的 paused source. 现有 tenant task 解析 S 时取得 E, bounded summary 只携
根 S/E 身份及上述摘要, 不把完整配置或 MANIFEST_KEY 相关解析移入 conductor.
相同摘要重试幂等, E 不同的重试冲突; 只有匹配根, RunID 和 starting 状态的 worker
能够把完整对与 running 一起提交. 失败保持初始 source 未受理; 重启不恢复缺失 E 的对.
preparation 不得替换已导入或已持久保存的 E.
最终 `taskrun` 只根据 task-local `PreparedSource` 追加 `--from <E>` 或 `--restore <S>`。

运行配置使用三种明确 DTO,不以一份完整 `SandboxConfig` YAML 服务所有模式:

- `ImageColdConfig`:用于 `run --config`;可包含 image root、writable diff template、launch/env/files、
  mounts/init/plugin、resources/network/metadata。
- `SandboxHostConfig`:用于 `run --from E --config`;只声明 `ApplyFromRules` 允许的 host/instance 字段,
  包括 kernel/runtime 实际 binding、active diff binding、resource controller/host policy、network provider
  和实例 IP/MAC/hostname、timeout、允许的 persistent override、`ephemeral_files`、
  `launch.ephemeral_env`。它不能声明 immutable root/data disk graph。paused E 或 S+cold 时 E 的 C0
  权威,不重放 row 中旧 launch/env/files;只有 fresh `KindSbx` Create 可有意提交 create-time persistent
  override。
- `SnapshotHostConfig`:用于 `run --restore S --config`;renderer 输出 `ApplyRestoreRules` 允许的 runtime 候选、
  active diff、resource controller/allocatable、network provider/实例字段、restore policy 和 timeout。
  renderer 清空并省略 kernel binding，因为运行中的 guest kernel 已保存在 S 中。
  类型本身不包含 launch、files、ephemeral files、init/plugin、mounts、metadata、boot cmdline 或 disk graph。

node 生成的 `/etc/hosts`、`/etc/resolv.conf` 在 cold 路径进入 `ephemeral_files`,不写 portable C0;
memory restore 不声称重新注入它们。persistent env/files 只随明确的 C0 override;ephemeral env/files
只影响本次 cold launch。renderer 输出须通过 `LoadMergedWithPresence` 后分别被 `ApplyFromRules`、
`ApplyRestoreRules` 接受,且多 data-disk name/order/mount topology 保持由 E 权威定义。

<a id="application-startup-policy"></a>

## 应用启动策略

节点配置 `sandbox.timeouts.app_start` 是 SDK ACK 后可选的整体期限。
省略、空值或 `0` 不增加期限；正时长显式选择有界等待，负数/非法值为配置错误。
既有 clone、bootstrap、workorder 和 Builder rendering 传递此宿主策略，portable
E/S 制品不携带它。Socket 期限、API/Proxy 等待、assignment 预算、running TTL 及
Builder 总期限/步骤/readiness 保持各自职责。

普通已接纳启动从 prepare、readiness 到 /init 跟随 attempt owner，不再隐式附加
60 秒总期限。Sandbox bootstrap 版本 7 继续允许 `absolute_deadline_unix_nano` 为零或
省略；正数是显式绝对截止，负数非法。版本 7 还要求首次 snp task 执行 runtime 准入。
Conductor 与 sandbox runner 须部署配套版本；bootstrap 版本 6 会被拒绝，避免静默漏检。
共用 prepare summary 保持版本 5；Builder 版本 7、正数 work deadline 及结果上报预留、
cluster protocol 和 KMT V1 均不变。

Manifest 打开复用 Sandboxer 的 `OpenManifestStream`/readretry，保留原 scoped
fetcher、Bundle 身份和调用方 context。Builder 在自身 work deadline 内受益；
file carrier 保持既有恢复边界。Store/Cache Get/GetShard 仍为 single attempt；
恢复不重放 Prepare/Create/Resume，不重复分配 runner/网络。

请同时升级 conductor 与待分配 node-ctl runner。Reconcile 停止旧 orphan 的精确
unit 并确认 inactive 后才补充 pool；失败带 RunID/unit 报错并阻止启动。新 pool
只接纳本次创建的 RunID。生成 unit 保留已解析的 node-ctl executable；自管 unit
操作者须在 ExecStart 路径安装兼容二进制。已有运行 VM 不被替换；Conductor
自身重启后接续所有 starting 不属于本合同。

新初始 Launch 在 apply 前要求 Guest `initial_launch_state_v1`。须使用匹配的
sandbox-init 重建 runtime Bundle；用于新 Launch 的预热 Guest 同样需要升级。
Host 升级无法替换旧 snapshot 中的 init。普通 restore 保留恢复语义，不消费旧
token 作为新启动事实。初始通知和已校验 Pong 确认同一 token；PID 存在或 Ping
可达本身不能证明成功。

依赖长期不可用会持续占用 `starting` runner、网络和目录。通过现有状态、launch
phase、接纳后持续时间和精确 RunID 诊断；running TTL 不清理 starting。Kill/Delete、
owner 取消、真实 runner/VM 退出、永久读取错误、readiness EOF/格式错误及 /init
非 204 仍终止启动。Cleanup 按精确 runner 停止、确认 inactive、detach/删除资源
执行。正数 app_start 或显式 PingFatal 选择有界终止；默认 PingFatalThreshold
仍为零。可选结构见 [conductor 示例](../deploy/conductor.example.yaml)。

已准备的 [启动恢复用例](../test/e2e/cases/orchestrator.launch-recovery.sh) 在 image、
E、S 路径观测超过 60 秒的实际 Manifest Get 故障，核对 SID、RunID、精确 runner/
进程身份与网络归属、唯一 runtime spawn，并删除仍在等待依赖的已接纳 attempt。
E/S prepare 先于 VM 创建；Sandboxer 恢复用例另行证明 post-ACK 及普通读取故障
期间原 VM 存活。须记录实际选定 SDK 和 Guest Bundle，不能只用本地相对 replace
构建成功证明正式 CI 的输入。
