[English](node-build.md) | [简体中文](node-build_zh.md)

# 节点模板构建

本篇完整定义 Build 注册、目标选择、不可变执行资源、任务准备、顺序阶段执行、发布及恢复。[节点规范](node_zh.md) 维护共享进程、socket、凭据与节点恢复边界；[Registry 规范](cluster_zh.md) 维护集群构建意图和放置。

## 1. Build API

实现 e2b **v2 build system** 的端点族(SDK `Template.build` / CLI 走它);构建语义与
资源池见 [§5](#5-按目标执行与发布)。

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| register | `POST /v3/templates` → 202 | body `{name, tags, profile?, cpuCount, memoryMB, metadata?, envVars?, secure?}`;CPU/memory 定义用于准入和 A/B 阶段沙箱规格的不可变 **Build.Resources**,最终目标 Sandbox 资源独立解析. `X-Kuasar-Sandbox-Builder.resources` 可声明同值并补 storage;同维度不等即 400. Builder `target` 是 register-only immutable 定义; `X-Kuasar-Sandbox-Resource` 则定义最终 Sandbox template resources, 不供 A/B 使用. `profile∈{e2b,bare}`,省略取 `e2b`;响应暴露 requested `target`(省略即 auto) |
| trigger | `POST /v2/templates/{tid}/builds/{bid}` → 202 | body 兼容 `{fromImage, fromTemplate, fromImageRegistry{username,password}, steps[], startCmd, readyCmd}`;只允许一次 `registered→waiting`。兼容的 `cpuCount/memoryMB` 仅可断言等于注册值,放大或缩小均在 credential/COPY/queue 副作用前 400。Trigger-time metadata、Builder/Resource 及其它通用配置 header 全部拒绝;execution 不足时留在固定节点 FIFO waiting |
| status | `GET /templates/{tid}/builds/{bid}/status` | 回基本 SDK 字段, requested `target`, 终态 derived `kind`, 及规范化 `resources{cpuMilli,memoryBytes,storageBytes}`, `cancelRequested`, `deleteRequested`, `executionClaimed`, `runID`, `storageEnforcement` 和当前 `phase{name,sandboxID}`;进行中 SDK status 仍统一为 `building`,内部 phase/claim 不丢失 |
| files | `GET /templates/{tid}/files/{hash}` → 201 | COPY context 上传协商:`tid→build→归属`校验后回 `{present, url}`——present 即对象已在桶(客户端跳过上传),url 为**直传桶的 presigned PUT**(字节不过控制面);未配 `files_storage`→**501**,未知/非属主 tid→**404**。详见 [§5](#5-按目标执行与发布) |
| list | `GET /templates` | 本租户 ready 模板;`templateID` 列为持久 id,同时回不可变 `profile`、requested `target` 和 resolved artifact `kind` |

### 1.1 取消与删除 Build 记录

Cancel 停止执行并清理本地资源,保留 Build 诊断行. DELETE 精确删除一条 Build 行及其拥有的附属数据库数据. 两者沿用 Build 认证和 ownership 校验,不申请注册或执行准入,容量满时仍可调用. 这是 E2B 风格 API 的项目扩展;DELETE 不删除远端模板或产物.

```http
POST /templates/{transientID}/builds/{buildID}/cancel
DELETE /templates/{transientID}
DELETE /templates/{transientID}?cancel=true
DELETE /templates/{transientID}
X-Kuasar-Sandbox-Builder: {"cancel":true}
```

均无需请求体. Cancel 不接受 Query 选项或 Builder Header,本地管理入口执行相同校验. 只接受注册返回的 transient TemplateID,兼容当前单机 UUID 与集群随机 ID 格式. canonical/PersistID、name、alias、远端引用和非法 transient ID 返回 400. 合法但不存在或非 owner 返回 404;Cancel 还要求路径两个 ID 对应同一行. 即使 List 展示成功后的 canonical ID,调用方仍应保留注册 ID.

DELETE 按字段 presence 合并 `Header.cancel > Query.cancel > false`. Header 缺省或 `{}` 保留 Query;`{"cancel":false}` 覆盖 `?cancel=true`,`{"cancel":true}` 覆盖 `?cancel=false`. 先分别严格校验两种输入,低优先级非法即使被覆盖也返回 400. Query cancel 只能出现一次且值精确为 true/false. Header 只能出现一份,必须是仅允许 cancel 字段的单一 JSON object. 空 Header、null、数组、重复/未知字段、非 bool、第二个 JSON value 和尾随内容均拒绝. 动作 parser 与 BuildOptions 分离:cancel 不进入 builder_json/metadata,Register/Trigger 拒绝 cancel,DELETE 拒绝 target/resources/referer/registry. 不提供 force 或产物删除选项.

| 持久条件 | Cancel | DELETE | DELETE cancel=true |
|---|---|---|---|
| registered/waiting,无执行或清理归属 | 204,保留 error 行 | 与 claim 原子竞争后删行,204 | 相同 |
| 任何执行/清理归属,此前无删除意图 | 持久取消,202 | 无副作用,409 | 同事务持久取消和删除,202 |
| 已接受删除,仍未完成 | 保留意图,202 | 保留意图,202 | 保留意图,202 |
| ready/error,完全无归属 | 保留既有结果,204 | 删行,204 | 相同 |
| 行不存在、非 owner 或 Cancel ID 不匹配 | 404 | 404 | 404 |

归属包括取得 claim 但还没有 RunID/pending 准备、assignment、host prepare、活动 phase 和已接受结果但未清理. 已取消但清理未完仍有归属. 单纯 waiting 不要求 cancel=true. 重复请求保留首次时间,只能增强意图.

Cancel 204 表示执行和清理均已完成;未被其他操作删除的记录保留. DELETE 204 表示节点行及附属数据库数据确实已删除,执行和本地清理归属均解除. 只有意图成功提交才能返回 202. DELETE 202 提供相对 `Location: /templates/{transientID}/builds/{buildID}/status`. 轮询既有接口,通过 cancelRequested/deleteRequested 和 executionClaimed 查看进度;SDK status 保持 building/ready/error. 最终删行后为 404;网络错误和 5xx 不代表完成. 再次删除已不存在的行返回 404,不保存墓碑维持 204.

`node-ctl builder cancel <build-id>` 和 `node-ctl builder delete <transient-template-id> [--cancel]` 经本地管理 socket 调用同一 Core 操作. `node-ctl builder status` 展示用量对应的 Build 身份、意图、claim 与资源向量.

## 2. 模板 ID 与工件权威

```
persist  templateID = <profile>-<kind>-<base64url(canonical-portable-ref)>
                                            profile∈{e2b,bare}; kind∈{img,sbx,snp}
transient templateID = transient-<registration-id>  保留以查询、取消和删除 Build 记录
```

- **持久 id 自描述**:payload 是 `manifest://<key>` 或
  `file://<digest>.image@digest:<digest>@location:<name>`；encrypted carrier 使用 `@hmac:`，
  Bundle 用 `@manifest:<root-key>`；运行期解析 profile（选择共享 runtime 的 guest 行为）、kind(img = image cold,sbx = Sandbox E cold,snp = Snapshot S memory restore)
  和 canonical portable ref。snp 可在 Connect 时显式选择 cold,但 TemplateID 的缺省仍是 memory。
  local file ref、宿主绝对路径、非 canonical ref 或 artifact kind 不匹配均拒绝。
- **临时 id** 由注册生成;构建完成后持久 id 写入该构建的 names + aliases 一并返回,
  之后启动使用持久 id,取消和删除记录仍用注册临时 id。Build status、临时 id、name/alias 与本机 list 仅在终态 Build row 的
  retention window 内可用。
- **无独立 templates 表**:canonical TemplateID 本身编码 profile、artifact kind 与 portable ref，
  其制品才是长期 launch authority。`builds` 只承担构建执行、短期 status/index/alias，不是模板
  catalog；终态 row 删除后 canonical TemplateID 仍可创建 img/sbx/snp Sandbox，也可直接作为后续
  Build 的 `fromTemplate`。快照晋升的模板
  ([§8.1](node_zh.md#81-artifact-lifecycle转模板与迁移))同样无需写 builds 表。

## 3. Build 配置

下列字段属于 Conductor 配置，但其完整 Build 服务语义在本篇维护。共享进程、路径和凭据配置见 [节点规范](node_zh.md#3-配置)。

| 字段 | 默认值 | 含义 |
|---|---|---|
| `units.builder_pools` | 缺省 | 每项 `{unit, size}` 创建独立 pool，重复模板和相同项均合法；size 是 idle 目标数，0 按需启动。缺省则沿用 `builder` / `builder_pool_size` 单池（默认 `sandbox-builder@.service` / `0`）。新旧冲突规则及共享 dir/install/pool_wait_timeout 见 Node §3 |
| `builder.admission.execution.max_builds` | `2` | 同时持有 durable execution claim 的 Build 上限 |
| `builder.admission.execution.resources.{cpu,memory,storage}` | 不限制 | execution 的聚合准入资源向量;不生成 service/slice CPU 或内存策略,storage V1 仅准入记账 |
| `builder.admission.registration` | 完整继承 resolved execution | 未带 cancel/delete 意图的 registered/waiting/building Build 注册上限;显式块不做字段级继承,且同一有限维度不得小于 execution |
| `builder.registration_ttl` / `.queue_ttl` | `1h` / `30m` | 未 Trigger 的 registered Build 与 waiting Build 的持久超时;终态可查询并释放 registration usage |
| `builder.terminal_ttl` | `24h` | 已完成 cleanup、无 execution/runtime/result owner 的 `ready/error` Build 历史保留期；必须为正 Go duration |
| `builder.insecure_registry` | `false` | 经明文 HTTP 拉取 base 镜像(dev/本机 registry) |
| `builder.platform` | 空 | 拉取平台,如 `linux/amd64` |
| `builder.image_uri_mask` | 空 | 客户端推送镜像的命名约定(含 `{templateID}`/`{buildID}` 占位,须与 e2b CLI 的 `E2B_IMAGE_URI_MASK` 一致);trigger 缺 `fromImage` 时据此推导;**须从构建沙箱内可达**——拉取在 guest 内进行([§5](#5-按目标执行与发布)) |
| `builder.referer` | 关 | fromImage import 的 OCI Referrers cache:`enabled` 默认 false;`fallback`/`writeback` 默认 true;`desc` 为公开 owner descriptor(启用时必填);`key` 为空则等于 desc;`validity` 为可选 Go duration。build 可经 `X-Kuasar-Sandbox-Builder` 进一步禁用 lookup/writeback,不能越权启用([§4.4](node_zh.md#44-沙箱配置传递链)、[§5](#5-按目标执行与发布)) |
| `builder.diff_template` | – | 构建沙箱可写盘使用 `mkfs.ext4 -O ^has_journal` 预格式化 ext4(拉取缓存 + steps 增量 + 导出 scratch;稀疏文件,建议 ≥ 最大预期镜像的 3 倍) |
| `builder.{pull,step,ready,total}_timeout_sec` | `600`/`600`/`120`/`1800` | 阶段超时:guest 内拉取+展平、单条 RUN step(经 `Connect-Timeout-Ms` 同步到 guest 侧)、readyCmd 轮询预算(2s 间隔;缺省 readyCmd = `sleep 20`)、整个构建(单元不设置 `TimeoutStartSec`;另有独立的 60 秒 fencing/宿主清理窗口,见 §6) |
| `builder.files_storage` | 空 | COPY 构建上下文的 S3/OBS 对象存储(子键 `endpoint`/`region`/`bucket`(必填)/`prefix`/`access_key`/`secret_key`/`force_path_style`/`presign_expiry`);空 = COPY 回 501。serve 仅 presign + HEAD;custom Runtime credentials provider 优先于静态 YAML/AWS 默认链、支持 session token/expiration/refresh 且失败不回退;`force_path_style` 默认 false(versitygw/minio 置 true);`presign_expiry` 默认 1h(PUT;GET 用 total+5m)。本地/单机无云对象存储用 versitygw([§5](#5-按目标执行与发布)) |

多池的主要应用场景是 NUMA 部署，双节点 runner/Builder 配置、systemd 放置策略 drop-in、安装顺序及应用边界（不要求 NUMA 实测）见 [Node §5.3](node_zh.md#numa-deployment)。仅在全局执行准入后选择一次 Builder pool；实际执行的 A/B/C 阶段保持在同一 Builder unit 及其宿主放置策略下。Builder size 是预热目标，不是每 NUMA 节点的并发/内存预算。由构建产物创建的后续 Sandbox 独立选择 runner，制品不会把它固定到 Build 宿主的 NUMA 节点。

Builder 配置直接替换下列旧字段，不保留 alias：

```text
builder.max_concurrent -> builder.admission.execution.max_builds
builder.cpu_quota      -> builder.admission.execution.resources.cpu
builder.memory_max     -> builder.admission.execution.resources.memory
builder.vcpu/memory    -> 删除；A/B phase 从 immutable Build.Resources 派生
```

若显式配置 registration,它不会从 execution 隐式继承省略维度;若整块省略则完整继承
resolved execution。仅缺少当前 schema marker 的不兼容开发数据库要求重建;已支持 schema 的 additive migration、拒绝边界与无双读规则见 §6。

### 3.1 请求级 Builder 输入

构建注册额外接受 **build-only** 命名空间 `kuasar-sandbox.builder`,对应请求头
`X-Kuasar-Sandbox-Builder`,当前形态:

```json
{
  "target":{"kind":"sandbox","memory":false},
  "resources":{"cpu":4,"memory":"8GiB","storage":"64GiB"},
  "referer":{"enabled":false,"writeback":false}
}
```

`target` 只接受 `{kind:"image"}`、`{kind:"sandbox",memory:false}`、
`{kind:"sandbox",memory:true}`；省略表示 auto，显式 `null`、unknown/duplicate 字段、
`image+memory:true` 均拒绝。`sandbox.memory` 省略等同 false。Auto 只在解析来源 E 默认值后按
effective start/ready 决定：任一非空即 memory Sandbox，否则 Image；永不自动生成
top-level Sandbox E。显式 Image 与本次 trigger 显式 start/ready 冲突，来源 E 的命令则被忽略。

其中 `resources` 只定义 Build execution/admission resources,referer/registry 只控制本次模板构建。
target 和这些 build-only 字段解析后均从模板 metadata 中剥离,
持久化到 `builds.builder_json`;不会随模板 create/resume 进入运行时配置。

Create 与 Register 复用 resource/network/traffic/launch/init/mounts/files/metadata、
`envVars` 和实例选项的共享 typed parser,各 namespace 的字段与 Header/metadata 合并规则
见 [Node §4.4](node_zh.md#44-沙箱配置传递链)。Register 始终拒绝 `kuasar-sandbox.identity`、
租户提供的 node-managed cluster metadata、`restore` 与 `autoPauseMemory`。
普通 Build runtime 输入存 `builds.metadata_json`,build-only 输入存 `builds.builder_json`;
只服务本次执行与制品生成,不是 canonical TemplateID 的长期 metadata lookup。
Trigger 不能覆盖注册 metadata、Builder/Resource 或其他通用配置头;非空通用 metadata/header 被拒绝。

- 同步接纳时,已知为 Image 的 target 拒绝归一化后仍存在的 Sandbox resource/traffic/launch/init/mounts/files/metadata/checkpoint/MMDS namespace,
  以及非空 `envVars`、`secure=true`、非零 credential override 或显式 MMDS routes/secrets。
  普通 metadata label 与 Build execution resources 仍允许;空 `envVars`、`secure=false` 本身不构成拒绝条件。
- 所有 target 均接受 `X-Kuasar-Sandbox-Network` / `kuasar-sandbox.network` 作为 Build 执行网络,
  包括显式或自动解析的 Image target,也允许空 network 对象。网络输入不要求输出 Sandbox;
  A/B 使用它完成镜像导入和构建步骤,Sandbox target 还会将其投影到 E。
- 同步接纳时,显式顶层 Sandbox E 接受 portable Create 配置(包括 env),但拒绝 traffic、非零 credentials、
  显式 MMDS routes/secrets、`secure=true` 与非空 checkpoint 等 instance/action-only 输入。
- 只有 resolved `sandbox,memory:true` 应用后一类输入。`instance_config_enc` 加密 env/secure/credentials;
  traffic/checkpoint/MMDS routes 存 metadata,MMDS values 使用独立加密表。实例/动作字段不进入 portable E/S;
  portable env 保留自身投影规则。空对象不是统一豁免:例如 `resource:{}`、`traffic:{}` 仍会被 Image 拒绝;
  空 checkpoint 与零值 credential 经各 namespace 归一化处理。
- 尽早同步校验目标兼容性:显式 target 与 bare auto 在 Register 校验;e2b auto 在 Trigger 完成 Hook 和来源别名解析后,若命令或 Image 来源已能确定目标,则立即校验。冲突返回 HTTP 400 并指出参数;Trigger 拒绝后保留 registered,可修正重试,不进入队列。
- e2b auto 若仍依赖 task-local 来源 E 的命令默认值,则允许同步请求通过。后续 preparation、worker publication、结果校验和 IMG 直接复用均忽略实际目标不支持的配置,保留原始注册定义,不自动升级 target 或额外启动 Phase C。目标、effective commands 和唯一产物的完整性校验继续保留。需要携带 env 可选择 `sandbox,memory:false`;instance-only 输入需要 `sandbox,memory:true`。


Build execution resources 与最终 Sandbox resources 独立,不互相默认、比较或推导;
Trigger `cpuCount`/`memoryMB` 只能断言不可变 Build resources。

Build Register 的 routes 只服务
可能运行 memory Phase C 的 synthetic builder sandbox；显式 Image/顶层 Sandbox E target
在注册期拒绝它;auto target 能同步确定冲突时返回 400,否则接纳后若来源解析为 Image 则忽略该输入。initial values 以 build owner 加密保存。build 终态事务同时
从 `builds.metadata_json` 删除 routes namespace 并删除 value blob,因此两者都不进入最终
template/snapshot/image。Build Trigger 不接受 MMDS 覆盖。

Build register
只在 resolved `sandbox,memory:true` 时应用 `kuasar-sandbox.checkpoint` policy并用于 Phase C capture,且不写入最终 portable
config。已知不兼容时同步拒绝;已接纳且依赖来源解析的 Image 不解析 checkpoint policy 或目标 Sandbox resources。

Build 临时 VM 与最终模板使用同一 NetworkSpec resolver；未声明 hostname 时临时 VM 用
  `build-<short-build-id>`，成品用 `sandbox.network.hostname`，所以临时 hostname 不进入模板。
  顶层 E 与 C 的 E 均保存最终有效 NetworkSpec，使仅持 canonical artifact、没有原 builds row 的
  Create/restore 仍可恢复逻辑网络。IMG 没有 Sandbox metadata 通道，只使用当前 Create/group/node
  配置，绝不从 retention 内旧 Build row 回填；SBX/SNP 长期权威是自身 E。

## 4. 任务交接

复用已认证的 [节点 config socket/run 平面](node_zh.md#6-本机控制-socketrun--task--admin--plugin--api-平面)。业务身份、exact RunID 与已锁定 task pidfile 在解密 provider 前验证。共享平面维护分配、phase/result envelope；下述为 Build 专属 payload。

- **run-builder**:取得 bid 后锁
  `<BuildRunDir>/builder.pid`,以 bid + exact run-id 取 bootstrap。
  fromImage 与 IMG fromTemplate 在这一次 RPC 中直接取得最终 BuildSpec；SBX/SNP fromTemplate
  则安装 authoritative `MANIFEST_KEY`,在本进程执行 cold/E-selection Artifact prepare；SNP
  只以 S 定位 E，完整 E/source-image config 与已过滤的 sorted ref-location 留在 task 内，
  向 conductor 只提交非秘密 bounded summary 并等待最终 BuildSpec。reader/fetcher
  在提交前已显式关闭，conductor 不读取任何 tenant artifact。run-builder 将本地保留结果合入
  final spec 后**驻留**驱动 target-aware、最多三阶段的构建流水线(§5):各阶段沙箱(`sandbox-ctl run`)是它的直接子进程,
  整个构建计入本单元 cgroup;结束把结果
  `{target, exactly-one-of image_ref|sandbox_ref|snapshot_ref, start_cmd, ready_cmd, error, failure_stage}` 经 config-socket 回传。根cfg读取、
  prepare RPC、phase report与pipeline统一在同一个absolute deadline内预留最多最后5秒用于持久
  回传结果；剩余预算不足10秒时预留其一半，避免短timeout在工作开始前即过期，同时不会重置
  或延长总预算。只有最终result POST可使用这段尾窗。
  BuildID 始终是持久业务身份并原样作为目录 leaf；48-byte 上限保证最深 phase UDS 在
  支持的 RunRoot 下仍不超过 Linux `sun_path`。

- `POST /internal/task/build/bootstrap`(run-builder;req `{build_id,run_id,version}`)
  当前 BuildTask schema 为 v6（v6 要求来源命令存在性摘要以按实际目标准备配置;v5 分离 checkpoint-location 与 image-class Bundle publication，
  删除旧 broad `publish_location_parent`；v4 增加 register-time requested target，并把 SBX/SNP
  source 收敛为通用 cold Sandbox E；v3 以 `run_dir`/`base_dir` 替代混合语义 workdir，
  `checkpoint_mode` 自 v2 起必需),sandbox ArtifactPrepare
  schema 独立为 v5（v5 返回精确根 S/E 对，并以 RunID 与 resolution digest 绑定；v4 增加 Build-only image Bundle publication preflight；v3 增加只供 Build source 使用的 image-config 读取 capability、同一
  Bundle 内 E/image 的 carrier scope，以及 portable allocatable/deflate resource defaults；v2 以 typed E/S、durable
  LaunchMode 和 bounded network/disk summary 取代旧 v1 Snapshot-only wire),两个版本号独立演进；bootstrap 和 build prepare 的版本不匹配均在读取
  secret-bearing provider 前返回 400,
  防止旧 task 静默忽略必需的 publication 语义。参见 [build_task.go](../internal/configsock/build_task.go)
  与 [sandbox_task.go](../internal/configsock/sandbox_task.go)。可选的 Build-only 命令存在性摘要与 digest 字段受 BuildTask v6 gate 保护,不回传命令文本;普通 Sandbox preparation 不使用该 Build-only 字段。认证后返回 task env 与 exactly one of
  `Final|Prepare`。Final 是
  **BuildSpec(构建工作单)**:`{build_id, profile, run_dir, base_dir, from_image | from_template
  (+kind), requested_target?, checkpoint_mode, steps[], start_cmd, ready_cmd, paths, net,
  resources, sandbox_resources, sandbox_spec/namespaces/env, mmds_enabled, envd_token,
  insecure, platform, timeouts}`；task env 含
  `MANIFEST_KEY` + 租户 `FLATTEN_*` 拉取凭据。artifact Prepare 含 RunID、source kind/ref、恢复已准备 Snapshot 时的受理 E、LaunchMode、manifest config、
  ref-location parent、ref 上限和从 execution claim 起算的绝对 deadline。task 读取根 cfg 后向
  `POST /internal/task/build/prepare` 提交 `{build_id,run_id,version,summary}`；相同对与 digest replay 返回同一 final result，
  冲突返回 409，HTTP waiter 取消不撤销已接受 summary。`paths` 是宿主侧工件与工具
  (kernel / runtime / 两个 diff template / sandbox-ctl /
  flatten-ctl / manifest-ctl / manifest_config);`net` 是 serve 预先 attach 的
  网络槽(tapfd transport、mac、inner_ip、nexthop、hostname、dns),全构建复用。
  SBX task 直接读取 E；SNP task 只读取 S 的 `sandbox_ref` 来定位并读取 E，丢弃 S memory
  payload/from-refs。返回 conductor 的 bounded summary 包含精确根 S/E 对；完整 E config
  与选中的 runtime argv source ref 留在 run-builder 进程内，随后强制 Phase B materialize。
  run-builder 据此自建阶段沙箱([§5](#5-按目标执行与发布));仅在该构建单元运行期间可取(serve 持挂
  pending 状态,单元退出即失效)。

### 4.1 Builder unit 与进程生命周期

单元安装、共享 pool 分配与 cgroup 基础设施见 [Node §5](node_zh.md#5-进程管理systemd-模板单元启动时自动生成安装);下方单元日志注释中的 §5.2 指 [Node journald](node_zh.md#52-日志journald-单汇--标签词表)。

**builder 单元**(`%i` = run-id):

```ini
# sandbox-builder@.service (生成内容)
[Unit]
Description=kuasar image build runner %i
CollectMode=inactive-or-failed

[Service]
Type=exec
WorkingDirectory=/run/sandbox
StandardError=journal
# 关闭此单元日志限流以保留构建细节；不保证 journal 故障下零丢失（§5.2）
LogRateLimitIntervalSec=0
ExecStart=<node-ctl> run-builder --pidfile=/run/sandbox/runners/%i.pid \
          --config-socket=/run/sandbox/node-ctl.socket --run-id=%i
ExecStopPost=/bin/rm -f /run/sandbox/runners/%i.pid
KillMode=control-group
Slice=sandbox-builder.slice
# phase ctl/vmm 子 cgroup 与可信 VMM cgroup FD
Delegate=yes
```

builder 取得 bid 后再锁
`<BuildRunDir>/builder.pid`,取 exact-run bootstrap；artifact source
完成 task-local root prepare 和第二阶段后再取最终 BuildSpec,image 路径在 bootstrap 内直接取 final,
**驻留**驱动 target-aware、最多三阶段的流水线(§5),阶段沙箱(`sandbox-ctl run` + cloud-hypervisor)是其
直接子进程、整个构建计入本单元 cgroup,结果经 config-socket 回传。`KillMode=control-group`
保证 StopUnit/超时连阶段 VM 一并回收。

builder pool 在全局 execution 准入成功后的实际 Assign 边界独立轮询，锁不覆盖等待。
注册和准入拒绝不推进游标，size 不影响比例，也不向其它 pool 回退。单个 Build 全部阶段
仍由同一 builder 驱动，不进入普通 runner pool。全局 registration/execution ledger、
FIFO/claim 与清理释放顺序不变。共享模板仅安装/枚举去重，pool 不去重。
StartUnit 前登记 RunID 的创建池；assignment 后保留实际 unit 归属，直到原生命周期清理。
重启按配置模板恢复实际 unit，保留先查持久 Build 绑定的 WaitAssignment 重试路径。
取消、终态删除、失败、已接受结果与待清理归属均使用实际 unit；枚举失败不能清空归属。
仍有执行或待清理归属的模板必须保留配置，旧未分配预热进程按 orphan 清理，新 pool 独立补齐。

Builder service 和 sandbox-builder.slice 负责进程归属, 委托与整组回收,orchestrator 不增加
CPU/内存限额或其它父级资源策略. 有限 execution 准入资源允许非零 builder_pool_size;
idle 单元不持有 Build claim. idle 与按需 worker 都必须先有持久 execution claim,再成功
绑定 exact run-id,最后发布 assignment. 绑定失败不发布 assignment,未分配 worker 与 Build
沿用既有 pool 和归属清理规则.

不可变 Build.Resources 同时用于准入/记账以及 resolveBuildExecutionResources 的 A/B 阶段
规格. resolveBuildTargetResources 独立解析目标 Sandbox. sandbox-ctl 负责 VMM 的
memory.max, memory.high, CPU capacity ceiling 和 weight;ctl 保留独立的同级 cgroup.
恢复继续核对执行身份, 存活, 持久 preparation, 已接受结果, deadline, phase 和清理归属,
不读取 service/slice 资源属性.

#### 既有部署的一次性升级

外层 systemd enforcement 契约直接删除,不替换为父级 quota, weight, memory.high, overhead
预算或后台清理器. 新 unit instance 不接收 orchestrator 资源属性. 更新项目生成的
sandbox-builder.slice 会删除 CPUQuota/MemoryMax 行并 reload systemd;既有 live service
仍可能保留旧进程写入的 runtime properties,daemon-reload 不会删除这些属性的来源文件.

隔离用例 `test/e2e/e2e_builder_unit_upgrade.sh` 实际检查 reload 前后的 cgroup 文件:
项目生成的 slice 解除旧策略,同一 live service 的 runtime 限额和 PID 保持. 该 exact fixture
执行结束, 仅删除其两份已知属性文件后,新执行的 cpu.max 首字段为 max, memory.max=max.
真实 Builder 套件另行检查相同父级值,同时保持 sandbox-ctl 的有限 VMM 叶子控制.

一次性操作步骤:

1. 安排维护窗口,暂缓新 Build 提交,让已有 Build 按正常生命周期完成. 核对 exact Build
   状态, execution claim 和 runtime/phase 归属;不得为清理主动终止 live Build 或释放未完成
   claim. 升级 writer 前保留一致的数据库/配置备份. 如带 live Build 升级,先保留其旧 runtime
   设置直至完成;恢复不再以这些属性裁决执行有效性.
2. `units.install=true` 由新 conductor 更新项目生成的 slice 文件;`units.install=false` 的
   文件调整属于运维. 先核对 FragmentPath, DropInPaths 和文件内容,仅移除已确认由旧项目
   生成的 CPUQuota/MemoryMax 指令,保留 operator drop-in 及其它单元属性.
3. 逐个以 exact unit name 检查已经完成的旧 run. 旧 SetResources 经 D-Bus 写 runtime-only
   属性,通常对应下列两份文件. 删除前确认来源,并确认 Build/unit 不再有执行或清理归属.
   缺失或不同名称的文件需要检查,不得改为通配符或 systemctl revert,不得对模板, 所有
   instance 或祖先 slice 批量执行.

```bash
unit='sandbox-builder@<completed-run-id>.service'
systemctl show "$unit" -p ActiveState -p SubState -p ControlGroup -p FragmentPath -p DropInPaths
systemctl cat "$unit"
# Only after exact ownership and source confirmation:
rm -- "/run/systemd/system.control/$unit.d/50-CPUQuota.conf" \
      "/run/systemd/system.control/$unit.d/50-MemoryMax.conf"
systemctl daemon-reload
```

4. 使用 `systemctl show --value -p ControlGroup <exact-unit>` 定位更新后的 slice 与新执行,
   实际检查 cgroup 文件. 在没有运维附加策略的隔离环境中,父 service/slice 的 cpu.max 首字段
   为 max, memory.max=max,ctl 不进入 VMM 限额. VMM memory.max, memory.high, CPU capacity
   ceiling/weight 继续由 sandbox-ctl 管理. 已完成 service 可能已无 cgroup,不能据此证明后续
   instance 的值;残留设置只能按已确认来源处理,不覆盖运维策略或任意祖先 slice.

Store.Open 独立处理受支持 schema:存在才删除 builds.enforcement_status 单列,保留业务行,
凭据, 已接受结果, claim 和全部真实归属,迁移错误使启动失败. 重复 Open 安全,pre-#300
拒绝边界不变. 新 schema/API/BuildView 不含 systemd enforcement 字段,独立的
storageEnforcement: admission-only 语义保留. 旧 writer 仍引用已删除列,不支持混用或直接
回退旧二进制;writer 版本协同和回退必须结合升级前的一致备份.


## 5. 按目标执行与发布

构建经 e2b API 提交(端点见 [§1](#1-build-api);无独立构建 CLI),落 `builds` 表,由资源池调度,
每次执行绑定一个 `sandbox-builder@<run-id>` 单元。**镜像拉取与 step 执行都发生在构建沙箱
(microVM)内**——宿主 run-builder 会中转工件 stream、下载并 gunzip COPY context 及发布输出；
该边界是租户构建命令在 guest 内执行，不是租户数据从不经过宿主用户态。

Register-time `builder.target` 明确三种结果：`{kind:"image"}`、
`{kind:"sandbox",memory:false}`、`{kind:"sandbox",memory:true}`。前两者不运行 C；
顶层 Sandbox E 由 final image + 普通 Create config 无 VM 组装。只有 memory Sandbox
冷启动 C 并捕获 Snapshot。省略 target 时，run-builder 在 task-local 读取来源 E
默认命令后，只按 effective start/ready 是否任一非空选择 memory Sandbox 或 Image。
source kind、steps、profile 和其它 Sandbox config 都不参与推导。

来源统一先收敛为 image 或 cold Sandbox E，再由 A/B 得到本次 Build 的顶层 image。
Build 从不恢复来源 Snapshot S 的 memory/VMM 状态；S 只用于找到它的 E。最终 Image/E/S
不得引用来源 S、来源 E、来源 writable/data disk 或 memory parent，memory 结果中唯一允许的
`S -> E` 是本次 Build 自身产生的关系。

**serve 侧(每构建一次)**:registered/waiting 阶段不建对象目录；durable execution claim
成功后才创建 `BuildRunDir=<RunRoot>/builds/<BuildID>` 与
`BuildBaseDir=<BaseRoot>/builds/<BuildID>`，其中预建 `BuildBaseDir/checkpoint` → 做
request-only spec/resource policy解析 → 从 builder pool 分配,再以 exact run-id 持久绑定(无 idle 时按需
`StartUnit`)→ run-builder取得 bootstrap；SBX/SNP fromTemplate由task按 cold/E-selection
语义读根 cfg 并提交 summary，image fast path无需第二次 RPC → strict解析继承network并与本次请求合并 → 配
`tapfd_socket` 时经 `TAPFD/1 PREPARE`、否则经 `connector-ctl vswitch attach` 分配一个网络槽
(整个构建复用,各阶段顺序交接 tapfd)→ 铸 envd token → 在一个 exact-run SQLite CAS 中原子写入
port/token 与非秘密 `runtime_prepare_json`(完整根对与 prepare digest、resolved build/template network、
独立的 A/B execution 与 target Sandbox resources)。runtime preparation schema v5 同时冻结已受理的根 S/E 对、digest 和来源命令存在性以保持恢复语义；重启后 replay 仍同时比较对与 digest；
仅实际 Sandbox target 解析目标 resources,仅 memory=true 解析 checkpoint/instance policy → (e2b + `mmds.enabled` 且实际 target 为 memory Sandbox 时)
先挂一行 synthetic sandbox route → 返回最终
BuildSpec。route 必须先于 final handoff 可见，避免 task 取得 spec 后立即启动 phase C 时尚无法
解析自身；该 route 让模板阶段 FC 模式的 envd 能按
floatingip 自解析,并把 Register 时声明的 MMDS routes 及 build-owner initial values
投影给本次 builder guest → run-builder执行流水线
→ 经 config-socket 回传 `{target, exactly-one-of image_ref|sandbox_ref|snapshot_ref,
start_cmd,ready_cmd,error,failure_stage}` → conductor 在 execution claim 仍持有时先 durable 保存并按
requested/auto target fail-closed 校验；终态 derived kind 分别为 `img`/`sbx`/`snp`，持久 id
`<profile>-<kind>-<base64url(portable-ref)>` 写入 names/aliases。`Build.Kind` 在非终态保持空，
不充当第二个 target authority。profile 从注册到
BuildSpec 全链路显式携带;节点的 `checkpoint.mode` 也作为 `checkpoint_mode` 随 BuildSpec
交给 Phase C。register/trigger 在入队前校验请求自身的 network metadata；
snapshot继承metadata的strict解析属于task summary后的异步prepare failure。执行时只解析一次
并补齐 profile/node 默认值,同一个 `NetworkSpec` 同时派生 host
`vswitch.AttachReq` 与 guest `BuildNet`。`transit_*` 只在 host Attach 消费,不进入
`BuildNet`;无 transit 时保持零值。

node 配置加载首先验证 publication matrix，尤其
`checkpoint.remote.manifest=true` 必须同时具有 absolute
`checkpoint.remote.ref_location_parent`。run-builder 随后在任何 source image 扫描、目录大文件
写入或 VM 启动之前加载 `manifest_config`、固定 task customer key，并为 image Bundle 取得和
验证 write admission；SBX/SNP 两阶段 bootstrap 在 task-local E/S reader 打开 root carrier
前执行同一预检。失败只产生 Build error，不启动 phase 或返回 artifact ref。

MMDS synthetic route 只在 real builder run-id 已持久化后发布,其 `RunID` 同样约束
MMDSv2 token incarnation。流水线结束先撤销 route view;所有 ready/error/cleanup 终态再与
build row 更新原子删除 MMDS routes namespace 和 encrypted value blob。Register 的 MMDS
namespace 是 request-scoped,不会进入最终 template metadata、snapshot.cfg、镜像或后续从该
模板创建的 Sandbox;Trigger 也不能重写它。

**单元内(run-builder,[§2.4](node_zh.md#24-node-ctl-run-sandbox--run-builder))** 依 BuildSpec([§4](#4-任务交接))最多跑三个阶段,每阶段一台
microVM(`sandbox-ctl run` 直接子进程)。父进程为每个 phase 建匿名 pipe,通过
`ExtraFiles` 传 `--ready-fd=<实际 child fd>`,严格等待
`control_ready\nready\nEOF`;父端 writer 在 `Start` 成功后立即关闭,child 提前退出
即表现为 EOF。A 阶段只等待这条 runtime wire(60s),不再用 guest exec 轮询;B/C
在 runtime wire 后继续等 envd `/health`,两者共用一次 90s boot deadline:

每个 phase 的逻辑 SandboxID 保持现有全局唯一值；目录 PathID 固定为 `a`、`b`、`c`。
run-builder 对每阶段调用
`sandbox-ctl run --run-root <BuildRunDir> --base-root <BuildBaseDir>
--path-id <a|b|c> --sandbox-id <logical-phase-sid>`。因此 phase YAML、envd/ctl socket 与
小型 JSON 位于相应 `BuildRunDir/<a|b|c>`，writable diff 位于
`BuildBaseDir/<a|b|c>` 且文件名仍含逻辑 SandboxID。phase exec/snapshot 只用 PathID
定位 ctl.sock；一个 phase 的 sandboxer 退出只删除自己的 RunDir，不会删除 BuildRunDir
或 sibling。最终 node-local Build cleanup 才删除完整 BuildRunDir/BuildBaseDir。

- **A import**(有 fromImage):**空**单盘沙箱——root 即 `builder.diff_template`
  复制出的可写 ext4(无 base 镜像),`launch.placeholder` 锚定;单一 guest runtime
  经 `/opt/sandbox-runtime` 投影出 `flatten-ctl` 与 `mkfs.erofs`。若
  `builder.referer.enabled=true`,guest 先以租户 registry 凭据执行
  `flatten-ctl referer lookup --json --owner <owner> <fromImage>`;lookup 只接受格式有效且
  未过期的 referrer,hit 时宿主校验
  返回的 manifest id 后直接用 `manifest://<id>` 作 base,跳过拉取与展平。lookup
  unsupported/error 时按 `fallback` 继续或失败。miss 时 guest 内
  `flatten-ctl export --output - <subject digest>` 拉取 + 展平,确保 lookup、export 与后续
  writeback 使用同一不可变镜像身份;tarstream 镜像工件经 exec
  stdio 流回宿主 `BuildBaseDir/checkpoint/image.img`。若 lookup 已确认 registry 支持 Referrers 且
  `writeback=true`，只有最终 image policy 本来就要求该 IMG 进入 Manifest store 时，宿主才经
  sandboxer package publisher 得到 `manifest://<id>`，随后在 guest 内执行
  `flatten-ctl referer put --owner <owner> --manifest-id <id> <subject>`。顶层 Sandbox E 是唯一
  image-class root 时，以及 `checkpoint.remote.manifest=true` 时，writeback 被跳过，禁止为了
  referrer 单独创建中间 IMG Manifest。`MANIFEST_KEY` 只在宿主用于 owner token 和 publication，
  不进入 guest referer 命令。
- **B build/materialize**(有 steps 或 source E 时):image source 以 base 镜像为 root
  (本地工件或 `manifest://`)+ builder runtime + 大可写 upper(同一 diff_template)；
  SBX/SNP source 则始终 `sandbox-ctl run --from <E>` 冷启其完整 root graph，并追加新的
  builder upper。source 的 mounts/files/init/workload 在 B 中清空，避免为 materialize 执行一次
  后又保留给成品 Create 重复执行；其 non-boot config 另由共享 cold projection继承。
  **envd 为 app**(构建工具姿态:恒
  `-isnotfc`、不 `/init`、无 token;唯一盘足迹 `/run/e2b` 落在 tmpfs 挂载上,导出
  排除)。`RUN` **经 envd `process.Start`** 逐条执行——与 e2b 自家构建同形:
  `/bin/bash -l -c <cmd>`、按操作用户经 `Authorization: Basic`、`Connect-Timeout-Ms`
  带 step 预算(guest 侧也会到点杀)——上下文为宿主累积的 ENV/WORKDIR/USER,**初值
  灌自 base 镜像 config**(RUN 所见与 docker build 一致;`ARG` 仅做 `${k}` 替换,
  不入镜像;**bash 因此是带 steps/startCmd 构建的镜像契约**,e2b 同款)。步完后宿主
  把累积上下文叠回 base config 写回 guest,`flatten-ctl mountpoint /.kuasar-build`
  (自绑挂载点)+ `export --skip-mounts --runtime-config … --tmpdir /.kuasar-build
  --output - /` 导出新镜像工件流回——挂载点自身与 `/opt/sandbox-runtime` 投影都是
  挂载,被 `--skip-mounts` 排除,导出不自吞、工具链不进镜像。即使没有 steps，E source
  也必须走 B 并导出完整顶层 image，所以成品不引用 source E/root layers。
- **C memory snapshot**(只在 resolved `target={kind:sandbox,memory:true}`):使用与普通
  Sandbox Create/顶层 E 相同的 cold-config projection，以 final image 完整替换 boot。
  image source 普通 cold run；有 source E defaults 时调用
  `sandbox-ctl run --from <E> --replace-boot --config <C0>`，绝不 `--restore S`。
  **生产 e2b runtime** 的 envd 姿态随部署([Proxy §7](node-proxy_zh.md#7-mmds))，`/init` 注入 register
  `envVars` 和可选 instance credentials（此后 RPC 携 `X-Access-Token`）。startCmd 可选，
  **经 envd 启动**(e2b 默认身份 `user`、`/home/user`),流挂至就绪后断开——envd
  不因断流杀进程，进程以 **envd 管理进程**身份冻入快照。readyCmd 以 2s 间隔轮询至
  成功(预算 `ready_timeout_sec`)；没有 readyCmd 时，无论有无 startCmd，都执行受 Build
  absolute deadline/取消约束的固定 20 秒等待。C 没有 startCmd 也合法。随后先断
  startCmd 流(已败则构建失败)，再 `sandbox-ctl snapshot --path-id c
  --output <BuildBaseDir>/checkpoint` 出本地快照 bundle。

**两类 guest 信道,刻意分离**:e2b 语义命令(steps/startCmd/readyCmd)走 envd,
与 e2b 自家模板构建逐项同形;平台机制(flatten-ctl 拉取/导出、运行时配置注入、
工件流回)走 `sandbox-ctl exec`——任意 rootfs 可用、裸 stdio 接力,
不依赖镜像 userland。

**fromTemplate**:base 来自既有 canonical artifact。IMG 已是 image carrier：无 steps 可
直接复用，有 steps 才跑 B。SBX 由 task-local reader 打开 E；SNP 只打开 S 的根 config、
读取 `sandbox_ref` 定位 E，随后与 SBX 完全相同。不会向任何 phase 传 S，不读取/预取
memory payload，不把 memory from-refs 纳入 closure，也不存在 `run --restore`。E 的完整
`PortableSandboxConfig` 留在 run-builder，用于 B materialization 和 sandbox target 的
non-boot defaults；来源含任意 `boot.disks[]` 时第一版明确拒绝。

fromTemplate 与 fromImage 互斥；所有 SBX/SNP source 即使没有 steps 也必须跑 B。因此
fromTemplate 只有“无 steps 的 IMG”可零 VM 复用原 image。E metadata 中
`e2b.start_cmd`/`e2b.ready_cmd` 仅对 e2b profile 作为缺省（trigger 非空值优先）；显式 Image
target 清除继承命令且不执行，显式 Image 与 trigger 命令则同步拒绝。source E 的 network
summary 在 host Attach 前 strict 解析并按字段继承，优先级为
**当前 Build 显式 NetworkSpec > source E NetworkSpec > 当前 profile/node 默认值**；
IMG source 没有 artifact metadata 通道，也不从 retention-bounded Build row 回填。

task-local ref-location mapping 只保留选中 E 及其 B 冷启所需 root refs；S memory-only Bundle
locations和 source S ref 被过滤。B 导出的 image 是完整新顶层，之后顶层 E assembly 或 C 的
`--replace-boot` 都只安装该 image，所以最终图不再携 source E/root/writable refs。若 C 的
final image 按 policy 发布成 located Bundle，builder 把该次 publication 的实际 name→directory
mapping 加入 C argv；checkpoint publication 继续携带所有仍被 graph 引用的 mapping。

**COPY/ADD step(构建上下文经对象存储直传)**:COPY 的本质是"把一份 tar 摊进
rootfs"——文件系统操作,不是 e2b 进程语义,故走 sandbox-ctl exec + flatten-ctl(不经
envd),且**镜像无需自带 tar/gzip**(flatten-ctl 纯 Go 解包,scratch/distroless 亦可
COPY)。三段:

1. **上传协商(files 端点,[§1](#1-build-api))**:客户端先 `GET /templates/{tid}/files/{hash}`,
   服务端校验归属后回 `{present, url}`;`present=false` 时客户端把 **gzip(tar)** 上下文
   经 presigned PUT **直传桶**(字节不过控制面,e2b SDK 同款裸 PUT)。对象 key =
   `{prefix}/files/{aaaa}/{bb}/{uuid}/{hash}`(uuid=tid 的 uuidv7 部分;aaaa=uuid[0:4]
   ~50 天、bb=uuid[4:6] ~5 小时,时间分桶散列前缀 + 便于按日期 GC)。隔离靠端点归属
   校验:只为属主签当前 tid 路径的 URL,桶私有、客户端无凭据。
2. **触发校验**:trigger 时每个 COPY 的 (tid,hash) 经 HeadObject 确认已上传——未配
   `files_storage`→501,未上传→400,失败快。
3. **构建期解包**:`BuildSpecFor` 为每个 COPY 预签 GET URL(TTL=total+5m,绑构建全程)。
   run-builder:`http.Get`(平台取数,宿主侧)→ 宿主 gunzip → `sandbox-ctl exec
   --stdin-from <tar> -- flatten-ctl tar extract --dense [--chown O][--chmod M]
   <rule>`。rule 由 (src,dst)+context 条目派生(整根 `:dst/`、目录前缀 `src/:dst/`、
   单文件 `src:out`;dst 相对则按累积 WORKDIR 解析,默认 `/`);owner 缺省 `0:0`
   (Docker 语义,`--chown` 覆盖,名字在解包根的 /etc/passwd 解析);`--dense` 守稀疏
   铁律(源码无权威洞元数据,零即数据)。单源 COPY(e2b executor 同限)。

**对象存储(`builder.files_storage`,[§3](#3-build-配置))**:S3/OBS;serve 只 presign + HEAD,
唯一 aws-sdk 落点;本地/单机无云对象存储时指向 versitygw(`make -C guest-runtime/native-deps versitygw`)。force_path_style 默认 false(虚拟主机式;versitygw/minio 置 true)。

**发布与 target matrix**:

发布计划在 target 解析完成后一次性建立，不从 worker 的 result field 反推。Image 类包括
最终 IMG、`sandbox,memory=false` 的顶层 E，以及 Phase C 使用的 immutable IMG；checkpoint
类包括 Phase C 增量 E、S 及其 memory/disk 增量层。顶层 E 和增量 E 都是标准 Sandbox E，
但只有前者携带完整 EROFS payload，因而采用 image policy。

| 配置 | Image target | Sandbox, memory=false | Sandbox, memory=true |
|---|---|---|---|
| parent 空，`manifest=false` | IMG → Manifest store | 顶层 E → Manifest store | IMG、EΔ、S → Manifest store |
| parent 非空，`manifest=false` | IMG → Manifest store | 顶层 E → Manifest store | IMG → Manifest store；EΔ/S → named location |
| parent 非空，`manifest=true` | IMG Bundle → named location | 顶层 E Bundle → named location | IMG Bundle → named location；EΔ/S → named location |
| parent 空，`manifest=true` | 配置非法 | 配置非法 | 配置非法 |

`checkpoint.remote.manifest=true` **不表示上传 Manifest store**；它表示把 manifest-backed
image 类逻辑制品直接物化为 checkpoint named location 中的 single-root Manifest Bundle。
`checkpoint.mode=local|bundle` 只选择 checkpoint 类的 role-specific tarstream 或 Snapshot
Bundle carrier，对 image 类始终是 Bundle。`ref_location_parent` 在 `manifest=false` 时不改变
Image target 或顶层 E 的 Manifest-store destination。

- **Image**：当前 final image logical source 直接进入 sandboxer typed publisher。
  `manifest=false` 返回 `manifest://IMG`；无 steps 的既有 IMG 可保留 identity fast path。
  `manifest=true` 必须经过 Bundle publisher，既有 `manifest://` 或 located Bundle 也不得绕过
  统一验证/内容地址复用，最终只返回 located `image_ref`。
- **Sandbox + memory=false**：直接打开当前 final image carrier——digest-qualified
  `file://<BuildBaseDir>/checkpoint/image.img@digest:...`、`manifest://` 或 located image Bundle——
  物化 image defaults 和 canonical portable runtime config，并由 sandboxer 组装标准 direct-EROFS
  self-layout Sandbox E logical source。该 source 直接进入最终 Manifest 或 Bundle publisher；
  不发布独立 IMG、不调用 `manifest-ctl store`、不经 Manifest fetcher 回读本地 IMG，也不在
  RunDir/BaseDir staging 完整 `.sandbox`。此 target 不启动 C，不执行 start/ready，只返回
  `sandbox_ref`。
- **Sandbox + memory=true**：先按 image policy 发布 final IMG，并把返回的 portable ref 设为
  C replacement boot。located IMG Bundle 的实际 location mapping 进入
  `sandbox-ctl run --from E --replace-boot`；C cold run 后捕获 EΔ/S，再按 checkpoint policy
  发布，只返回 `snapshot_ref`。EΔ 保留该 IMG ref；local tarstream 与 Snapshot Bundle 都不重新
  物化或复制 portable image Bundle。不存在来源 S memory restore 或 C disk-only 分支。

每个 named publication 都以裸 build ID 作为 name,因此同一 Build 的全部发布收敛到同一个
BuildID 目录;每个 located ref 自带自己的 name。single-root image
Bundle 不创建 `.image`/`.sandbox` tarstream或 BuildID/SandboxID semantic alias，只提交
`<manifest-root>.bundle`。提交使用 target-directory 临时文件、root-last finalize、完整校验、
独占 final、检查完整 write/Close 并重新打开校验 final path；publisher 明确不 fsync fresh final
或 parent directory，成功只代表 logical publication，不保证掉电持久性（[实现](https://github.com/kuasar-sandbox/sandboxer/blob/main/pkg/artifact/location_target.go)）。并发/重试只复用严格验证通过的同-key regular final，损坏、
symlink 或不匹配 final fail closed。失败不返回 terminal ref；现有 Build finalizer仍清理完整
RunDir/BaseDir。

顶层 E 与 C 初始 C0 都由同一个 `BuildColdConfig` 投影产生：source E non-boot defaults <
register Create options < builder managed start/ready；resources/network/boot/launch/env/mounts/files/
init/metadata 语义一致。start/ready 与最终有效 `NetworkSpec` 写入 E metadata，使 canonical
TemplateID 在 Build row TTL 删除后仍完全自描述；未声明 hostname 时记录普通 sandbox 默认值，
绝不记录 `build-<id>`。

**构建日志流(journald → status API → SDK on_build_logs)**:SDK 按需消费 `build` 流,
不包含 `console` 或 `sandbox-ctl` component 诊断。每个 phase 的完整目标携
`KUASAR_BUILD_ID`/`KUASAR_RUN_ID`;target 构造与独立输出身份见
[Build journal 身份](node-journald_zh.md#build-输出)。run-builder 经 pure-Go `go-systemd/journal`
写里程碑(`import: pulling…`、`step N: RUN…`、`template: ready`、`uploading…`),
并把保留的 RUN/startCmd envd stream 回放到同一汇(envd 自身无 journal)。
sandbox-ctl 的 phase app stdout/stderr 与四个 journal-routed flatten/export/referrer exec 调用点
使用完整 `build` target;flatten 不带 `--no-progress`,所以仍可见 layer count/flatten 进度。
文件/制品输出与 stderr error-tail capture 保持独立。流水线失败补写 `build failed: <err>`。
日志不经临时文件;发送与存储仍是 best-effort,关闭单元限流不保证零丢失。
**读取**:status([§1](#1-build-api))按 `?logsOffset`(已读条数)分页;
serve `journalctl KUASAR_BUILD_ID=<bid> SYSLOG_IDENTIFIER=build --output=json`
取 MESSAGE/PRIORITY/时间戳,PRIORITY→e2b level
(≤3 error、4 warn、≥7 debug、余 info),切片 `[offset:]` 回
`{logs[], logEntries[{timestamp, level, message}]}`;尽力而为(非 systemd / 无日志 →
空,不阻断 status)。CGO-free:sdjournal 读需 CGO,故 journalctl 子进程读、
`go-systemd/journal` 纯 Go 写。**失败语义**:流水线失败 ⇒ `reason.message` 保持通用
(`build failed; see build logs`),详情已在日志流里(零额外机制);基础设施失败
(流水线未起或未回传结果)无构建日志,`reason` 直陈宿主侧错误。

**fromImage 的来源**:trigger body 显式给出;或(e2b CLI 在客户端 `docker build` +
`docker push` 到约定名、trigger 不带镜像引用的工作流)由 `builder.image_uri_mask`
替换完整字符串中的 `{templateID}` 与 `{buildID}` placeholder（例如 `registry/repo/{templateID}:{buildID}`），
不会自动追加路径；掩码须与 CLI 侧 `E2B_IMAGE_URI_MASK` 一致,
且**须从构建沙箱内可达**(拉取在 guest 内:本机 registry 须绑非环回地址、按
vswitch mgmt VIP 寻址;第三方 registry 经 NAT 出网)。两者皆缺则 trigger 报错。
配本机/私网 registry 时设 `builder.insecure_registry`、`builder.platform`。

**registry TLS**(HTTPS + 内部/自签名 CA 场景):`--insecure` 只切 URL scheme(允许
`http://`),**不影响 TLS 证书校验**;HTTPS + 内部 CA 需用 flatten-ctl 的 TLS 配置能力。
registry TLS 是**单次 Build 的信任策略**,经 register-time 的 `X-Kuasar-Sandbox-Builder`
头传入(`builder.registry.tls`),不进 Node 配置:

- `ca_bundle_pem`(内联 PEM 文本,≤ 16 KiB,须含可解析 X.509 `CERTIFICATE` 块)或
  `insecure_skip_verify: true`(跳过校验),二者互斥;空 `tls` 被拒。
- 只在 register 时设置;trigger 时带 `builder.registry` 直接 400。
- 仅 `fromImage` 构建可用;`fromTemplate` + `registry.tls` 被拒。
- 与 Node `insecure_registry`(plain HTTP)互斥:同 Build 同时配置二者被拒。
- builder 把 PEM 与生成的 flatten 配置 YAML 投影进 **Phase A import sandbox** 的只读文件
  (`/run/kuasar-build/flatten/registry-ca.pem` + `/run/kuasar-build/flatten/config.yaml`,
  `mode 0444` + `read_only`,`/run` tmpfs 不落 Build 根盘),import 阶段的
  `export`/`referer lookup`/`referer put` 三处 flatten-ctl 调用追加
  `--config /run/kuasar-build/flatten/config.yaml`;flatten-ctl 据此把 CA **追加到系统根证书池**
  (非替换)后构建带 CA 的 TLS transport。不进最终模板 metadata,不被其他 Build 继承。

**两级准入**:Register 在 SQLite 同一事务内按 count/CPU/memory/storage 检查
`builder.admission.registration`,插入 immutable definition 并占用;Trigger 只做
`registered→waiting`。scheduler 按 `waiting_sequence`(成功 Trigger 事务的持久提交顺序) 稳定 FIFO,在单条持久
事务中按 `builder.admission.execution` 建 claim;不足保持 waiting,到 `queue_ttl` 后持久终态。
claim 后先持久绑定 exact run-id,最后发布 assignment. execution CPU/memory 保留为聚合
准入上限;storage V1 为 admission-only. `node-ctl builder status` 和 metrics 暴露配置, 持久用量, headroom, 队列与
拒绝/过期计数。旧 `max_concurrent/cpu_quota/memory_max/vcpu/memory` 配置直接拒绝。
两处原子准入写入也在无限维度拒绝 aggregate int64 溢出, 保护账本算术, 不添加配置资源上限.

每个实际运行的 A/B/C phase 使用独立 SID，通过普通 `sandbox-ctl run` 的 controller
Admit/heartbeat/Release，并在 teardown/Release 完成后才进入下一阶段。A/B execution VM
resources 只从不可变 `Build.Resources` 派生；register Create resource 则以 source E portable
capacity（如有）为默认、供最终 Sandbox E/C0 使用，Image target 为精确零值且不必解析。
两者不互相推导。Build.Resources 本身不进入 nodectl，因此 active phase 只出现一条普通
Sandbox reservation，不存在双重记账；`target=sandbox,memory=false` 没有 C reservation。

ready/error 是有保留期的 Build 历史. registration usage 在 cancel/delete 意图提交或正常终态转换时释放.
终态事务写入 `finished_unix`. 执行过的 Build 先完成 unit/cgroup、network、runtime/result 与
BuildRunDir/BuildBaseDir 清理, 再由终态提交释放 execution claim. 完全清理且无 claim 的行可立即
显式删除, 或在 `builder.terminal_ttl` 到期时由有界 reaper 删除. status、注册 transient ID、name/alias
与本机 list 随行消失; 已返回的 canonical TemplateID 用于 Create 或 `fromTemplate` 均不受影响.

**镜像拉取凭据**(按优先级解析,无凭据则匿名):

1. **任务级 pull token**:SDK `api_headers` 头 `X-Kuasar-Pull-Token`,值为
   `e2b-key-ctl seal-pull-token` 用租户 manifest_key 派生密钥封装的 `kpt_` 令牌,
   serve 以该租户存量 key 解封;
2. **SDK 明文**:trigger body `fromImageRegistry{username, password}`;
3. **租户默认**:`manifest_keys.registry_auth_enc`(与完整凭据对绑定的 docker config.json;
   `manifest-key add --registry-auth` 或 `--registry-username/--password/--token`
   自动组装 catch-all `*` 条目;按 fromImage host → `*` 匹配取条)。

解析结果加密存 `builds.registry_auth_enc`,构建时解出注入
`FLATTEN_REGISTRY_{USERNAME,PASSWORD|TOKEN}`(flatten-ctl `pkg/remote` 读取,token
优先),**经 exec env 进入 import 阶段的 guest——入 guest 的只有 `FLATTEN_*`,
`MANIFEST_KEY` 永不入 guest**。租户 roots/pull credentials 仅加密存库与运行期 env，不进入 phase YAML；调用方 workload
env/files 自身可含敏感值，应与这些平台根凭据区分。

**不支持**:多源 COPY(e2b executor 亦只取 src+dst)、step 级缓存(`force` 字段
接受但忽略,总是全量执行)、服务端 Dockerfile 解析(CLI 已在客户端展开为 steps)。

## 6. 持久化、恢复与保留

Build 使用同一节点 SQLite,完整专属表为:

```text
builds         build_id PK, template_id(transient-…), persist_id(<profile>-<kind>-<base64url-ref>),
               api_secret_hash, api_secret_enc, manifest_key_hash, manifest_key_enc,
               profile, kind, from_image, from_template, start_cmd, ready_cmd, steps_json,
               status(registered|waiting|building|ready|error), reason, run_id,
               names_json, aliases_json, registry_auth_enc,
               registration_image_repo, registration_registry_auth_enc,
               registration_mmds_routes_digest, registration_mmds_values_digest,
               registration_request_digest, cluster_group,
               resources_cpu(milli-CPU), resources_memory(bytes), resources_storage(bytes),
               metadata_json, builder_json, instance_config_enc,
               waiting_unix, waiting_sequence, execution_claimed, execution_claimed_unix,
               phase, phase_sandbox_id,
               runtime_vswitch_port, runtime_floating_ip, runtime_port_mac,
               runtime_envd_access_token_enc, runtime_prepare_json,
               execution_result_json, created_unix, finished_unix,
               cancel_requested_unix, delete_requested_unix
build_mmds_route_secret_values
               build_id PK/FK builds(build_id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
```

`builds` 是 registration/execution 两级准入与执行状态的持久真相，也是 retention window 内的
status/index/alias；它不是长期模板 catalog（§2）。


`resources_*` 是不可变 Build execution/admission resources；最终 Sandbox resource 属于
`metadata_json` 中的 Create config，二者不互相推导。`instance_config_enc` 整体加密保存
register `envVars`（sandbox target 的 portable launch env）以及 secure/credentials 等只供
memory Phase C 的实例输入；读取后这些类别仍在 target validation/portable projection 前分离。
不为旧 Build row/schema 增加双 reader。启动时把 `instance_config_enc` 作为
#300 Build schema 的 clean-break marker；缺少该列的开发数据库会被明确拒绝并要求重建，
不会原地推导、回填明文或延迟到首次 Build 查询才失败。
`execution_claimed` 及 runtime/phase 字段使重启后可重建
用量并先收敛活单元再释放 claim。`runtime_prepare_json` 通过启动时的 additive SQLite migration
加入既有数据库；它与 runtime port在同一 exact-run CAS 中提交，终态/cleanup一并清空。


启动先幂等初始化 schema(包括 MMDS value 表),再以 `instance_config_enc` 检查 #300 clean-break marker。
缺失 marker 的不兼容开发数据库在 Build row 查询与后续 additive column migration 前被拒绝,
不得尝试明文回填或双读。已带 marker 的数据库独立增加 Build `runtime_prepare_json`、
`registration_request_digest`、`finished_unix` 与 Sandbox `dead_unix`。
已有终态时间为零的记录从迁移时刻获得完整 retention 窗口;不是要求每次启动都重建数据库。
MMDS 每 owner 至多一行 secretbox ciphertext,共用 [Node 凭据模型](node_zh.md) 的 AAD/事务/CAS/清理契约。

Builder 在同一次 startup gate 内对账，所有 live owner重建完成后才允许 task bootstrap/result
跨过 `buildRecoveryReady`：

- `run_id`已绑定且port为空是合法 preparing。新conductor收养同一live run-builder，重建
  completion owner；artifact task可重取bootstrap或重交同一summary，conductor不读工件；
- port与合法`runtime_prepare_json`同时存在是prepared/pipeline-running。新conductor从其中冻结的
  network/resources重建final BuildSpec，相同digest重试不再次attach，也不受当前node defaults漂移；
- port存在而preparation缺失、损坏或schema未知时先fence exact unit，再detach并终态失败，不能
  猜测一份可能与现有port不一致的配置；
- 已持久接受的`execution_result_json`优先于preparing/prepared重建，先fence worker后按该结果
  收尾，不再读取snapshot；task/host prepare、pipeline与结果上报共用原
  `execution_claimed_unix + total_timeout`截止时间，随后60s只保留给unit fencing和host cleanup，
  重启不重置或延长任一预算。

Build row 不存目录字段；Reconcile 只用 BuildID 与当前 RunRoot/BaseRoot 重新派生
BuildRunDir/BuildBaseDir. 任何终态都先 fence exact unit/cgroup, 等待动态阶段资源预留释放,
detach port 并删除两个目录. 清理完成前保留持久归属; terminal commit 再原子清空 RunID、
execution result 并释放 execution claim. phase 子进程的自清理不是最终正确性依据.节点级数据库、config socket 与 runner pidfile 不位于对象目录内，不受 Sandbox/Build
`RemoveAll` 影响。

共享 SQLite 基础设施与终态 reaper 由 [节点可靠性](node_zh.md#14-可靠性) 维护;Build schema 与恢复由本节维护。Build 行是有保留期的执行/status/alias 记录，不是永久模板目录；已发布 canonical template ref 不依赖原 Build 行。

### 6.1 取消、预算与恢复

registration usage 只统计 status IN (registered, waiting, building) 且两个意图时间均为零的行. execution usage 统计全部 execution_claimed=1 的行,独立于 status 和意图. 数量和 CPU/memory/storage 向量、真实准入 SQL、管理、metrics 和节点上报采用相同口径. waiting 数量与 oldest waiting 只包含仍可执行的记录. 接受取消/删除的事务提交即释放注册容量;exact unit 停止、本地清理和 PutBuildTerminal 成功后才释放执行容量. 正常成功、失败和总超时仍自动清理. 容量变化以可合并通知唤醒既有 FIFO pool 和节点用量上报,周期检查继续兜底.

现有进程内执行 owner 在 claim 发布前登记. 意图阻止新的 Trigger/claim/assignment/bootstrap/prepare/phase/result 推进,通知唯一 owner 停止 exact RunID unit,覆盖阶段 VM 和辅助进程. host prepare 先退出或回滚未提交资源,再由同一 owner 清理精确网络、运行入口、BuildRunDir 和 BuildBaseDir. unit 停止后,清理读取持久化 PhaseSandboxID,等待既有资源控制器释放对应预留. 若 runtime 字段是最后的归属,则保留到两个目录删除完成,由 PutBuildTerminal 条件提交释放. Build 锁只保护短身份及数据库/事件提交,不等待 unit stop 或目录删除. cleanup CAS 在取消后仍有效. HTTP 断开不撤销已接受意图;清理的外部操作使用独立有界上下文.

取消和结果接受按持久先后裁决:cancel-first 拒绝新结果,以 `build cancelled by user` 结束;result-first 保留已接受的成功/失败及相同结果重放,继续停止和清理. direct Image 复用采用同一终态条件. 旧回调不能 upsert 复活已删行或影响后继 transient 身份. 数据库提交先于观察通知;真实硬删除后才发 BuildDelete. error 导致观察集合移除是另一事实.

清理完成后 PutBuildTerminal 释放 claim,提交后立即通知容量变化,再按既有注册/retention 锁单独条件硬删除. 等待终态发布的注册重放不能重新插入已删除身份. 最终 DELETE 失败保留 delete 意图,不重新占执行预算. 立即重试、既有生命周期有界补偿和启动恢复继续处理,不等 terminal_ttl. Stop/unit 存活状态回读/detach/目录/终态失败保留必要归属以重试. 启动先处理 cancel/delete 再收养流水线:停止活 unit、清理已退出 unit、直接删无归属 marked 终态行,不为已取消任务重读源产物或重置原执行截止时间.

两个意图列通过加法迁移增加,均为 INTEGER NOT NULL DEFAULT 0,既有行从零开始. Registry 分别保留不可变注册 transient ID 与结果 PersistID,以 group + transient ID 定位原节点. 缺少 transient 字段的旧 owner ref 只依据仍匹配的 route/binding 补齐. provisional 插入、注册 ACK/拒绝及重放 ref 更新均比较原身份和当前 revision,迟到操作不能覆盖后继记录. 节点完整同步在既有 Registry binding 下更新投影,并在丢失 BuildDelete 后删除缺失记录. 它不重建已丢失的 Registry 归属分片(见 cluster_zh.md §13). Router 原样转发 Query 与 Builder Header;节点 ownership 是最终权威. 节点不可达、投影不完整或权威查询失败返回服务错误,不能改派或猜测删除.

按现有版本协调一起升级 node/router/Registry writer. 回退到不理解意图的旧 writer 前,停止接受新的取消/删除,由当前版本收敛全部待处理操作. 有未完成意图时,加法 schema 本身不能保证安全回退. 不增加长期双 writer、新任务表或兼容服务. 记录删除后已发布 canonical img/sbx/snp 引用、已有 Sandbox、下游 fromTemplate 和共享产物的其他 Build 仍可用;绝不删除远端内容.

托管沙箱的 checkpoint 选择性清理不在 Build 阶段之间执行。后续 phase 可能仍读取前一阶段的 S/E，因此阶段输入与任意 FileSink 输出继续由原 owner 持有。Build 终态收尾先 fence exact runner 并释放阶段资源，再由既有 BuildBaseDir finalizer 删除所属 checkpoint 目录。失败、取消和重启仍保留该 finalizer 的持久责任，不删除共享发布 ref。

### Checkpoint 发布报告

Builder 对 checkpoint S 调用 `sandbox-ctl publish --json --quiet`，接受 checkpoint
前验证且只接受 `snapshotRef`、最终 `sandboxRef` 和 `removedRefs`。根必须 portable
且角色正确；差集必须有序、唯一并采用安全的公开引用。无效/尾随 JSON、缺少根和 stdout
污染都会使发布失败，进度 stderr 单独处理。Builder 继续返回规范 `snp` TemplateID；
Export 新增 HTTP 报告字段见[节点发布与迁移](node_zh.md#814-publishtemplate-与-migration)。
直接发布 image/Sandbox 的 `PublishSource` 调用保持既有 role/ref 契约、source 所有权和
流式行为。报告不增加 payload 缓存、额外 staging 文件、GC 或删除权限。
