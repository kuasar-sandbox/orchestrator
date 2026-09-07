[English](node-build.md) | [简体中文](node-build_zh.md)

# 节点模板构建

本篇完整定义 Build 注册、目标选择、不可变执行资源、任务准备、顺序阶段执行、发布及恢复。[节点规范](node_zh.md) 维护共享进程、socket、凭据与节点恢复边界；[Registry 规范](cluster_zh.md) 维护集群构建意图和放置。

<a id="42-控制面模板构建-api"></a>
<a id="42-control-plane-template-build-api"></a>
## 1. Build API

实现 e2b **v2 build system** 的端点族(SDK `Template.build` / CLI 走它);构建语义与
资源池见 [§5](#5-按目标执行与发布)。

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| register | `POST /v3/templates` → 202 | body `{name, tags, profile?, cpuCount, memoryMB, metadata?, envVars?, secure?}`;CPU/memory 只定义不可变 **Build.Resources**,绝不改 Sandbox capacity。`X-Kuasar-Sandbox-Builder.resources` 可声明同值并补 storage;同维度不等即 400。Builder `target` 是 register-only immutable 定义；`X-Kuasar-Sandbox-Resource` 则定义最终 Sandbox template resources，不供 A/B 使用。`profile∈{e2b,bare}`,省略取 `e2b`;响应暴露 requested `target`（省略即 auto） |
| trigger | `POST /v2/templates/{tid}/builds/{bid}` → 202 | body 兼容 `{fromImage, fromTemplate, fromImageRegistry{username,password}, steps[], startCmd, readyCmd}`;只允许一次 `registered→waiting`。兼容的 `cpuCount/memoryMB` 仅可断言等于注册值,放大或缩小均在 credential/COPY/queue 副作用前 400。Trigger-time metadata、Builder/Resource 及其它通用配置 header 全部拒绝;execution 不足时留在固定节点 FIFO waiting |
| status | `GET /templates/{tid}/builds/{bid}/status` | 回基本 SDK 字段、requested `target`、终态 derived `kind`，及规范化 `resources{cpuMilli,memoryBytes,storageBytes}`、`executionClaimed`、`runID`、`systemdEnforcement`、`storageEnforcement` 和当前 `phase{name,sandboxID}`;进行中 SDK status 仍统一为 `building`,内部 phase/claim 不丢失 |
| files | `GET /templates/{tid}/files/{hash}` → 201 | COPY context 上传协商:`tid→build→归属`校验后回 `{present, url}`——present 即对象已在桶(客户端跳过上传),url 为**直传桶的 presigned PUT**(字节不过控制面);未配 `files_storage`→**501**,未知/非属主 tid→**404**。详见 [§5](#5-按目标执行与发布) |
| list | `GET /templates` | 本租户 ready 模板;`templateID` 列为持久 id,同时回不可变 `profile`、requested `target` 和 resolved artifact `kind` |

<a id="44-templateid-与模板形态transient--persist无-templates-表"></a>
<a id="44-template-ids-and-transientpersistent-forms"></a>
## 2. 模板 ID 与工件权威

```
persist  templateID = <profile>-<kind>-<base64url(canonical-portable-ref)>
                                            profile∈{e2b,bare}; kind∈{img,sbx,snp}
transient templateID = transient-<uuidv7>       构建注册期临时句柄,build 完即弃
```

- **持久 id 自描述**:payload 是 `manifest://<key>` 或
  `file://<digest>.image@digest:<digest>@location:<name>`；encrypted carrier 使用 `@hmac:`，
  Bundle 用 `@manifest:<root-key>`；运行期解析 profile（选择共享 runtime 的 guest 行为）、kind(img = image cold,sbx = Sandbox E cold,snp = Snapshot S memory restore)
  和 canonical portable ref。snp 可在 Connect 时显式选择 cold,但 TemplateID 的缺省仍是 memory。
  local file ref、宿主绝对路径、非 canonical ref 或 artifact kind 不匹配均拒绝。
- **临时 id** 由注册生成;构建完成后持久 id 写入该构建的 names + aliases 一并返回,
  之后只用持久 id。Build status、临时 id、name/alias 与本机 list 仅在终态 Build row 的
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
| `units.builder_pool_size` | `0` | 预启动 idle Builder 数;有 execution CPU/memory 聚合上限时必须为零。共享 units.dir/install/pool_wait_timeout 见 Node §3;完整 unit 与 claim/readback 顺序见 §4.1 |
| `builder.admission.execution.max_builds` | `2` | 同时持有 durable execution claim 的 Build 上限 |
| `builder.admission.execution.resources.{cpu,memory,storage}` | 不限制 | execution 的聚合资源向量;CPU/memory 同时施加到 `sandbox-builder.slice`,storage V1 仅准入记账 |
| `builder.admission.registration` | 完整继承 resolved execution | 所有非终态 Build 的注册上限;显式块不做字段级继承,且同一有限维度不得小于 execution |
| `builder.registration_ttl` / `.queue_ttl` | `1h` / `30m` | 未 Trigger 的 registered Build 与 waiting Build 的持久超时;终态可查询并释放 registration usage |
| `builder.terminal_ttl` | `24h` | 已完成 cleanup、无 execution/runtime/result owner 的 `ready/error` Build 历史保留期；必须为正 Go duration |
| `builder.insecure_registry` | `false` | 经明文 HTTP 拉取 base 镜像(dev/本机 registry) |
| `builder.platform` | 空 | 拉取平台,如 `linux/amd64` |
| `builder.image_uri_mask` | 空 | 客户端推送镜像的命名约定(含 `{templateID}`/`{buildID}` 占位,须与 e2b CLI 的 `E2B_IMAGE_URI_MASK` 一致);trigger 缺 `fromImage` 时据此推导;**须从构建沙箱内可达**——拉取在 guest 内进行([§5](#5-按目标执行与发布)) |
| `builder.referer` | 关 | fromImage import 的 OCI Referrers cache:`enabled` 默认 false;`fallback`/`writeback` 默认 true;`desc` 为公开 owner descriptor(启用时必填);`key` 为空则等于 desc;`validity` 为可选 Go duration。build 可经 `X-Kuasar-Sandbox-Builder` 进一步禁用 lookup/writeback,不能越权启用([§4.6](node_zh.md#46-沙箱配置传递链)、[§5](#5-按目标执行与发布)) |
| `builder.diff_template` | – | 构建沙箱可写盘的预格式化 ext4(拉取缓存 + steps 增量 + 导出 scratch;稀疏文件,建议 ≥ 最大预期镜像的 3 倍) |
| `builder.{pull,step,ready,total}_timeout_sec` | `600`/`600`/`120`/`1800` | 阶段超时:guest 内拉取+展平、单条 RUN step(经 `Connect-Timeout-Ms` 同步到 guest 侧)、readyCmd 轮询预算(2s 间隔;缺省 readyCmd = `sleep 20`)、整个构建(单元不设置 `TimeoutStartSec`;另有独立的 60 秒 fencing/宿主清理窗口,见 §6) |
| `builder.files_storage` | 空 | COPY 构建上下文的 S3/OBS 对象存储(子键 `endpoint`/`region`/`bucket`(必填)/`prefix`/`access_key`/`secret_key`/`force_path_style`/`presign_expiry`);空 = COPY 回 501。serve 仅 presign + HEAD;custom Runtime credentials provider 优先于静态 YAML/AWS 默认链、支持 session token/expiration/refresh 且失败不回退;`force_path_style` 默认 false(versitygw/minio 置 true);`presign_expiry` 默认 1h(PUT;GET 用 total+5m)。本地/单机无云对象存储用 versitygw([§5](#5-按目标执行与发布)) |

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

构建端点额外接受 **build-only** 命名空间 `kuasar-sandbox.builder`,对应请求头
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
见 [Node §4.6](node_zh.md#46-沙箱配置传递链)。Register 始终拒绝 `kuasar-sandbox.identity`、
租户提供的 node-managed cluster metadata、`restore` 与 `autoPauseMemory`。
普通 Build runtime 输入存 `builds.metadata_json`,build-only 输入存 `builds.builder_json`;
只服务本次执行与制品生成,不是 canonical TemplateID 的长期 metadata lookup。
Trigger 不能覆盖注册 metadata、Builder/Resource 或其他通用配置头;非空通用 metadata/header 被拒绝。

- 显式 Image 拒绝归一化后仍存在的 Sandbox resource/traffic/network/launch/init/mounts/files/metadata/checkpoint/MMDS namespace,
  以及非空 `envVars`、`secure=true`、非零 credential override 或显式 MMDS routes/secrets。
  普通 metadata label 与 Build execution resources 仍允许;空 `envVars`、`secure=false` 本身不构成拒绝条件。
- 顶层 Sandbox E 接受 portable Create 配置(包括 env),但拒绝 traffic、非零 credentials、
  显式 MMDS routes/secrets、`secure=true` 与非空 checkpoint 等 instance/action-only 输入。
- 只有 resolved `sandbox,memory:true` 接受后一类输入。`instance_config_enc` 加密 env/secure/credentials;
  traffic/checkpoint/MMDS routes 存 metadata,MMDS values 使用独立加密表。实例/动作字段不进入 portable E/S;
  portable env 保留自身投影规则。空对象不是统一豁免:例如 `resource:{}`、`traffic:{}` 仍会被 Image 拒绝;
  空 checkpoint 与零值 credential 经各 namespace 归一化处理。
- Auto 必须等来源 E 与 effective start/ready 解析后才校验目标兼容性。


Build execution resources 与最终 Sandbox resources 独立,不互相默认、比较或推导;
Trigger `cpuCount`/`memoryMB` 只能断言不可变 Build resources。

Build Register 的 routes 只服务
可能运行 memory Phase C 的 synthetic builder sandbox；显式 Image/顶层 Sandbox E target
在注册期拒绝它，auto target 则在 task-local target 解析后 fail closed。initial values 以 build owner 加密保存。build 终态事务同时
从 `builds.metadata_json` 删除 routes namespace 并删除 value blob,因此两者都不进入最终
template/snapshot/image。Build Trigger 不接受 MMDS 覆盖。

Build register
只在 resolved `sandbox,memory:true` 时接受 `kuasar-sandbox.checkpoint` policy并用于 Phase C capture，且不写入最终 portable
config。

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
  当前 BuildTask schema 为 v5（v5 分离 checkpoint-location 与 image-class Bundle publication，
  删除旧 broad `publish_location_parent`；v4 增加 register-time requested target，并把 SBX/SNP
  source 收敛为通用 cold Sandbox E；v3 以 `run_dir`/`base_dir` 替代混合语义 workdir，
  `checkpoint_mode` 自 v2 起必需),sandbox ArtifactPrepare
  schema 独立为 v4（v4 增加 Build-only image Bundle publication preflight；v3 增加只供 Build source 使用的 image-config 读取 capability、同一
  Bundle 内 E/image 的 carrier scope，以及 portable allocatable/deflate resource defaults；v2 以 typed E/S、durable
  LaunchMode 和 bounded network/disk summary 取代旧 v1 Snapshot-only wire),两个版本号独立演进；bootstrap 和 build prepare 的版本不匹配均在读取
  secret-bearing provider 前返回 400,
  防止旧 task 静默忽略必需的 publication 语义。参见 [build_task.go](../internal/configsock/build_task.go)
  与 [sandbox_task.go](../internal/configsock/sandbox_task.go)。认证后返回 task env 与 exactly one of
  `Final|Prepare`。Final 是
  **BuildSpec(构建工作单)**:`{build_id, profile, run_dir, base_dir, from_image | from_template
  (+kind), requested_target?, checkpoint_mode, steps[], start_cmd, ready_cmd, paths, net,
  resources, sandbox_resources, sandbox_spec/namespaces/env, mmds_enabled, envd_token,
  insecure, platform, timeouts}`；task env 含
  `MANIFEST_KEY` + 租户 `FLATTEN_*` 拉取凭据。artifact Prepare 含 source kind/ref、LaunchMode、manifest config、
  ref-location parent、ref 上限和从 execution claim 起算的绝对 deadline。task 读取根 cfg 后向
  `POST /internal/task/build/prepare` 提交 `{build_id,run_id,version,summary}`；相同 digest replay 返回同一 final result，
  冲突返回 409，HTTP waiter 取消不撤销已接受 summary。`paths` 是宿主侧工件与工具
  (kernel / runtime / 两个 diff template / sandbox-ctl /
  flatten-ctl / manifest-ctl / manifest_config);`net` 是 serve 预先 attach 的
  网络槽(tapfd transport、mac、inner_ip、nexthop、hostname、dns),全构建复用。
  SBX task 直接读取 E；SNP task 只读取 S 的 `sandbox_ref` 来定位并读取 E，丢弃 S memory
  payload/from-refs。二者返回 conductor 的仍只有 bounded summary；完整 E config 与选中的
  source ref 只留在 run-builder 进程内，随后强制 Phase B materialize。
  run-builder 据此自建阶段沙箱([§5](#5-按目标执行与发布));仅在该构建单元运行期间可取(serve 持挂
  pending 状态,单元退出即失效)。

### 4.1 Builder unit 与进程强制

单元安装、共享 pool 分配与 cgroup 基础设施见 [Node §5](node_zh.md#5-进程管理systemd-模板单元启动时自动生成安装);下方单元日志注释中的 §5.2 指 [Node journald](node_zh.md#52-日志journald-单汇--标签词表)。

**builder 单元**(`%i` = run-id,§5):

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
# execution aggregate CPU/memory 防御性硬限制
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

Builder execution 配置 CPU 或 memory 聚合上限时不允许保留长期 idle builder
(`units.builder_pool_size` 必须为 0)。按需单元仍先启动并进入 WaitAssignment，但它已有
对应 Build 的 durable execution claim；orchestrator 在发布 assignment 前设置并回读单元
属性。这样未 claim 的 idle RSS/CPU 不会侵占 `sandbox-builder.slice` 为 active Build 保留的
完整 aggregate ceiling，也不需要引入隐藏的 idle 资源预算。

<a id="12-模板构建target-aware最多三阶段的流水线构建在沙箱内进行"></a>
<a id="12-template-builds-target-aware-up-to-three-phases-inside-sandboxes"></a>
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
request-only spec/resource policy解析 → 从 builder pool 分配，先设置/回读 systemd limits，再以 exact run-id 持久绑定(无 idle 时按需
`StartUnit`)→ run-builder取得 bootstrap；SBX/SNP fromTemplate由task按 cold/E-selection
语义读根 cfg 并提交 summary，image fast path无需第二次 RPC → strict解析继承network并与本次请求合并 → 配
`tapfd_socket` 时经 `TAPFD/1 PREPARE`、否则经 `connector-ctl vswitch attach` 分配一个网络槽
(整个构建复用,各阶段顺序交接 tapfd)→ 铸 envd token → 在一个 exact-run SQLite CAS 中原子写入
port/token 与非秘密 `runtime_prepare_json`(prepare digest、resolved build/template network、
独立的 A/B execution 与 target Sandbox resources)→ (e2b + `mmds.enabled` 且 target 仍可能为 memory Sandbox 时)
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

**两级准入与强制**:Register 在 SQLite 同一事务内按 count/CPU/memory/storage 检查
`builder.admission.registration`,插入 immutable definition 并占用;Trigger 只做
`registered→waiting`。scheduler 按 `waiting_sequence`(成功 Trigger 事务的持久提交顺序) 稳定 FIFO,在单条持久
事务中按 `builder.admission.execution` 建 claim;不足保持 waiting,到 `queue_ttl` 后持久终态。
claim 后先设置并回读 `sandbox-builder@<run-id>` 的 CPUQuota/MemoryMax,再绑定 run-id、最后
发布 assignment。execution CPU/memory 同时施加到 `sandbox-builder.slice`;storage V1 为
admission-only。`node-ctl builder status` 和 metrics 暴露配置、持久用量、headroom、队列与
拒绝/过期计数。旧 `max_concurrent/cpu_quota/memory_max/vcpu/memory` 配置直接拒绝。

每个实际运行的 A/B/C phase 使用独立 SID，通过普通 `sandbox-ctl run` 的 controller
Admit/heartbeat/Release，并在 teardown/Release 完成后才进入下一阶段。A/B execution VM
resources 只从不可变 `Build.Resources` 派生；register Create resource 则以 source E portable
capacity（如有）为默认、供最终 Sandbox E/C0 使用，Image target 为精确零值且不必解析。
两者不互相推导。Build.Resources 本身不进入 nodectl，因此 active phase 只出现一条普通
Sandbox reservation，不存在双重记账；`target=sandbox,memory=false` 没有 C reservation。

ready/error 都是 retention-bounded Build history。终态事务原子写 `finished_unix` 并释放 registration usage。执行过的 Build 先完成完整
unit/cgroup、network、runtime/result 与 BuildRunDir/BuildBaseDir cleanup ，再 terminal commit 释放 execution
claim；只有 fully-cleaned 且无 claim 的 row 才可能在 `builder.terminal_ttl` 到期时被有界 reaper 删除。status、register-time
transient TemplateID、name/alias 与本机 list 随 row 消失；返回过的 canonical TemplateID 用于
Create 或 `fromTemplate` 均不受影响。

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
               enforcement_status, phase, phase_sandbox_id,
               runtime_vswitch_port, runtime_floating_ip, runtime_port_mac,
               runtime_envd_access_token_enc, runtime_prepare_json,
               execution_result_json, created_unix, finished_unix
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
BuildRunDir/BuildBaseDir。任何终态都先 fence exact unit/cgroup、detach port、清 runtime
ownership，并删除两个目录；terminal commit 再原子清空 RunID、execution result 并释放
execution claim。phase 子进程的自清理不是最终正确性
依据。节点级数据库、config socket 与 runner pidfile 不位于对象目录内，不受 Sandbox/Build
`RemoveAll` 影响。

共享 SQLite 基础设施与终态 reaper 由 [节点可靠性](node_zh.md#15-可靠性) 维护;Build schema 与恢复由本节维护。Build 行是有保留期的执行/status/alias 记录，不是永久模板目录；已发布 canonical template ref 不依赖原 Build 行。
