[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING_zh.md)

<a id="overview"></a>

# 贡献指南

欢迎为 `kuasar-sandbox/orchestrator` 贡献。文档遵循主项目的[文档贡献规范](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/CONTRIBUTING_zh.md#文档贡献)。本篇保留本仓源码、构建、测试与打包职责；公共能力用法从 [README](README_zh.md#documentation)进入。

<a id="contribution-workflow"></a>

## 贡献流程

使用公开 Fork 和主题分支。任何贡献者均可提 issue、review 或 PR，无须上游写权限。维护者负责审查、可信验证和 squash merge；不直接向上游 `main` 推送。

<a id="prepare-a-fork"></a>

## 准备 Fork

先将仓库 Fork 到自己的 GitHub 账号，再配置 HTTPS remote：

```bash
git clone https://github.com/<your-login>/orchestrator.git
cd orchestrator
git remote add upstream https://github.com/kuasar-sandbox/orchestrator.git
git fetch upstream
git switch -c feat/short-description upstream/main
# edit, test, and commit
git push -u origin feat/short-description
```

维护修复从受支持的 `upstream/release/vMAJOR.MINOR.x` 创建分支，PR 也以该分支为目标。适当时允许维护者编辑。同步 Fork 的 `main` 前提交或安全保存未提交改动：

```bash
git fetch upstream
git switch main
git merge --ff-only upstream/main
git push origin main
```

`--ff-only` 因历史分叉拒绝时，先检查本地提交并有意解决历史，不把 reset/force-push 当作日常同步方式。每个新主题分支基于最新 fetch 的上游目标分支，不基于无关功能分支。

<a id="pull-request-requirements"></a>

## Pull request 要求

PR 应聚焦明确的问题与解决方案，关联已有 issue，在行为变化时更新测试和文档，不含凭据、客户数据或无关生成文件，并描述已完成验证及运维/兼容风险。Draft PR 可用于早期反馈，但不是 merge 候选。

<a id="source-ownership"></a>

## 源码职责

| 路径 | 角色 |
|---|---|
| `cmd/node-ctl` | Conductor/Proxy/Telemetry serve、受管 run-sandbox/run-builder、resource status/list/drain、builder status、config、manifest-key、export/import 与 version |
| `cmd/cluster-ctl` | 独立 Registry/Router/Placer、config 与 version |
| `cmd/node-stub-ctl` | node-link E2E 参与方,独立 admin/API/data listener、restart/reset、Sandbox/Build 状态与故障注入;不启动 microVM |
| `cmd/e2b-key-ctl` | 无 DB/config 的 gen-key、derive-api-secret、gen-apikey、fingerprint、seal-pull-token |
| `config`, `app/conductor`, `app/proxy`, `app/telemetry` | 公共 declarative Config 与静态 custom App;sealed-memfd/原地 exec bootstrap、启动期 Config/Runtime Hook |
| `internal/telemetry`, `internal/telemetryapp` | 可信 route/peer 身份、envd/OTLP Collector receiver、storage/exporter、E2B compatibility 与组件生命周期 |
| `internal/orch` | 节点生命周期、Build pool、本地路由权威、单元生成与重启对账 |
| `internal/nodectl` | reservation 准入、pool/水位/grant、inventory/StateSync 恢复与审计;不接管 Sandbox balloon/cgroup 闭环 |
| `internal/nodelink` | Conductor–Registry 注册、心跳、事件与命令,帧化 JSON/h2c |
| `internal/{registry,router,placer}` | 复制 shardkv namespace、Reserve/Build 状态与 node-link key cache;E2B 统一入口/active cache;provider/importer、WATCH_LIST、P2C 放置 |
| `internal/api` | E2B 控制 REST、X-API-KEY 鉴权、export/import 与 ExecAccessToken 签发 |
| `internal/proxy`, `internal/proxyshm`, `internal/proxyadmission`, `internal/routesync` | L7 转发、service-addressed CONNECT/native exec gate、固定共享 route view、独立 inflight arena、MMDS confidential heap、park/wake 与 generation cleanup;route/node-link 注册/bookmark/typed result |
| `internal/configsock` | Run/task/admin/plugin/API、exact-run bootstrap/prepare、manifest-key/MMDS/Builder 管理与受控 route;SO_PEERCRED/pidfile 鉴权 |
| `internal/{apikey,secretbox,keys,regcreds}` | APISecret/API-key MAC、AES-GCM 根凭据存储、Forward/Exec kat1 与 data token、ManifestKey 封装的 image-pull 凭据 |
| `internal/{config,clustercfg,sandboxcfg,store}` | 节点/集群配置、SANDBOX_CONFIG 渲染与节点 SQLite Sandbox/Build/凭据对状态 |
| `internal/{mmds,mmdsrpc,mmdssvc,metrics,launcher,vswitch,util}` | MMDSv2/exact route、worker-master 查询、HTTP-over-UDS service、Prometheus、systemd D-Bus、connector-ctl 适配与工具 |
| `deploy/` | 各角色 example YAML 与 node/Proxy/Telemetry/Registry/Router/Placer systemd unit |
| `examples/custom-conductor` | 可编译 xconductor,由 node-ctl conductor serve 进入 |
| `examples/custom-proxy` | 可编译 xproxy,由 node-ctl proxy serve 进入,master reexec worker |
| `examples/custom-telemetry` | 可编译静态 telemetry App, 演示原生 Collector factory、pipeline 与 query 扩展 |

<a id="build-and-test"></a>

## 构建与测试

本地构建使用环境提供的 Go, 并继承 `GOROOT`、`GOTOOLCHAIN` 等工具链选择.
CI 构建和打包使用原生 x86_64/aarch64 Runner, 在一次运行中为每种架构固定已通过
验证的 Workbench 镜像. 发布构建、普通测试和打包在 build 模式中使用 ordinary UID
和任务私有可写路径. 完整的 `bash scripts/ci-source-checks.sh` 门禁在 Workbench
system 模式运行, 一并执行源码、race、vet、helper 测试和 systemd、真实 Docker
telemetry backend、network namespace 检查, 保留全部能力要求与断言.
发布编排使用环境提供的、支持 `api --slurp` 的 `gh`, 凭据不进入
源码执行环境.
Publisher 的归档校验器在独立 Workbench 调用中从受信发布源码编译, 该调用禁用缓存
恢复和保存. 组装与发布执行同一校验器, 不再次编译.

Go 源码构建需要 Go 1.26.1+。独立构建解析 `go.mod` 中真实可获取的版本；协调 Preview 构建使用选定的本地 `orchestrator`、`sandboxer`、`accelerator`、`connector` 模块：

```bash
GOWORK="$(python3 ../kuasar-sandbox/ci/integration/go_workspace.py --root .. --owners orchestrator)" || exit 1
export GOWORK
make build
```

工作区验证 module path，并通过 `use` 精确选择该闭包，不重复添加 `replace`，不改写 `go.mod`/`go.sum`。产品、helper 和源码测试使用同一组模块；真实沙箱测试另需 Runtime/Kernel 制品。


Go 二进制使用 `CGO_ENABLED=0` 构建。

```bash
make build                      # node-ctl, cluster-ctl, node-stub-ctl, e2b-key-ctl
make build TARGET_ARCH=aarch64  # 交叉编译;也接受 amd64/arm64 别名
make test                       # 单元和 helper 测试
ORG=/path/to/exact/sources make test-source  # source/race/vet/integration 门禁
```

Capture CLI/DB fixture 合同属于源码集成。必需 source gate 编译 exact sibling
sandboxer 的 `sandbox-ctl`，从当前 orchestrator 源码执行
`TestCapturePairCLIToPausedDatabase`、`TestUploadCaptureCLIToPausedDatabase`
和 `TestExportPublicationCLIAPI`。每个组选中后必须执行子用例，所有已开始的子用例
必须通过；Skip、失败、仅父测试结果、缺 exact sibling 源码均失败。

产品 E2E 遵循 **预构建产品 → prepare → `<suite>.<case>.sh` → 统一公开 runner**。
完整文件名是 case ID，第一段是 suite。项目只使用 `basic`、`storage`、`image`、
`network`、`sandbox`、`snapshot`、`orchestrator`、`builder`、`telemetry` 九个 suite。
用例消费 `BIN`、`WORK`、`OUT`、`E2E_LIB`、已准备的 helper 路径与本地镜像；
不编译、不发现兄弟源码仓、不自动拉取后备镜像、不以主机产品替代缺失的准备输入。
真实 guest build、flatten、snapshot 与 publish 是被测操作，继续真实执行。

当公开 runner 提供 `E2E_TEMPLATE_CACHE_DIR` 与
`E2E_TEMPLATE_CACHE_PROVENANCE` 时，元数据、Proxy 和 guest telemetry 用例可以
复用不可变的 execute 镜像准备。第一次 `build_image_template` 仍执行真实
`fromImage` 构建，导出已验证且未加密的 tarstream；后续调用验证内容，用自己的
密钥导入独立 Store，再通过 `fromTemplate` 注册新的 template/Build 身份。
请求的 e2b/bare profile 保持用例独立。Guest 启动、工作负载、网络、快照和清理
断言仍真实执行；专门的 Builder/Demo 与 `build_ready_template` 准备不使用缓存。

条目绑定完整准备摘要、原生架构、完整 execute 镜像 config ID、配方版本和读取
工具摘要，持锁原子发布。匹配条目损坏即失败，不重建；中断暂存不会命中。
不共享密钥、加密 Store、活动 VM、快照或可写磁盘。runner 负责保留目录，后续
运行不会复用旧目录。`run --no-template-cache`（或未提供这些输入的旧 runner）
保留普通准备路径。每个用例的 `template-preparation.jsonl` 记录 hit/miss/disabled
和总时长，`template-cache-phases.jsonl` 分别记录生产、导出/校验、导入和 API
注册成本。性能收益必须通过相同输入与资源下的真实配对运行测量。

在已组装的项目平台目录中，使用唯一公开入口：

```sh
python3 test/e2e/e2e prepare --release-dir /path/to/platform --workdir /path/to/prepared
python3 /path/to/prepared/test/e2e/e2e list --suite orchestrator
python3 /path/to/prepared/test/e2e/e2e run --workdir /path/to/prepared --include orchestrator.lifecycle.sh
```

准备阶段必须提供所选产品、helper 可执行文件和本地镜像。KVM 用例还要求 Linux、
root、systemd 与已准备的 Kernel/Runtime；缺少输入直接失败。共享 helper 只负责
fixture、进程、网络、HTTP 和观测原语，每个 case 自己拥有测试场景。
聚合与发布验收见项目 [E2E 合同](https://github.com/kuasar-sandbox/kuasar-sandbox/issues/172)。

[Owner 覆盖表](#node-validation) 将每个实质合同映射到维护的独立用例。

真实沙箱要求 Linux、systemd、root 权限、KVM、sandboxer、connector 及 guest-runtime 产生的 Runtime/VMLinux 制品。
变更范围可以限定在本仓，构建仍需上述依赖闭包；跨仓契约变更必须关联 companion PR 并使用精确源码组合的集成测试验证，
见 [Organization 贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。

源码 `REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh` 流程启动真实控制面进程,但不启动 MicroVM。
运行目录保持私有;失败输出只报告诊断文件名和大小,不输出原始响应、日志或含
capability 的对象。本地排查可设置 `CLUSTER_STUB_KEEP_WORK=1` 保留运行目录,
并私下检视;不要上传未脱敏文件。它不能替代真实 MicroVM 集成测试。
守护进程输出直接写入私有文件,不流向 CI;诊断文件的私有 umask 仅在本地
二进制构建结束后应用。

真实 execute/MMDS 用例从已有私有运行目录生成交换机、netns、veth 和
runner/builder unit 名称。清理只停止这些 unit 实例,只移除本次创建的资源。
显式指定的名称若已存在,或交换机状态不一致,会被拒绝,不会接管或强制删除。
由于主机路由、转发开关和 slice 名称共享,用例在既有 systemd 运行目录上持有
主机级互斥锁;第二个并行 execute/MMDS 调用在创建资源前被拒绝。锁描述符不会
传给守护进程。异步启动断言等待实际 runner 调用记录,不放宽 sandbox 身份或
启动模式检查。`make test` 包含隔离清理及并发回归;这些检查不能替代真实
对应的 lifecycle、exec、pause/wake、snapshot 和 MMDS 用例。

<a id="node-validation"></a>

## 测试

单元测试:`make test`(MMDS strict parser/top-level merge/minimal persistence、route value
encrypted owner blob/AAD/CAS/cleanup、admin UDS、service relay、routesync confidential projection、
Proxy master/worker resync/rotation;handler 路由,apikey/secretbox/regcreds,routesync(注册/bookmark
往返)/proxyshm(共享路由表、park/wake、世代清扫)/proxyadmission(多 worker 有界误差、generation
复用、crash-after-Wait 清理)、plugin 注册表(同 id 顶替)、proxy CONNECT 隧道 +
Exec KAT/64 KiB API/CmdExecSession/H1/H2 request gate 与
buffered half-close tunnel,mmds(确定性密钥),launch ownership,沙箱配置注入(命名空间解析/容量折叠/网络合并),
migrate,node-link(注册/事件/命令往返)等).

配对 source 验证覆盖真实 capture 生产者与 CLI 响应, SQL 原子回滚及重开,
生命周期 CAS, preparation 身份/digest 重放, Builder 对已受理配对的恢复,
以及严格认证的 token. 真实 CLI/API 发布矩阵覆盖 S/E × token/template × keep/move
× local/portable 全部 16 种组合. portable 用例先导入 token, 再使源工件不可用,
最后重新导出. capture 用例在返回结构化结果前删除载体, 并要求撕裂响应保留 running
行和原有对.

必需源码集成使用 `make test-source`，要求 Go 1.26.1+，并以 `ORG` 指定
所选集成源码工作区。它编译 exact sibling sandboxer CLI 来执行上述三组 capture/发布
测试；缺组、未完成子用例或任何 Skip 均失败。`test/source/builder_state.sh` 实际执行
必需的状态、并发、策略及发布规划 Go 合同，并拒绝缺失/跳过测试。单元/race/vet、helper
回归、特权 UFFD/Collector 检查、unit upgrade、journal 身份、cluster stub 集成和性能
门禁属于独立源码检查。
Helper 回归直接执行资源用例使用的就绪、grow 事件和实时 reservation 判定，覆盖
延迟、非法及被后续状态替代的观察值；同时验证 capture 结果完整性和先恢复再准备的顺序。

产品 E2E 遵循 **预构建产品 → prepare → `<suite>.<case>.sh` → 统一公开 runner**。
完整文件名是 case ID，第一段是 suite。项目只使用 `basic`、`storage`、`image`、
`network`、`sandbox`、`snapshot`、`orchestrator`、`builder`、`telemetry` 九个 suite。
用例消费 `BIN`、`WORK`、`OUT`、`E2E_LIB`、已准备的 helper 路径与本地镜像；
不编译、不发现兄弟源码仓、不自动拉取后备镜像、不以主机产品替代缺失的准备输入。
真实 guest build、flatten、snapshot 与 publish 是被测操作，继续真实执行。

在已组装的项目平台目录中，使用唯一公开入口：

```sh
python3 test/e2e/e2e prepare --release-dir /path/to/platform --workdir /path/to/prepared
python3 /path/to/prepared/test/e2e/e2e list --suite orchestrator
python3 /path/to/prepared/test/e2e/e2e run --workdir /path/to/prepared --include orchestrator.lifecycle.sh
```

准备阶段必须提供所选产品、helper 可执行文件和本地镜像。KVM 用例还要求 Linux、
root、systemd 与已准备的 Kernel/Runtime；缺少输入直接失败。共享 helper 只负责
fixture、进程、网络、HTTP 和观测原语，每个 case 自己拥有测试场景。
聚合与发布验收见项目 [E2E 合同](https://github.com/kuasar-sandbox/kuasar-sandbox/issues/172)。

| 维护的用例 | 必需断言 |
|---|---|
| [`basic.orchestrator-cli.sh`](test/e2e/cases/basic.orchestrator-cli.sh); [`orchestrator.api.sh`](test/e2e/cases/orchestrator.api.sh); [`builder.api.sh`](test/e2e/cases/builder.api.sh) | CLI help/config、unit 安装、health/鉴权、注册/触发/状态、跨 key 归属和无效请求。 |
| [`orchestrator.runtask.sh`](test/e2e/cases/orchestrator.runtask.sh) | 常驻 unit 父进程/直接子进程身份；Run session 先于 assignment；同 Run 重连不重复 assignment/launch；父进程专属描述符不传给子进程；子进程退出前 readiness EOF；唯一 reap/result report；重复父进程拒绝；delegated ctl/vmm 清理；exact-run bootstrap 及 TASK_* /重复 MANIFEST_KEY 清理。 |
| [`orchestrator.lifecycle.sh`](test/e2e/cases/orchestrator.lifecycle.sh) | WaitAssignment/readiness/init 失败、starting 状态 SetTimeout/Pause/Kill、Create parking、RunDir/BaseDir 归属、资源/lease、list/detail，以及保留节点文件的最终清理；真实 runner/runtime/VMM 退出自动收敛，存活中控制器重启保持 exact run 与 StateSync 身份。 |
| [`orchestrator.exec.sh`](test/e2e/cases/orchestrator.exec.sh) | 显式 KAT 及条件（空、exact、OR、cwd/user/TTY）、PTY resize/stdin/stdout/stderr/退出码、envd 初始化/hostname、marker 和真实 FloatingIP HTTP。 |
| [`orchestrator.pause-wake.sh`](test/e2e/cases/orchestrator.pause-wake.sh) | Guest /app 委派和冻结的进程/listener/counter；已受理 Pause 的 SIGTERM/143 取消屏障；同一 token 唤醒请求；memory 策略 node/body/header false/null 优先级、cold E、重复清理、native usage 和 TTL/reaper。 |
| [`orchestrator.reset.sh`](test/e2e/cases/orchestrator.reset.sh) | 真实当前磁盘手动/自动 Reset、存活运行接管、conductor 中断恢复、历史 checkpoint 数据、首次启动失败及最终删除。 |
| [`orchestrator.cluster-reset.sh`](test/e2e/cases/orchestrator.cluster-reset.sh) | 节点重连后的 dead 管理定位、route_key 创建冲突、checkpoint ID 操作、Reset 后立即 Wake、autoReset 传递，以及集群入口的精确父对象删除。 |
| [`orchestrator.snapshot.sh`](test/e2e/cases/orchestrator.snapshot.sh) | 本地 baseline/delta 内存恢复、磁盘层分离、portable keep/move 导出、noWake 拒绝、self-only prefetch 与清理；真实 Bundle A/B/C 闭包保持历史内存，Store 发布、本地恢复及 E-only 导出/导入保持 canonical 引用。 |
| [`orchestrator.resource-startup.sh`](test/e2e/cases/orchestrator.resource-startup.sh); [`orchestrator.resource-admission.sh`](test/e2e/cases/orchestrator.resource-admission.sh) | 重复 8GiB/低分配启动、有限 VMM 与无限制 parent/ctl；默认和小容量在 exec/pause/resume 后的 headroom、有界关闭；硬件容量、显式 512MiB 冷启动 reservation/balloon、Python 执行与 connected/settled 准入，随后验证红区拒绝。 |
| [`orchestrator.resource-control.sh`](test/e2e/cases/orchestrator.resource-control.sh); [`orchestrator.resource-recovery.sh`](test/e2e/cases/orchestrator.resource-recovery.sh); [`orchestrator.resource-reservation.sh`](test/e2e/cases/orchestrator.resource-reservation.sh) | 真实 sandbox-local 压力/grow、CH balloon 生效且无 OOM，控制器重启/inventory 收敛以及 reservation 生命周期。 |
| [`orchestrator.proxy.sh`](test/e2e/cases/orchestrator.proxy.sh); [`orchestrator.proxy-auth.sh`](test/e2e/cases/orchestrator.proxy-auth.sh) | 唯一数据入口；显式 local/stable 身份、重复 ID 拒绝、parking、静态 stats/native traffic、正确/缺失/错误 token 且不刷新 idle、canonical WebSocket、signed files、被动 noWake、raw CONNECT 和 native exec。 |
| [`orchestrator.proxy-wake.sh`](test/e2e/cases/orchestrator.proxy-wake.sh) | 条件拒绝的 exec 不改变 paused guest；同一合法 KAT 冷恢复并使 health/traffic 收敛。 |
| [`orchestrator.proxy-restart.sh`](test/e2e/cases/orchestrator.proxy-restart.sh) | 已准备的自定义扩展入口/private WebSocket；master 缺失时准入 503 且不占资源；master 路由 resync、worker 故障/替换就绪、metrics 和 worker 回收。 |
| [`orchestrator.mmds.sh`](test/e2e/cases/orchestrator.mmds.sh); [`orchestrator.mmds-recovery.sh`](test/e2e/cases/orchestrator.mmds-recovery.sh) | 独立 guest 覆盖 static、initial/unresolved secret、更新/轮换/删除、未声明 route 拒绝及 UDS service；恢复用例在更新和 full resync 之间重启 Proxy；两者检查 DB/log/route-table 明文不泄露。 |
| [`orchestrator.cluster-lifecycle.sh`](test/e2e/cases/orchestrator.cluster-lifecycle.sh) | node-link/placer 就绪后单次 Create；group/stable-ID exec、E pause/wake、WebSocket、data/stats/list 及 route/node/目录最终收敛；路由 Build 取消释放容量；Router 重启后首先读取保留状态；canonical peer/reuse 在 transient 删除后仍可用，且无目录 FD 残留。 |
| [`orchestrator.cluster-recovery.sh`](test/e2e/cases/orchestrator.cluster-recovery.sh) | 三个 Registry 观测真实 node-link redirect、重启实际 owner、等待 node-link 和 placer 收敛，随后单次 Create、data 就绪及 Registry/node DELETE 最终清理。 |
| [`builder.image.sh`](test/e2e/cases/builder.image.sh) | 重复 pool 独立预热、请求所选 DNS 的真实 image pull/flatten 和 Phase-A reservation、终态冲突/list、e2b/bare image 结果与 Build-row TTL 后 canonical cold Create。 |
| [`builder.steps.sh`](test/e2e/cases/builder.steps.sh) | 真实 RUN/ENV/WORKDIR、guest cgroup 委派、start/ready 继承、conductor SIGKILL 后原 worker/claim/进程接管、限定 phase 清理和 A/B/C 资源归属；snapshot/image metadata、单次 task-local source 准备、SNP-to-image 实体化及 Build MMDS 不可变/终态保密。 |
| [`builder.context.sh`](test/e2e/cases/builder.context.sh) | 未配置 COPY/files 的 501、格式错误或缺失 context 的 400、presigned 直接上传与 present false→true 幂等、真实 COPY 默认/chown 归属及 A/B 请求 DNS。 |
| [`builder.sandbox.sh`](test/e2e/cases/builder.sandbox.sh) | 不启动 A/B/C 的顶层 E、目标 capacity/env 和 source 闭包；E-to-image auto 解析；memory-C 无 ready 的固定等待、新 S→E 身份且不继承命令；row TTL 后 canonical E/snapshot Create。 |
| [`builder.failure.sh`](test/e2e/cases/builder.failure.sh) | 重复无 assignment 的常驻 runner/Builder unit 在 run-session 准入失败后被回收且保留 journal；真实挂起 RUN 取消与自动总超时、队列容量恢复、claim/进程/cgroup/port/目录回收、canonical peer 和存活 guest 保留、FD 关闭及确定业务错误诊断。 |
| [`builder.publish.sh`](test/e2e/cases/builder.publish.sh) | 保持 DB/key 的五个真实 local/Bundle、Manifest/named-location 发布操作：顶层 E 无中间 image、S→E delta 的 portable image 引用、精确 Store 对象变化、Bundle 目录闭包和 row TTL 后 canonical image/snapshot Create。 |
| [`telemetry.proxy.sh`](test/e2e/cases/telemetry.proxy.sh); [`telemetry.backends.sh`](test/e2e/cases/telemetry.backends.sh); [`telemetry.guest.sh`](test/e2e/cases/telemetry.guest.sh) | 真实 404 前后的 Prometheus counter 基线/增加与独立 backend 合同；真实 guest OTLP HTTP/gRPC/native traffic、local/stable 身份、pause/restart history/noWake、Collector 缺失/断连及 namespace 归属。 |

<a id="native-usage-read-validation"></a>

### 原生 usage 读取验证

密度 benchmark 测量有界读取及 FD/goroutine 行为,
不设置机器相关门槛; 原生采样和 history append 算法保持不变.

```sh
go test ./internal/orch -run '^$' -bench '^BenchmarkNativeUsageBatch$' -benchmem -count=3
```

此 benchmark 在每批 1/16/64 个对象下读取真实 SQLite 对象和原生 saved 文件,
报告分配量及保留 FD/goroutine 的变化; 不代表 guest 采样或远端导出吞吐.

<a id="e2e-execution-requirements"></a>

## E2E 执行要求

每个 `test/e2e/cases/<suite>.<case>.sh` 在 shebang 下仅声明一个静态 `# e2e-requires: [...]` JSON array，owner 格式由 `test/e2e/lib/requirements.json` 的 `{"schema":1}` 定义。声明用例及 helper 所需最小 kernel/service capability；无要求时显式写 `[]`。缺失、重复或未知项校验失败。普通 pinned source selection 自动发现新增、删除和要求变化，不维护平台 case allowlist。Runner labels 与放置属于可信平台配置。名称、校验与历史兼容见[静态执行要求](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/test/README_zh.md#静态执行需求)。

<a id="continuous-idle-regression-coverage"></a>

### 连续空闲回归覆盖

维护的 `orchestrator.idle-timeout.sh` 与 `orchestrator.idle-condition.sh` 在每个受支持 VM 架构的公共 prepared E2E runner 上执行，覆盖从未使用的 Sandbox、`idleTimeout=0` 与正数、HTTP/Native Exec 内存保持、并发与流式流量、要求旧 runner result 先持久化再提交 paused 的 capture 后 barrier、park/Wake、保持不变的 park deadline 和公共 UTC 条件 Pause。`prepare` 与 `run` 通过 `--include orchestrator.idle-timeout.sh --include orchestrator.idle-condition.sh` 选中。UTC 用例在 setup 流量结束后建立新的 Proxy observation；条件期间发生时钟跳变仍 fail closed。

每项策略输出 `idle-timeout-<seconds>.json`，包含原始 end-to-paused/wake-response 样本、状态变化、各进程 CPU/RSS、握手/capture timing、attempt/retry 次数。`pause-exit-facts.jsonl` 记录强制 exit-before-commit 观测。wake-response 包含客户端开销和业务执行；starting-to-running 是 100 ms polling 的 Resume 区间估计。这些是该次运行测量，不是性能保证；两个刻意保持的 barrier 的时间含人工等待，不应纳入普通 capture 延迟分布。源码测试使用可控时钟/barrier 覆盖 stale async stats、旧 RunID/lease 通知、时钟跳变和恢复。模拟、跳过用例或 ARM cross-build 均不构成真实 VM 验收。

<a id="component-packaging"></a>

## 组件打包

所选 owner 源码在 `release/helper-inputs.json` 声明完整的编译型 E2E helper 清单。配方 `scripts/build-e2e-helpers.sh` 接收目标架构和全新的输出目录，仅在源码构建阶段使用已准入的依赖工作区执行；打包 prepare 和用例执行消费经校验的二进制。内部构建目标和包路径由本仓维护。

使用组件 Makefile 从选定源码构建。`release.sh package` 使用匹配的
`bin/<arch>` 二进制,或显式指定的 `RELEASE_BIN_DIR`;只收集材料并生成 bundle,
不重新构建二进制,不重置源码或构建缓存。所选源码 checkout、依赖版本、原生
构建记录与产物应一并保留。

打包记录实际 Go 版本和生效的 module 替换。Go/module LICENSE、NOTICE 取自所选
编译器安装和匹配的 module 源码,保留嵌套路径。模块解析沿用正常 Go 缓存与路由,
下载模块的校验和须匹配二进制记录。只有明确单独采集的内部兄弟组件使用其自身
源码材料;组织命名空间本身不豁免其他模块。官方包中不受支持的第三方本地替换
需要改用带版本的 module 输入。选定的 Kuasar 源码通过显式 Go workspace 提供。

许可证和声明放在 `share/licenses/<component>`。声明缺失、目录不可读或遍历不完整时收集失败。包验证检查必需文件、载荷身份及归档路径、类型和权限。下载使用 GitHub 原生资产大小和摘要，包内及 Release 不发布来源或历史测试记录、独立 `SHA256SUMS`。

`make release` 先核对 `ACCELERATOR_VERSION`、`CONNECTOR_VERSION`、`SANDBOXER_VERSION` 与干净的相邻源码 checkout，生成四模块 Go workspace，再编译并打包同一输出。项目 checkout 不在 `../kuasar-sandbox` 时设置 `KUASAR_PLATFORM_DIR`。缺 Tag、缺模块或源码目录冲突会在编译前停止。发布者使用人类说明，拒绝覆盖同名不同内容资产。

四个官方 Go 可执行文件必须分别标识自身的 Orchestrator 命令 main package,
目标为所选 Linux 架构（x86_64 对应 amd64，aarch64 对应 arm64），且 `CGO_ENABLED=0`。
打包从所选源码树复制十二个部署文件,
通过既有 `RELEASE_*_SOURCE_DIR`、可选 `RELEASE_*_SOURCE_SHA` 及版本选择
收集实际 Accelerator、Connector、Sandboxer 依赖材料。独立验证只检查 bundle
中的依赖 URL/commit 字段相互一致,不要求验证主机具备兄弟仓或依赖 Tag。
解包前拒绝额外载荷、路径别名、重复条目、链接、错误 root 数字属主和错误权限。
材料目录仍按组件隔离。

<a id="ci-and-merge-policy"></a>

## CI 与合入规则

可信默认分支 wrapper 处理 PR 事件；candidate workflow 不决定准入，也不取得控制面秘密。任何贡献者均可提交公开 Fork PR；受限 runner 的执行信任与提交权分开。维护者在需要时准备经审查的可信候选，保留原作者与讨论出处。

[主项目 CI 契约](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci_zh.md)是准入条件、exact integration evidence、companion 声明、重新运行、凭据边界及 squash merge 的唯一权威。按当前 base/head 和 companion 集合重新验证；跳过/取消不等于成功，不绕过准入或分支保护。公开 Fork/PR 流程保持不变。

<a id="licensing-of-contributions"></a>

## 贡献许可

提交贡献即同意按所改文件适用的许可证授权。没有其它明确许可的新项目文件使用 [Apache License 2.0](LICENSE)。只提交有权贡献的内容，保留第三方或不同许可材料适用的版权、归属、NOTICE 与 SPDX 声明。

<a id="review-expectations"></a>

## Review 期望

反馈应具体且有建设性。作者应解决 review 对话，或说明不适用的原因。维护者可说明理由后关闭过时、被替代、不安全或超范围的 PR。

<a id="documentation-validation"></a>

## 文档归属与验证

精确定义随能力 owner 维护；README 是入口，示例项目文件就近保留。新文件应对应独立稳定主题，不按 flag 或 PR 拆篇。保留完整契约、代码示例与失败语义。英文为默认；新增中文版本以 `_zh.md` 配对。用主项目 `ci/check_docs.py --root . --paths ... --json` 显式检查所有改动与新增 Markdown，并运行 `git diff --check`。Checker 默认只发现 tracked files；新增文件在暂存前需显式 `--paths`。
