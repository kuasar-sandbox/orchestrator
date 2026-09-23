[English](node.md) | [简体中文](node_zh.md)

# node — 节点 e2b 兼容沙箱主机与集群接入

`node-ctl conductor serve` 是计算节点上的单实例控制 daemon,对外提供一套 **e2b 兼容 API**,把节点上的
microVM 沙箱以 e2b 协议暴露给客户端——在本文协议与已验证版本支持的操作范围内，未改造的 e2b SDK（python/js `e2b`、
`@e2b/code-interpreter`）与 e2b CLI 可直接指向本机运行。`node-ctl conductor serve` 一身兼数职:
**api**(控制面 REST:沙箱生命周期、模板构建、鉴权)、**主机**(经 systemd 模板单元
拉起/停止任务与 `sandbox-ctl`，在构建 guest 中执行 `flatten-ctl`，调 `connector-ctl vswitch` 编排网络)、可选的**资源控制器**(节点级资源仲裁,经
`resource_listen` 内置,node-resource.md)、以及 **node-link 客户端**(接入集群,把本节点
交由 cluster-ctl 编排,§10)。独立 `node-ctl proxy serve` 承载数据面，不嵌入 conductor。

node-ctl 既可**独立运行**(直供 e2b SDK / CLI,单机即可用),也可经 node-link **接入集群**
由 registry / router / placer 编排(cluster.md);两态共用同一套 e2b 控制面与生命周期原语——
**控制面 create / pause / kill / 模板构建始终在本节点**,集群只下发高层命令复用这些原语(§10)。

沙箱本体由 `sandbox-ctl` 运行(microVM,cloud-hypervisor);e2b profile 的 guest 内
跑原版 envd(由单一 `sandbox-runtime.bundle` 内置,§11),独立 Proxy 经 UDS
反代其单端口协议(49983/Connect-RPC)。同一租户范围内的根凭据是
**APISecret + ManifestKey**:APISecret 用于签发/验证 api_key 并派生每个 Sandbox 的
ServiceSecret,ManifestKey 保护 manifest 内容和镜像拉取令牌;APISecret 缺省时由
ManifestKey 通过固定 KDF 派生。两者成对加密存储,永不落明文盘(§7)。

产物两个二进制:**`node-ctl`**(daemon + 启动器 + 资源控制器 + 管理 CLI,§2)与
**`e2b-key-ctl`**(纯派生凭据工具,无 DB/config/编排状态,§2.8)。

## 1. 概述

### 1.1 业务问题

平台的沙箱栈(runtime/accelerator/builder/vswitch)各自提供 CLI 原语:起一台 microVM、
展平一个镜像、attach 一个端口。缺一个北向面把它们组合成"客户可用的服务":客户拿着
现成的 e2b SDK/CLI 与一个 api key,要能 create/exec/pause/resume/kill 沙箱、构建自定义
模板,而不感知 microVM、manifest、eBPF 交换机的存在。

node-ctl 补这一层,并刻意选择 **e2b 协议兼容**而非自定义 API:e2b 的 SDK 生态
(代码解释器、agent 框架集成)直接可用,客户端零改造;协议契约清晰(端点、token、
状态机皆有参照实现),兼容性可用真实 SDK/CLI 端到端验收。大规模部署时,多个 node-ctl
节点经 node-link 聚合到 cluster-ctl 控制面,做机群级会话亲和路由与按需调度(§10、cluster.md);
单节点亦可独立承载。

### 1.2 设计原则

1. **依赖面薄,经 CLI 组合**:驱动 `sandbox-ctl`(run/snapshot)、`connector-ctl vswitch`
   (attach/detach)、`flatten-ctl`(export)通过子进程 CLI；typed artifact/config 与 Builder assembly/publication 还复用兄弟仓公开包，
   不 import 兄弟仓 internal 包；
   经 systemd D-Bus 管单元;经 UDS 反代 envd。叶子组件,纯 Go,`CGO_ENABLED=0`。
2. **资源仲裁内置且可分离**:沙箱准入/配额由 `sandbox-ctl`(`pkg/resource` 的 client)
   与节点级**资源控制器**对话完成;控制器由 `node-ctl conductor serve` 经 `resource_listen` 内置
   (node-resource.md),调参随 serve 配置内联。它仍是与 conductor API/主机及独立 Proxy
   逻辑解耦的可分离子系统。构建任务的资源池由 serve 自管([Build §5](node-build_zh.md#5-按目标执行与发布))。
3. **进程管理交给 systemd**:runner/builder 模板单元以 run-id 为实例名,可预启动等待
   config-socket 下发 assignment;runner 单元下以 `ctl/vmm` 隔离监督进程与沙箱资源,
   builder 仍按完整单元核算;`StopUnit` 即完整回收,serve 不自己当进程监督者。
4. **密钥不落明文盘**:租户 APISecret+ManifestKey 凭据对在库内 AES-256-GCM 加密;
   ManifestKey 只经认证后的 task bootstrap 注入单租户 runner/builder 进程的 authoritative
   env,sandbox launch的conductor路径不用它打开或解析snapshot,也不进入guest(§6、§7)。集群下经node-link
   下行的凭据对同样仅加密落盘(§10)。
5. **本节点路由权威,独立数据面,集群可接入**:conductor 是**本节点**路由与生命周期的
   权威;独立 Proxy 是唯一 sandbox data ingress(§9).接入集群时,机群级
   路由权威是 registry——serve 经 node-link 上报沙箱事件、受理集群命令(§10),不与
   集群争路由权威。
6. **重启可对账**:状态在 sqlite + systemd 单元集,serve 重启后以单元集为
   存活权威对账收养/清理(§14);集群下另经 node-link 重连重报本节点沙箱集让 registry 收敛。

### 1.3 两类沙箱(profile)

| profile | 是什么 | 数据面 | 对外服务 |
|---|---|---|---|
| **e2b** | guest 内跑原版 envd,agent 经其 exec / 读写文件 / 跑代码 | envd fs/process/pty/runCode + 平台 native exec | envd/CI + floatingip 用户端口 + native exec |
| **bare** | 把客户镜像当网络化 microVM 跑起来,无 envd | 平台 native exec;不提供 envd API | floatingip 网络 + native exec |

`bare` 直接复用基础沙箱运行时(`sandbox-runtime.bundle`),把基础沙箱接上北向 API;
Build register 缺省为 **e2b**，也明确支持 **bare**；输出 profile 在注册后不可变。
profile 编码在 templateID 前缀里（[Build §2](node-build_zh.md#2-模板-id-与工件权威)），运行期据此选择共享 runtime Bundle 内的 guest 启动行为与数据服务。

### 1.4 边界与依赖

- 北向客户:e2b SDK / CLI 直连;集群下经 cluster-ctl router 转发(数据面)+ node-link
  下发命令(控制),平台管理面亦可经 e2b API 对接。
- 既可**独立运行**也可**接入集群**:控制面(create/pause/kill/模板构建)始终在本节点;
  接入集群仅多一条 node-link(§10),不改 e2b 契约。
- 不实现 envd 协议:数据面只透传到 guest 内原版 envd(§4.2)。
- 独立 Telemetry 实现 envd/OTLP 采集与 E2B `/sandboxes/{SandboxID}/metrics` 历史；
  Conductor 只鉴权、检查 ownership 并转发到 live query UDS，完整契约见
  [Telemetry](telemetry_zh.md)。`/stats/resource`、`/stats/traffic` 和 `/stats/usage` 均保持独立; 参见 §4.1.1 和[原生 usage](node_zh.md#原生-usage).
- 服务端不解析 Dockerfile；客户端展开的结构化 steps 会在构建 guest 内执行，支持镜像拉取、
  展平与最多三阶段流水线（[Build §5](node-build_zh.md#5-按目标执行与发布)）。
- 节点本地:路由、存储、单元管理都是节点本地的;跨机快照/模板使用 canonical
  portable ref(数据位于 manifest store 或统一挂载的 named location,§8.1),编排走
  cluster-ctl 的 node-link(§10)。
- 依赖:stdlib + `modernc.org/sqlite`(纯 Go)+ `golang.org/x/net/http2`(h2c,
  config-socket 与 node-link 共用)+ `golang.org/x/sys`(pidfile 锁 / SO_PEERCRED /
  mmap)+ `coreos/go-systemd`(D-Bus)+ `google/uuid`(v7)+ `gopkg.in/yaml.v3`。
  还使用公开 sibling packages、CEL/protobuf 与 S3 AWS SDK。envd/node-link 的本文 wire 使用
  Connect+JSON 或 framed JSON，不使用 gRPC transport；不能据此声称整个 go.mod 没有 protobuf。

### 1.5 架构与数据通路

除下图应用数据通路外，`node-ctl telemetry serve` 还订阅同一 Plugin Plane 的完整
RouteEntry stream，经 UDS 采集 envd，并在 management namespace 直接接收以 FloatingIP
识别 guest 的 OTLP。Collector pipeline 按标准组件配置写入 exporter; 本地 TSDB 仅在
显式启用时打开。查询 backend 和 HTTP handler 独立选择, query-only 无需 Collector graph。
Conductor 仅把鉴权后的 metrics query 转发到独立注册的 API UDS, 不把
telemetry 加入生命周期 barrier。完整拓扑、故障与身份契约见 [Telemetry](telemetry_zh.md)。

```
             client / cluster router
                    │ control
                    ▼
             APIEndpoint
                    │
      ┌─ conductor ─┴────────────────────────┐
      │ API, lifecycle, route authority      │
      │ sandbox/build ownership and systemd  │
      └──────────────┬───────────────────────┘
                     │ config_socket
                     │ route sync / Wake / barrier / MMDS / stats registration
                     ▼
      ┌─ proxy master + workers ─────────────┐
      │ DataEndpoint: HTTP / CONNECT / exec  │◄── client / cluster router
      │ MMDS and traffic observation         │
      └──────────────┬───────────────────────┘
                     │ UDS / TCP / ctl.sock
                     ▼
              sandbox backend
```

create 同步受理流程只做请求校验/纯解析、选定请求身份或生成默认身份、生成 token、进程内 launch ownership claim,
然后 insert `starting, run_id=""`(网络字段为空)并 cache/publish starting,在同一有序 routesync
下发 `route_barrier`,等待当前 proxy
master 成功应用先行 Upsert并 ACK,再次核验 registration lease 后才调度 launch并返回 HTTP
201.201 表示资源已被持久接受,且 proxy 已具备用该 starting identity 鉴权并
parking 的必要信息;不表示 runner 已分配、runtime 已 ready、backend 已可拨或 e2b `/init`
已完成。barrier 无 proxy、断连、apply 失败或超时时返回 503,在启动任何资源前删除该对象的
RunDir/BaseDir，再以 exact owner CAS 将 `starting,run_id=""` 收敛为零 ownership `dead` history
并发布 route Delete。cold image 后台路径保持原有单阶段 fast path:先完成
resource/network/YAML,再从 runner pool 分配 run-id。artifact launch 路径先建目录和绑定
`ready.sock`,再由 pool commit callback 以 `starting AND run_id=''` CAS 绑定 exact run-id;
task 取得 assignment 后立即连接 readiness、锁 task pidfile并取 bootstrap。认证通过后
ManifestKey 覆盖进程环境,task 按 E/S kind 与 LaunchMode 打开 root、选择 PreparedSource 并提交
capacity/network/ref closure 的非秘密 summary。唯一 launch worker随后 resolve resource/network→attach→以
`starting AND run_id=<exact>` CAS 持久化 ownership→写非密 YAML→返回最终 LaunchSpec。
runner追加本地保留的 ref-location并以同一 PID `execve sandbox-ctl run`→起 microVM→
严格完成 runtime readiness wire→(e2b)直接
`POST /init` 置 env/默认用户→以 exact run-id CAS 为 `running`并开放数据面。集群下,该
create 由 node-link 的
`create` 命令触发;profile、group、route-key
和可选认证主体通过结构化系统上下文下发并独立持久化。事件回报 profile、node-owned
执行事实和受保护路由凭据投影,registry 从既有节点归属记录恢复其 cluster identity
(§10、§4.4)。

数据面按 `Host`(`<port>-<sid>.<domain>`)或 `E2b-Sandbox-Id`/`E2b-Sandbox-Port` 头解析
legacy `(sid, port)`.e2b profile 的 49983/49999 使用 EnvdAccessToken,拨 sandbox-ctl
`--connect` 暴露的 host UDS 直达 envd/CI;bare 的 49983/49999 与其它合法端口一样,
以 ForwardAccessToken 鉴权并拨 `floatingip:port`.CONNECT 可以显式携带
`E2b-Sandbox-Service: forward|e2b:envd|e2b:code-interpreter|exec`;service 是权威 backend
选择器；普通 HTTP 使用 legacy routing，但显式 `exec` 在激活前返回 405。Native exec 使用显式申请的 ExecAccessToken,在任何
恢复副作用之前完成 KAT 校验,最终进入
`<RunRoot>/sandboxes/<NodeSandboxID>/ctl.sock`.
TrafficAccessToken 仅供外部网关及 e2b
数据面组件使用,node 平台层不消费。对 paused
沙箱的请求触发自动 resume(与其它入口共用 launch owner,§8).独立 Proxy 是唯一节点数据面,
转发层设计见 [node-proxy.md](node-proxy_zh.md)。集群下,数据面由 cluster-ctl router 经
把公开稳定 SandboxID 转换为当前 NodeSandboxID,再注入 `E2b-Sandbox-Id` +
`X-Access-Token` 转发进本节点 proxy(cluster-router.md)。

### 1.6 节点目录与身份

配置中的 `paths.run_root`/`paths.base_root` 分别是节点级 **RunRoot/BaseRoot**；对象的
实际目录称为 **RunDir/BaseDir**。RunRoot 只放 pid/lock、Unix socket、readiness、Sandbox
YAML、runtime config、CH 小型 snap-stage state 与小型临时 JSON。BaseRoot 放 writable diff、
本机 Sandbox checkpoint、Build image、Build Sandbox/Snapshot artifact 与其他大体积数据：

```text
<RunRoot>/
├── node-level files
├── runners/<RunID>.pid
├── sandboxes/<SandboxID>/
└── builds/<BuildID>/
    ├── builder.pid
    ├── a/
    ├── b/
    └── c/

<BaseRoot>/
├── node-level persistent files
├── sandboxes/<SandboxID>/checkpoint/
└── builds/<BuildID>/
    ├── checkpoint/
    ├── a/
    ├── b/
    └── c/
```

普通 Sandbox row 保存精确 `RunDir=<RunRoot>/sandboxes/<SandboxID>` 与
`BaseDir=<BaseRoot>/sandboxes/<SandboxID>`。`SandboxID` 始终是逻辑身份；sandboxer 的
`PathID` 只决定调用级 root 下的目录 leaf，普通 Sandbox 两者相同。Build phase 保留全局唯一
逻辑 SandboxID，PathID 固定为 `a`、`b`、`c`；资源、日志、memfd 与 artifact identity
仍使用逻辑 SandboxID。`RunID` 是 runner execution identity，pidfile 只位于 `runners/`。

`BuildID` 统一限制为 `[A-Za-z0-9_-]{1,48}`，原样作为 `builds/<BuildID>` leaf。
`BuildRunDir`/`BuildBaseDir` 由两个 root 与 BuildID 唯一派生，不写入 Build row，不 hash、
不 sanitize，也没有旧路径 fallback/migration。registered/waiting 不建目录，execution claim
后才创建；所有 Build image 与 Sandbox/Snapshot artifact 位于
`BuildBaseDir/checkpoint`。本机普通 Sandbox capture 位于 `BaseDir/checkpoint`。配置校验要求
自定义 RunRoot 在最大 SandboxID 下容纳 sandboxer 的最长 socket，并在 48-byte BuildID 下容纳
最长 phase socket（Linux pathname 上限 107 bytes）；不会按 root 动态改变身份长度合同。

## 2. 命令行接口

### 2.1 子命令总览

**`node-ctl`**:

| 子命令 | 用途 |
|---|---|
| `conductor serve` | 启动 conductor:控制面 + 本机控制 socket + reaper + 构建池;配 `cluster` 则起 node-link 客户端(§10),配 `resource_listen` 则内置资源控制器(node-resource.md) |
| `proxy serve` | 启动独立数据面 master/worker(见 §2.3 与 node-proxy.md) |
| `run-sandbox` / `run-builder` | systemd 单元内启动器,非给人用(§2.4、§6) |
| `resource` | `status`/`list`/`drain`:reservation 控制器巡检与 admission 排空(node-resource.md §2) |
| `builder status` / `builder cancel <build-id>` / `builder delete <transient-template-id> [--cancel]` | 查看持久用量、transient ID、操作意图与 claim; 取消执行或删除一条 Build 记录 ([Build §1.1](node-build_zh.md#11-取消与删除-build-记录)) |
| `config` | 配置规范化/校验,或输出带注释骨架 |
| `manifest-key` | `add`/`remove`/`check`/`list`:create/build/import 凭据对白名单管理(§7;集群下另由 registry 租约写入,§10) |
| `export-sandbox` / `import-sandbox` | 暂停沙箱转模板 / 跨机迁移(§8.1) |
| `version` | 版本 |

**`e2b-key-ctl`**(纯派生,不触 DB/config/daemon):

| 子命令 | 用途 |
|---|---|
| `gen-key` | 生成随机 32B 根凭据(64-hex) |
| `derive-api-secret [<MANIFEST_KEY>]` | 用固定 KDF 派生缺省 APISecret(§7) |
| `gen-apikey [<API_SECRET>]` | 用 APISecret 签发 e2b api key(`e2b_` + hex,§7) |
| `fingerprint [<API_SECRET>]` | 打印 APISecret 的完整 64-hex SHA-256 指纹 |
| `seal-pull-token [<MANIFEST_KEY>] …` | 封装不透明镜像拉取令牌(`kpt_`,[Build §5](node-build_zh.md#5-按目标执行与发布)) |
| `version` | 版本 |

接住一个新节点(独立模式)的典型顺序:

```bash
# 1) 生成内容根密钥,派生缺省 APISecret,登记凭据对,签发 SDK 用的 api key
MK=$(e2b-key-ctl gen-key)
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
node-ctl manifest-key add --api-secret "$API_SECRET" --label tenant-a "$MK"
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$API_SECRET")

# 2) e2b SDK/CLI 直接指向本机
export E2B_DOMAIN=sandboxes.example.com        # 生产(TLS, §12)
# dev: E2B_API_URL=http://host:3000  E2B_SANDBOX_URL=http://host:3443
```

集群模式下密钥由 registry 经 node-link 租约下发(§10、cluster.md),无须手动
`manifest-key add`。

### 2.2 `node-ctl conductor serve`

```
node-ctl conductor serve [--config /etc/node-ctl/conductor.yaml]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--config` | `/etc/node-ctl/conductor.yaml` | conductor 配置文件(§3) |

启动序列:打开 sqlite(文件 chmod 0600)→ 生成并安装 systemd 模板单元(§5)→
重启对账(§14)→ 起 reaper(TTL,5s 周期)→(配 `resource_listen` 则起内置资源控制器,
node-resource.md)→ 起本机控制 socket并确认监听成功(§6)→ 起 runner/builder 预启动池与
构建准入循环([Build §5](node-build_zh.md#5-按目标执行与发布))→(配 `cluster.node_link.endpoint` 则拨 registry 起 node-link 客户端,§10)→
监听 `api.listen`.该 listener 只服务 wrapped control API;sandbox data Host 或 CONNECT
不会转发到 backend.TLS 证书缺省时以明文 h2c 服务(dev:SDK 控制面走 `E2B_API_URL`).

随仓 systemd 单元模板:`deploy/node-ctl.service`。

### 2.3 `node-ctl proxy`

独立数据面 master;运维带外起,与 conductor 同节点.配置文件驱动
(`proxy.yaml`,自带 schema,见 [node-proxy.md](node-proxy_zh.md) §2),worker 由 master
从自己的当前 executable 内部 reexec 和监督；worker 不经过 node-ctl CLI，也不重新读取配置:

```
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

进程端点与 bootstrap 策略(`config_socket`/`data_listen`/`proxy_netns`/
`stats_socket`/`shm_path`/`workers`/`tls`/`auth`/`park_timeout`)在 `proxy.yaml`;
MMDS listen 与 service registry 只配置在 conductor。master 在 plugin 平面注册一次,
由握手取得 MMDS policy,维护共享路由视图并把唯一 Data listener fd 传给 worker.拓扑见
node-proxy.md §2,§5;conductor 与 Proxy 必须成对部署.

### 2.3.1 `node-ctl telemetry`

```sh
node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
```

独立组件拥有自身 strict config schema 和 `paths.telemetry_executable` static bootstrap
入口。以固定 Plugin ID `telemetry` 注册，订阅 `Kind: route`；只有同时选定 Reader 和 HTTP handler
时才注册独立 query UDS. 写入与查询独立; query-only 不要求 Collector 或 local TSDB.它不加入 Create/Resume readiness，也不调用 Wake。
原生 Collector 配置选择 envd、sandboxstats、sandboxotlp receiver, 标准 processor、
exporter、connector、extension 及多 pipeline. Sandboxstats 只经 conductor config_socket
上的当前 telemetry lease 读取, 原生 stats 不依赖 telemetry. Local TSDB、Prometheus、ClickHouse reader、直接 sandbox OTLP 网络、E2B step/MAX 与
boundary 契约见 [Telemetry](telemetry_zh.md)。`deploy/node-telemetry.service` 与
conductor/Proxy 并列部署，不作为它们的 required dependency。

### 2.4 `node-ctl run-sandbox` / `run-builder`

systemd 单元的 ExecStart,非给人用。共用的进入骨架:`--run-id` 是 systemd 实例名,
`--pidfile` 指向 `<RunRoot>/runners/<RunID>.pid`,以 `fcntl(F_SETLK)` 排他锁防重入并写本
PID → 拨 `--config-socket` WaitAssignment 取得业务 id(§6)。之后两者分道:

- **run-sandbox**:取得 sid 后立即连接固定的
  `<RunRoot>/sandboxes/<SandboxID>/ready.sock`(此时 readiness fd 保持 `FD_CLOEXEC`)→ 锁
  `<RunRoot>/sandboxes/<SandboxID>/<SandboxID>.pid`→以 sid + exact run-id 取 bootstrap。cold image bootstrap
  直接带最终 LaunchSpec,只需一次 RPC。E/S artifact bootstrap 带 task-local
  `ArtifactPrepareSpec` 与 authoritative `MANIFEST_KEY`;runner 覆盖继承环境,在本进程按
  source kind 与 durable LaunchMode 打开 E/S,提交同一 completion并等待最终 LaunchSpec。
  它保留 task-local `PreparedSource`、carrier binding 和 sorted ref-location,
  显式关闭 reader/fetcher后才 `chdir(LaunchSpec.Workdir)`、剥除 `TASK_*`、合入 task/spec env。
  仅在最后一次 `execve` 前清 readiness fd 的 `FD_CLOEXEC`,向 argv 追加其实际编号
  `--ready-fd=<fd>`并替换为 `sandbox-ctl run`。目标继承本 PID、单元 cgroup、pidfile
  锁 fd 和 readiness fd;任一 pre-exec/prepare 失败都会关闭 readiness连接,serve立即读到 EOF。
- **run-builder**:完整任务准备、驻留执行、deadline 与结果交接见 [Build §4](node-build_zh.md#4-任务交接).

```
node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
node-ctl run-builder --pidfile=<f> --config-socket=<uds> --run-id=<rid>
```

flags 缺省回落 `TASK_PIDFILE` / `TASK_CONFIG_SOCKET` / `TASK_RUN_ID`
env(systemd `%i` 接线用)。

### 2.5 `node-ctl config`

配置诊断 + 生成工具,**按角色**(`conductor` / `proxy` / `telemetry`,各自独立文件与 schema):

```
node-ctl config <conductor|proxy|telemetry> --template            # 输出该角色带注释骨架
node-ctl config <conductor|proxy|telemetry> --config <file>       # 加载(补默认 + 校验)后重排输出
node-ctl config <conductor|proxy|telemetry> --config <file> --resolve   # 再展开 auto/派生(实际生效形态)
                              -o <file>             # 写文件(默认 stdout)
```

角色作首参以消歧 schema:`conductor` 对应 `conductor.yaml`(§3),`proxy` 对应 `proxy.yaml`
(node-proxy.md §2)，`telemetry` 对应 `telemetry.yaml`（[Telemetry](telemetry_zh.md)）。
`--resolve` 对 `conductor` 额外展开 `resource_listen` 的 `auto` 内存/CPU
(并深校验水位),其余角色与 `--config` 等价。骨架与 `deploy/{conductor,proxy,telemetry}.example.yaml` 对应。
该命令只做严格 declarative decode、默认化与诊断，绝不执行 `conductor_executable` /
`proxy_executable` / `telemetry_executable`，也不接触 custom App 的运行时材料。custom 路径非空时，输出首行明确
标记这里只完成 bootstrap 校验；最终配置仍由对应 App 在启动副作用前校验。

### 2.6 `node-ctl manifest-key`

create/build 凭据对白名单(`manifest_keys` 表)管理,是 serve daemon **admin 平面**的瘦
客户端(经本机控制 socket,§6)——daemon 是该表唯一写者,CLI 不开 DB、不读 config,
只需 `--socket`(或 `NODE_CTL_SOCKET` env,默认 `/run/sandbox/node-ctl.socket`)。
ManifestKey 取自位置参数或 `MANIFEST_KEY` env;APISecret 可用 `--api-secret` / `API_SECRET`
显式指定,缺省则按 §7 固定 KDF 派生。两者原子成对入库;输出只含两者的完整指纹,
绝不回显根凭据。集群下该表另由 registry 经 node-link 以租约项写入(§10、cluster.md),
与手动项共存。

```
node-ctl manifest-key add    [--api-secret S] [--label L] [--ttl 24h]
                             [--registry-auth <docker.json> |
                              --registry-username U --registry-password P |
                              --registry-token T]      [--socket S] <KEY>…
node-ctl manifest-key remove [--api-secret S] [--socket S] <KEY>…
node-ctl manifest-key check  [--api-secret S] [--socket S] <KEY>…
node-ctl manifest-key list   [--socket S]
```

- `--api-secret`:显式指定与该 ManifestKey 配对的 APISecret;只接受单个 `<KEY>`。
  缺省时派生默认值。`add/remove/check` 始终按完整凭据对操作。
- `--ttl`:失效时长(`0`/缺省 = 永不);只有完整 pair 完全相同时,重复 add 才刷新失效时间;
  相同 APISecret 完整指纹绑定不同凭据材料时报冲突。过期凭据对视同不在
  白名单,由 reaper 惰性清理;`list` 显示 `expires`。
- `--registry-auth`/`--registry-*`:租户默认镜像拉取凭据,加密存白名单行
  (`registry_auth_enc`),构建拉取时按 fromImage 的 host 匹配取用([Build §5](node-build_zh.md#5-按目标执行与发布))。
- `add/remove/check` 输出 `STATUS api=<64-hex> manifest=<64-hex>`;`list` 逐行输出两项完整指纹。
- 鉴权:`SO_PEERCRED`——配置了 `paths.admin_pidfile` 则 peer pid 须在其中;未配则仅靠
  socket 0600 权限(同 uid / root)。

### 2.7 `node-ctl export-sandbox` / `import-sandbox`

serve daemon **api 平面**的客户端(经本机控制 socket 调 `POST /sandboxes/{id}/export`
与 `POST /sandboxes/import`),鉴权 `E2B_API_KEY` env(须属主)。语义见 §8.1。

```
node-ctl export-sandbox <sid> [--to-template] [--keep-source] [--json] [--socket S]
node-ctl import-sandbox <token> [--socket S]
```

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

`--json` 输出完整的成功 Export API 响应；默认仍只打印一行 `result`（token 或
TemplateID），发布进度不会进入 stdout。无效/尾随 JSON、被污染的 stdout、缺少 result
或根、错误根角色、不安全本地引用，以及 TemplateID 与根不一致都会明确失败。客户端也
检查响应读取错误，不会把空凭据打印为成功。完整响应契约见 §8.1.4。

### 2.8 `e2b-key-ctl`

总览见 §2.1。`seal-pull-token` 完整形式:

```
e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] {--registry-username U --registry-password P |
                                              --registry-token T}
```

输出 `kpt_` 前缀的不透明令牌:以租户 manifest_key 派生密钥 AES-GCM 封装的镜像拉取
凭据,经 SDK `api_headers`(头 `X-Kuasar-Pull-Token`)随构建请求传入,serve 用
该租户的存量 key 解封([Build §5](node-build_zh.md#5-按目标执行与发布))。manifest key 取自首个位置参数或 `MANIFEST_KEY` env。

## 3. 配置

serve daemon 的配置文件是 `conductor.yaml`。完整带注释样例见 `deploy/conductor.example.yaml`
(`node-ctl config conductor --template` 输出同形骨架),权威结构是公共包
`github.com/kuasar-sandbox/orchestrator/config` 中的 `config.Conductor`；内部代码复用同一 schema，
不维护第二份配置事实来源。该包提供严格的 `LoadConductor` / `DecodeConductor`、
`LoadProxy` / `DecodeProxy`、hook 后不重套默认值的 `ValidateConductorFinal` /
`ValidateProxyFinal`，以及真正深拷贝的 `Clone`。Decode/Load 只执行 bounded strict decode、
declarative defaults 和已提供值的枚举/duration/range/格式校验；它们不读取环境、检查 component
文件或根据 executable 决定启动必填项。同一输入的结果不随 `NODE_CONFIG_ENCRYPTION_KEY` 改变。
未知 YAML 字段和多文档输入会失败，Config 的 JSON/YAML 只包含可序列化 declarative 数据，
不含 logger、provider 或运行时句柄。
`ResourceAllocatable.Memory` 以 `*string` 公开 presence：`nil` 表示继承 internal resolver
中的 `256MiB` 默认值，非 nil（包括显式 `256MiB`）表示 operator/custom `Configure`
的明确策略，必须满足 capacity 上界。`SetMemory` / `InheritMemory` 只是便利方法；直接赋
pointer 具有同一语义，`Clone` 与 component bootstrap JSON 都保留该 presence。
配置按关注点分组:`api`、`proxy`、`paths`、`units`、`sandbox`(实例级默认,子组
`resources`/`network`/`boot`)、`builder`、`checkpoint`、`mmds`、`cluster`(node-link,§10)、
`resource_listen`(内置资源控制器,调参全部内联,node-resource.md),外加顶层单值
`encryption_key`、`manifest_config`。内置模式由 node-ctl 显式执行 final declarative validation，
再在 runtime resolution 读取 `NODE_CONFIG_ENCRYPTION_KEY` / YAML key；custom conductor 则在
`Configure` 后 final validate，并可由 Runtime provider 满足 key/TLS/object-store 材料。provider
优先于 environment/YAML 且失败不回退。运行 helper(sandbox-ctl/connector-ctl vswitch/flatten-ctl)
不进入公共 Config；它们先以最初的精确 node-ctl 相邻发行目录为解析基准，缺失时保留
现有 PATH fallback。

| 字段 | 默认 | 说明 |
|---|---|---|
| `api.domain` | (必填) | 服务域,如 `sandboxes.example.com`;控制面 = `api.<domain>` |
| `api.listen` | `:443` | 北向监听;dev 用 `:3000` 走明文 h2c |
| `api.tls.cert/key` | 空 | 通配证书(`*.<domain>` 与 `api.<domain>`,§12);空 = 明文。custom Runtime TLS provider 非 nil 时为权威材料源，core 仍固定 TLS version/ALPN/client-auth 策略 |
| `proxy.park_timeout` | `30s` | 数据面请求挂起预算:等路由同步 / paused 沙箱 resume 的上限(node-proxy.md §4) |
| `proxy.auth` | `enforce` | 数据面鉴权:`off`/`log`/`enforce`,校验 `X-Access-Token`(node-proxy.md §6) |
| `proxy.metrics_listen` | 空(关) | conductor 进程的全局 Prometheus 文本端点;Proxy worker 数据面指标在 `proxy.yaml` 的 `metrics_listen` |
| `encryption_key` | 内置模式必填 | APISecret/ManifestKey 凭据对落盘加密的 AES-256 密钥:`:` 分隔多个 64-hex,首个为活动密钥,其余备用解旧记录(轮换);优先级为 custom Runtime provider > `NODE_CONFIG_ENCRYPTION_KEY` > YAML，provider 失败不回退 |
| `manifest_config` | `/opt/sandbox/manifest.yaml` | 共享远程 manifest store 配置(`manifest.key` 留空,租户 key 经 env 按任务下发) |
| `paths.conductor_executable` | 空 | 静态定制 conductor 的绝对 executable；空使用内置实现。`node-ctl config` 只诊断 regular/executable、非 group/world-writable、非 node-ctl same-file 元数据，不按诊断 EUID 判断 owner；实际 dispatch 中 root node-ctl 只接受 root-owned，非 root node-ctl 接受 root-owned 或本 EUID-owned。它执行已打开并校验的同一 FD，失败绝不回退 |
| `paths.run_root` | `/run/sandbox` | 节点 RunRoot:node-level 小文件、`runners/`、`sandboxes/`、`builds/`；通常为 tmpfs；必须为最大 BuildID 的最长 phase UDS 留出 107-byte Linux pathname 预算(§1.6) |
| `paths.base_root` | `/var/lib/sandbox` | 节点 BaseRoot:node-level 持久文件与 `sandboxes/`、`builds/` 大体积数据(§1.6) |
| `paths.db_path` | `<base_root>/node-ctl.db` | sqlite 路径(§14) |
| `paths.config_socket` | `/run/sandbox/node-ctl.socket` | 本机控制 socket(run assignment/result + task/admin/plugin/api,§6);manifest-key/export/import CLI,Proxy 与平台 agent 的连接点 |
| `paths.admin_pidfile` | 空 | admin 平面的多行 PID 白名单(`#` 注释);未配则仅靠 socket 0600 |
| `paths.plugin_pidfile` | 空 | plugin 平面(proxy/agent 注册)的多行 PID 白名单;未配则仅靠 socket 0600 |
| `units.dir` | `/etc/systemd/system` | 模板单元安装目录 |
| `units.runner_pools` / `units.builder_pools` | 缺省 | 有序独立 `{unit, size}` pool 列表，见下文 |
| `units.runner` / `units.builder` | `sandbox-runner@.service` / `sandbox-builder@.service` | 模板单元名 |
| `units.runner_pool_size` / `units.builder_pool_size` | `0` / `0` | 空闲预启动 run-id 单元数;0 = 不保留 idle,有任务时仍按需经 WaitAssignment 流程启动. 有限 execution CPU/memory 准入资源允许非零 Builder pool,idle 单元不持有 Build claim.  |
| `units.pool_wait_timeout` | `5s` | 从调用 `StartUnit` 到单元进入 WaitAssignment 的正数时限;超时清理该 run-id 并补池 |
| `units.install` | `true` | `false` = 单元由运维带外管理,serve 不生成安装 |
| `sandbox.timeout_sec` | `300` | 沙箱默认 TTL(秒) |
| `sandbox.usage.enabled` / `.sample_interval` / `.flush_interval` | `false` / `1s` / `5m` | image cold、`run --from`、`run --restore` 共用的原生生命周期计量策略; 严格校验, 不进入 portable artifact, 独立于 telemetry. 参见[原生 usage 策略](#33-原生-usage-策略) |
| `sandbox.dead_ttl` | `24h` | 已完成全部本地 cleanup、无任何 owner 的 `dead` Sandbox 诊断记录保留期；必须为正 Go duration |
| `sandbox.resources.capacity.cpu` / `.memory` | `2` / `2GiB` | guest 可见的 VM 上限/SKU;E2B `cpuCount`/`memoryMB` 继续表示 Capacity。img 冷启可由 create/group 覆盖;restore Capacity 由 snapshot 固定 |
| `sandbox.resources.allocatable.cpu` / `.memory` | 最终 capacity CPU / 省略时继承 `256MiB` | CPU 是相对调度权重，不是硬性小数核保证；memory 是 settled guest headroom,不是 total Budget。conductor 配置省略 memory 时可随最终 Capacity 收敛；operator/custom 显式值（即使等于 `256MiB`）不得静默收敛，越界直接拒绝。request patch 的 pointer schema 与规则不变 |
| `sandbox.resources.startup.memory` | 最终 `capacity.memory` | cold 首份可信 report 前的 headroom,static/dynamic 均有效,与 settled headroom 独立;restore 不使用 |
| `sandbox.resources.overhead.memory` | `32MiB` | node-owned host VMM overhead;sandbox-ctl 不在 VMM cgroup 内;request/template 不可覆盖 |
| `sandbox.resources.watermark_high.ratio` | `0.875` | node-owned sandbox `memory.high` pressure ratio,必须在 `(0,1)`;request/template 不可覆盖 |
| `sandbox.network.switch` | `sw0` | vswitch 交换机名 |
| `sandbox.network.hostname` | `sandbox` | guest 主机名:sethostname + `/etc/hosts` 条目(§11) |
| `sandbox.network.dns` | `[169.254.169.253]` | 注入 guest `/etc/resolv.conf` 的 nameserver;该地址需部署侧路由到真实 DNS |
| `sandbox.network.e2b` / `.bare` | `169.254.0.21/30`+`169.254.0.22` / `169.254.1.1/31`+`169.254.1.0` | 按 profile 的 guest 内 `{inner_ip, nexthop}`:每 profile 复用同一对,沙箱唯一身份是 floatingip;e2b 的 /30 + 网关让 envd 端口转发可用 |
| `sandbox.boot.kernel` | – | vmlinux 路径 |
| `sandbox.boot.runtime` | – | 单一 guest runtime bundle;offset-zero EROFS + digest marker ZIP,内置 envd、flatten-ctl、mkfs.erofs(§11) |
| `sandbox.boot.overlay_diff_template` | – | 预格式化空 ext4,img 冷启时稀疏复制为可写 upper(裸空 diff 非合法 fs 会被拒);部署方使用 `mkfs.ext4 -O ^has_journal` 格式化稀疏文件提供;restore 不需要(overlay 链来自快照) |
| `checkpoint.mode` | `local` | 暂停态本机 capture:`local` = 现有 tarstream,`bundle` = multi-Manifest ZIP Bundle；输出固定在 Sandbox `BaseDir/checkpoint`(§1.6、§8.1) |
| `checkpoint.merge_ref` / `.drop_caches` | 未设置 | Pause 的节点级三态策略:`true`/`false` 显式传给 `sandbox-ctl snapshot`;省略或 YAML `null` 则交给 sandbox-ctl 缺省 |
| `checkpoint.remote.ref_location_parent` | 空 | 可选 absolute hostless `file://` URI；Builder checkpoint 类 graph 和 `export-sandbox` 的 named-location parent，也用于解析 located Build image Bundle；不改变 Pause capture mode |
| `checkpoint.remote.manifest` | `false` | `false`：Build image 类进入 Manifest store；`true`：image 类不写 Manifest store，而以 single-root Manifest Bundle 物化到上述 named location，且 parent 必填。它不改变 `checkpoint.mode` 或 checkpoint 类 policy（[Build §5](node-build_zh.md#5-按目标执行与发布)） |
| `mmds.enabled` | `false` | envd 鉴权姿态开关(§9.2,node-proxy.md §7):false = `-isnotfc` + proxy 单闸门;true = FC 模式 + MMDS re-key |
| `mmds.listen` | `127.0.0.1:19254` | MMDS 监听地址(vswitch `--mgmt-service` 的转换目标) |
| `mmds.routes.enabled` | `false` | 是否接受租户声明的 static/secret/service exact route;要求 `mmds.enabled=true` |
| `mmds.routes.max_routes_per_sandbox` | `32` | 每个 Sandbox 或 Build 的 route 数量上限 |
| `mmds.routes.max_namespace_bytes` | `65536` | Header 或 metadata 中单份 MMDS JSON 的字节上限 |
| `mmds.routes.max_static_body_bytes` | `16384` | 单条 static `data` 的 UTF-8 字节上限 |
| `mmds.routes.max_secret_value_bytes` | `16384` | initial secret string 或 admin PUT opaque body 的字节上限 |
| `mmds.routes.reserved_path_prefixes` | `[/latest/api/, /internal/]` | 租户 route 禁止占用的 exact path 前缀;内置 `/` 也保留 |
| `mmds.services` | 空 | conductor-only 本机 service registry:`name.endpoint`;V1 只接受 `unix://` absolute path,不在 `proxy.yaml` 复制 |
| `cluster.node_link.endpoint` | 空 | registry 的 node_link 地址(§10);空 = 独立模式,不接入集群 |
| `cluster.node_link.tls` | 空 | node_link mTLS 证书 / key / CA(`{cert,key,ca}`;生产必配,§10 / cluster.md) |
| `cluster.node_id` | (接入集群必填) | 本节点唯一标识(node-link 注册,cluster.md) |
| `cluster.labels` | 空 | 节点标签 `{zone,pool,slot,node}`(placer nodeSelectors 匹配,cluster-placer.md) |
| `cluster.api_endpoint` | (接入集群必填) | conductor control API 的显式 `host:port` advertised endpoint;不得从 `api.listen` 推导 |
| `cluster.data_endpoint` | (接入集群必填) | Proxy sandbox data 的显式 `host:port` advertised endpoint;不得从 `proxy.data_listen` 推导 |
| `resource_listen` | 缺省(不内置) | controller endpoint 的唯一配置源:`socket` 解析为 bind 用的绝对 `Listen` 与 owner/inventory/lease/sandbox.yaml 使用的 canonical `SocketIdentity`;sandbox client 经 canonical path 连接同一 socket inode。`enabled` 开关及其余调参见 node-resource.md §3.2。省略或 disabled = 静态 cgroup |

新建 `sandbox.boot.overlay_diff_template` 和 `builder.diff_template` 时，使用
`mkfs.ext4 -O ^has_journal`，避免可丢弃工作盘的文件系统 journal 占用及元数据
日志写入。这不影响 journald 或应用日志，也不提供崩溃恢复保证。已有带 journal
的模板和用户自带镜像继续兼容；sync/quiesce、快照/恢复及挂载行为保持不变。
此约定仅适用于新建空模板，不要重新格式化已有数据盘。

多池的主要应用场景是 [NUMA 部署（§5.3）](#numa-deployment)：由不同 unit 模板承载不同节点的 CPU/内存放置策略，再轮询分配新执行。以下重复模板示例仅说明配置语义，不代表不同 NUMA 绑定。

`units.runner_pools` / `units.builder_pools` 每项仅包含 `unit` 和 `size`。
每个列表项创建独立 pool，同类重复模板及完全相同的配置项也保留独立位置。
`size >= 0` 是空闲预启动 worker 目标数，不是并发容量或轮询权重；0 仍参与轮询并按需启动。
`dir`、`install`、`pool_wait_timeout` 由所有 pool 共享。

```yaml
units:
  dir: /etc/systemd/system
  install: true
  pool_wait_timeout: 5s
  runner_pools:
    - {unit: sandbox-runner@.service, size: 8}
    - {unit: sandbox-runner@.service, size: 4}
    - {unit: sandbox-runner-special@.service, size: 0}
  builder_pools:
    - {unit: sandbox-builder@.service, size: 2}
    - {unit: sandbox-builder@.service, size: 0}
```

对应列表缺省时，沿用旧 `runner` / `runner_pool_size` 或 `builder` / `builder_pool_size`
形成一个 pool，保留原模板及 size=0 默认值。runner 与 builder 可分别使用新旧输入。
显式空列表、非法项、同类显式新旧混用（包括显式旧 size=0）均拒绝；程序补入的默认值
不算混用。兼容范围仅为新版读取旧配置。

### 3.1 静态定制 conductor

定制 Conductor 的构造、受保护配置、运行期绑定与 provider 契约统一见 [扩展指南](extensions_zh.md#conductor-bootstrap)。节点核心始终拥有授权、持久记录与生命周期不变量，不能由配置 Hook 绕过。

### 3.2 静态定制 Proxy

定制 Proxy 的配置/绑定生命周期、Master/Worker hook、公共路由与授权转发 SDK 统一见 [扩展指南](extensions_zh.md#proxy-bootstrap)。Proxy 核心进程、共享内存与取消不变量见 [Proxy 规范](node-proxy_zh.md)。

Builder 配置与 legacy schema 边界见 [Build 配置](node-build_zh.md#3-build-配置)。

远程内存 Prefetch 没有节点统一开关。是否请求 Prefetch 由每个 sandbox 的
`kuasar-sandbox.restore` 命名空间决定(§4.4)。

配置自洽校验:`mmds.enabled=false` 时 `proxy.auth` 必须为 `enforce`(envd 非 secure,
Proxy 是唯一数据面闸门);`mmds.routes.enabled=true` 还要求 `mmds.enabled=true`,service endpoint
必须是 absolute Unix socket URI.`proxy_netns` 和 worker 数只配置在 `proxy.yaml`.
配 `cluster.node_link.endpoint` 时 `cluster.node_id`,`cluster.api_endpoint` 和
`cluster.data_endpoint` 都必填;配
`resource_listen.enabled=true` 时 node-ctl 在启动阶段解析一次 canonical controller
identity并自动写入每个 dynamic sandbox YAML;没有第二个
`sandbox.resources.control_socket` 配置源。startup 对 static/dynamic 使用同一 headroom
语义,不依赖 controller 是否启用。

### 3.3 原生 usage 策略

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

## 4. e2b API 契约

基址 `https://api.<domain>`;鉴权 **`X-API-KEY`**(SDK)或 **`Authorization: Bearer`**
(e2b CLI 构建面,两者同样解析)。api_key 由 APISecret 签发(`e2b-key-ctl
gen-apikey`),serve 经 MAC 校验解析出租户——无静态 api_keys 表(§7)。

**归属校验**:按 id 的控制操作用 api_key 的 MAC 对该资源行的(解密)APISecret
校验,不符回 **404**(不泄露他租户存在性);create / build / import 另需对应的
APISecret+ManifestKey 凭据对在白名单,否则 **403**。

### 4.1 控制面:沙箱生命周期

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| create | `POST /sandboxes` → 201 | body `{templateID, timeout, metadata, envVars, autoPauseMemory?}` + 可选 `X-Kuasar-Sandbox-*` Header;`autoPauseMemory` omitted/null/true 使 TTL capture S,false 使 TTL capture E,且不改变显式 Pause 缺省;201 表示 durable starting acceptance,不等待 runner/runtime/envd;e2b 回 Envd/Traffic/Forward token,bare 只回 Forward token |
| get | `GET /sandboxes/{id}` | 附 `state`/`startedAt`/`endAt`/`metadata` |
| usage stats | `GET /sandboxes/{id}/stats/usage` | 共用原生 current/saved/history Reader; 无损整数和 coverage, 在线 owner/离线锁, 不 Wake 或采样 |
| resource stats | `GET /sandboxes/{id}/stats/resource` | 只读最终资源规格和宿主 VMM counter,附可观测的节点 reservation;sparse JSON,不访问 guest |
| metrics history | `GET /sandboxes/{SandboxID}/metrics?start=...&end=...` | 精确 SandboxID ownership、opaque live telemetry UDS 转发；不可用为 503，不 Wake/Resume；[E2B 契约](telemetry_zh.md#6-e2b-历史查询) |
| traffic stats | `GET /sandboxes/{id}/stats/traffic` | 最终 node proxy 当前 parking/connected 与保守 `idleSince`;不 Wake/Resume |
| list | `GET /v2/sandboxes` | 仅本租户;query `state`/`limit`/`nextToken`,省略 state 时只列 running/paused,显式 state 可供内部故障诊断;分页头 `x-next-token`;每项含 `cpuCount`/`memoryMB`/`diskSizeMB`(`cpuCount`/`memoryMB` 的合同仍是 capacity/SKU,不改成 memory headroom)与 ISO-8601 `startedAt`/`endAt` |
| kill | `DELETE /sandboxes/{id}` → 204 | 非本租户 ⇒ 404;先把当前完整 owner 原子转为内部 `deleting`,从节点 cache/full snapshot 排除并在返回前发布 route Delete,再由 finalizer 取消 launch,fence runner,在 allocation fence 内 detach 并 durable exact-clear network tuple,删除 RunDir/BaseDir 并 hard-delete row;route Delete 只表示 projection withdrawal,pending 时重复调用幂等 |
| resume | `POST /sandboxes/{id}/connect` | body `{timeout:秒, memory?:bool\|null}`;`memory` 在本实现中 nil=auto,true=memory,false=cold;paused 在返回前原子变为 `starting` 并持久化 `launch_mode`;目标缺失时可携 `X-Kuasar-Migration-Token` 同步 import paused 后执行同一受理;返回不等待异步 launch |
| exec session | `POST /sandboxes/{id}/exec-sessions` → 201 | 只接受 `X-API-KEY`;为 native exec 签发一个 `execAccessToken`,body 可含 `ttlSeconds` 和 CEL `conditions`,并可携 `X-Kuasar-Migration-Token`;不创建 guest process |
| pause | `POST /sandboxes/{id}/pause` → 204 | body `memory` omitted/null/true 保存 Snapshot S,false 保存 Sandbox E;false 与 snapshot-only merge/drop 字段组合返回 400;已暂停或正在 starting 回 409 |
| timeout | `POST /sandboxes/{id}/timeout` | body `{timeout:秒}`,重置 TTL;starting 允许窄字段更新 |

兼容范围以实际验证的上游 API/SDK 版本和测试为准。2026-09-07 核验的 E2B 官方
[Create](https://docs.e2b.dev/api-reference/sandboxes/create-sandbox)、
[Pause](https://docs.e2b.dev/api-reference/sandboxes/pause-sandbox) 与
[Connect](https://docs.e2b.dev/api-reference/sandboxes/connect-to-sandbox) 均记录相关 memory 选择，
Connect 已有 `memory=false`；该字段并非 Kuasar 独有扩展。Kuasar 的 nil→按 E/S source 自动选择、
以及所有 paused E/S 可被授权流量 Wake 是本地语义；上游对 filesystem-only snapshot 的流量触发恢复有明确限制。
本文不宣称完整实现 E2B `autoResume` policy。

create 的 `templateID` 接受三种引用:持久 id(`<profile>-<kind>-<base64url-ref>`,[Build §2](node-build_zh.md#2-模板-id-与工件权威))、注册期
transient id、或已 ready 构建的 name/alias——后两者解析到持久 id 再走统一路径。
`envdVersion` 回 `0.6.1`(e2b)或 stub `0.1.0`(bare,≥0.1.0 否则 SDK 自毁)。bare 无
envd,不生成也不返回 Envd/Traffic token;两种 profile 都返回独立的
`forwardAccessToken`。该 token 在创建时签发并随 Sandbox 记录持久化。

Create 响应保持既有 body(不新增 `state` 字段),且与 cache 和后台 worker 使用不同的对象
副本。GET 可观察 `starting` 或内部 cleanup-pending `deleting`;默认 List 仍只列 running/paused,
显式 `state=starting|deleting|dead`
用于诊断。Connect 已是 running 时直接返回;已是 starting 时只应用显式 timeout 的窄更新,
不重复启动。paused Connect 在返回前完成 durable resume acceptance,因此成功响应可以对应
starting,但不会仍对应旧 paused 状态。paused/starting 上的显式 deadline intent 在同一
conductor 进程内的恢复失败回 paused 后继续保留,只在某次 exact-run 成功提交 running 后
消费。V1 没有持久 intent 字段,所以 Reconcile 可从仍为 starting 的中断 resume 推断并在
本次进程内保守恢复 intent,避免紧随其后的普通 Wake/Connect 被 node default 覆盖。如果该行
已经回到 paused 后 conductor 再次重启,数据库中已没有办法把它与普通 paused/default-rearm
区分;精确跨多次重启保留需要 #139 单独批准 schema discriminator,不在 #135 中用隐式编码或
sidecar 绕过。

Exec session 是显式授权动作,不是服务端 session 对象,也不启动 guest process.请求 body
是严格 JSON object,可含 int64 `ttlSeconds` 和 `conditions:[{"expr":"..."}]`.例如:

```json
{
  "ttlSeconds": 3600,
  "conditions": [
    {"expr": "request.argv == ['/usr/bin/python3', '/workspace/task.py']"},
    {"expr": "request.cwd == '/workspace'"},
    {"expr": "request.user == '1000:1000' && !request.stdio.tty"}
  ]
}
```

`conditions` 缺失或 `[]` 都表示 unrestricted,规范化为 `nil` 并从 token payload 省略;
显式 `null`、非数组、unknown/duplicate 字段、非 object 元素、元素中的 unknown/duplicate
字段和空 `expr` 均返回 400.多个表达式按 AND 组合;需要 OR 时在单条 CEL 中使用 `||`.
完整原始 body(含尾随空白)上限 64 KiB;第二个 JSON value、负数或越界 TTL 同样在生命周期
副作用之前拒绝.64 KiB + 1 返回 413.`ttlSeconds` 缺省或为 0 时 token 长期有效;为正数时
以实际签发时刻计算 `exp`,Unix 秒加法或 `time.Time` 表示溢出均返回 400.

standalone node 在编译 caller-controlled CEL 前先执行无副作用 credential preflight:既有目标验证
resource-bound APISecret,缺失且准备 KMT import 的目标验证 allowlisted credential pair;编译后
`prepareStandaloneTarget` 再次做权威校验以关闭并发变化,随后才可能 import/resume.
CEL 使用固定强类型 `request` view:`argv list(string)`,`env map(string,string)`,`cwd string`,
`user string` 和 `stdio.{tty,stdin,stdout,stderr} bool`;不暴露 route、claims、metadata、时间、
文件系统、网络、secret 或可产生 I/O/副作用的函数.签发节点在 token mint 和任何
paused→starting 变化之前编译、bool type-check 并执行静态 bounds;unknown field、非 bool、
超出 source/AST/cost bounds 均拒绝.这仍保留既有 eager exec-session activation 合同;
deferred activation 属于独立的 #240,本合同不实现它.

conditions 在目标准备和 lifecycle mutation 前完成编译.SID lifecycle fence 随后等待前一
attempt 的 terminal cleanup,再按实际签发时间生成 token,最后才允许新的 paused→starting;
因此 fence 等待不消耗 token TTL,编译或签名失败也不会产生新的 launch side effect.

目标已存在时不解析 migration token;目标缺失且提供该 token 时可以先同步 import 并完成对象,
credential binding 和 profile 校验.随后 node 以沙箱记录中的 ServiceSecret 签发 KAT;
MigrationToken 只作为本次同步 import 的瞬时输入,不写 Sandbox 业务行,日志或错误文本,
消费后立即丢弃.
paused 目标只接受异步 resume,响应不等待 READY.成功响应设置
`Cache-Control: no-store`,body 严格只有:

```json
{"execAccessToken":"kat1.<payload>.<signature>"}
```

响应不返回 session ID,过期时间,ServiceSecret 或其它 route/credential 字段.
鉴权失败沿用现有 401/403,沙箱不存在或不属于调用方返回 404;同步 import,
credential 读取/签发或异步 resume 任务接受失败统一对外返回脱敏 503,
错误文本不包含 fingerprint,ServiceSecret,NodeSandboxID,socket path 或 KAT payload.

#### 4.1.1 即时 resource / traffic stats

两个接口都先读取 Sandbox 业务记录并验证 API key ownership;失败统一 404。它们是只读观察,
不调用 Connect、Wake、Resume、Pause 或 envd,响应带 `Cache-Control: no-store`。

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

各宿主字段分别保留有效性:合法零值正常返回,缺失的 memory 或 CPU 观测分别省略. `timestampUnix` 是实际读取时间;两个宿主字段都没有观测时省略,不把 heartbeat/cache 时间刷新为当前时间. 没有 live VMM 时仍可从 owner 读取最终规格,但不伪造当前宿主观测. starting/running 没有可达 owner 为 503;paused 和其它没有当前 runtime 的状态为 409. 与 runtime 或 binding replacement 并发的读取失效.

静态/动态资源控制、usage 关闭、telemetry 停止时均可独立使用. 读取不 Wake,不采样/保存 usage,不调用 guest 或 Cloud Hypervisor,不修改控制策略,也不创建 resource 历史. 原生 API 的 `cpuCount`、`memTotal`、`memAllocatable` 和 `memUsed` 已删除;旧的 reservation 值 `memAllocatable` 由 `memoryReserved` 替代,`memoryHeadroom` 单独表达 headroom. E2B `/metrics` 和 list/SKU 兼容字段保持原有含义.

`GET /sandboxes/{id}/stats/traffic` 返回最终 node proxy 已鉴权接纳的逻辑 ingress:

```text
ingress = parking + connected
parking = 鉴权成功,但生命周期激活/最终 backend dial 尚未完成
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

e2b 的 service 集是 `forward/e2b:envd/e2b:code-interpreter/exec`,bare 是
`forward/exec`。顶层 `inflight` 是各 service 求和。每个 service 仅在两项为零时附
`idleSince`;顶层仅在 state=running 且全零时附所有适用 service 时间的最大值。
starting/paused 返回 inflight 但不返回顶层 idle。接口不返回 idle bool/duration、last
open/close、累计连接数、速率/延迟/端口明细或 worker 信息。

conductor 经当前 trusted Proxy registration 的 `stats_socket` 读取 master cache,查询时不扇出 worker.master 未注册,
route 未完成同步、RunID/profile/state 不匹配、worker stream 故障或 replacement 未 ready 为 503。
stats 的 503 窗口不影响 Proxy master 的 route/admission authority 或 Create barrier。完整共享
admission算法、误差证明、worker-local状态机、绝对快照 stream 和故障窗口见
[node-proxy.md](node-proxy_zh.md) §8。

响应为平铺结构: `state`、`maxInflight`、`inflight`、`idleSince`、`services`、`platform`、`transit` 和 `egress`. Conductor 拥有该原生 API,按照当前 Sandbox 到 switch/port 的绑定组织既有 Proxy 观测与网络计数. `platform` 映射 connector 的 Mgmt RX/TX 包数和字节数,`transit` 映射 Transit RX/TX;两者都采用沙箱视角. 已配置且当前有效的端口返回全部四项无符号整数,包括合法的 0. 没有 attached port 的沙箱返回空 `platform`/`transit` 对象,表示没有适用的当前观测. `egress: {}` 始终表示尚无可发布的 egress 统计,不是观测到零流量. 不发布 per-service 包计数、来源分组或 network API 别名.

`connected` 替代原生 `inflight.egress` 和 `services[*].egress`,保持后端连接已建立至最终 Close 的原有含义. 顶层 `idleSince` 仅描述 Proxy 已接纳 ingress;management 监控包不会刷新它,它不对沙箱计算或整个网络空闲作出结论. packets 是包数而非应用请求数;bytes 是观测帧字节而非吞吐速率. management 使用既有端口/management ingress 帧长度;transit 使用封装前或去除外层 GENEVE 头后的帧长度,保留以太网头. 不把不同观测点相加成总流量. 累计计数属于当前 attachment,端口复用后可以重置;API 不建立网络历史.

Conductor 每个 switch 最多批量读取 64 个当前端口,调用 connector Go `Stats(ports)`,不逐沙箱启动 CLI,不维护第二套生命周期权威. 它复用原有 allocation/detach fence;connector 复用 pin 目录共享锁、当前 pinned-map ID 和清零确认标记. 既有 Proxy stats socket 也支持有界批量. 已配置来源读取失败、清零未确认、控制锁占用、结果不完整或绑定变化时,整个读取返回 503,不会用 0 或旧样本伪装完整响应. 公开 API、可信本机 batch 与 conductor extension 共用相同领域读取. Stats 独立于 telemetry,不参与 Create/Resume readiness.

##### 原生 usage

Conductor 在 `/stats/resource` 和 `/stats/traffic` 之外发布
`GET /sandboxes/{SandboxID}/stats/usage`. Usage 读取 sandboxer 已有的原生生命周期
计量. Resource stats 描述当前生效规格和 VMM 观测; traffic stats 描述既有 Proxy
ingress 观测. 这些接口独立于 telemetry 及其
`/sandboxes/{SandboxID}/metrics` 历史查询. 原生账本格式和 runtime 查询合同由
[sandboxer usage](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md#usage-query)维护.

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
仅 owner socket 不存在或拒绝连接时允许离线回退. 成功连接之后, 协议错误、EOF、
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

#### 4.1.2 Create 身份

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
准入独立执行相同的 1..57 字节格式校验（§7）。

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

周边契约参见[集群 Router](cluster-router_zh.md)与
[扩展指南](extensions_zh.md)。


### 4.2 数据面协议(envd 与 native exec)

envd 单端口 **49983**,HTTP/1.1 与 h2c 双栈,Connect-RPC(proto 包无版本):
`process.Process`、`filesystem.Filesystem`(仅元数据);文件内容走 HTTP
`GET/POST /files`(+ 签名 query);另有 `/health`、`/init`、`/metrics`。每操作用户
经 `Authorization: Basic base64("user:")`。**code-interpreter** =
`POST https://49999-<sid>.<domain>/execute`(NDJSON)→ guest FastAPI(:49999) →
Jupyter(:8888)。exec = `POST /process.Process/Start`(头 `X-Access-Token`)。
租户数据面本组件只透传不实现;**构建流水线自带一个最小 `process.Start` 客户端**
(connect+JSON 信封手写,无 protobuf/gRPC 依赖)驱动 steps/startCmd/readyCmd([Build §5](node-build_zh.md#5-按目标执行与发布))。

guest 内 envd 的两个控制端口经 sandbox-ctl `--connect` 映射为 host UDS
(`<rundir>/envd.sock`、`ci.sock`),仅 proxy 可达——不走 floatingip,沙箱间无通路。

Native exec 不复用 envd HTTP/Connect-RPC.客户端先按 §4.1 取得 ExecAccessToken,
再建立一条 service-addressed CONNECT:

```http
CONNECT sandbox:443 HTTP/1.1
E2b-Sandbox-Id: <NodeSandboxID>
E2b-Sandbox-Service: exec
X-Access-Token: kat1.<payload>.<signature>
```

authority 的 443 只是 transport 占位,不是 guest port;即使请求同时携带
`E2b-Sandbox-Port`,Node 也不用它选择 backend.`service=exec` 只接受 CONNECT,
且始终强制验证 KAT,不受普通 proxy `off|log|enforce` 模式影响.最终 proxy 在回复
CONNECT 200 后严格读取并授权完整 `exec_request` 首帧;条件通过后才可以恢复 paused
sandbox、连接 `ctl.sock` 并原样转发首帧;详见
[node-proxy.md](node-proxy_zh.md) §5.

### 4.3 SDK / CLI 对接与协议 pin

- 重定向:`E2B_DOMAIN=<domain>` + `E2B_API_KEY`(生产,TLS);dev 走
  `E2B_API_URL`/`E2B_SANDBOX_URL`(http/h2c)。控制面要求 Host 命中 `api.*`。
- api_key 形态:`e2b_` + 72 hex(共 76 字符);e2b SDK 以 `/^e2b_[0-9a-f]+$/` 校验
  格式,服务端另验 MAC(§7)。
- envd 版本 pin:按 e2b-dev/infra 发布 tag 定版(guest-runtime/native-deps `ENVD_TARBALL`,默认
  `2026.22`,对应 envd 0.6.x);SDK:`e2b` js 2.27.x / py 2.25.x 实测兼容。
- 数据面鉴权头 `X-Access-Token`(= `envdAccessToken`):secure 沙箱自 SDK v2.0.0
  默认开,SDK 每次数据面调用携带。
- routesync(Proxy / 路由观察者):版本 8,帧 `[4B LE len][JSON]`,消息
  `register|hello|upsert|delete|bookmark|wake|route_barrier|route_barrier_ack`,路径
  `PUT /internal/plugin/{id}/register`(config-socket plugin 平面,§6;线格式 node-proxy.md §4).

### 4.4 沙箱配置传递链

每实例沙箱配置经**命名空间化的 e2b metadata 保留键** `kuasar-sandbox.<ns>`(各值一个
JSON 对象)注入,零 SDK/API 改动。这些独立的 typed 租户 schema 定义可配置子集，再由 renderer 生成 sandboxer 配置；
不能把完整 runtime YAML 当作租户策略直接导入：

| 命名空间 | 去向 |
|---|---|
| `resource` | 严格 partial patch:`resources.{capacity.{cpu,memory},allocatable.{cpu,memory},startup.memory}` |
| `traffic` | host-only 的 per-Sandbox `max_inflight.{total,forward,e2b:envd,e2b:code-interpreter,exec}` 显式 patch;不进入 guest |
| `network` | 拆分:`hostname`/`nexthop`→guest;`inner_ip`/`transit_*`→`vswitch.Attach`;`dns`→`/etc/resolv.conf` |
| `launch` | `launch.{exec,args,env,workdir,restart,user,stop_signal,plugin,cgroup_control}`——**仅 bare**;e2b profile 拒(envd 占用 launch) |
| `init` / `mounts` / `files` | 直透 `init[]` / `mounts[]` / `files[]` |
| `metadata` | `SANDBOX_CONFIG.metadata` 透传(如 `e2b.start_cmd`) |
| `restore` | 本次 host restore 的 `prefetch` 策略;可省略,显式值只允许 `off`/`memory` |
| `credentials` | 创建期 ServiceSecret、Envd/Traffic token override;解析后从普通 metadata 剥离,不进入 guest |
| `checkpoint` | host-only、仅本次 Create 的 local Pause 缺省:`merge_ref`/`drop_caches` 各自为 `true`/`false`/`null`;只存 sandbox row,不进入 runtime YAML 或 snapshot.cfg |
| `mmds` | portable exact `routes` + request-scoped initial `secrets`;持久化前拆分,metadata 最终只保留 routes |

`resource` 与 `traffic` 按 leaf 合并,而不是整段 namespace 覆盖。`resource` 的公开 JSON 只允许:

```json
{
  "capacity": {"cpu": 2, "memory": "8GiB"},
  "allocatable": {"cpu": 0.5, "memory": "256MiB"},
  "startup": {"memory": "1GiB"}
}
```

整个值必须是单个 JSON object。unknown、`null`、数组/scalar、trailing JSON、显式
零/负 CPU、空/非法/非正 memory 均返回 400,错误带完整
`kuasar-sandbox.resource.<path>`。request/template/group/header/migration token 都不能
携带 `allocatable.deflate_on_oom`、`overhead`、`watermark_high`、`control` 或
`sensor`:这些字段属于 node/runtime,其中 `watermark_high.ratio` 由 node policy 注入。
合法 patch 持久化为 compact canonical JSON。

固定的 leaf priority 是:

```text
node resource policy
  < cluster group defaults
  < create / reserve body resource
  < X-Kuasar-Sandbox-Resource
  < E2B first-class cpuCount / memoryMB (只覆盖 capacity 对应 leaf)
  < portable artifact capacity constraint
```

五个 leaf 独立 overlay；group 只设 `capacity.memory`、create 只设
`allocatable.memory` 时两者同时保留。Build row metadata 不是 Create overlay 层。其它
namespace 遵循下文各自的合并规则，多数采用 whole-namespace 覆盖。每一层都先
严格解析,所以合法高层不能隐藏非法低层。group/reserve、standalone/cluster 与 build
registration 共用同一 helper。

Build traffic 仅作用于本次 Build runtime 及其 synthetic route，不会被输出制品后续创建的
Sandbox 继承。canonical TemplateID Create 不查询保留的 Build metadata、Template catalog
或制品 metadata 来取得 traffic 默认值。`traffic` 的固定 leaf priority 是:

```text
cluster group explicit metadata (when present)
  < create / reserve body metadata
  < X-Kuasar-Sandbox-Traffic
```

其 JSON 形状为 `{"max_inflight":{"total":32,"exec":2,"forward":0}}`。每个
leaf 可独立省略;显式 `0` 保留并清除低优先级 patch 或目标节点 Proxy 默认限制。整个
metadata key 省略时保持省略,最终由目标 Proxy master 合并该节点
`traffic.max_inflight`;目标节点默认值不写入 Sandbox row、MigrationToken 或 Registry。
MigrationToken 已携 Metadata,所以 absent 在目标端继续 absent,显式 patch 原样迁移并改用
目标节点默认值补齐其它 leaf。unknown、duplicate、`null`、负数、非整数和 `uint32`
overflow 均返回 400。bare Sandbox 显式声明 `e2b:envd` 或
`e2b:code-interpreter` 拒绝;节点默认可以包含这些 service,bare 只消费
`total`、`forward`、`exec`。该限制是 inflight concurrency,不是 QPS 或 Proxy global capacity;
完整算法与误差边界见 [node-proxy.md](node-proxy_zh.md) §8。

最终 resolver 先确定 capacity,再解析 allocatable/startup,最后添加 node-only
overhead/watermark/deflate/controller。node policy 中省略的 allocatable.memory 超过最终
capacity 可安全收敛；node policy 或 request 的显式值越界拒绝。startup 与 allocatable 独立,request 显式值只需位于
`(0,capacity.memory]`;node 显式 startup 超过最终 capacity 时收敛到 capacity,双方都未
显式设置则取最终 capacity。static/dynamic 都渲染 startup,static 只省略 controller。
settled policy 需要 balloon(`capacity.memory > allocatable.memory`)时最终 YAML 显式
写入 `deflate_on_oom: true`;仅 cold startup target 非零时也会创建 balloon device,
并使用 sandboxer 的同一 effective true 缺省。

MMDS 在 Sandbox Create 与 Build Register 共用以下外部 schema。Header 直接携 JSON;
metadata 的 value 仍是一个 JSON string:

```json
{
  "secrets": {
    "key1": "data1",
    "key2": "data2"
  },
  "routes": [
    {
      "path": "/path/to/secret",
      "type": "secret",
      "secret": "key1",
      "content_type": "application/json"
    },
    {
      "path": "/path/to/relay",
      "type": "service",
      "service": "external-mmds"
    },
    {
      "path": "/path/to/data",
      "data": "value",
      "content_type": "application/json"
    }
  ]
}
```

`type` 缺省表示 static。static 必须有 `path`,可有 `data`/`content_type`,禁止
`secret`/`service`;secret 必须有 `path`/`secret`,可有 `content_type`,禁止
`data`/`service`;service 必须有 `path`/`service`,禁止 `data`/`secret`/`content_type`。
static/secret 未声明 Content-Type 时只在响应阶段取 `text/plain`;service 使用本机服务的
受校验响应 Content-Type。Create/Register initial value 必须是 JSON string,每个 name
至少由一条 secret route 引用;secret route 可以暂时没有 value。

Header 与 metadata 分别严格解析为 partial document,再按顶层 key 合并:

```text
effective.secrets = Header.secrets if present, else metadata.secrets
effective.routes  = Header.routes  if present, else metadata.routes
```

同 key 不拼接、不递归或按 path 深合并。显式 `"routes":[]` 与 `"secrets":{}` 也算
present,会清空低层值。两份 JSON 均拒绝 unknown field、duplicate key、trailing value、
malformed JSON 和超限输入。

校验后立即分离 portable routes 与 confidential values。exact absolute path 必须能按原字节
作为 HTTP request target 使用,禁止 query、fragment、percent escape、需 percent-encode 的字符、
空/dot segment、backslash、trailing slash、wildcard、duplicate
或 reserved path/prefix collision。持久化使用稳定最小 JSON:显式 `type:"static"` 被删除,
输入缺省的 `type`/`content_type` 保持缺省,不写运行时默认值,不增加外部 version。例如:

```text
input:     {"routes":[{"path":"/data","type":"static","data":"x"}]}
metadata:  {"routes":[{"path":"/data","data":"x"}]}
```

`metadata["kuasar-sandbox.mmds"]` 绝不包含 `secrets`。Build 专属作用域与终态清理见 [Build §3.1](node-build_zh.md#31-请求级-builder-输入)。

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

凭据 override 同样支持两种等价入口:

```http
X-Kuasar-Sandbox-Credentials: {"service_secret":"<64 lowercase hex>","envd_access_token":"...","traffic_access_token":"..."}
```

```json
{"metadata":{"kuasar-sandbox.credentials":"{...}"}}
```

Header 中存在完整 credentials object 时覆盖 metadata object,不做字段级合并。对象只允许
`service_secret`、`envd_access_token`、`traffic_access_token`;unknown、duplicate、null、
非字符串及 trailing value 均拒绝。bare 显式指定 Envd/Traffic token 返回 400。
Envd/Traffic override 必须是有效 UTF-8 且各不超过 256 bytes。
ServiceSecret 缺省从 APISecret 与 `StableID()` 派生;Forward token 始终由最终
ServiceSecret 自动签发,不能由请求指定。credentials 在验证后立即从普通 metadata 分离。

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
`null`/缺失则删除整个 namespace。该 namespace 不从 source template 继承；Build 的 checkpoint 输入见 [Build §3.1](node-build_zh.md#31-请求级-builder-输入)。`local` 与 `bundle` 使用完全相同的 policy 解析、覆盖和副作用边界。

`kuasar-sandbox.cluster` 不属于上述租户配置命名空间。cluster Build 的 group 使用独立
持久系统字段,不会进入 portable template metadata;普通 sandbox 的 `Profile`、`Group`、`RouteKey` 和可选
`StableID` 则通过 node-link 的结构化系统上下文下发并独立持久化,不进入用户
metadata。node 不在事件中回传 Registry 自有的 group、route key 或认证主体;Registry
通过节点归属记录恢复这些信息。

请求级 `kuasar-sandbox.builder` 的完整输入和目标解析规则见 [Builder 输入](node-build_zh.md#31-请求级-builder-输入)。下述规则继续定义共享 Sandbox 配置。

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
  Build 的共享 parser、目标准入与持久化由 [Build §3.1](node-build_zh.md#31-请求级-builder-输入) 完整定义。
  集群 Sandbox 所有权使用独立 node-link 系统上下文(§10)。
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
  Build 的模板网络投影见 [Build §3.1](node-build_zh.md#31-请求级-builder-输入)。MigrationToken 也携 metadata。
  除 host restore/checkpoint 策略外，其余配置在 cold launch 生效或已冻入 memory；network 还用于
  host 侧重建连接，因而保留 portable 网络声明。
- **host policy 不随快照或模板**:`kuasar-sandbox.restore` 与
  `kuasar-sandbox.checkpoint` 只由 create 请求写入 sandbox metadata。image cold boot
  不把它们渲染进运行 YAML;snp create、pause 后 resume 和 migration import 在存在
  restore ref 时重新渲染 restore policy;checkpoint 始终只由 host Pause 路径读取,
  不进入 `SANDBOX_CONFIG`/`snapshot.cfg`。connect/resume 不提供临时覆盖。
- **持久化**:`sandboxes.metadata_json` 是 Sandbox launch 配置；`builds.metadata_json` /
  `builds.builder_json` 是 retention-bounded Build 执行记录。后两者不会成为另一份模板权威。

## 5. 进程管理(systemd 模板单元,启动时自动生成安装)

serve 启动时生成并安装各配置模板及固定的两个 slice(`sandbox-runner.slice`、
`sandbox-builder.slice`)到 `units.dir`；同名同内容只写一次，同名不同内容在写入前拒绝。
内容变更才 `daemon-reload`(D-Bus `Reload`);
`units.install: false` 则交由运维带外管理,程序不生成文件,也不读取或校验运维资源属性. ExecStart 里的 `node-ctl` 路径
取自 serve 自身所在目录(自动发现,§3)。

runner 与 builder 分别在现有实际 Assign 边界按列表位置取池并推进独立游标。
锁只保护取池，不持锁等待 Assign。注册、被拒绝的 Build 准入及已绑定 RunID 不重新取池；
Resume 需要新 runner 时使用同一入口。不增加 idle 优先、坏池跳过、跨池回退、权重或资源调度。
一个 Build 全部阶段仍由同一 builder 执行；全局 registration/execution ledger、FIFO/claim
及释放顺序不变。NUMA/绑核由运维 unit 提供，conductor 不理解或验证，不新增池级 slice 或资源限额。

允许 StartUnit 前先登记 RunID → 创建它的 pool，让共享模板的 WaitAssignment 也精确路由。
另保留 RunID → 实际 unit 到生命周期清理，assignment 完成不删除执行归属。
仍先持久绑定 RunID 再发布 assignment，builder 重试先查持久 Build 绑定。
RunID 格式、pidfile/config socket、Delegate、ctl/vmm 及可信 FD 合同不变。

**runner 单元**(`%i` = run-id):

```ini
# sandbox-runner@.service (生成内容,路径按配置渲染)
[Service]
Type=exec
WorkingDirectory=/run/sandbox
ExecStart=<node-ctl> run-sandbox --pidfile=/run/sandbox/runners/%i.pid \
          --config-socket=/run/sandbox/node-ctl.socket --run-id=%i
ExecStopPost=/bin/rm -f /run/sandbox/runners/%i.pid
# 一进程一沙箱：有状态崩溃不重试
Restart=no
# 默认先 SIGTERM，TimeoutStopSec 后 SIGKILL；递归包含 CH（§5.1）
KillMode=control-group
TimeoutStopSec=20
Slice=sandbox-runner.slice
# 委派 cpu/memory controller（§5.1）
Delegate=yes
```

完整 Builder unit 生命周期与资源责任边界见 [Build §4.1](node-build_zh.md#41-builder-unit-与进程生命周期).

两单元的 ExecStart 都先锁 run-id pidfile,再经 config-socket WaitAssignment 等待
业务 id。runner 取得 sid 后立即连接 readiness,再锁 `<RunDir>/<SandboxID>.pid`,以
exact run-id取 bootstrap;artifact task在进程内完成 E/S prepare 和第二阶段后才取最终 LaunchSpec,
随后 `execve` 替换为 `sandbox-ctl run`(继承单元主 PID 与 cgroup,`Type=exec` 故无需
sd_notify)。Builder 的进程/子 VM 与结果回传见 [Build §4](node-build_zh.md#4-任务交接)。

serve 分别维护 runner/builder 的目标 idle 数量。分配会消费一个已进入 WaitAssignment
的单元并立即登记异步补池;无 idle 时也生成 run-id、按同一路径启动单元并等待其
WaitAssignment,不存在另一套直接启动模型。Start/Stop 请求只由一个固定控制循环串行
执行,状态循环通过 channel 投递请求,不为每次补池派生启动协程。`pool_wait_timeout` 覆盖
`StartUnit` 调用到 WaitAssignment 的完整区间;启动失败、等待超时或等待连接取消都会
`StopUnit` + `ResetFailedUnit`,再生成新的 UUIDv7 run-id 补足目标数量。沙箱 launch 的
runner assignment budget 从调用 pool Assign 起覆盖排队、按需 StartUnit 和
WaitAssignment;匹配 idle runner 后,pool 先调用 commit callback 绑定 run-id,commit 成功
才把 task ID 发给 runner 并向 Assign 调用方返回成功。commit 是 assignment 线性化点:
其后发生的 caller cancel 不得把成功翻转为 canceled;pending cancel 与 pool shutdown 由
pool loop 唯一回复,每个请求恰有一次结果。



- **kill**:在 SID lifecycle fence 内把完整 owner exact-CAS 为 `deleting`→取消 active launch→
  撤销 cache 并发布 route Delete→异步 `StopUnit`（默认先 SIGTERM，超时后 SIGKILL，递归包含 CH）→`ResetFailedUnit`→
  tapfd `RELEASE` 或 `connector-ctl vswitch detach`→删运行目录→hard-delete row.route Delete 不等待
  这些 finalizer 步骤.launch claim 仍保留到旧
  attempt 完成其局部资源清理,因此迟到 CAS 不能复活该行,同 SID 也不能提前启动后继 attempt。
  `kill` 在进程层生效,不受 guest 内 restart 策略阻挡。
- **就绪**:serve 在分配 runner 前先绑定 `<RunDir>/ready.sock`(目录 0700、
  socket 0600),分配后 runner立即连接;serve同时等待 snapshot completion、readiness EOF、
  attempt cancel与绝对 deadline,因此task在根读取期间退出会立即失败。随后从node-ctl的
  one-shot连接严格读取
  `control_ready\nready\nEOF`;bare 到此启动成功。e2b 随后把 `POST /init` 作为首个 envd
  请求,不以 `/health` 作为启动门槛;health 仅在初始化完成后用于外部存活检查。restore
  launch的单一绝对 budget从 Assign成功时开始,覆盖 task根读取、completion RPC、host
  resource/network准备、final wait、exec/startup、runtime wire与mandatory `/init`,最终 spec
  生成后不重置。cold path仍在runner handoff后使用现有runtime budget。首个 `/init`立即发出;仅连接/传输
  错误按 1ms,2ms,4ms,5ms 上限退避重试,每次请求最多 50ms.只有 204 表示成功;
  非 204 只返回状态码,不记录可能回显 access token/用户 env 的响应体;协议错误、提前
  EOF、取消或总预算超时都返回
  launch 失败:create 将已持久化的 starting 行按 run-id fence 标 dead,resume 则回退
  paused;durable acceptance 后即使尚未分配 runner,失败也按空 run-id fence 收敛到该终态。
  只有 `/init` 成功后才以同一 run-id 把
  starting CAS 为 running 并发布 running route.
- **存活权威**:按去重后的各配置 runner 模板调用 `ListUnitsByPatterns`，取得权威存活 run-id 集,再与库内 `sandboxes.run_id` 对账(§14)。
- 宿主 `Restart=no` 与 guest 内 envd `restart=always`(sandbox-init 管)是两层,
  互不相干。

### 5.1 cgroup(ctl/vmm 隔离与 FD capability)

runner unit 的 cgroup 根只作为委托边界,不放进程:

```text
sandbox-runner@<run-id>.service/
├── ctl/   node-ctl,exec 后为 sandbox-ctl
└── vmm/   cloud-hypervisor
```

`node-ctl run-sandbox` 在等待 assignment 前只接受当前进程位于对应 run-id 的 runner
unit 根或其 `ctl/`。前者创建 `ctl/` 并通过 `cgroup.procs` 将自身完整迁入,随后重读
`/proc/self/cgroup` 验证身份;后者用于运维带外安装的已预置单元。两条路径统一验证 unit
根无进程及 cpu/memory 委派,再于 unit 根启用 controller,幂等创建 `vmm/` 并以 CLOEXEC
打开目录 FD。该方式只依赖 `Delegate=yes`,不要求 systemd v254 才提供的
`DelegateSubgroup=`。取得 LaunchSpec 后,启动器在最终 exec 前才使该 FD 可继承,本地
追加 `--cgroup-path=fd=N`;LaunchSpec 自身不携带任何主机 cgroup 路径或 FD。

sandbox-ctl 接收 FD 后立即恢复 CLOEXEC,写入资源上限,并以
`clone3(CLONE_INTO_CGROUP)` 把 CH 原子创建到 `vmm/`。因此 `memory.high` 只限制
VMM,sandbox-ctl 在 guest 压力下仍可处理 UFFD、vsock、信号和进程回收。具体 high
值、balloon target/current 和 grow/shrink 顺序全部由 sandboxer 本地控制;node
controller 只处理它发起的 reservation 请求。
节点要求 Linux 5.7+ 且 seccomp 允许 `clone3`;不满足时 sandbox 启动 fail closed,
不回退到启动后迁移或共享 cgroup。

`KillMode=control-group` 递归覆盖 `ctl/` 与 `vmm/`;StopUnit 后 systemd 回收整个委托
子树,无需 serve 单独搬迁进程或 rmdir。任何委托、层次、controller 或 FD 校验失败均在
CH 启动前 fail closed。

### 5.2 日志:journald 单汇 + 标签词表

Conductor 为 runtime component、app stdio 与 guest console 显式构造独立 journald targets;
普通 Sandbox 的每个 managed target 均携 `KUASAR_STABLE_ID`、`KUASAR_SANDBOX_ID`、`KUASAR_RUN_ID`,
Build phase target 携 `KUASAR_BUILD_ID`、`KUASAR_RUN_ID`。字段不从环境、unit 或制品推导,
也不在输出之间继承。完整身份规则、target 编码、查询与发送边界由
[journal 身份指南](node-journald_zh.md) 维护;运行时参数由
[sandboxer journal 指南](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md#journal-output-targets) 维护。

| 标签 | 写者 | 内容 | 去向 |
|---|---|---|---|
| `sandbox-ctl` | runner/Build phase 的 `--log-to` | runtime component 诊断 | 仅宿主排障 |
| `sandbox` | runner 的 `--stdout-to`/`--stderr-to` | 沙箱 app stdio | 仅宿主排障 |
| `build` | Build phase/run-builder | app stdio、里程碑、envd RUN 回放与 flatten 进度 | SDK 与宿主;完整读取/分页/失败语义见 [Build §5](node-build_zh.md#5-按目标执行与发布) |
| `console` | runner/Build phase 的 `--console` | guest 内核 dmesg | 仅宿主;不进入 SDK Build 流 |

所有这些 native stream 直写 journal,不经临时日志文件。run-builder 里程碑与 envd RUN 回放使用
pure-Go `go-systemd/journal`。`--log-to` 不替换原始进程 stderr;早期 CLI、CH stderr、panic 与
发送失败 fallback 保留原有出口。Builder 单元设置 `LogRateLimitIntervalSec=0`,runner 保留默认限流;
这不保证 journal/storage 故障下零丢失、恰好一次持久化或零阻塞发送。

<a id="numa-deployment"></a>

### 5.3 使用多池进行 NUMA 部署

多池的主要部署场景是：让单个 Sandbox 的宿主侧执行保持在一个 NUMA 节点内，
同时把新的执行分散到多个节点。Conductor 选择 pool 及其 systemd 模板；运维在模板上
配置 CPU 放置与内存分配策略。这是宿主侧部署能力，不是 guest vNUMA 拓扑，
也不是 NUMA 感知的容量调度器。

**NUMA 隔离是基于多池机制的上层应用模式，不是 Conductor 新增的隔离能力。**
以下是部署配置示例，不是项目测试规范。项目只验收多池配置、独立预热、简单轮询、
实际 unit 定位及既有生命周期，并静态检查示例与文档一致性；不要求或安排真实
NUMA 主机验证、页分布检查、NUMA E2E 或性能测试，也不将其列为合入前置或遗留工作。

对具有在线且带内存的 NUMA 节点 0、1 的主机，用不同模板名称表达不同放置策略。
以下内容替换完整 Conductor 配置中的 `units` 块；不要同时保留旧单池字段。
示例 size 只是空闲预热目标，不是推荐容量：

```yaml
units:
  dir: /etc/systemd/system
  install: true
  pool_wait_timeout: 5s
  runner_pools:
    - {unit: sandbox-runner-numa0@.service, size: 4}
    - {unit: sandbox-runner-numa1@.service, size: 4}
  builder_pools:
    - {unit: sandbox-builder-numa0@.service, size: 1}
    - {unit: sandbox-builder-numa1@.service, size: 1}
```

**先读取实际主机拓扑。** NUMA 节点号和逻辑 CPU 编号（含 SMT 线程）由机器决定，
不能根据节点号推算 CPU 范围。确认 cgroup v2、`cpuset` controller 可用，
且祖先 slice 允许两个目标节点及 CPU 集合：

```sh
lscpu -e=CPU,NODE,SOCKET,CORE
numactl --hardware
cat /sys/fs/cgroup/cgroup.controllers
cat /sys/devices/system/node/node{0,1}/cpulist
```

**在启动 Conductor 和任何预热 worker 之前，安装运维管理的放置策略 drop-in。**
在示例双节点主机上以 root 执行下列命令；所选节点必须有在线 CPU 和内存。
示例使用节点完整 CPU 列表；需要为宿主服务保留 CPU 时，应将 `cpus` 改成部署选定的子集：

```sh
set -eu
unit_dir=/etc/systemd/system
for node in 0 1; do
  cpus=$(cat "/sys/devices/system/node/node${node}/cpulist")
  test -n "$cpus"
  for kind in runner builder; do
    dropin="$unit_dir/sandbox-${kind}-numa${node}@.service.d"
    install -d -m 0755 "$dropin"
    cat > "$dropin/20-numa.conf" <<EOF
[Service]
CPUAffinity=
CPUAffinity=$cpus
AllowedCPUs=
AllowedCPUs=$cpus
NUMAPolicy=bind
NUMAMask=
NUMAMask=$node
AllowedMemoryNodes=
AllowedMemoryNodes=$node
EOF
  done
done
systemctl daemon-reload
```

`CPUAffinity` 设置初始 CPU 亲和性；`AllowedCPUs` 通过 cgroup v2 约束整个 unit 子树。
`NUMAPolicy=bind` 配合 `NUMAMask` 在 worker 创建线程/子进程前设置分配策略；
`AllowedMemoryNodes` 约束 unit 允许使用的内存节点。这些是放置策略，**不是**
`CPUQuota`、`MemoryMax`、`MemoryHigh`、每池 reservation 或 CPU 独占配置。
Sandbox 的运行时资源执行仍由 `sandbox-ctl` 管理。

`install: true` 时，Conductor 生成各个命名基础模板及现有的两个固定 slice，
独立管理的 `.service.d/20-numa.conf` 提供放置策略。不要把自定义属性写进会被
Conductor 重写的基础文件。
采用 `install: false` 时，运维应基于本版本生成的 runner/Builder 模板（§5 和 Build §4.1），
安装四个独立的实际模板文件，以及 `sandbox-runner.slice`、`sandbox-builder.slice`，
然后自行 reload systemd。保留当前部署的二进制路径、`%i` **RunID**、pidfile/config socket、
`Delegate=yes`、`ctl/vmm`、清理行为及既有 slice 合同。不要用 unit alias 表达不同绑定。
该模式下 Conductor 不执行任何模板安装或 reload。

普通 runner 在两个 runner pool 间轮询；Builder 在两个 builder pool 间独立轮询。
预热进程在分配业务前已处于对应放置策略下。Runner 的 exec 替换及 Builder 的子进程
留在选定 unit 下；一个 Build 实际执行的 A/B/C 阶段仍由同一个 Builder 驱动，
不按阶段重新取池；guest 内的工作通过这些阶段 VMM 执行。
不要只给 Conductor daemon 绑核：unit 由 systemd 启动，放置策略必须配置到任务模板上。
参见 [Build §4.1](node-build_zh.md#41-builder-unit-与进程生命周期)。

部署仍遵循以下边界：

- 两个 pool 共用一个模板，就共用该模板的放置策略；重复项不会自动绑定到不同节点。
  `size` 不改变轮询权重，也不限制活动 worker。分配次数接近不代表 CPU/内存负载均衡，
  更不代表按 NUMA 节点做准入；现有节点全局准入与 Build FIFO/claim 保持不变。
- 已绑定执行保留实际 unit，Conductor 重启也不改变它。Pause/resume 需要新 runner 时
  重新取池，可能使用另一 NUMA 节点；本特性不提供 NUMA 粘性、在线迁移，也不把宿主
  节点身份写进可移植制品。活动或待清理执行仍依赖的模板必须保留在配置中。
  修改/reload drop-in 不会让已有预热或已分配 worker 重新 exec 并取得新的进程策略；
  应按生命周期安全安排 worker 更替，不要当成热迁移。
- 严格单节点绑定可能在其他节点仍有空闲内存时触发本地回收或分配失败/OOM；
  Conductor 不会自动换池重试。如有意用 `NUMAPolicy=preferred` 允许内存回退，
  必须同时去掉或扩大单节点 `AllowedMemoryNodes`，因为 cpuset 限制优先。
  这是以严格内存本地性换取回退，不是新增 NUMA 调度。

设置语义参考：[systemd.exec(5)](https://manpages.debian.org/trixie/systemd/systemd.exec.5.en.html)、
[systemd.resource-control(5)](https://manpages.debian.org/trixie/systemd/systemd.resource-control.5.en.html)、
[Linux NUMA memory policy](https://docs.kernel.org/admin-guide/mm/numa_memory_policy.html)、
[cgroup v2 cpuset](https://docs.kernel.org/admin-guide/cgroup-v2.html#cpuset)。

## 6. 本机控制 socket(run / task / admin / plugin / api 平面)

serve 在 UDS `paths.config_socket`(默认 `/run/sandbox/node-ctl.socket`,**0600**)
跑一个 h2c HTTP 服务(兼容 HTTP/1.1):单 socket 复用五个平面、各自鉴权。连接建立时
经 **`SO_PEERCRED`** 取 peer pid 注入请求上下文;socket 0600 ⇒ 仅同 uid / root 可连,
各平面在此之上再细分。`/internal/*` 前缀 e2b SDK 永不使用,与 api 路径不冲突。

**Run 平面**提供 `POST /internal/run/assignment`、`/internal/run/build-result` 与
`/internal/run/build-phase`，用于预启动单元等待任务及 Builder 回报 phase/result。
`SO_PEERCRED` 的 peer PID 必须与 `runners/` 下已锁定 RunID pidfile 一致，report 还校验 exact task owner；
它不同于 task 平面的业务 ID pidfile 鉴权，见 [configsock/server.go](../internal/configsock/server.go)。

**① task 平面** — 启动器取工作规约。sandbox 与 artifact-backed builder 都使用 exact-run
两阶段端点，cold/image 路径保持单次 bootstrap fast path:

- `POST /internal/task/sandbox/bootstrap`(run-sandbox;req `{sandbox_id,run_id,version}`)
  先取得不含秘密的 exact-run pidfile identity,完成 `SO_PEERCRED` + pidfile认证后才调用
  secret-bearing provider。cold image返回 `Final LaunchSpec`;artifact launch返回
  `ArtifactPrepareSpec` + env。task完成 E/S prepare 后向
  `POST /internal/task/sandbox/prepare` 提交 `{sandbox_id,run_id,summary}`并等待最终
  **LaunchSpec** `{exec,args,workdir}`。相同 digest replay等待/返回同一final result且不重复
  host side effect;冲突 replay返回409并fail closed;单次HTTP断开只取消该wait。
  `exec=sandbox-ctl`,`args=[run --sandbox-id <sid> --path-id <sid>
  --config <RunDir>/<sid>.yaml --manifest-config <shared>
  --run-root <RunRoot>/sandboxes --base-root <BaseRoot>/sandboxes
  (--from <E>|--restore <S>) (--connect <uds:ip:port>)…]`。`--path-id` 与两个调用级 root
  把 sandbox-ctl 的 socket/staging 目录(`ch.sock`/`ctl.sock`/…)钉到持久化的 RunDir/BaseDir；
  pause/snapshot/exec 客户端用同一 PathID 才能拨到 `ctl.sock`。node-ctl 在最终 exec
  时另行强制追加本机 `--cgroup-path=fd=N`,不允许 LaunchSpec 覆盖。
- Build bootstrap/prepare 的完整版本、payload 与恢复约束见 [Build 任务交接](node-build_zh.md#4-任务交接)；下述身份和消息平面规则同时约束 Sandbox 与 Build。
- **鉴权**:sandbox/build端点先以业务 id + exact run-id 查不含秘密的 task identity，再校验
  peer pid ⟷ 已锁定 task pidfile；认证通过后才调用可解密 `MANIFEST_KEY`/registry credential
  的 provider。builder 使用 `BuildRunDir/builder.pid`，stale run 或未授权 peer
  均不能触达 secret-bearing provider。
- 设计意图:**根凭据不进入配置文件**(`<sid>.yaml` 为 0600;构建 phase YAML 为 0600,
  由 run-builder 写进 0700 的 phase RunDir)、**根凭据与 pull credential 走 spec env**，加密存储并仅在受信运行期使用；
  调用方 workload env/files 本身仍可含敏感内容，不能把它们笼统视为非敏感配置。

**② admin 平面** — `/internal/admin/manifest-keys`(`GET` = list,`POST
{op: add|remove|check, manifest_key, api_secret?, label, ttl_seconds, registry_auth}`):
APISecret+ManifestKey 凭据对白名单管理(§7);响应和 list 只返回两者的完整指纹。
鉴权:配 `paths.admin_pidfile` 则 peer pid 须在其中;未配则仅靠 socket
0600。`node-ctl manifest-key` 即此平面客户端;集群下 node-link 心跳维系收到的原子
凭据对 `key_put` 写入 / 重发续租;未续租条目按 TTL 淘汰,`key_drop` 只作为
best-effort 清理命令(§10)。

同一 admin 平面还提供 sandbox-local MMDS route value 更新:

```http
PUT    /internal/admin/sandboxes/{id}/mmds/secrets/{name}
DELETE /internal/admin/sandboxes/{id}/mmds/secrets/{name}
```

`name` 必须被该 sandbox 当前的一条 secret route 引用。PUT body 是受
`mmds.routes.max_secret_value_bytes` 限制的 opaque bytes,完整替换当前 value;DELETE
幂等。成功均回 204,不返回 plaintext。API 不定义 TTL/expiration 参数或 header,不从请求
Content-Type 派生 value metadata;响应 Content-Type 始终属于 route。store CAS 成功后才
发布 Upsert 使 Proxy 收敛,失败时不发布伪状态.日志只记录 sid/name/outcome,
绝不记录 body。鉴权仍是本 socket 的 0600 + SO_PEERCRED + 可选 admin pidfile。

**③ plugin 平面** — `PUT /internal/plugin/{id}/register`:一个订阅者(Proxy
master,或路由观察者如平台 agent)注册其能力并**持挂该 h2c 连接**——连接本身即它的
租约 + 路由流(routesync,线格式见 node-proxy.md §4).请求体首帧是 `register{caps}`,之后(route_wake)是
`wake` 或 `route_barrier_ack` 上行;响应体下行
`hello(policy) → upsert* → bookmark → upsert/delete/route_barrier`。Wake 与 barrier ACK
经同一个上行 writer 串行发送。能力相互
**独立,不强制组合**:`subscribe`(`route` | `route_wake`),`proxy{stats_socket?}`
(可信 Proxy registration marker 及可选 traffic stats endpoint),`mmds`.route barrier participant
只要求固定 `proxy` id,`route_wake` subscription 与 `proxy` marker,不依赖 stats socket.**断连即反注册**;同
id 二次注册自动反注册(并断链)前者。鉴权:配 `paths.plugin_pidfile` 则 peer pid 须在
其中,未配则仅靠 socket 0600(同 admin).Proxy master,平台 agent 均经此订阅.
此平面是**节点本地** UDS,与接入集群的 node-link(§10,跨网 mTLS)正交。

MMDS confidential 投影不是任意 plugin 自报的权限。只有 id 精确为 `proxy`,且同时满足
`subscribe.kind=route_wake`、`proxy!=nil`、`mmds=true` 的 registration 才收到 conductor
MMDS service registry、`mmds_routes` 与 `mmds_route_secret_values`;普通 route observer
两者均不收,node-link/cluster stream 也不收。伪装为其它 id 的 `mmds=true` registration
被拒绝.Proxy master 在完整 Bookmark 前 fail closed,断流即清空可变 route/value/service
视图,不会无限期服务断连前的 secret。

**④ api 平面** — 其余路径回落到 e2b 控制面 handler(与 TLS `api.listen` **同一个**
`http.Handler`,含 export/import 扩展),明文 h2c、`X-API-KEY` 鉴权。
`export-sandbox`/`import-sandbox` CLI 即此平面客户端(§8.1)。

**信任模型**:host root / daemon uid 可信;租户代码在 guest 内,够不到 host UDS。

### 原生 stats batch

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

## 7. 密钥与归属模型(APISecret + ManifestKey 凭据对,加密存 sqlite)

- **StableID 与本地 ID**:`Sandbox.ID` 是当前 node-local store/runtime/route lookup key；
  `StableID()` 是跨 node-local ID 变化保持不变的 sandbox identity。standalone 普通 create
  未显式物化 `StableIDValue`，因此 helper 回退为本地 `Sandbox.ID`；standalone import
  指定不同 target ID 时只改变 `Sandbox.ID`，保留 source StableID 和全部 service
  credential。身份保持型 migration/copy 可以产生多个共享 StableID 的 node-local row，
  `stable_id` 没有 UNIQUE constraint，也不提供本地反向 lookup。cluster 中本地 ID 是
  NodeSandboxID，StableID 等于 Registry 的公开 SandboxID。同节点 resume 保持两者；跨节点
  migration/re-place 只更换 NodeSandboxID。Forward/Exec KAT 的 canonical `sid` claim 始终
  绑定 StableID；这不把 copy 定义为 independently authorized fork。
  发布位置也按 StableID 键控（§8.1.4）：import 改变 target ID 后再次导出仍进入原目录，
  各个 cluster generation 也共享该目录。Import 准入使用 `types.ValidLocalSandboxID`
  校验 token 携带的 StableID，非法身份在导入时拒绝，避免延后到导出时才因 Resolve 失败。
- SQLite schema 直接使用 `stable_id`，不探测或迁移旧列，也没有双读/双写；不兼容的
  旧 preview database 必须按 schema 边界明确重建；这不是项目未曾发布 Stable Release 的声明。
- **根凭据分工**:APISecret 与 ManifestKey 是同一租户范围内用途分离的根凭据,都是
  32B / 64-lowercase-hex。APISecret 用于 API 请求认证和派生 Sandbox ServiceSecret;
  ManifestKey 是 manifest 内容键(`MANIFEST_KEY` env / `manifest.key`),也用于封装镜像
  拉取令牌。创建凭据对时可显式给出 APISecret;
  缺省值采用固定、域隔离的 KDF:

  ```
  APISecret = HMAC-SHA256(decodeHex(ManifestKey), "kuasar-api-secret-v1")
  ```

  **api_key 只由 APISecret 签发和验证**(`e2b-key-ctl gen-apikey`):

  ```
  api_key = "e2b_" + hex( fp(12) ‖ ts(4) ‖ nonce(4) ‖ mac(16) )      # 76 字符
  fp  = SHA256(APISecret)[:12]                                       # 候选预筛,不是唯一身份
  mac = HMAC-SHA256(APISecret, fp‖ts‖nonce)[:16]                     # 无 APISecret 不可伪造
  ```

  e2b SDK 以 `/^e2b_[0-9a-f]+$/` 校验 api_key 格式(故用 hex 编码);serve 另
  自校验 MAC(`internal/apikey`)。APISecret 与 ManifestKey 本身都不发给 SDK。
- **加密落盘**:`manifest_keys` / `sandboxes` / `builds` 三表均保存完整凭据对;
  `api_secret_enc`、`manifest_key_enc` 使用 AES-256-GCM(`internal/secretbox`:
  记录 = `keytag(4)‖nonce(12)‖ct+tag`,keytag 选解密钥)。两者各存完整 64-hex
  SHA-256 指纹;API key 内的 24-hex 短指纹只作候选预筛,不作唯一身份。
  加密密钥经 `encryption_key` / `NODE_CONFIG_ENCRYPTION_KEY`(`:` 分隔多键,[0]
  活动、其余备用解旧记录,支持轮换)。
- **鉴权解析**(短 hash 匹配 + 完整 MAC 校验):api_key → 按 APISecret 候选指纹
  命中行/白名单 → 解密 APISecret → 重算 HMAC 比对:
  - **create / build / import**:APISecret+ManifestKey 凭据对须在 `manifest_keys` 白名单
    (`node-ctl manifest-key`,§2.6;daemon 是该表唯一写者;集群下 registry 经
    node-link 租约写入,§10),否则 **403**。
  - **其他按 id / list 操作**:只对资源行自身的 APISecret 校验,不查白名单——即
    清空白名单,存量 sandbox/build 仍可正常操作直至生命周期结束。
  - list 的 hash 预筛非唯一,逐行再验 MAC,杜绝 hash 碰撞串租户。
- **业务记录复制**:create/build/import 插入时把完整 pair 复制进 sandbox/build 行。
  后续 allowlist add/drop/TTL 或 provider 凭据更新只影响新插入记录,不重绑既有业务记录。
- **与收敛加密的关系**:manifest_key 只封 manifest 的密钥表;chunk 加密密钥派生自
  `SHA256(salt‖明文)`.共享同一 store salt 的 writer 属于同一内容共享安全域,域内相同
  明文可以复用 physical object;需要隔离的租户或部署必须使用 `extra_salt` 或独立
  store/salt domain.租户之间不共享 manifest key、APISecret 或模板,内容相同也不自动
  扩大凭据和模板边界.
- **根凭据不落明文**:sqlite 内加密;运行期只在必要的进程内存、受保护路由投影和进程 env 中。
  sandbox ManifestKey只在exact-run bootstrap认证后投递,runner以它覆盖继承环境中的同名项,
  且最终exec env只有一个authoritative `MANIFEST_KEY`。conductor可持久化/投递该值,但不调用
  sandbox/build host preparation中不调用CustomerKey、不建立key-bound reader、不打开/解密/解析snapshot。
  `<sid>.yaml`非密不含根凭据。
  builder task同样只在认证后取得 authoritative env；run-builder仅把`MANIFEST_KEY`安装为
  process-wide reader authority，并先清除继承的registry credential keys。registry凭据只保留在
  本地BuildSpec中，定向传给需要它的host flatten调用或guest import命令，不被phase
  `sandbox-ctl`及其后代环境继承；最终仍只有一个`MANIFEST_KEY`条目。
  APISecret 由 serve 用于 API 认证和 ServiceSecret 派生,并可投影给可信 router/proxy;
  不下发给 guest 或业务进程。auto-resume从资源行解密ManifestKey仅用于认证后投递给runner。
  集群下
  node-link 原子下发完整凭据对,
  两者同样仅入加密存储 + 运行期内存(§10)。
- 每个 Sandbox 持久化独立 ServiceSecret。未 override 时按
  `HMAC-SHA256(APISecret,"kuasar-service-secret-v1:"+StableID())` 派生;保存后不再重建。
  e2b 的 `envdAccessToken`/`trafficAccessToken` 可 override,否则分别随机生成;bare 两项恒空。
  EnvdAccessToken 用于 e2b 49983/49999,TrafficAccessToken 仅供外部网关及 e2b 数据面组件
  验证,不由 node 平台层消费。两个 e2b opaque token 均限制为有效 UTF-8、最多 256 bytes,
  在写业务行前校验。e2b/bare 的 `forwardAccessToken` 均为 ServiceSecret 签发、绑定
  StableID 且 `aud=forward` 的严格 `kat1`,用于其他 forward 目标。四项凭据均加密
  落盘并在 lifecycle upsert 中不可重绑。
  ExecAccessToken 不在 create/get/list 中缺省生成,也不写 Sandbox 业务行;它只由
  `POST /sandboxes/{id}/exec-sessions` 按次签发.每个 token 使用 UUIDv7 `session_id`,
  线格式为 `kat1.<base64url-no-padding(payload)>.<base64url-no-padding(signature)>`,
  payload 的 canonical field 顺序为 `v,session_id,sid,aud[,exp][,conditions]`,其中 `sid=StableID()`,
  `aud=exec`,不包含 `iat`.signature 以解码后的 32-byte ServiceSecret 直接执行
  HMAC-SHA256,不另派生 exec key.`conditions` 是按 API 顺序保存的紧凑字符串数组,
  unrestricted 时省略;它与其它 payload 字段一起受 HMAC 覆盖,不另加 digest.KAT holder
  可以解码 payload,所以 condition 不具保密性,表达式不得含需要保密的字面量.payload 不含 generation,node ID 或
  route revision.Node 是唯一签发方;Router/proxy 只使用受保护 route 验证.验证严格检查
  3 段线格式,canonical no-padding base64url,32-byte signature,固定 JSON 字段/顺序,
  UUIDv7,SID,audience 和可选 expiry.session ID 只在 KAT 内部使用,不在 API 响应中外显.
  `MmdsSecret =
  HMAC-SHA256(manifest_key, "kuasar-mmds-v1:"+sid)`(每沙箱确定性派生的 MMDS 会话签名
  密钥,`mmds.enabled` 时随路由分发给 proxy 校验 envd 身份,见 node-proxy.md §7)。
- **MMDS route value 独立存储**:`MmdsSecret` 只签名/验证 MMDSv2 token;
  `MMDSRouteSecretValues` 才是 secret route 返回的敏感 value 集合。每个 sandbox/build
  owner 各有一份 encrypted JSON blob,只编码实际 name→opaque bytes,不保存 version、
  Content-Type、TTL 或 configured/wait 状态。AAD 至少绑定 owner kind、owner id、canonical
  routes digest 与 revision;更新使用 CAS/revision,轮换密钥沿用 secretbox 活动键 + retained
  decrypt keys。Create/Register 在一个 sqlite transaction 内写业务 row、routes-only metadata
  与 initial blob;Sandbox 删除走 FK cascade,Build 终态/清理删除 blob。数据库中只出现
  ciphertext,plaintext 只在受信 conductor/proxy heap 的有界生命周期内存在。

## 8. 生命周期与状态机

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
Create(img|sbx|snp)
  |
  v
starting --success--> running --Pause/TTL--> paused(source=E|S)
  |                      |                       |
  |                      +--Kill--> deleting    | Resume(auto|memory|cold)
  |                                              v
  +--fresh failure--> dead                    starting(launch_mode=cold|memory)
                                                   |
                              running <--success---+
                                                   +--failure--> paused(original E|S)

starting|paused|dead --explicit Delete--> deleting --finalizer success--> absent
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
unit identity,port,RunDir,BaseDir 与制品 owner,取消 attempt 并立即撤销节点 cache 与后续 full
snapshot activation,同时发布 route Delete 撤销既有 live projection.该 Delete 不证明本地资源已完成清理.
请求在 durable acceptance 后返回;节点 finalizer 等待 late launch owner 退出,再按
Stop/Reset + inactive readback,allocation fence 内 Detach + full-owner exact CAS 清空 network tuple,
RemoveAll RunDir,RemoveAll BaseDir,exact hard-delete 收敛.只有 durable network clear 成功才释放
detached-port fence;其后的目录或 hard-delete 故障继续保留 path/runner owner,但不阻塞新的 Attach.
hard-delete 后才发送 Extension/object terminal observation,不再发布第二次 route Delete.失败不清尚未
完成的 ownership,当前进程持续重试,崩溃后由 startup Reconcile 重试.每次 fresh Create 必须在启动资源前通过
route-applied barrier;失败在完成本地 cleanup 后以 exact owner CAS 收敛为零 ownership `dead`，
并发布 Delete 撤销 route。

Pause 的 `CommitRunningPaused` 继续原子提交 state 与 source。其后 RunID、port、RunDir 分别只在
Stop/Reset fence、Detach、checkpoint 选择性清理及 RemoveAll 成功后 exact-clear；RunDir CAS 同时清除该目录拥有的 envd/ci
UDS path。任一步失败都保留尚未完成的字段供当前进程或 startup Reconcile 重试。fully-cleaned paused
row 只保留 BaseDir/checkpoint 与 source；Resume/Wake/Exec 在取得新 runtime owner 前完成 backlog，
并在 `paused -> starting` acceptance 中原子恢复 canonical RunDir/UDS。这样 paused BaseDir 永不被
runtime cleanup 删除，显式 Delete 仍可从 fully-cleaned paused row 删除 BaseDir 后 hard-delete。

### 8.1 Artifact lifecycle、转模板与迁移

#### 8.1.1 Capture 与 Pause

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

显式 `POST /sandboxes/{id}/pause` 的 body 可为空或为:

```json
{"memory":false,"checkpoint_merge_ref":null,"checkpoint_drop_caches":null}
```

`memory` omitted/null/true 选择 `CaptureSnapshot`;false 选择 `CaptureSandbox`。显式 Pause 的缺省始终
是 Snapshot S,不继承 `autoPauseMemory`。因此 `Create(autoPauseMemory=false) + Pause({})` 保存 S,
而同一 Sandbox TTL 到期保存 E。`checkpoint_merge_ref` 和 `checkpoint_drop_caches` 只属于
`SnapshotPolicy`;若 `memory=false` 同时携任一字段,API 在 guest freeze、capture 或 lifecycle
副作用前返回 400。`X-Kuasar-Sandbox-Checkpoint` 与 `kuasar-sandbox.checkpoint` 仍只接受
`merge_ref`、`drop_caches`,不接受 `memory`。

`checkpoint.mode` 只接受 `local|bundle`,两种 CaptureKind 都支持两种 carrier:

```text
CaptureSnapshot:
  sandbox-ctl snapshot --json --path-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root> [--merge-ref=...] [--drop-caches=...]
  -> ResumeSource{kind:snapshot, ref:<actual S basename + identity>, sandboxRef:<actual E basename + identity>}

CaptureSandbox:
  sandbox-ctl export --json --path-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root>
  -> ResumeSource{kind:sandbox, ref:<actual E basename + identity>, sandboxRef:""}
```

capture 返回生产者的实际身份: `snapshot --json` 返回
`{snapshotRef,sandboxRef,removedRefs}`, `export --json` 返回
`{sandboxRef,removedRefs}`. capture 的差集为 `[]`. Snapshot Bundle 在同一物理文件
中携带 S 和 E 两个 selector. 本地引用保留身份限定符, 只公开 basename; checkpoint
目录属于执行上下文. 默认人类可读输出及 upload-key stdout 不变. 适配器先验证完整
结构化结果, 再原子提交 running→paused; Snapshot 保存实际 S/E 对, E-only capture
清除旧 Snapshot 及其关联. capture 失败绝不提交半对.

Pause 的顺序固定为 resolve request -> accepted operation -> runtime capture ->
`CommitRunningPaused(id, exactRunID, ResumeSource)` -> stop/reset exact runner -> detach exact network ->
selective checkpoint cleanup -> RemoveAll RunDir -> publish paused route。commit 原子写 `state=paused` 与完整 source kind/S/E；之后非空
RunID、port 与 RunDir 共同表示 cleanup pending。Stop/Reset 成功后 exact-CAS 清 RunID，Detach 成功后
exact-CAS 清 network；任一步失败保留尚需重试的字段。checkpoint 或 RunDir 清理失败不允许新的 Resume/Wake/Exec
取得 runtime owner，当前进程的下一次 admission 与 startup Reconcile 都会重试。BaseDir 与当前 checkpoint 依赖始终保留，已知且无用的旧 checkpoint 文件会选择性清除。capture 失败保持 state=running、旧 source、runner 和 network 不变,不创建
成功 alias,也不从 S 降级为 E。

##### 托管 checkpoint 历史合并与选择性清理

`merge_ref=false` 独立记录本轮驻留工作集。已有 `S1 -> S0` 时，下一次捕获先流式生成新的
不可变历史 Snapshot `S1′ = S1(memory) 覆盖 S0(memory)`，再写出
`S2(当前工作集) -> S1′`。后续本地捕获重复该组合，最新 S 下最多保留一个本沙箱拥有的本地
内存 lower。`merge_ref=true` 将当前驻留内存与整个可合并本地前缀合并；false → true → false
切换同样收敛。reader 仍支持旧的多层输入。恢复执行状态和 `sandbox_ref` 只来自最新 S，
历史 S 只提供 memory。

前缀必须属于当前沙箱的规范 `BaseDir/checkpoint`。先按完整来源绑定和 Bundle 实际成员选择
物理载体，再比较路径。named location 或外部模板是边界，即使文件可在本机读取、甚至映射
到同一路径也不能越界合并。该边界及其后的 lower refs 保持原顺序。local tarstream、Bundle
和混合载体使用相同规则。可写磁盘链始终吸收同设备的连续本地前缀，不受内存开关影响；
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

保留集合包含当前 S/E、历史内存载体、当前磁盘/upper/不可变 base 载体及复用文件。历史 S
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

#### 8.1.2 Resume admission、Connect 与 Wake

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
`RollbackStartingDead` 清空 `launch_mode`。失败的 S+cold 恢复原 paused S,不会改写成 E,所以之后
仍可选择 memory。conductor 重启遇到 starting resume 时从 durable source + `launch_mode` 重建
attempt;例如 S+cold 不会因进程内缓存丢失而错误执行 memory restore。

Proxy 的 ordinary ingress 和 native exec activation 都经 `OnWake` 使用
`ResumeTriggerWake`;`ExecSession` 使用 `ResumeTriggerExecSession`.公开的
`ResumeTriggerRoute` / `ResumeTriggerExec` 常量继续保留扩展契约,但 conductor 不再承载
对应的数据面 adapter.这些入口全部只传 `ResumeAuto`:S 默认 memory,E 默认 cold.两种 source
都保留既有 Wake 能力;
proxy SHM 不携 source kind 或 cold gate。已鉴权的首个 E 请求保持 parked,直到 node
完成 cold launch、route 变为 running 并成功拨通 backend 后才继续转发。未来的 `autoResume=false`
若实现,必须是独立 traffic policy,不能从 E/S kind 推导。本变更不宣称完整实现 E2B autoResume。

#### 8.1.3 Task-local artifact prepare 与三种 config

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
- `SnapshotHostConfig`:用于 `run --restore S --config`;只声明 `ApplyRestoreRules` 允许的 kernel/runtime、
  active diff、resource controller/allocatable、network provider/实例字段、restore policy 和 timeout。
  类型本身不包含 launch、files、ephemeral files、init/plugin、mounts、metadata、boot cmdline 或 disk graph。

node 生成的 `/etc/hosts`、`/etc/resolv.conf` 在 cold 路径进入 `ephemeral_files`,不写 portable C0;
memory restore 不声称重新注入它们。persistent env/files 只随明确的 C0 override;ephemeral env/files
只影响本次 cold launch。renderer 输出须通过 `LoadMergedWithPresence` 后分别被 `ApplyFromRules`、
`ApplyRestoreRules` 接受,且多 data-disk name/order/mount topology 保持由 E 权威定义。

#### 8.1.4 Publish、template 与 migration

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
S0/E0. capture 及其生产者结果由 [§8.1.1](#811-capture-与-pause) 维护.

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

KMT V1 payload 直接携 `resumeSourceKind`、`resumeSourceRef`、`resumeSandboxRef` 和
`autoPauseMemory`。Snapshot 必须在 `resumeSandboxRef` 中携带关联 E；E-only 的该字段必须
为空。缺失字段、重复或未知 JSON 字段以及错误组合均被拒绝，包括缺少 E 的旧 Snapshot
token。目标 Expectations 同时比较 S 和 E。Import 不访问工件，原子恢复完整对、deadline、
portable env/metadata 和既有
ServiceSecret/Envd/Traffic/Forward token;目标 node 从本地 trusted key table 取得 APISecret +
ManifestKey 并校验 fingerprints、runtime digest 和 profile。token 不携 raw tenant roots、host path、
MMDS secret value、cluster Group/RouteKey 或 generation。

Export 的 publish 阶段不持有长 lifecycle lock。结果校验通过后，在 per-SID lifecycle lock
内检查完整原始 S/E 对、源实例和导出 attempt。Resume 先成功 `BeginResume` 时，KMT
export 被取消并返回 409；template publish 可 detached 完成并返回 TemplateID，但不清理
source。Export 先赢得收尾权时，`keepSource=true` 保持 ResumeSource、row、cache、route
与 checkpoint 全部不变。之后再对保留的源执行 move，仍使用同一删除流程。

`keepSource=false` 校验本地路径所有权后，调用正常的锁内删除接纳入口。成功表示
有效 token/template 结果和持久 `deleting` 行，物理清理可能尚未完成。Export 返回前
撤销 source cache、排除后续全量 route snapshot，并发布 live route Delete。已有 finalizer
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

`running` 仅表示 runtime readiness 与 mandatory e2b `/init` 已完成,不保证用户应用端口健康。
通用 backend health/dial retry 仍由独立工作跟踪。cluster 的 create/connect 复用上述 node-local
原语;secrets-only CONNECT import 仍只属于 standalone,node-link 不传 MMDS secret plaintext。

## 9. 数据面边界

数据面**转发层**——L7 反代:按 `(sid, port)` 路由到 guest envd / floatingip,逐请求
鉴权,CONNECT 隧道,MMDS,以及 routesync 线格式——自成一文,见 [node-proxy.md](node-proxy_zh.md)。
conductor 只服务控制 API,生命周期和本节点路由权威;它不构造 proxy,不监听数据口,
不接收或转发 sandbox data.独立 `node-ctl proxy serve` 的必填 `data_listen` 是唯一节点数据入口.

### 9.1 APIEndpoint 与 DataEndpoint

独立部署和集群部署都必须把两个 endpoint 分开:

| Endpoint | Owner | 用途 |
|---|---|---|
| `APIEndpoint` | conductor `api.listen` | Sandbox/Build control HTTP |
| `DataEndpoint` | Proxy `data_listen` | ordinary HTTP,CONNECT 与 native exec CONNECT |

集群节点注册显式携带两者,不从 bind address 推导.cluster Router 的 control/build 转发只拨
`APIEndpoint`,data/exec 只拨 `DataEndpoint`;任一缺失都 fail closed,不跨平面回退.native exec
由 Proxy worker 执行 KAT + ExecRequest gate 和 sandboxer ctl tunnel,并使用 master 冻结的
EffectiveConfig 中必填的 `paths.run_root` 本地构造
`<RunRoot>/sandboxes/<NodeSandboxID>/ctl.sock`.该路径不通过 routesync `Policy` 或共享路由视图传递.

当前 Router→node 跳使用明文 HTTP/CONNECT,因此集群注册的两个 endpoint 必须指向不同的
明文内部 listener,不能指向启用 TLS 的 node listener.随附部署样例使用 conductor `:3000`
和 Proxy `:3443`,对外 TLS 在 cluster Router 或负载均衡器终止.这不改变 node-link 自身的
mTLS.

Proxy master 注册可选 `stats_socket`,每个 worker 经独立 socketpair 推送 Prometheus counter
与 per-sandbox traffic 绝对值;conductor traffic API 只查询当前 trusted Proxy registration 的
stats endpoint.route barrier participation 不依赖 stats socket.每条 logical ingress 只在最终
node worker 统计一次.

### 9.2 路由权威与广播

serve 是**本节点**路由与生命周期的权威:create/resume/pause/kill 实时更新路由,经
**routesync** 广播 Upsert/Delete 给所有 plugin 平面订阅者(Proxy master 与路由
观察者如平台 agent)。proxy master 把路由投影到共享内存,worker 只读;观察者持只读缓存
感知状态。广播逐条 upsert + 末尾 bookmark(高密度下发端内存有界)。线格式(帧化 JSON over h2c)、容错重同步、
`RouteEntry` 字段(含驱动迁移的 `artifact_location`、MMDS 使用的 `mmds_secret` 与
presence-aware `max_inflight` patch)见 node-proxy.md §4;
plugin 平面的注册与鉴权见 §6。机群级路由权威是 registry(cluster.md);serve 经 node-link
把本节点沙箱事件上报 registry(§10),与本节点 plugin 平面的路由广播是两条正交通道。

广播前执行服务端投影:受信 MMDS proxy 的 Upsert 额外携 `mmds_routes` 与
`mmds_route_secret_values`,二者在同一 event 中原子替换;普通 observer 不收到这些字段,
cluster/node-link 也绝不收到 value。conductor-owned `mmds.services` 仅放在受信 proxy 的
Hello policy,不广播给 route observer.Proxy master 原子替换 registry,worker 经本机 MMDS RPC 取得已解析 Unix socket endpoint,
不读取 `proxy.yaml` 中的第二份配置。

- **auto-resume launch owner**:数据面打到 paused 沙箱时,Proxy 经 routesync `Wake` 上行;同一 sid 的
  Connect,Proxy Wake,ExecSession 与其它 launch 入口共用唯一 attempt(§8)。
- **starting 投影**:初始 `starting,run_id=""` durable insert 后即广播,此时没有 FloatingIP
  或可用 endpoint;network ownership 已 CAS 持久化且 YAML/ready.sock 已准备后再广播 enriched
  starting,供 Proxy MMDS 完成 envd `/init`.starting 不开放普通数据面,也不触发
  Wake.每次 Create 在初始 Upsert 后发送 ordered route barrier;Proxy master 先校验并将
  目标节点默认与显式 patch 合并、事务化发布 admission binding 和 route SHM,全部成功后才
  ACK。barrier 不等待 worker readiness、worker stats 或健康 worker 数;即使没有 serving worker,
  master apply 成功仍可 ACK。任一 admission/route apply 失败不 ACK,也不会留下只发布一半的
  route/admission 状态。master apply+ACK且 lease
  复核成功后才返回 201并启动 launch,因此 201 后的首个合法请求应直接看到 starting并
  parking,不再把正常 route propagation gap 当作 missing。launch 成功广播 running;create
  失败广播 Delete,resume 失败广播 paused。
- **envd 鉴权姿态(`mmds.enabled`)**:该开关决定 create 是否给 envd 下发 token、proxy
  是否寄宿 MMDS 服务——`false` = envd 非 secure、proxy 单闸门(配置强制
  `proxy.auth=enforce`);`true` = proxy 组件内起 FC MMDS v2、经 `/init` re-key 每身份新
  token,使快照扇出沙箱数据面可用.姿态对比与 MMDS 两段式协议见 node-proxy.md §7.

## 10. 集群接入(node-link)

配 `cluster.node_link.endpoint`(§3)时,`node-ctl conductor serve` 拨 registry 把本节点接入集群,交由
`cluster-ctl registry/router/placer` 编排。node-link 复用 routesync 的帧化 JSON over h2c 引擎
(node-proxy.md §4),但角色相反:node 是本节点路由 / 构建权威,registry 是订阅者和命令下发方.

本节只讲 node 侧行为。registry 的 owner 选择、redirect/relay、shardkv 复制和 membership 变更由
[cluster.md](cluster_zh.md) 定义。

```text
node-ctl conductor serve
  │ dial registry node_link endpoint
  │ register node profile
  │ stream heartbeat + sandbox route events
  │ stream Build full snapshot + live BuildUpsert/BuildDelete
  │ receive create/connect/exec_session/delete/key/build commands
  ▼
registry node_link owner or relay holder
```

集群下两条到节点的路径:

- **node-link**:注册、心跳、sandbox route 事件、Build snapshot/delta、命令、manifest key 租约。
- **router 转发到本节点 e2b 控制面 / 数据面**:pause/kill/timeout、build status/files 转发本机 e2b
  控制面;数据面经 router 注入 `E2b-Sandbox-Id` + `X-Access-Token` 后进入本机 proxy(node-proxy.md)。

### 10.1 注册与 redirect

节点拨 registry 的 node_link endpoint 后,首帧发送:

```text
register{
  node_id,
  labels,
  capacity,
  build_registration_capacity,
  build_execution_capacity,
  api_endpoint,
  data_endpoint,
  runtime_digest,
  accept_redirect
}
```

若接入成员不是该 node 的 node_link owner,且节点支持 redirect,registry 可返回 owner `node_advertise`
列表。node 会按返回顺序重连 owner;失败时尝试下一个目标。若没有 redirect 目标或未启用 redirect,接入成员
可以 relay 到首个可用 owner。

### 10.2 心跳与低频目录

节点周期发送:

```text
heartbeat{zone, allocated, pool, build_registration_usage,
          build_execution_usage, counts, draining}
```

两级 Build capacity 在首个 `register` frame 中发送；沙箱水位取自资源控制器
(node-resource.md)。`allocated` 的 memory 分量是本节点全部 sandbox 的
NodeReservation 之和,`pool` 是 node allocatable pool；两者都不是 host
`memory.current`、VMM charge 或 guest demand。两级 Build usage 由节点 SQLite 中每条
非终态 Build.Resources / execution claim 精确求和,不是 active count × 默认向量。`draining` 由节点侧
资源 drain 或维护策略置位。普通 heartbeat 只更新 node_link profile 中的 liveness 和本地水位，不更新
node_list；首次注册和 draining 变化驱动低频目录投影。registry node owner 持有的当前连接是 placement
提交时唯一的存活判断。

### 10.3 Sandbox 与 Build 投影

节点作为权威上报本机执行态:

```text
sandbox{
  sid, profile, state(starting|running|paused|dead), artifact_location, template_id,
  stable_id, api_secret, api_secret_fingerprint,
  manifest_key_fingerprint, service_secret,
  envd_access_token, traffic_access_token, forward_access_token,
  mmds_secret
}
delete{sid}
build_sync_begin{}
build_upsert{build_event:{build_id, state, template_id, persist_id?, reason?}}
build_delete{build_event:{build_id, template_id}}
build_sync_end{}
bookmark{full_sync}
```

`deleting` 是 node-local cleanup-pending state,不作为 sandbox upsert 投影.节点一旦持久接纳
Delete,就立即从本地 cache 和后续 full sync route set 排除,并在既有 live 链路发送
`delete{sid}`.该事件只撤销 route projection,不证明 unit/network/path cleanup 或 hard-delete 已完成.
若进程在 durable transition 与增量发布之间退出,旧 stream 随进程失效;下一代完整 route snapshot
因该 SID 已被排除而撤下旧 projection.node 重启仍能从 durable `deleting` row 中尚未完成的 owner
重试清理;已经 exact-clear 的 network tuple 不再取得.本地 finalizer 正确性不依赖 Registry
projection,也不在完成时发送第二个 route Delete.

该 node route event 保留既有 `mmds_secret` 字段供节点 proxy/MMDS 路径使用;cluster Registry
物化受保护 route 时不采纳该字段。starting 只表示 node-local launch 正在进行,Registry
将它计入 full-sync seen set 但不改写 reserved/paused route;running/paused/delete 才驱动
cluster route 状态收敛。节点事件按以上用途显式携带其余凭据。

node 不在 sandbox event 中自报 Registry-owned 的 cluster context。nodelink owner 在任务下发前已维护
本节点完整的 sandbox/build 归属表,收到事件后以 `(node_id,sid)` 或
`(node_id,build_id)` 查表取得 group/route_key。sandbox event 的 `sid` 是 node-local NodeSandboxID;集群下由
Registry 以 `<stable-sandbox-id>-g<N>` 分配.node 不接收 SandboxGeneration,不解析该 ID;稳定 SandboxID
和代际由 Registry 归属表恢复,不存在跨 group 的 SandboxID 索引.

Sandbox 全量 Range 结束的 bookmark 带 `full_sync=true`。nodelink owner 仅将本轮出现的 sid 与
订阅建立前捕获的本节点归属表基线比较;清理前再次确认当前表项仍与基线一致,避免删除
同步期间新下发或重新绑定的任务。增量 replay 的 bookmark 只推进 resume token。

Build 事件分别保留注册 template_id 与终态 persist_id. 硬删除携带原 template_id, 迟到事件不能影响
同 BuildID 的新注册. 只有节点硬删除提交后才发 BuildDelete; error 行离开 observer 集合不等于硬删除.

当前仍存在的 post-registration Registry Build projection 使用独立 bracket:节点先订阅 live Build
变化，再发送 `build_sync_begin`、SQLite 中该节点仍保留的全部 cluster Build row、
`build_sync_end`；集合包含 registered/waiting/building，也包含 retention window 内的 ready/error。
Registry 以 NodeID session fence 接受这些 frame；新 node-link 生效后，旧重叠连接的 Build frame
不会再修改 projection。节点侧每个 Build durable transition 与其 live publication 也由无条件
per-Build fence 排序，不依赖 conductor Extension。
snapshot 期间发生的变化在 end 之后按顺序发送，慢订阅者会断线并重做完整 snapshot。Registry 在
`build_sync_end` 只删除连接建立前 immutable `(NodeID, BuildID, TemplateID)` binding 基线中未出现、且删除时
仍精确属于该节点的 post-registration projection/ref；Registry-owned `BuildStarting` ambiguous
dispatch intent 不是节点 projection，空 snapshot 不能将其当作 definitive rejection。同步期间的
新注册因此不会被误删，丢失的 live
`build_delete` 会由下次重连收敛。Registry 不运行独立 Build terminal TTL。
长 Build snapshot 不阻塞控制回执：同一个 stream writer 在 Build snapshot item 之间有界清空
command ACK 与合并后的 heartbeat；Build live changes 仍等 `build_sync_end` 后发送。

### 10.4 命令受理

registry 上行下发命令.serve 复用既有 e2b 生命周期原语(§8 / §8.1)执行,
所有 sandbox 操作的 `sid` 均是精确 NodeSandboxID.普通命令受理后回 `cmd_ack`,
终态经 sandbox route 或 BuildUpsert 事件上报。CmdCreate 只有在唯一 launch owner 已 claim、starting 行已
insert、cache/route starting 已发布、ordered route-applied barrier 成功且 lease 复核后才 Ack;该 Ack 表示 durable acceptance,不表示 READY。
CmdConnect 在 Ack 前清理旧 runner/network/RunDir ownership，并在 paused→starting 原子 acceptance
中恢复 canonical RunDir/UDS、提交最终 deadline 后 cache/publish；CmdExecSession 在 Ack 前完成可选
import、鉴权/签名及同一 resume acceptance。
三者随后均由共同 lifecycle root 异步 launch:

  | 命令 | 节点动作 |
  |---|---|
  | `create{cmd_id, sid, template_ref, profile, api_secret_fingerprint, config, cluster}` | 冷启 `template_ref` + 保存/合并 `config`(§8;snp 模板 = fresh Create 的快照恢复快启);完整 APISecret 指纹选择本机已安装的凭据对;`cluster={group,route_key,stable_id?}` 与 profile 作为系统字段独立持久化;credentials namespace 在写业务行前解析并剥离;Ack 前已是 `starting,run_id=""` 且有 active attempt、成功的 route/lease barrier |
  | `connect{cmd_id, sid, profile, api_secret_fingerprint, cluster, migration_token?, timeout_seconds?, memory?}` | `sid` 是 NodeSandboxID.target 已存在时忽略 token,校验完整指纹、profile 和 cluster context;target 缺失且带 KMT1 时,先用本机 matching pair 同步校验并以命令 sid/context insert paused 行;再于 Ack 前完成旧 runner/network/RunDir 清理，并在 paused→starting acceptance 中恢复 canonical RunDir/UDS、持久化 deadline 与 resolved launch mode；`memory?` 保留 omitted/null/false/true 三态选择（§8.1.2）。Ack 携 `ConnectResult{NodeSandboxID,TemplateID,Profile,EnvdAccessToken,TrafficAccessToken,ForwardAccessToken}` 且不携 root/fingerprint;restore 异步,缺失且无 token 则拒绝 |
  | `exec_session{cmd_id, sid, profile, api_secret_fingerprint, cluster, ttl_seconds, exec_conditions, migration_token?}` | 复用 connect 的精确目标,可选同步 import,profile/cluster context 和完整 APISecret 指纹校验;原始 API key 不进入 node-link.节点权威编译 `exec_conditions`,再生成 UUIDv7 session ID,以实际签发时间计算可选 `exp`,Ack 仅携 `ExecSessionResult{ExecAccessToken}`;随后按既有合同异步 resume,不等待 READY;conditions 不写 Sandbox row/route/event/metadata |
  | `delete{cmd_id, sid, api_secret_fingerprint}` | 完整指纹必须与既有 Sandbox 业务行绑定一致;Ack 仍只表示 exact owner 已持久转为 `deleting`,不等待 node-local finalizer 完成;route Delete 在 Ack 返回前发布且不是 cleanup 完成证明;pending 重放幂等(§5 kill) |
  | `key_put{api_secret_fingerprint, api_secret_type, api_secret?, api_secret_ref?, manifest_key_fingerprint, manifest_key_type, manifest_key?, manifest_key_ref?, expires_unix}` / `key_drop{api_secret_fingerprint}` | `key_put` 原子校验并写入 / 重发续租完整凭据对;两项指纹均为 64-hex SHA-256。`key_drop` 按完整 APISecret 指纹 best-effort 清理,正确性依赖 TTL 淘汰(§7);registry 的密钥分发见 cluster.md |
  | `build_register{build_id, template_id, profile, resources, image_repo, registry_auth, api_secret_fingerprint, config}` | `config.builder.target` 是 canonical register-only requested target，参与 replay digest/node validation；节点以同一 canonical Build.Resources 做最终、事务化 registration admission。最终 Sandbox Create 配置/resources、build-only options 与 cluster group 分开持久化；A/B resources 不从 target Sandbox resources 推导。definitive 无副作用拒绝才可换候选,歧义结果固定同节点/BuildID重试 |

无 `drain` 命令。节点排空 / 维护由节点侧发起(node-resource.md §2 的 resource drain 或本机维护策略),
集群侧只停止向其分配。

CmdCreate 的任一 Ack 后 launch failure 将 fresh starting 回滚为 dead 并发 Delete;
CmdConnect/ExecSession 的 resume failure 则回滚为 paused 并发 paused Upsert,不得误走 create
replacement Delete 分支。Registry 既有 replacement reservation rollback fence 保留:匹配本次
create 的 Delete 才恢复 Reserve 前旧 route,不能提前删除 reservation 丢失回滚依据。并发
cluster create/connect/delete 不能取得第二个 node-local launch owner。

standalone 与 cluster Delete 共用同一个 node-local finalizer;node-link command 不拥有另一套
cleanup 或路径推导.finalizer 失败不会把 `deleting` 作为 ready,paused 或 starting 上报,
也不会延迟或重复 route withdrawal;terminal object observation 仍等待 hard-delete.

每个 exec-session API 调用是独立授权,因此使用新 CmdID 并签发新 KAT;resume
可以继续按 SID 查找同一 launch attempt.`CmdID` 只关联当前 Command 与 Ack waiter,node 不持久化
command digest 或 typed result,Registry 也不在断线、超时或 node 重启后自动重投同一
`CmdID`.本次调用失败后,API 重试是新的 operation;Connect 重新执行可重试的目标校验/恢复,
Exec Session 可以签发新 KAT.

### 10.5 断线与安全

断线后节点指数退避重连并重注册。Registry 在 Hello.resume_from 下发自己保留的 Sandbox route
revision，node 按该订阅者 cursor 重放；它不是 node register 字段。node replay window 允许时补增量，
否则逐条全量 route + bookmark。Build projection 每个新 session 都执行完整 bracket，不依赖 route token；
Registry 重启亦然。

node-link 生产走 mTLS(`cluster.node_link.tls`)。下行 APISecret+ManifestKey 凭据对及创建期
credentials 只进入加密存储和运行期内存;沙箱级 token 属敏感数据,仅在可信链路内传输。

接入集群与本机 plugin 平面使用同一 routesync 引擎和线格式,仅订阅者 kind 不同。router 不订阅节点
plugin 平面,机群路由经 registry 聚合。

## 11. guest profile:envd 与工具链

- 单一 **`sandbox-runtime.bundle`** 由 `guest-runtime` 构建:把 `sandboxer` 产出的
  `sandbox-init` 打成 virtio-pmem/DAX runtime,并在 `/opt/sandbox-runtime/bin/`
  内置固定版本 `envd`、`flatten-ctl`、`mkfs.erofs`。runtime bundle 保持 raw EROFS
  从 offset 0 开始,随后是 zero padding 和只含空 `.kuasar.digest.<hex>` marker 的
  ZIP;该runtime carrier的identity覆盖EROFS+padding,构建时一次生成,启动/恢复从 EOF
  直接读取。成品总长
  **补齐到 2 MiB 对齐**(virtio-pmem 后端要求,否则 cloud-hypervisor 报
  `PmemSizeNotAligned`;EROFS superblock 自描述范围,尾部 padding/ZIP 对 guest mount
  不可见)。
- **启动链**:`/opt/sandbox-runtime` 被 sandbox-init 自动 bind-mount 进
  guest 同名路径,envd 直接作 `launch.exec`:
  `/opt/sandbox-runtime/bin/envd -isnotfc -port 49983`(`mmds.enabled` 时去
  `-isnotfc`,node-proxy.md §7),`restart=always`,**以 root(`user: "0:0"`)运行**——envd 需要
  `CAP_SETUID/SETGID` 才能按镜像配置的用户跑工作负载命令(镜像设了 `Config.User` 时
  非 root 的 envd 会 exec EPERM);工作负载本身仍以目标用户执行。
- **cgroup 委托**:普通 e2b 创建、builder steps 和最终 snapshot template 均固定生成
  `launch.cgroup_control: true`;不向 envd 增加 cgroup root 参数。真实
  `/sys/fs/cgroup/app` 是 namespace/freezer root 且保持无直属进程,sandbox-init 长期管理的
  envd、plugin 与 native exec 位于真实 `/app/init`;它们在 scoped cgroup namespace 中看到
  `/init`,而 envd 仍按默认 `/sys/fs/cgroup` 在 namespace root 下创建 `user`、`ptys`、
  `socats`(真实路径分别为 `/app/user`、`/app/ptys`、`/app/socats`)。bare profile 默认
  `false`,可经 `kuasar-sandbox.launch.cgroup_control` 显式启用。
- envd 是 **guest-runtime/native-deps** 的原生构建产物(与 vmlinux/mkfs.erofs
  并列):`make -C guest-runtime/native-deps envd` 拉取 e2b-dev/infra 发布 tarball(默认 tag
  `2026.22`,`ENVD_TARBALL` 可覆盖)→ `go build packages/envd`。`make -C guest-runtime sandbox-runtime` 负责把它和构建工具链一起注入 runtime。
- **userland 门槛**(对 base/客户镜像的约束):须有 `bash`、`coreutils`/`util-linux`、
  预建默认用户(默认 `user`,含 `/home/user`)、cgroup v2、可写 `/run`。envd 跑每条
  guest 命令以默认用户、并包一层 `ionice -c 2 -n 4 nice -n N "$@"`——缺用户或缺
  util-linux/coreutils 会报 `invalid default user` / `ionice: not found`(裸 alpine
  两者皆缺)。
- **guest 须有 `/etc/hosts`**:展平的 docker 镜像不带它(docker 仅在容器运行时注入),
  而 guest 内 `socket.getfqdn(hostname)` 类调用(许多服务器在 bind 后、listen 前调它,
  如 Python `http.server.server_bind`)查无本地条目即落到 DNS,解析主机名曾观察到阻塞约 20s（不是固定 timeout 保证），
  表象是"host→floatingip 应用端口转发失败"。serve 在 cold launch 的 SANDBOX_CONFIG
  中通过 `ephemeral_files:` 注入 `/etc/hosts`(`127.0.1.1 <hostname>` 条目)与
  `/etc/resolv.conf`(`sandbox.network.dns`),并经 `network.hostname` sethostname。
  这些 node-derived 文件不进入 portable C0;memory restore 不重新注入它们。
- host 在 runtime readiness wire 完成后直接调 mandatory **`POST /init`**(经 UDS):置
  `envVars`、默认用户 `user`/workdir `/home/user`,时间戳;仅 `mmds.enabled` 时携带
  `accessToken`(node-proxy.md §7).envd socket 尚未可拨由上述短退避传输重试吸收,
  无启动期 `/health` 探测.

## 12. DNS / TLS

生产:`*.<domain>` + `api.<domain>` 通配 DNS + TLS(operator 提供,on-prem/离线
友好).控制面由 conductor `api.listen` 承载,数据面由独立 Proxy `data_listen` 承载;
二者必须是不同 listener.独立模式可让两者直接终止 TLS;若都使用端口 443,必须绑定不同
地址或由外部负载均衡器按域名转发.dev:`E2B_API_URL`/`E2B_SANDBOX_URL` 分别指向
明文 http(h2c) listener.集群下 cluster-ctl router 持对外通配证书,Router→node 的
`APIEndpoint`/`DataEndpoint` 跳当前为明文,node-link 则用独立的
`cluster.node_link.tls` mTLS(§10).

## 13. 契约边界

| 对象 | 方式 | 说明 |
|---|---|---|
| `sandbox-ctl`(runtime) | run-sandbox(单元)最终 `execve`:`run --ready-fd=<fd> --cgroup-path=fd=<vmm-fd> --config <sid>.yaml --manifest-config … --run-root … --base-root … [--from E\|--restore S] [--connect]`;run-builder 以直接子进程启动 A/B/C，source E 的 C 使用 `run --from E --replace-boot`，经 `exec --env/--stdin-from/--stdout-to` 做平台接力，memory 收尾 `snapshot`/`publish`;顶层 E 则直接调用 sandboxer package assembly/publication API，不启动 sandbox-ctl;serve 的 Capture 调用 `snapshot` 或 `export` | managed launch/build prepare 不启动 `sandbox-ctl info`;readiness wire 固定为 `control_ready`→`ready`→EOF;runner 的 VMM cgroup FD 仅由 node-ctl 本地注入;非密配置文件 + 密钥 env;资源准入在其内部;e2b 语义命令不走它(走 envd,[Build §5](node-build_zh.md#5-按目标执行与发布)) |
| 资源控制器(node-resource.md) | serve 内置(`resource_listen`,调参内联);dynamic sandbox 自动使用同一 canonical `SocketIdentity` 拨号(`pkg/resource` 协议) | `resource_listen` 是唯一 endpoint 来源;每个 runner 的 `vmm/` 是沙箱资源 cgroup,FD 由 run-sandbox 注入;controller disabled = static cgroup |
| registry(cluster-ctl) | node-link:serve 拨 registry、反向注册为路由权威,上报 register/heartbeat/Sandbox route 与 Build full-sync/upsert/delete，受理 create/connect/delete/key_put/key_drop/build_register 命令(§10、cluster.md) | mTLS;cluster kill 走 node-link delete 命令;Build projection 无 Registry-side TTL;空 `cluster.node_link.endpoint` = 独立模式不接入 |
| `connector-ctl vswitch`(vswitch) | 不配 `tapfd_socket` 时经 CLI:`attach <switch> --inner-ip [--transit-*]` / `detach --port`;配 `tapfd_socket` 时经常驻 `TAPFD/1 PREPARE` / `OPEN` / `RELEASE`;sandbox 配置仍渲染为 `network.tapfd.socket/request` | 交换机预先起好(`connector-ctl vswitch start/serve`,内核态数据面);port 对外、slot 内部;一个构建复用一个槽 |
| `flatten-ctl`(builder) | **guest 内**(guest runtime 自带,经 sandbox-ctl exec 驱动):`export --output -`(import 拉取 / steps 导出)、`mountpoint`;宿主侧只用 `info --json --manifest-config` 验证 registry referer hit | 三种 image carrier 的 config 读取和最终发布均走 sandboxer typed opener；租户 `FLATTEN_*` 仅经 exec env 入 guest;tarstream 镜像工件经 exec stdio 接力 |
| sandboxer `pkg/artifact` + `pkg/sandbox` | Builder 的 image/Sandbox logical source 直接 Manifest ingest 或 single-root Bundle publication；本地 IMG→顶层 E 无中间文件 | typed role、customer key、write admission、root-last、sparse semantics、named-location atomic/reuse validation 均由 sandboxer 统一实现；Build finale 不调用 `manifest-ctl store` |
| `manifest-ctl`(accelerator) | 独立的 Manifest store CLI；Builder final publication 不经该 CLI | `MANIFEST_KEY` 经调用进程环境 |
| `mkfs.erofs`(deps) | guest-runtime `make sandbox-runtime` 与 guest 内 `flatten-ctl` 后端 | 确定性打包 runtime;构建沙箱内导出 EROFS 镜像(§11、[Build §5](node-build_zh.md#5-按目标执行与发布)) |
| guest envd | UDS(sandbox-ctl `--connect` 映射);构建流水线另以最小 connect+JSON 客户端调 `process.Start`(steps/startCmd/readyCmd,[Build §5](node-build_zh.md#5-按目标执行与发布)) | 原版不改;协议 pin 见 §4.2/§4.3 |
| systemd | D-Bus:StartUnit/StopUnit/ResetFailed/ListUnitsByPatterns/Reload | 进程管理 + 单元自装(§5) |
| `node-ctl proxy` | UDS routesync(双向 h2c 帧化 JSON)+ 独立 Data listener | 同节点,运维带外起;Proxy master 注册一次,worker 共享继承 Data listener fd + shm 路由视图;master 冻结的 EffectiveConfig 含 `paths.run_root`,worker 不重读 `proxy.yaml`(node-proxy.md §2/§3/§4) |

公共 Config/App/extension 契约位于 `config`、`app/conductor`、`app/proxy`、`app/telemetry`；
advanced Collector binding 隔离在 `app/telemetry/otel`；
`CGO_ENABLED=0`;内部 core 继续保持 `internal/*` 依赖边界。

## 14. 可靠性

### 14.1 状态存储(sqlite)

单文件 sqlite(`paths.db_path`,WAL,文件 0600),纯 Go 驱动。核心表:

```
sandboxes      id(node-local SandboxID,1..57 bytes DNS-label subset) PK,
               profile, cluster_group, cluster_route_key, stable_id,
               template_id, state(starting|running|paused|deleting|dead), deadline_unix, dead_unix,
               run_dir, base_dir, envd_uds, ci_uds, floatingip, vswitch_port,
               inner_ip, port_mac, api_secret_hash, api_secret_enc,
               manifest_key_hash, manifest_key_enc,
               resume_source_kind, resume_source_ref, resume_sandbox_ref, auto_pause_memory, launch_mode,
               service_secret_enc, envd_access_token_enc, traffic_access_token_enc,
               forward_access_token_enc, metadata_json, env_json,
               created_unix
sandbox_mmds_route_secret_values
               sandbox_id PK/FK sandboxes(id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
manifest_keys  api_secret_hash PK, api_secret_enc, manifest_key_hash,
               manifest_key_enc, label, created_unix, expires_unix, registry_auth_enc
```

Build schema、准入与记录权威见 [Build §6](node-build_zh.md#6-持久化恢复与保留)。
`resume_source_kind/ref` 是 paused E/S 的 typed root，`resume_sandbox_ref` 保存 Snapshot 的精确关联 E；`auto_pause_memory` 只决定 TTL CaptureKind;
`launch_mode` 是 starting 中已接受的实际 image/cold/memory 模式。生命周期
schema 直接替换旧单字符串 lifecycle 模型,不保留双读/双写或迁移 shim;已有开发数据库须重建。

`*_enc` 根凭据及 Sandbox service credential 均
AES-256-GCM、两项 `*_hash` 均为
完整 SHA-256。`substr(api_secret_hash,1,24)` 仅建候选预筛索引(§7)。
MMDS value 表每 owner 最多一行 secretbox ciphertext;AAD/事务/CAS/cleanup 见 §7。Build schema marker、additive migration 与终态时间补入见 [Build §6](node-build_zh.md#6-持久化恢复与保留)。

### 14.2 重启对账

任何实际清理前（包括 Build 前置取消/删除）都能按去重后的配置模板枚举并定位实际 unit，
每个 unit 只对账一次。重启只恢复 RunID → 实际 unit，不恢复共享模板下历史 pool 序号。
索引缺失不证明进程不存在；枚举错误保留持久归属，不能拼默认模板猜测。
仍有执行或待清理归属的模板必须保留配置，不提供已删除模板自动发现、迁移、热删除或 drain。

conductor 在开放 API、config-socket routesync 和 node-link 前按配置 runner unit 集合对账:

- 库内 `deleting` 是已经接纳、尚未完成的显式删除.Reconcile 先取消同 SID 的 launch owner,
  fence exact unit;network tuple 非空时在 allocation fence 内 detach exact port 并 full-owner
  exact-clear 四个 network 字段,tuple 已空则直接跳过 Detach;随后依次删除 canonical
  RunDir/BaseDir,再 exact hard-delete.任一步失败则启动 fail closed,尚未完成的 owner 保留供重试;
  已 durable clear 的 network owner 不会因目录重试重新取得.`deleting` 不进入 route full snapshot,
  Wake,Resume,Exec 或任何 activation;
- 库内 starting 不直接收养为 running。fresh Create 先 Stop/Reset exact runner、detach network、
  清理 RunDir/BaseDir,再以 exact run-id CAS 到 dead。带合法 `ResumeSource` 的 starting
  是已接受 resume:同样先释放旧 ownership,但保持 `state=starting`、source 和 durable
  `launch_mode`,只删除 RunDir、保留 BaseDir/checkpoint，清空 runtime owner 后排入恢复队列，
  用原 cold/memory 决定重新启动。任一
  Stop/Reset/detach/目录 cleanup 失败时 Reconcile 使节点启动失败并保留 ownership,不得先清字段
  或开放 API;
- 单元 active/activating 且库内 running ⇒ **收养**(重挂内存路由、TTL 继续生效,
  随快照重新推给 Proxy worker;集群下经 node-link 重报);
- 库内 running 但无对应活单元 ⇒ fence unit、detach network、删除 RunDir/BaseDir，再以 exact
  owner CAS 原子清空路径、runtime、network、artifact 字段并标 `dead`；`dead` row 只保留历史，
  绝不拥有本地资源;
- 无 running 行对应的 runner 单元属于上一个 pool 的 idle/orphan run-id ⇒
  `StopUnit` + `ResetFailedUnit`,随后由新 pool 按配置补足;
- paused 行无论 RunID/port 是否已清空都重试完整 cleanup：Stop/Reset + inactive fence、Detach、
  exact CAS、checkpoint 选择性清理和 RunDir RemoveAll；source 与当前 checkpoint 依赖保留。Resume/Wake/Exec 使用相同
  admission gate，旧 ownership 未完成时不得进入 `starting`。`run_root` 为 tmpfs ⇒ 整机重启后失联 running
  判 dead;paused E/S 与 sbx/snp template 保留,可被 Connect/Wake 重新拉起(本机制品位于
  持久 `BaseDir/checkpoint`)。

同一次 startup gate 也按 [Build 恢复契约](node-build_zh.md#6-持久化恢复与保留) 重建 Builder ownership，完成前不得处理 bootstrap/result。Build 仍受下述共享 reaper 与故障域规则约束。

conductor 持续运行时, 独立的低频 runner 扫描发现 live-unit 列表中缺少 runner
的 running 行. 发现过程有时间预算, 每轮限制候选处理量, 游标使后续行不会持续
被失败候选阻塞. ListUnits 缺席只产生候选, 不证明死亡. Sandbox 生命周期锁忙时
推迟处理; 取得锁后重读持久行, 比较完整 ownership 身份: state, SID, RunID,
创建身份, launch mode, network tuple, RunDir/BaseDir, runtime UDS 和 ResumeSource.
并发替换、Pause 或 Delete 因而会在资源释放前使旧候选失效.

清理必须使用已记录的精确 RunID-to-unit 关联. 扫描重读该 unit, 保留可能仍存活
的单元, 对非活动候选执行 Stop 后再次检查. launcher 还必须证明 systemd 状态为
inactive/failed, 且 ControlGroup 为空或 cgroup v2 报告 `populated 0`.
关联缺失、枚举/proof 错误、单元仍活跃或 cgroup 非空时, 保留 durable running 行
及后续轮次的清理重试责任. 列表缺席或单个子进程退出都不充分.

证明成立后复用既有 ownership teardown: Stop/Reset 并 fence exact runner,
在 allocation fence 内 detach 其 network ownership, 删除 canonical RunDir/BaseDir.
只有 teardown 成功后才允许条件 dead commit 清空全部持久 ownership 并写入终态
历史; 完整 ownership 比较是防止并发替换或删除的最后一道检查. teardown/commit
失败时保留 durable running owner 供重试, 不能发布成功的 dead 转移. commit 后才
忘记该 run、释放 detached-port fence, 并经既有 route stream 发布 Delete,
使 Proxy 与 cluster node-link 撤销该投影.

proof 前取消会保留清理重试责任. 一旦已证明为空, 接纳后的 teardown 和 commit
使用独立且有界的 cleanup context, 不继承已到期的 proof 或 conductor context.
shutdown 关闭新扫描接纳并 drain 已接纳的 runner 检查、清理和发布, 然后才关闭
launcher/store. 扫描周期、候选数量和内部预算是实现限制, 不是公开的清理完成时限.

同一个 conductor reaper 每 5 秒执行一次终态保留清理，不增加 systemd timer/unit。Sandbox
进入 `dead` 与 Build 进入 `ready/error` 的 store transition 分别原子写 `dead_unix` 与
`finished_unix`；每轮每类最多处理 128 条。只有到达 `sandbox.dead_ttl` /
`builder.terminal_ttl` 且完全 owner-free 的行才 exact-delete：Sandbox 不能仍有 unit、network、
RunDir/BaseDir、UDS、ResumeSource 或 launch owner；Build 不能仍有 execution claim、unit/cgroup、
phase、network、runtime prepare/result owner。候选扫描后的并发变化会使 delete CAS 失败并保留行，
进程重启后按数据库时间继续。实现不自动 `VACUUM`，也不触碰任何远端 artifact。

以上 node-local finalizer 是 #132/#133 的 cleanup 合同实现边界。Export 保持
publish/finalize 两阶段竞争，move 复用相同的持久删除接纳与 finalizer（#348）；#205 的 Build resources、两级准入和 cgroup
权威不变；当前 main 仍有 post-registration Build projection，故 routesync v8 沿用 v6 引入的 Build full sync +
live delete 收敛它，但不改变 #46 的 immutable registered-node binding；若 #46 删除该 projection，
节点 TTL 本身不要求重建 lifecycle event。这里不执行任何远端 artifact GC，也不增加逐步骤
cleanup stage 或第二份路径权威。

因此 RouteSource.Range 与后续全量同步不会看到遗留 starting 被误发布为 running;初始 starting
已经持久化 network 但尚未分配 runner 的 crash 也能确定性释放端口并收敛到 dead/paused。

### 14.3 故障域

| 故障 | 影响 | 自愈 |
|---|---|---|
| conductor 崩溃/重启 | 控制面中断;沙箱(microVM/单元)与 running 数据流不受影响 | systemd 重启 → 重启对账收养;Proxy master 仍可用共享路由视图服务 running 流量(Wake 无人应答,paused 唤醒挂起至超时);集群下 node-link 重连重报；MMDS confidential authority 仍按 routesync loss 清空 |
| Proxy worker 崩溃 | 该 worker 上的连接断;其共享 admission 绝对计数 stale-high,其余 worker 可继续接新连接或保守拒绝 | stats stream fault 先终止该 worker;仅 `cmd.Wait` 确认旧进程退出后 master 才清空该 index,再以同一 index、新 epoch 启动 replacement;不改 route/Sandbox state,不影响 Create |
| Proxy master 崩溃 | 数据面中断,plugin 租约断开,Create fail closed | systemd 重启 master → 重新注册,重建共享表,启动 worker |
| runner 单元/CH 崩溃 | 该沙箱死(`Restart=no`,有状态不重试) | 在线 runner 扫描与启动对账先释放 exact ownership 再记录 dead(§14.2);proof/cleanup 不确定时保留 running 供重试. 客户重新 create 或从单独保留的 paused 制品 resume |
| routesync 断流 | Proxy 数据面视图停更;MMDS route/value/service 立即不可用 | Proxy master 清空 confidential heap 并指数退避重连重注册,完整同步 bookmark 后才重开 MMDS；fixed routes 保持原 retention/convergence 行为（node-proxy.md §4） |
| node-link 断流(集群) | registry 暂失本节点视图 | 节点指数退避重连重注册重报沙箱集(§10、cluster.md);本节点沙箱不受影响 |
| sqlite 损坏 | 控制面不可用 | 文件级备份/重建;沙箱单元仍可被 ListUnits 发现并由运维处置 |

## 15. 测试

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

必需的 owner 入口 [e2e_capture_cli.sh](../test/e2e/e2e_capture_cli.sh) 使用所选
真实产品执行三组 CLI 集成用例及全部当前子用例. 缺少 `BIN` 产品或已准备的
`ORCH_CLI_TEST_BIN`、空选择、未完成的子用例或任何 Skip 都使该入口失败.
既有 build/helper 阶段按独立 test pin 编译测试执行文件; hosted artifact E2E
只消费该文件和 `BIN`, 不检出源码或编译. 本地可选 `go test` 未设置
`KUASAR_TEST_SANDBOX_CTL` 时仍可跳过这些组. 将 `BIN` 指向所选且已构建的
产品目录, 从本仓库执行:

```bash
KUASAR_TEST_SANDBOX_CTL="$BIN/sandbox-ctl" go test ./internal/orch \
  -run '^(TestCapturePairCLIToPausedDatabase|TestUploadCaptureCLIToPausedDatabase|TestExportPublicationCLIAPI)$' \
  -count=1 -v
```

逐组核对当前子用例是否实际执行; 包含 Skip 的进程退出成功不构成 CLI 合同验证.

Native exec 的真实 microVM 特性用例分别覆盖 standalone 和 cluster 路径的
ExecAccessToken 签发,`service=exec` CONNECT 以及 guest 命令执行.该结论只对上述
native exec 路径负责,不表示同一聚合脚本的后续 pause/resume 等其它阶段已一并验收.

编排特性的 E2E 与实现一起维护在 `orchestrator/test/e2e/`。轻量 cluster stub 与需要
vmlinux、cloud-hypervisor、mkfs.erofs、sandbox-runtime.bundle 等多仓制品的真实 microVM
用例使用同一个 `run_all.sh`。直接调用脚本时通过 `BIN` 指向项目主仓组装的二进制目录；
`make test-e2e` 传入 `E2E_BIN`，默认 sibling 主仓 `bin/<architecture>`；它准备测试 helper 并执行必需的源码检查，但不构建这些产品前置，需先组装；
本地 stub 则使用 `make build` 后 `make test-e2e-cluster-stub`。
组件 PR 的集成测试则把候选仓与其余仓源码组成统一环境后执行该入口。缺少重型前置时单脚本可
跳过,完整门禁设置 `REQUIRE_*=1` 后硬失败。

直接运行 Proxy E2E（包括其 MMDS restart wrapper）时，先准备本机架构的 helper，并传入两个可执行文件路径。以下命令从 orchestrator 仓库执行，要求产品二进制已组装且既有 KVM/Docker/zot 前置可用：

```bash
e2e_arch="$(uname -m)"
make e2e-fixtures TARGET_ARCH="$e2e_arch"
e2e_tools="$PWD/build/e2e-tools/$e2e_arch"
BIN="$PWD/../kuasar-sandbox/bin/$e2e_arch" \
CUSTOM_PROXY_BIN="$e2e_tools/custom-proxy" \
TELEMETRY_GRPC_PROBE_BIN="$e2e_tools/telemetry-grpc-probe" \
REQUIRE_PROXY=1 bash test/e2e/e2e_orchestrator_proxy.sh
```

如产品组装在其它目录，请调整 `BIN`。脚本在重型环境准备前检查必需的 gRPC probe，不在 E2E 中编译 helper。完整套件的 `make test-e2e` 会自动传入这些路径。

| 脚本 | 覆盖 |
|---|---|
| `e2e_orchestrator.sh` | 单元自动安装 + 控制面(`/health`、401 路径)+ 构建 API 生命周期(register/trigger/status、跨 key 归属 404)+(有 KVM 时)bare create/list/kill |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器(纯用户态,无 root/systemd/KVM):pidfile 锁/双起拒绝、exact-run bootstrap、cold单阶段/restore两阶段、execve、`TASK_*`和重复MANIFEST_KEY剥除;`config` CLI 往返 |
| `e2e_builder_unit_upgrade.sh` | 隔离真实 systemd:生成 slice 更新, live runtime properties 保留, 按 exact source 一次性移除;reload 后检查实际 cgroup 值.  |
| `e2e_run_builder.sh` | target-aware 三阶段构建流水线(KVM + vswitch + store-ctl + zot,guest 经 mgmt VIP 拉取):真实 IMG/SBX/SNP、fromImage/fromTemplate(img/sbx/snp)、image/checkpoint publication matrix、Manifest 与 named-location Bundle、顶层 E 无完整 staging/零 Phase C、portable Phase-C image ref、local/Bundle checkpoint mode、S→E cold selection、memory C 固定等待、source-ref closure 与 Build-row TTL 后 canonical Create;另覆盖 COPY/bare,父级/ctl 隔离与有限 VMM 控制,conductor 崩溃后保持 exact claim/run-id/进程的 live Build 接管,真实总超时 fencing 与 claim/reservation 释放,终态 cleanup,日志/DB/artifact secrecy |
| `e2e_execute.sh` | 从已建模板冷启真实 microVM、guest 内 exec、持久 RunDir/BaseDir、BaseDir writable diff/checkpoint、RunRoot 无大工件、PathID native exec、local Pause→resume 与恢复策略、failed Create 的 dead 零 ownership、显式 delete finalizer 删除 row/RunDir/BaseDir 且不伤 node-level 文件 |
| `e2e_mmds_routes.sh` | 复用 execute 的 Proxy 拓扑覆盖 static,secret 生命周期与 local UDS service |
| `e2e_mmds_routes_proxy_restart.sh` | 复用 Proxy 拓扑覆盖 MMDS full resync/fail-closed 恢复 |
| `e2e_orchestrator_proxy.sh` | Proxy master/worker,唯一 data ingress,路由同步,数据面鉴权,auto-resume 与 CONNECT relay |
| `e2e_cluster_stub.sh` | 真实 registry/router/placer + node-stub-ctl,以不同 API/Data listener 覆盖 node-link,Reserve,control/data/exec/build 路由,稳定 SandboxID 与成员变更 |
| `go test ./test/e2e/cluster_stub` | 可执行的 h2c node-link/Registry/Router/Placer 集成；覆盖 Build live projection、连接中丢失 Delete 后以空 Build full snapshot 删除 projection 与 exact owner ref |
| `e2e_cluster_real.sh` | 真实 cluster 控制面、node-ctl 与 microVM,覆盖单 registry、node-link redirect，以及 cluster Delete 后 Registry route 与节点 row/RunDir/BaseDir 的共同收敛 |
| `e2e_density.sh` | 节点资源准入、回收与密度行为 |
| `e2e_sandbox_cold_target.sh` | node-ctl 资源控制器驱动 production-shaped sandbox target 冷启动 |

`make test-e2e` 即执行 `test/e2e/run_all.sh`;项目主仓只提供统一环境、聚合入口及真正跨组件
组合本身的用例,不复制上述脚本。

### 原生 usage 读取验证

密度 benchmark 测量有界读取及 FD/goroutine 行为,
不设置机器相关门槛; 原生采样和 history append 算法保持不变.

```sh
go test ./internal/orch -run '^$' -bench '^BenchmarkNativeUsageBatch$' -benchmem -count=3
```

此 benchmark 在每批 1/16/64 个对象下读取真实 SQLite 对象和原生 saved 文件,
报告分配量及保留 FD/goroutine 的变化; 不代表 guest 采样或远端导出吞吐.

## 16. See Also

- [node-proxy.md](node-proxy_zh.md) —— 独立数据面转发层:路由判定 / routesync /
  数据面鉴权 / MMDS / CONNECT 隧道(本文 §9 的唯一数据入口,集群下 router 转发进入)
- [node-resource.md](node-resource_zh.md) —— 节点资源控制协议、sandbox resource policy
  与控制器内部组织(serve 经唯一的 `resource_listen` endpoint 内置)
- [cluster.md](cluster_zh.md) —— 集群控制面:node-link 线格式(§6,本文 §10 的对端),注册表,
  Reserve 状态机;[cluster-router_zh.md](cluster-router_zh.md) 数据面入口,[cluster-placer_zh.md](cluster-placer_zh.md)
  放置与密钥分发
- [沙箱生命周期](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md) —— sandbox-ctl:SANDBOX_CONFIG 模式、
  run/snapshot/restore/connect 原语、cgroup 模型
- [vSwitch 运维](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch-operations_zh.md) —— attach/detach/open-port;
  [vSwitch 设计](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch_zh.md) 维护 floatingip 与 mgmt-service(MMDS VIP 转换)语义
- [镜像展平](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten_zh.md) —— flatten-ctl export 与 OCI Referrers 幂等流、
  `FLATTEN_REGISTRY_*`
- [Manifest](https://github.com/kuasar-sandbox/accelerator/blob/main/docs/manifest_zh.md) —— manifest 内容键、收敛加密与去重域
  (§7 的存储侧)
- [部署](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment_zh.md) —— 节点部署拓扑中本组件的位置与单元安装
- [演示](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/test/demo/DEMO_zh.md) —— e2b CLI/SDK 全流程演示
