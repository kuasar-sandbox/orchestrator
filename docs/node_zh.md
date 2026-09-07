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
   逻辑解耦的可分离子系统。构建任务的资源池由 serve 自管(§12)。
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
   存活权威对账收养/清理(§15);集群下另经 node-link 重连重报本节点沙箱集让 registry 收敛。

### 1.3 两类沙箱(profile)

| profile | 是什么 | 数据面 | 对外服务 |
|---|---|---|---|
| **e2b** | guest 内跑原版 envd,agent 经其 exec / 读写文件 / 跑代码 | envd fs/process/pty/runCode + 平台 native exec | envd/CI + floatingip 用户端口 + native exec |
| **bare** | 把客户镜像当网络化 microVM 跑起来,无 envd | 平台 native exec;不提供 envd API | floatingip 网络 + native exec |

`bare` 直接复用基础沙箱运行时(`sandbox-runtime.bundle`),把基础沙箱接上北向 API;
Build register 缺省为 **e2b**，也明确支持 **bare**；输出 profile 在注册后不可变。
profile 编码在 templateID 前缀里（§4.4），运行期据此选择共享 runtime Bundle 内的 guest 启动行为与数据服务。

### 1.4 边界与依赖

- 北向客户:e2b SDK / CLI 直连;集群下经 cluster-ctl router 转发(数据面)+ node-link
  下发命令(控制),平台管理面亦可经 e2b API 对接。
- 既可**独立运行**也可**接入集群**:控制面(create/pause/kill/模板构建)始终在本节点;
  接入集群仅多一条 node-link(§10),不改 e2b 契约。
- 不实现 envd 协议:数据面只透传到 guest 内原版 envd(§4.3)。
- 不实现 e2b `/sandboxes/{id}/metrics` 时间序列或 envd metrics 采集;平台另提供只读的
  `/sandboxes/{id}/stats/resource` 与 `/sandboxes/{id}/stats/traffic` 即时快照(§4.1.1)。
- 服务端不解析 Dockerfile；客户端展开的结构化 steps 会在构建 guest 内执行，支持镜像拉取、
  展平与最多三阶段流水线（§12）。
- 节点本地:路由、存储、单元管理都是节点本地的;跨机快照/模板使用 canonical
  portable ref(数据位于 manifest store 或统一挂载的 named location,§8.1),编排走
  cluster-ctl 的 node-link(§10)。
- 依赖:stdlib + `modernc.org/sqlite`(纯 Go)+ `golang.org/x/net/http2`(h2c,
  config-socket 与 node-link 共用)+ `golang.org/x/sys`(pidfile 锁 / SO_PEERCRED /
  mmap)+ `coreos/go-systemd`(D-Bus)+ `google/uuid`(v7)+ `gopkg.in/yaml.v3`。
  还使用公开 sibling packages、CEL/protobuf 与 S3 AWS SDK。envd/node-link 的本文 wire 使用
  Connect+JSON 或 framed JSON，不使用 gRPC transport；不能据此声称整个 go.mod 没有 protobuf。

### 1.5 架构与数据通路

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

create 同步受理流程只做请求校验/纯解析、身份和 token 生成、进程内 launch ownership claim,
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
(§10、§4.6)。

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
| `builder status` | 查看构建两级准入、持久用量、headroom 与队列（§12） |
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
| `seal-pull-token [<MANIFEST_KEY>] …` | 封装不透明镜像拉取令牌(`kpt_`,§12) |
| `version` | 版本 |

接住一个新节点(独立模式)的典型顺序:

```bash
# 1) 生成内容根密钥,派生缺省 APISecret,登记凭据对,签发 SDK 用的 api key
MK=$(e2b-key-ctl gen-key)
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
node-ctl manifest-key add --api-secret "$API_SECRET" --label tenant-a "$MK"
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$API_SECRET")

# 2) e2b SDK/CLI 直接指向本机
export E2B_DOMAIN=sandboxes.example.com        # 生产(TLS, §13)
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
重启对账(§15)→ 起 reaper(TTL,5s 周期)→(配 `resource_listen` 则起内置资源控制器,
node-resource.md)→ 起本机控制 socket并确认监听成功(§6)→ 起 runner/builder 预启动池与
构建准入循环(§12)→(配 `cluster.node_link.endpoint` 则拨 registry 起 node-link 客户端,§10)→
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
- **run-builder**:取得 bid 后锁
  `<BuildRunDir>/builder.pid`,以 bid + exact run-id 取 bootstrap。
  fromImage 与 IMG fromTemplate 在这一次 RPC 中直接取得最终 BuildSpec；SBX/SNP fromTemplate
  则安装 authoritative `MANIFEST_KEY`,在本进程执行 cold/E-selection Artifact prepare；SNP
  只以 S 定位 E，完整 E/source-image config 与已过滤的 sorted ref-location 留在 task 内，
  向 conductor 只提交非秘密 bounded summary 并等待最终 BuildSpec。reader/fetcher
  在提交前已显式关闭，conductor 不读取任何 tenant artifact。run-builder 将本地保留结果合入
  final spec 后**驻留**驱动 target-aware、最多三阶段的构建流水线(§12):各阶段沙箱(`sandbox-ctl run`)是它的直接子进程,
  整个构建计入本单元 cgroup;结束把结果
  `{target, exactly-one-of image_ref|sandbox_ref|snapshot_ref, start_cmd, ready_cmd, error, failure_stage}` 经 config-socket 回传。根cfg读取、
  prepare RPC、phase report与pipeline统一在同一个absolute deadline内预留最多最后5秒用于持久
  回传结果；剩余预算不足10秒时预留其一半，避免短timeout在工作开始前即过期，同时不会重置
  或延长总预算。只有最终result POST可使用这段尾窗。
  BuildID 始终是持久业务身份并原样作为目录 leaf；48-byte 上限保证最深 phase UDS 在
  支持的 RunRoot 下仍不超过 Linux `sun_path`。

```
node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
node-ctl run-builder --pidfile=<f> --config-socket=<uds> --run-id=<rid>
```

flags 缺省回落 `TASK_PIDFILE` / `TASK_CONFIG_SOCKET` / `TASK_RUN_ID`
env(systemd `%i` 接线用)。

### 2.5 `node-ctl config`

配置诊断 + 生成工具,**按角色**(`conductor` / `proxy`,各自独立文件与 schema):

```
node-ctl config <conductor|proxy> --template            # 输出该角色带注释骨架
node-ctl config <conductor|proxy> --config <file>       # 加载(补默认 + 校验)后重排输出
node-ctl config <conductor|proxy> --config <file> --resolve   # 再展开 auto/派生(实际生效形态)
                              -o <file>             # 写文件(默认 stdout)
```

角色作首参以消歧 schema:`conductor` 对应 `conductor.yaml`(§3),`proxy` 对应 `proxy.yaml`
(node-proxy.md §2)。`--resolve` 对 `conductor` 额外展开 `resource_listen` 的 `auto` 内存/CPU
(并深校验水位),其余角色与 `--config` 等价。骨架与 `deploy/{conductor,proxy}.example.yaml` 对应。
该命令只做严格 declarative decode、默认化与诊断，绝不执行 `conductor_executable` /
`proxy_executable`，也不接触 custom App 的运行时材料。custom 路径非空时，输出首行明确
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
  (`registry_auth_enc`),构建拉取时按 fromImage 的 host 匹配取用(§12)。
- `add/remove/check` 输出 `STATUS api=<64-hex> manifest=<64-hex>`;`list` 逐行输出两项完整指纹。
- 鉴权:`SO_PEERCRED`——配置了 `paths.admin_pidfile` 则 peer pid 须在其中;未配则仅靠
  socket 0600 权限(同 uid / root)。

### 2.7 `node-ctl export-sandbox` / `import-sandbox`

serve daemon **api 平面**的客户端(经本机控制 socket 调 `POST /sandboxes/{id}/export`
与 `POST /sandboxes/import`),鉴权 `E2B_API_KEY` env(须属主)。语义见 §8.1。

```
node-ctl export-sandbox <sid> [--to-template] [--keep-source] [--socket S]
node-ctl import-sandbox <token> [--socket S]
```

- `export-sandbox <sid>`:打印单行 `kmt1.` opaque 迁移 token.成功进入 source
  finalizer 后默认删除源沙箱;`--keep-source` 保留 paused source.
- `export-sandbox <sid> --to-template`:发布 paused Sandbox E 或 Snapshot S 并打印对应的
  持久 `sbx` / `snp` templateID(扇出用).
  `--keep-source` 使用相同的 source retention 语义;未指定时同样删除源沙箱.
- local artifact 上传期间统一允许 Resume,不增加额外开关。Resume 先受理时,KMT
  Export 取消上传并返回 409;Template Export 继续上传并返回 templateID。两者都放弃
  source finalizer,不更新/删除 source,也不删除 local artifact.
- `import-sandbox <token>`:缺省复用 token 中的 source NodeSandboxID,以 insert-only 方式
  写入 paused 行并打印 sid;目标已存在返回 409。API body 可通过可选 `sandboxID` 指定另一
  个 node-local target,但不会改变逻辑认证主体或既有 service credential。随后调用
  `connect` 即可异步恢复。

### 2.8 `e2b-key-ctl`

总览见 §2.1。`seal-pull-token` 完整形式:

```
e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] {--registry-username U --registry-password P |
                                              --registry-token T}
```

输出 `kpt_` 前缀的不透明令牌:以租户 manifest_key 派生密钥 AES-GCM 封装的镜像拉取
凭据,经 SDK `api_headers`(头 `X-Kuasar-Pull-Token`)随构建请求传入,serve 用
该租户的存量 key 解封(§12)。manifest key 取自首个位置参数或 `MANIFEST_KEY` env。

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
| `api.tls.cert/key` | 空 | 通配证书(`*.<domain>` 与 `api.<domain>`,§13);空 = 明文。custom Runtime TLS provider 非 nil 时为权威材料源，core 仍固定 TLS version/ALPN/client-auth 策略 |
| `proxy.park_timeout` | `30s` | 数据面请求挂起预算:等路由同步 / paused 沙箱 resume 的上限(node-proxy.md §4) |
| `proxy.auth` | `enforce` | 数据面鉴权:`off`/`log`/`enforce`,校验 `X-Access-Token`(node-proxy.md §6) |
| `proxy.metrics_listen` | 空(关) | conductor 进程的全局 Prometheus 文本端点;Proxy worker 数据面指标在 `proxy.yaml` 的 `metrics_listen` |
| `encryption_key` | 内置模式必填 | APISecret/ManifestKey 凭据对落盘加密的 AES-256 密钥:`:` 分隔多个 64-hex,首个为活动密钥,其余备用解旧记录(轮换);优先级为 custom Runtime provider > `NODE_CONFIG_ENCRYPTION_KEY` > YAML，provider 失败不回退 |
| `manifest_config` | `/opt/sandbox/manifest.yaml` | 共享远程 manifest store 配置(`manifest.key` 留空,租户 key 经 env 按任务下发) |
| `paths.conductor_executable` | 空 | 静态定制 conductor 的绝对 executable；空使用内置实现。`node-ctl config` 只诊断 regular/executable、非 group/world-writable、非 node-ctl same-file 元数据，不按诊断 EUID 判断 owner；实际 dispatch 中 root node-ctl 只接受 root-owned，非 root node-ctl 接受 root-owned 或本 EUID-owned。它执行已打开并校验的同一 FD，失败绝不回退 |
| `paths.run_root` | `/run/sandbox` | 节点 RunRoot:node-level 小文件、`runners/`、`sandboxes/`、`builds/`；通常为 tmpfs；必须为最大 BuildID 的最长 phase UDS 留出 107-byte Linux pathname 预算(§1.6) |
| `paths.base_root` | `/var/lib/sandbox` | 节点 BaseRoot:node-level 持久文件与 `sandboxes/`、`builds/` 大体积数据(§1.6) |
| `paths.db_path` | `<base_root>/node-ctl.db` | sqlite 路径(§15) |
| `paths.config_socket` | `/run/sandbox/node-ctl.socket` | 本机控制 socket(run assignment/result + task/admin/plugin/api,§6);manifest-key/export/import CLI,Proxy 与平台 agent 的连接点 |
| `paths.admin_pidfile` | 空 | admin 平面的多行 PID 白名单(`#` 注释);未配则仅靠 socket 0600 |
| `paths.plugin_pidfile` | 空 | plugin 平面(proxy/agent 注册)的多行 PID 白名单;未配则仅靠 socket 0600 |
| `units.dir` | `/etc/systemd/system` | 模板单元安装目录 |
| `units.runner` / `units.builder` | `sandbox-runner@.service` / `sandbox-builder@.service` | 模板单元名 |
| `units.runner_pool_size` / `units.builder_pool_size` | `0` / `0` | 空闲预启动 run-id 单元数;0 = 不保留 idle,有任务时仍按需经 WaitAssignment 流程启动。execution 配置 CPU 或 memory 聚合上限时 `builder_pool_size` 必须为 0，避免未获 execution claim 的 idle 进程占用受限 Builder slice。 |
| `units.pool_wait_timeout` | `5s` | 从调用 `StartUnit` 到单元进入 WaitAssignment 的正数时限;超时清理该 run-id 并补池 |
| `units.install` | `true` | `false` = 单元由运维带外管理,serve 不生成安装 |
| `sandbox.timeout_sec` | `300` | 沙箱默认 TTL(秒) |
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
| `sandbox.boot.overlay_diff_template` | – | 预格式化空 ext4,img 冷启时稀疏复制为可写 upper(裸空 diff 非合法 fs 会被拒);部署方 `mkfs.ext4` 于稀疏文件提供;restore 不需要(overlay 链来自快照) |
| `builder.admission.execution.max_builds` | `2` | 同时持有 durable execution claim 的 Build 上限 |
| `builder.admission.execution.resources.{cpu,memory,storage}` | 不限制 | execution 的聚合资源向量;CPU/memory 同时施加到 `sandbox-builder.slice`,storage V1 仅准入记账 |
| `builder.admission.registration` | 完整继承 resolved execution | 所有非终态 Build 的注册上限;显式块不做字段级继承,且同一有限维度不得小于 execution |
| `builder.registration_ttl` / `.queue_ttl` | `1h` / `30m` | 未 Trigger 的 registered Build 与 waiting Build 的持久超时;终态可查询并释放 registration usage |
| `builder.terminal_ttl` | `24h` | 已完成 cleanup、无 execution/runtime/result owner 的 `ready/error` Build 历史保留期；必须为正 Go duration |
| `builder.insecure_registry` | `false` | 经明文 HTTP 拉取 base 镜像(dev/本机 registry) |
| `builder.platform` | 空 | 拉取平台,如 `linux/amd64` |
| `builder.image_uri_mask` | 空 | 客户端推送镜像的命名约定(含 `{templateID}`/`{buildID}` 占位,须与 e2b CLI 的 `E2B_IMAGE_URI_MASK` 一致);trigger 缺 `fromImage` 时据此推导;**须从构建沙箱内可达**——拉取在 guest 内进行(§12) |
| `builder.referer` | 关 | fromImage import 的 OCI Referrers cache:`enabled` 默认 false;`fallback`/`writeback` 默认 true;`desc` 为公开 owner descriptor(启用时必填);`key` 为空则等于 desc;`validity` 为可选 Go duration。build 可经 `X-Kuasar-Sandbox-Builder` 进一步禁用 lookup/writeback,不能越权启用(§4.6、§12) |
| `builder.diff_template` | – | 构建沙箱可写盘的预格式化 ext4(拉取缓存 + steps 增量 + 导出 scratch;稀疏文件,建议 ≥ 最大预期镜像的 3 倍) |
| `builder.{pull,step,ready,total}_timeout_sec` | `600`/`600`/`120`/`1800` | 阶段超时:guest 内拉取+展平、单条 RUN step(经 `Connect-Timeout-Ms` 同步到 guest 侧)、readyCmd 轮询预算(2s 间隔;缺省 readyCmd = `sleep 20`)、整个构建(单元 `TimeoutStartSec` = total+60) |
| `builder.files_storage` | 空 | COPY 构建上下文的 S3/OBS 对象存储(子键 `endpoint`/`region`/`bucket`(必填)/`prefix`/`access_key`/`secret_key`/`force_path_style`/`presign_expiry`);空 = COPY 回 501。serve 仅 presign + HEAD;custom Runtime credentials provider 优先于静态 YAML/AWS 默认链、支持 session token/expiration/refresh 且失败不回退;`force_path_style` 默认 false(versitygw/minio 置 true);`presign_expiry` 默认 1h(PUT;GET 用 total+5m)。本地/单机无云对象存储用 versitygw(§12) |
| `checkpoint.mode` | `local` | 暂停态本机 capture:`local` = 现有 tarstream,`bundle` = multi-Manifest ZIP Bundle；输出固定在 Sandbox `BaseDir/checkpoint`(§1.6、§8.1) |
| `checkpoint.merge_ref` / `.drop_caches` | 未设置 | Pause 的节点级三态策略:`true`/`false` 显式传给 `sandbox-ctl snapshot`;省略或 YAML `null` 则交给 sandbox-ctl 缺省 |
| `checkpoint.remote.ref_location_parent` | 空 | 可选 absolute hostless `file://` URI；Builder checkpoint 类 graph 和 `export-sandbox` 的 named-location parent，也用于解析 located Build image Bundle；不改变 Pause capture mode |
| `checkpoint.remote.manifest` | `false` | `false`：Build image 类进入 Manifest store；`true`：image 类不写 Manifest store，而以 single-root Manifest Bundle 物化到上述 named location，且 parent 必填。它不改变 `checkpoint.mode` 或 checkpoint 类 policy（§12） |
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

### 3.1 静态定制 conductor

运维入口保持不变：`node-ctl conductor serve --config ...` 先做环境无关的严格解析、默认化和
declarative validation。`paths.conductor_executable` 为空时 node-ctl 显式 final validate 并在
runtime resolution 解析 key/TLS/credential 材料；非空时 node-ctl 打开 protected absolute
executable，依据该 FD 校验 runtime owner/mode/file identity，并通过 `/proc/self/fd` 执行同一文件，
不在校验后重新解析可替换的 pathname；sealed bootstrap 同时记录该已打开文件的 device/inode，
xconductor 只将它与 `/proc/self/exe` 比较，部署期间 pathname 被替换或删除不会改变已验证身份。
随后 node-ctl 用 `exec` 原地替换为 xconductor。
bootstrap 环境变量只含 FD 编号；配置正文/摘要在 memfd 中，FD 禁止 write/grow/shrink 并
最终 seal。xconductor 直接运行、bootstrap 缺失/截断/超限/version/digest/component 不匹配
均 fail closed。这个交接用于进程组织和防误用，不宣称抵抗同 UID 恶意进程。

custom main 只需要公共包；完整可编译版本见 `examples/custom-conductor`：

```go
app := conductor.New(conductor.Hooks{
    Configure: func(ctx context.Context, cfg *conductor.Config, rt *conductor.Runtime) error {
        // 替换/调整 declarative Config；绑定启动期 Runtime provider。
        rt.Extension = myExtension
        return nil
    },
})
if err := app.Run(); err != nil {
    log.Fatal(err)
}
```

`New` 无副作用，`Run` one-shot 并处理 SIGINT/SIGTERM；托管方可用 `RunContext`。App 不调用
`os.Exit`。执行顺序固定为：decode bootstrap → clone Config → `Configure` exactly once →
校验 `paths.conductor_executable` 未改变 → final declarative validation → 再 clone/freeze →
解析 Runtime 材料 → 启动共享 conductor core。Configure/provider/final-validation 失败时尚未打开
durable store、listener、systemd launcher/unit 或 node-link。Hook 可整体替换 Config，但必须
恢复最初冻结的 executable；Hook 后不会重新应用默认值。

`App` 必须由 `conductor.New` 构造；零值或 nil receiver 的 `Run` / `RunContext` 在安装 signal
handler、读取 bootstrap FD 或启动 goroutine 之前返回明确错误。`node-ctl config conductor`
只做 declarative/bootstrap 与 executable metadata 诊断，不执行 custom App/provider，也不以
诊断进程 EUID 代替实际 service owner policy；custom 模式明确提示 runtime owner 与 final
validation 均延后到 component startup。

`Config` 只含可序列化声明；`Runtime` 是禁止 JSON 序列化的进程对象，开放 logger、
TLS material、AES-256 ordered key set、builder files-storage neutral credentials provider，
以及一个可信、静态编译的 `Extension`。
TLS provider 返回 DER certificate chain、`crypto.Signer` 与 root/client CA pool，不能返回任意
`*tls.Config`；最低 TLS 版本、ALPN、mTLS/client verification 仍由 core 固定。所有 provider
只在启动/SDK credential refresh 使用，不进入请求热路径；provider 非 nil 即为权威来源，
任何错误都不回退文件、环境或静态 credential。V1 不支持热更新。

Extension 的 `Start(ctx, Host)` 在 store/launcher/core 构造后、InstallUnits/reconcile/pool/
node-link/listener 之前恰好调用一次；失败会中止启动，`ctx` 取消通知 Extension 自有 goroutine
退出。`Host` 提供 Sandbox/Build `Get+Watch` 非秘密深拷贝视图。Watch 使用
`sync_begin → snapshot → sync_end → live` generation；慢 watcher 只使自己的 generation
失效并自动 full resync，不保证观察每个中间变化，也不是 durable audit。完整合同见
[extensions.md](extensions.md)。

同一 Extension 可选实现 `SandboxHook`、`BuildHook` 与 `APIWrapper`；这些能力只在 Start
成功后检查一次并冻结。生命周期 Hook 都在认证后、durable/runner/network/snapshot 副作用前
调用，并采用“锁内捕获 precondition → 锁外 Hook → 锁内权威重读/重验 → commit”。普通显式
Delete 可拒绝，TTL/rollback/reconcile/shutdown 等 mandatory cleanup 永远绕过 Hook。
Build Register Hook 位于 capacity transaction 前，Trigger Hook 位于最终 registry credential
解析和 registered→waiting CAS 前；waiting→building claim 无 Hook。cluster BuildRegister 的
exact replay 不重复执行可变 Hook。`APIWrapper` 可增加、改写或覆盖任意 core API route；同一
wrapped handler 同时服务公网 API 与 config-socket API fallback，config-socket internal route
不经过它。Hook 的 `ErrRejected` 映射固定 policy rejection，其他错误映射固定 503，不回显私有
细节。

xconductor 从 bootstrap 得到最初 node-ctl 的精确路径。生成的 runner/builder systemd unit
仍执行该 node-ctl；`sandbox-ctl`、`connector-ctl`、`flatten-ctl`、`manifest-ctl` 的相邻目录
解析也以 node-ctl 发行目录为准，不以 xconductor 目录为准。custom component 与 node-ctl
必须来自兼容版本。该 API 不开放 store/launcher/vswitch/orch/Router；API 只以
`http.Handler` next 形式交给可信 wrapper，不暴露 internal 类型。它不引入 Go plugin、运行时
发现,全局 registry,动态 middleware 注册或 DI container.conductor 始终只服务 control API.

### 3.2 静态定制 Proxy

`paths.proxy_executable` 为空时 `node-ctl proxy` 在 declarative decode 后显式 final validate并
运行内置 App；非空时 node-ctl 校验 protected absolute executable 的 runtime owner/mode/identity，
并通过 sealed memfd + 原地 exec 交接
到 xproxy，失败不回退。xproxy 必须经 `node-ctl proxy serve` 启动，不能独立运行。公共
`app/proxy` 只开放 `New(Hooks)`、one-shot `Run`/`RunContext`、master-only `Configure` 及
master/every-worker `BindRuntime`；`Config` 是声明式值，`Runtime` 是不可序列化的 logger/TLS
material provider，并可在 master/worker 分别绑定一个可信、静态编译的
`MasterExtension`/`WorkerExtension`。provider 非 nil
时权威，错误不回退文件，TLS policy 仍由 core 固定。

master 创建共享 route table 与 traffic aggregate 后、绑定任何 listener 或启动 routesync/worker
前，恰好调用一次 `MasterExtension.Start(ctx, MasterHost)`；失败清理 SHM 并中止。Host 的
RouteSource 提供 applied route `Get+Watch+SyncState`，generation/resync 允许重复且不是 durable
audit；TrafficSource 直接读进程内聚合，不走 stats UDS。observer 只在 core apply 成功后有界、
非阻塞发布，慢 callback 不影响 SHM、routesync、barrier ACK、Wake 或 worker。Start 后同一对象
的可选 `ManagementWrapper` 可添加、覆盖或透传 `stats_socket` route；没有 namespace 或 conflict
registry。公共 View 不复制原始 secret/token，也不新增 route metadata 或 SHM schema。详见
[extensions.md](extensions.md)。

master 在 Configure/final validation 后 deep-clone、canonical serialize 并 digest 冻结
EffectiveConfig，再用自己的 `/proc/self/exe` 启动 worker：内置模式是 node-ctl，custom 模式是
xproxy。worker 通过 sealed bootstrap 验证 config digest、role/id/epoch、FD mapping 和 executable
identity,并映射独立 admission arena。`proxy.yaml` 的 `traffic.max_inflight` 是目标节点对每个
Sandbox 的默认 logical inflight policy;全部字段 `0` 表示 unlimited,不是 QPS 或 Proxy global
capacity。worker 调用 `BindRuntime(worker)` 并在 ready 前完成 stats/route sync；它不读取
`proxy.yaml`，不调用 `Configure`。每个 worker epoch 的 `BindRuntime` 必须创建新 Extension；
初始 route sync 后 core 调用其 `Start` 一次,再冻结可选 `IngressWrapper`,成功后才开放
Data listener.wrapper 在 canonical parser 前接收 raw request,只服务节点 sandbox data ingress;
MMDS 不经过它.`GetRoute` 只提供当前 SHM 点查副本,不提供 worker Watch。
Worker `Run` 是 one-shot；返回后调用方必须退出进程，不能在同进程重启。共享 mapping 为 process-owned，
Close 只关闭 FD；master 必须 Wait 确认退出后清计数，见 [worker lifetime](proxy-worker-lifetime_zh.md)。

已完成私有认证的 wrapper 可调用拥有 HTTP 响应的 `ForwardAuthorized`，复用 lookup、traffic
admission、parking、
activation/Wake、binding revalidation、dial 和 traffic 生命周期；它不验证 Kuasar token。
`Revalidate` 在 activation 后、dial 前 fence 私有 revision，`Rewrite` 只修改 ordinary HTTP 的
guest clone，CONNECT 不调用它；generic helper 拒绝 native exec，标准 `next` 仍走 KAT/CEL。
配置中的 executable 仅选择 node-ctl → master，用户 Hook
不得改变它。V1 不支持热更新，custom component 与 node-ctl 必须来自兼容版本。完整 API、
示例、进程模型、安全边界和非目标见 [node-proxy.md](node-proxy_zh.md) §2.1。

该扩展仅覆盖独立 Proxy;cluster-router/registry/placer 不增加 Extension.
除上述 master management 与 worker ingress wrapper 外，不开放 listener、原始
SHM/Router/routesync/stats，也不引入 namespace、plugin registry、动态加载、通用生命周期 hook
或 DI。WebSocket 不属于 Issue #256，由 Issue #269 独立跟踪。

Builder 配置直接替换下列旧字段，不保留 alias：

```text
builder.max_concurrent -> builder.admission.execution.max_builds
builder.cpu_quota      -> builder.admission.execution.resources.cpu
builder.memory_max     -> builder.admission.execution.resources.memory
builder.vcpu/memory    -> 删除；A/B phase 从 immutable Build.Resources 派生
```

若显式配置 registration,它不会从 execution 隐式继承省略维度;若整块省略则完整继承
resolved execution。现有开发数据库按当前 schema 直接重建,没有旧/新字段双读或迁移 fallback。

远程内存 Prefetch 没有节点统一开关。是否请求 Prefetch 由每个 sandbox 的
`kuasar-sandbox.restore` 命名空间决定(§4.6)。

配置自洽校验:`mmds.enabled=false` 时 `proxy.auth` 必须为 `enforce`(envd 非 secure,
Proxy 是唯一数据面闸门);`mmds.routes.enabled=true` 还要求 `mmds.enabled=true`,service endpoint
必须是 absolute Unix socket URI.`proxy_netns` 和 worker 数只配置在 `proxy.yaml`.
配 `cluster.node_link.endpoint` 时 `cluster.node_id`,`cluster.api_endpoint` 和
`cluster.data_endpoint` 都必填;配
`resource_listen.enabled=true` 时 node-ctl 在启动阶段解析一次 canonical controller
identity并自动写入每个 dynamic sandbox YAML;没有第二个
`sandbox.resources.control_socket` 配置源。startup 对 static/dynamic 使用同一 headroom
语义,不依赖 controller 是否启用。

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
| resource stats | `GET /sandboxes/{id}/stats/resource` | 只读 resource controller reservation/report;sparse JSON,不访问 envd |
| traffic stats | `GET /sandboxes/{id}/stats/traffic` | 最终 node proxy 当前 parking/egress 与保守 `idleSince`;不 Wake/Resume |
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

create 的 `templateID` 接受三种引用:持久 id(`<profile>-<kind>-<base64url-ref>`,§4.4)、注册期
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

`GET /sandboxes/{id}/stats/resource` 只消费 node-ctl 内置 resource controller 的当前
reservation 和 sandbox-ctl 已上报的 host charge 样本。示例:

```json
{
  "timestampUnix": 1786482600,
  "cpuCount": 2,
  "cpuAllocatable": 0.5,
  "memUsed": 536870912,
  "memTotal": 2147483648,
  "memAllocatable": 1073741824
}
```

每个字段都可省略:未采集就不序列化,不以零值伪造。`timestampUnix` 是最近一次携带
非零 host VMM charge 的 Settled/Heartbeat 时间;`memUsed` 是 sandboxer 报告的 VMM
cgroup `memory.current`,不是 guest demand/working set。`memTotal` 是 Capacity;
`memAllocatable` 是现有 API 名称,其值为 node reservation。controller 未启用为 501;
starting 且 reservation 已存在可返回 sparse 200;paused 无 live reservation 为 409;running
但 reservation 缺失为 503。reservation 存在而尚无 host-charge report 时仍返回其它可得字段。
它不是 guest `/metrics` 的兼容实现。

`GET /sandboxes/{id}/stats/traffic` 返回最终 node proxy 已鉴权接纳的逻辑 ingress:

```text
ingress = parking + egress
parking = 鉴权成功,但生命周期激活/最终 backend dial 尚未完成
egress  = 最终 node proxy→sandbox backend 已建立且尚未最终 Close
```

```json
{
  "state": "running",
  "maxInflight": {
    "total": 128,
    "forward": 96,
    "e2b:envd": 16,
    "e2b:code-interpreter": 8,
    "exec": 8
  },
  "inflight": {"parking": 0, "egress": 0},
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "forward": {"parking": 0, "egress": 0, "idleSince": "2026-08-12T14:03:21.123456789Z"},
    "exec": {"parking": 0, "egress": 0, "idleSince": "2026-08-12T14:00:00Z"}
  }
}
```

`maxInflight` 来自 Proxy master 当前 applied route 的目标节点 effective policy,不是从
conductor Sandbox row 推导;全零对象表示 unlimited。配置 `M` 是整个 node Proxy 对该
Sandbox 的近似上限,`N` 个 worker 的短暂理论上界为 `M+N-1`,不是每 worker 的 `M`。

e2b 的 service 集是 `forward/e2b:envd/e2b:code-interpreter/exec`,bare 是
`forward/exec`。顶层 `inflight` 是各 service 求和。每个 service 仅在两项为零时附
`idleSince`;顶层仅在 state=running 且全零时附所有适用 service 时间的最大值。
starting/paused 返回 inflight 但不返回顶层 idle。V1 不返回 idle bool/duration、last
open/close、累计连接数、bytes/延迟/端口明细或 worker 信息。

conductor 经当前 trusted Proxy registration 的 `stats_socket` 读取 master cache,查询时不扇出 worker.master 未注册,
route 未完成同步、RunID/profile/state 不匹配、worker stream 故障或 replacement 未 ready 为 503。
stats 的 503 窗口不影响 Proxy master 的 route/admission authority 或 Create barrier。完整共享
admission算法、误差证明、worker-local状态机、绝对快照 stream 和故障窗口见
[node-proxy.md](node-proxy_zh.md) §8。

### 4.2 控制面:模板构建 API

实现 e2b **v2 build system** 的端点族(SDK `Template.build` / CLI 走它);构建语义与
资源池见 §12。

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| register | `POST /v3/templates` → 202 | body `{name, tags, profile?, cpuCount, memoryMB, metadata?, envVars?, secure?}`;CPU/memory 只定义不可变 **Build.Resources**,绝不改 Sandbox capacity。`X-Kuasar-Sandbox-Builder.resources` 可声明同值并补 storage;同维度不等即 400。Builder `target` 是 register-only immutable 定义；`X-Kuasar-Sandbox-Resource` 则定义最终 Sandbox template resources，不供 A/B 使用。`profile∈{e2b,bare}`,省略取 `e2b`;响应暴露 requested `target`（省略即 auto） |
| trigger | `POST /v2/templates/{tid}/builds/{bid}` → 202 | body 兼容 `{fromImage, fromTemplate, fromImageRegistry{username,password}, steps[], startCmd, readyCmd}`;只允许一次 `registered→waiting`。兼容的 `cpuCount/memoryMB` 仅可断言等于注册值,放大或缩小均在 credential/COPY/queue 副作用前 400。Trigger-time metadata、Builder/Resource 及其它通用配置 header 全部拒绝;execution 不足时留在固定节点 FIFO waiting |
| status | `GET /templates/{tid}/builds/{bid}/status` | 回基本 SDK 字段、requested `target`、终态 derived `kind`，及规范化 `resources{cpuMilli,memoryBytes,storageBytes}`、`executionClaimed`、`runID`、`systemdEnforcement`、`storageEnforcement` 和当前 `phase{name,sandboxID}`;进行中 SDK status 仍统一为 `building`,内部 phase/claim 不丢失 |
| files | `GET /templates/{tid}/files/{hash}` → 201 | COPY context 上传协商:`tid→build→归属`校验后回 `{present, url}`——present 即对象已在桶(客户端跳过上传),url 为**直传桶的 presigned PUT**(字节不过控制面);未配 `files_storage`→**501**,未知/非属主 tid→**404**。详见 §12 |
| list | `GET /templates` | 本租户 ready 模板;`templateID` 列为持久 id,同时回不可变 `profile`、requested `target` 和 resolved artifact `kind` |

### 4.3 数据面协议(envd 与 native exec)

envd 单端口 **49983**,HTTP/1.1 与 h2c 双栈,Connect-RPC(proto 包无版本):
`process.Process`、`filesystem.Filesystem`(仅元数据);文件内容走 HTTP
`GET/POST /files`(+ 签名 query);另有 `/health`、`/init`、`/metrics`。每操作用户
经 `Authorization: Basic base64("user:")`。**code-interpreter** =
`POST https://49999-<sid>.<domain>/execute`(NDJSON)→ guest FastAPI(:49999) →
Jupyter(:8888)。exec = `POST /process.Process/Start`(头 `X-Access-Token`)。
租户数据面本组件只透传不实现;**构建流水线自带一个最小 `process.Start` 客户端**
(connect+JSON 信封手写,无 protobuf/gRPC 依赖)驱动 steps/startCmd/readyCmd(§12)。

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

### 4.4 templateID 与模板形态(transient / persist,无 templates 表)

```
persist  templateID = <profile>-<kind>-<base64url(canonical-portable-ref)>
                                            profile∈{e2b,bare}; kind∈{img,sbx,snp}
transient templateID = transient-<uuidv7>       构建注册期临时句柄,build 完即弃
```

- **持久 id 自描述**:payload 是 `manifest://<key>` 或
  `file://<digest>.image@digest:<digest>#<location>`；encrypted carrier 使用 `@hmac:`，
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
  (§8.1)同样无需写 builds 表。

### 4.5 SDK / CLI 对接与协议 pin

- 重定向:`E2B_DOMAIN=<domain>` + `E2B_API_KEY`(生产,TLS);dev 走
  `E2B_API_URL`/`E2B_SANDBOX_URL`(http/h2c)。控制面要求 Host 命中 `api.*`。
- api_key 形态:`e2b_` + 72 hex(共 76 字符);e2b SDK 以 `/^e2b_[0-9a-f]+$/` 校验
  格式,服务端另验 MAC(§7)。
- envd 版本 pin:按 e2b-dev/infra 发布 tag 定版(guest-runtime/native-deps `ENVD_TARBALL`,默认
  `2026.22`,对应 envd 0.6.x);SDK:`e2b` js 2.27.x / py 2.25.x 实测兼容。
- 数据面鉴权头 `X-Access-Token`(= `envdAccessToken`):secure 沙箱自 SDK v2.0.0
  默认开,SDK 每次数据面调用携带。
- routesync(Proxy / 路由观察者):版本 7,帧 `[4B LE len][JSON]`,消息
  `register|hello|upsert|delete|bookmark|wake|route_barrier|route_barrier_ack`,路径
  `PUT /internal/plugin/{id}/register`(config-socket plugin 平面,§6;线格式 node-proxy.md §4).

### 4.6 沙箱配置传递链

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

`traffic` 的固定 leaf priority 是:

```text
template / group defaults
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

`metadata["kuasar-sandbox.mmds"]` 绝不包含 `secrets`。Build Register 的 routes 只服务
可能运行 memory Phase C 的 synthetic builder sandbox；显式 Image/顶层 Sandbox E target
在注册期拒绝它，auto target 则在 task-local target 解析后 fail closed。initial values 以 build owner 加密保存。build 终态事务同时
从 `builds.metadata_json` 删除 routes namespace 并删除 value blob,因此两者都不进入最终
template/snapshot/image。Build Trigger 不接受 MMDS 覆盖。

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
`null`/缺失则删除整个 namespace。该 namespace 不从 source template 继承；Build register
只在 resolved `sandbox,memory:true` 时接受它并用于 Phase C capture，且不写入最终 portable
config。`local` 与 `bundle` 使用完全相同的 policy 解析、覆盖和副作用边界。

`kuasar-sandbox.cluster` 不属于上述租户配置命名空间。cluster Build 的 group 使用独立
持久系统字段,不会进入 portable template metadata;普通 sandbox 的 `Profile`、`Group`、`RouteKey` 和可选
`StableID` 则通过 node-link 的结构化系统上下文下发并独立持久化,不进入用户
metadata。node 不在事件中回传 Registry 自有的 group、route key 或认证主体;Registry
通过节点归属记录恢复这些信息。

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

- **渲染**:cold image 仍在 network Attach/runner Assign 前用纯 resolver 生成完整
  `ResourcesConfig`。restore 在 runner task summary 到达后由唯一 launch worker解析
  snapshot capacity并生成同一 canonical `ResourcesConfig`;conductor 不打开 snapshot。
  renderer 直接安装该对象,不再逐字段重解释。YAML 不含
  `control.cgroup_path`;run-sandbox 最后以继承 cgroup FD 注入该 capability。
  dynamic `control.controller` 只取启动时解析的 `resource_listen.SocketIdentity`;
  `watermark_high.ratio` 由 node policy 注入,sensor 保持 omitted。其它 boot/tapfd/已解析 network 与租户子集再经
  config-socket 交 sandbox-ctl。
- **两个注入面**:e2b metadata,与 `X-Kuasar-Sandbox-<Ns>` 请求头(API 边缘归一化进
  metadata；resource/traffic 按 leaf，MMDS 按顶层 key，checkpoint 按字段存在性，
  其余按各自 whole-namespace 规则取 header 优先)。create
  与模板 register 复用同一 Create parser/typed options，支持 resource/network/traffic/launch/
  init/mounts/files/metadata、`envVars` 及实例字段；trigger 明确拒绝非空通用 metadata/header;
  create 的 runtime sandbox 配置存 `sandboxes.metadata_json`,模板构建的普通 runtime 配置存
  `builds.metadata_json`,build-only 配置存 `builds.builder_json`。后两者服务本次 Build 与其
  制品生成，不是 canonical TemplateID Create 的长期 metadata lookup。
  Build register 始终拒绝 `restore`/`autoPauseMemory`；traffic/credentials/MMDS/secure/checkpoint
  等 instance/action-only 输入只有 resolved `sandbox,memory:true` 可用，并单独加密存储/
  task-local 注入，绝不进入 portable E/S。顶层 Sandbox E 和 Image target 明确拒绝这些输入。
  对 auto target 的判断必须等来源 E 和 effective 命令解析后再作该校验。
  集群下 sandbox `create` 的所有权信息使用独立的 node-link 系统上下文(§10)。
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
  Build 临时 VM 与最终模板使用同一 NetworkSpec resolver；未声明 hostname 时临时 VM 用
  `build-<short-build-id>`，成品用 `sandbox.network.hostname`，所以临时 hostname 不进入模板。
  顶层 E 与 C 的 E 均保存最终有效 NetworkSpec，使仅持 canonical artifact、没有原 builds row 的
  Create/restore 仍可恢复逻辑网络。IMG 没有 Sandbox metadata 通道，只使用当前 Create/group/node
  配置，绝不从 retention 内旧 Build row 回填；SBX/SNP 长期权威是自身 E。MigrationToken 也携 metadata。
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

serve 启动时生成并安装两个模板单元 + 两个 slice(`sandbox-runner.slice`、
`sandbox-builder.slice`)到 `units.dir`,内容变更才 `daemon-reload`(D-Bus `Reload`);
`units.install: false` 则交由运维带外管理。ExecStart 里的 `node-ctl` 路径
取自 serve 自身所在目录(自动发现,§3)。

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

**builder 单元**(`%i` = run-id,§12):

```ini
# sandbox-builder@.service (生成内容)
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

两单元的 ExecStart 都先锁 run-id pidfile,再经 config-socket WaitAssignment 等待
业务 id。runner 取得 sid 后立即连接 readiness,再锁 `<RunDir>/<SandboxID>.pid`,以
exact run-id取 bootstrap;artifact task在进程内完成 E/S prepare 和第二阶段后才取最终 LaunchSpec,
随后 `execve` 替换为 `sandbox-ctl run`(继承单元主 PID 与 cgroup,`Type=exec` 故无需
sd_notify);builder 取得 bid 后再锁
`<BuildRunDir>/builder.pid`,取 exact-run bootstrap；artifact source
完成 task-local root prepare 和第二阶段后再取最终 BuildSpec,image 路径在 bootstrap 内直接取 final,
**驻留**驱动 target-aware、最多三阶段的流水线(§12),阶段沙箱(`sandbox-ctl run` + cloud-hypervisor)是其
直接子进程、整个构建计入本单元 cgroup,结果经 config-socket 回传。`KillMode=control-group`
保证 StopUnit/超时连阶段 VM 一并回收。

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

Builder execution 配置 CPU 或 memory 聚合上限时不允许保留长期 idle builder
(`units.builder_pool_size` 必须为 0)。按需单元仍先启动并进入 WaitAssignment，但它已有
对应 Build 的 durable execution claim；orchestrator 在发布 assignment 前设置并回读单元
属性。这样未 claim 的 idle RSS/CPU 不会侵占 `sandbox-builder.slice` 为 active Build 保留的
完整 aggregate ceiling，也不需要引入隐藏的 idle 资源预算。

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
- **存活权威**:`ListUnitsByPatterns("sandbox-runner@*.service")` 一次拿权威存活
  run-id 集,再与库内 `sandboxes.run_id` 对账(§15)。
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

沙箱栈的 app stdio 与 guest 内核 dmesg 全部直写 journald(无临时日志文件),由
`SYSLOG_IDENTIFIER` 标签区分,并附加 `KUASAR_RUN_ID` / `KUASAR_SANDBOX_ID` /
`KUASAR_BUILD_ID` 等字段。用户侧按业务 id 查询,无需知道 run-id 或单元实例名。
写者是 **sandbox-ctl** 的 stdio bridge
(`--stdout-to/--stderr-to/--console journald=<tag>`,语义见 [沙箱生命周期](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md)
§2.2)与 run-builder(自身里程碑 + envd RUN 输出回放,`go-systemd/journal`
纯 Go 直发):

| 标签 | 写者 / 单元 | 内容 | 去向 |
|---|---|---|---|
| `sandbox` | runner 沙箱 / `KUASAR_SANDBOX_ID=<sid>` | 沙箱 app stdio | 仅宿主排障 |
| `build` | builder 阶段沙箱 + run-builder / `KUASAR_BUILD_ID=<bid>` | 阶段 app stdio + 里程碑 + RUN 回放 + flatten 进度 | **SDK 构建日志**(§12);宿主亦可见 |
| `console` | runner / builder 两类沙箱 | guest 内核 dmesg | **仅宿主**(故意不入 SDK 构建日志) |

- **runner**:LaunchSpec(§6)给 sandbox-ctl 带 `--stdout-to journald=sandbox
  --stderr-to journald=sandbox --console journald=console`;exec 替换后在 runner 单元
  cgroup 内直写,并带 `KUASAR_SANDBOX_ID=<sid>`。`journalctl KUASAR_SANDBOX_ID=<sid>
  SYSLOG_IDENTIFIER=sandbox` 按沙箱取流。
- **builder**:run-builder 给每台阶段沙箱带 `journald=build`(app)/`journald=console`
  (内核),并带 `KUASAR_BUILD_ID=<bid>`;单元 `LogRateLimitIntervalSec=0` 关闭自身日志限流，不保证 journal/storage 故障下仍零丢失（§5）。
- **限流策略**:builder 单元关限流(构建少、要全量细节);runner 单元保留默认限流
  (数千沙箱不得刷爆 journal)。
- sandbox-ctl 自身进程日志(其 stderr)随单元落 journal 但**不带标签**——属宿主排障,
  不进任何标签过滤流。

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
  run-builder 据此自建阶段沙箱(§12);仅在该构建单元运行期间可取(serve 持挂
  pending 状态,单元退出即失效)。
- **鉴权**:sandbox/build端点先以业务 id + exact run-id 查不含秘密的 task identity，再校验
  peer pid ⟷ 已锁定 task pidfile；认证通过后才调用可解密 `MANIFEST_KEY`/registry credential
  的 provider。builder 使用 `BuildRunDir/builder.pid`，stale run 或未授权 peer
  均不能触达 secret-bearing provider。
- 设计意图:**非密配置走文件**(`<sid>.yaml`;构建的阶段 yaml 由 run-builder 写进
  对应 phase RunDir）、**根凭据与 pull credential 走 spec env**，加密存储并仅在受信运行期使用；
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
| `ResumeSource` | `{kind:snapshot\|sandbox, ref}` | paused 行持有的可恢复根制品 |
| `ResumeMode` | `auto` / `memory` / `cold` | 调用方对本次 Resume 的选择 |
| `LaunchMode` | `image` / `memory` / `cold` | 解析完成并将在本次 starting 中实际执行的模式 |
| `ResumeTrigger` | `connect` / `wake` / `route` / `exec` / `exec-session` | 低基数日志与指标来源,不参与模式或权限判断 |

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
Stop/Reset fence、Detach、RemoveAll 成功后 exact-clear；RunDir CAS 同时清除该目录拥有的 envd/ci
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
  sandbox-ctl snapshot --sandbox-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root> [--merge-ref=...] [--drop-caches=...]
  -> ResumeSource{kind:snapshot, ref:<dir>/<sid>.snapshot}

CaptureSandbox:
  sandbox-ctl export --sandbox-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root>
  -> ResumeSource{kind:sandbox, ref:<dir>/<sid>.sandbox}
```

Pause 的顺序固定为 resolve request -> accepted operation -> runtime capture ->
`CommitRunningPaused(id, exactRunID, ResumeSource)` -> stop/reset exact runner -> detach exact network ->
RemoveAll RunDir -> publish paused route。commit 原子写 `state=paused` 与 source kind/ref;之后非空
RunID、port 与 RunDir 共同表示 cleanup pending。Stop/Reset 成功后 exact-CAS 清 RunID，Detach 成功后
exact-CAS 清 network；任一步失败保留尚需重试的字段。RunDir 删除失败不允许新的 Resume/Wake/Exec
取得 runtime owner，当前进程的下一次 admission 与 startup Reconcile 都会重试。BaseDir 及其中
checkpoint 始终保留。capture 失败保持 state=running、旧 source、runner 和 network 不变,不创建
成功 alias,也不从 S 降级为 E。

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

artifact launch 的 tenant task 先执行 `internal/taskartifact`。`ArtifactPrepareSpec` 至少携 root
source kind/ref、durable launch mode、manifest config、ref-location parent、relative dir、max refs
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
Manifest Bundle 生成指向同一 Bundle file 并以 E Manifest key 为 selector 的 file ref,不假设远端
Store 已有 E。Result 中的 `PreparedSource`、ref-location URI、carrier/Bundle binding 和 cfg 只留在
task 进程。task 在本地严格解析 artifact network metadata,拒绝 duplicate/unknown/malformed 字段,
只把 typed network topology 与 capacity、required ref count、`resolution_digest` 组成 bounded summary
交给 conductor;原始 metadata 不跨 task 边界。digest 覆盖 root source、launch mode、selected source、
closure、locations、carrier binding、capacity/network summary。相同 runID + digest replay 返回同一结果;
冲突 replay fail closed。
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
sandbox-ctl publish --manifest-config <cfg> [--to-ref-location <name>=<uri>] <artifact>
```

`promoteArtifact` 接受并返回 `ResumeSource`,kind 不变。没有 named location 时发布到 Manifest Store;
配置 `checkpoint.remote.ref_location_parent` 时,publication name 为
`<entity-id>-<YYYYMMDD>`,URI 为
`<parent>/<YYYYMMDD>/<sha256(name)[0:2]>/<sha256(name)[2:4]>/<name>`。local tarstream 可得到 located
`.sandbox`/`.snapshot`:plaintext carrier 使用 `@digest:<digest>`,encrypted carrier 使用
`@hmac:<digest>`。Bundle 得到使用 `@manifest:<root-key>` 选择 root Manifest 的 located `.bundle`。
即使发布到 named location 也始终传 `--manifest-config`,因为 Bundle exact publication 仍须验证并
发布其 Manifest graph。Bundle->Store 验证 recorded admission、physical digest 和 salt domain,
根 Manifest 最后提交。

首层日期目录按实际 publication 日期有序分区,再用 name 的 SHA-256 两级扇出限制单目录条目;
晚发布的旧实体落入当前日期分区。GC retention、可达性、在途发布和安全删除策略不属于 node
生命周期职责。conductor 与 task reader 共用 `internal/reflocation` 的 deterministic 解析,
location name 自足,恢复不依赖额外 side table。

`TemplateID` 为 `<profile>-<kind>-<base64url(canonical-portable-ref)>`:

```text
img: manifest ref,或 located .image/.bundle
sbx: manifest ref,或 located .sandbox/.bundle
snp: manifest ref,或 located .snapshot/.bundle
```

paused E 转模板得到 `KindSbx`;paused S 得到 `KindSnp`。两者均可 publish,不会因 E 无内存而拒绝。
build pipeline 的 Snapshot 输出仍是 `snp`,但 publication 同样调用 `sandbox-ctl publish`,不依赖
单独的 Snapshot publication 命令。

KMT V1 payload 直接携 `resumeSourceKind`、`resumeSourceRef` 和 `autoPauseMemory`,不接受旧
`snapshotRef` payload。Import 原样恢复 E/S kind/ref、deadline、portable env/metadata 和既有
ServiceSecret/Envd/Traffic/Forward token;目标 node 从本地 trusted key table 取得 APISecret +
ManifestKey 并校验 fingerprints、runtime digest 和 profile。token 不携 raw tenant roots、host path、
MMDS secret value、cluster Group/RouteKey 或 generation。

Export 第一阶段 publish 不持有长 lifecycle lock;第二阶段在 per-SID lock 内以 exact original
`ResumeSource` 赢得 finalizer。Resume 先成功 `BeginResume` 时,KMT export 被取消并返回 409;
template publish 可 detached 完成并返回 TemplateID,但不清理 source。finalizer 先获胜时,
`keepSource=true` 把 row 的 source 切为 portable 并删除明确的 local artifact directory;
`keepSource=false` 先 exact teardown ownership,再删除 row/cache/route/local artifact。任何 teardown
或 Store 失败都保留可重试的 durable ownership。located/remote artifact 永不被本机 cleanup 删除。

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
build_upsert{build_event:{build_id, state, template_id?, reason?}}
build_delete{build_event:{build_id}}
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

当前仍存在的 post-registration Registry Build projection 使用独立 bracket：节点先订阅 live Build
变化，再发送 `build_sync_begin`、SQLite 中该节点仍保留的全部 cluster Build row、
`build_sync_end`；集合包含 registered/waiting/building，也包含 retention window 内的 ready/error。
Registry 以 NodeID session fence 接受这些 frame；新 node-link 生效后，旧重叠连接的 Build frame
不会再修改 projection。节点侧每个 Build durable transition 与其 live publication 也由无条件
per-Build fence 排序，不依赖 conductor Extension。
snapshot 期间发生的变化在 end 之后按顺序发送，慢订阅者会断线并重做完整 snapshot。Registry 在
`build_sync_end` 只删除连接建立前 immutable `(NodeID, BuildID)` binding 基线中未出现、且删除时
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

## 12. 模板构建(target-aware、最多三阶段的流水线,构建在沙箱内进行)

构建经 e2b API 提交(端点见 §4.2;无独立构建 CLI),落 `builds` 表,由资源池调度,
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

**单元内(run-builder,§2.4)** 依 BuildSpec(§6)最多跑三个阶段,每阶段一台
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
  **生产 e2b runtime** 的 envd 姿态随部署(node-proxy.md §7)，`/init` 注入 register
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

1. **上传协商(files 端点,§4.2)**:客户端先 `GET /templates/{tid}/files/{hash}`,
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

**对象存储(`builder.files_storage`,§3)**:S3/OBS;serve 只 presign + HEAD,
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

每个 named publication 都在实际写入时用 build ID + 当时 UTC 日期生成 name；跨午夜时 image
与 checkpoint 可以位于不同 date bucket，每个 located ref 自带自己的 name。single-root image
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

**构建日志流(journald 单汇 → status API → SDK on_build_logs)**:构建进度对 SDK
实时可见,零临时文件——全部写 journald 标签 `build`(机制 + 标签词表见 §5.2),
serve 按需查询日志回给 SDK。**写入**(tag build,`KUASAR_BUILD_ID=<bid>`):
run-builder 里程碑(`import: pulling…`、`step N: RUN…`、
`template: ready`、`uploading…`)经 `go-systemd/journal` 直发;RUN/startCmd 输出由
持流的 run-builder 从 envd 流回放进同一汇(envd 自身无 journal);阶段 app stdio 由
sandbox-ctl `--stdout-to/--stderr-to journald=build` 直写;flatten 拉取/导出进度经
`sandbox-ctl exec --stderr-to journald=build`(去掉 `--no-progress`,故 SDK 见
`pull: N/M layers`、`flatten: …` 滚动);失败时 run-builder 补写一行
`build failed: <err>`。guest 内核 dmesg(tag console)与 sandbox-ctl 自身日志不在此
过滤,SDK 见干净构建日志。**读取**:status(§4.2)按 `?logsOffset`(已读条数)分页;
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
`registered→waiting`。scheduler 按 `(waiting_unix,build_id)` 稳定 FIFO,在单条持久
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

## 13. DNS / TLS

生产:`*.<domain>` + `api.<domain>` 通配 DNS + TLS(operator 提供,on-prem/离线
友好).控制面由 conductor `api.listen` 承载,数据面由独立 Proxy `data_listen` 承载;
二者必须是不同 listener.独立模式可让两者直接终止 TLS;若都使用端口 443,必须绑定不同
地址或由外部负载均衡器按域名转发.dev:`E2B_API_URL`/`E2B_SANDBOX_URL` 分别指向
明文 http(h2c) listener.集群下 cluster-ctl router 持对外通配证书,Router→node 的
`APIEndpoint`/`DataEndpoint` 跳当前为明文,node-link 则用独立的
`cluster.node_link.tls` mTLS(§10).

## 14. 契约边界

| 对象 | 方式 | 说明 |
|---|---|---|
| `sandbox-ctl`(runtime) | run-sandbox(单元)最终 `execve`:`run --ready-fd=<fd> --cgroup-path=fd=<vmm-fd> --config <sid>.yaml --manifest-config … --run-root … --base-root … [--from E\|--restore S] [--connect]`;run-builder 以直接子进程启动 A/B/C，source E 的 C 使用 `run --from E --replace-boot`，经 `exec --env/--stdin-from/--stdout-to` 做平台接力，memory 收尾 `snapshot`/`publish`;顶层 E 则直接调用 sandboxer package assembly/publication API，不启动 sandbox-ctl;serve 的 Capture 调用 `snapshot` 或 `export` | managed launch/build prepare 不启动 `sandbox-ctl info`;readiness wire 固定为 `control_ready`→`ready`→EOF;runner 的 VMM cgroup FD 仅由 node-ctl 本地注入;非密配置文件 + 密钥 env;资源准入在其内部;e2b 语义命令不走它(走 envd,§12) |
| 资源控制器(node-resource.md) | serve 内置(`resource_listen`,调参内联);dynamic sandbox 自动使用同一 canonical `SocketIdentity` 拨号(`pkg/resource` 协议) | `resource_listen` 是唯一 endpoint 来源;每个 runner 的 `vmm/` 是沙箱资源 cgroup,FD 由 run-sandbox 注入;controller disabled = static cgroup |
| registry(cluster-ctl) | node-link:serve 拨 registry、反向注册为路由权威,上报 register/heartbeat/Sandbox route 与 Build full-sync/upsert/delete，受理 create/connect/delete/key_put/key_drop/build_register 命令(§10、cluster.md) | mTLS;cluster kill 走 node-link delete 命令;Build projection 无 Registry-side TTL;空 `cluster.node_link.endpoint` = 独立模式不接入 |
| `connector-ctl vswitch`(vswitch) | 不配 `tapfd_socket` 时经 CLI:`attach <switch> --inner-ip [--transit-*]` / `detach --port`;配 `tapfd_socket` 时经常驻 `TAPFD/1 PREPARE` / `OPEN` / `RELEASE`;sandbox 配置仍渲染为 `network.tapfd.socket/request` | 交换机预先起好(`connector-ctl vswitch start/serve`,内核态数据面);port 对外、slot 内部;一个构建复用一个槽 |
| `flatten-ctl`(builder) | **guest 内**(guest runtime 自带,经 sandbox-ctl exec 驱动):`export --output -`(import 拉取 / steps 导出)、`mountpoint`;宿主侧只用 `info --json --manifest-config` 验证 registry referer hit | 三种 image carrier 的 config 读取和最终发布均走 sandboxer typed opener；租户 `FLATTEN_*` 仅经 exec env 入 guest;tarstream 镜像工件经 exec stdio 接力 |
| sandboxer `pkg/artifact` + `pkg/sandbox` | Builder 的 image/Sandbox logical source 直接 Manifest ingest 或 single-root Bundle publication；本地 IMG→顶层 E 无中间文件 | typed role、customer key、write admission、root-last、sparse semantics、named-location atomic/reuse validation 均由 sandboxer 统一实现；Build finale 不调用 `manifest-ctl store` |
| `manifest-ctl`(accelerator) | 独立的 Manifest store CLI；Builder final publication 不经该 CLI | `MANIFEST_KEY` 经调用进程环境 |
| `mkfs.erofs`(deps) | guest-runtime `make sandbox-runtime` 与 guest 内 `flatten-ctl` 后端 | 确定性打包 runtime;构建沙箱内导出 EROFS 镜像(§11、§12) |
| guest envd | UDS(sandbox-ctl `--connect` 映射);构建流水线另以最小 connect+JSON 客户端调 `process.Start`(steps/startCmd/readyCmd,§12) | 原版不改;协议 pin 见 §4.3/§4.5 |
| systemd | D-Bus:StartUnit/StopUnit/ResetFailed/ListUnitsByPatterns/Reload | 进程管理 + 单元自装(§5) |
| `node-ctl proxy` | UDS routesync(双向 h2c 帧化 JSON)+ 独立 Data listener | 同节点,运维带外起;Proxy master 注册一次,worker 共享继承 Data listener fd + shm 路由视图;master 冻结的 EffectiveConfig 含 `paths.run_root`,worker 不重读 `proxy.yaml`(node-proxy.md §2.1/§3/§4) |

公共导出面仅为 `config`、`app/conductor` 与 `app/proxy` 的启动期契约；
`CGO_ENABLED=0`;内部 core 继续保持 `internal/*` 依赖边界。

## 15. 可靠性

### 15.1 状态存储(sqlite)

单文件 sqlite(`paths.db_path`,WAL,文件 0600),纯 Go 驱动。核心表:

```
sandboxes      id(node-local SandboxID,1..57 bytes DNS-label subset) PK,
               profile, cluster_group, cluster_route_key, stable_id,
               template_id, state(starting|running|paused|deleting|dead), deadline_unix, dead_unix,
               run_dir, base_dir, envd_uds, ci_uds, floatingip, vswitch_port,
               inner_ip, port_mac, api_secret_hash, api_secret_enc,
               manifest_key_hash, manifest_key_enc,
               resume_source_kind, resume_source_ref, auto_pause_memory, launch_mode,
               service_secret_enc, envd_access_token_enc, traffic_access_token_enc,
               forward_access_token_enc, metadata_json, env_json,
               created_unix
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
               waiting_unix, execution_claimed, execution_claimed_unix,
               enforcement_status, phase, phase_sandbox_id,
               runtime_vswitch_port, runtime_floating_ip, runtime_port_mac,
               runtime_envd_access_token_enc, runtime_prepare_json,
               execution_result_json, created_unix, finished_unix
sandbox_mmds_route_secret_values
               sandbox_id PK/FK sandboxes(id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
build_mmds_route_secret_values
               build_id PK/FK builds(build_id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
manifest_keys  api_secret_hash PK, api_secret_enc, manifest_key_hash,
               manifest_key_enc, label, created_unix, expires_unix, registry_auth_enc
```

`builds` 是 registration/execution 两级准入与执行状态的持久真相，也是 retention window 内的
status/index/alias；它不是长期模板 catalog（§4.4）。
`resume_source_kind/ref` 是 paused E/S 的 typed root;`auto_pause_memory` 只决定 TTL CaptureKind;
`launch_mode` 是 starting 中已接受的实际 image/cold/memory 模式。生命周期
schema 直接替换旧单字符串 lifecycle 模型,不保留双读/双写或迁移 shim;已有开发数据库须重建。

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
`*_enc` 根凭据及 Sandbox service credential 均
AES-256-GCM、两项 `*_hash` 均为
完整 SHA-256。`substr(api_secret_hash,1,24)` 仅建候选预筛索引(§7)。
两张 MMDS value 表每 owner 最多一行,只保存 secretbox ciphertext;AAD 与事务/CAS/cleanup
语义见 §7。只有已经带上述 #300 marker 的当前 Build schema 才继续执行仓库既有的独立 additive
维护：`CREATE TABLE IF NOT EXISTS` 增加两张 value 表，并以幂等 `ALTER TABLE ADD COLUMN` 补入
`runtime_prepare_json`、`dead_unix`、`finished_unix`；迁移时已存在的 terminal row 从迁移时刻开始
取得完整保留窗口，无 plaintext 回填或兼容双写。

### 15.2 重启对账

conductor 在开放 API、config-socket routesync 和 node-link 前先以
`ListUnitsByPatterns("sandbox-runner@*.service")` 对账:

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
  exact CAS 和 RunDir RemoveAll；source 与 BaseDir/checkpoint 不变。Resume/Wake/Exec 使用相同
  admission gate，旧 ownership 未完成时不得进入 `starting`。`run_root` 为 tmpfs ⇒ 整机重启后失联 running
  判 dead;paused E/S 与 sbx/snp template 保留,可被 Connect/Wake 重新拉起(本机制品位于
  持久 `BaseDir/checkpoint`)。

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

同一个 conductor reaper 每 5 秒执行一次终态保留清理，不增加 systemd timer/unit。Sandbox
进入 `dead` 与 Build 进入 `ready/error` 的 store transition 分别原子写 `dead_unix` 与
`finished_unix`；每轮每类最多处理 128 条。只有到达 `sandbox.dead_ttl` /
`builder.terminal_ttl` 且完全 owner-free 的行才 exact-delete：Sandbox 不能仍有 unit、network、
RunDir/BaseDir、UDS、ResumeSource 或 launch owner；Build 不能仍有 execution claim、unit/cgroup、
phase、network、runtime prepare/result owner。候选扫描后的并发变化会使 delete CAS 失败并保留行，
进程重启后按数据库时间继续。实现不自动 `VACUUM`，也不触碰任何远端 artifact。

以上 node-local finalizer 是 #132/#133 的 cleanup 合同实现边界。#196 的 Export 仍保持
publish/finalize 两阶段竞争与 source cleanup 顺序；#205 的 Build resources、两级准入和 cgroup
权威不变；当前 main 仍有 post-registration Build projection，故 routesync v7 沿用 v6 引入的 Build full sync +
live delete 收敛它，但不改变 #46 的 immutable registered-node binding；若 #46 删除该 projection，
节点 TTL 本身不要求重建 lifecycle event。这里不执行任何远端 artifact GC，也不增加逐步骤
cleanup stage 或第二份路径权威。

因此 RouteSource.Range 与后续全量同步不会看到遗留 starting 被误发布为 running;初始 starting
已经持久化 network 但尚未分配 runner 的 crash 也能确定性释放端口并收敛到 dead/paused。

### 15.3 故障域

| 故障 | 影响 | 自愈 |
|---|---|---|
| conductor 崩溃/重启 | 控制面中断;沙箱(microVM/单元)与 running 数据流不受影响 | systemd 重启 → 重启对账收养;Proxy master 仍可用共享路由视图服务 running 流量(Wake 无人应答,paused 唤醒挂起至超时);集群下 node-link 重连重报；MMDS confidential authority 仍按 routesync loss 清空 |
| Proxy worker 崩溃 | 该 worker 上的连接断;其共享 admission 绝对计数 stale-high,其余 worker 可继续接新连接或保守拒绝 | stats stream fault 先终止该 worker;仅 `cmd.Wait` 确认旧进程退出后 master 才清空该 index,再以同一 index、新 epoch 启动 replacement;不改 route/Sandbox state,不影响 Create |
| Proxy master 崩溃 | 数据面中断,plugin 租约断开,Create fail closed | systemd 重启 master → 重新注册,重建共享表,启动 worker |
| runner 单元/CH 崩溃 | 该沙箱死(`Restart=no`,有状态不重试) | 对账标 dead;客户重新 create(或从 paused 快照 resume) |
| routesync 断流 | Proxy 数据面视图停更;MMDS route/value/service 立即不可用 | Proxy master 清空 confidential heap 并指数退避重连重注册,完整同步 bookmark 后才重开 MMDS；fixed routes 保持原 retention/convergence 行为（node-proxy.md §4） |
| node-link 断流(集群) | registry 暂失本节点视图 | 节点指数退避重连重注册重报沙箱集(§10、cluster.md);本节点沙箱不受影响 |
| sqlite 损坏 | 控制面不可用 | 文件级备份/重建;沙箱单元仍可被 ListUnits 发现并由运维处置 |

## 16. 测试

单元测试:`make test`(MMDS strict parser/top-level merge/minimal persistence、route value
encrypted owner blob/AAD/CAS/cleanup、admin UDS、service relay、routesync confidential projection、
Proxy master/worker resync/rotation;handler 路由,apikey/secretbox/regcreds,routesync(注册/bookmark
往返)/proxyshm(共享路由表、park/wake、世代清扫)/proxyadmission(多 worker 有界误差、generation
复用、crash-after-Wait 清理)、plugin 注册表(同 id 顶替)、proxy CONNECT 隧道 +
Exec KAT/64 KiB API/CmdExecSession/H1/H2 request gate 与
buffered half-close tunnel,mmds(确定性密钥),launch ownership,沙箱配置注入(命名空间解析/容量折叠/网络合并),
migrate,node-link(注册/事件/命令往返)等).

Native exec 的真实 microVM 特性用例分别覆盖 standalone 和 cluster 路径的
ExecAccessToken 签发,`service=exec` CONNECT 以及 guest 命令执行.该结论只对上述
native exec 路径负责,不表示同一聚合脚本的后续 pause/resume 等其它阶段已一并验收.

编排特性的 E2E 与实现一起维护在 `orchestrator/test/e2e/`。轻量 cluster stub 与需要
vmlinux、cloud-hypervisor、mkfs.erofs、sandbox-runtime.bundle 等多仓制品的真实 microVM
用例使用同一个 `run_all.sh`。直接调用脚本时通过 `BIN` 指向项目主仓组装的二进制目录；
`make test-e2e` 传入 `E2E_BIN`，默认 sibling 主仓 `bin/<architecture>`，没有 build prerequisite，需先组装；
本地 stub 则使用 `make build` 后 `make test-e2e-cluster-stub`。
组件 PR 的 BMS 则把候选仓与其余仓源码组成统一环境后执行该入口。缺少重型前置时单脚本可
跳过,完整门禁设置 `REQUIRE_*=1` 后硬失败。

| 脚本 | 覆盖 |
|---|---|
| `e2e_orchestrator.sh` | 单元自动安装 + 控制面(`/health`、401 路径)+ 构建 API 生命周期(register/trigger/status、跨 key 归属 404)+(有 KVM 时)bare create/list/kill |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器(纯用户态,无 root/systemd/KVM):pidfile 锁/双起拒绝、exact-run bootstrap、cold单阶段/restore两阶段、execve、`TASK_*`和重复MANIFEST_KEY剥除;`config` CLI 往返 |
| `e2e_run_builder.sh` | target-aware 三阶段构建流水线(KVM + vswitch + store-ctl + zot,guest 经 mgmt VIP 拉取):真实 IMG/SBX/SNP、fromImage/fromTemplate(img/sbx/snp)、image/checkpoint publication matrix、Manifest 与 named-location Bundle、顶层 E 无完整 staging/零 Phase C、portable Phase-C image ref、local/Bundle checkpoint mode、S→E cold selection、memory C 固定等待、source-ref closure 与 Build-row TTL 后 canonical Create；另覆盖 COPY/bare、资源隔离、终态 cleanup、日志/DB/artifact secrecy |
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

## 17. See Also

- [node-proxy.md](node-proxy_zh.md) —— 独立数据面转发层:路由判定 / routesync /
  数据面鉴权 / MMDS / CONNECT 隧道(本文 §9 的唯一数据入口,集群下 router 转发进入)
- [node-resource.md](node-resource_zh.md) —— 节点资源控制协议、sandbox resource policy
  与控制器内部组织(serve 经唯一的 `resource_listen` endpoint 内置)
- [cluster.md](cluster_zh.md) —— 集群控制面:node-link 线格式(§6,本文 §10 的对端),注册表,
  Reserve 状态机;[cluster-router_zh.md](cluster-router_zh.md) 数据面入口,[cluster-placer_zh.md](cluster-placer_zh.md)
  放置与密钥分发
- [沙箱生命周期](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox_zh.md) —— sandbox-ctl:SANDBOX_CONFIG 模式、
  run/snapshot/restore/connect 原语、cgroup 模型
- [虚拟交换机](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch_zh.md) —— attach/detach/open-port、floatingip 与
  mgmt-service(MMDS VIP 转换)语义
- [镜像展平](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten_zh.md) —— flatten-ctl export 与 OCI Referrers 幂等流、
  `FLATTEN_REGISTRY_*`
- [Manifest](https://github.com/kuasar-sandbox/accelerator/blob/main/docs/manifest_zh.md) —— manifest 内容键、收敛加密与去重域
  (§7 的存储侧)
- [部署](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment_zh.md) —— 节点部署拓扑中本组件的位置与单元安装
- [演示](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/test/demo/DEMO_zh.md) —— e2b CLI/SDK 全流程演示
