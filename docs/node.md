# node — 节点 e2b 兼容沙箱主机与集群接入

`node-ctl` 是计算节点上的单实例常驻 daemon,对外提供一套 **e2b 兼容 API**,把节点上的
microVM 沙箱以 e2b 协议暴露给客户端——未改造的 e2b SDK(python / js `e2b`、
`@e2b/code-interpreter`)与 e2b CLI 可直接指向本机运行。`node-ctl conductor serve` 一身兼数职:
**api**(控制面 REST:沙箱生命周期、模板构建、鉴权)、**主机**(经 systemd 模板单元
拉起/停止 `sandbox-ctl` 与 `flatten-ctl`,调 `connector-ctl vswitch` 编排网络)、**proxy**(把
客户端到沙箱的数据面流量反代到 guest)、可选的**资源控制器**(节点级资源仲裁,经
`resource_listen` 内置,node-resource.md)、以及 **node-link 客户端**(接入集群,把本节点
交由 cluster-ctl 编排,§10)。

node-ctl 既可**独立运行**(直供 e2b SDK / CLI,单机即可用),也可经 node-link **接入集群**
由 registry / router / placer 编排(cluster.md);两态共用同一套 e2b 控制面与生命周期原语——
**控制面 create / pause / kill / 模板构建始终在本节点**,集群只下发高层命令复用这些原语(§10)。

沙箱本体由 `sandbox-ctl` 运行(microVM,cloud-hypervisor);e2b profile 的 guest 内
跑原版 envd(由单一 `sandbox-runtime.bundle` 内置,§11),serve 经 UDS
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
   (attach/detach)、`flatten-ctl`(export)全部经子进程 CLI,不 import 兄弟仓内部包;
   经 systemd D-Bus 管单元;经 UDS 反代 envd。叶子组件,纯 Go,`CGO_ENABLED=0`。
2. **资源仲裁内置且可分离**:沙箱准入/配额由 `sandbox-ctl`(`pkg/resource` 的 client)
   与节点级**资源控制器**对话完成;控制器由 `node-ctl conductor serve` 经 `resource_listen` 内置
   (node-resource.md),调参随 serve 配置内联。它仍是与 serve 的 api / 主机 / proxy
   逻辑解耦的可分离子系统。构建任务的资源池由 serve 自管(§12)。
3. **进程管理交给 systemd**:runner/builder 模板单元以 run-id 为实例名,可预启动等待
   config-socket 下发 assignment;runner 单元下以 `ctl/vmm` 隔离监督进程与沙箱资源,
   builder 仍按完整单元核算;`StopUnit` 即完整回收,serve 不自己当进程监督者。
4. **密钥不落明文盘**:租户 APISecret+ManifestKey 凭据对在库内 AES-256-GCM 加密;
   运行期根凭据只存在于必要的内存、受保护路由投影和启动器 LaunchSpec 的 env 帧,
   ManifestKey 不进入 guest(§6、§7)。集群下经 node-link 下行的凭据对同样仅加密落盘(§10)。
5. **本节点路由权威,数据面可外置,集群可接入**:serve 是**本节点**路由与生命周期的
   权威;数据面 proxy 可内置(单二进制)或外置为独立 worker(§9)。接入集群时,机群级
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
经 e2b API 构建的模板恒为 **e2b** profile。profile 编码在 templateID 前缀里(§4.4),
运行期据此选 runtime erofs 与数据通路。

### 1.4 边界与依赖

- 北向客户:e2b SDK / CLI 直连;集群下经 cluster-ctl router 转发(数据面)+ node-link
  下发命令(控制),平台管理面亦可经 e2b API 对接。
- 既可**独立运行**也可**接入集群**:控制面(create/pause/kill/模板构建)始终在本节点;
  接入集群仅多一条 node-link(§10),不改 e2b 契约。
- 不实现 envd 协议:数据面只透传到 guest 内原版 envd(§4.3)。
- 不实现 e2b `/sandboxes/{id}/metrics` 时间序列或 envd metrics 采集;平台另提供只读的
  `/sandboxes/{id}/stats/resource` 与 `/sandboxes/{id}/stats/traffic` 即时快照(§4.1.1)。
- 构建不支持 server 端执行 Dockerfile steps:服务端只对一个已存在的镜像引用做拉取 +
  展平(§12)。
- 节点本地:路由、存储、单元管理都是节点本地的;跨机快照/模板使用 canonical
  portable ref(数据位于 manifest store 或统一挂载的 named location,§8.1),编排走
  cluster-ctl 的 node-link(§10)。
- 依赖:stdlib + `modernc.org/sqlite`(纯 Go)+ `golang.org/x/net/http2`(h2c,
  config-socket 与 node-link 共用)+ `golang.org/x/sys`(pidfile 锁 / SO_PEERCRED /
  mmap)+ `coreos/go-systemd`(D-Bus)+ `google/uuid`(v7)+ `gopkg.in/yaml.v3`。
  无 gRPC/protobuf。

### 1.5 架构与数据通路

```
          e2b SDK / CLI                                    cluster-ctl (cluster.md)
               │ https://api.<domain>          │           registry · router · placer
               ▼                                ▼                    ▲
      ┌─ node-ctl conductor serve ──────────────────────┐   ┌─ data plane ──┐ │ node-link (§10):
      │ api: e2b control plane (REST)         │   │ proxy         │ │  up: register/HB/
      │   sandboxes create/connect/exec/...   │   │ (internal /   │ │      sandbox events
      │   templates register/trigger/status   │   │  external     │ │  down: create/connect/
      │ host:                                 │   │  workers,     │ │       exec_session/delete/
      │   vswitch PREPARE/attach → floatingip           │   │  route-synced)│ │       key_put/key_drop
      │   assign sandbox-runner@<run-id>      │   │ 49983/49999   │ └─ node-link client ───┐
      │   envd /init → sqlite + TTL           │   │  → UDS (envd) │                        │
      │ build: builds table → pool →          │   │ forward → TCP │◄── cluster-ctl router  │
      │   assign sandbox-builder@<run-id>     │   │ exec → ctl UDS│    forwards data plane  │
      │ [resource_listen]: resource ctrl      │   └──────┬────────┘                        │
      └──────┬───────────────┬────────────────┘         │                                 │
             │ D-Bus         │ UDS config-socket         │                                 │
             ▼               ▼ (LaunchSpec / BuildSpec,  │                                 │
      systemd units            secrets in env)           │                                 │
      + slices         run-sandbox → execve sandbox-ctl  │                                 │
                       run-builder → 3-phase build (§12) │                                 │
                             │                            ▼                                 │
                       cloud-hypervisor microVM ◄── tap ── vswitch (eBPF) ◄─────────────────┘
                         guest: sandbox-init + envd
```

create 同步受理流程只做请求校验/纯解析、身份和 token 生成、进程内 launch ownership claim,
然后 insert `starting, run_id=""`(网络字段为空)、cache/publish starting 并调度后台 launch。
此时即返回 HTTP 201;它表示资源已被持久接受,不表示 runner 已分配、runtime 已 ready 或
e2b `/init` 已完成。后台流程再建 `<run_root>/<sid>/`(tmpfs)+
`<base_root>/<sid>/`(disk)→检查 snapshot→配 `tapfd_socket` 时经 `TAPFD/1 PREPARE`、
否则经 `connector-ctl vswitch attach` 拿 `{port, floatingip, mac}` 并先以 CAS 持久化网络
ownership→写非密配置 `<sid>.yaml`、绑定 `ready.sock`、发布 enriched starting→从 runner
pool 分配 run-id(无 idle 时按需 `StartUnit(sandbox-runner@<run-id>)`)。pool commit callback
先以 `starting AND run_id=''` CAS 绑定 run-id,成功后才把 sid 交给 runner。单元内
`run-sandbox` 经 config-socket 的 WaitAssignment 取得 sid,再取 LaunchSpec(密钥经 env)后
`execve` 成 `sandbox-ctl run`→起 microVM→严格完成 runtime readiness wire→(e2b)直接
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
选择器,普通 HTTP 不解析它.Native exec 使用显式申请的 ExecAccessToken,在任何
恢复副作用之前完成 KAT 校验,最终进入 `<run_root>/<NodeSandboxID>/ctl.sock`.
TrafficAccessToken 仅供外部网关及 e2b
数据面组件使用,node 平台层不消费。对 paused
沙箱的请求触发自动 resume(与其它入口共用 launch owner,§8)。部署形态
(internal/external/off)见 §9.1,
转发层设计见 [node-proxy.md](node-proxy.md)。集群下,数据面由 cluster-ctl router 经
把公开稳定 SandboxID 转换为当前 NodeSandboxID,再注入 `E2b-Sandbox-Id` +
`X-Access-Token` 转发进本节点 proxy(cluster-router.md)。

## 2. 命令行接口

### 2.1 子命令总览

**`node-ctl`**:

| 子命令 | 用途 |
|---|---|
| `serve` | 启动 daemon:控制面 + 数据面 + 本机控制 socket + reaper + 构建池;配 `cluster` 则起 node-link 客户端(§10),配 `resource_listen` 则内置资源控制器(node-resource.md) |
| `proxy` | 外置数据面 worker(`proxy.mode=external`;见 §2.3 与 node-proxy.md) |
| `run-sandbox` / `run-builder` | systemd 单元内启动器,非给人用(§2.4、§6) |
| `resource` | `status`/`list`/`drain`/`grant`/`reclaim`:资源控制器只读巡检与运维(node-resource.md §2) |
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
# dev: E2B_API_URL=http://host:3000  E2B_SANDBOX_URL=http://host:3000
```

集群模式下密钥由 registry 经 node-link 租约下发(§10、cluster.md),无须手动
`manifest-key add`。

### 2.2 `node-ctl conductor serve`

```
node-ctl conductor serve [--config /etc/node-ctl/conductor.yaml]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--config` | `/etc/node-ctl/conductor.yaml` | 配置文件(§3);`proxy.mode` 等一律以文件为准 |

启动序列:打开 sqlite(文件 chmod 0600)→ 生成并安装 systemd 模板单元(§5)→
重启对账(§15)→ 起 reaper(TTL,5s 周期)→(配 `resource_listen` 则起内置资源控制器,
node-resource.md)→ 起本机控制 socket并确认监听成功(§6)→ 起 runner/builder 预启动池与
构建准入循环(§12)→ 按 `proxy.mode` 装配
数据面(§9)→(配 `cluster.node_link.endpoint` 则拨 registry 起 node-link 客户端,§10)→
监听 `api.listen`。`api.<domain>`(及任何 `api.` 前缀 Host)路由到控制面,其余 Host
进数据面。TLS 证书缺省时以明文 h2c 服务(dev:SDK 走 `E2B_API_URL`/`E2B_SANDBOX_URL`)。

随仓 systemd 单元模板:`deploy/node-ctl.service`。

### 2.3 `node-ctl proxy`

外置数据面 master(`proxy.mode=external`);运维带外起、与 serve 同节点。配置文件驱动
(`proxy.yaml`,自带 schema,见 [node-proxy.md](node-proxy.md) §2),worker 由 master
内部 reexec 和监督:

```
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

进程端点与 bootstrap 策略(`config_socket`/`data_listen`/`proxy_netns`/`proxy_socket`/
`stats_socket`/`shm_path`/`workers`/`tls`/`auth`/`park_timeout`)在 `proxy.yaml`;
MMDS listen 与 service registry 只配置在 conductor。master 在 plugin 平面注册一次,
由握手取得 MMDS policy,维护共享路由视图并把 listener fd 传给 worker。部署模式与拓扑见
node-proxy.md §2、§5——转发层自成一文,本仓控制面只在 §9 讲如何按 `proxy.mode` 装配它。

### 2.4 `node-ctl run-sandbox` / `run-builder`

systemd 单元的 ExecStart,非给人用。共用的进入骨架:`--run-id` 是 systemd 实例名,
`--pidfile` 指向 `<run_root>/runs/<run-id>.pid`,以 `fcntl(F_SETLK)` 排他锁防重入并写本
PID → 拨 `--config-socket` WaitAssignment 取得业务 id(§6)。之后两者分道:

- **run-sandbox**:取得 sid 后立即连接固定的
  `<run_root>/<sid>/ready.sock`(此时 readiness fd 保持 `FD_CLOEXEC`)→ 锁
  `<run_root>/<sid>/<sid>.pid`→ 取 LaunchSpec → `chdir(workdir)`、剥除 `TASK_*`
  引导变量、合入 `spec.env`(密钥)→ 仅在最后一次 `execve` 前清 readiness fd 的
  `FD_CLOEXEC`,向 argv 追加其实际编号 `--ready-fd=<fd>`并替换为
  `sandbox-ctl run`。目标继承本 PID、单元 cgroup、pidfile 锁 fd 和 readiness fd;
  任一 pre-exec 失败都会关闭 readiness 连接,serve 立即读到 EOF。
- **run-builder**:取得 bid 后锁 `<run_root>/<bid>/<bid>.pid`,取 BuildSpec →
  **驻留**驱动三阶段构建流水线(§12):各阶段沙箱(`sandbox-ctl run`)是它的直接子进程,
  整个构建计入本单元 cgroup;结束把结果
  `{image_ref|snapshot_ref, start_cmd, ready_cmd, error}` 经 config-socket 回传。

```
node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
node-ctl run-builder --pidfile=<f> --config-socket=<uds> --run-id=<rid>
```

flags 缺省回落 `TASK_PIDFILE` / `TASK_CONFIG_SOCKET` / `TASK_RUN_ID`
env(systemd `%i` 接线用)。

### 2.5 `node-ctl config`

配置诊断 + 生成工具,**按角色**(`serve` / `proxy`,各自独立文件与 schema):

```
node-ctl config <serve|proxy> --template            # 输出该角色带注释骨架
node-ctl config <serve|proxy> --config <file>       # 加载(补默认 + 校验)后重排输出
node-ctl config <serve|proxy> --config <file> --resolve   # 再展开 auto/派生(实际生效形态)
                              -o <file>             # 写文件(默认 stdout)
```

角色作首参以消歧 schema:`serve` 对应 `conductor.yaml`(§3),`proxy` 对应 `proxy.yaml`
(node-proxy.md §2)。`--resolve` 对 `serve` 额外展开 `resource_listen` 的 `auto` 内存/CPU
(并深校验水位),其余角色与 `--config` 等价。骨架与 `deploy/{serve,proxy}.example.yaml` 对应。

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

- `export-sandbox <sid>`:打印单行 `kmt1.` opaque 迁移 token(默认 move,回收源行;
  `--keep-source` = copy)。
- `export-sandbox <sid> --to-template`:晋升为远程快照并打印持久 templateID(扇出用)。
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
(`node-ctl config conductor --template` 输出同形骨架),权威结构是 `internal/config/config.go`。
配置按关注点分组:`api`、`proxy`、`paths`、`units`、`sandbox`(实例级默认,子组
`resources`/`network`/`boot`)、`builder`、`checkpoint`、`mmds`、`cluster`(node-link,§10)、
`resource_listen`(内置资源控制器,调参全部内联,node-resource.md),外加顶层单值
`encryption_key`、`manifest_config`。**必填仅 `api.domain` 与 `encryption_key`**(后者可用
`NODE_CONFIG_ENCRYPTION_KEY` env 覆盖)。外部二进制(sandbox-ctl/connector-ctl vswitch/flatten-ctl)**不配置**:按"与
node-ctl 同目录 → PATH"自动发现。

| 字段 | 默认 | 说明 |
|---|---|---|
| `api.domain` | (必填) | 服务域,如 `sandboxes.example.com`;控制面 = `api.<domain>` |
| `api.listen` | `:443` | 北向监听;dev 用 `:3000` 走明文 h2c |
| `api.tls.cert/key` | 空 | 通配证书(`*.<domain>` 与 `api.<domain>`,§13);空 = 明文 |
| `proxy.mode` | `internal` | 数据面承载:`internal`/`external`/`off`(装配见 §9.1,部署模式见 node-proxy.md §3) |
| `proxy.data_listen` | 空 | internal 模式专用数据面监听;空 = 与 `api.listen` 共口。external 模式数据口在 worker 的 `proxy.yaml`(serve 不绑) |
| `proxy.proxy_netns` | 空 | internal 模式转发平面 netns:proxy 到 `floatingip:port` 的 TCP dial 与 `mmds.listen` 绑定都在该 netns;external 模式在 `proxy.yaml` 配同名字段,external native exec 另要求 `proxy.yaml` 必填 `paths.run_root` |
| `proxy.park_timeout` | `30s` | 数据面请求挂起预算:等路由同步 / paused 沙箱 resume 的上限(node-proxy.md §4) |
| `proxy.auth` | `enforce` | 数据面鉴权:`off`/`log`/`enforce`,校验 `X-Access-Token`(node-proxy.md §6) |
| `proxy.metrics_listen` | 空(关) | conductor 进程 Prometheus 文本端点:internal 模式含 `data_requests_total`,external 模式主要含 `proxy_forwarder_total`;external worker 数据面指标在 proxy.yaml `metrics_listen` |
| `encryption_key` | (必填) | APISecret/ManifestKey 凭据对落盘加密的 AES-256 密钥:`:` 分隔多个 64-hex,首个为活动密钥,其余备用解旧记录(轮换);`NODE_CONFIG_ENCRYPTION_KEY` env 优先 |
| `manifest_config` | `/opt/sandbox/manifest.yaml` | 共享远程 manifest store 配置(`manifest.key` 留空,租户 key 经 env 按任务下发) |
| `paths.run_root` | `/run/sandbox` | tmpfs 运行态:`<sid>/` 运行目录、UDS、pidfile |
| `paths.base_root` | `/var/lib/sandbox` | 持久态根 |
| `paths.db_path` | `<base_root>/node-ctl.db` | sqlite 路径(§15) |
| `paths.config_socket` | `/run/sandbox/node-ctl.socket` | 本机控制 socket(run assignment/result + task/admin/plugin/api,§6);manifest-key/export/import CLI 与 external proxy / 平台 agent 的连接点 |
| `paths.admin_pidfile` | 空 | admin 平面的多行 PID 白名单(`#` 注释);未配则仅靠 socket 0600 |
| `paths.plugin_pidfile` | 空 | plugin 平面(proxy/agent 注册)的多行 PID 白名单;未配则仅靠 socket 0600 |
| `units.dir` | `/etc/systemd/system` | 模板单元安装目录 |
| `units.runner` / `units.builder` | `sandbox-runner@.service` / `sandbox-builder@.service` | 模板单元名 |
| `units.runner_pool_size` / `units.builder_pool_size` | `0` / `0` | 空闲预启动 run-id 单元数;0 = 不保留 idle,有任务时仍按需经 WaitAssignment 流程启动 |
| `units.pool_wait_timeout` | `5s` | 从调用 `StartUnit` 到单元进入 WaitAssignment 的正数时限;超时清理该 run-id 并补池 |
| `units.install` | `true` | `false` = 单元由运维带外管理,serve 不生成安装 |
| `sandbox.timeout_sec` | `300` | 沙箱默认 TTL(秒) |
| `sandbox.resources.vcpu` / `.memory` | `2` / `2GiB` | 每沙箱容量;同时回显在 e2b list/get 的 `cpuCount`/`memoryMB`。**restore 类启动(snp 模板 create / resume / 迁移导入)按快照内 snapshot.cfg 的 capacity 覆盖**——快照自描述,可与本机默认不同(如构建预算下产出的模板) |
| `sandbox.resources.control_socket` | 空 | 资源控制器 UDS,**opt-in**;空 = 静态 cgroup(单元自身,§5.1);非空指向内置控制器(`resource_listen`,通常即其 `socket`,node-resource.md) |
| `sandbox.network.switch` | `sw0` | vswitch 交换机名 |
| `sandbox.network.hostname` | `sandbox` | guest 主机名:sethostname + `/etc/hosts` 条目(§11) |
| `sandbox.network.dns` | `[169.254.169.253]` | 注入 guest `/etc/resolv.conf` 的 nameserver;该地址需部署侧路由到真实 DNS |
| `sandbox.network.e2b` / `.bare` | `169.254.0.21/30`+`169.254.0.22` / `169.254.1.1/31`+`169.254.1.0` | 按 profile 的 guest 内 `{inner_ip, nexthop}`:每 profile 复用同一对,沙箱唯一身份是 floatingip;e2b 的 /30 + 网关让 envd 端口转发可用 |
| `sandbox.boot.kernel` | – | vmlinux 路径 |
| `sandbox.boot.runtime` | – | 单一 guest runtime bundle;offset-zero EROFS + digest marker ZIP,内置 envd、flatten-ctl、mkfs.erofs(§11) |
| `sandbox.boot.overlay_diff_template` | – | 预格式化空 ext4,img 冷启时稀疏复制为可写 upper(裸空 diff 非合法 fs 会被拒);部署方 `mkfs.ext4` 于稀疏文件提供;restore 不需要(overlay 链来自快照) |
| `builder.max_concurrent` | `2` | 构建池并发(serve 内计数信号量,§12) |
| `builder.cpu_quota` / `.memory_max` | 空 | 施加到 `sandbox-builder.slice` 的 `CPUQuota`/`MemoryMax` |
| `builder.insecure_registry` | `false` | 经明文 HTTP 拉取 base 镜像(dev/本机 registry) |
| `builder.platform` | 空 | 拉取平台,如 `linux/amd64` |
| `builder.image_uri_mask` | 空 | 客户端推送镜像的命名约定(含 `{templateID}`/`{buildID}` 占位,须与 e2b CLI 的 `E2B_IMAGE_URI_MASK` 一致);trigger 缺 `fromImage` 时据此推导;**须从构建沙箱内可达**——拉取在 guest 内进行(§12) |
| `builder.referer` | 关 | fromImage import 的 OCI Referrers cache:`enabled` 默认 false;`fallback`/`writeback` 默认 true;`desc` 为公开 owner descriptor(启用时必填);`key` 为空则等于 desc;`validity` 为可选 Go duration。build 可经 `X-Kuasar-Sandbox-Builder` 进一步禁用 lookup/writeback,不能越权启用(§4.6、§12) |
| `builder.diff_template` | – | 构建沙箱可写盘的预格式化 ext4(拉取缓存 + steps 增量 + 导出 scratch;稀疏文件,建议 ≥ 最大预期镜像的 3 倍) |
| `builder.vcpu` / `.memory` | `2` / `4GiB` | 每台构建沙箱(阶段 microVM)的容量 |
| `builder.{pull,step,ready,total}_timeout_sec` | `600`/`600`/`120`/`1800` | 阶段超时:guest 内拉取+展平、单条 RUN step(经 `Connect-Timeout-Ms` 同步到 guest 侧)、readyCmd 轮询预算(2s 间隔;缺省 readyCmd = `sleep 20`)、整个构建(单元 `TimeoutStartSec` = total+60) |
| `builder.files_storage` | 空 | COPY 构建上下文的 S3/OBS 对象存储(子键 `endpoint`/`region`/`bucket`(必填)/`prefix`/`access_key`/`secret_key`/`force_path_style`/`presign_expiry`);空 = COPY 回 501。serve 仅 presign + HEAD;`access_key` 空走 AWS 默认链;`force_path_style` 默认 false(versitygw/minio 置 true);`presign_expiry` 默认 1h(PUT;GET 用 total+5m)。本地/单机无云对象存储用 versitygw(§12) |
| `checkpoint.mode` | `local` | 暂停态 capture:`local` = 本机 working-set bundle;`remote` 仅保留旧部署兼容且已废弃(§8.1) |
| `checkpoint.local_dir` | `/var/lib/sandbox-saved` | 本机快照目录 |
| `checkpoint.merge_ref` / `.drop_caches` | 未设置 | local Pause 的节点级三态策略:`true`/`false` 显式传给 `sandbox-ctl snapshot`;省略或 YAML `null` 则交给 sandbox-ctl 缺省。remote mode 禁止设置 |
| `checkpoint.remote.ref_location_parent` | 空 | remote 兼容字段:可选 absolute `file://` URI;非空时旧路径实际仍 local capture。portable publish 应在 local Pause 后独立执行 |
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
| `cluster.data_endpoint` | 空 | 本节点数据面端点(供 router 转发);缺省由 `api.domain` + `proxy`/`api` 监听推导 |
| `resource_listen` | 缺省(不内置) | 内置资源控制器整块(调参内联,无独立文件):`enabled` 开关、`socket`(控制器 UDS,**唯一权威**;空 = `pkg/resource` 默认,与 sandbox-ctl 一致),其余 `state_path`/`audit_path`/`cgroup_scan_paths`/`resources`/`watermarks`/`rate_limits`/`admission`/`dampening` 均有默认(语义见 node-resource.md §3.2);整块省略或 `enabled: false` = 不内置(沙箱用静态 cgroup) |

远程内存 Prefetch 没有节点统一开关。是否请求 Prefetch 由每个 sandbox 的
`kuasar-sandbox.restore` 命名空间决定(§4.6)。

配置自洽校验:`mmds.enabled=false` 时 `proxy.auth` 必须为 `enforce`(envd 非 secure,
proxy 是唯一数据面闸门);`mmds.enabled=true` 时 `proxy.mode` 不得为 `off`(MMDS 寄宿
proxy 组件);`mmds.routes.enabled=true` 还要求 `mmds.enabled=true`,service endpoint
必须是 absolute Unix socket URI。`proxy.proxy_netns` 仅在 internal 模式有效;external 模式在 `proxy.yaml`
配置 `proxy_netns`。external 模式无须静态 worker 列表——proxy master 经 plugin 平面注册,
proxyForwarder 按活跃注册集转发(§9.1、§9.3)。配 `cluster.node_link.endpoint` 时 `cluster.node_id` 必填;配
`resource_listen` 时 `sandbox.resources.control_socket` 通常指向它(否则控制器空跑)。

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
| create | `POST /sandboxes` → 201 | body `{templateID, timeout, metadata, envVars}` + 可选 `X-Kuasar-Sandbox-*` Header(timeout 缺省取 `sandbox.timeout_sec`,默认 300s);`X-Kuasar-Sandbox-MMDS`/`metadata["kuasar-sandbox.mmds"]` 可声明 routes 与 initial secrets(§4.6);201 表示 durable starting acceptance,不等待 runner/runtime/envd;e2b 回 Envd/Traffic/Forward token,bare 只回 Forward token |
| get | `GET /sandboxes/{id}` | 附 `state`/`startedAt`/`endAt`/`metadata` |
| resource stats | `GET /sandboxes/{id}/stats/resource` | 只读 resource controller reservation/report;sparse JSON,不访问 envd |
| traffic stats | `GET /sandboxes/{id}/stats/traffic` | 最终 node proxy 当前 parking/egress 与保守 `idleSince`;不 Wake/Resume |
| list | `GET /v2/sandboxes` | 仅本租户;query `state`/`limit`/`nextToken`,省略 state 时只列 running/paused,显式 state 可供内部故障诊断;分页头 `x-next-token`;每项含 `cpuCount`/`memoryMB`/`diskSizeMB`(节点统一配置值 + overlay 模板尺寸)与 ISO-8601 `startedAt`/`endAt` |
| kill | `DELETE /sandboxes/{id}` → 204 | 非本租户 ⇒ 404;starting 会取消当前 launch、删除行并精确清理已持久化的 runner/network ownership |
| resume | `POST /sandboxes/{id}/connect` | e2b 语义:resume 走 `/connect`;body `{timeout:秒}` 顺带续期;paused 在返回前原子变为 `starting,run_id=""` 并清空旧网络 ownership;目标缺失时可携 `X-Kuasar-Migration-Token` 同步 import paused 后执行同一受理;返回不等待异步 restore |
| exec session | `POST /sandboxes/{id}/exec-sessions` → 201 | 只接受 `X-API-KEY`;为 native exec 签发一个 `execAccessToken`,可选 `ttlSeconds` 和 `X-Kuasar-Migration-Token`;不创建 guest process |
| pause | `POST /sandboxes/{id}/pause` → 204 | 已暂停或正在 starting 回 **409**;starting 不调用 snapshot/ctl.sock |
| timeout | `POST /sandboxes/{id}/timeout` | body `{timeout:秒}`,重置 TTL;starting 允许窄字段更新 |

create 的 `templateID` 接受三种引用:持久 id(`<profile>-<kind>-<base64url-ref>`,§4.4)、注册期
transient id、或已 ready 构建的 name/alias——后两者解析到持久 id 再走统一路径。
`envdVersion` 回 `0.6.1`(e2b)或 stub `0.1.0`(bare,≥0.1.0 否则 SDK 自毁)。bare 无
envd,不生成也不返回 Envd/Traffic token;两种 profile 都返回独立的
`forwardAccessToken`。该 token 在创建时签发并随 Sandbox 记录持久化。

Create 响应保持既有 body(不新增 `state` 字段),且与 cache 和后台 worker 使用不同的对象
副本。GET 可观察 `starting`;默认 List 仍只列 running/paused,显式 `state=starting|dead`
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
只允许空,`{}` 或仅含一个 int64 `ttlSeconds` 字段的 JSON object.完整原始 body
(含尾随空白)上限 64 KiB;unknown/duplicate 字段,`null`,负数,第二个 JSON value
和越界 TTL 均在生命周期副作用之前拒绝.64 KiB + 1 返回 413;其它无效 body
返回 400.`ttlSeconds` 缺省或为 0 时 token 长期有效;为正数时以实际签发时刻计算
`exp`,Unix 秒加法或 `time.Time` 表示溢出均返回 400.签发位于 SID lifecycle fence 内:
先等前一 attempt 的 terminal cleanup 完成,再生成 token,随后才允许新的 paused→starting;
因此 fence 等待不消耗 token TTL,签名失败也不会产生新的 launch side effect.

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
reservation、预算和 sandbox-ctl 已上报样本。示例:

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

每个字段都可省略:未采集就不序列化,不以零值伪造。`timestampUnix` 是最近一次携
`CurrentRSS>0` 的 Settled/Heartbeat report 时间;`memUsed` 是 sandboxer 报告的 cgroup
`memory.current`;capacity/allocatable 取 controller reservation。controller 未启用为 501;
starting 且 reservation 已存在可返回 sparse 200;paused 无 live reservation 为 409;running
但 reservation 缺失为 503。reservation 存在而尚无 RSS report 时仍返回其它可得字段。
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
  "inflight": {"parking": 0, "egress": 0},
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "forward": {"parking": 0, "egress": 0, "idleSince": "2026-08-12T14:03:21.123456789Z"},
    "exec": {"parking": 0, "egress": 0, "idleSince": "2026-08-12T14:00:00Z"}
  }
}
```

e2b 的 service 集是 `forward/e2b:envd/e2b:code-interpreter/exec`,bare 是
`forward/exec`。顶层 `inflight` 是各 service 求和。每个 service 仅在两项为零时附
`idleSince`;顶层仅在 state=running 且全零时附所有适用 service 时间的最大值。
starting/paused 返回 inflight 但不返回顶层 idle。V1 不返回 idle bool/duration、last
open/close、累计连接数、bytes/延迟/端口明细或 worker 信息。

internal proxy 在同进程聚合;external conductor 经当前 trusted proxy registration 的
`stats_socket` 读取 master cache,查询时不扇出 worker。proxy mode=off 为 501;master 未注册、
route 未完成同步、RunID/profile 不匹配、worker stream 故障或 replacement 未 ready 为 503。
完整 worker-local 状态机、绝对快照 stream 和故障窗口见 [node-proxy.md](node-proxy.md) §8。

### 4.2 控制面:模板构建 API

实现 e2b **v2 build system** 的端点族(SDK `Template.build` / CLI 走它);构建语义与
资源池见 §12。

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| register | `POST /v3/templates` → 202 | body `{name, tags, profile?, cpuCount, memoryMB, metadata?}`;`profile∈{e2b,bare}`,省略按此 e2b 兼容端点语义取 `e2b`,注册后不可变;`X-Kuasar-Sandbox-*` 头 → 模板默认配置(cpu/memory→`resource.capacity`,§4.6);MMDS routes/initial secrets 只供本次 builder sandbox,secret 不进入模板;回 `{templateID: transient-<uuidv7>, buildID, profile, names, tags, aliases, public:false}` |
| trigger | `POST /v2/templates/{tid}/builds/{bid}` → 202 | body 兼容 `{fromImage, fromTemplate, fromImageRegistry{username,password}, steps[], startCmd, readyCmd}` 与 CLI 形态 `{start_cmd, ready_cmd, …}`;`fromImage`/`fromTemplate` 互斥,皆缺时由 `builder.image_uri_mask` 推 fromImage;steps 支持 `RUN/ENV/ARG/WORKDIR/USER/COPY`(COPY 须先经 files 端点上传 context:未配 files_storage→**501**、未上传→**400**,§12);e2b 的 `startCmd` 非空或 fromTemplate ⇒ 暂记 snp(终态以流水线产物为准),否则 img;bare 禁止 start/ready(400)且恒为 image-only;fromTemplate 且无 steps 无 startCmd ⇒ 拒绝(无事可做);`cpu_count`/`memory_mb` + 非 MMDS `X-Kuasar-Sandbox-*` 头 → 模板配置,**覆盖 register**;`X-Kuasar-Sandbox-Builder` → build-only 配置(§4.6)。Trigger 出现 MMDS Header 或 metadata key 直接拒绝,不能覆盖 Register 的 MMDS |
| status | `GET /templates/{tid}/builds/{bid}/status` | 回 `{templateID, buildID, profile, status, logs:[], logEntries:[]}` + 失败时 `reason{message}`;`logs`/`logEntries` 取自 journald 构建流(tag build),按 `?logsOffset`(已读条数)分页,SDK `on_build_logs` 即据此流式输出(§12);**进行中恒报 `building`**(registered/waiting/building 均映射,CLI wait 循环仅在 `building` 续轮询),终态 `ready`/`error`;失败 `reason` 通用(详情在日志流);ready 后 `templateID` 即报持久 id,并附 `names`/`aliases` |
| files | `GET /templates/{tid}/files/{hash}` → 201 | COPY context 上传协商:`tid→build→归属`校验后回 `{present, url}`——present 即对象已在桶(客户端跳过上传),url 为**直传桶的 presigned PUT**(字节不过控制面);未配 `files_storage`→**501**,未知/非属主 tid→**404**。详见 §12 |
| list | `GET /templates` | 本租户 ready 模板;`templateID` 列为持久 id,同时回不可变 `profile` |

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
且始终强制验证 KAT,不受普通 proxy `off|log|enforce` 模式影响.验证成功后
才可以恢复 paused sandbox,并由最终 node proxy 连接 `ctl.sock`;详见
[node-proxy.md](node-proxy.md) §5.

### 4.4 templateID 与模板形态(transient / persist,无 templates 表)

```
persist  templateID = <profile>-<kind>-<base64url(canonical-portable-ref)>
                                                profile∈{e2b,bare}; kind∈{img,snp}
transient templateID = transient-<uuidv7>       构建注册期临时句柄,build 完即弃
```

- **持久 id 自描述**:payload 是 `manifest://<key>` 或
  `file://<content-addressed-basename>@location:<name>`;运行期解析 profile(选
  runtime erofs)、kind(img = 冷启,snp = restore)和 canonical portable ref。
  local file ref、宿主绝对路径、非 canonical ref 或 artifact kind 不匹配均拒绝。
- **临时 id** 由注册生成;构建完成后持久 id 写入该构建的 names + aliases 一并返回,
  之后只用持久 id。
- **无独立 templates 表**:`builds` 表兼任模板登记(§15);持久 id 由构建产物推导。
  快照晋升的模板(§8.1)甚至不写 builds 表——id 本身即完整引用。

### 4.5 SDK / CLI 对接与协议 pin

- 重定向:`E2B_DOMAIN=<domain>` + `E2B_API_KEY`(生产,TLS);dev 走
  `E2B_API_URL`/`E2B_SANDBOX_URL`(http/h2c)。控制面要求 Host 命中 `api.*`。
- api_key 形态:`e2b_` + 72 hex(共 76 字符);e2b SDK 以 `/^e2b_[0-9a-f]+$/` 校验
  格式,服务端另验 MAC(§7)。
- envd 版本 pin:按 e2b-dev/infra 发布 tag 定版(guest-runtime/native-deps `ENVD_TARBALL`,默认
  `2026.22`,对应 envd 0.6.x);SDK:`e2b` js 2.27.x / py 2.25.x 实测兼容。
- 数据面鉴权头 `X-Access-Token`(= `envdAccessToken`):secure 沙箱自 SDK v2.0.0
  默认开,SDK 每次数据面调用携带。
- routesync(external proxy / 路由观察者):版本 1,帧 `[4B LE len][JSON]`,消息
  `register|hello|upsert|delete|bookmark|wake`,路径
  `PUT /internal/plugin/{id}/register`(config-socket plugin 平面,§6;线格式 node-proxy.md §4).

### 4.6 沙箱配置传递链

每实例沙箱配置经**命名空间化的 e2b metadata 保留键** `kuasar-sandbox.<ns>`(各值一个
JSON 对象)注入,零 SDK/API 改动。命名空间是 sandbox-runtime `config.SandboxConfig`
(serve import,单一真源)的**租户可控子集**:

| 命名空间 | 去向 |
|---|---|
| `resource` | `resources.{capacity,allocatable}`(不含 `control`,节点托管) |
| `network` | 拆分:`hostname`/`nexthop`→guest;`inner_ip`/`transit_*`→`vswitch.Attach`;`dns`→`/etc/resolv.conf` |
| `launch` | `launch.{exec,args,env,workdir,restart,user,stop_signal,plugin,cgroup_control}`——**仅 bare**;e2b profile 拒(envd 占用 launch) |
| `init` / `mounts` / `files` | 直透 `init[]` / `mounts[]` / `files[]` |
| `metadata` | `SANDBOX_CONFIG.metadata` 透传(如 `e2b.start_cmd`) |
| `restore` | 本次 host restore 的 `prefetch` 策略;可省略,显式值只允许 `off`/`memory` |
| `credentials` | 创建期 ServiceSecret、Envd/Traffic token override;解析后从普通 metadata 剥离,不进入 guest |
| `checkpoint` | host-only、仅本次 Create 的 local Pause 缺省:`merge_ref`/`drop_caches` 各自为 `true`/`false`/`null`;只存 sandbox row,不进入 runtime YAML 或 snapshot.cfg |
| `mmds` | portable exact `routes` + request-scoped initial `secrets`;持久化前拆分,metadata 最终只保留 routes |

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
本次 synthetic builder sandbox;initial values 以 build owner 加密保存。build 终态事务同时
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
ServiceSecret 缺省从 APISecret 与 `AuthSandboxID()` 派生;Forward token 始终由最终
ServiceSecret 自动签发,不能由请求指定。credentials 在验证后立即从普通 metadata 分离。

Local checkpoint policy 也支持 metadata 与 Create header 两个入口:

```http
X-Kuasar-Sandbox-Checkpoint: {"merge_ref":false,"drop_caches":null}
```

```json
{"metadata":{"kuasar-sandbox.checkpoint":"{\"merge_ref\":true,\"drop_caches\":false}"}}
```

两处共用严格 JSON object 解析:只允许 `merge_ref`、`drop_caches`,每个值只允许
`true`、`false`、`null`;unknown、错误类型、第二个 value 或尾随内容均返回 400。
Header 按字段覆盖 metadata;`null`/缺失表示不覆盖,而不是清除低层值。合并后全为
`null`/缺失则删除整个 namespace。该 namespace 不从 template/build metadata 继承;
`X-Kuasar-Sandbox-Checkpoint` 也不是 Build header。remote mode 下非空 policy 在 Create
副作用前拒绝,应迁移节点为 `checkpoint.mode=local`。

`kuasar-sandbox.cluster` 不属于上述租户配置命名空间。构建任务仍用该 metadata 字段携带
cluster 自有的 group;普通 sandbox 的 `Profile`、`Group`、`RouteKey` 和可选
`AuthSandboxID` 则通过 node-link 的结构化系统上下文下发并独立持久化,不进入用户
metadata。node 不在事件中回传 Registry 自有的 group、route key 或认证主体;Registry
通过节点归属记录恢复这些信息。

构建端点额外接受 **build-only** 命名空间 `kuasar-sandbox.builder`,对应请求头
`X-Kuasar-Sandbox-Builder`,当前形态:

```json
{"referer":{"enabled":false,"writeback":false}}
```

它只控制本次模板构建的 import referer 行为,解析后从模板 sandbox metadata 中剥离,
持久化到 `builds.builder_json`;不会随模板 create/resume 进入运行时配置。

- **渲染**:serve 建 `config.SandboxConfig` 基座(boot/tapfd/control/capacity/已解析
  网络)再叠租户命名空间,yaml 序列化经 config-socket 交 sandbox-ctl。深校验(ValidateCold)
  在 sandbox-ctl——serve 侧 yaml 是半成品(cgroup_path 由 run-sandbox 以继承 FD
  覆盖、base 经快照填),这里只对租户网络做格式校验。
- **两个注入面**:e2b metadata,与 `X-Kuasar-Sandbox-<Ns>` 请求头(API 边缘归一化进
  metadata,**同名头胜过 metadata 键**)。create 与模板构建(register/trigger)都支持;
  create 的 runtime sandbox 配置存 `sandboxes.metadata_json`,模板构建的普通 runtime 配置存
  `builds.metadata_json`,build-only 配置存 `builds.builder_json`。
  `restore`、`credentials`、`checkpoint` 是 request-scoped 例外:只接受 create 请求,
  不从模板继承;checkpoint header 不进入模板构建。
  集群下 sandbox `create` 的所有权信息使用独立的 node-link 系统上下文(§10)。
- **优先级**:`节点默认 ⊕ 模板配置 ⊕ create 配置`(create 按命名空间胜)。模板配置:snp
  经快照、img 经 `builds.metadata_json`。构建内 `register ⊕ trigger`(trigger 胜);
  register/trigger 的 `cpuCount`/`memoryMB` → `resource.capacity`(胜过 resource 头),决定
  phase-C 构建 VM 容量。
- **capacity**:img create 自由(create/模板/默认);snp create / resume / 迁移导入**钉死
  快照**(runtime 拒容量不等)。
- **network 随快照**:普通 sandbox 渲染时把已解析逻辑网络注入
  `SANDBOX_CONFIG.metadata["kuasar-sandbox.network"]`,随 snapshot.cfg 落盘并跨 restore
  继承;restore 时 serve 读回,填 create 未指定的网络字段(**显式 create 胜**,§8)。
  Build 的临时 VM 与最终模板分别从同一个 `NetworkSpec` 解析:未声明 hostname 时前者使用
  `build-<short-build-id>`,后者使用 `sandbox.network.hostname`;故临时 hostname 不进入模板。
  C 阶段把模板的完整有效 `NetworkSpec` 写入同一 snapshot metadata,使仅持有 snapshot
  制品、没有原 `builds` 记录的 create/restore 仍能恢复网络语义。`builds.metadata_json`
  保留 register/trigger 声明,承担控制面索引和模板默认配置;snapshot metadata 承担制品
  自描述。两者同时存在时先按既有 namespace 规则得到 create/模板声明,再以该声明字段覆盖
  snapshot 字段,最后补 node/profile 默认值。img-only(包括 bare)没有 snapshot metadata
  通道,仍由 `builds.metadata_json` 与运行节点默认值提供模板网络。迁移 token 同样携带
  metadata。除 host-side restore/checkpoint policy 外,其余命名空间只在冷启生效或已冻入快照,故只
  network 需随快照。
- **host policy 不随快照或模板**:`kuasar-sandbox.restore` 与
  `kuasar-sandbox.checkpoint` 只由 create 请求写入 sandbox metadata。image cold boot
  不把它们渲染进运行 YAML;snp create、pause 后 resume 和 migration import 在存在
  restore ref 时重新渲染 restore policy;checkpoint 始终只由 host Pause 路径读取,
  不进入 `SANDBOX_CONFIG`/`snapshot.cfg`。connect/resume 不提供临时覆盖。
- **持久化**:`sandboxes.metadata_json` / `builds.metadata_json` / `builds.builder_json`。

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
ExecStart=<node-ctl> run-sandbox --pidfile=/run/sandbox/runs/%i.pid \
          --config-socket=/run/sandbox/node-ctl.socket --run-id=%i
ExecStopPost=/bin/rm -f /run/sandbox/runs/%i.pid
Restart=no                  # 一进程一沙箱、有状态:崩 = 该沙箱已死,不重试
KillMode=control-group      # StopUnit 连 cloud-hypervisor 一并 SIGKILL(§5.1)
TimeoutStopSec=20
Slice=sandbox-runner.slice
Delegate=yes                # 委派 cpu/memory controller(§5.1)
DelegateSubgroup=ctl        # node-ctl / sandbox-ctl 留在不受沙箱水位限制的 ctl/
```

**builder 单元**(`%i` = run-id,§12):

```ini
# sandbox-builder@.service (生成内容)
[Service]
Type=exec
WorkingDirectory=/run/sandbox
StandardError=journal
LogRateLimitIntervalSec=0                        # 构建少且要全量细节(每阶段控制台 + flatten/RUN 进度):关本单元限流不丢行(runner 单元保留默认限流,§5.2)
ExecStart=<node-ctl> run-builder --pidfile=/run/sandbox/runs/%i.pid \
          --config-socket=/run/sandbox/node-ctl.socket --run-id=%i
ExecStopPost=/bin/rm -f /run/sandbox/runs/%i.pid
KillMode=control-group
Slice=sandbox-builder.slice   # 构建池 cgroup 上限(builder.cpu_quota/memory_max)
```

两单元的 ExecStart 都先锁 run-id pidfile,再经 config-socket WaitAssignment 等待
业务 id。runner 取得 sid 后再锁 `<run_root>/<sid>/<sid>.pid`,取 LaunchSpec 并
`execve` 替换为 `sandbox-ctl run`(继承单元主 PID 与 cgroup,`Type=exec` 故无需
sd_notify);builder 取得 bid 后再锁 `<run_root>/<bid>/<bid>.pid`,取 BuildSpec,
**驻留**驱动三阶段流水线(§12),阶段沙箱(`sandbox-ctl run` + cloud-hypervisor)是其
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

- **kill**:在 SID lifecycle fence 内取消 active launch→删除 durable row→
  `StopUnit`(连 CH 一并 SIGKILL)→`ResetFailedUnit`→tapfd `RELEASE` 或
  `connector-ctl vswitch detach`→删运行目录/cache→发布 Delete。launch claim 仍保留到旧
  attempt 完成其局部资源清理,因此迟到 CAS 不能复活该行,同 SID 也不能提前启动后继 attempt。
  `kill` 在进程层生效,不受 guest 内 restart 策略阻挡。
- **就绪**:serve 在分配 runner 前先绑定 `<run_root>/<sid>/ready.sock`(目录 0700、
  socket 0600),分配后从 node-ctl 的 one-shot 连接严格读取
  `control_ready\nready\nEOF`;bare 到此启动成功。e2b 随后把 `POST /init` 作为首个 envd
  请求,不以 `/health` 作为启动门槛;health 仅在初始化完成后用于外部存活检查。runtime
  launch budget 从 Assign 成功/runner handoff 后才开始,与前述 runner assignment budget
  独立;它覆盖 wire 与 mandatory `/init` 的 60s 启动预算.首个 `/init` 立即发出;仅连接/传输
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

systemd 通过 `DelegateSubgroup=ctl` 从 exec 前即把 node-ctl 放入 `ctl/`。
`node-ctl run-sandbox` 在等待 assignment 前验证该身份,于空的 unit 根启用 cpu/memory
controller,幂等创建 `vmm/` 并以 CLOEXEC 打开目录 FD。取得 LaunchSpec 后,启动器在最终
exec 前才使该 FD 可继承,本地追加 `--cgroup-path=fd=N`;LaunchSpec 自身不携带任何主机
cgroup 路径或 FD。

sandbox-ctl 接收 FD 后立即恢复 CLOEXEC,写入资源上限,并以
`clone3(CLONE_INTO_CGROUP)` 把 CH 原子创建到 `vmm/`。因此 `memory.high` 只限制
VMM,sandbox-ctl 在 guest 压力下仍可处理 UFFD、vsock、信号和进程回收。
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
(`--stdout-to/--stderr-to/--console journald=<tag>`,语义见 `sandboxer/docs/sandbox.md`
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
  (内核),并带 `KUASAR_BUILD_ID=<bid>`;单元 `LogRateLimitIntervalSec=0` 保证不丢行(§5)。
- **限流策略**:builder 单元关限流(构建少、要全量细节);runner 单元保留默认限流
  (数千沙箱不得刷爆 journal)。
- sandbox-ctl 自身进程日志(其 stderr)随单元落 journal 但**不带标签**——属宿主排障,
  不进任何标签过滤流。

## 6. 本机控制 socket(run / task / admin / plugin / api 平面)

serve 在 UDS `paths.config_socket`(默认 `/run/sandbox/node-ctl.socket`,**0600**)
跑一个 h2c HTTP 服务(兼容 HTTP/1.1):单 socket 复用五个平面、各自鉴权。连接建立时
经 **`SO_PEERCRED`** 取 peer pid 注入请求上下文;socket 0600 ⇒ 仅同 uid / root 可连,
各平面在此之上再细分。`/internal/*` 前缀 e2b SDK 永不使用,与 api 路径不冲突。

**① task 平面** — 启动器取工作规约,两条路径、同一鉴权:

- `POST /internal/task/launchspec`(run-sandbox;req `{config_id: "sandbox:<sid>"}`)
  → **LaunchSpec** `{exec, args, workdir, env}`:`exec=sandbox-ctl`,
  `args=[run --sandbox-id <sid> --config <rundir>/<sid>.yaml --manifest-config
  <shared> --run-root <run_root> (--restore <ref>)
  (--connect <uds:ip:port>)…]`,`env={MANIFEST_KEY}`。`--run-root` 把 sandbox-ctl
  的 socket/staging 目录(`ch.sock`/`ctl.sock`/…)钉到 serve 的 run_root,
  pause/snapshot 客户端(同 `--run-root`)才能拨到 `ctl.sock`。node-ctl 在最终 exec
  时另行强制追加本机 `--cgroup-path=fd=N`,不允许 LaunchSpec 覆盖。
- `POST /internal/task/buildspec`(run-builder;req `{config_id: "build:<bid>"}`)
  → **BuildSpec(构建工作单)**:`{build_id, profile, workdir, from_image | from_template
  (+kind), steps[], start_cmd, ready_cmd, env, paths, net, vcpu, memory,
  mmds_enabled, envd_token, insecure, platform, timeouts}`——`env` 含
  `MANIFEST_KEY` + 租户 `FLATTEN_*` 拉取凭据;`paths` 是宿主侧工件与工具
  (kernel / runtime / 两个 diff template / sandbox-ctl /
  flatten-ctl / manifest-ctl / manifest_config);`net` 是 serve 预先 attach 的
  网络槽(tapfd transport、mac、inner_ip、nexthop、hostname、dns),全构建复用。
  run-builder 据此自建阶段沙箱(§12);仅在该构建单元运行期间可取(serve 持挂
  pending 状态,单元退出即失效)。
- **鉴权**:peer pid ⟷ `<rundir>/<id>/<id>.pid`(启动器拨号前已锁写本 PID),相等即
  认证。
- 设计意图:**非密配置走文件**(`<sid>.yaml`;构建的阶段 yaml 由 run-builder 写进
  workdir)、**密钥走 spec env**——秘密只在内存与 env 中,不落盘。

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
发布 Upsert 使 external proxy 收敛,失败时不发布伪状态。日志只记录 sid/name/outcome,
绝不记录 body。鉴权仍是本 socket 的 0600 + SO_PEERCRED + 可选 admin pidfile。

**③ plugin 平面** — `PUT /internal/plugin/{id}/register`:一个订阅者(external proxy
master,或路由观察者如平台 agent)注册其能力并**持挂该 h2c 连接**——连接本身即它的
租约 + 路由流(routesync,线格式见 node-proxy.md §4).请求体首帧是 `register{caps}`,之后(route_wake)是
`wake` 上行;响应体下行 `hello(policy) → upsert* → bookmark → upsert/delete`。能力相互
**独立、不强制组合**:`subscribe`(`route` | `route_wake`)、`proxy{socket{path}}`
(声明 serve proxyForwarder 转发数据面请求的目标 UDS)、`mmds`。**断连即反注册**;同
id 二次注册自动反注册(并断链)前者。鉴权:配 `paths.plugin_pidfile` 则 peer pid 须在
其中,未配则仅靠 socket 0600(同 admin)。external proxy master、平台 agent 均经此订阅。
此平面是**节点本地** UDS,与接入集群的 node-link(§10,跨网 mTLS)正交。

MMDS confidential 投影不是任意 plugin 自报的权限。只有 id 精确为 `proxy`,且同时满足
`subscribe.kind=route_wake`、`proxy!=nil`、`mmds=true` 的 registration 才收到 conductor
MMDS service registry、`mmds_routes` 与 `mmds_route_secret_values`;普通 route observer
两者均不收,node-link/cluster stream 也不收。伪装为其它 id 的 `mmds=true` registration
被拒绝。external master 在完整 Bookmark 前 fail closed,断流即清空可变 route/value/service
视图,不会无限期服务断连前的 secret。

**④ api 平面** — 其余路径回落到 e2b 控制面 handler(与 TLS `api.listen` **同一个**
`http.Handler`,含 export/import 扩展),明文 h2c、`X-API-KEY` 鉴权。
`export-sandbox`/`import-sandbox` CLI 即此平面客户端(§8.1)。

**信任模型**:host root / daemon uid 可信;租户代码在 guest 内,够不到 host UDS。

## 7. 密钥与归属模型(APISecret + ManifestKey 凭据对,加密存 sqlite)

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
  `SHA256(salt‖明文)`、与租户 key 无关 ⇒ chunk 去重仍跨租户;租户之间不共享 key 与
  模板。
- **根凭据不落明文**:sqlite 内加密;运行期只在必要的进程内存、受保护路由投影和进程 env 中。
  ManifestKey 经 LaunchSpec/BuildSpec 注入需要内容访问的宿主进程;`<sid>.yaml` 非密不含根凭据。
  APISecret 由 serve 用于 API 认证和 ServiceSecret 派生,并可投影给可信 router/proxy;
  不下发给 guest 或业务进程。auto-resume 从资源行解密 ManifestKey 访问快照。集群下
  node-link 原子下发完整凭据对,
  两者同样仅入加密存储 + 运行期内存(§10)。
- 每个 Sandbox 持久化独立 ServiceSecret。未 override 时按
  `HMAC-SHA256(APISecret,"kuasar-service-secret-v1:"+AuthSandboxID())` 派生;保存后不再重建。
  e2b 的 `envdAccessToken`/`trafficAccessToken` 可 override,否则分别随机生成;bare 两项恒空。
  EnvdAccessToken 用于 e2b 49983/49999,TrafficAccessToken 仅供外部网关及 e2b 数据面组件
  验证,不由 node 平台层消费。两个 e2b opaque token 均限制为有效 UTF-8、最多 256 bytes,
  在写业务行前校验。e2b/bare 的 `forwardAccessToken` 均为 ServiceSecret 签发、绑定
  AuthSandboxID 且 `aud=forward` 的严格 `kat1`,用于其他 forward 目标。四项凭据均加密
  落盘并在 lifecycle upsert 中不可重绑。
  ExecAccessToken 不在 create/get/list 中缺省生成,也不写 Sandbox 业务行;它只由
  `POST /sandboxes/{id}/exec-sessions` 按次签发.每个 token 使用 UUIDv7 `session_id`,
  线格式为 `kat1.<base64url-no-padding(payload)>.<base64url-no-padding(signature)>`,
  payload 的 canonical field 顺序为 `v,session_id,sid,aud[,exp]`,其中 `sid=AuthSandboxID()`,
  `aud=exec`,不包含 `iat`.signature 以解码后的 32-byte ServiceSecret 直接执行
  HMAC-SHA256,不另派生 exec key.payload 也不含 constraints,generation,node ID 或
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

```
create(img: cold boot / snp: restore)
  │
  ▼
starting ──success──► running ──pause / TTL──► paused
  │                     │                       │
  │                     └──kill──► row deleted  │ connect / data wake
  │                                             ▼
  └──create failure──► dead                  starting
                                                │
                          running ◄──success─────┤
                                                └──resume failure──► paused
```

- 操作映射:create(img = 冷启 / snp = restore)、connect = resume、pause = snapshot、
  timeout = 续期、TTL 到期 = auto-suspend(pause)、kill = 销毁(删行)。重启对账可把
  失联的 running 标为 `dead`(§15);paused/dead 行中,paused 可再拉起,kill 删行。
  集群下,这些操作另由 node-link 命令触发(create/connect/exec_session/delete,§10),并把
  状态变化作为事件上报 registry。
- `starting` 是一个持久业务生命周期状态,不是 runner 状态或 e2b readiness。它从请求已被
  durable acceptance 开始,连续覆盖资源/snapshot/network/YAML/ready.sock 准备、runner pool
  排队和分配、sandbox-ctl/VMM 启动、runtime readiness 与 mandatory `/init`;对外不增加
  queued/assigned/booting/initializing。初始行为 `starting,run_id=""`,pool commit callback
  以 `state=starting AND run_id=''` 绑定 run-id;最终 running commit 和分配后 rollback 均要求
  exact run-id,分配前 rollback 则要求空 run-id。create 失败到 dead 并发布 Delete;resume
  失败清空本次 runner/network ownership、回到 paused 并发布 paused Upsert。默认 list 隐藏
  starting/dead,显式 state 过滤仍可用于诊断。
- 所有 fresh Create、snapshot-template Create、paused resume、KMT restore、cluster
  Create/Connect、data Wake 与 native exec activation 共用一个进程内 launch group。每个 SID
  在 starting 持久化/发布前先 claim 唯一 owner;Kill/Delete 的 Cancel 只发取消信号,claim
  要等该 attempt 完成 runner/network/local resource cleanup 才释放。waiter 被唤醒前终态
  store/cache/route 已收敛,并且 waiter 始终重读权威状态。若 store 为 starting 但进程内没有
  attempt,节点记录 invariant violation 并 fail closed,不得再分配第二个 runner;重启 Reconcile
  或终态操作负责收敛。Stop/Reset/detach/local cleanup 任一步失败时按有界退避重试,在全部
  成功前不清空 durable runner/network ownership、不提交 dead/paused、也不释放 claim。
- network attach 后先以 `starting AND run_id=''` CAS 持久化 ownership,再发布 enriched
  starting;CAS 丢失时立即 detach 本地 port,不再写 cache/route 或启动 runner。初始 starting
  route 没有 FloatingIP,预先确定的 UDS 路径也尚未绑定,因而没有可用 backend endpoint;
  普通数据面只能 park。enriched starting 才可供 guest MMDS `/init` 查到完整身份。后台只使用
  窄 CAS,不得用 admission 时的旧 Sandbox 整行覆盖
  并发 SetTimeout/Kill/Connect 更新。
- ordinary envd、forward 和 native exec 数据面在 running 前不得转发。internal proxy 对
  starting 只等待当前 attempt;paused 先完成同一个 durable resume acceptance 再等待。external
  worker 见 starting 时只 park 等待 running/dead/Delete/paused 更新,不发送第二次 Wake;
  回滚为 paused/Delete 时立即结束等待,只有初始 missing/paused 请求最多发一次 Wake。所有
  waiter 以权威终态决定 not-found/route-error,不向普通数据面泄漏 raw launch error。
- Create/Connect handler 返回后的 launch 使用 node lifecycle root,不继承 HTTP request context;
  node shutdown 和 Kill 可取消 attempt。conductor 关闭 store/systemd launcher 前会 drain
  launch group,保证所有已受理 attempt 已完成终态发布与 cleanup;永久不可用的 cleanup 依赖
  由外层 service-manager stop budget 最终约束。Pause starting 明确返回 409,不接触 ctl.sock;
  SetTimeout starting 只更新 deadline;Connect starting 不重复 launch。Reaper 仍只扫描 running,
  Sandbox TTL 的既有定义在本改动中不变。
- `POST /sandboxes/{id}/pause` body 可为空或为:

  ```json
  {"memory":true,"checkpoint_merge_ref":false,"checkpoint_drop_caches":null}
  ```

  `memory` 缺失/`null`/`true` 均表示内存 checkpoint;`false` 在调用 Core 前返回 400。
  两个 checkpoint 字段仅覆盖本次动作,不写回 metadata。可同时携
  `X-Kuasar-Sandbox-Checkpoint`;Header 的具体 `true`/`false` 按字段覆盖 body,
  Header `null`/缺失继续继承 body。body/header 解析失败均无 Pause 副作用。
- **auto-suspend**:reaper(5s 周期)发现 `deadline` 已过 → 对运行中 sandbox-ctl 封
  快照(按 `checkpoint.mode`,§8.1)→ 记 `snapshot_ref`、标 paused → `StopUnit` →
  detach。路由表保留 paused 路由,后续数据面流量可唤醒。
- **auto-resume**:数据面流量打到 paused 沙箱 → 读库 → 重走 launch(建目录 + attach
  + StartUnit),LaunchSpec 带 `--restore <snapshot_ref>` → sandbox-ctl 解封恢复。
  - **launch ownership**:同一 sid 的并发 Connect/Wake/exec activation 与 create/resume 均由
    上述 launch group 合并,杜绝重复 IP 分配、attach 或 StartUnit。internal 模式 proxy 在
    请求内接受/等待;external 模式经 routesync `Wake` 上行。集群数据面激活另由 registry
    route CAS 和稳定 lineage wait 收敛,但 node-local CmdConnect 仍使用同一 launch owner。
  - 数据面 auto-resume 等待恢复完成后再转发;`POST /sandboxes/{id}/connect` 则只同步
    完成鉴权、可选 KMT import 和凭据读取,接受/加入同一 launch attempt 后立即返回;
    带 `timeout` 时该期限在恢复后仍覆盖节点缺省 TTL。
  - exec-session 签发同步完成可选 import,对象/凭据校验和 KAT 签名,
    然后只接受异步 resume 并立即返回;目标已 starting 时可继续签发但不重复 resume。
    KAT 签名失败时不启动 resume;
    后续数据面的无效 KAT 也不能触发本地恢复.
- **phase timing**:launch 以低基数 `kind=create|resume`、`profile=e2b|bare`、
  `result=success|failure` 和 bounded `failure_stage` 记录 admission/prepare/runner_wait/
  runner_commit/runner_handoff/runtime_ready/envd_init/starting_total duration。sandbox ID 和
  run ID 只进入结构化日志字段,不作为 metric label。`runner_wait` 从调用 Assign 到 commit
  callback 首次拿到 run-id,`runner_commit` 只计窄 Bind/cache,`runtime_ready` 从 handoff 完成
  到 readiness wire 完整成功,`starting_total` 从 durable starting 到终态 commit。
- `running` 仅表示 orchestrator runtime readiness 与 mandatory e2b `/init` 已完成,不保证
  code interpreter、forward 业务端口或用户应用 HTTP/TCP health 已监听。通用业务 backend
  readiness/dial retry 仍由 [#125](https://github.com/kuasar-sandbox/orchestrator/issues/125)
  独立跟踪。
- **每实例配置**(create/构建经 metadata + `X-Kuasar-Sandbox-*` 头,命名空间化,详见
  §4.6):配置随沙箱持久化(`metadata_json`),resume 时重新解析、全生命周期一致;无白名单
  门(沙箱以完整能力经 sandbox API 发布,平台自身亦经此 API 管理)。**network 另随快照**——
  restore 时 serve 读回快照内的逻辑网络,填 create 未指定的字段(显式 create 胜)。

### 8.1 暂停态分层、转模板与跨机迁移

新部署只应使用 `checkpoint.mode=local`:Pause 执行
`sandbox-ctl snapshot --sandbox-id <sid> --output <checkpoint.local_dir>/<sid>
--run-root <run-root>`,并把 `<sid>.snapshot` 存为 node-local `snapshot_ref`。local capture
可按字段解析 working-set policy:

```text
Pause Header > Pause body > sandbox metadata > node config > sandbox-ctl default
```

API 先把 body 与 Header 合为 action override;Core 在 lifecycle lock 内重读 sandbox row,
严格解析历史 metadata 后再逐字段叠加。reaper 没有 action override,所以使用
`sandbox metadata > node config > sandbox-ctl default`。某字段最终为 `nil` 时完全不传
对应 flag;只有显式值才在基础 argv 后追加 `--merge-ref=true|false` 或
`--drop-caches=true|false`。两字段全未设置时 argv 与旧 local Pause 完全相同,缺省行为由
sandbox-ctl 决定。policy 校验完成前不会 cancel resume、snapshot、停 unit、detach 或改库。
snapshot 成功后才写 ref/paused state;Pause 本身不执行 promote。Build/template 的 snapshot
命令不接入这两个 flag,也不把 ready/start command 解释为 working-set warm-up。
本阶段只承诺 local W 的 capture/restore;含 local memory parent 的 W 要变为 portable artifact,
仍须等待 `kuasar-sandbox/sandboxer#54` 的 publisher 修复后由独立 publish/restore E2E 验收,
不得借 deprecated remote Pause 生成 working-set。

`checkpoint.mode=remote` 已废弃,conductor 启动时会告警
`checkpoint.mode=remote is deprecated; use checkpoint.mode=local and publish separately`。
它只保留以下旧路由,两条都拒绝 node/sandbox/action checkpoint policy 且不追加新 flag:

| remote 兼容配置 | 保留行为 |
|---|---|
| `ref_location_parent` 为空 | `sandbox-ctl snapshot --upload` → `manifest://<key>` |
| `ref_location_parent` 非空 | 仍走旧 `snapshot --output` local capture;启动告警会指出应迁移为 local mode |

remote mode 不增加 working-set、local-parent fallback、capture 后自动 promote 或重试。
显式 Pause 遇到 action/metadata policy 返回 400且 sandbox 保持 running;auto-pause 遇到
metadata policy 记录 warning 并保持 running。remote node config 中设置 merge/drop 在加载时失败。

推荐配置 `mode=local` 后独立 publish。若配置
`checkpoint.remote.ref_location_parent`,`export-sandbox` 根据 source sandbox ID 计算:

```text
hash = SHA256(location-name)
location URI = <parent>/<hash[0:2]>/<hash[2:4]>/<location-name>
```

随后用 `upload-snapshot --to-ref-location` 发布,得到
`file://<digest>.snapshot@location:<source-sid>`。没有 parent 时仍发布到 manifest。
两者都是 canonical portable ref;数据库成功重指后才 best-effort 删除明确的本机
checkpoint,located 目录绝不进入本机 cleanup。没有 parent 时独立发布到 manifest。
全部基于现有 sandbox-ctl 原语(`snapshot --output`、`upload-snapshot`、`run --restore`);
legacy remote 的 `snapshot --upload` 只作兼容回归。

`export-sandbox` / `import-sandbox`(CLI 形式见 §2.7)是 api 平面端点
`POST /sandboxes/{id}/export`、`POST /sandboxes/import` 的客户端;两端点在 TLS
`api.listen` 上同样可达(api-key 已按租户隔离),晋升/导出/插行全由 daemon 进程内
完成,无第二写者。

- **晋升 / 转模板**(`--to-template`,须 paused):确保 portable 后,把完整 SnapshotRef
  以 base64url-no-padding 编进 `<profile>-snp-<payload>`(不写 builds 表)。之后
  `e2b sandbox create <id>` 即从该快照扇出新沙箱(新 sid)。
- **迁移 token**:`export-sandbox <sid>` 确保 portable 后导出
  `kmt1.<base64url-no-padding(nonce|ciphertext)>`。ManifestKey 经
  `HMAC-SHA256(decodeHex(ManifestKey),"kuasar-migration-token-v1")` 派生 AES-256-GCM
  key,每次 export 使用随机 12-byte nonce。完整 wire 上限 512 KiB,旧 plain-base64
  token 不再接受。
- **迁移内容与连续性**:GCM payload 携 source NodeSandboxID、`AuthSandboxID()`、Profile、
  template/canonical portable SnapshotRef/runtime、env/metadata、创建/截止时间、两个 tenant root 的完整指纹,
  以及既有 ServiceSecret、Envd/Traffic/Forward token。它不携 APISecret/ManifestKey 原文、
  MMDS route secret value、host absolute path、Group/RouteKey 或 generation。routes-only
  `kuasar-sandbox.mmds` 可随 portable metadata 移动,但 secret plaintext 永不进入 migration
  token、snapshot 或 template。目标 node 从本地 key 表取得完整 pair,
  校验 fingerprints/runtime/Profile/Forward KAT 后原样落库,不重新派生或生成 service credential。
- **target 与冲突**:standalone import 省略 `sandboxID` 时复用 source NodeSandboxID;显式 target
  只替换本地 ID,保留 AuthSandboxID 与全部 credential。ID 使用 1..57 bytes 的 lowercase
  DNS-label 子集 `^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$`。插入为原子 insert-only,
  已存在返回 409且不覆盖。token 可在现有授权下重复用于不同 target,不增加 single-use 状态。
  目标机须预装匹配的 tenant pair;manifest ref 依赖同一 store,located ref 依赖同一
  `ref_location_parent` 部署映射。启动前 conductor 递归读取 snapshot `from_refs`,收集
  graph 中实际出现的全部 location name,逐个派生 URI 并展开为 `--ref-location`。
- **一步迁移**:`Sandbox.connect(<sid>, api_headers={"X-Kuasar-Migration-Token":
  <token>})`——path sid 是明确 target。目标不存在时,connect 在当前请求内同步完成
  decrypt/validate/insert并读取 response credential,接受异步 resume 后返回同一 sid;
  standalone/node-local 请求可同时用 `X-Kuasar-Sandbox-MMDS` 或 metadata 只注入
  `{secrets:{...}}`;routes 权威只能来自 token,请求只要出现 `routes` key(包括 `[]`)即 400,
  每个 initial name 必须被 token 的 secret route 引用,业务 row/routes metadata/secret blob
  原子写入。目标已存在时完全忽略 token 和 MMDS secret 输入,不解析、不校验、不更新。
  超过 512 KiB 的 Header 返回 431;独立 import body/token
  超限返回 413。`import-sandbox` CLI 保留作显式预导入。
- **状态感知驱动迁移**:暂停态的本地/远程经 `RouteEntry.snap_loc`(`local`|`remote`)随
  路由流下发(node-proxy.md §4);订阅 plugin 平面的平台 agent(`subscribe=route`)据此识别哪些 paused
  沙箱节点绑定(腾空节点前须先迁移)、哪些已可移植,再按需调 export-sandbox 铸造
  MIGRATION_TOKEN 完成自动迁移。迁移 token 是凭据且会回收源行,故按需铸造、绝不随路由广播。
- 限制:token 不含 tenant raw root,但携带沙箱自有 env 与数据面 credential,按"沙箱级敏感"对待;
  快照绑定其 guest runtime(erofs 摘要校验),不同 runtime 的节点拒绝导入。

上述 secrets-only 注入只属于 standalone CONNECT import。cluster CONNECT 不透传 MMDS
config,node-link command schema、registry/router/placer 不理解 MMDS,本阶段也没有 cluster
MMDS Secret API、placement/admission/retry 或 cluster MMDS E2E。现有通用 metadata 偶然携带
routes 不构成 cluster 支持合同。

## 9. 数据面装配(serve 侧)

数据面**转发层**——L7 反代:按 `(sid, port)` 路由到 guest envd / floatingip,逐请求
鉴权,CONNECT 隧道,MMDS,以及 routesync 线格式——自成一文,见 [node-proxy.md](node-proxy.md)。
本节只讲 serve 控制面对数据面的三项职责:按 `proxy.mode` 装配承载、作本节点路由
权威向订阅者广播、以及 proxyForwarder。

### 9.1 按 proxy.mode 装配

serve 启动末段按 `proxy.mode`(§3)装配数据面;控制面 `api.<domain>` 始终由
`api.listen` 提供,按 Host 与数据面(`<port>-<sid>.<domain>`)分流:

| 模式 | 数据面承载 | 进程 |
|---|---|---|
| **internal**(默认) | serve 进程内 proxy(与控制面同一 `http.Handler`,按 Host 分流) | 单二进制 |
| **external** | 独立 `node-ctl proxy` master 注册一次,内部 worker 共享继承 listener fd 与 shm 路由视图 | serve + proxy master + N×worker |
| **off** | 拒绝(501) | – |

internal 直接在进程内挂转发层;external 下 serve 不绑数据口,改为在 plugin 平面(§6)
接受 proxy master 注册并向其广播路由(§9.2),数据面字节流不经 serve.#63 的
legacy/explicit forward,envd 和 CI 转发判定由两模式共用.native exec 也在最终
node proxy 执行 KAT 校验和 `ctl.ProxyExec` gate:internal 使用 conductor `paths.run_root`,
external worker 使用自身 `proxy.yaml` 必填且与 conductor 一致的 `paths.run_root` 本地构造
`<run_root>/<NodeSandboxID>/ctl.sock`.该路径不通过 routesync `Policy` 或共享路由视图传递.
部署拓扑见 node-proxy.md §3,共享内存路由视图见 §4,转发判定与隧道详细见 §5.集群下,
cluster-ctl router 把数据面转发进本节点的
数据端点(internal 的 `api.listen`/`data_listen` 或 external proxy master 的数据口),节点侧
按 `E2b-Sandbox-Id` 寻址照常处理(cluster-router.md),无须区分来源。

两种 proxy mode 都使用相同的 traffic flow 状态机。external master 额外注册
`stats_socket`,每个 worker 经独立 socketpair 推送 Prometheus counter 与 per-sandbox
traffic 绝对值;serve 的 traffic API 只查询 master 聚合 cache。proxyForwarder 与 cluster
第二跳只是内部中继,不重复计数,每条 logical ingress 只在最终 node worker 统计一次。

### 9.2 路由权威与广播

serve 是**本节点**路由与生命周期的权威:create/resume/pause/kill 实时更新路由,经
**routesync** 广播 Upsert/Delete 给所有 plugin 平面订阅者(external proxy master 与路由
观察者如平台 agent)。proxy master 把路由投影到共享内存,worker 只读;观察者持只读缓存
感知状态。广播逐条 upsert + 末尾 bookmark(高密度下发端内存有界)。线格式(帧化 JSON over h2c)、容错重同步、
`RouteEntry` 字段(含驱动迁移的 `snap_loc`、MMDS 使用的 `mmds_secret`)见 node-proxy.md §4;
plugin 平面的注册与鉴权见 §6。机群级路由权威是 registry(cluster.md);serve 经 node-link
把本节点沙箱事件上报 registry(§10),与本节点 plugin 平面的路由广播是两条正交通道。

广播前执行服务端投影:受信 MMDS proxy 的 Upsert 额外携 `mmds_routes` 与
`mmds_route_secret_values`,二者在同一 event 中原子替换;普通 observer 不收到这些字段,
cluster/node-link 也绝不收到 value。conductor-owned `mmds.services` 仅放在受信 proxy 的
Hello policy,不广播给 route observer。internal proxy 直接读 conductor 内存 registry;
external master 原子替换 registry,worker 经本机 MMDS RPC 取得已解析 Unix socket endpoint,
不读取 `proxy.yaml` 中的第二份配置。

- **auto-resume launch owner**:数据面打到 paused 沙箱触发 resume——internal 在请求内完成
  durable acceptance 并等待,external 经 routesync `Wake` 上行;同一 sid 的 Connect/Wake/
  exec activation 与其它 launch 入口共用唯一 attempt(§8)。
- **starting 投影**:初始 `starting,run_id=""` durable insert 后即广播,此时没有 FloatingIP
  或可用 endpoint;network ownership 已 CAS 持久化且 YAML/ready.sock 已准备后再广播 enriched
  starting,供 internal/external MMDS 完成 envd `/init`。starting 不开放普通数据面,也不触发
  Wake。launch 成功广播 running;create 失败广播 Delete,resume 失败广播 paused。
- **envd 鉴权姿态(`mmds.enabled`)**:该开关决定 create 是否给 envd 下发 token、proxy
  是否寄宿 MMDS 服务——`false` = envd 非 secure、proxy 单闸门(配置强制
  `proxy.auth=enforce`);`true` = proxy 组件内起 FC MMDS v2、经 `/init` re-key 每身份新
  token,使快照扇出沙箱数据面可用.姿态对比与 MMDS 两段式协议见 node-proxy.md §7.

### 9.3 proxyForwarder

数据面请求误达 serve 控制面监听口时(external 模式下客户端未分流到数据口),serve 经
已注册的 `proxy_socket` UDS 建立一次性 chained CONNECT,由 proxy worker 照常处理(含鉴权)。
链式请求显式复用 `E2b-Sandbox-Id`,可选 `E2b-Sandbox-Service`/
`E2b-Sandbox-Port` 和原始 `X-Access-Token`,不创造内部 token Header.普通 HTTP 在该隧道内
发送一条请求,CONNECT 则直接 splice 客户端与 worker;无 proxy 注册时回 502.
链式隧道与转发细节见 node-proxy.md §5.

## 10. 集群接入(node-link)

配 `cluster.node_link.endpoint`(§3)时,`node-ctl conductor serve` 拨 registry 把本节点接入集群,交由
`cluster-ctl registry/router/placer` 编排。node-link 复用 routesync 的帧化 JSON over h2c 引擎
(node-proxy.md §4),但角色相反:node 是本节点路由 / 构建权威,registry 是订阅者和命令下发方.

本节只讲 node 侧行为。registry 的 owner 选择、redirect/relay、shardkv 复制和 membership 变更由
[cluster.md](cluster.md) 定义。

```text
node-ctl conductor serve
  │ dial registry node_link endpoint
  │ register node profile
  │ stream heartbeat + sandbox/build events
  │ receive create/connect/exec_session/delete/key/build commands
  ▼
registry node_link owner or relay holder
```

集群下两条到节点的路径:

- **node-link**:注册、心跳、sandbox/build 事件、命令、manifest key 租约。
- **router 转发到本节点 e2b 控制面 / 数据面**:pause/kill/timeout、build status/files 转发本机 e2b
  控制面;数据面经 router 注入 `E2b-Sandbox-Id` + `X-Access-Token` 后进入本机 proxy(node-proxy.md)。

### 10.1 注册与 redirect

节点拨 registry 的 node_link endpoint 后,首帧发送:

```text
register{
  node_id,
  labels,
  capacity,
  build_capacity,
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
heartbeat{zone, allocated, pool, build_alloc, counts, draining}
```

沙箱水位取自资源控制器(node-resource.md),`build_alloc` 为本机在跑 / 预留构建占用,`draining` 由节点侧
资源 drain 或维护策略置位。普通 heartbeat 只更新 node_link profile 中的 liveness 和本地水位，不更新
node_list；首次注册和 draining 变化驱动低频目录投影。registry node owner 持有的当前连接是 placement
提交时唯一的存活判断。

### 10.3 sandbox/build 事件

节点作为权威上报本机执行态:

```text
sandbox{
  sid, profile, state(starting|running|paused|dead), snap_loc, template_id,
  auth_sandbox_id, api_secret, api_secret_fingerprint,
  manifest_key_fingerprint, service_secret,
  envd_access_token, traffic_access_token, forward_access_token,
  mmds_secret
}
build{build_id, state, template_id?, reason?}
delete{sid}
bookmark{full_sync}
```

该 node route event 保留既有 `mmds_secret` 字段供节点 proxy/MMDS 路径使用;cluster Registry
物化受保护 route 时不采纳该字段。starting 只表示 node-local launch 正在进行,Registry
将它计入 full-sync seen set 但不改写 reserved/paused route;running/paused/delete 才驱动
cluster route 状态收敛。节点事件按以上用途显式携带其余凭据。

node 不在 sandbox event 中自报 Registry-owned 的 cluster context。nodelink owner 在任务下发前已维护
本节点完整的 sandbox/build 归属表,收到事件后以 `(node_id,sid)` 或
`(node_id,build_id)` 查表取得 group/route_key。sandbox event 的 `sid` 是 node-local NodeSandboxID;集群下由
Registry 以 `<stable-sandbox-id>-g<N>` 分配.node 不接收 SandboxGeneration,不解析该 ID;稳定 SandboxID
和代际由 Registry 归属表恢复,不存在跨 group 的 SandboxID 索引.

全量 Range 结束的 bookmark 带 `full_sync=true`。nodelink owner 仅将本轮出现的 sid 与
订阅建立前捕获的本节点归属表基线比较;清理前再次确认当前表项仍与基线一致,避免删除
同步期间新下发或重新绑定的任务。增量 replay 的 bookmark 只推进 resume token。

### 10.4 命令受理

registry 上行下发命令.serve 复用既有 e2b 生命周期原语(§8 / §8.1)执行,
所有 sandbox 操作的 `sid` 均是精确 NodeSandboxID.普通命令受理后回 `cmd_ack`,
终态经 sandbox/build 事件上报。CmdCreate 只有在唯一 launch owner 已 claim、starting 行已
insert 且 cache/route starting 已发布后才 Ack;该 Ack 表示 durable acceptance,不表示 READY。
CmdConnect 在 Ack 前原子完成 paused→starting、清空旧 run/network ownership、提交最终 deadline
并 cache/publish;CmdExecSession 在 Ack 前完成可选 import、鉴权/签名及同一 resume acceptance。
三者随后均由共同 lifecycle root 异步 launch:

  | 命令 | 节点动作 |
  |---|---|
  | `create{cmd_id, sid, template_ref, profile, api_secret_fingerprint, config, cluster}` | 冷启 `template_ref` + 保存/合并 `config`(§8;snp 模板 = fresh Create 的快照恢复快启);完整 APISecret 指纹选择本机已安装的凭据对;`cluster={group,route_key,auth_sandbox_id?}` 与 profile 作为系统字段独立持久化;credentials namespace 在写业务行前解析并剥离;Ack 前已是 `starting,run_id=""` 且有 active attempt |
  | `connect{cmd_id, sid, profile, api_secret_fingerprint, cluster, migration_token?, timeout_seconds?}` | `sid` 是 NodeSandboxID.target 已存在时忽略 token,校验完整指纹、profile 和 cluster context;target 缺失且带 KMT1 时,先用本机 matching pair 同步校验并以命令 sid/context insert paused 行;再于 Ack 前完成 paused→starting、旧 network/run 清理与 deadline 持久化.Ack 携 `ConnectResult{NodeSandboxID,TemplateID,Profile,EnvdAccessToken,TrafficAccessToken,ForwardAccessToken}` 且不携 root/fingerprint;restore 异步,缺失且无 token 则拒绝 |
  | `exec_session{cmd_id, sid, profile, api_secret_fingerprint, cluster, ttl_seconds, migration_token?}` | 复用 connect 的精确目标,可选同步 import,profile/cluster context 和完整 APISecret 指纹校验;原始 API key 不进入 node-link.校验通过后生成 UUIDv7 session ID,以实际签发时间计算可选 `exp`,Ack 仅携 `ExecSessionResult{ExecAccessToken}`;随后异步 resume,不等待 READY |
  | `delete{cmd_id, sid, api_secret_fingerprint}` | 完整指纹必须与既有 Sandbox 业务行绑定一致,随后销毁沙箱(§5 kill) |
  | `key_put{api_secret_fingerprint, api_secret_type, api_secret?, api_secret_ref?, manifest_key_fingerprint, manifest_key_type, manifest_key?, manifest_key_ref?, expires_unix}` / `key_drop{api_secret_fingerprint}` | `key_put` 原子校验并写入 / 重发续租完整凭据对;两项指纹均为 64-hex SHA-256。`key_drop` 按完整 APISecret 指纹 best-effort 清理,正确性依赖 TTL 淘汰(§7);registry 的密钥分发见 cluster.md |
  | `build_register{build_id, template_id, profile, resources, image_repo, registry_auth, api_secret_fingerprint, config}` | 预配 registry 分配的构建(§12;`profile` 必填且只接受 e2b/bare;按完整 APISecret 指纹解析凭据对、建 build 记录、瞬态用镜像凭据);`config` metadata 原样保存,构建态经 `build_event` 上报 |

无 `drain` 命令。节点排空 / 维护由节点侧发起(node-resource.md §2.5 资源 drain 或本机维护策略),
集群侧只停止向其分配。

CmdCreate 的任一 Ack 后 launch failure 将 fresh starting 回滚为 dead 并发 Delete;
CmdConnect/ExecSession 的 resume failure 则回滚为 paused 并发 paused Upsert,不得误走 create
replacement Delete 分支。Registry 既有 replacement reservation rollback fence 保留:匹配本次
create 的 Delete 才恢复 Reserve 前旧 route,不能提前删除 reservation 丢失回滚依据。并发
cluster create/connect/delete 不能取得第二个 node-local launch owner。

每个 exec-session API 调用是独立授权,因此使用新 CmdID 并签发新 KAT;resume
可以继续按 SID 查找同一 launch attempt.`CmdID` 只关联当前 Command 与 Ack waiter,node 不持久化
command digest 或 typed result,Registry 也不在断线、超时或 node 重启后自动重投同一
`CmdID`.本次调用失败后,API 重试是新的 operation;Connect 重新执行可重试的目标校验/恢复,
Exec Session 可以签发新 KAT.

### 10.5 断线与安全

断线后节点指数退避重连并重注册,带 `resume_from=<rev>` 请求增量重放。registry/node 留存窗口内只补增量,
否则逐条全量 + bookmark。registry 重启亦然。

node-link 生产走 mTLS(`cluster.node_link.tls`)。下行 APISecret+ManifestKey 凭据对及创建期
credentials 只进入加密存储和运行期内存;沙箱级 token 属敏感数据,仅在可信链路内传输。

接入集群与本机 plugin 平面使用同一 routesync 引擎和线格式,仅订阅者 kind 不同。router 不订阅节点
plugin 平面,机群路由经 registry 聚合。

## 11. guest profile:envd 与工具链

- 单一 **`sandbox-runtime.bundle`** 由 `guest-runtime` 构建:把 `sandboxer` 产出的
  `sandbox-init` 打成 virtio-pmem/DAX runtime,并在 `/opt/sandbox-runtime/bin/`
  内置固定版本 `envd`、`flatten-ctl`、`mkfs.erofs`。runtime bundle 保持 raw EROFS
  从 offset 0 开始,随后是 zero padding 和只含空 `.kuasar.sha256.<hex>` marker 的
  ZIP;摘要覆盖 EROFS+padding,构建时一次生成,启动/恢复从 EOF 直接读取。成品总长
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
  `2026.22`,`ENVD_TARBALL` 可覆盖)→ `go build packages/envd`。`guest-runtime make
  sandbox-runtime` 负责把它和构建工具链一起注入 runtime。
- **userland 门槛**(对 base/客户镜像的约束):须有 `bash`、`coreutils`/`util-linux`、
  预建默认用户(默认 `user`,含 `/home/user`)、cgroup v2、可写 `/run`。envd 跑每条
  guest 命令以默认用户、并包一层 `ionice -c 2 -n 4 nice -n N "$@"`——缺用户或缺
  util-linux/coreutils 会报 `invalid default user` / `ionice: not found`(裸 alpine
  两者皆缺)。
- **guest 须有 `/etc/hosts`**:展平的 docker 镜像不带它(docker 仅在容器运行时注入),
  而 guest 内 `socket.getfqdn(hostname)` 类调用(许多服务器在 bind 后、listen 前调它,
  如 Python `http.server.server_bind`)查无本地条目即落到 DNS,解析主机名阻塞约 20s,
  表象是"host→floatingip 应用端口转发失败"。serve 经 SANDBOX_CONFIG 既有
  `files:` 机制注入 `/etc/hosts`(`127.0.1.1 <hostname>` 条目)与 `/etc/resolv.conf`
  (`sandbox.network.dns`),并经 `network.hostname` sethostname;launch 与 restore
  均生效。
- host 在 runtime readiness wire 完成后直接调 mandatory **`POST /init`**(经 UDS):置
  `envVars`、默认用户 `user`/workdir `/home/user`,时间戳;仅 `mmds.enabled` 时携带
  `accessToken`(node-proxy.md §7).envd socket 尚未可拨由上述短退避传输重试吸收,
  无启动期 `/health` 探测.

## 12. 模板构建(三阶段流水线,构建在沙箱内进行)

构建经 e2b API 提交(端点见 §4.2;无独立构建 CLI),落 `builds` 表,由资源池调度,
每次执行绑定一个 `sandbox-builder@<run-id>` 单元。**镜像拉取与 step 执行都发生在构建沙箱
(microVM)内**——租户的网络流量与镜像内容不触宿主用户态,宿主侧只做工件接力与
收尾上传。

**serve 侧(每构建一次)**:建 workdir → 配 `tapfd_socket` 时经 `TAPFD/1 PREPARE`、否则经
`connector-ctl vswitch attach` 分配一个网络槽(整个构建复用,各阶段顺序交接 tapfd)→
铸 envd token → 从 builder pool 分配并持久化 run-id(无 idle 时按需 `StartUnit`)→
(`mmds.enabled` 时)挂一行 synthetic sandbox route,让模板阶段 FC 模式的 envd 能按
floatingip 自解析,并把 Register 时声明的 MMDS routes 及 build-owner initial values
投影给本次 builder guest → run-builder WaitAssignment 取得 bid 后执行流水线
→ 经 config-socket 回传结果 → 终态落库:产物为快照 ⇒ `kind=snp`、为镜像 ⇒
`img`,持久 id `<profile>-<kind>-<base64url(portable-ref)>` 写入 names/aliases。profile 从注册到
BuildSpec 全链路显式携带。register/trigger 在入队前校验最终 network metadata;
执行时只解析一次并补齐 profile/node 默认值,同一个 `NetworkSpec` 同时派生 host
`vswitch.AttachReq` 与 guest `BuildNet`。`transit_*` 只在 host Attach 消费,不进入
`BuildNet`;无 transit 时保持零值。

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
  stdio 流回宿主 `workdir/image.img`;若 lookup 已确认 registry 支持 Referrers 且
  `writeback=true`,宿主立即 `manifest-ctl store image.img` 得到 manifest id 并把 base
  改为 `manifest://<id>`,随后在 guest 内 `flatten-ctl referer put --owner <owner>
  --manifest-id <id> <subject>` 回写。writeback 失败即构建失败;禁用 writeback 时不做
  这次中间上传。`MANIFEST_KEY` 只在宿主用于计算 owner token,不进入 guest referer 命令。
- **B steps**(有 steps):以 base 镜像为 root(本地工件或 `manifest://`)+ builder
  runtime + 大可写 upper(同一 diff_template),**envd 为 app**(构建工具姿态:恒
  `-isnotfc`、不 `/init`、无 token;唯一盘足迹 `/run/e2b` 落在 tmpfs 挂载上,导出
  排除)。`RUN` **经 envd `process.Start`** 逐条执行——与 e2b 自家构建同形:
  `/bin/bash -l -c <cmd>`、按操作用户经 `Authorization: Basic`、`Connect-Timeout-Ms`
  带 step 预算(guest 侧也会到点杀)——上下文为宿主累积的 ENV/WORKDIR/USER,**初值
  灌自 base 镜像 config**(RUN 所见与 docker build 一致;`ARG` 仅做 `${k}` 替换,
  不入镜像;**bash 因此是带 steps/startCmd 构建的镜像契约**,e2b 同款)。步完后宿主
  把累积上下文叠回 base config 写回 guest,`flatten-ctl mountpoint /.kuasar-build`
  (自绑挂载点)+ `export --skip-mounts --runtime-config … --tmpdir /.kuasar-build
  --output - /` 导出新镜像工件流回——挂载点自身与 `/opt/sandbox-runtime` 投影都是
  挂载,被 `--skip-mounts` 排除,导出不自吞、工具链不进镜像。
- **C template**(仅 e2b,有 startCmd,显式或自 base 模板继承):**生产 e2b runtime** 冷启
  最终镜像(runtime_ref 冻入快照——模板的子沙箱不得继承构建工具链),envd 为 app
  (MMDS 姿态随部署,node-proxy.md §7),`/init` 预置(此后 RPC 携 `X-Access-Token`).startCmd
  **经 envd 启动**(e2b 默认身份 `user`、`/home/user`),流挂至就绪后断开——envd
  不因断流杀进程(其源码明言进程上下文与请求解耦),进程以 **envd 管理进程**身份
  冻入快照,扇出沙箱里 SDK 可 list/connect;readyCmd 以 2s 间隔轮询至成功(预算
  `ready_timeout_sec`;缺省时同 e2b:`sleep 20`),就绪后先断 startCmd 流(已败则
  构建失败)再 `sandbox-ctl snapshot --output` 出本地快照 bundle。

**两类 guest 信道,刻意分离**:e2b 语义命令(steps/startCmd/readyCmd)走 envd,
与 e2b 自家模板构建逐项同形;平台机制(flatten-ctl 拉取/导出、运行时配置注入、
工件流回)走 `sandbox-ctl exec`——任意 rootfs 可用、裸 stdio 接力,
不依赖镜像 userland。

**fromTemplate**:base 来自既有模板——img 模板直接用 canonical image ref;snp 模板经
`sandbox-ctl info --json <portable-ref>` 读 snapshot.cfg 取 `base_ref`,并继承
metadata 里的 `e2b.start_cmd`/`e2b.ready_cmd`(请求显式给出者优先)。fromTemplate
与 fromImage 互斥;fromTemplate 且无 steps 无 startCmd 拒绝(无事可做)。bare 不继承
e2b start/ready metadata,也不进入 C 阶段,只上传 image 产物。overlay top 与
`base_from_refs` 保持显式 top-to-bottom 数组,不编码复合 manifest ref。snp 源模板的
`kuasar-sandbox.network` 在 host Attach 前读取并按字段继承,优先级为
**当前 Build 显式 NetworkSpec > 源 snapshot NetworkSpec > 当前 profile/node 默认值**;
img 源模板没有 snapshot metadata 通道,不从本地数据库增加入口相关的隐式回退。

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
唯一 aws-sdk 落点;本地/单机无云对象存储时指向 versitygw(`guest-runtime/native-deps make
versitygw`)。force_path_style 默认 false(虚拟主机式;versitygw/minio 置 true)。

**收尾发布(平台凭据唯一出现点)**:img-only(包括所有 bare build) ⇒ `manifest-ctl store image.img`
(stdout 的 key 转为 canonical manifest ref;若 import referer 已命中则直接复用);
产出快照 ⇒ **一条** `sandbox-ctl upload-snapshot <bundle>`。配置
`ref_location_parent` 时使用 build ID 作为 location name 发布到 named location,
否则发布到 manifest。结果
`{image_ref|snapshot_ref, start_cmd, ready_cmd, error}` 经 config-socket 回传;
快照模板的 start/ready 与模板有效 `NetworkSpec` 同时记进 snapshot.cfg metadata,
模板自描述(fromTemplate 继承与 create 都读它);未显式声明 hostname 时这里记录正常
sandbox 默认值,绝不记录 `build-<id>`。

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
推出 `<mask>/{templateID}:{buildID}`——掩码须与 CLI 侧 `E2B_IMAGE_URI_MASK` 一致,
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

**资源池**:`builder.max_concurrent`(默认 2)= serve 内计数信号量准入;
`waiting → building` 用 builds 表 CAS 抢占(重启/多实例安全);CPU/内存上限经
`sandbox-builder.slice` 的 `CPUQuota`/`MemoryMax` 施加(整条流水线都在单元 cgroup
内,上限对阶段 VM 生效)。

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
`MANIFEST_KEY` 永不入 guest**。凭据仅加密存库 + 运行期 env;workdir 里的阶段 yaml
无秘密。

**不支持**:多源 COPY(e2b executor 亦只取 src+dst)、step 级缓存(`force` 字段
接受但忽略,总是全量执行)、服务端 Dockerfile 解析(CLI 已在客户端展开为 steps)。

## 13. DNS / TLS

生产:`*.<domain>` + `api.<domain>` 通配 DNS + TLS(operator 提供,on-prem/离线
友好)。控制面与数据面可同口(`api.listen`)或分口(`proxy.data_listen` /
external worker 的 `data_listen`,proxy.yaml),证书同一张。dev:`E2B_API_URL`/
`E2B_SANDBOX_URL` 指向明文 http(h2c),无需证书与通配 DNS。集群下 cluster-ctl router
持对外通配证书,node-link 用独立的 `cluster.node_link.tls` mTLS(§10)。

## 14. 契约边界

| 对象 | 方式 | 说明 |
|---|---|---|
| `sandbox-ctl`(runtime) | 经 run-sandbox(单元)`execve`:`run --ready-fd=<fd> --cgroup-path=fd=<vmm-fd> --config <sid>.yaml --manifest-config … --run-root … [--restore] [--connect]`;run-builder 以直接子进程 `run --ready-fd=<pipe-fd>` 启动阶段沙箱,经 `exec --env/--stdin-from/--stdout-to` 做平台接力(flatten-ctl 调用、配置注入、工件流),收尾 `snapshot --output` / `upload-snapshot` / `info --json`;serve 调 `snapshot --upload`(pause) | readiness wire 固定为 `control_ready`→`ready`→EOF;runner 的 VMM cgroup FD 仅由 node-ctl 本地注入;非密配置文件 + 密钥 env;资源准入在其内部;e2b 语义命令不走它(走 envd,§12) |
| 资源控制器(node-resource.md) | serve 内置(`resource_listen`,调参内联);沙箱经 `sandbox.resources.control_socket` 拨号(`pkg/resource` 协议) | 每个 runner 的 `vmm/` 是沙箱资源 cgroup;不配 control_socket = 静态 cgroup,配了才进 SANDBOX_CONFIG `resources.control.controller` |
| registry(cluster-ctl) | node-link:serve 拨 registry、反向注册为路由权威,上报 register/heartbeat/sandbox/build_event 事件、受理 create/connect/delete/key_put/key_drop/build_register 命令(§10、cluster.md) | mTLS;cluster kill 走 node-link delete 命令;空 `cluster.node_link.endpoint` = 独立模式不接入 |
| `connector-ctl vswitch`(vswitch) | 不配 `tapfd_socket` 时经 CLI:`attach <switch> --inner-ip [--transit-*]` / `detach --port`;配 `tapfd_socket` 时经常驻 `TAPFD/1 PREPARE` / `OPEN` / `RELEASE`;sandbox 配置仍渲染为 `network.tapfd.socket/request` | 交换机预先起好(`connector-ctl vswitch start/serve`,内核态数据面);port 对外、slot 内部;一个构建复用一个槽 |
| `flatten-ctl`(builder) | **guest 内**(guest runtime 自带,经 sandbox-ctl exec 驱动):`export --output -`(import 拉取 / steps 导出)、`mountpoint`;宿主侧:`info --json`(读镜像运行时配置,本地工件或 manifest://) | 租户 `FLATTEN_*` 仅经 exec env 入 guest;tarstream 镜像工件经 exec stdio 接力 |
| `manifest-ctl`(accelerator) | `store <image.img>`(img-only 构建的收尾上传) | manifest key 经 stdout 回收;`MANIFEST_KEY` 经 env |
| `mkfs.erofs`(deps) | guest-runtime `make sandbox-runtime` 与 guest 内 `flatten-ctl` 后端 | 确定性打包 runtime;构建沙箱内导出 EROFS 镜像(§11、§12) |
| guest envd | UDS(sandbox-ctl `--connect` 映射);构建流水线另以最小 connect+JSON 客户端调 `process.Start`(steps/startCmd/readyCmd,§12) | 原版不改;协议 pin 见 §4.3/§4.5 |
| systemd | D-Bus:StartUnit/StopUnit/ResetFailed/ListUnitsByPatterns/Reload | 进程管理 + 单元自装(§5) |
| `node-ctl proxy`(external) | UDS routesync(双向 h2c 帧化 JSON)+ 兜底反代 | 同节点,运维带外起;proxy master 注册一次,worker 共享继承 listener fd + shm 路由视图;`proxy.yaml` 必填 `paths.run_root` 以供 worker 本地定位 native exec `ctl.sock`(node-proxy.md §3/§4) |

不新增导出包;`CGO_ENABLED=0`;依赖层级 = 叶子。

## 15. 可靠性

### 15.1 状态存储(sqlite)

单文件 sqlite(`paths.db_path`,WAL,文件 0600),纯 Go 驱动。核心表:

```
sandboxes      id(node-local SandboxID,1..57 bytes DNS-label subset) PK,
               profile, cluster_group, cluster_route_key, auth_sandbox_id,
               template_id, state(starting|running|paused|dead), deadline_unix,
               run_dir, base_dir, envd_uds, ci_uds, floatingip, vswitch_port,
               inner_ip, port_mac, api_secret_hash, api_secret_enc,
               manifest_key_hash, manifest_key_enc, snapshot_ref,
               service_secret_enc, envd_access_token_enc, traffic_access_token_enc,
               forward_access_token_enc, metadata_json, env_json,
               created_unix
builds         build_id PK, template_id(transient-…), persist_id(<profile>-<kind>-<base64url-ref>),
               api_secret_hash, api_secret_enc, manifest_key_hash, manifest_key_enc,
               profile, kind, from_image,
               start_cmd, status(registered|waiting|building|ready|error), reason,
               names_json, aliases_json, registry_auth_enc, created_unix
sandbox_mmds_route_secret_values
               sandbox_id PK/FK sandboxes(id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
build_mmds_route_secret_values
               build_id PK/FK builds(build_id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
manifest_keys  api_secret_hash PK, api_secret_enc, manifest_key_hash,
               manifest_key_enc, label, created_unix, expires_unix, registry_auth_enc
```

`builds` 兼任模板登记(§4.4);`*_enc` 根凭据及 Sandbox service credential 均
AES-256-GCM、两项 `*_hash` 均为
完整 SHA-256。`substr(api_secret_hash,1,24)` 仅建候选预筛索引(§7)。
两张 MMDS value 表每 owner 最多一行,只保存 secretbox ciphertext;AAD 与事务/CAS/cleanup
语义见 §7。既有数据库在启动时用 `CREATE TABLE IF NOT EXISTS` 原地增加两表,无需
plaintext 回填或兼容双写。

### 15.2 重启对账

serve 在开放 API、routesync、node-link 和数据面前先以
`ListUnitsByPatterns("sandbox-runner@*.service")` 对账:

- 库内 starting 不收养为 running,也不装入 cache。`run_id=""` 表示进程中断于 runner
  assignment 前;非空则先 Stop/Reset exact runner。两种情况都 detach 已持久化的新 network
  ownership、清理 stale ready.sock/run dir;有 `snapshot_ref` 表示被中断的 resume,按 exact
  空/非空 run-id CAS 回 paused 并保留 base/snapshot identity,否则是被中断的 fresh create,
  按同一 fence CAS 为 dead 并清理其 base dir。任一 Stop/Reset/detach/目录 cleanup 失败时
  Reconcile 直接使节点启动失败并保留原 starting ownership,不得先清字段或开放 API;
  resume 回 paused 后在本次 conductor 进程内把 durable deadline 保守恢复为显式 intent,
  直到下次 exact-run 成功;paused 后再次重启的持久 discriminator 由 #139 跟踪;
- 单元 active/activating 且库内 running ⇒ **收养**(重挂内存路由、TTL 继续生效,
  external 模式随快照重新推给 worker;集群下经 node-link 重报);
- 库内 running 但无对应活单元 ⇒ 清理(StopUnit/detach/删运行目录)并标 `dead`;
- 无 running 行对应的 runner 单元属于上一个 pool 的 idle/orphan run-id ⇒
  `StopUnit` + `ResetFailedUnit`,随后由新 pool 按配置补足;
- `run_root` 为 tmpfs ⇒ 整机重启后 running 全部判 dead;`paused` 行与 snp 模板保留,
  可被 connect/auto-resume 重新拉起(本机快照存于磁盘 `checkpoint.local_dir`)。

因此 RouteSource.Range 与后续全量同步不会看到遗留 starting 被误发布为 running;初始 starting
已经持久化 network 但尚未分配 runner 的 crash 也能确定性释放端口并收敛到 dead/paused。

### 15.3 故障域

| 故障 | 影响 | 自愈 |
|---|---|---|
| serve 崩溃/重启 | 控制面与 internal 数据面中断;沙箱(microVM/单元)不受影响 | systemd 重启 → 重启对账收养;external proxy master 仍可用共享路由视图服务 running 流量(Wake 无人应答,paused 唤醒挂起至超时);集群下 node-link 重连重报 |
| proxy worker 崩溃(external) | 该 worker 上的连接断;其余 worker 继续接新连接 | proxy master 重启该 worker;worker 重新读取共享路由视图 |
| proxy master 崩溃(external) | external 数据面中断,plugin 租约断开 | systemd 重启 master → 重新注册、重建共享表、启动 worker |
| runner 单元/CH 崩溃 | 该沙箱死(`Restart=no`,有状态不重试) | 对账标 dead;客户重新 create(或从 paused 快照 resume) |
| routesync 断流 | external 固定数据面视图停更;MMDS route/value/service 立即不可用 | proxy master 清空 confidential heap 并指数退避重连重注册,完整同步 bookmark 后才重新服务(node-proxy.md §4) |
| node-link 断流(集群) | registry 暂失本节点视图 | 节点指数退避重连重注册重报沙箱集(§10、cluster.md);本节点沙箱不受影响 |
| sqlite 损坏 | 控制面不可用 | 文件级备份/重建;沙箱单元仍可被 ListUnits 发现并由运维处置 |

## 16. 测试

单元测试:`make test`(MMDS strict parser/top-level merge/minimal persistence、route value
encrypted owner blob/AAD/CAS/cleanup、admin UDS、service relay、routesync confidential projection、
external master/worker resync/rotation;handler 路由、apikey/secretbox/regcreds、routesync(注册/bookmark
往返)/proxyshm(共享路由表、park/wake、世代清扫)、plugin 注册表(同 id 顶替)、proxy CONNECT 隧道 +
proxyForwarder 链式 relay,Exec KAT/64 KiB API/CmdExecSession,H1/H2 `ctl.ProxyExec` gate 与
buffered half-close tunnel,mmds(确定性密钥),launch ownership,沙箱配置注入(命名空间解析/容量折叠/网络合并),
migrate,node-link(注册/事件/命令往返)等).

Native exec 的真实 microVM 特性用例分别覆盖 standalone 和 cluster 路径的
ExecAccessToken 签发,`service=exec` CONNECT 以及 guest 命令执行.该结论只对上述
native exec 路径负责,不表示同一聚合脚本的后续 pause/resume 等其它阶段已一并验收.

编排特性的 E2E 与实现一起维护在 `orchestrator/test/e2e/`。轻量 cluster stub 与需要
vmlinux、cloud-hypervisor、mkfs.erofs、sandbox-runtime.bundle 等多仓制品的真实 microVM
用例使用同一个 `run_all.sh`。本仓直接运行时通过 `BIN` 指向 platform 组装的二进制目录;
组件 PR 的 BMS 则把候选仓与其余仓源码组成统一环境后执行该入口。缺少重型前置时单脚本可
跳过,完整门禁设置 `REQUIRE_*=1` 后硬失败。

| 脚本 | 覆盖 |
|---|---|
| `e2e_orchestrator.sh` | 单元自动安装 + 控制面(`/health`、401 路径)+ 构建 API 生命周期(register/trigger/status、跨 key 归属 404)+(有 KVM 时)bare create/list/kill |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器(纯用户态,无 root/systemd/KVM):pidfile 锁/双起拒绝、HTTP 取 LaunchSpec、execve、`TASK_*` 剥除;`config` CLI 往返 |
| `e2e_run_builder.sh` | 三阶段构建流水线(KVM + vswitch + store-ctl + zot,guest 经 mgmt VIP 拉取):fromImage/fromTemplate/COPY/bare 链;Build Register MMDS、终态 cleanup、日志/DB/image 隔离 |
| `e2e_execute.sh` | 从已建模板冷启真实 microVM、guest 内 exec、local Pause→resume 与恢复策略 |
| `e2e_mmds_routes_internal.sh` | 复用 execute 拓扑覆盖 internal static、secret 生命周期与 local UDS service |
| `e2e_mmds_routes_external.sh` | 复用 external proxy 拓扑覆盖同一合同及 full resync/fail-closed 恢复 |
| `e2e_orchestrator_proxy.sh` | external proxy master/worker、路由同步、数据面鉴权、auto-resume 与 CONNECT relay |
| `e2e_cluster_stub.sh` | 真实 registry/router/placer + node-stub-ctl,覆盖 node-link、Reserve、路由、稳定 SandboxID、ExecSession 与成员变更 |
| `e2e_cluster_real.sh` | 真实 cluster 控制面、node-ctl 与 microVM,覆盖单 registry 和 node-link redirect |
| `e2e_density.sh` | 节点资源准入、回收与密度行为 |
| `e2e_sandbox_cold_target.sh` | node-ctl 资源控制器驱动 production-shaped sandbox target 冷启动 |

`make test-e2e` 即执行 `test/e2e/run_all.sh`;platform 只提供统一环境、聚合入口及真正跨组件
组合本身的用例,不复制上述脚本。

## 17. See Also

- [node-proxy.md](node-proxy.md) —— 数据面转发层:路由判定 / 部署模式(internal/external/off)/
  routesync / 数据面鉴权 / MMDS / CONNECT 隧道(本文 §9 装配的转发层实现,集群下 router 转发进入)
- [node-resource.md](node-resource.md) —— 节点资源控制协议(`sandbox.resources.control_socket`
  的对端)与控制器内部组织(serve 经 `resource_listen` 内置,调参内联)
- [cluster.md](cluster.md) —— 集群控制面:node-link 线格式(§6,本文 §10 的对端),注册表,
  Reserve 状态机;[cluster-router.md](cluster-router.md) 数据面入口,[cluster-placer.md](cluster-placer.md)
  放置与密钥分发
- `sandboxer/docs/sandbox.md` —— sandbox-ctl:SANDBOX_CONFIG 模式、
  run/snapshot/restore/connect 原语、cgroup 模型
- `connector/docs/vswitch.md` —— attach/detach/open-port、floatingip 与
  mgmt-service(MMDS VIP 转换)语义
- `guest-runtime/docs/flatten.md` —— flatten-ctl export 与 OCI Referrers 幂等流、
  `FLATTEN_REGISTRY_*`
- `accelerator/docs/manifest.md` —— manifest 内容键、收敛加密与去重域
  (§7 的存储侧)
- `platform/docs/deployment.md` —— 节点部署拓扑中本组件的位置与单元安装
- `platform/test/demo/DEMO.md` —— e2b CLI/SDK 全流程演示
