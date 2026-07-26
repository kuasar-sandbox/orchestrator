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
跑原版 envd(由单一 `sandbox-runtime.erofs` 内置,§11),serve 经 UDS
反代其单端口协议(49983/Connect-RPC)。租户根凭据是 **APISecret + ManifestKey**:
APISecret 仅用于签发/验证 api_key,ManifestKey 保护 manifest 内容和镜像拉取令牌;
APISecret 缺省时由 ManifestKey 通过固定 KDF 派生。两者成对加密存储,永不落明文盘(§7)。

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
   config-socket 下发 assignment;分配后单元 cgroup 即沙箱/构建资源 cgroup
   (`--cgroup-adopt`,§5.1),`StopUnit` 即完整回收;serve 不自己当进程监督者。
4. **密钥不落明文盘**:租户 manifest_key 库内 AES-256-GCM 加密,运行期只经内存与
   启动器 LaunchSpec 的 env 帧传递(§6、§7);集群下经 node-link 下行的 manifest_key
   同样仅加密落盘(§10)。
5. **本节点路由权威,数据面可外置,集群可接入**:serve 是**本节点**路由与生命周期的
   权威;数据面 proxy 可内置(单二进制)或外置为独立 worker(§9)。接入集群时,机群级
   路由权威是 registry——serve 经 node-link 上报沙箱事件、受理集群命令(§10),不与
   集群争路由权威。
6. **重启可对账**:状态在 sqlite + systemd 单元集,serve 重启后以单元集为
   存活权威对账收养/清理(§15);集群下另经 node-link 重连重报本节点沙箱集让 registry 收敛。

### 1.3 两类沙箱(profile)

| profile | 是什么 | 数据面 | 对外服务 |
|---|---|---|---|
| **e2b** | guest 内跑原版 envd,agent 经其 exec / 读写文件 / 跑代码 | 支持(fs/process/pty/runCode,经 envd 代理) | envd + floatingip 用户端口 |
| **bare** | 把客户镜像当网络化 microVM 跑起来,无 envd | 不支持(控制端口回 501) | 仅 floatingip 网络 |

`bare` 直接复用基础沙箱运行时(`sandbox-runtime.erofs`),把基础沙箱接上北向 API;
经 e2b API 构建的模板恒为 **e2b** profile。profile 编码在 templateID 前缀里(§4.4),
运行期据此选 runtime erofs 与数据通路。

### 1.4 边界与依赖

- 北向客户:e2b SDK / CLI 直连;集群下经 cluster-ctl router 转发(数据面)+ node-link
  下发命令(控制),平台管理面亦可经 e2b API 对接。
- 既可**独立运行**也可**接入集群**:控制面(create/pause/kill/模板构建)始终在本节点;
  接入集群仅多一条 node-link(§10),不改 e2b 契约。
- 不实现 envd 协议:数据面只透传到 guest 内原版 envd(§4.3)。
- 不提供 sandbox metrics 端点(e2b API 的 `/sandboxes/{id}/metrics` 面)。
- 构建不支持 server 端执行 Dockerfile steps:服务端只对一个已存在的镜像引用做拉取 +
  展平(§12)。
- 节点本地:路由、存储、单元管理都是节点本地的;跨机协作经远程 manifest store 携带
  快照/模板(§8.1)与 cluster-ctl 的 node-link 编排(§10)。
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
      │   sandboxes create/connect/pause/...  │   │ (internal /   │ │      sandbox events
      │   templates register/trigger/status   │   │  external     │ │  down: create/connect/
      │ host:                                 │   │  workers,     │ │       delete/key_put/key_drop
      │   vswitch PREPARE/attach → floatingip           │   │  route-synced)│ │
      │   assign sandbox-runner@<run-id>      │   │ 49983/49999   │ └─ node-link client ───┐
      │   envd /init → sqlite + TTL           │   │  → UDS (envd) │                        │
      │ build: builds table → pool →          │   │ other ports → │◄── cluster-ctl router  │
      │   assign sandbox-builder@<run-id>     │   │  floatingip   │    forwards data plane  │
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

create 流程:建 `<run_root>/<sid>/`(tmpfs)+ `<base_root>/<sid>/`(disk)→
配 `tapfd_socket` 时经 `TAPFD/1 PREPARE`、否则经 `connector-ctl vswitch attach` 拿
`{port, floatingip, mac}` → 写非密配置 `<sid>.yaml`
→ 从 runner pool 分配一个 run-id(无 idle 时按需 `StartUnit(sandbox-runner@<run-id>)`)
→ 持久化 `sid ↔ run-id` → 单元内 `run-sandbox` 经 config-socket 的
WaitAssignment 取得 sid,再取 LaunchSpec(密钥经 env)后 `execve` 成 `sandbox-ctl run`
→ 起 microVM → (e2b)等 envd `/health` 就绪(60s 上限)→ `POST /init` 置 env/默认用户
→ 起 TTL。集群下,该 create 由 node-link 的 `create` 命令触发;profile、group、route-key
和可选认证主体通过结构化系统上下文下发并独立持久化。事件只回报 profile 和 node-owned
执行事实,registry 从既有节点归属记录恢复其 cluster identity(§10、§4.6)。

数据面按 `Host`(`<port>-<sid>.<domain>`)或 `E2b-Sandbox-Id`/`E2b-Sandbox-Port` 头解析
`(sid, port)`,校验 `X-Access-Token` 后转发:e2b profile 的 49983/49999 拨 sandbox-ctl
`--connect` 暴露的 host UDS 直达 envd;其余任意端口拨 `floatingip:port`。对 paused
沙箱的请求触发自动 resume(单飞合并,§8)。部署形态(internal/external/off)见 §9.1,
转发层设计见 [node-proxy.md](node-proxy.md)。集群下,数据面由 cluster-ctl router 经
注入 `E2b-Sandbox-Id` + `X-Access-Token` 转发进本节点 proxy,节点侧零改动(cluster-router.md)。

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

策略与端点(`config_socket`/`data_listen`/`proxy_netns`/`proxy_socket`/`shm_path`/`workers`/`tls`/
`auth`/`park_timeout`/`mmds_listen`)在 `proxy.yaml`;master 在 plugin 平面注册一次,
维护共享路由视图并把 listener fd 传给 worker。部署模式与拓扑见
node-proxy.md §2、§5——转发层自成一文,本仓控制面只在 §9 讲如何按 `proxy.mode` 装配它。

### 2.4 `node-ctl run-sandbox` / `run-builder`

systemd 单元的 ExecStart,非给人用。共用的进入骨架:`--run-id` 是 systemd 实例名,
`--pidfile` 指向 `<run_root>/runs/<run-id>.pid`,以 `fcntl(F_SETLK)` 排他锁防重入并写本
PID → 拨 `--config-socket` WaitAssignment 取得业务 id(§6)。之后两者分道:

- **run-sandbox**:取得 sid 后锁 `<run_root>/<sid>/<sid>.pid`,取 LaunchSpec →
  `chdir(workdir)`、剥除 `TASK_*` 引导变量、合入
  `spec.env`(密钥)→ `execve` 替换为 `sandbox-ctl run`,目标继承本 PID 与单元
  cgroup(锁 fd 已清 `FD_CLOEXEC`,随 execve 存活)。
- **run-builder**:取得 bid 后锁 `<run_root>/<bid>/<bid>.pid`,取 BuildSpec →
  **驻留**驱动三阶段构建流水线(§12):各阶段沙箱(`sandbox-ctl run`)是它的直接子进程,
  整个构建计入本单元 cgroup;结束把结果
  `{image_key|snapshot_key, start_cmd, ready_cmd, error}` 经 config-socket 回传。

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
| `proxy.mode` | `internal` | 数据面承载:`internal`/`external`/`off`(装配见 §9.1,部署模式见 node-proxy.md §5) |
| `proxy.data_listen` | 空 | internal 模式专用数据面监听;空 = 与 `api.listen` 共口。external 模式数据口在 worker 的 `proxy.yaml`(serve 不绑) |
| `proxy.proxy_netns` | 空 | internal 模式转发平面 netns:proxy 到 `floatingip:port` 的 TCP dial 与 `mmds.listen` 绑定都在该 netns;external 模式在 `proxy.yaml` 配同名字段 |
| `proxy.park_timeout` | `30s` | 数据面请求挂起预算:等路由同步 / paused 沙箱 resume 的上限(node-proxy.md §5) |
| `proxy.auth` | `enforce` | 数据面鉴权:`off`/`log`/`enforce`,校验 `X-Access-Token`(node-proxy.md §7) |
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
| `sandbox.restore.file_refs` | `verify` | restore 时本地 `file://` runtime/base 引用校验策略:`verify` 重算 SHA256 并比对 snapshot.cfg;`trust` 只校验协议、basename 和文件存在,由 LaunchSpec 传给 `sandbox-ctl --restore-file-refs trust`,仅适合受信本地性能模式 |
| `sandbox.boot.kernel` | – | vmlinux 路径 |
| `sandbox.boot.runtime` | – | 单一 guest runtime erofs;内置 envd、flatten-ctl、mkfs.erofs(§11) |
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
| `checkpoint.mode` | `local` | 暂停态落地:`local` = 本机文件(节点绑定)/ `remote` = 远程 manifest(可移植 = 模板)(§8.1) |
| `checkpoint.local_dir` | `/var/lib/sandbox-saved` | 本机快照目录(`mode=local`) |
| `mmds.enabled` | `false` | envd 鉴权姿态开关(§9.2、node-proxy.md §8):false = `-isnotfc` + proxy 单闸门;true = FC 模式 + MMDS re-key |
| `mmds.listen` | `127.0.0.1:19254` | MMDS 监听地址(vswitch `--mgmt-service` 的转换目标) |
| `cluster.node_link.endpoint` | 空 | registry 的 node_link 地址(§10);空 = 独立模式,不接入集群 |
| `cluster.node_link.tls` | 空 | node_link mTLS 证书 / key / CA(`{cert,key,ca}`;生产必配,§10 / cluster.md) |
| `cluster.node_id` | (接入集群必填) | 本节点唯一标识(node-link 注册,cluster.md) |
| `cluster.labels` | 空 | 节点标签 `{zone,pool,slot,node}`(placer nodeSelectors 匹配,cluster-placer.md) |
| `cluster.data_endpoint` | 空 | 本节点数据面端点(供 router 转发);缺省由 `api.domain` + `proxy`/`api` 监听推导 |
| `resource_listen` | 缺省(不内置) | 内置资源控制器整块(调参内联,无独立文件):`enabled` 开关、`socket`(控制器 UDS,**唯一权威**;空 = `pkg/resource` 默认,与 sandbox-ctl 一致),其余 `state_path`/`audit_path`/`cgroup_scan_paths`/`resources`/`watermarks`/`rate_limits`/`admission`/`dampening` 均有默认(语义见 node-resource.md §3.2);整块省略或 `enabled: false` = 不内置(沙箱用静态 cgroup) |

远程内存 Prefetch 没有节点统一开关。`sandbox.restore.file_refs` 是节点运营者控制的
本地文件信任策略;是否请求 Prefetch 由每个 sandbox 的 `kuasar-sandbox.restore`
命名空间决定(§4.6),二者不能互相覆盖。

配置自洽校验:`mmds.enabled=false` 时 `proxy.auth` 必须为 `enforce`(envd 非 secure,
proxy 是唯一数据面闸门);`mmds.enabled=true` 时 `proxy.mode` 不得为 `off`(MMDS 寄宿
proxy 组件)。`proxy.proxy_netns` 仅在 internal 模式有效;external 模式在 `proxy.yaml`
配置 `proxy_netns`。external 模式无须静态 worker 列表——worker 自行经 plugin 平面注册,proxyForwarder
按活跃注册集转发(§9.1、§9.3)。配 `cluster.node_link.endpoint` 时 `cluster.node_id` 必填;配
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
| create | `POST /sandboxes` → 201 | body `{templateID, timeout, metadata, envVars}` + 可选 `X-Kuasar-Sandbox-*` Header(timeout 缺省取 `sandbox.timeout_sec`,默认 300s);e2b 回 Envd/Traffic/Forward token,bare 只回 Forward token |
| get | `GET /sandboxes/{id}` | 附 `state`/`startedAt`/`endAt`/`metadata` |
| list | `GET /v2/sandboxes` | 仅本租户;query `state`/`limit`/`nextToken`,分页头 `x-next-token`;每项含 `cpuCount`/`memoryMB`/`diskSizeMB`(节点统一配置值 + overlay 模板尺寸)与 ISO-8601 `startedAt`/`endAt` |
| kill | `DELETE /sandboxes/{id}` → 204 | 非本租户 ⇒ 404 |
| resume | `POST /sandboxes/{id}/connect` | e2b 语义:resume 走 `/connect`;body `{timeout:秒}` 顺带续期;目标缺失时可携 `X-Kuasar-Migration-Token` 同步 import,随后接受异步 resume 并返回(§8.1);目标已存在时不解析 token |
| pause | `POST /sandboxes/{id}/pause` → 204 | 已暂停回 **409** |
| timeout | `POST /sandboxes/{id}/timeout` | body `{timeout:秒}`,重置 TTL |

create 的 `templateID` 接受三种引用:持久 id(`<profile>-<kind>-<key>`,§4.4)、注册期
transient id、或已 ready 构建的 name/alias——后两者解析到持久 id 再走统一路径。
`envdVersion` 回 `0.6.1`(e2b)或 stub `0.1.0`(bare,≥0.1.0 否则 SDK 自毁)。bare 无
envd,不生成也不返回 Envd/Traffic token;两种 profile 都返回独立的
`forwardAccessToken`。该 token 在创建时签发并随 Sandbox 记录持久化。

### 4.2 控制面:模板构建 API

实现 e2b **v2 build system** 的端点族(SDK `Template.build` / CLI 走它);构建语义与
资源池见 §12。

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| register | `POST /v3/templates` → 202 | body `{name, tags, profile?, cpuCount, memoryMB}`;`profile∈{e2b,bare}`,省略按此 e2b 兼容端点语义取 `e2b`,注册后不可变;`X-Kuasar-Sandbox-*` 头 → 模板默认配置(cpu/memory→`resource.capacity`,§4.6);回 `{templateID: transient-<uuidv7>, buildID, profile, names, tags, aliases, public:false}` |
| trigger | `POST /v2/templates/{tid}/builds/{bid}` → 202 | body 兼容 `{fromImage, fromTemplate, fromImageRegistry{username,password}, steps[], startCmd, readyCmd}` 与 CLI 形态 `{start_cmd, ready_cmd, …}`;`fromImage`/`fromTemplate` 互斥,皆缺时由 `builder.image_uri_mask` 推 fromImage;steps 支持 `RUN/ENV/ARG/WORKDIR/USER/COPY`(COPY 须先经 files 端点上传 context:未配 files_storage→**501**、未上传→**400**,§12);e2b 的 `startCmd` 非空或 fromTemplate ⇒ 暂记 snp(终态以流水线产物为准),否则 img;bare 禁止 start/ready(400)且恒为 image-only;fromTemplate 且无 steps 无 startCmd ⇒ 拒绝(无事可做);`cpu_count`/`memory_mb` + `X-Kuasar-Sandbox-*` 头 → 模板配置,**覆盖 register**;`X-Kuasar-Sandbox-Builder` → build-only 配置(§4.6) |
| status | `GET /templates/{tid}/builds/{bid}/status` | 回 `{templateID, buildID, profile, status, logs:[], logEntries:[]}` + 失败时 `reason{message}`;`logs`/`logEntries` 取自 journald 构建流(tag build),按 `?logsOffset`(已读条数)分页,SDK `on_build_logs` 即据此流式输出(§12);**进行中恒报 `building`**(registered/waiting/building 均映射,CLI wait 循环仅在 `building` 续轮询),终态 `ready`/`error`;失败 `reason` 通用(详情在日志流);ready 后 `templateID` 即报持久 id,并附 `names`/`aliases` |
| files | `GET /templates/{tid}/files/{hash}` → 201 | COPY context 上传协商:`tid→build→归属`校验后回 `{present, url}`——present 即对象已在桶(客户端跳过上传),url 为**直传桶的 presigned PUT**(字节不过控制面);未配 `files_storage`→**501**,未知/非属主 tid→**404**。详见 §12 |
| list | `GET /templates` | 本租户 ready 模板;`templateID` 列为持久 id,同时回不可变 `profile` |

### 4.3 数据面协议(envd,数据面仅透传)

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

### 4.4 templateID 与模板形态(transient / persist,无 templates 表)

```
persist  templateID = <profile>-<kind>-<key>    profile∈{e2b,bare}; kind∈{img,snp};
                                                key = manifest content key (64-hex)
transient templateID = transient-<uuidv7>       构建注册期临时句柄,build 完即弃
```

- **持久 id 自描述**:即 manifest 键,是 create 的正式 templateID;运行期从前缀解析
  profile(选 runtime erofs)与 kind(img = 冷启,snp = restore)。格式/枚举不合法
  当场 4xx。
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
  `PUT /internal/plugin/{id}/register`(config-socket plugin 平面,§6;线格式 node-proxy.md §6)。

### 4.6 沙箱配置传递链

每实例沙箱配置经**命名空间化的 e2b metadata 保留键** `kuasar-sandbox.<ns>`(各值一个
JSON 对象)注入,零 SDK/API 改动。命名空间是 sandbox-runtime `config.SandboxConfig`
(serve import,单一真源)的**租户可控子集**:

| 命名空间 | 去向 |
|---|---|
| `resource` | `resources.{capacity,allocatable}`(不含 `control`,节点托管) |
| `network` | 拆分:`hostname`/`nexthop`→guest;`inner_ip`/`transit_*`→`vswitch.Attach`;`dns`→`/etc/resolv.conf` |
| `launch` | `launch.{exec,args,env,workdir,restart,user,stop_signal,plugin}`——**仅 bare**;e2b profile 拒(envd 占用 launch) |
| `init` / `mounts` / `files` | 直透 `init[]` / `mounts[]` / `files[]` |
| `metadata` | `SANDBOX_CONFIG.metadata` 透传(如 `e2b.start_cmd`) |
| `restore` | 本次 host restore 的 `prefetch` 策略;可省略,显式值只允许 `off`/`memory`,不开放节点托管的 `file_refs` |
| `credentials` | 创建期 ServiceSecret、Envd/Traffic token override;解析后从普通 metadata 剥离,不进入 guest |

单 sandbox 显式启用的两种等价请求形态:

```http
X-Kuasar-Sandbox-Restore: {"prefetch":"memory"}
```

```json
{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}"}}
```

`restore` 只接受严格 JSON object。未知字段、非字符串 `prefetch`、非法枚举及
`file_refs` 均在生命周期副作用前拒绝。未提供时默认关闭;同一请求的 Header 覆盖
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
  在 sandbox-ctl——serve 侧 yaml 是半成品(cgroup_path 经 `--cgroup-adopt`、base 经
  快照填),这里只对租户网络做格式校验。
- **两个注入面**:e2b metadata,与 `X-Kuasar-Sandbox-<Ns>` 请求头(API 边缘归一化进
  metadata,**同名头胜过 metadata 键**)。create 与模板构建(register/trigger)都支持;
  create 的 runtime sandbox 配置存 `sandboxes.metadata_json`,模板构建的普通 runtime 配置存
  `builds.metadata_json`,build-only 配置存 `builds.builder_json`。
  `restore` 是例外:只接受 create 请求,不进入模板构建。
  集群下 sandbox `create` 的所有权信息使用独立的 node-link 系统上下文(§10)。
- **优先级**:`节点默认 ⊕ 模板配置 ⊕ create 配置`(create 按命名空间胜)。模板配置:snp
  经快照、img 经 `builds.metadata_json`。构建内 `register ⊕ trigger`(trigger 胜);
  register/trigger 的 `cpuCount`/`memoryMB` → `resource.capacity`(胜过 resource 头),决定
  phase-C 构建 VM 容量。
- **capacity**:img create 自由(create/模板/默认);snp create / resume / 迁移导入**钉死
  快照**(runtime 拒容量不等)。
- **network 随快照**:渲染时把已解析逻辑网络注入
  `SANDBOX_CONFIG.metadata["kuasar-sandbox.network"]`,随 snapshot.cfg 落盘并跨 restore 继承;
  restore 时 serve 读回,填 create 未指定的网络字段(**显式 create 胜**,§8)。迁移
  token 同样携带 metadata。除 host-side restore policy 外,其余命名空间只在冷启生效或
  已冻入快照,故只 network 需随快照。
- **restore policy 不随快照或模板**:`kuasar-sandbox.restore` 只由 create 请求写入
  sandbox metadata。image cold boot 不把它渲染进运行 YAML;snp create、pause 后 resume
  和 migration import 在存在 restore ref 时重新渲染。connect/resume 不提供临时覆盖。
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
Delegate=yes                # 委派控制器,--cgroup-adopt 才能写 cpu.max/memory.max(§5.1)
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
`StopUnit` + `ResetFailedUnit`,再生成新的 UUIDv7 run-id 补足目标数量。

- **kill**:`StopUnit`(连 CH 一并 SIGKILL)→ `ResetFailedUnit` → tapfd `RELEASE` 或
  `connector-ctl vswitch detach`
  → 删运行目录 → 删库行。`kill` 在进程层生效,不受 guest 内 restart 策略阻挡。
- **就绪**:`Type=exec` 下 exec 成功即视为单元已启动;e2b profile 再轮询 envd
  `/health`(UDS,60s 上限)判数据面就绪。
- **存活权威**:`ListUnitsByPatterns("sandbox-runner@*.service")` 一次拿权威存活
  run-id 集,再与库内 `sandboxes.run_id` 对账(§15)。
- 宿主 `Restart=no` 与 guest 内 envd `restart=always`(sandbox-init 管)是两层,
  互不相干。

### 5.1 cgroup(cgroup-adopt:单元自身 cgroup 即沙箱资源 cgroup)

serve 不预建 cgroup、不做进程搬迁。LaunchSpec 给 sandbox-ctl 带
**`--cgroup-adopt`**:它接管自己所在 systemd 单元的 cgroup 作为沙箱资源 cgroup——读
`/proc/self/cgroup` 求出路径,cloud-hypervisor 与自身都留在其中。于是:

- 一个已分配 runner 单元 = 一个沙箱 cgroup;资源控制器(配置了 `control_socket` 时)在该路径上原地
  仲裁;`KillMode=control-group` 使 `StopUnit` 连 CH 一起 SIGKILL,无需 serve
  排空/rmdir 安全网。
- 单元必须 `Delegate=yes`:否则单元 cgroup 的控制器接口文件(`cpu.max`/`memory.max`)
  非本进程可写,`--cgroup-adopt` 写资源上限会 `permission denied`。
- 代价:sandbox-ctl 与 CH 同处受限 cgroup,`memory.high` 节流存在死锁风险(见
  sandbox-runtime `pkg/sandbox/cgroup.go` 头注)。采纳此模型并照常设 `memory.high`。

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
  <shared> --run-root <run_root> --cgroup-adopt (--restore <ref>)
  (--restore-file-refs trust) (--connect <uds:ip:port>)…]`,`env={MANIFEST_KEY}`。
  `--restore-file-refs trust` 只在 restore 且 `sandbox.restore.file_refs=trust`
  时追加。`--run-root` 把 sandbox-ctl
  的 socket/staging 目录(`ch.sock`/`ctl.sock`/…)钉到 serve 的 run_root,
  pause/snapshot 客户端(同 `--run-root`)才能拨到 `ctl.sock`。
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

**③ plugin 平面** — `PUT /internal/plugin/{id}/register`:一个订阅者(external proxy
master,或路由观察者如平台 agent)注册其能力并**持挂该 h2c 连接**——连接本身即它的
租约 + 路由流(routesync,线格式见 node-proxy.md §6)。请求体首帧是 `register{caps}`,之后(route_wake)是
`wake` 上行;响应体下行 `hello(policy) → upsert* → bookmark → upsert/delete`。能力相互
**独立、不强制组合**:`subscribe`(`route` | `route_wake`)、`proxy{socket{path}}`
(声明 serve proxyForwarder 转发数据面请求的目标 UDS)、`mmds`。**断连即反注册**;同
id 二次注册自动反注册(并断链)前者。鉴权:配 `paths.plugin_pidfile` 则 peer pid 须在
其中,未配则仅靠 socket 0600(同 admin)。external proxy master、平台 agent 均经此订阅。
此平面是**节点本地** UDS,与接入集群的 node-link(§10,跨网 mTLS)正交。

**④ api 平面** — 其余路径回落到 e2b 控制面 handler(与 TLS `api.listen` **同一个**
`http.Handler`,含 export/import 扩展),明文 h2c、`X-API-KEY` 鉴权。
`export-sandbox`/`import-sandbox` CLI 即此平面客户端(§8.1)。

**信任模型**:host root / daemon uid 可信;租户代码在 guest 内,够不到 host UDS。

## 7. 密钥与归属模型(APISecret + ManifestKey 凭据对,加密存 sqlite)

- **根凭据分工**:APISecret 与 ManifestKey 都是 32B / 64-lowercase-hex。
  APISecret 只用于 API 请求认证;ManifestKey 是 manifest 内容键(`MANIFEST_KEY` env /
  `manifest.key`),也用于封装镜像拉取令牌。创建凭据对时可显式给出 APISecret;
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
- **根凭据不落明文**:sqlite 内加密;运行期只在 serve 内存和必要的进程 env 中。
  ManifestKey 经 LaunchSpec/BuildSpec 注入需要内容访问的宿主进程;`<sid>.yaml` 非密不含根凭据。
  APISecret 留在 serve 内用于认证,不下发给子进程。auto-resume 从资源行解密 ManifestKey
  访问快照。集群下 node-link 原子下发完整凭据对,
  两者同样仅入加密存储 + 运行期内存(§10)。
- 每个 Sandbox 持久化独立 ServiceSecret。未 override 时按
  `HMAC-SHA256(APISecret,"kuasar-service-secret-v1:"+AuthSandboxID())` 派生;保存后不再重建。
  e2b 的 `envdAccessToken`/`trafficAccessToken` 可 override,否则分别随机生成;bare 两项恒空。
  两个 e2b opaque token 均限制为有效 UTF-8、最多 256 bytes,在写业务行前校验。
  e2b/bare 的 `forwardAccessToken` 均为 ServiceSecret 签发、绑定 AuthSandboxID 且
  `aud=forward` 的严格 `kat1`。四项凭据均加密落盘并在 lifecycle upsert 中不可重绑。
  `MmdsSecret =
  HMAC-SHA256(manifest_key, "kuasar-mmds-v1:"+sid)`(每沙箱确定性派生的 MMDS 会话签名
  密钥,`mmds.enabled` 时随路由分发给 proxy 校验 envd 身份,见 node-proxy.md §7)。

## 8. 生命周期与状态机

```
            create(img: cold boot / snp: restore)
                 │
                 ▼            pause / TTL(auto-suspend)
   ┌────────► running ───────────────────────────────► paused
   │             │                                       │
   │             │ kill                                  │
   │             ▼                                       │
   │           (row deleted)                             │
   └──────◄── connect(resume) / data-plane wake ──◄──────┘
```

- 操作映射:create(img = 冷启 / snp = restore)、connect = resume、pause = snapshot、
  timeout = 续期、TTL 到期 = auto-suspend(pause)、kill = 销毁(删行)。重启对账可把
  失联的 running 标为 `dead`(§15);paused/dead 行中,paused 可再拉起,kill 删行。
  集群下,这些操作另由 node-link 命令触发(create/connect/delete,§10),并把
  状态变化作为事件上报 registry。
- **auto-suspend**:reaper(5s 周期)发现 `deadline` 已过 → 对运行中 sandbox-ctl 封
  快照(按 `checkpoint.mode`,§8.1)→ 记 `snapshot_ref`、标 paused → `StopUnit` →
  detach。路由表保留 paused 路由,后续数据面流量可唤醒。
- **auto-resume**:数据面流量打到 paused 沙箱 → 读库 → 重走 launch(建目录 + attach
  + StartUnit),LaunchSpec 带 `--restore <snapshot_ref>` → sandbox-ctl 解封恢复。
  - **单飞**:同一 sid 的并发数据面请求经 per-sid single-flight 合并为一次 resume,
    杜绝重复 IP 分配 / attach / StartUnit 竞态。internal 模式 proxy 在请求内同步触发;
    external 模式经 routesync `Wake` 上行,serve 端同样单飞(§9.2)。集群级会话亲和的
    单飞在 registry 端按 (group, route-key) 进行(cluster.md)。
  - 数据面 auto-resume 等待恢复完成后再转发;`POST /sandboxes/{id}/connect` 则只同步
    完成鉴权、可选 KMT import 和凭据读取,接受同一 single-flight 的异步 resume 后立即返回;
    带 `timeout` 时该期限在恢复后仍覆盖节点缺省 TTL。
- **每实例配置**(create/构建经 metadata + `X-Kuasar-Sandbox-*` 头,命名空间化,详见
  §4.6):配置随沙箱持久化(`metadata_json`),resume 时重新解析、全生命周期一致;无白名单
  门(沙箱以完整能力经 sandbox API 发布,平台自身亦经此 API 管理)。**network 另随快照**——
  restore 时 serve 读回快照内的逻辑网络,填 create 未指定的字段(显式 create 胜)。

### 8.1 暂停态分层、转模板与跨机迁移

沙箱快照有两态,`checkpoint.mode` 选 pause 的默认落地:

| | **本机快照**(`local`,默认) | **远程快照**(`remote`) |
|---|---|---|
| 实现 | `sandbox-ctl snapshot --output` → `checkpoint.local_dir/<sid>/<sid>.snapshot` | `sandbox-ctl snapshot --upload` → manifest store |
| `snapshot_ref` | 本机 bundle 路径 | `manifest://<key>` |
| 可恢复范围 | 仅本机(恰合"沙箱附着宿主") | 任意共享同一 store 的节点 |
| 本质 | 暂停态 | **可移植,即模板** |

本机快照可按需晋升:`export-sandbox` 内部走 `sandbox-ctl upload-snapshot <bundle>`
上传为远程 manifest 并重指 `snapshot_ref`(本机 bundle 随之删除)。全部基于现有
sandbox-ctl 原语(`snapshot --output|--upload`、`upload-snapshot`、`run --restore`),
e2b API/CLI 零改动。

`export-sandbox` / `import-sandbox`(CLI 形式见 §2.7)是 api 平面端点
`POST /sandboxes/{id}/export`、`POST /sandboxes/import` 的客户端;两端点在 TLS
`api.listen` 上同样可达(api-key 已按租户隔离),晋升/导出/插行全由 daemon 进程内
完成,无第二写者。

- **晋升 / 转模板**(`--to-template`,须 paused):确保远程后,组装并打印自描述持久
  id `<profile>-snp-<key>`(不写 builds 表)。之后 `e2b sandbox create <id>` 即从该
  快照扇出新沙箱(新 sid);create 由 api_key→白名单解析完整凭据对,再以 ManifestKey
  访问快照内容。
- **迁移 token**:`export-sandbox <sid>` 确保远程后导出
  `kmt1.<base64url-no-padding(nonce|ciphertext)>`。ManifestKey 经
  `HMAC-SHA256(decodeHex(ManifestKey),"kuasar-migration-token-v1")` 派生 AES-256-GCM
  key,每次 export 使用随机 12-byte nonce。完整 wire 上限 512 KiB,旧 plain-base64
  token 不再接受。
- **迁移内容与连续性**:GCM payload 携 source NodeSandboxID、`AuthSandboxID()`、Profile、
  template/snapshot/runtime、env/metadata、创建/截止时间、两个 tenant root 的完整指纹,
  以及既有 ServiceSecret、Envd/Traffic/Forward token。它不携 APISecret/ManifestKey 原文、
  Group/RouteKey、generation、Exec token 或 session。目标 node 从本地 key 表取得完整 pair,
  校验 fingerprints/runtime/Profile/Forward KAT 后原样落库,不重新派生或生成 service credential。
- **target 与冲突**:standalone import 省略 `sandboxID` 时复用 source NodeSandboxID;显式 target
  只替换本地 ID,保留 AuthSandboxID 与全部 credential。ID 使用 1..57 bytes 的 lowercase
  DNS-label 子集 `^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$`。插入为原子 insert-only,
  已存在返回 409且不覆盖。token 可在现有授权下重复用于不同 target,不增加 single-use 状态。
  目标机须共享同一 `manifest_config`(远程 store)并预装匹配的 tenant pair。
- **一步迁移**:`Sandbox.connect(<sid>, api_headers={"X-Kuasar-Migration-Token":
  <token>})`——path sid 是明确 target。目标不存在时,connect 在当前请求内同步完成
  decrypt/validate/insert并读取 response credential,接受异步 resume 后返回同一 sid;
  目标已存在时完全忽略 token。超过 512 KiB 的 Header 返回 431;独立 import body/token
  超限返回 413。`import-sandbox` CLI 保留作显式预导入。
- **状态感知驱动迁移**:暂停态的本地/远程经 `RouteEntry.snap_loc`(`local`|`remote`)随
  路由流下发(node-proxy.md §6);订阅 plugin 平面的平台 agent(`subscribe=route`)据此识别哪些 paused
  沙箱节点绑定(腾空节点前须先迁移)、哪些已可移植,再按需调 export-sandbox 铸造
  MIGRATION_TOKEN 完成自动迁移。迁移 token 是凭据且会回收源行,故按需铸造、绝不随路由广播。
- 限制:token 不含 tenant raw root,但携带沙箱自有 env 与数据面 credential,按"沙箱级敏感"对待;
  快照绑定其 guest runtime(erofs 摘要校验),不同 runtime 的节点拒绝导入。

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
接受 proxy master 注册并向其广播路由(§9.2),数据面字节流不经 serve。两模式共用同一转发判定
函数;部署拓扑、共享内存路由视图与确定性 MMDS 密钥(多 worker 对等)等细节见 node-proxy.md §5,
转发判定与即时刷流见 node-proxy.md §4。集群下,cluster-ctl router 把数据面转发进本节点的
数据端点(internal 的 `api.listen`/`data_listen` 或 external proxy master 的数据口),节点侧
按 `E2b-Sandbox-Id` 寻址照常处理(cluster-router.md),无须区分来源。

### 9.2 路由权威与广播

serve 是**本节点**路由与生命周期的权威:create/resume/pause/kill 实时更新路由,经
**routesync** 广播 Upsert/Delete 给所有 plugin 平面订阅者(external proxy master 与路由
观察者如平台 agent)。proxy master 把路由投影到共享内存,worker 只读;观察者持只读缓存
感知状态。广播逐条 upsert + 末尾 bookmark(高密度下发端内存有界)。线格式(帧化 JSON over h2c)、容错重同步、
`RouteEntry` 字段(含驱动迁移的 `snap_loc`、扇出用的 `mmds_secret`)见 node-proxy.md §6;
plugin 平面的注册与鉴权见 §6。机群级路由权威是 registry(cluster.md);serve 经 node-link
把本节点沙箱事件上报 registry(§10),与本节点 plugin 平面的路由广播是两条正交通道。

- **auto-resume 单飞**:数据面打到 paused 沙箱触发 resume——internal 在请求内同步触发,
  external 经 routesync `Wake` 上行;同一 sid 的并发请求经 per-sid single-flight 合并为
  一次 resume(§8)。
- **envd 鉴权姿态(`mmds.enabled`)**:该开关决定 create 是否给 envd 下发 token、proxy
  是否寄宿 MMDS 服务——`false` = envd 非 secure、proxy 单闸门(配置强制
  `proxy.auth=enforce`);`true` = proxy 组件内起 FC MMDS v2、经 `/init` re-key 每身份新
  token,使快照扇出沙箱数据面可用。姿态对比与 MMDS 两段式协议见 node-proxy.md §8。

### 9.3 proxyForwarder

数据面请求误达 serve 控制面监听口时(external 模式下客户端未分流到数据口),serve 经
已注册的 `proxy_socket` UDS 建立一次性 chained CONNECT,由 proxy worker 照常处理(含鉴权)。
普通 HTTP 在该隧道内发送一条请求,CONNECT 则直接 splice 客户端与 worker;无 proxy 注册时回 502。链式隧道与转发细节见
node-proxy.md §9。

## 10. 集群接入(node-link)

配 `cluster.node_link.endpoint`(§3)时,`node-ctl conductor serve` 拨 registry 把本节点接入集群,交由
`cluster-ctl registry/router/placer` 编排。node-link 复用 routesync 的帧化 JSON over h2c 引擎
(node-proxy.md §6),但角色相反:node 是本节点路由 / 构建权威,registry 是订阅者和命令下发方。

本节只讲 node 侧行为。registry 的 owner 选择、redirect/relay、shardkv 复制和 membership 变更由
[cluster.md](cluster.md) 定义。

```text
node-ctl conductor serve
  │ dial registry node_link endpoint
  │ register node profile
  │ stream heartbeat + sandbox/build events
  │ receive create/connect/delete/key/build commands
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
sandbox{sid, profile, state, snap_loc, access_token, template_id}
build{build_id, state, template_id?, reason?}
delete{sid}
bookmark{full_sync}
```

node 不在 sandbox event 中自报 Registry-owned 的 cluster context。nodelink owner 在任务下发前已维护
本节点完整的 sandbox/build 归属表,收到事件后以 `(node_id,sid)` 或
`(node_id,build_id)` 查表取得 group/route_key。生产 ID 由 UUIDv7 或等价随机机制保证
全局唯一,但事件处理不依赖该假设,也不存在 cluster 全局 ID 索引。

全量 Range 结束的 bookmark 带 `full_sync=true`。nodelink owner 仅将本轮出现的 sid 与
订阅建立前捕获的本节点归属表基线比较;清理前再次确认当前表项仍与基线一致,避免删除
同步期间新下发或重新绑定的任务。增量 replay 的 bookmark 只推进 resume token。

### 10.4 命令受理

registry 上行下发命令。serve 复用既有 e2b 生命周期原语(§8 / §8.1)执行,以 sid / build_id 幂等,
受理即回 `cmd_ack`,终态经 sandbox/build 事件上报:

  | 命令 | 节点动作 |
  |---|---|
  | `create{cmd_id, sid, template_ref, profile, api_secret_fingerprint, config, cluster}` | 冷启 `template_ref` + 保存/合并 `config`(§8;snp 模板 = 快照恢复快启);完整 APISecret 指纹选择本机已安装的凭据对;`cluster={group,route_key,auth_sandbox_id?}` 与 profile 作为系统字段独立持久化;credentials namespace 在写业务行前解析并剥离 |
  | `connect{cmd_id, sid, profile, api_secret_fingerprint, cluster, migration_token?}` | target 已存在时忽略 token,校验完整指纹、profile 和 cluster context后接受异步恢复;target 缺失且带 KMT1 时,先用本机 matching pair 同步校验并以命令 sid/context insert paused 行,再 Ack并异步恢复;缺失且无 token 则拒绝 |
  | `delete{cmd_id, sid, api_secret_fingerprint}` | 完整指纹必须与既有 Sandbox 业务行绑定一致,随后销毁沙箱(§5 kill) |
  | `key_put{api_secret_fingerprint, api_secret_type, api_secret?, api_secret_ref?, manifest_key_fingerprint, manifest_key_type, manifest_key?, manifest_key_ref?, expires_unix}` / `key_drop{api_secret_fingerprint}` | `key_put` 原子校验并写入 / 重发续租完整凭据对;两项指纹均为 64-hex SHA-256。`key_drop` 按完整 APISecret 指纹 best-effort 清理,正确性依赖 TTL 淘汰(§7);registry 的密钥分发见 cluster.md |
  | `build_register{build_id, template_id, profile, resources, image_repo, registry_auth, api_secret_fingerprint, config}` | 预配 registry 分配的构建(§12;`profile` 必填且只接受 e2b/bare;按完整 APISecret 指纹解析凭据对、建 build 记录、瞬态用镜像凭据);`config` metadata 原样保存,构建态经 `build_event` 上报 |

无 `drain` 命令。节点排空 / 维护由节点侧发起(node-resource.md §2.5 资源 drain 或本机维护策略),
集群侧只停止向其分配。

### 10.5 断线与安全

断线后节点指数退避重连并重注册,带 `resume_from=<rev>` 请求增量重放。registry/node 留存窗口内只补增量,
否则逐条全量 + bookmark。registry 重启亦然。

node-link 生产走 mTLS(`cluster.node_link.tls`)。下行 APISecret+ManifestKey 凭据对及创建期
credentials 只进入加密存储和运行期内存;沙箱级 token 属敏感数据,仅在可信链路内传输。

接入集群与本机 plugin 平面使用同一 routesync 引擎和线格式,仅订阅者 kind 不同。router 不订阅节点
plugin 平面,机群路由经 registry 聚合。

## 11. guest profile:envd 与工具链

- 单一 **`sandbox-runtime.erofs`** 由 `guest-runtime` 构建:把 `sandboxer` 产出的
  `sandbox-init` 打成 virtio-pmem/DAX runtime,并在 `/opt/sandbox-runtime/bin/`
  内置固定版本 `envd`、`flatten-ctl`、`mkfs.erofs`。runtime 构建保持确定性 mkfs
  参数,并把成品**补齐到 2 MiB 对齐**(virtio-pmem 后端要求,否则 cloud-hypervisor
  报 `PmemSizeNotAligned`;EROFS superblock 自描述范围,尾部稀疏 padding 对 guest
  mount 不可见)。
- **零 sandbox-init 改动**:`/opt/sandbox-runtime` 被 sandbox-init 自动 bind-mount 进
  guest 同名路径,envd 直接作 `launch.exec`:
  `/opt/sandbox-runtime/bin/envd -isnotfc -port 49983`(`mmds.enabled` 时去
  `-isnotfc`,node-proxy.md §8),`restart=always`,**以 root(`user: "0:0"`)运行**——envd 需要
  `CAP_SETUID/SETGID` 才能按镜像配置的用户跑工作负载命令(镜像设了 `Config.User` 时
  非 root 的 envd 会 exec EPERM);工作负载本身仍以目标用户执行。
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
- host 在 envd 就绪后调 **`POST /init`**(经 UDS):置 `envVars`、默认用户
  `user`/workdir `/home/user`、时间戳;仅 `mmds.enabled` 时携带 `accessToken`(node-proxy.md §8)。

## 12. 模板构建(三阶段流水线,构建在沙箱内进行)

构建经 e2b API 提交(端点见 §4.2;无独立构建 CLI),落 `builds` 表,由资源池调度,
每次执行绑定一个 `sandbox-builder@<run-id>` 单元。**镜像拉取与 step 执行都发生在构建沙箱
(microVM)内**——租户的网络流量与镜像内容不触宿主用户态,宿主侧只做工件接力与
收尾上传。

**serve 侧(每构建一次)**:建 workdir → 配 `tapfd_socket` 时经 `TAPFD/1 PREPARE`、否则经
`connector-ctl vswitch attach` 分配一个网络槽(整个构建复用,各阶段顺序交接 tapfd)→
铸 envd token →(`mmds.enabled` 时)挂一行合成路由,
让模板阶段 FC 模式的 envd 能按 floatingip 自解析 → 从 builder pool 分配 run-id
(无 idle 时按需 `StartUnit`)→ run-builder WaitAssignment 取得 bid 后执行流水线
→ 经 config-socket 回传结果 → 终态落库:产物为快照 ⇒ `kind=snp`、为镜像 ⇒
`img`,持久 id `<profile>-<kind>-<key>` 写入 names/aliases。profile 从注册到
BuildSpec 全链路显式携带,网络槽的 inner IP/nexthop 亦按该 profile 选择。

**单元内(run-builder,§2.4)** 依 BuildSpec(§6)最多跑三个阶段,每阶段一台
microVM(`sandbox-ctl run` 直接子进程);A 阶段就绪探针 = guest 内
`flatten-ctl mountpoint /.probe`,B/C 阶段以 envd `/health` 为就绪:

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
  (MMDS 姿态随部署,node-proxy.md §8),`/init` 预置(此后 RPC 携 `X-Access-Token`)。startCmd
  **经 envd 启动**(e2b 默认身份 `user`、`/home/user`),流挂至就绪后断开——envd
  不因断流杀进程(其源码明言进程上下文与请求解耦),进程以 **envd 管理进程**身份
  冻入快照,扇出沙箱里 SDK 可 list/connect;readyCmd 以 2s 间隔轮询至成功(预算
  `ready_timeout_sec`;缺省时同 e2b:`sleep 20`),就绪后先断 startCmd 流(已败则
  构建失败)再 `sandbox-ctl snapshot --output` 出本地快照 bundle。

**两类 guest 信道,刻意分离**:e2b 语义命令(steps/startCmd/readyCmd)走 envd,
与 e2b 自家模板构建逐项同形;平台机制(flatten-ctl 拉取/导出、运行时配置注入、
工件流回、就绪探针)走 `sandbox-ctl exec`——任意 rootfs 可用、裸 stdio 接力,
不依赖镜像 userland。

**fromTemplate**:base 来自既有模板——img 模板直接用其镜像 key;snp 模板经
`sandbox-ctl info --json manifest://<key>` 读 snapshot.cfg 取 `base_ref`,并继承
metadata 里的 `e2b.start_cmd`/`e2b.ready_cmd`(请求显式给出者优先)。fromTemplate
与 fromImage 互斥;fromTemplate 且无 steps 无 startCmd 拒绝(无事可做)。bare 不继承
e2b start/ready metadata,也不进入 C 阶段,只上传 image 产物。

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

**收尾上传(平台凭据唯一出现点)**:img-only(包括所有 bare build) ⇒ `manifest-ctl store image.img`
(stdout = 64-hex manifest key;若 import referer hit/miss 已得到 base manifest id 则直接
复用);产出快照 ⇒ **一条** `sandbox-ctl upload-snapshot <bundle>`——自动上传
snapshot.cfg 引用的全部本地工件(base 镜像、overlay)并把引用改写为
`manifest://`(runtime_ref 不动,宿主提供)。结果
`{image_key|snapshot_key, start_cmd, ready_cmd, error}` 经 config-socket 回传;
快照模板的 start/ready 同时记进 snapshot.cfg metadata,模板自描述(fromTemplate
继承与 create 都读它)。

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
| `sandbox-ctl`(runtime) | 经 run-sandbox(单元)`execve`:`run --config <sid>.yaml --manifest-config … --run-root … --cgroup-adopt [--restore] [--connect]`;run-builder 以直接子进程 `run` 阶段沙箱,经 `exec --env/--stdin-from/--stdout-to` 做平台接力(flatten-ctl 调用、配置注入、工件流、探针),收尾 `snapshot --output` / `upload-snapshot` / `info --json`;serve 调 `snapshot --upload`(pause) | 非密配置文件 + 密钥 env;资源准入在其内部;e2b 语义命令不走它(走 envd,§12) |
| 资源控制器(node-resource.md) | serve 内置(`resource_listen`,调参内联);沙箱经 `sandbox.resources.control_socket` 拨号(`pkg/resource` 协议) | 单元 cgroup 即沙箱 cgroup,控制器原地仲裁;不配 control_socket = 静态 cgroup(`--cgroup-adopt`),配了才进 SANDBOX_CONFIG `resources.control.controller` |
| registry(cluster-ctl) | node-link:serve 拨 registry、反向注册为路由权威,上报 register/heartbeat/sandbox/build_event 事件、受理 create/connect/delete/key_put/key_drop/build_register 命令(§10、cluster.md) | mTLS;cluster kill 走 node-link delete 命令;空 `cluster.node_link.endpoint` = 独立模式不接入 |
| `connector-ctl vswitch`(vswitch) | 不配 `tapfd_socket` 时经 CLI:`attach <switch> --inner-ip [--transit-*]` / `detach --port`;配 `tapfd_socket` 时经常驻 `TAPFD/1 PREPARE` / `OPEN` / `RELEASE`;sandbox 配置仍渲染为 `network.tapfd.socket/request` | 交换机预先起好(`connector-ctl vswitch start/serve`,内核态数据面);port 对外、slot 内部;一个构建复用一个槽 |
| `flatten-ctl`(builder) | **guest 内**(guest runtime 自带,经 sandbox-ctl exec 驱动):`export --output -`(import 拉取 / steps 导出)、`mountpoint`;宿主侧:`info --json`(读镜像运行时配置,本地工件或 manifest://) | 租户 `FLATTEN_*` 仅经 exec env 入 guest;tarstream 镜像工件经 exec stdio 接力 |
| `manifest-ctl`(accelerator) | `store <image.img>`(img-only 构建的收尾上传) | manifest key 经 stdout 回收;`MANIFEST_KEY` 经 env |
| `mkfs.erofs`(deps) | guest-runtime `make sandbox-runtime` 与 guest 内 `flatten-ctl` 后端 | 确定性打包 runtime;构建沙箱内导出 EROFS 镜像(§11、§12) |
| guest envd | UDS(sandbox-ctl `--connect` 映射);构建流水线另以最小 connect+JSON 客户端调 `process.Start`(steps/startCmd/readyCmd,§12) | 原版不改;协议 pin 见 §4.3/§4.5 |
| systemd | D-Bus:StartUnit/StopUnit/ResetFailed/ListUnitsByPatterns/Reload | 进程管理 + 单元自装(§5) |
| `node-ctl proxy`(external) | UDS routesync(双向 h2c 帧化 JSON)+ 兜底反代 | 同节点、运维带外起;proxy master 注册一次,worker 共享继承 listener fd + shm 路由视图(node-proxy.md §5) |

不新增导出包;`CGO_ENABLED=0`;依赖层级 = 叶子。

## 15. 可靠性

### 15.1 状态存储(sqlite)

单文件 sqlite(`paths.db_path`,WAL,文件 0600),纯 Go 驱动。三张表:

```
sandboxes      id(node-local SandboxID,1..57 bytes DNS-label subset) PK,
               profile, cluster_group, cluster_route_key, auth_sandbox_id,
               template_id, state(running|paused|dead), deadline_unix,
               run_dir, base_dir, envd_uds, ci_uds, floatingip, vswitch_port,
               inner_ip, port_mac, api_secret_hash, api_secret_enc,
               manifest_key_hash, manifest_key_enc, snapshot_ref,
               service_secret_enc, envd_access_token_enc, traffic_access_token_enc,
               forward_access_token_enc, metadata_json, env_json,
               created_unix
builds         build_id PK, template_id(transient-…), persist_id(<profile>-<kind>-<key>),
               api_secret_hash, api_secret_enc, manifest_key_hash, manifest_key_enc,
               profile, kind, from_image,
               start_cmd, status(registered|waiting|building|ready|error), reason,
               names_json, aliases_json, registry_auth_enc, created_unix
manifest_keys  api_secret_hash PK, api_secret_enc, manifest_key_hash,
               manifest_key_enc, label, created_unix, expires_unix, registry_auth_enc
```

`builds` 兼任模板登记(§4.4);`*_enc` 根凭据及 Sandbox service credential 均
AES-256-GCM、两项 `*_hash` 均为
完整 SHA-256。`substr(api_secret_hash,1,24)` 仅建候选预筛索引(§7)。

### 15.2 重启对账

serve 重启后以 `ListUnitsByPatterns("sandbox-runner@*.service")` 为存活权威:

- 单元 active/activating 且库内 running ⇒ **收养**(重挂内存路由、TTL 继续生效,
  external 模式随快照重新推给 worker;集群下经 node-link 重报);
- 库内 running 但无对应活单元 ⇒ 清理(StopUnit/detach/删运行目录)并标 `dead`;
- 无 running 行对应的 runner 单元属于上一个 pool 的 idle/orphan run-id ⇒
  `StopUnit` + `ResetFailedUnit`,随后由新 pool 按配置补足;
- `run_root` 为 tmpfs ⇒ 整机重启后 running 全部判 dead;`paused` 行与 snp 模板保留,
  可被 connect/auto-resume 重新拉起(本机快照存于磁盘 `checkpoint.local_dir`)。

### 15.3 故障域

| 故障 | 影响 | 自愈 |
|---|---|---|
| serve 崩溃/重启 | 控制面与 internal 数据面中断;沙箱(microVM/单元)不受影响 | systemd 重启 → 重启对账收养;external proxy master 仍可用共享路由视图服务 running 流量(Wake 无人应答,paused 唤醒挂起至超时);集群下 node-link 重连重报 |
| proxy worker 崩溃(external) | 该 worker 上的连接断;其余 worker 继续接新连接 | proxy master 重启该 worker;worker 重新读取共享路由视图 |
| proxy master 崩溃(external) | external 数据面中断,plugin 租约断开 | systemd 重启 master → 重新注册、重建共享表、启动 worker |
| runner 单元/CH 崩溃 | 该沙箱死(`Restart=no`,有状态不重试) | 对账标 dead;客户重新 create(或从 paused 快照 resume) |
| routesync 断流 | external 共享路由视图停更 | proxy master 指数退避重连重注册,重连即重新同步(逐条 upsert + bookmark,node-proxy.md §6) |
| node-link 断流(集群) | registry 暂失本节点视图 | 节点指数退避重连重注册重报沙箱集(§10、cluster.md);本节点沙箱不受影响 |
| sqlite 损坏 | 控制面不可用 | 文件级备份/重建;沙箱单元仍可被 ListUnits 发现并由运维处置 |

## 16. 测试

单元测试:`make test`(handler 路由、apikey/secretbox/regcreds、routesync(注册/bookmark
往返)/proxyshm(共享路由表、park/wake、世代清扫)、plugin 注册表(同 id 顶替)、proxy CONNECT 隧道 +
proxyForwarder 链式 relay、mmds(确定性密钥)、单飞、沙箱配置注入(命名空间解析/容量折叠/网络合并)、
migrate、node-link(注册/事件/命令往返)等)。跨仓 e2e 集中在 umbrella
`orchestrator/release-builder/test/e2e/`(需多仓产物:vmlinux/cloud-hypervisor/mkfs.erofs/
sandbox-runtime.erofs 等),均已注册为 umbrella make 目标,缺前置则自跳过
(`REQUIRE_*=1` 改为硬失败):

| 脚本 | 覆盖 | make 目标 |
|---|---|---|
| `e2e_node.sh` | 单元自动安装 + 控制面(`/health`、401 路径)+ 构建 API 生命周期(register/trigger/status、跨 key 归属 404)+(有 KVM 时)bare create/list/kill | `test-e2e-node` |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器(纯用户态,无 root/systemd/KVM):pidfile 锁/双起拒绝、HTTP 取 LaunchSpec、execve、`TASK_*` 剥除;`config` CLI 往返 | `test-e2e-runtask` |
| `e2e_run_builder.sh` | 三阶段构建流水线(KVM + vswitch + store-ctl + zot,guest 经 mgmt VIP 拉取):fromImage → e2b-img;fromTemplate(img)+steps+startCmd → e2b-snp(manifest:// base、配置合并、snapshot.cfg metadata 断言);fromTemplate(snp)+steps → e2b-snp(start/ready 继承);profile=bare fromImage → bare-img(拒绝 start、bare 网络、image-only);分别从 e2b-snp/bare-img create/list/kill;COPY 与 files 端点 501 | `test-e2e-run-builder` |
| `e2e_execute.sh` | 从已建模板冷启真实 microVM、guest 内 exec、pause(snapshot)→ resume 全链路;create 经 `X-Kuasar-Sandbox-Network` 注入 hostname 并在 guest 校验(§4.6) | `test-e2e-execute` |
| `e2e_node_proxy.sh` | `proxy.mode=external` 全链路:serve + proxy master + 多 worker(shm 路由视图 + 继承 listener fd)+ 真实 microVM/envd,数据面经 proxy 走(401/转发/wake/resume/CONNECT 隧道/proxyForwarder relay) | `test-e2e-node-proxy` |
| `orchestrator/test/e2e/e2e_cluster_stub.sh` | 用 `make build` 产物真实启动 `cluster-ctl registry/router/placer` + `node-stub-ctl`,覆盖 group 导入、key 分发、Reserve→READY→数据面转发、活动路由缓存、build_register、孤儿 route 清理、节点清空和 registry joint/old_grace cutover | `orchestrator: make test-e2e` |

本仓 `make test-e2e` 运行集群 stub e2e,不依赖 KVM/root/systemd。真实 microVM 端到端路径由
`orchestrator/release-builder` umbrella 目录的 e2e 脚本聚合执行。

## 17. See Also

- [node-proxy.md](node-proxy.md) —— 数据面转发层:路由判定 / 部署模式(internal/external/off)/
  routesync / 数据面鉴权 / MMDS / CONNECT 隧道(本文 §9 装配的转发层实现,集群下 router 转发进入)
- [node-resource.md](node-resource.md) —— 节点资源控制协议(`sandbox.resources.control_socket`
  的对端)与控制器内部组织(serve 经 `resource_listen` 内置,调参内联)
- [cluster.md](cluster.md) —— 集群控制面:node-link 线格式(§5,本文 §10 的对端)、注册表、
  Reserve 状态机;[cluster-router.md](cluster-router.md) 数据面入口、[cluster-placer.md](cluster-placer.md)
  放置与密钥分发
- `sandboxer/docs/sandbox.md` —— sandbox-ctl:SANDBOX_CONFIG 模式、
  run/snapshot/restore/connect 原语、cgroup 模型
- `connector/docs/vswitch.md` —— attach/detach/open-port、floatingip 与
  mgmt-service(MMDS VIP 转换)语义
- `guest-runtime/docs/flatten.md` —— flatten-ctl export 与 OCI Referrers 幂等流、
  `FLATTEN_REGISTRY_*`
- `accelerator/docs/manifest.md` —— manifest 内容键、收敛加密与去重域
  (§7 的存储侧)
- `orchestrator/release-builder/docs/deployment.md` —— 节点部署拓扑中本组件的位置与单元安装
- `orchestrator/release-builder/test/demo/DEMO.md` —— e2b CLI/SDK 全流程演示
