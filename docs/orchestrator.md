# sandbox-orchestrator — 节点编排与 e2b 兼容控制面 设计 v0.7

## §1 概述与定位

`sandbox-orchestrator` 是计算节点上的**单实例常驻 daemon**，对外提供一套 **e2b 兼容 API**，
把节点上的 microVM 沙箱以 e2b 协议暴露给客户端——未改造的 e2b SDK（python / js `e2b`、
`@e2b/code-interpreter`）与 e2b CLI（`e2b template build`）可直接指向本机运行。一身兼 e2b 的三角色：

- **api**：e2b 控制面 REST（沙箱生命周期、模板构建、鉴权）；
- **orchestrator**：用 **systemd 模板单元** + **`run-sandbox`/`run-builder`** 启动器拉起 / 停止 `sandbox-ctl`（沙箱）与 `flatten-ctl`（构建），
  调 `vswitch-ctl` 编排网络；启动器经 **config-socket** 取通用 **LaunchSpec**（非密配置走文件、密钥走 env）后 `execve` 替换为目标进程；
- **proxy**：把客户端到沙箱的数据面流量反代到 guest（envd 经 UDS，用户服务经 floatingip）。

本方案新增组件，收编 PROPOSAL §10.13 / deployment §2.4 长期预留的"节点编排代理"槽位。
产物为二进制 **`orchestrator-ctl`**，`orchestrator-ctl serve` 启动服务。

### §1.1 两类沙箱（profile）

| profile | 是什么 | 数据面 | 对外服务 |
|---|---|---|---|
| **e2b** | guest 内跑原版 envd，agent 经其 exec / 读写文件 / 跑代码 | 支持（fs/process/pty/runCode，经 envd 代理） | envd + floatingip 用户口 |
| **bare** | 把客户镜像当网络化 microVM 跑起来，无 envd | 不支持 | **仅 floatingip 网络** |

`bare` 直接复用基础沙箱 profile，把基础沙箱第一次接上北向 API。经 e2b API 构建的模板恒为 **e2b** profile。

### §1.2 边界与依赖

- **不感知资源仲裁**：沙箱准入/配额全在 `sandbox-ctl` 内部（它是 `pkg/resource` 的 client）；orchestrator **不 import `pkg/resource`、不拨 sentinel**。**构建**任务的资源池由 orchestrator 自管（§11）。
- 依赖面薄：经 **CLI** 驱动 `sandbox-ctl`(run/snapshot)、`vswitch-ctl`、`flatten-ctl`、`mkfs.erofs`；经 **systemd D-Bus** 管单元；经 **UDS** 反代 envd + 跑 config-socket + 跑 routesync（§2.1）。
- 叶子组件、纯 Go、`CGO_ENABLED=0`；依赖 stdlib + modernc.org/sqlite（纯 Go）+ golang.org/x/net/http2（h2c：proxy 上游 + routesync）+ golang.org/x/sys（pidfile 锁 / SO_REUSEPORT）+ coreos/go-systemd（D-Bus）+ google/uuid（v7）+ gopkg.in/yaml.v3。**外置 proxy 不引入 gRPC/protobuf**（routesync 是帧化 JSON over h2c，§2.1）。
- 北向客户：**e2b SDK / CLI**（直连）+ **platform-agent**（本方案外/未来；区域面 = **platform-service**）。

## §2 架构与数据通路

```
        ┌─ e2b SDK / CLI ─────────┐            ┌─ Platform mgmt plane (future) ─┐
        │ 直连                    │            │                                │
        ▼                         │            ▼ platform-agent (本方案外/未来)
   orchestrator-ctl  (本方案内)  ◄──── 驱动 ─────────────────────────────────────┘
     │ TLS 终结 → 按 Host/SNI 路由 (sid, port)
     ├─ api:    POST /sandboxes / connect / timeout / pause / kill / list
     │          POST /v3/templates · POST /v2/templates/{tid}/builds/{bid} · GET …/status · GET /templates
     ├─ orchestrator (sandbox):
     │    建 /run/sandbox/<sid>/ + /var/lib/sandbox/<sid>/   (不预建 cgroup)
     │    vswitch-ctl attach <switch> → {port, floatingip, mac, ip}
     │    StartUnit(sandbox-runner@<sid>.service)
     │    └ unit: orchestrator-ctl run-sandbox --pidfile=… --config-socket=… --sandbox-id=%i
     │         │ run-sandbox 锁+写 <sid>.pid → 取 LaunchSpec → execve sandbox-ctl run --config <sid>.yaml (key 经 env) → 起 VM
     │         │ (e2b) connect …:127.0.0.1:49983/49999 (UDS) → guest 原版 envd
     │    等就绪 → envd POST /init → 登记 sqlite + TTL
     ├─ orchestrator (build):  builds 表排队 → 资源池准入 → 写 flatten.yaml → StartUnit(sandbox-builder@<bid>) → run-builder → flatten-ctl (stdout→.result)
     └─ proxy (数据面；部署形态见 §2.1)：解析 (sid,port) → 校验 X-Access-Token(§3.3) → 转发
          profile=e2b 且 port∈{49983,49999} → dial UDS(connect) → guest envd
          profile=bare 且 port∈{49983,49999} → 501 (no data-plane)
          其余(任意用户端口)                  → dial floatingip:port
```

proxy = L7：按 `Host`(`<port>-<sid>.<domain>`)/`E2b-Sandbox-*` 头解析 `(sid, port)` → 校验数据面 token（§3.3）→
`httputil.ReverseProxy{FlushInterval:-1}`，上游对 envd 走 **h2c（prior-knowledge）**、透传 trailers、不缓冲。
对 paused 沙箱的并发数据面请求经 **单飞 resume**（§8）合并为一次唤醒。**部署形态见 §2.1**（internal / external / off）。

### §2.1 proxy 部署模式（internal / external / off）

`proxy_mode` 选择数据面如何承载（控制面 `api.<domain>` 始终由 orchestrator 的 `listen` 提供）：

| 模式 | 数据面承载 | 进程 | 适用 |
|---|---|---|---|
| **internal**（默认） | orchestrator 进程内 proxy | 单二进制 | 简单部署、无额外组件 |
| **external** | 独立 `orchestrator-ctl proxy` worker（≥1），**SO_REUSEPORT** 共享数据面口 | serve + N×proxy | 数据面/控制面**进程隔离**、独立扩展 |
| **off** | 拒绝（501） | — | 该节点不提供数据面 |

external 模式拓扑（数据面字节流**不经** orchestrator）：

```
 client ─► orchestrator-ctl proxy  :443 (SO_REUSEPORT, N worker 共享)
                │ 本地路由表(routesync 推送) + per-sid 请求挂起(park)
                │ 命中 running → 校验 X-Access-Token → dial envd-UDS / floatingip:port
                │ 未命中/paused → Wake↑ + 挂起 → 等 Upsert↓ 就绪 / park_timeout→503
                └── UDS ── routesync (双向 h2c, 帧化 JSON) ── orchestrator-ctl serve
                      serve→worker: Hello(policy) → Snapshot(全量) → Upsert/Delete(增量)
                      worker→serve: Wake{sid} → 单飞 resume → Upsert(running) 回灌 → 解挂
 client ─► orchestrator-ctl serve  :443 (兜底：数据面请求误达控制口时)
                └─ 经同一 UDS 反代给某 worker(按 sid 哈希) 处理
```

- **路由权威在 orchestrator**：worker 是缓存。create/resume/pause/kill 实时广播给所有 worker（Upsert/Delete）；
  worker 重启 → 自动重连 + 重取全量快照；订阅滞后 → orchestrator 断开该订阅，worker 重连重快照（有界内存、最终一致）。
- **token 校验在 worker**（§3.3）：orchestrator 兜底转发时**不**校验，由 worker 校验。
- **协议无 gRPC**：帧化 JSON over h2c（`[4B LE len][JSON]`，复用 `golang.org/x/net/http2`），零新依赖。
- **运营策略集中下推**：握手时 orchestrator 把 `Policy{domain, auth_mode, park_timeout}` 推给 worker（worker 的 `--auth` 仅为策略到达前的回退）。
- **可观测**：`metrics_listen` 暴露 Prometheus 文本（`data_requests_total{result=…}`、`gateway_forward_total` 等）。
- worker 由运维带外管理（`deploy/orchestrator-proxy@.service`），orchestrator **不**自动安装；worker 须与 orchestrator 同节点（本地 dial envd-UDS/floatingip）。

## §3 e2b API 契约

### §3.1 控制面（orchestrator-ctl 自实现）

`orchestrator-ctl` 的全部子命令：**`serve`**（启动 daemon，`--config`/`--proxy`/`--proxy-socket`）、
**`proxy`**（external 模式独立数据面 worker，§2.1）、**`run-sandbox`/`run-builder`**（单元内启动器，§6，非给人用）、
**`config`**（`--config` 规范化+校验、`--template` 输出带注释骨架、`-o` 写文件；对齐 `sandbox-ctl config`/`flatten-ctl config`）、
**`manifest-key`**（`add`/`remove`/`check`/`list` 维护白名单，§7）、**`version`**。下表为 `serve` 提供的 e2b 控制面 HTTP 契约。

基址 `https://api.<domain>`；鉴权 **`X-API-KEY`**（沙箱）/ **`Authorization: Bearer`**（构建，e2b CLI）。
api_key 由 manifest_key 派生（`e2b-key-ctl gen-apikey`），orchestrator 经 §7 的 MAC 校验解析出租户——**无静态 api_keys 表**。

**沙箱生命周期**

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| create | `POST /sandboxes` → 201 | 回 `{sandboxID, templateID, domain, envdVersion, envdAccessToken, trafficAccessToken}`；`envdVersion<0.1.0` SDK 自毁 |
| get | `GET /sandboxes/{id}` | 归属校验 |
| list | `GET /v2/sandboxes` | 仅列**本租户**沙箱；分页头 `x-next-token` |
| kill | `DELETE /sandboxes/{id}` → 204 | 非本租户 ⇒ 404 |
| resume | `POST /sandboxes/{id}/connect` | **非 `/resume`**；body `{timeout:秒}` |
| pause | `POST /sandboxes/{id}/pause` → 204 | 已暂停回 **409** |
| timeout | `POST /sandboxes/{id}/timeout` | body `{timeout:秒}`；默认超时 **300s** |

> **归属校验**：按 id 的控制操作用 api_key 的 MAC 对该资源行的（解密）manifest_key 校验，不符当
> **404**（不泄露他租户存在性）；create / build 另需 manifest_key 在白名单，否则 **403**（§7）。`metrics` 暂不实现。

**模板构建（§11）** —— 同时实现 e2b 的 **v2 build system**（`/v3/templates`）与 **v1 build system**（`/templates`，`e2b template build` CLI 2.10.3 默认走 v1，仅告警 deprecated）。

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| register(v2) | `POST /v3/templates` → 202 | body `{name, tags, cpuCount?, memoryMB?}`；回 `{templateID:transient-<uuid>, buildID, names, tags, aliases, public:false}` |
| trigger(v2) | `POST /v2/templates/{tid}/builds/{bid}` → 202 | body 兼容 `{fromImage, startCmd?}` 与真实 CLI `{dockerfile, start_cmd, ready_cmd, cpu_count, memory_mb, team_id}`；缺 `fromImage` 时由 `builder_image_uri_mask` 推出（§11） |
| create(v1) | `POST /templates` → 202 | 配置在 create 期到达 `{alias, dockerfile, start_cmd, …}`；回 `{templateID, buildID}` |
| start(v1) | `POST /templates/{tid}/builds/{bid}`（**无 body**）→ 202 | 客户端已 push 镜像后触发；翻 waiting 入池 |
| status | `GET /templates/{tid}/builds/{bid}/status` | 回 `{templateID, buildID, status, reason, logs:[]}`；**进行中恒报 `building`**（CLI wait 循环仅在 `status=="building"` 续，故 registered/waiting/building 都映射为 `building`），终态 `ready`/`error`；ready 后附 `names`+`aliases`（内含 persist id） |
| list | `GET /templates` | 本租户 ready 模板；`templateID` 列为 **persist id** |

SDK/CLI 重定向：`E2B_DOMAIN`/`domain` → `https://api.<domain>` + 沙箱 host（Host 须 `api.*` 命中控制面）；`E2B_API_KEY`；dev 走 `E2B_API_URL`/`E2B_SANDBOX_URL` http。`e2b template build` 另需 `E2B_IMAGE_URI_MASK`（§11）。

### §3.2 数据面（仅 e2b，proxy 透传到 guest envd，本组件不实现协议）

envd 单端口 **49983**，H2C，Connect-RPC（proto 包无版本）：`process.Process`、`filesystem.Filesystem`（仅元数据）；
**文件内容走 HTTP `GET/POST /files`**（+ 签名 query）；另有 `/health`/`/init`/`/metrics`。
每操作用户走 `Authorization: Basic base64("user:")`。**code-interpreter** = `POST https://49999-<sid>.<domain>/execute`（NDJSON）→ guest FastAPI(:49999)→Jupyter(:8888)。

### §3.3 鉴权与两 token

- 控制面 `X-API-KEY` / 构建面 `Bearer`：api_key 是 manifest_key 派生的 MAC 令牌（§7），orchestrator 据此解析租户并校验归属，无静态 api_keys。
- **数据面 token = `envd_access_token`，头 `X-Access-Token`**（与原版 e2b 一致；secure 沙箱自 SDK v2.0.0 默认开，SDK 每次数据面调用带此头）：create 铸造 → 回 SDK。**proxy 校验** `X-Access-Token` 匹配该沙箱 token（`data_plane_auth ∈ {off|log|enforce}`，默认 **enforce**；external 模式在 worker，§2.1）。**例外放行**：auth=off、route 无 token（bare）、envd 预签名文件 URL（带 `signature` query，envd 自验签）。envd **是否另行**自校验此 token 由 `mmds.enabled` 决定（§3.3.1）；无论哪种姿态，envd 仅经 proxy 的 host-UDS 可达（控制口 49983/49999 走 connect-UDS，非 floatingip），沙箱间无通路。
- create 响应仍返回 `trafficAccessToken`（SDK 兼容字段；非数据面强制头）。bare 无 envd：回 stub `envdVersion(≥0.1.0)` + 占位 token，数据面控制口 proxy 回 **501**。

### §3.3.1 envd 鉴权姿态与快照扇出（`mmds.enabled`）

快照扇出的子沙箱（`export-sandbox --to-template`，或任何 snp 模板 create）由内存恢复，其 envd 持**源**沙箱 token；`-isnotfc` 下 envd 不能把 token 改成新身份的值（envd `/init` 仅允许首设/重确认同 token/匹配 MMDS hash），会拒绝子沙箱数据面。单开关 `mmds.enabled` 选两种姿态，二者都让扇出沙箱数据面可用：

| | `enabled=false`（默认） | `enabled=true` |
|---|---|---|
| envd 启动 | `-isnotfc` | FC 模式（去 `-isnotfc`） |
| envd token | orchestrator 不下发（`/init` 省 `accessToken`）→ 非 secure | 经 MMDS 授权后 `/init` 重置为每身份新 token |
| 数据面闸门 | 仅 proxy（须 `data_plane_auth=enforce`） | proxy + envd（纵深防御） |
| 扇出 fork | ✅（envd 无 token 故不错配） | ✅（envd 重置为新 token） |

- **proxy-only（默认）**：依赖 envd 仅经 proxy host-UDS 可达 + 沙箱间无通路 + host 信任边界；故 `enabled=false` 强制 `data_plane_auth=enforce`（proxy 是唯一外部闸门）。
- **MMDS（`enabled=true`）**：orchestrator 在 **proxy 组件**内起 Firecracker MMDS v2（internal：serve 绑 `mmds.listen`；external：worker `--mmds-listen`）。两段式：**PUT `/latest/api/token`** 按请求**源 IP = 沙箱 floatingip**（vswitch mgmt-extract 已 SNAT）查路由表、**park 等到该沙箱注册上来**（复用数据面 park 语义；故 external 模式**无**"先注册后轮询"的时序约束），回一个 HMAC 签名、编码了沙箱 id 的 session token；**GET `/`** 校验并解码该 token（沙箱内代码不可信，**不**复读源 IP）取沙箱 id，回 `{instanceID,envID,accessTokenHash}`，`accessTokenHash = hex(sha512(token))`（= envd `keys.HashAccessTokenBytes`）。部署侧用 vswitch `--mgmt-service 169.254.169.254:80:<mmds.listen>` 把该 VIP 在 eBPF 数据面直译到 `mmds.listen`（**无 iptables**，且自动改写回程；loopback target 需 mgmt 设备 `route_localnet=1`），故本进程不占特权口、不需 root。要求 `proxy.mode≠off`（MMDS 寄宿 proxy）。

## §4 Profile 模型与 templateID（transient / persist 两形态，无 templates 表）

```
persist templateID = <profile>-<kind>-<key>     profile∈{e2b,bare} ; kind∈{img,snp} ; key=manifest content key(64hex)
transient templateID = transient-<uuidv7>        构建注册期临时句柄，build 完即弃
```

- **持久 id**（如 `e2b-snp-<64hex>`）**自描述**、即 manifest 键，是 create 沙箱的正式 templateID；运行期 profile/kind 从前缀解析。runtime.erofs：`bare`=基础 `sandbox-runtime.erofs`，`e2b`=`sandbox-runtime-e2b.erofs`。
- **临时 id**：`POST /v3/templates` 注册期由 uuidv7 生成；构建完成后持久 id 作为该模板的 **names + aliases** 一并返回（`GET /templates` 的 `templateID` 直接列为持久 id），之后只用持久 id。
- **无 templates 表**：`builds` 表兼任模板登记（§9）；持久 id 由构建产物推导，不另存一份。
- create 沿用 restore 同款 runtime.erofs 校验，profile/kind 选错当场 4xx。

## §5 进程管理（systemd 模板单元，启动时自动生成安装）

orchestrator-ctl `serve` 启动时**生成并安装**两个模板单元 + 两个 slice 到 `unit_dir`（默认 `/etc/systemd/system`），
内容变更才 `daemon-reload`（D-Bus `Reload`）。单元名、unit_dir、ExecStart 路径均可配；ExecStart 所用
`sandbox-ctl`/`orchestrator-ctl` 路径默认从 **orchestrator 自身所在目录**探测（`exec_dir`）。`install_units:false` 则交由运维带外管理。

**runner 单元**（`%i`=sid）：

```ini
# sandbox-runner@.service  (生成内容)
[Service]
Type=exec
WorkingDirectory=/run/sandbox/%i
ExecStart=<orchestrator-ctl> run-sandbox --pidfile=/run/sandbox/%i/%i.pid --config-socket=/run/sandbox/orchestrator.socket --sandbox-id=%i
Restart=no                       # 一进程一沙箱、有状态：崩=该沙箱已死，不重试
KillMode=control-group           # StopUnit 连 cloud-hypervisor 一并 SIGKILL（见 §5.1）
TimeoutStopSec=20
Slice=sandbox-runner.slice
Delegate=yes                     # 委派 cgroup 控制器，使 --cgroup-adopt 能写 cpu.max/memory.max（见 §5.1）
```

**builder 单元**（`%i`=build id，§11）：

```ini
# sandbox-builder@.service  (生成内容)
[Service]
Type=oneshot
WorkingDirectory=/run/sandbox/%i
StandardOutput=file:/run/sandbox/%i/%i.result   # 捕获 flatten-ctl stdout 的 manifest key
StandardError=journal                           # 否则 StandardError 默认 inherit 会把 stderr 也写入 .result，污染 key
ExecStart=<orchestrator-ctl> run-builder --pidfile=/run/sandbox/%i/%i.pid --config-socket=/run/sandbox/orchestrator.socket --build-id=%i
TimeoutStartSec=1800
KillMode=control-group
Slice=sandbox-builder.slice       # 资源池 cgroup 上限（builder_cpu_quota/builder_memory_max）
```

ExecStart 为 `orchestrator-ctl run-sandbox`（runner）/ `run-builder`（builder）（同框架，§6）：锁+写 pidfile → 取 LaunchSpec →
`execve` 替换为目标（sandbox-ctl / flatten-ctl），目标**继承本 PID 与单元 cgroup**。`Type=exec` 故无需 sd_notify。

- **create 流程**：建 `/run/sandbox/<sid>/`(tmpfs) + `/var/lib/sandbox/<sid>/`(disk) → `vswitch-ctl attach <switch>` →
  **写 `<sid>.yaml`（非密 SANDBOX_CONFIG）** → `StartUnit("sandbox-runner@<sid>.service","replace")` → 等就绪 → envd `/init` → 登记 sqlite + 起 TTL。不预建 cgroup；密钥与 restore/connect 经 run-sandbox 的 LaunchSpec 下发（§6）。
  - **冷启动可写上层**：img 冷启时 `<sid>.yaml` 的 `boot.root.overlay.diff_template` 取自 config `overlay_diff_template`——一块**预格式化空 ext4**（sandbox-ctl 稀疏复制为可写 upper；裸空 diff 非合法 fs 会被拒）。部署方提供（`mkfs.ext4` 于稀疏文件，如 `/opt/sandbox/overlay-templates/basic-1G.ext4`）；restore（snp/resume）不需要（overlay 链来自快照）。
- **kill**：`StopUnit`（`KillMode=control-group` 连 CH 一并 SIGKILL）→ `ResetFailedUnit` → `vswitch-ctl detach <port>` → 删 run-dir → 标 dead。
- **就绪**：`Type=exec`：exec 成功即视为启动；e2b 再轮询 envd `/health` 判数据面就绪。
- **存活权威**：`ListUnitsByPatterns("sandbox-runner@*.service")` 一次拿权威存活集（§9）。
- 宿主 `Restart=no` vs **客户机内 envd `restart=always`**（sandbox-init 管，两层别混）。

### §5.1 cgroup（cgroup-adopt：单元自身 cgroup 即沙箱资源 cgroup）

不再预建 `/sys/fs/cgroup/sandboxes/<sid>`、不再让 sandbox-ctl 做进程搬迁。sandbox-ctl 新增 **`--cgroup-adopt`**：
它**接管自己所在 systemd 单元的 cgroup**作为沙箱资源 cgroup——读 `/proc/self/cgroup` 求出该路径、把 cloud-hypervisor 留在原地、自身也在其中。于是：

- 一个单元 = 一个沙箱 cgroup；sentinel 在该路径上原地仲裁；`KillMode=control-group` 使 `StopUnit` 连 CH 一起 SIGKILL，**无需 orchestrator 排空/rmdir 安全网**。单元须 **`Delegate=yes`**：否则单元 cgroup 的控制器接口文件（`cpu.max`/`memory.max`）非本进程可写，`--cgroup-adopt` 写资源上限会 `permission denied`。
- 代价：sandbox-ctl 与 CH 同处受限 cgroup，`memory.high` 节流存在死锁风险（见 `pkg/sandbox/cgroup.go` 头注）。采纳此模型并照常设 `memory.high`；`--cgroup-adopt` 在 run-sandbox 下发的 LaunchSpec args 里，需要时去掉即退回旧的预建-cgroup 模式。

## §6 本机控制 socket（task / admin / api 三面）

orchestrator 在 UDS **`/run/sandbox/orchestrator.socket`**（`paths.config_socket`，**0600**）跑一个 **h2c HTTP** 服务（兼容 HTTP/1.1）；单 socket 复用三个平面、各自鉴权；连接建立时经 **`SO_PEERCRED`** 取 peer pid 注入请求上下文。socket 0600 ⇒ 仅 daemon 同 uid / root 可连，各平面在此之上再细分。`/internal/*` 前缀 e2b SDK 永不使用、与 api 路径不冲突。

**① task 平面** —— `POST /internal/task/launchspec`，下发**通用启动规约 LaunchSpec**（run-sandbox/run-builder 据此 `execve` 替换为目标进程；非密沙箱配置走文件 `<sid>.yaml`、密钥走 `LaunchSpec.env`）：

- 协议：req `{config_id}`（`sandbox:<sid>` | `build:<bid>`）→ resp **LaunchSpec** `{exec, args, workdir, env}`：
  - sandbox：`exec=sandbox-ctl`，`args=[run --sandbox-id <sid> --config <rundir>/<sid>.yaml --manifest-config <shared> --run-root <run_root> --cgroup-adopt (--restore …) (--connect …)]`，`env={MANIFEST_KEY}`；`--run-root` 把 sandbox-ctl 的 socket/staging 目录（`ch.sock`/`ctl.sock`/…）钉到 orchestrator 的 run_root，使 RunDir == `<run_root>/<sid>`，pause/snapshot 客户端（同 `--run-root`）才能拨到 `ctl.sock`（否则 sandbox-ctl 默认 `/run/sandbox`，目录分裂、快照拨号失败）；
  - build：`exec=flatten-ctl`，`args=[export --config <rundir>/flatten.yaml --manifest-config <shared> --upload <fromImage>]`，`env={MANIFEST_KEY}`。
- **run-sandbox / run-builder**（每个单元的 ExecStart，§5）：`--pidfile` 用 `fcntl(F_SETLK)` 排他锁防重入、写 PID、清 `FD_CLOEXEC` 使锁随 `execve` 存活（**不清理 pidfile**）→ 拨 socket 取 LaunchSpec → `chdir(workdir)` + 清 `TASK_*` env + 合 `spec.env` → `execve` 替换为目标（目标继承本 PID 与单元 cgroup）。
- **鉴权**：peer pid ⟷ `/run/sandbox/<id>/<id>.pid`（启动器拨号前已写本 PID），相等即认证。
- build 结果：flatten-ctl 把 manifest key 打到 stdout，单元 `StandardOutput=file:<rundir>/<bid>.result` 捕获，orchestrator 读回（§11）。

**② admin 平面** —— `/internal/admin/manifest-keys`（`GET`=list，`POST {op:add|remove|check, key, label, ttl_seconds}`）：manifest-key 白名单管理（含 TTL，§7）。**鉴权**：配 `paths.admin_pidfile`（多行 PID 白名单、`#` 注释）则 peer pid 须在其中；未配则仅靠 socket 0600（同 uid/root）。`orchestrator-ctl manifest-key …` 即此平面客户端——**daemon 是 manifest_keys 表唯一写者**，CLI 不再开 DB（亦不读 config，仅需 `--socket`/`ORCHESTRATOR_SOCKET`）。

**③ api 平面** —— 其余路径回落到 **e2b 控制面 handler**（与 TLS `api.listen` 同一 `http.Handler`，含 export/import 扩展），明文 h2c 暴露、**`X-API-KEY` 鉴权**。`export-sandbox`/`import-sandbox` CLI 即此平面客户端（§8.1）。

**信任模型**：host-root / daemon-uid 可信；租户代码在 guest 内、够不到 host UDS。

## §7 密钥与归属模型（manifest_key 根密钥，加密存 sqlite）

- **manifest_key 是每租户根密钥**（32B / 64-hex，亦即 manifest 内容键 / `MANIFEST_KEY` env / `manifest.key`）。**api_key 由它派生**（`e2b-key-ctl gen-apikey`）：
  `api_key = e2b_ + hex( fp(12)‖ts(4)‖nonce(4)‖mac(16) )`，`fp=SHA256(mk)[:12]`、`mac=HMAC-SHA256(mk, fp‖ts‖nonce)[:16]`（`e2b_`+72hex=**76 字符**）。**e2b SDK 校验 api_key 格式 `/^e2b_[0-9a-f]+$/`**（实测 CLI 2.10.3 内置 js-sdk 会拒绝非 hex，故用 hex 编码——非 base64url），orchestrator 另自校验 MAC（`internal/apikey`）。
- **加密落盘**：`manifest_keys` / `sandboxes` / `builds` 表的 manifest_key 字段 **AES-256-GCM 加密**（`internal/secretbox`），另存 `manifest_key_hash = fp`（非唯一索引，快速匹配/排除）。加密密钥经 config `encryption_key` / env `ORCHESTRATOR_ENCRYPTION_KEY`（`:`-分隔多键、[0]活动、备用键解旧记录支持轮换）。
- **鉴权解析**（短 hash 匹配 + 完整 MAC 校验）：api_key → 按 `fp` 命中行/白名单 → 解密 manifest_key → 重算 HMAC 比对：
  - **create / build**：manifest_key 须在 `manifest_keys` 白名单（`orchestrator-ctl manifest-key add|remove|check|list` —— 经本机控制 socket 的 **admin 平面**打到 serve daemon（§6），daemon 是该表唯一写者；输出仅指纹、绝不含 key），否则 **403**。`add --ttl <dur>` 设失效时间（如 `24h`；`0`/缺省=永不），**重复 add 刷新失效时间**（简化外部清理）；过期 key 即视为不在白名单（reaper 惰性 `PruneExpiredManifestKeys` 清理），`list` 显示 `expires`。另：行内可存**租户默认镜像拉取凭据**（`add --registry-auth <docker.json>` 或 `--registry-username/--password/--token` 自动组装、加密存 `registry_auth_enc`；构建拉取用，§11）。
  - **其他按 id / list 操作**：只对资源行自身的 manifest_key 校验，**不查 manifest_keys** —— 即清空白名单，存量 sandbox/build 仍可正常操作。
- 收敛加密：manifest_key 只封 manifest 密钥表；chunk 加密 `SHA256(salt‖明文)`、与 key 无关 ⇒ **chunk 去重仍跨租户**；租户**不共享 key/模板**。
- **key 不落明文**：sqlite 内加密；运行期只在 orchestrator 内存 + 启动器 LaunchSpec 帧 + sandbox-ctl/flatten-ctl 子进程 env（`MANIFEST_KEY`），`<sid>.yaml` 非密不含 key。auto-resume 从加密存储解出 key 解封（数据面唤醒无 api_key）。

## §8 生命周期与状态机

- create(img=冷启/snp=restore)、connect=resume、pause=snapshot、timeout=TTL→pause、kill。
- **每实例配置覆盖（create 经 metadata，零 SDK/API 改动）**：create 可在 e2b `metadata` 里带保留键 **`kuasar-sandbox/config`**（值为 JSON 串，metadata 值本就是字符串），覆盖该沙箱的网络项——`hostname`/`dns`/`inner_ip`(CIDR)/`nexthop`/`transit_gateway_ip`/`transit_geneve_vni`/`transit_mac`（GENEVE 经 `vswitch-ctl attach --transit-*`）。空字段回落 profile/config 默认；仅做**格式校验、无白名单门**（沙箱以完整能力对外发布，平台自身亦经 sandbox API 管理）。metadata 持久化 → **resume 重新解析**、覆盖在沙箱全生命周期一致。实现：`internal/orch/override.go` `parseOverrides` → `launch`(allocInnerIP/`vswitch.AttachReq`) + `sandboxParams`(hostname/dns/nexthop)。
- **auto-suspend**：TTL/空闲 → reaper 触发运行中 sandbox-ctl 封快照、记 `snapshot_ref`、标 paused、StopUnit、detach。
  封快照按 **`checkpoint.mode`**（§8.1）：`local`（默认）→ `snapshot --output` 落本机文件（`snapshot_ref`=本机 bundle 路径，节点绑定）；
  `remote` → `snapshot --upload` 落远程 manifest（`snapshot_ref`=`manifest://<key>`，可移植）。
- **auto-resume**：数据面流量打到 paused 沙箱 → orchestrator 读 sqlite → 建目录 + attach + `StartUnit` →
  config-socket 下发 `restore=<snapshot_ref>` + key → sandbox-ctl 解封恢复。`snapshot_ref` 为完整引用
  （`manifest://<key>` 或本机路径），`sandbox-ctl run --restore` 两形态皆收（旧裸 key 行向后兼容补 `manifest://`）。
  - **单飞**：同一 sid 的并发数据面请求经 per-sid single-flight 合并为**一次** resume，杜绝重复 IP 分配 / attach / StartUnit 竞态。
  - **internal**：proxy 在请求内同步触发 resume（有界）。**external**：worker 经 routesync 上行 `Wake{sid}` → orchestrator 单飞 resume → 回灌 `Upsert(running)` → worker 解挂；resume 未在 `park_timeout` 内就绪 → worker 回 **503**（§2.1）。
- 终态仅 `paused`(snapshot) / `dead`(kill/TTL/整机故障)。`kill` 在进程层经 StopUnit 生效，不受 guest 内 restart 策略阻挡。

### §8.1 暂停态分层、转模板与跨机迁移（checkpoint / export-sandbox / import-sandbox）

沙箱的 snapshot 有两态：**本机快照**=`snapshot --output` 落本机文件（节点绑定、只能本机 resume，恰合"沙箱附着宿主"）；
**远程快照**=manifest（`--upload` 直传或晋升而得，**可移植、本质即模板**）。`checkpoint.mode` 选 pause 默认落地（默认 `local`）。
全部基于现有 sandbox-ctl 原语（`snapshot --output|--upload`、`upload-snapshot <本机快照>`、`run --restore`），e2b API/CLI 零改动。

`export-sandbox` / `import-sandbox` 两个 CLI 是 serve daemon **api 平面**的客户端（经本机控制 socket，§6；`POST /sandboxes/{id}/export`、`POST /sandboxes/import`，`X-API-KEY=E2B_API_KEY`，**不读 config、不开 DB**）——晋升/导出/插行全由 daemon 在进程内完成，故无第二写者。两接口在 TLS `api.listen` 上同样可达（api-key 已按租户隔离）。

- **晋升 / 转模板**：`orchestrator-ctl export-sandbox <sid> --to-template`（`E2B_API_KEY` 鉴权、须暂停）——若本机快照则先
  `upload-snapshot` 晋升为远程 manifest（并 repoint 该行），随后**组装并打印自描述持久 id `<profile>-snp-<key>`**（**不写 builds 表**）。
  之后 `e2b sandbox create <id>` 即从该快照扇出新沙箱（新 sid）。create 解密用的租户 key 仍由 api_key→白名单解析，故无需登记。
- **迁移（同 sid 跨机）**：`export-sandbox <sid>`（不带 `--to-template`）确保远程后，导出**单行 base64 token**＝沙箱行
  （含 env/metadata/deadline/数据面 token，**manifest_key 仅指纹、不含密钥**，附 runtime 摘要）；默认**回收源行**（move；`--keep-source`=拷贝）。
  目标机 `import-sandbox <token>`：`E2B_API_KEY`→白名单解析租户 key（**须先 `manifest-key add`**，同 create 前置）、校验 token 指纹与本机
  runtime 摘要一致，插入 paused 行 → `Sandbox.connect(<sid>)` 在新机恢复。目标机须共享同一 `manifest_config`（远程 store）。
  - **一步迁移（推荐）**：`Sandbox.connect(<sid>, api_headers={"X-Kuasar-Migration-Token": <token>})`——目标机本地无此 sid 且带迁移 token 时，connect 在 resume 前**自动 import**（同上租户/指纹/runtime 校验，且 `token.ID` 须 == 连接的 sid，否则报错并回收误插行），迁移收敛为**单次 SDK 调用**；`import-sandbox` CLI 保留作显式预导入。
  - 限制：token 不含系统密钥但携带沙箱自有 env/数据面 token，按"沙箱级敏感"对待。export/import 由 daemon 在进程内改表（无第二写者）；目标机须先 `manifest-key add` 本租户 key 且共享同一远程 store。

## §9 状态存储与重启对账

sqlite，路径由 config `db_path` 配置（默认 `<base_root>/orchestrator.db`，如 `/var/lib/sandbox/orchestrator.db`）。

`sandboxes`：`id(uuidv7) PK, template_id, state(running|paused|dead), deadline_unix, run_dir, base_dir,
envd_uds, ci_uds, floatingip, vswitch_port, inner_ip, port_mac, manifest_key_hash, manifest_key_enc,
snapshot_ref, envd_access_token, traffic_access_token, metadata_json, env_json, created_unix`。

`builds`（兼模板登记）：`build_id PK, template_id(transient-…), persist_id(<profile>-<kind>-<key>),
manifest_key_hash, manifest_key_enc, profile, kind, from_image, start_cmd,
status(registered|waiting|building|ready|error), reason, names_json, aliases_json, created_unix`。

`manifest_keys`（create/build 白名单）：`key_hash(索引), key_enc, label, created_unix`。三表的
`manifest_key_enc` 均 AES-256-GCM 加密、`*_hash` 为 `SHA256(mk)[:12]` 非唯一索引（§7）。

**重启对账**：以 `ListUnitsByPatterns("sandbox-runner@*.service")` 为存活权威：单元 active 且 sqlite running ⇒ adopt（重挂路由、重武装 TTL）；running 但无对应 active 单元 ⇒ 清理标 dead；run_root=tmpfs ⇒ 整机重启后 running 全判 dead，`paused`/snp 记录保留可被 connect/auto-resume 拉起。

## §10 guest profile：envd 嵌入

- 独立 **`sandbox-runtime-e2b.erofs`**（主 runtime 保持精简）；`deps/build-runtime-e2b.sh`（纯 shell，shell out `fsck.erofs`/`mkfs.erofs`）把固定版本 envd 注入到运行时层的 **`/opt/sandbox-runtime/bin/envd`**，并把成品 **补齐到 2MiB 对齐**（virtio-pmem 后端要求 size 为 2MiB 整数倍，否则 cloud-hypervisor `PmemSizeNotAligned`；EROFS superblock 自描述范围，尾部稀疏 padding 对 guest mount 不可见——与 `sandbox-runtime.erofs` 同款）。
- **零 sandbox-init 改动**：`/opt/sandbox-runtime` 已被 sandbox-init **自动 bind-mount** 进 guest 同名路径，故 envd 在 guest 内即 `/opt/sandbox-runtime/bin/envd`，直接作 `launch.exec`：`launch.exec=/opt/sandbox-runtime/bin/envd -isnotfc -port 49983`、restart=always。
- envd 是 **sandbox-deps** 的原生构建产物（与 cloud-hypervisor/vmlinux/mkfs.erofs 并列）：`make -C sandbox-deps envd` 经 `deps/build-envd.sh` 拉取 `e2b-dev/infra` 发布 tarball（默认 tag `2026.22`，`ENVD_TARBALL` 可覆盖）→ `build/tarball` 缓存 → extract 到 `build/src/e2b-infra` → `go build packages/envd` → `bin/<arch>/envd`。orchestrator 的 `make sandbox-runtime-e2b` 再把它（默认从 umbrella `bin/<arch>/envd`，`ENVD=` 可覆盖）注入裸 runtime。
- **userland 门槛**（文档约束）：base/客户镜像须有 `bash`、`coreutils/util-linux`、**预建 `/init` 默认用户（默认 `user`，含 `/home/user`）**、`cgroup v2`、可写 `/run`。envd 跑每条 guest 进程时以默认用户、并包一层 `ionice -c 2 -n 4 nice -n N "$@"`（须有 `/usr/bin/{ionice,nice}`）——故缺用户或缺 util-linux/coreutils 会 `invalid default user` / `ionice: not found`（实测裸 alpine 两者皆缺）。
- **guest 须有 `/etc/hosts`（实测根因，端口转发相关）**：展平的 docker 镜像**不带 `/etc/hosts`**（docker 仅容器运行时注入）。guest 内 `socket.getfqdn(hostname)`——许多服务器在 bind 后调它，如 Python `http.server.server_bind()` 恰在 `bind()` 与 `listen()` **之间**——查无本地条目即落到 DNS（`resolv.conf`，e2bdev=`1.1.1.1`），解析主机名 `sandbox` 阻塞 **~20s**（此间端口已 bind 但未 listen、`ss` 看不到 listener），令"host→floatingip 应用端口转发"看似失败。**已治本**：orchestrator 经 SANDBOX_CONFIG **既有 `files:` 机制**注入 `/etc/hosts`（含 `127.0.1.1 <hostname>` 条目）+ `/etc/resolv.conf`，并经 `network.hostname` sethostname（launch+restore 均生效），任何解析主机名的应用即时启动（实测 `getfqdn` 20.23s→0.01s）。guest DNS 由 `sandbox.network.dns`（默认 `169.254.169.253`）决定，该地址需部署侧路由到真实 DNS（demo 加一条 iptables DNAT 到本机首个 nameserver 以跑通）。
- host 启动后调 envd **`POST /init`**（经 UDS）装 token/env/默认用户/workdir/时间。数据面 exec = `POST /process.Process/Start`（Connect-RPC，`X-Access-Token` 头 = create 返回的 `envdAccessToken`）。

## §11 模板构建（经 e2b API + builds 表 + 资源池）

构建经 **e2b API** 提交（无独立构建 CLI；客户用 `e2b template build`），落 `builds` 表，由资源池调度，分两类：

- **镜像 build（img）**：无 `startCmd`。资源池准入 → orchestrator 写 `<rundir>/flatten.yaml`（`referer.enabled:true`、`key=fromImage`、`tmpdir=<rundir>`；按配置加 `insecure`/`platform`）→ `StartUnit(sandbox-builder@<bid>)` → 单元内 `run-builder` 取 build LaunchSpec → `execve` 进 `flatten-ctl export --config <rundir>/flatten.yaml --manifest-config <shared> --upload <fromImage>`（`MANIFEST_KEY` 经 env）→ flatten-ctl 把 manifest 键打到 stdout、单元 `StandardOutput=file` 写 `<bid>.result` → orchestrator 读回 → persist id `<e2b-img-key>`。`referer.enabled` 走 flatten-ctl 的 OCI Referrers 幂等流：同 `(MANIFEST_KEY, key=fromImage)` 已存在则跳过重导出。
- **快照 build（snp）**：给 `startCmd`。先做 img，再以该 img 模板冷启一台 e2b 沙箱（走 `sandbox-runner@<bid>`）、
  等 envd 就绪、`sandbox-ctl snapshot --upload` → persist id `<e2b-snp-key>`，随后销毁临时沙箱。

**与真实 `e2b template build` 的桥接（client 端 docker build + push，server 端 flatten 所推镜像）**：CLI 在**客户端** `docker build` 整个 Dockerfile（`RUN` 步骤由客户端 docker 执行——**故 steps 可用**），再 `docker push` 到约定名 `<registry>/e2b/{templateID}:{buildID}`，trigger body **不带镜像引用**。orchestrator 据 `builder_image_uri_mask`（须与 CLI 的 `E2B_IMAGE_URI_MASK` 一致，含 `{templateID}`/`{buildID}` 占位）推出 `fromImage` = 客户刚 push 的镜像，再走上面**不变**的 flatten 路径。`E2B_IMAGE_URI_MASK` 一旦设置即让 CLI **跳过** `docker.<domain>` 的 `/v2/token` broker 与 `docker login`（无需 token broker；可 push 到本机 insecure registry）。配本机/私网 registry 时设 `builder_insecure_registry:true`（HTTP 拉取）、`builder_platform`（如 `linux/amd64`）。

> **资源池**：`builder_max_concurrent`（默认 2）= orchestrator 内计数信号量准入，`waiting→building` 用 builds 表
> CAS 抢占（多 worker/重启安全）；CPU/内存上限经 `sandbox-builder.slice` 的 `CPUQuota`/`MemoryMax` 施加（cgroup）。

> **镜像拉取凭据（三级、加密存、无 host 匹配于 token）**：build 触发时按优先级解析——**任务级 pull token**（SDK `api_headers` 头 `X-Kuasar-Pull-Token`，`e2b-key-ctl seal-pull-token` 用**租户 manifest_key 派生密钥**封装的不透明 `kpt_` 令牌，orchestrator 以该租户存量 key 解封）＞ SDK `fromImageRegistry`（`from_image(image,username,password)` 明文）＞**租户默认**（`manifest_keys.registry_auth_enc`，docker config.json；`manifest-key add --registry-auth <file>` 或 `--registry-username/--password/--token` 自动组装 catch-all `*`；按 fromImage host→`*`→首条取）＞匿名。解析结果（user/pass 或 bearer）**加密存 `builds.registry_auth_enc`**，flatten 时解出注入 `FLATTEN_REGISTRY_{USERNAME,PASSWORD|TOKEN}`（flatten-ctl `pkg/remote` 读）。
> **不落盘**：凭据仅加密存库 + 运行期子进程 env；`MANIFEST_KEY` 同经 run-builder LaunchSpec→env；`<rundir>/flatten.yaml` 仅 referer（key/desc）+ tmpdir + insecure/platform，无秘密。

> **运行时契约（已实测，`e2e_execute.sh`/`e2e_build_real.sh`）**：snp build 的 boot→`sandbox-ctl snapshot --upload`→64-hex 键→persist；以及 pause(snapshot)→connect(resume)→restore 全链路均经真实 microVM + envd 跑通；`startCmd` 经 guest env `E2B_START_CMD` 交 envd。

`sandbox-runtime-e2b.erofs` 由 `deps/build-runtime-e2b.sh`（Makefile `sandbox-runtime-e2b` 调用）纯 shell 经 `fsck.erofs --extract` → 注入 envd → `mkfs.erofs` 重打，**不再是 orchestrator-ctl 子命令**。

## §12 DNS / TLS

生产：`*.<domain>` + `api.<domain>` 通配 DNS+TLS（operator 提供，on-prem/离线友好；可选内置 ACME）。dev：`E2B_API_URL`/`E2B_SANDBOX_URL` http（h2c）。

## §13 契约边界

| 对象 | 方式 | 说明 |
|---|---|---|
| `sandbox-ctl`(runtime) | 经 run-sandbox(单元) `execve`：`run --config <sid>.yaml --manifest-config --cgroup-adopt`（key 经 env） | 非密配置文件 + 密钥 env；准入在其内部 |
| `node-ctl`(sentinel) | **无直接交互**；可选 `resource_socket` | 单元 cgroup 即沙箱 cgroup，sentinel 默认扫描原地仲裁。`resource_socket` **opt-in**（不配=静态 cgroup，单元自身 via `--cgroup-adopt`；配了才进 SANDBOX_CONFIG `resources.control.controller` 拨 sentinel） |
| `vswitch-ctl`(vswitch) | CLI（须 `cfg.Bin` 解析路径）`attach <switch>`/`detach`；`TapFD.Exec` 绑 port | 交换机预先起好（`ip netns add <ns>` + `vswitch-ctl start <sw> --netns --ports --mac-addr --floating-ip-base`；交换机运行于内核态，非守护进程，清理 `vswitch-ctl stop`）；port 对外、slot 内部 |
| `flatten-ctl`(builder) | 经 run-builder(单元) `execve`：`export --config --manifest-config --upload`（referer.enabled 默认开；按配置 insecure/platform） | manifest key 不透明串；`MANIFEST_KEY` env；`fromImage` 可由 `builder_image_uri_mask` 推出（§11） |
| `fsck.erofs`/`mkfs.erofs`(deps) | CLI（`deps/build-runtime-e2b.sh`） | 确定性展平/重打 e2b runtime |
| guest envd | UDS（经 sandbox-ctl `connect` 反代） | 原版不改；嵌于 `/opt/sandbox-runtime/bin/envd` |
| systemd | D-Bus（StartUnit/StopUnit/ListUnits/Reload） | 进程管理 + 单元自装 |
| `orchestrator-ctl proxy`(external) | UDS routesync（双向 h2c 帧化 JSON）：serve→ Snapshot/Upsert/Delete，worker→ Wake | 同节点、运维带外起（`deploy/orchestrator-proxy@.service`）；数据面口 SO_REUSEPORT 共享（§2.1） |

不新增胖导出面；`CGO_ENABLED=0`；Tier = 叶子。

## §14 协议版本 pin

envd `0.6.x`（按 infra 发布 tag 定版，sandbox-deps `ENVD_TARBALL`，默认 `2026.22`）；proto 包无版本 `filesystem`·`process`，JSON/H2C，端口 **49983**；
文件内容走 HTTP `/files`；code-interpreter **49999**；exec=`POST /process.Process/Start`（Connect-RPC，`X-Access-Token`）。控制面端点见 §3.1；构建 API **v2 系统**：register `POST /v3/templates`、
trigger `POST /v2/templates/{tid}/builds/{bid}`；**v1 系统**（CLI 2.10.3 默认）：create `POST /templates`、start `POST /templates/{tid}/builds/{bid}`（无 body）；共用 status `GET /templates/{tid}/builds/{bid}/status`、list `GET /templates`；
status enum 对外 `building|ready|error`（进行中恒 `building`，CLI wait 循环要求）；`X-API-KEY`（**api_key=`e2b_`+hex(76 字符)**，manifest_key 派生 MAC，SDK 校验 `/^e2b_[0-9a-f]+$/`）/ 构建 `Bearer`；`e2b template build` 经 `E2B_IMAGE_URI_MASK` 桥接（§11）；resume=`/connect`；pause 已暂停 409；
list 分页 `x-next-token`；默认超时 300s。**数据面鉴权头 `X-Access-Token`**（=envdAccessToken；§3.3）。SDK：`e2b` js 2.27.x / py 2.25.x。
routesync（external proxy，§2.1）：版本 1，帧 `[4B LE len][JSON]`，消息 `hello|hello_ack|snapshot|upsert|delete|wake`，标识头 `X-Orch-Routesync:1`。

## §15 交付顺序（依赖序）

```
build-runtime-e2b.sh（sandbox-runtime-e2b.erofs；envd 注入 /opt/sandbox-runtime/bin/envd 验证）
└─ orchestrator-ctl: store(sqlite: sandboxes + builds)
   ├─ launcher(systemd D-Bus: Start/Stop/List/Reload) + 单元自动生成安装 + 目录 + vswitch attach/detach
   ├─ config-socket(server: LaunchSpec(sandbox/build) + SO_PEERCRED·pidfile 鉴别) + run-sandbox/run-builder(锁 pidfile/execve)
   ├─ api(e2b 控制面 + 构建 v3 端点 + 鉴权 + 归属校验)
   ├─ provisioner(envd /init) + 就绪门
   ├─ proxy(L7 + h2c 上游 + 路由判据 + bare 501 + X-Access-Token 校验 + 单飞 resume)
   ├─ proxy 模式: internal | external(routesync serve↔worker 双向 h2c + worker SO_REUSEPORT + 兜底转发) | off
   ├─ reaper(TTL/auto-suspend) + proxy 触发 auto-resume + 重启对账
   └─ build(builds 表 + 资源池 + run-builder→flatten-ctl(img,stdout→.result) + boot+snapshot(snp))
```

## §16 配置（字段参考）

完整可注释样例见 **`deploy/config.example.yaml`**（亦即 `orchestrator-ctl config --template` 的输出骨架），
权威结构见 `internal/config/config.go`；daemon 的 systemd 单元见 **`deploy/orchestrator-ctl.service`**
（`ExecStart=orchestrator-ctl serve --config /etc/orchestrator-ctl/config.yaml`）。**配置按关注点分组**：
`api`（控制面+TLS）、`proxy`（数据面）、`paths`、`units`、`sandbox`（实例级，子组 `resources`/`network`/`boot`）、
`builder`、`checkpoint`（暂停态分层），外加顶层单值 `encryption_key` / `manifest_config`。**必填**仅
`api.domain` + `encryption_key`（或 `ORCHESTRATOR_ENCRYPTION_KEY` env）。**外部二进制**
（sandbox-ctl/vswitch-ctl/flatten-ctl/orchestrator-ctl）**不配置**——按"与 orchestrator-ctl 同目录 → PATH"
自动发现（`cfg.Bin()`；erofs 工具由 `deps/build-runtime-e2b.sh` 直接调）。以下为几处要点：

| 字段 | 默认 | 说明 |
|---|---|---|
| `paths.db_path` | `<paths.base_root>/orchestrator.db` | sqlite 路径，可配；随 `base_root` 派生，亦可显式覆盖（§9） |
| `paths.config_socket` | `/run/sandbox/orchestrator.socket` | 本机控制 socket（三平面 h2c：task/admin/api，§6）；run-sandbox/run-builder 与 manifest-key/export-sandbox/import-sandbox CLI 的连接点（CLI 经 `--socket`/`ORCHESTRATOR_SOCKET` 指向，不读 config） |
| `paths.admin_pidfile` | `""`（仅 socket 0600） | 可选：admin 平面（manifest-key）的多行 PID 白名单（`#` 注释）；配置则 peer pid 须在其中，未配则仅靠 socket 同 uid/root 权限（§6） |
| `sandbox.network.{e2b,bare}.{inner_ip,nexthop}` | e2b `169.254.0.21/30`+`169.254.0.22` ; bare `169.254.1.1/31`+`169.254.1.0` | **按 profile** 的 guest 内 IP/默认网关（每 profile 复用同一对、唯一身份是 floatingip）；e2b 用 **/30 + 网关**让 envd 端口转发可用 |
| `sandbox.network.{hostname,dns}` | `sandbox` ; `[169.254.169.253]` | guest 主机名（`network.hostname`→sethostname）+ DNS；orchestrator 经 SANDBOX_CONFIG **`files:`** 注入 `/etc/hosts`（含 hostname 条目，治 getfqdn DNS 卡顿，见 §10）与 `/etc/resolv.conf`（launch+restore 均生效） |
| `sandbox.resources.control_socket` | `""`（静态 cgroup） | sandbox-sentinel 资源控制 UDS（opt-in）；空=单元自身 cgroup |
| `checkpoint.mode` | `local` | 暂停态落地：`local`=本机文件（`checkpoint.local_dir`，节点绑定）/ `remote`=远程 manifest（可移植=模板）。见 §8 |
| `proxy.metrics_listen` | `""`（关） | 可选 Prometheus 文本端点（`data_requests_total{result=…}`/`gateway_forward_total` 等，§2.1） |

## §17 如何测试

跨仓 e2e 脚本集中在 umbrella **`kuasar-sandbox/test/e2e/`**（需多仓产物：vmlinux/CH/mkfs.erofs 等）：

| 脚本 | 覆盖 | 运行 |
|---|---|---|
| `e2e_orchestrator.sh` | 单元自动安装 + 控制面（`/health`、`X-API-KEY` 401 路径）+ 构建 API（v3 register/trigger/status、跨 key 归属 404） | `make test-e2e-orchestrator` |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器 + `config`/`info` CLI（纯用户态，无 root/systemd/KVM）：pidfile 锁、HTTP 取 LaunchSpec、execve、`TASK_*` 清理 | `make test-e2e-runtask` |
| `e2e_orchestrator_proxy.sh` | `proxy_mode=external` 全链路：serve（控制面）+ 独立 proxy worker（SO_REUSEPORT 数据面）+ routesync + 真实 microVM/envd，数据面**经 proxy** 走 | `bash test/e2e/e2e_orchestrator_proxy.sh` |
| `e2e_build_real.sh` | 真实镜像 build 后端（store-ctl + 本地 zot）：register → trigger（fromImage）→ poll 至 `status=ready` 出 persist id；e2b Python SDK `from_image` 走同款服务端展平路径 | `bash test/e2e/e2e_build_real.sh` |
| `e2e_execute.sh` | 从已建模板冷启真实 microVM 并在 guest 内执行；pause(snapshot)→resume 全链路 | `bash test/e2e/e2e_execute.sh` |

`make test-e2e-orchestrator` / `make test-e2e-runtask` 是 umbrella 已注册的聚合目标；其余四个脚本目前直接 `bash` 运行
（未注册为 `make` 目标）。各 Go 仓单元测试经 `make test`（在对应仓或 umbrella）。

## 修订历史

| 版本 | 日期 | 修改人 | 说明 |
|------|------|--------|------|
| v0.7 | 2026-06-05 | chenxiaohui | **真实 e2b CLI 2.10.3 端到端实测并适配**（build + execute 全绿）：api_key 改 **hex**（SDK 实校验 `/^e2b_[0-9a-f]+$/`，非 base64url）；实现 **v1 build system**（`POST /templates` + 无 body start）与 v3 并存，trigger 兼容真实 body；`e2b template build` 经 **`builder_image_uri_mask`**（配 CLI `E2B_IMAGE_URI_MASK`）桥接——client docker build+push、server 推出 fromImage 走原 flatten（steps 客户端执行故可用），跳过 token broker；status 进行中恒 `building`（CLI wait 循环）；builder 单元 `StandardError=journal`；config 加 `builder_insecure_registry`/`builder_platform`/`builder_image_uri_mask`。**execute**：真实 microVM 冷启→envd→guest 内执行→pause(snapshot)→resume 全链路实测；修 `vswitch.New`/`snapshot` 经 `cfg.Bin`、sandbox-ctl `run`/`snapshot` 加 `--run-root`（统一 ctl.sock 目录）、runner 单元 `Delegate=yes`（cgroup-adopt 写限额）、`resource_socket` 不再默认（空=静态 cgroup）、`overlay_diff_template`（冷启 ext4 upper）、runtime erofs 须真构建 + 2MiB pmem 对齐（`build-runtime-e2b.sh` 加 pad）；e2e `e2e_build_real.sh`/`e2e_build_cli.sh`/`e2e_execute.sh` |
| v0.6 | 2026-06-05 | chenxiaohui | proxy 可靠性/扩展性（不引入 Envoy，路径 A）：**proxy_mode ∈ {internal,external,off}**；external = 独立 `orchestrator-ctl proxy` worker（SO_REUSEPORT 共享数据面口）+ **routesync**（serve↔worker 双向 h2c 帧化 JSON：Snapshot/Upsert/Delete↓ + Wake↑，零 gRPC/protobuf）+ worker 本地路由表/请求挂起(park)/兜底转发；**单飞 resume**（per-sid 合并并发唤醒，修竞态）；**数据面鉴权移入 proxy 并纠正为真实 `X-Access-Token`**（`data_plane_auth=off\|log\|enforce`，默认 enforce；预签名 query 放行）——文档原 `E2B-Traffic-Access-Token` 系笔误（未改造 SDK 不发）；新增 `internal/{routesync,routetable,metrics}` + `orchestrator-ctl proxy` 子命令 + `metrics_listen`；config 加 `proxy_mode/proxy_sockets/data_listen/park_timeout/data_plane_auth/metrics_listen`；`deploy/orchestrator-proxy@.service`；e2e `e2e_orchestrator_proxy.sh`（真实 serve+worker+VM+envd 经 proxy） |
| v0.1 | 2026-06-03 | chenxiaohui | 初稿：e2b 兼容控制面 + envd-in-guest 代理 + e2b/bare 双 profile |
| v0.2 | 2026-06-03 | chenxiaohui | systemd 模板单元 + config-socket 动态配置/密钥 + manifest_key 存 sqlite + auto-suspend/resume |
| v0.3 | 2026-06-04 | chenxiaohui | config-socket 唯一配置通道（去 yaml/env/args）；cgroup-adopt（单元自身 cgroup）；单元启动自动生成安装（runner+builder）；归属校验全覆盖；构建改经 e2b v3 API + builds 表 + 资源池 + build-exec/flatten-ctl(--with-referer) + boot/snapshot；envd 嵌 `/opt/sandbox-runtime/bin/envd`（零 sandbox-init 改动）；templateID transient/persist 两形态；去 `build` CLI/metrics |
| v0.5 | 2026-06-05 | chenxiaohui | 反转密钥模型：**manifest_key 为根密钥、api_key 由其派生**（`e2b_`+base64url(fp12‖ts4‖nonce4‖mac16)，e2b SDK 不校验格式已核源码——**已被 v0.7 取代：改 hex 编码，SDK 实校验 `/^e2b_[0-9a-f]+$/`**）；manifest_key 字段 **AES-256-GCM 加密落盘** + `*_hash` 非唯一索引；新 `manifest_keys` 白名单表（create/build 查；其他操作不查）；config 去 `api_keys`、加 `encryption_key`(+`ORCHESTRATOR_ENCRYPTION_KEY` env，多键轮换)；新增 `orchestrator-ctl manifest-key {add|remove|check|list}` + 独立二进制 `e2b-key-ctl {gen-apikey|gen-key|fingerprint}`；403=非白名单 create/build、404=非属主 |
| v0.4 | 2026-06-04 | chenxiaohui | config-socket 改为通用 **run-task + LaunchSpec**（exec/args/workdir/env），非密配置回落文件、密钥走 env；sandbox-ctl 去 `--config-socket`（pkg/manifest 读 MANIFEST_KEY env）；单元 ExecStart 统一 `run-task`、`Type=exec`、pidfile `F_SETLK` 锁防重入跨 execve；去 `build-exec`（build 经 run-task→flatten-ctl，结果经 `StandardOutput=file` 捕获）；`build-runtime` 改 `deps/build-runtime-e2b.sh` 脚本；flatten-ctl 配置统一 `--config/FLATTEN_CONFIG`（artifact_type 常量、cache opt-in、referer.enabled、--platform）；新增 `sandbox-ctl info` / `flatten-ctl config` / `orchestrator-ctl config` |
