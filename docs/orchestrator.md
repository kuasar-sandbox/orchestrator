# orchestrator — 单机 e2b 兼容沙箱编排

`orchestrator-ctl` 是计算节点上的单实例常驻 daemon,对外提供一套 **e2b 兼容 API**,
把节点上的 microVM 沙箱以 e2b 协议暴露给客户端——未改造的 e2b SDK(python / js `e2b`、
`@e2b/code-interpreter`)与 e2b CLI 可直接指向本机运行。一身兼 e2b 的三角色:**api**
(控制面 REST:沙箱生命周期、模板构建、鉴权)、**orchestrator**(经 systemd 模板单元
拉起/停止 `sandbox-ctl` 与 `flatten-ctl`,调 `vswitch-ctl` 编排网络)、**proxy**(把
客户端到沙箱的数据面流量反代到 guest)。

沙箱本体由 `sandbox-ctl` 运行(microVM,cloud-hypervisor);e2b profile 的 guest 内
跑原版 envd(经注入 envd 的 `sandbox-runtime-e2b.erofs`,§10),orchestrator 经 UDS
反代其单端口协议(49983/Connect-RPC)。密钥模型以租户 **manifest_key** 为根:api_key
由它派生(MAC 令牌),库内只存 AES-GCM 密文,密钥永不落明文盘(§7)。

产物两个二进制:**`orchestrator-ctl`**(daemon + 启动器 + 管理 CLI,§2)与
**`e2b-key-ctl`**(纯派生凭据工具,无 DB/config/编排状态,§2.8)。

## 1. 概述

### 1.1 业务问题

平台的沙箱栈(runtime/accelerator/builder/vswitch)各自提供 CLI 原语:起一台 microVM、
展平一个镜像、attach 一个端口。缺一个北向面把它们组合成"客户可用的服务":客户拿着
现成的 e2b SDK/CLI 与一个 api key,要能 create/exec/pause/resume/kill 沙箱、构建自定义
模板,而不感知 microVM、manifest、eBPF 交换机的存在。

orchestrator 补这一层,并刻意选择 **e2b 协议兼容**而非自定义 API:e2b 的 SDK 生态
(代码解释器、agent 框架集成)直接可用,客户端零改造;协议契约清晰(端点、token、
状态机皆有参照实现),兼容性可用真实 SDK/CLI 端到端验收。

### 1.2 设计原则

1. **依赖面薄,经 CLI 组合**:驱动 `sandbox-ctl`(run/snapshot)、`vswitch-ctl`
   (attach/detach)、`flatten-ctl`(export)全部经子进程 CLI,不 import 兄弟仓内部包;
   经 systemd D-Bus 管单元;经 UDS 反代 envd。叶子组件,纯 Go,`CGO_ENABLED=0`。
2. **不感知资源仲裁**:沙箱准入/配额在 `sandbox-ctl` 内部(它是 `pkg/resource` 的
   client);orchestrator 不 import `pkg/resource`、不拨 sentinel。构建任务的资源池
   由 orchestrator 自管(§11)。
3. **进程管理交给 systemd**:一沙箱一单元(`sandbox-runner@<sid>`),单元 cgroup 即
   沙箱资源 cgroup(`--cgroup-adopt`,§5.1),`StopUnit` 即完整回收;orchestrator 不
   自己当进程监督者。
4. **密钥不落明文盘**:租户 manifest_key 库内 AES-256-GCM 加密,运行期只经内存与
   启动器 LaunchSpec 的 env 帧传递(§6、§7)。
5. **路由权威单点,数据面可外置**:orchestrator 是路由与生命周期的唯一权威;数据面
   proxy 可内置(单二进制)或外置为独立 worker 进程(routesync 推送路由,§9)。
6. **重启可对账**:状态在 sqlite + systemd 单元集,orchestrator 重启后以单元集为
   存活权威对账收养/清理(§14)。

### 1.3 两类沙箱(profile)

| profile | 是什么 | 数据面 | 对外服务 |
|---|---|---|---|
| **e2b** | guest 内跑原版 envd,agent 经其 exec / 读写文件 / 跑代码 | 支持(fs/process/pty/runCode,经 envd 代理) | envd + floatingip 用户端口 |
| **bare** | 把客户镜像当网络化 microVM 跑起来,无 envd | 不支持(控制端口回 501) | 仅 floatingip 网络 |

`bare` 直接复用基础沙箱运行时(`sandbox-runtime.erofs`),把基础沙箱接上北向 API;
经 e2b API 构建的模板恒为 **e2b** profile。profile 编码在 templateID 前缀里(§4.4),
运行期据此选 runtime erofs 与数据通路。

### 1.4 边界与依赖

- 北向客户:e2b SDK / CLI 直连;平台管理面代理同样经此 API 对接,不属本仓。
- 不实现 envd 协议:数据面只透传到 guest 内原版 envd(§4.3)。
- 不提供 sandbox metrics 端点(e2b API 的 `/sandboxes/{id}/metrics` 面)。
- 构建不支持 server 端执行 Dockerfile steps:服务端只对一个已存在的镜像引用做拉取 +
  展平(§11)。
- 单机:路由、存储、单元管理都是节点本地的;跨机协作仅经远程 manifest store 携带
  快照/模板(§8.1)。
- 依赖:stdlib + `modernc.org/sqlite`(纯 Go)+ `golang.org/x/net/http2`(h2c)+
  `golang.org/x/sys`(pidfile 锁 / SO_REUSEPORT / SO_PEERCRED)+ `coreos/go-systemd`
  (D-Bus)+ `google/uuid`(v7)+ `gopkg.in/yaml.v3`。无 gRPC/protobuf。

### 1.5 架构与数据通路

```
          e2b SDK / CLI
               │ https://api.<domain>          https://<port>-<sid>.<domain>
               ▼                                        ▼
      ┌─ orchestrator-ctl serve ──────────────┐   ┌─ data plane ─────────────┐
      │ api: e2b control plane (REST)         │   │ proxy (internal mode:    │
      │   sandboxes create/connect/pause/...  │   │ in-process; external:    │
      │   templates register/trigger/status   │   │ orchestrator-ctl proxy   │
      │ orchestrator:                         │   │ workers, route-synced)   │
      │   vswitch-ctl attach → floatingip     │   │  port 49983/49999 → UDS  │
      │   StartUnit(sandbox-runner@<sid>)     │   │   (guest envd)           │
      │   envd /init → sqlite + TTL           │   │  other ports →           │
      │ build: builds table → pool →          │   │   floatingip:port        │
      │   StartUnit(sandbox-builder@<bid>)    │   └──────────┬───────────────┘
      └──────┬───────────────┬────────────────┘              │
             │ D-Bus         │ UDS config-socket             │
             ▼               ▼ (LaunchSpec / BuildSpec,      │
      systemd units            secrets in env)               │
      + slices         run-sandbox → execve sandbox-ctl run  │
                       run-builder → drives the 3-phase     │
                         build (import/steps/template VMs    │
                         as direct children, §11)            │
                             │                               │
                             ▼                               ▼
                       cloud-hypervisor microVM ◄── tap ── vswitch (eBPF)
                         guest: sandbox-init + envd
```

create 流程:建 `<run_root>/<sid>/`(tmpfs)+ `<base_root>/<sid>/`(disk)→
`vswitch-ctl attach` 拿 `{port, floatingip, mac}` → 写非密配置 `<sid>.yaml` → 登记
sqlite → `StartUnit(sandbox-runner@<sid>)` → 单元内 `run-sandbox` 经 config-socket 取
LaunchSpec(密钥经 env)后 `execve` 成 `sandbox-ctl run` → 起 microVM → (e2b)等
envd `/health` 就绪(60s 上限)→ `POST /init` 置 env/默认用户 → 起 TTL。

数据面按 `Host`(`<port>-<sid>.<domain>`)或 `E2b-Sandbox-Id`/`E2b-Sandbox-Port` 头解析
`(sid, port)`,校验 `X-Access-Token` 后转发:e2b profile 的 49983/49999 拨 sandbox-ctl
`--connect` 暴露的 host UDS 直达 envd;其余任意端口拨 `floatingip:port`。对 paused
沙箱的请求触发自动 resume(单飞合并,§8)。部署形态(internal/external/off)见 §9.1。

## 2. 命令行接口

### 2.1 子命令总览

**`orchestrator-ctl`**:

| 子命令 | 用途 |
|---|---|
| `serve` | 启动 daemon:控制面 + 数据面 + 本机控制 socket + reaper + 构建池 |
| `proxy` | 外置数据面 worker(`proxy.mode=external`,§9.1) |
| `run-sandbox` / `run-builder` | systemd 单元内启动器,非给人用(§2.4、§6) |
| `config` | 配置规范化/校验,或输出带注释骨架 |
| `manifest-key` | `add`/`remove`/`check`/`list`:create/build 白名单管理(§7) |
| `export-sandbox` / `import-sandbox` | 暂停沙箱转模板 / 跨机迁移(§8.1) |
| `version` | 版本 |

**`e2b-key-ctl`**(纯派生,不触 DB/config/daemon):

| 子命令 | 用途 |
|---|---|
| `gen-key` | 生成随机 32B manifest key(64-hex) |
| `gen-apikey [<MANIFEST_KEY>]` | 从 manifest key 派生 e2b api key(`e2b_` + hex,§7) |
| `fingerprint [<MANIFEST_KEY>]` | 打印 24-hex 指纹(与白名单/库内索引一致) |
| `seal-pull-token [<MANIFEST_KEY>] …` | 封装不透明镜像拉取令牌(`kpt_`,§11) |
| `version` | 版本 |

接住一个新节点的典型顺序:

```bash
# 1) 生成租户根密钥,登记白名单,派生 SDK 用的 api key
MK=$(e2b-key-ctl gen-key)
orchestrator-ctl manifest-key add "$MK" --label tenant-a
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$MK")

# 2) e2b SDK/CLI 直接指向本机
export E2B_DOMAIN=sandboxes.example.com        # 生产(TLS, §12)
# dev: E2B_API_URL=http://host:3000  E2B_SANDBOX_URL=http://host:3000
```

### 2.2 `orchestrator-ctl serve`

```
orchestrator-ctl serve [--config /etc/orchestrator-ctl/config.yaml]
                       [--proxy internal|external|off] [--proxy-socket <uds>[,<uds>…]]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--config` | `/etc/orchestrator-ctl/config.yaml` | 配置文件(§3) |
| `--proxy` | – | 覆盖配置的 `proxy.mode` |
| `--proxy-socket` | – | 覆盖 `proxy.sockets`(逗号分隔,external 模式) |

启动序列:打开 sqlite(文件 chmod 0600)→ 生成并安装 systemd 模板单元(§5)→
重启对账(§14)→ 起 reaper(TTL,5s 周期)与构建池(§11)→ 起本机控制 socket(§6)
→ 按 `proxy.mode` 装配数据面(§9)→ 监听 `api.listen`。`api.<domain>`(及任何
`api.` 前缀 Host)路由到控制面,其余 Host 进数据面。TLS 证书缺省时以明文 h2c 服务
(dev:SDK 走 `E2B_API_URL`/`E2B_SANDBOX_URL`)。

随仓 systemd 单元模板:`deploy/orchestrator-ctl.service`。

### 2.3 `orchestrator-ctl proxy`

外置数据面 worker(§9.1)。运维带外起(`deploy/orchestrator-proxy@.service`),
与 orchestrator 同节点(本地拨 envd-UDS / floatingip)。

```
orchestrator-ctl proxy --socket=<uds> [--data-listen=:443]
                       [--tls-cert <pem> --tls-key <pem>]
                       [--auth off|log|enforce] [--park-timeout 30s]
                       [--metrics-listen <addr>] [--mmds-listen <addr>]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--socket` | (必填) | 本 worker 的 UDS:orchestrator 拨它跑 routesync 流 + 兜底转发 |
| `--data-listen` | 空 | 数据面入口,**SO_REUSEPORT**(多 worker 共享同一端口);空 = 仅 UDS 服务 |
| `--tls-cert/--tls-key` | 空 | 数据面 TLS(与 orchestrator 同一张通配证书);空 = h2c |
| `--auth` | `enforce` | 数据面鉴权回退值,仅在 orchestrator 策略到达前生效(§9.3) |
| `--park-timeout` | `30s` | 请求挂起预算回退值,同上 |
| `--metrics-listen` | 空 | Prometheus 文本端点(`/metrics`) |
| `--mmds-listen` | 空 | MMDS 元数据服务监听地址;serve 配 `mmds.enabled` 时设置(§9.4) |

### 2.4 `orchestrator-ctl run-sandbox` / `run-builder`

systemd 单元的 ExecStart,非给人用。共用的进入骨架:`--pidfile` 以
`fcntl(F_SETLK)` 排他锁防重入、写本 PID → 拨 `--config-socket` 取规约(§6)。
之后两者分道:

- **run-sandbox**:取 LaunchSpec → `chdir(workdir)`、剥除 `TASK_*` 引导变量、合入
  `spec.env`(密钥)→ `execve` 替换为 `sandbox-ctl run`,目标继承本 PID 与单元
  cgroup(锁 fd 已清 `FD_CLOEXEC`,随 execve 存活)。
- **run-builder**:取 BuildSpec → **驻留**驱动三阶段构建流水线(§11):各阶段沙箱
  (`sandbox-ctl run`)是它的直接子进程,整个构建计入本单元 cgroup;结束把结果 JSON
  (`{image_key|snapshot_key, start_cmd, ready_cmd, error}`)打到 stdout,由单元
  `StandardOutput=file:` 捕获为 `<bid>.result`。

```
orchestrator-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --sandbox-id=<sid>
orchestrator-ctl run-builder --pidfile=<f> --config-socket=<uds> --build-id=<bid>
```

flags 缺省回落 `TASK_PIDFILE` / `TASK_CONFIG_SOCKET` / `TASK_SANDBOX_ID` /
`TASK_BUILD_ID` env(systemd `%i` 接线用)。

### 2.5 `orchestrator-ctl config`

```
orchestrator-ctl config --config <file>   # 加载(补默认 + 校验)后重排输出
orchestrator-ctl config --template        # 输出带注释骨架
                        [-o <file>]       # 写文件(默认 stdout)
```

与 `sandbox-ctl config` / `flatten-ctl config` 同形态。骨架与
`deploy/config.example.yaml` 对应。

### 2.6 `orchestrator-ctl manifest-key`

create/build 白名单(`manifest_keys` 表)管理,是 serve daemon **admin 平面**的瘦
客户端(经本机控制 socket,§6)——daemon 是该表唯一写者,CLI 不开 DB、不读 config,
只需 `--socket`(或 `ORCHESTRATOR_SOCKET` env,默认 `/run/sandbox/orchestrator.socket`)。
key 取自位置参数或 `MANIFEST_KEY` env;输出只含指纹,绝不回显 key。

```
orchestrator-ctl manifest-key add    [--label L] [--ttl 24h]
                                     [--registry-auth <docker.json> |
                                      --registry-username U --registry-password P |
                                      --registry-token T]      [--socket S] <KEY>…
orchestrator-ctl manifest-key remove [--socket S] <KEY>…
orchestrator-ctl manifest-key check  [--socket S] <KEY>…
orchestrator-ctl manifest-key list   [--socket S]
```

- `--ttl`:失效时长(`0`/缺省 = 永不);**重复 add 刷新失效时间**。过期 key 视同不在
  白名单,由 reaper 惰性清理;`list` 显示 `expires`。
- `--registry-auth`/`--registry-*`:租户默认镜像拉取凭据,加密存白名单行
  (`registry_auth_enc`),构建拉取时按 fromImage 的 host 匹配取用(§11)。
- 鉴权:`SO_PEERCRED`——配置了 `paths.admin_pidfile` 则 peer pid 须在其中;未配则仅靠
  socket 0600 权限(同 uid / root)。

### 2.7 `orchestrator-ctl export-sandbox` / `import-sandbox`

serve daemon **api 平面**的客户端(经本机控制 socket 调 `POST /sandboxes/{id}/export`
与 `POST /sandboxes/import`),鉴权 `E2B_API_KEY` env(须属主)。语义见 §8.1。

```
orchestrator-ctl export-sandbox <sid> [--to-template] [--keep-source] [--socket S]
orchestrator-ctl import-sandbox <token> [--socket S]
```

- `export-sandbox <sid>`:打印单行 base64 迁移 token(默认 move,回收源行;
  `--keep-source` = copy)。
- `export-sandbox <sid> --to-template`:晋升为远程快照并打印持久 templateID(扇出用)。
- `import-sandbox <token>`:在本机插入 paused 行并打印 sid,随后 `e2b sandbox resume`
  即可在本机恢复。

### 2.8 `e2b-key-ctl`

总览见 §2.1。`seal-pull-token` 完整形式:

```
e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] {--registry-username U --registry-password P |
                                              --registry-token T}
```

输出 `kpt_` 前缀的不透明令牌:以租户 manifest_key 派生密钥 AES-GCM 封装的镜像拉取
凭据,经 SDK `api_headers`(头 `X-Kuasar-Pull-Token`)随构建请求传入,orchestrator 用
该租户的存量 key 解封(§11)。manifest key 取自首个位置参数或 `MANIFEST_KEY` env。

## 3. 配置

完整带注释样例见 `deploy/config.example.yaml`(`orchestrator-ctl config --template`
输出同形骨架),权威结构是 `internal/config/config.go`。配置按关注点分组:`api`、
`proxy`、`paths`、`units`、`sandbox`(实例级默认,子组 `resources`/`network`/`boot`)、
`builder`、`checkpoint`、`mmds`,外加顶层单值 `encryption_key`、`manifest_config`。
**必填仅 `api.domain` 与 `encryption_key`**(后者可用 `ORCHESTRATOR_ENCRYPTION_KEY`
env 覆盖)。外部二进制(sandbox-ctl/vswitch-ctl/flatten-ctl)**不配置**:按"与
orchestrator-ctl 同目录 → PATH"自动发现。

| 字段 | 默认 | 说明 |
|---|---|---|
| `api.domain` | (必填) | 服务域,如 `sandboxes.example.com`;控制面 = `api.<domain>` |
| `api.listen` | `:443` | 北向监听;dev 用 `:3000` 走明文 h2c |
| `api.tls.cert/key` | 空 | 通配证书(`*.<domain>` 与 `api.<domain>`,§12);空 = 明文 |
| `proxy.mode` | `internal` | 数据面承载:`internal`/`external`/`off`(§9.1) |
| `proxy.sockets` | – | external:各 worker 的 routesync UDS,orchestrator 逐一拨号(external 模式必填) |
| `proxy.data_listen` | 空 | 专用数据面监听;空 = 与 `api.listen` 共口。external 模式由 worker 持有数据口,orchestrator 不绑它 |
| `proxy.park_timeout` | `30s` | 数据面请求挂起预算:等路由同步 / paused 沙箱 resume 的上限(§9.1) |
| `proxy.auth` | `enforce` | 数据面鉴权:`off`/`log`/`enforce`,校验 `X-Access-Token`(§9.3) |
| `proxy.metrics_listen` | 空(关) | Prometheus 文本端点(`data_requests_total{result=…}`、`gateway_forward_total` 等) |
| `encryption_key` | (必填) | manifest_key 落盘加密的 AES-256 密钥:`:` 分隔多个 64-hex,首个为活动密钥,其余备用解旧记录(轮换);`ORCHESTRATOR_ENCRYPTION_KEY` env 优先 |
| `manifest_config` | `/opt/sandbox/manifest.yaml` | 共享远程 manifest store 配置(`manifest.key` 留空,租户 key 经 env 按任务下发) |
| `paths.run_root` | `/run/sandbox` | tmpfs 运行态:`<sid>/` 运行目录、UDS、pidfile |
| `paths.base_root` | `/var/lib/sandbox` | 持久态根 |
| `paths.db_path` | `<base_root>/orchestrator.db` | sqlite 路径(§14) |
| `paths.config_socket` | `/run/sandbox/orchestrator.socket` | 本机控制 socket(三平面 h2c,§6);manifest-key/export/import CLI 的连接点 |
| `paths.admin_pidfile` | 空 | admin 平面的多行 PID 白名单(`#` 注释);未配则仅靠 socket 0600 |
| `units.dir` | `/etc/systemd/system` | 模板单元安装目录 |
| `units.runner` / `units.builder` | `sandbox-runner@.service` / `sandbox-builder@.service` | 模板单元名 |
| `units.install` | `true` | `false` = 单元由运维带外管理,serve 不生成安装 |
| `sandbox.timeout_sec` | `300` | 沙箱默认 TTL(秒) |
| `sandbox.resources.vcpu` / `.memory` | `2` / `2GiB` | 每沙箱容量;同时回显在 e2b list/get 的 `cpuCount`/`memoryMB`。**restore 类启动(snp 模板 create / resume / 迁移导入)按快照内 snapshot.cfg 的 capacity 覆盖**——快照自描述,可与本机默认不同(如构建预算下产出的模板) |
| `sandbox.resources.control_socket` | 空 | sandbox-sentinel 资源控制 UDS,**opt-in**;空 = 静态 cgroup(单元自身,§5.1) |
| `sandbox.network.switch` | `sw0` | vswitch 交换机名 |
| `sandbox.network.hostname` | `sandbox` | guest 主机名:sethostname + `/etc/hosts` 条目(§10) |
| `sandbox.network.dns` | `[169.254.169.253]` | 注入 guest `/etc/resolv.conf` 的 nameserver;该地址需部署侧路由到真实 DNS |
| `sandbox.network.e2b` / `.bare` | `169.254.0.21/30`+`169.254.0.22` / `169.254.1.1/31`+`169.254.1.0` | 按 profile 的 guest 内 `{inner_ip, nexthop}`:每 profile 复用同一对,沙箱唯一身份是 floatingip;e2b 的 /30 + 网关让 envd 端口转发可用 |
| `sandbox.boot.kernel` | – | vmlinux 路径 |
| `sandbox.boot.runtime_e2b` / `.runtime_base` | – | 两 profile 的 guest runtime erofs(§10) |
| `sandbox.boot.overlay_diff_template` | – | 预格式化空 ext4,img 冷启时稀疏复制为可写 upper(裸空 diff 非合法 fs 会被拒);部署方 `mkfs.ext4` 于稀疏文件提供;restore 不需要(overlay 链来自快照) |
| `builder.max_concurrent` | `2` | 构建池并发(orchestrator 内计数信号量,§11) |
| `builder.cpu_quota` / `.memory_max` | 空 | 施加到 `sandbox-builder.slice` 的 `CPUQuota`/`MemoryMax` |
| `builder.insecure_registry` | `false` | 经明文 HTTP 拉取 base 镜像(dev/本机 registry) |
| `builder.platform` | 空 | 拉取平台,如 `linux/amd64` |
| `builder.image_uri_mask` | 空 | 客户端推送镜像的命名约定(含 `{templateID}`/`{buildID}` 占位,须与 e2b CLI 的 `E2B_IMAGE_URI_MASK` 一致);trigger 缺 `fromImage` 时据此推导;**须从构建沙箱内可达**——拉取在 guest 内进行(§11) |
| `builder.runtime_builder` | – | 构建沙箱的 guest runtime erofs(e2b flavor + flatten-ctl + mkfs.erofs;umbrella `make sandbox-runtime-builder` 产出) |
| `builder.diff_template` | – | 构建沙箱可写盘的预格式化 ext4(拉取缓存 + steps 增量 + 导出 scratch;稀疏文件,建议 ≥ 最大预期镜像的 3 倍) |
| `builder.vcpu` / `.memory` | `2` / `4GiB` | 每台构建沙箱(阶段 microVM)的容量 |
| `builder.{pull,step,ready,total}_timeout_sec` | `600`/`600`/`120`/`1800` | 阶段超时:guest 内拉取+展平、单条 RUN step(经 `Connect-Timeout-Ms` 同步到 guest 侧)、readyCmd 轮询预算(2s 间隔;缺省 readyCmd = `sleep 20`)、整个构建(单元 `TimeoutStartSec` = total+60) |
| `builder.files_storage` | 空 | COPY 构建上下文的 S3/OBS 对象存储(子键 `endpoint`/`region`/`bucket`(必填)/`prefix`/`access_key`/`secret_key`/`force_path_style`/`presign_expiry`);空 = COPY 回 501。orchestrator 仅 presign + HEAD;`access_key` 空走 AWS 默认链;`force_path_style` 默认 false(versitygw/minio 置 true);`presign_expiry` 默认 1h(PUT;GET 用 total+5m)。本地/单机无云对象存储用 versitygw(§11) |
| `checkpoint.mode` | `local` | 暂停态落地:`local` = 本机文件(节点绑定)/ `remote` = 远程 manifest(可移植 = 模板)(§8.1) |
| `checkpoint.local_dir` | `/var/lib/sandbox-saved` | 本机快照目录(`mode=local`) |
| `mmds.enabled` | `false` | envd 鉴权姿态开关(§9.4):false = `-isnotfc` + proxy 单闸门;true = FC 模式 + MMDS re-key |
| `mmds.listen` | `127.0.0.1:19254` | MMDS 监听地址(vswitch `--mgmt-service` 的转换目标) |

配置自洽校验:`proxy.mode=external` 须给 `proxy.sockets`;`mmds.enabled=false` 时
`proxy.auth` 必须为 `enforce`(envd 非 secure,proxy 是唯一数据面闸门);
`mmds.enabled=true` 时 `proxy.mode` 不得为 `off`(MMDS 寄宿 proxy 组件)。

## 4. e2b API 契约

基址 `https://api.<domain>`;鉴权 **`X-API-KEY`**(SDK)或 **`Authorization: Bearer`**
(e2b CLI 构建面,两者同样解析)。api_key 由 manifest_key 派生(`e2b-key-ctl
gen-apikey`),orchestrator 经 MAC 校验解析出租户——无静态 api_keys 表(§7)。

**归属校验**:按 id 的控制操作用 api_key 的 MAC 对该资源行的(解密)manifest_key
校验,不符回 **404**(不泄露他租户存在性);create / build / import 另需 manifest_key
在白名单,否则 **403**。

### 4.1 控制面:沙箱生命周期

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| create | `POST /sandboxes` → 201 | body `{templateID, timeout, metadata, envVars}`(timeout 缺省取 `sandbox.timeout_sec`,默认 300s);回 `{sandboxID, templateID, clientID, domain, envdVersion, envdAccessToken, trafficAccessToken, alias}` |
| get | `GET /sandboxes/{id}` | 附 `state`/`startedAt`/`endAt`/`metadata` |
| list | `GET /v2/sandboxes` | 仅本租户;query `state`/`limit`/`nextToken`,分页头 `x-next-token`;每项含 `cpuCount`/`memoryMB`/`diskSizeMB`(节点统一配置值 + overlay 模板尺寸)与 ISO-8601 `startedAt`/`endAt` |
| kill | `DELETE /sandboxes/{id}` → 204 | 非本租户 ⇒ 404 |
| resume | `POST /sandboxes/{id}/connect` | e2b 语义:resume 走 `/connect`;body `{timeout:秒}` 顺带续期;可携迁移 token 自动 import(§8.1) |
| pause | `POST /sandboxes/{id}/pause` → 204 | 已暂停回 **409** |
| timeout | `POST /sandboxes/{id}/timeout` | body `{timeout:秒}`,重置 TTL |

create 的 `templateID` 接受三种引用:持久 id(`<profile>-<kind>-<key>`,§4.4)、注册期
transient id、或已 ready 构建的 name/alias——后两者解析到持久 id 再走统一路径。
`envdVersion` 回 `0.6.1`(e2b)或 stub `0.1.0`(bare,≥0.1.0 否则 SDK 自毁);bare 无
envd,回占位 token,数据面控制端口 501。`trafficAccessToken` 为 SDK 兼容字段;数据面
强制头是 `X-Access-Token`(= `envdAccessToken`,§9.3)。

### 4.2 控制面:模板构建 API

实现 e2b **v2 build system** 的端点族(SDK `Template.build` / CLI 走它);构建语义与
资源池见 §11。

| 操作 | 方法 + 路径 | 要点 |
|---|---|---|
| register | `POST /v3/templates` → 202 | body `{name, tags}`;回 `{templateID: transient-<uuidv7>, buildID, names, tags, aliases, public:false}` |
| trigger | `POST /v2/templates/{tid}/builds/{bid}` → 202 | body 兼容 `{fromImage, fromTemplate, fromImageRegistry{username,password}, steps[], startCmd, readyCmd}` 与 CLI 形态 `{start_cmd, ready_cmd, …}`;`fromImage`/`fromTemplate` 互斥,皆缺时由 `builder.image_uri_mask` 推 fromImage;steps 支持 `RUN/ENV/ARG/WORKDIR/USER/COPY`(COPY 须先经 files 端点上传 context:未配 files_storage→**501**、未上传→**400**,§11);`startCmd` 非空或 fromTemplate ⇒ 暂记 snp(终态以流水线产物为准,§11),否则 img;fromTemplate 且无 steps 无 startCmd ⇒ 拒绝(无事可做) |
| status | `GET /templates/{tid}/builds/{bid}/status` | 回 `{templateID, buildID, status, logs:[], logEntries:[]}` + 失败时 `reason{message}`;**进行中恒报 `building`**(registered/waiting/building 均映射,CLI wait 循环仅在 `building` 续轮询),终态 `ready`/`error`;ready 后 `templateID` 即报持久 id,并附 `names`/`aliases` |
| files | `GET /templates/{tid}/files/{hash}` → 201 | COPY context 上传协商:`tid→build→归属`校验后回 `{present, url}`——present 即对象已在桶(客户端跳过上传),url 为**直传桶的 presigned PUT**(字节不过控制面);未配 `files_storage`→**501**,未知/非属主 tid→**404**。详见 §11 |
| list | `GET /templates` | 本租户 ready 模板;`templateID` 列为持久 id |

### 4.3 数据面协议(envd,数据面仅透传)

envd 单端口 **49983**,HTTP/1.1 与 h2c 双栈,Connect-RPC(proto 包无版本):
`process.Process`、`filesystem.Filesystem`(仅元数据);文件内容走 HTTP
`GET/POST /files`(+ 签名 query);另有 `/health`、`/init`、`/metrics`。每操作用户
经 `Authorization: Basic base64("user:")`。**code-interpreter** =
`POST https://49999-<sid>.<domain>/execute`(NDJSON)→ guest FastAPI(:49999) →
Jupyter(:8888)。exec = `POST /process.Process/Start`(头 `X-Access-Token`)。
租户数据面本组件只透传不实现;**构建流水线自带一个最小 `process.Start` 客户端**
(connect+JSON 信封手写,无 protobuf/gRPC 依赖)驱动 steps/startCmd/readyCmd(§11)。

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
- **无独立 templates 表**:`builds` 表兼任模板登记(§14);持久 id 由构建产物推导。
  快照晋升的模板(§8.1)甚至不写 builds 表——id 本身即完整引用。

### 4.5 SDK / CLI 对接与协议 pin

- 重定向:`E2B_DOMAIN=<domain>` + `E2B_API_KEY`(生产,TLS);dev 走
  `E2B_API_URL`/`E2B_SANDBOX_URL`(http/h2c)。控制面要求 Host 命中 `api.*`。
- api_key 形态:`e2b_` + 72 hex(共 76 字符);e2b SDK 以 `/^e2b_[0-9a-f]+$/` 校验
  格式,服务端另验 MAC(§7)。
- envd 版本 pin:按 e2b-dev/infra 发布 tag 定版(sandbox-deps `ENVD_TARBALL`,默认
  `2026.22`,对应 envd 0.6.x);SDK:`e2b` js 2.27.x / py 2.25.x 实测兼容。
- 数据面鉴权头 `X-Access-Token`(= `envdAccessToken`):secure 沙箱自 SDK v2.0.0
  默认开,SDK 每次数据面调用携带。
- routesync(external proxy):版本 1,帧 `[4B LE len][JSON]`,消息
  `hello|hello_ack|snapshot|upsert|delete|wake`,标识头 `X-Orch-Routesync: 1`,
  路径 `POST /routesync`(§9.2)。

## 5. 进程管理(systemd 模板单元,启动时自动生成安装)

serve 启动时生成并安装两个模板单元 + 两个 slice(`sandbox-runner.slice`、
`sandbox-builder.slice`)到 `units.dir`,内容变更才 `daemon-reload`(D-Bus `Reload`);
`units.install: false` 则交由运维带外管理。ExecStart 里的 `orchestrator-ctl` 路径
取自 serve 自身所在目录(自动发现,§3)。

**runner 单元**(`%i` = sid):

```ini
# sandbox-runner@.service (生成内容,路径按配置渲染)
[Service]
Type=exec
WorkingDirectory=/run/sandbox/%i
ExecStart=<orchestrator-ctl> run-sandbox --pidfile=/run/sandbox/%i/%i.pid \
          --config-socket=/run/sandbox/orchestrator.socket --sandbox-id=%i
Restart=no                  # 一进程一沙箱、有状态:崩 = 该沙箱已死,不重试
KillMode=control-group      # StopUnit 连 cloud-hypervisor 一并 SIGKILL(§5.1)
TimeoutStopSec=20
Slice=sandbox-runner.slice
Delegate=yes                # 委派控制器,--cgroup-adopt 才能写 cpu.max/memory.max(§5.1)
```

**builder 单元**(`%i` = build id,§11):

```ini
# sandbox-builder@.service (生成内容)
[Service]
Type=oneshot
WorkingDirectory=/run/sandbox/%i
StandardOutput=file:/run/sandbox/%i/%i.result   # 捕获 run-builder stdout 的构建结果 JSON
StandardError=journal                           # 流水线进度/报错进 journal,不污染 .result
ExecStart=<orchestrator-ctl> run-builder --pidfile=/run/sandbox/%i/%i.pid \
          --config-socket=/run/sandbox/orchestrator.socket --build-id=%i
TimeoutStartSec=1860              # builder.total_timeout_sec + 60
KillMode=control-group
Slice=sandbox-builder.slice   # 构建池 cgroup 上限(builder.cpu_quota/memory_max)
```

两单元的 ExecStart 都是启动器(§2.4),锁 pidfile 后经 config-socket 取规约,然后
分道:runner `execve` 替换为 `sandbox-ctl run`(继承单元主 PID 与 cgroup,
`Type=exec` 故无需 sd_notify);builder **驻留**驱动三阶段流水线(§11),阶段沙箱
(`sandbox-ctl run` + cloud-hypervisor)是其直接子进程、整个构建计入本单元 cgroup,
`Type=oneshot` 使 `StartUnit` 阻塞至流水线退出,`KillMode=control-group` 保证
StopUnit/超时连阶段 VM 一并回收。

- **kill**:`StopUnit`(连 CH 一并 SIGKILL)→ `ResetFailedUnit` → `vswitch-ctl detach`
  → 删运行目录 → 删库行。`kill` 在进程层生效,不受 guest 内 restart 策略阻挡。
- **就绪**:`Type=exec` 下 exec 成功即视为单元已启动;e2b profile 再轮询 envd
  `/health`(UDS,60s 上限)判数据面就绪。
- **存活权威**:`ListUnitsByPatterns("sandbox-runner@*.service")` 一次拿权威存活集
  (§14)。
- 宿主 `Restart=no` 与 guest 内 envd `restart=always`(sandbox-init 管)是两层,
  互不相干。

### 5.1 cgroup(cgroup-adopt:单元自身 cgroup 即沙箱资源 cgroup)

orchestrator 不预建 cgroup、不做进程搬迁。LaunchSpec 给 sandbox-ctl 带
**`--cgroup-adopt`**:它接管自己所在 systemd 单元的 cgroup 作为沙箱资源 cgroup——读
`/proc/self/cgroup` 求出路径,cloud-hypervisor 与自身都留在其中。于是:

- 一个单元 = 一个沙箱 cgroup;sentinel(配置了 `control_socket` 时)在该路径上原地
  仲裁;`KillMode=control-group` 使 `StopUnit` 连 CH 一起 SIGKILL,无需 orchestrator
  排空/rmdir 安全网。
- 单元必须 `Delegate=yes`:否则单元 cgroup 的控制器接口文件(`cpu.max`/`memory.max`)
  非本进程可写,`--cgroup-adopt` 写资源上限会 `permission denied`。
- 代价:sandbox-ctl 与 CH 同处受限 cgroup,`memory.high` 节流存在死锁风险(见
  sandbox-runtime `pkg/sandbox/cgroup.go` 头注)。采纳此模型并照常设 `memory.high`。

## 6. 本机控制 socket(task / admin / api 三平面)

serve 在 UDS `paths.config_socket`(默认 `/run/sandbox/orchestrator.socket`,**0600**)
跑一个 h2c HTTP 服务(兼容 HTTP/1.1):单 socket 复用三个平面、各自鉴权。连接建立时
经 **`SO_PEERCRED`** 取 peer pid 注入请求上下文;socket 0600 ⇒ 仅同 uid / root 可连,
各平面在此之上再细分。`/internal/*` 前缀 e2b SDK 永不使用,与 api 路径不冲突。

**① task 平面** — 启动器取工作规约,两条路径、同一鉴权:

- `POST /internal/task/launchspec`(run-sandbox;req `{config_id: "sandbox:<sid>"}`)
  → **LaunchSpec** `{exec, args, workdir, env}`:`exec=sandbox-ctl`,
  `args=[run --sandbox-id <sid> --config <rundir>/<sid>.yaml --manifest-config
  <shared> --run-root <run_root> --cgroup-adopt (--restore <ref>)
  (--connect <uds:ip:port>)…]`,`env={MANIFEST_KEY}`。`--run-root` 把 sandbox-ctl
  的 socket/staging 目录(`ch.sock`/`ctl.sock`/…)钉到 orchestrator 的 run_root,
  pause/snapshot 客户端(同 `--run-root`)才能拨到 `ctl.sock`。
- `POST /internal/task/buildspec`(run-builder;req `{config_id: "build:<bid>"}`)
  → **BuildSpec(构建工作单)**:`{build_id, workdir, from_image | from_template
  (+kind), steps[], start_cmd, ready_cmd, env, paths, net, vcpu, memory,
  mmds_enabled, envd_token, insecure, platform, timeouts}`——`env` 含
  `MANIFEST_KEY` + 租户 `FLATTEN_*` 拉取凭据;`paths` 是宿主侧工件与工具
  (kernel / runtime_e2b / runtime_builder / 两个 diff template / sandbox-ctl /
  flatten-ctl / manifest-ctl / manifest_config);`net` 是 serve 预先 attach 的
  网络槽(tapfd exec、mac、inner_ip、nexthop、hostname、dns),全构建复用。
  run-builder 据此自建阶段沙箱(§11);仅在该构建单元运行期间可取(serve 持挂
  pending 状态,单元退出即失效)。
- **鉴权**:peer pid ⟷ `<rundir>/<id>/<id>.pid`(启动器拨号前已锁写本 PID),相等即
  认证。
- 设计意图:**非密配置走文件**(`<sid>.yaml`;构建的阶段 yaml 由 run-builder 写进
  workdir)、**密钥走 spec env**——秘密只在内存与 env 中,不落盘。

**② admin 平面** — `/internal/admin/manifest-keys`(`GET` = list,`POST
{op: add|remove|check, key, label, ttl_seconds, registry_auth}`):manifest-key 白名单
管理(§7)。鉴权:配 `paths.admin_pidfile` 则 peer pid 须在其中;未配则仅靠 socket
0600。`orchestrator-ctl manifest-key` 即此平面客户端。

**③ api 平面** — 其余路径回落到 e2b 控制面 handler(与 TLS `api.listen` **同一个**
`http.Handler`,含 export/import 扩展),明文 h2c、`X-API-KEY` 鉴权。
`export-sandbox`/`import-sandbox` CLI 即此平面客户端(§8.1)。

**信任模型**:host root / daemon uid 可信;租户代码在 guest 内,够不到 host UDS。

## 7. 密钥与归属模型(manifest_key 根密钥,加密存 sqlite)

- **manifest_key 是每租户根密钥**(32B / 64-hex),同时就是 manifest 内容键
  (`MANIFEST_KEY` env / `manifest.key`)。**api_key 由它派生**(`e2b-key-ctl
  gen-apikey`):

  ```
  api_key = "e2b_" + hex( fp(12) ‖ ts(4) ‖ nonce(4) ‖ mac(16) )      # 76 字符
  fp  = SHA256(manifest_key)[:12]                                    # O(1) 库内匹配
  mac = HMAC-SHA256(manifest_key, fp‖ts‖nonce)[:16]                  # 无 key 不可伪造
  ```

  e2b SDK 以 `/^e2b_[0-9a-f]+$/` 校验 api_key 格式(故用 hex 编码);orchestrator 另
  自校验 MAC(`internal/apikey`)。manifest_key 本身永不发给 SDK。
- **加密落盘**:`manifest_keys` / `sandboxes` / `builds` 三表的 manifest_key 字段
  AES-256-GCM 加密(`internal/secretbox`:记录 = `keytag(4)‖nonce(12)‖ct+tag`,
  keytag 选解密钥),另存 `manifest_key_hash = hex(fp)` 非唯一索引(快速匹配/排除)。
  加密密钥经 `encryption_key` / `ORCHESTRATOR_ENCRYPTION_KEY`(`:` 分隔多键,[0]
  活动、其余备用解旧记录,支持轮换)。
- **鉴权解析**(短 hash 匹配 + 完整 MAC 校验):api_key → 按 `fp` 命中行/白名单 →
  解密 manifest_key → 重算 HMAC 比对:
  - **create / build / import**:manifest_key 须在 `manifest_keys` 白名单
    (`orchestrator-ctl manifest-key`,§2.6;daemon 是该表唯一写者),否则 **403**。
  - **其他按 id / list 操作**:只对资源行自身的 manifest_key 校验,不查白名单——即
    清空白名单,存量 sandbox/build 仍可正常操作直至生命周期结束。
  - list 的 hash 预筛非唯一,逐行再验 MAC,杜绝 hash 碰撞串租户。
- **与收敛加密的关系**:manifest_key 只封 manifest 的密钥表;chunk 加密密钥派生自
  `SHA256(salt‖明文)`、与租户 key 无关 ⇒ chunk 去重仍跨租户;租户之间不共享 key 与
  模板。
- **key 不落明文**:sqlite 内加密;运行期只在 orchestrator 内存、LaunchSpec env 帧、
  子进程 env(`MANIFEST_KEY`)中;`<sid>.yaml` 非密不含 key。auto-resume 从加密存储
  解出 key 解封快照(数据面唤醒无 api_key 可用)。
- 数据面另有一对随机 token(`envdAccessToken`/`trafficAccessToken`,create 时铸造、
  随行存库),语义见 §9.3。

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
  失联的 running 标为 `dead`(§14);paused/dead 行中,paused 可再拉起,kill 删行。
- **auto-suspend**:reaper(5s 周期)发现 `deadline` 已过 → 对运行中 sandbox-ctl 封
  快照(按 `checkpoint.mode`,§8.1)→ 记 `snapshot_ref`、标 paused → `StopUnit` →
  detach。路由表保留 paused 路由,后续数据面流量可唤醒。
- **auto-resume**:数据面流量打到 paused 沙箱 → 读库 → 重走 launch(建目录 + attach
  + StartUnit),LaunchSpec 带 `--restore <snapshot_ref>` → sandbox-ctl 解封恢复。
  - **单飞**:同一 sid 的并发数据面请求经 per-sid single-flight 合并为一次 resume,
    杜绝重复 IP 分配 / attach / StartUnit 竞态。internal 模式 proxy 在请求内同步触发;
    external 模式经 routesync `Wake` 上行,orchestrator 端同样单飞(§9.1)。
  - resume 同时是 `POST /sandboxes/{id}/connect` 的实现;带 `timeout` 则顺带续期。
- **每实例配置覆盖**(create 经 metadata,零 SDK/API 改动):metadata 保留键
  **`kuasar-sandbox/config`**(值为 JSON 串)覆盖该沙箱网络项:`hostname`/`dns`/
  `inner_ip`(CIDR)/`nexthop`/`transit_gateway_ip`/`transit_geneve_vni`/`transit_mac`
  (后三者经 `vswitch-ctl attach --transit-*` 进 GENEVE 隧道)。空字段回落
  profile/config 默认;仅做格式校验、无白名单门(沙箱以完整能力经 sandbox API 发布,
  平台自身亦经此 API 管理)。metadata 持久化 ⇒ resume 时重新解析,覆盖在沙箱全生命
  周期一致。

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
  快照扇出新沙箱(新 sid);create 解密用的租户 key 仍由 api_key→白名单解析。
- **迁移(同 sid 跨机)**:`export-sandbox <sid>` 确保远程后导出**单行 base64 token**
  = 沙箱行(env/metadata/deadline/数据面 token + runtime erofs 摘要;manifest_key
  仅指纹、不含密钥);默认回收源行(move,`--keep-source` = copy)。目标机
  `import-sandbox <token>`:api_key → 白名单解析租户 key(须先 `manifest-key add`,
  与 create 同前置)、校验 token 指纹与本机 runtime 摘要一致,插入 paused 行;随后
  `connect` 在新机恢复。目标机须共享同一 `manifest_config`(远程 store)。
- **一步迁移**:`Sandbox.connect(<sid>, api_headers={"X-Kuasar-Migration-Token":
  <token>})`——目标机本地无此 sid 且带迁移 token 时,connect 在 resume 前自动
  import(同上校验,且 token 内 id 须等于所连 sid,否则报错并回收误插行),迁移收敛
  为单次 SDK 调用;`import-sandbox` CLI 保留作显式预导入。
- 限制:token 不含系统密钥,但携带沙箱自有 env 与数据面 token,按"沙箱级敏感"对待;
  快照绑定其 guest runtime(erofs 摘要校验),不同 runtime 的节点拒绝导入。

## 9. 数据面 proxy

proxy 是 L7 反代:按 `Host`(`<port>-<sid>.<domain>`)或 `E2b-Sandbox-Id`/
`E2b-Sandbox-Port` 头解析 `(sid, port)` → 路由判定 → 校验 `X-Access-Token`(§9.3)→
转发。`httputil.ReverseProxy{FlushInterval: -1}` 即时刷流(`process.Start`/`WatchDir`/
`/files` 都是流式),上游走 HTTP/1.1(envd 双栈,Connect 流在 h1 上承载),透传不
缓冲。路由判定(两种部署形态共用同一函数):

```
profile=e2b  且 port ∈ {49983, 49999}  → dial UDS (envd.sock / ci.sock)
profile=bare 且 port ∈ {49983, 49999}  → 501 (no data plane)
其余任意用户端口                        → dial floatingip:port
未知 sid / 挂起超时仍未 running         → 404
```

### 9.1 proxy 部署模式(internal / external / off)

`proxy.mode` 选数据面如何承载(控制面 `api.<domain>` 始终由 serve 的 `api.listen`
提供):

| 模式 | 数据面承载 | 进程 | 适用 |
|---|---|---|---|
| **internal**(默认) | serve 进程内 proxy | 单二进制 | 简单部署,无额外组件 |
| **external** | 独立 `orchestrator-ctl proxy` worker(≥1),**SO_REUSEPORT** 共享数据面端口 | serve + N×proxy | 数据面/控制面进程隔离、独立扩缩 |
| **off** | 拒绝(501) | – | 该节点不提供数据面 |

external 模式拓扑(数据面字节流不经 orchestrator):

```
 client ──► orchestrator-ctl proxy  :443 (SO_REUSEPORT, N workers share the port)
                 │ local route table (routesync push) + per-sid request park
                 │ running   → check X-Access-Token → dial envd-UDS / floatingip:port
                 │ paused /  → Wake (upstream) + park → unpark on Upsert(running)
                 │  missing     park timeout → 404
                 └── UDS ── routesync (bidi h2c, framed JSON) ── orchestrator-ctl serve
                       serve → worker : Hello(policy) → Snapshot(full) → Upsert/Delete
                       worker → serve : HelloAck → Wake{sid}
 client ──► orchestrator-ctl serve  :443 (fallback: data-plane request hits the
                 │                        control listener)
                 └─ reverse-proxy to one worker over its UDS (picked by sid hash)
```

- **路由权威在 orchestrator,worker 是缓存**:create/resume/pause/kill 实时广播
  Upsert/Delete 给所有 worker;worker 重启 → 自动重连 + 重取全量快照;订阅滞后 →
  orchestrator 断开该订阅,worker 重连重快照(有界内存、最终一致)。
- **park**:对 missing/paused sid 的请求先上行 `Wake`(同 sid 去重),再挂起等
  `Upsert(running)` 回灌解挂;`park_timeout`(策略下推,默认 30s)内未就绪回 404。
  首个快照未到前的请求同样先等同步完成。
- **运营策略集中下推**:握手 Hello 携带 `Policy{domain, auth_mode, park_timeout_ms}`;
  worker 的 `--auth`/`--park-timeout` 仅为策略到达前的回退。
- **兜底转发**:数据面请求误达 serve 监听口时,serve 按 sid 的 FNV 哈希挑一个 worker
  经其 UDS 反代过去(连接亲和),由 worker 照常处理(含鉴权)。
- worker 由运维带外管理(`deploy/orchestrator-proxy@.service`),serve 不自动安装;
  须与 serve 同节点(本地拨 envd-UDS/floatingip)。
- **可观测**:`proxy.metrics_listen`(serve)/`--metrics-listen`(worker)暴露
  Prometheus 文本:`data_requests_total{result=ok|unauthorized|notfound|denied|…}`、
  `gateway_forward_total` 等。

### 9.2 routesync 协议

serve ↔ worker 的路由分发协议,**帧化 JSON over h2c**(零 gRPC/protobuf):

- 传输:serve 拨 worker 的 UDS,发 `POST /routesync` + 头 `X-Orch-Routesync: 1`;
  单 HTTP/2 请求承载全双工流(请求体下行、响应体上行)。worker 的 UDS 上非该头的
  请求走兜底数据转发。
- 帧:`[4 字节 LE 长度][JSON]`,单帧上限 16 MiB(全量快照装得下)。
- 消息(`type` 字段判别):

  | 方向 | 消息 | 载荷 |
  |---|---|---|
  | serve → worker | `hello` | `{version:1, role, policy{domain, auth_mode, park_timeout_ms}}` |
  | worker → serve | `hello_ack` | `{version:1, role}` |
  | serve → worker | `snapshot` | `routes: [RouteEntry…]`(全量替换) |
  | serve → worker | `upsert` / `delete` | `route: RouteEntry` / `sid` |
  | worker → serve | `wake` | `sid`(请求 resume) |

- `RouteEntry = {sid, profile, template_id, state(running|paused|dead), envd_uds,
  ci_uds, floatingip, access_token}`——worker 据此独立服务数据面,无每请求回调。
- 容错:连接断 → serve 指数退避重连(0.2s 起、5s 封顶),重连即重发
  Hello + Snapshot;订阅积压 → 掐掉重来。Wake 的 resume 由 serve 端单飞去重。

### 9.3 数据面鉴权(X-Access-Token)

- 数据面 token = `envdAccessToken`,头 **`X-Access-Token`**(与原版 e2b 一致;secure
  沙箱自 SDK v2.0.0 默认开,SDK 每次数据面调用携带)。create 铸造 → 回 SDK → 随路由
  分发;**proxy 逐请求校验**其与该沙箱 token 一致(常数时间比较)。
- `proxy.auth ∈ {off | log | enforce}`,默认 **enforce**(不符回 401);`log` 告警但
  放行;external 模式校验在 worker(策略下推,兜底转发路径上 serve 不校验、由 worker
  校验)。
- **例外放行**:`auth=off`;路由无 token(bare);envd 预签名文件 URL(带 `signature`
  query,envd 自行验签、不带头)。
- envd 是否**另行**自校验此 token 由 `mmds.enabled` 决定(§9.4)。无论哪种姿态,envd
  仅经 proxy 的 host-UDS 可达(49983/49999 走 `--connect` UDS,非 floatingip),沙箱
  间无通路。
- create 响应同时返回 `trafficAccessToken`(SDK 兼容字段,非强制头)。

### 9.4 envd 鉴权姿态与快照扇出(mmds.enabled)

快照扇出的子沙箱(`export-sandbox --to-template` 或任何 snp 模板 create)由内存恢复,
其 envd 持**源**沙箱的 token;envd 在 `-isnotfc` 下不允许把 token 改成新身份的值
(`/init` 仅允许首设/重确认同 token/匹配 MMDS hash),会拒绝子沙箱数据面。单开关
`mmds.enabled` 选两种姿态,二者都使扇出沙箱数据面可用:

| | `enabled=false`(默认) | `enabled=true` |
|---|---|---|
| envd 启动 | `-isnotfc` | FC 模式(去 `-isnotfc`) |
| envd token | `/init` 省略 `accessToken` → envd 非 secure | 经 MMDS 授权后 `/init` 重置为每身份新 token |
| 数据面闸门 | 仅 proxy(配置强制 `proxy.auth=enforce`) | proxy + envd(纵深防御) |
| 扇出 fork | 可用(envd 无 token,故不错配) | 可用(envd 重置为新 token) |

**MMDS 服务**(`enabled=true`):orchestrator 在 **proxy 组件**内起 Firecracker MMDS
v2 兼容服务(internal:serve 绑 `mmds.listen`;external:worker `--mmds-listen`,
数据源是同步来的路由表)。两段式:

1. **`PUT /latest/api/token`**:按请求源 IP(= 沙箱 floatingip,vswitch mgmt-extract
   已 SNAT)查路由表解析沙箱 id——未注册则 **park 等待**(复用数据面挂起预算,故
   external 模式无"先注册后轮询"的时序约束),超时回 503(envd 的轮询会重试);
   命中则回 HMAC 签名、编码了沙箱 id 的 session token(`<sid>.<hmac>`)。
2. **`GET /`**(头 `X-metadata-token`):校验并解码 session token 取沙箱 id(guest 内
   代码不可信,不复读源 IP),回 `{instanceID, envID, accessTokenHash}`,
   `accessTokenHash = hex(sha512(token))`(= envd `keys.HashAccessTokenBytes`)。

envd 硬编码访问 `169.254.169.254:80`;部署侧用 vswitch
`--mgmt-service 169.254.169.254:80:<mmds.listen>` 在 eBPF 数据面把该 VIP 直译到
`mmds.listen`(无 iptables,自动改写回程;loopback target 需 mgmt 设备
`route_localnet=1`),故本进程不占特权端口、不需 root。配置校验强制
`proxy.mode != off`(MMDS 寄宿 proxy)。

## 10. guest profile:envd 嵌入

- 独立 **`sandbox-runtime-e2b.erofs`**(主 runtime 保持精简):`deps/build-runtime-e2b.sh`
  (`make sandbox-runtime-e2b` 调用,纯 shell,shell out `fsck.erofs`/`mkfs.erofs`)把
  固定版本 envd 注入运行时层的 `/opt/sandbox-runtime/bin/envd`,确定性重打(与
  flatten 同款 mkfs 参数),并把成品**补齐到 2 MiB 对齐**(virtio-pmem 后端要求,
  否则 cloud-hypervisor 报 `PmemSizeNotAligned`;EROFS superblock 自描述范围,尾部
  稀疏 padding 对 guest mount 不可见)。
- **零 sandbox-init 改动**:`/opt/sandbox-runtime` 被 sandbox-init 自动 bind-mount 进
  guest 同名路径,envd 直接作 `launch.exec`:
  `/opt/sandbox-runtime/bin/envd -isnotfc -port 49983`(`mmds.enabled` 时去
  `-isnotfc`,§9.4),`restart=always`,**以 root(`user: "0:0"`)运行**——envd 需要
  `CAP_SETUID/SETGID` 才能按镜像配置的用户跑工作负载命令(镜像设了 `Config.User` 时
  非 root 的 envd 会 exec EPERM);工作负载本身仍以目标用户执行。
- envd 是 **sandbox-deps** 的原生构建产物(与 cloud-hypervisor/vmlinux/mkfs.erofs
  并列):`make -C sandbox-deps envd` 拉取 e2b-dev/infra 发布 tarball(默认 tag
  `2026.22`,`ENVD_TARBALL` 可覆盖)→ `go build packages/envd`。本仓
  `make sandbox-runtime-e2b` 再把它注入裸 runtime(输入路径 `BASE_RUNTIME`/`ENVD`/
  `FSCK`/`MKFS` 可覆盖,默认取 umbrella 聚合 `bin/`)。
- **userland 门槛**(对 base/客户镜像的约束):须有 `bash`、`coreutils`/`util-linux`、
  预建默认用户(默认 `user`,含 `/home/user`)、cgroup v2、可写 `/run`。envd 跑每条
  guest 命令以默认用户、并包一层 `ionice -c 2 -n 4 nice -n N "$@"`——缺用户或缺
  util-linux/coreutils 会报 `invalid default user` / `ionice: not found`(裸 alpine
  两者皆缺)。
- **guest 须有 `/etc/hosts`**:展平的 docker 镜像不带它(docker 仅在容器运行时注入),
  而 guest 内 `socket.getfqdn(hostname)` 类调用(许多服务器在 bind 后、listen 前调它,
  如 Python `http.server.server_bind`)查无本地条目即落到 DNS,解析主机名阻塞约 20s,
  表象是"host→floatingip 应用端口转发失败"。orchestrator 经 SANDBOX_CONFIG 既有
  `files:` 机制注入 `/etc/hosts`(`127.0.1.1 <hostname>` 条目)与 `/etc/resolv.conf`
  (`sandbox.network.dns`),并经 `network.hostname` sethostname;launch 与 restore
  均生效。
- host 在 envd 就绪后调 **`POST /init`**(经 UDS):置 `envVars`、默认用户
  `user`/workdir `/home/user`、时间戳;仅 `mmds.enabled` 时携带 `accessToken`(§9.4)。

## 11. 模板构建(三阶段流水线,构建在沙箱内进行)

构建经 e2b API 提交(端点见 §4.2;无独立构建 CLI),落 `builds` 表,由资源池调度,
每个构建一个 `sandbox-builder@<bid>` 单元。**镜像拉取与 step 执行都发生在构建沙箱
(microVM)内**——租户的网络流量与镜像内容不触宿主用户态,宿主侧只做工件接力与
收尾上传。

**serve 侧(每构建一次)**:建 workdir → `vswitch-ctl attach` 一个网络槽(整个构建
复用,各阶段顺序交接 tapfd)→ 铸 envd token →(`mmds.enabled` 时)挂一行合成路由,
让模板阶段 FC 模式的 envd 能按 floatingip 自解析 → `StartUnit`(oneshot,阻塞至
流水线退出)→ 读 `<bid>.result` JSON → 终态落库:产物为快照 ⇒ `kind=snp`、为镜像 ⇒
`img`,持久 id `e2b-<kind>-<key>` 写入 names/aliases。

**单元内(run-builder,§2.4)** 依 BuildSpec(§6)最多跑三个阶段,每阶段一台
microVM(`sandbox-ctl run` 直接子进程);A 阶段就绪探针 = guest 内
`flatten-ctl mountpoint /.probe`,B/C 阶段以 envd `/health` 为就绪:

- **A import**(有 fromImage):**空**单盘沙箱——root 即 `builder.diff_template`
  复制出的可写 ext4(无 base 镜像),`launch.placeholder` 锚定;guest runtime 用
  **builder flavor**(`builder.runtime_builder`:e2b flavor + flatten-ctl +
  mkfs.erofs,经 `/opt/sandbox-runtime` 投影进任意 rootfs)。guest 内
  `flatten-ctl export --output - <fromImage>` 以租户凭据(`FLATTEN_*` 仅经 exec env
  入 guest)拉取 + 展平,tarstream 镜像工件经 exec stdio 流回宿主 `workdir/image.img`。
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
- **C template**(有 startCmd,显式或自 base 模板继承):**生产 e2b runtime** 冷启
  最终镜像(runtime_ref 冻入快照——模板的子沙箱不得继承构建工具链),envd 为 app
  (MMDS 姿态随部署,§9.4),`/init` 预置(此后 RPC 携 `X-Access-Token`)。startCmd
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
与 fromImage 互斥;fromTemplate 且无 steps 无 startCmd 拒绝(无事可做)。

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

**对象存储(`builder.files_storage`,§3)**:S3/OBS;orchestrator 只 presign + HEAD,
唯一 aws-sdk 落点;本地/单机无云对象存储时指向 versitygw(`sandbox-deps make
versitygw`)。force_path_style 默认 false(虚拟主机式;versitygw/minio 置 true)。

**收尾上传(平台凭据唯一出现点)**:img-only ⇒ `manifest-ctl store image.img`
(stdout = 64-hex manifest key);产出快照 ⇒ **一条** `sandbox-ctl upload-snapshot
<bundle>`——自动上传 snapshot.cfg 引用的全部本地工件(base 镜像、overlay)并把引用
改写为 `manifest://`(runtime_ref 不动,宿主提供)。结果 JSON
`{image_key|snapshot_key, start_cmd, ready_cmd, error}` 打 stdout → `<bid>.result`;
快照模板的 start/ready 同时记进 snapshot.cfg metadata,模板自描述(fromTemplate
继承与 create 都读它)。

**fromImage 的来源**:trigger body 显式给出;或(e2b CLI 在客户端 `docker build` +
`docker push` 到约定名、trigger 不带镜像引用的工作流)由 `builder.image_uri_mask`
推出 `<mask>/{templateID}:{buildID}`——掩码须与 CLI 侧 `E2B_IMAGE_URI_MASK` 一致,
且**须从构建沙箱内可达**(拉取在 guest 内:本机 registry 须绑非环回地址、按
vswitch mgmt VIP 寻址;第三方 registry 经 NAT 出网)。两者皆缺则 trigger 报错。
配本机/私网 registry 时设 `builder.insecure_registry`、`builder.platform`。

**资源池**:`builder.max_concurrent`(默认 2)= orchestrator 内计数信号量准入;
`waiting → building` 用 builds 表 CAS 抢占(重启/多实例安全);CPU/内存上限经
`sandbox-builder.slice` 的 `CPUQuota`/`MemoryMax` 施加(整条流水线都在单元 cgroup
内,上限对阶段 VM 生效)。

**镜像拉取凭据**(按优先级解析,无凭据则匿名):

1. **任务级 pull token**:SDK `api_headers` 头 `X-Kuasar-Pull-Token`,值为
   `e2b-key-ctl seal-pull-token` 用租户 manifest_key 派生密钥封装的 `kpt_` 令牌,
   orchestrator 以该租户存量 key 解封;
2. **SDK 明文**:trigger body `fromImageRegistry{username, password}`;
3. **租户默认**:`manifest_keys.registry_auth_enc`(docker config.json;
   `manifest-key add --registry-auth` 或 `--registry-username/--password/--token`
   自动组装 catch-all `*` 条目;按 fromImage host → `*` 匹配取条)。

解析结果加密存 `builds.registry_auth_enc`,构建时解出注入
`FLATTEN_REGISTRY_{USERNAME,PASSWORD|TOKEN}`(flatten-ctl `pkg/remote` 读取,token
优先),**经 exec env 进入 import 阶段的 guest——入 guest 的只有 `FLATTEN_*`,
`MANIFEST_KEY` 永不入 guest**。凭据仅加密存库 + 运行期 env;workdir 里的阶段 yaml
无秘密。

**不支持**:多源 COPY(e2b executor 亦只取 src+dst)、step 级缓存(`force` 字段
接受但忽略,总是全量执行)、服务端 Dockerfile 解析(CLI 已在客户端展开为 steps)。

## 12. DNS / TLS

生产:`*.<domain>` + `api.<domain>` 通配 DNS + TLS(operator 提供,on-prem/离线
友好)。控制面与数据面可同口(`api.listen`)或分口(`proxy.data_listen` /
external worker 的 `--data-listen`),证书同一张。dev:`E2B_API_URL`/
`E2B_SANDBOX_URL` 指向明文 http(h2c),无需证书与通配 DNS。

## 13. 契约边界

| 对象 | 方式 | 说明 |
|---|---|---|
| `sandbox-ctl`(runtime) | 经 run-sandbox(单元)`execve`:`run --config <sid>.yaml --manifest-config … --run-root … --cgroup-adopt [--restore] [--connect]`;run-builder 以直接子进程 `run` 阶段沙箱,经 `exec --env/--stdin-from/--stdout-to` 做平台接力(flatten-ctl 调用、配置注入、工件流、探针),收尾 `snapshot --output` / `upload-snapshot` / `info --json`;serve 调 `snapshot --upload`(pause) | 非密配置文件 + 密钥 env;资源准入在其内部;e2b 语义命令不走它(走 envd,§11) |
| `node-ctl`(sentinel) | 无直接交互;可选 `sandbox.resources.control_socket` | 单元 cgroup 即沙箱 cgroup,sentinel 原地仲裁;不配 = 静态 cgroup(`--cgroup-adopt`),配了才进 SANDBOX_CONFIG `resources.control.controller` |
| `vswitch-ctl`(vswitch) | CLI:`attach <switch> --inner-ip [--transit-*]` / `detach --port`;`open-port` 作 SANDBOX_CONFIG `network.tapfd.exec`(sandbox-ctl 执行,经 `TAPFD_SOCKET` 收 tap fd) | 交换机预先起好(`vswitch-ctl start`,内核态数据面);port 对外、slot 内部;一个构建复用一个槽 |
| `flatten-ctl`(builder) | **guest 内**(builder runtime 自带,经 sandbox-ctl exec 驱动):`export --output -`(import 拉取 / steps 导出)、`mountpoint`;宿主侧:`info --json`(读镜像运行时配置,本地工件或 manifest://) | 租户 `FLATTEN_*` 仅经 exec env 入 guest;tarstream 镜像工件经 exec stdio 接力 |
| `manifest-ctl`(accelerator) | `store <image.img>`(img-only 构建的收尾上传) | manifest key 经 stdout 回收;`MANIFEST_KEY` 经 env |
| `fsck.erofs`/`mkfs.erofs`(deps) | CLI(`deps/build-runtime-e2b.sh`、`deps/build-runtime-builder.sh`) | 确定性重打 e2b / builder runtime(§10、§11) |
| guest envd | UDS(sandbox-ctl `--connect` 映射);构建流水线另以最小 connect+JSON 客户端调 `process.Start`(steps/startCmd/readyCmd,§11) | 原版不改;协议 pin 见 §4.3/§4.5 |
| systemd | D-Bus:StartUnit/StopUnit/ResetFailed/ListUnitsByPatterns/Reload | 进程管理 + 单元自装(§5) |
| `orchestrator-ctl proxy`(external) | UDS routesync(双向 h2c 帧化 JSON)+ 兜底反代 | 同节点、运维带外起;数据口 SO_REUSEPORT 共享(§9.1) |

不新增导出包;`CGO_ENABLED=0`;依赖层级 = 叶子。

## 14. 可靠性

### 14.1 状态存储(sqlite)

单文件 sqlite(`paths.db_path`,WAL,文件 0600),纯 Go 驱动。三张表:

```
sandboxes      id(uuidv7) PK, template_id, state(running|paused|dead), deadline_unix,
               run_dir, base_dir, envd_uds, ci_uds, floatingip, vswitch_port,
               inner_ip, port_mac, manifest_key_hash, manifest_key_enc, snapshot_ref,
               envd_access_token, traffic_access_token, metadata_json, env_json,
               created_unix
builds         build_id PK, template_id(transient-…), persist_id(<profile>-<kind>-<key>),
               manifest_key_hash, manifest_key_enc, profile, kind, from_image,
               start_cmd, status(registered|waiting|building|ready|error), reason,
               names_json, aliases_json, registry_auth_enc, created_unix
manifest_keys  key_hash(索引), key_enc, label, created_unix, expires_unix,
               registry_auth_enc
```

`builds` 兼任模板登记(§4.4);`*_key_enc` 均 AES-256-GCM、`*_hash` 为
`SHA256(mk)[:12]` 非唯一索引(§7)。

### 14.2 重启对账

serve 重启后以 `ListUnitsByPatterns("sandbox-runner@*.service")` 为存活权威:

- 单元 active/activating 且库内 running ⇒ **收养**(重挂内存路由、TTL 继续生效,
  external 模式随快照重新推给 worker);
- 库内 running 但无对应活单元 ⇒ 清理(StopUnit/detach/删运行目录)并标 `dead`;
- `run_root` 为 tmpfs ⇒ 整机重启后 running 全部判 dead;`paused` 行与 snp 模板保留,
  可被 connect/auto-resume 重新拉起(本机快照存于磁盘 `checkpoint.local_dir`)。

### 14.3 故障域

| 故障 | 影响 | 自愈 |
|---|---|---|
| serve 崩溃/重启 | 控制面与 internal 数据面中断;沙箱(microVM/单元)不受影响 | systemd 重启 → 重启对账收养;external worker 凭本地路由表继续转发 running 流量(Wake 无人应答,paused 唤醒挂起至超时) |
| proxy worker 崩溃(external) | 该 worker 上的连接断;SO_REUSEPORT 下其余 worker 继续接新连接 | systemd 重启 → serve 重连重推快照,无状态恢复 |
| runner 单元/CH 崩溃 | 该沙箱死(`Restart=no`,有状态不重试) | 对账标 dead;客户重新 create(或从 paused 快照 resume) |
| routesync 断流 | worker 路由表停更 | serve 指数退避重连,重连即全量快照(§9.2) |
| sqlite 损坏 | 控制面不可用 | 文件级备份/重建;沙箱单元仍可被 ListUnits 发现并由运维处置 |

## 15. 测试

单元测试:`make test`(handler 路由、apikey/secretbox/regcreds、routesync/routetable、
mmds、单飞、override、migrate 等)。跨仓 e2e 集中在 umbrella
`kuasar-sandbox/test/e2e/`(需多仓产物:vmlinux/cloud-hypervisor/mkfs.erofs/
sandbox-runtime-e2b.erofs 等),均已注册为 umbrella make 目标,缺前置则自跳过
(`REQUIRE_*=1` 改为硬失败):

| 脚本 | 覆盖 | make 目标 |
|---|---|---|
| `e2e_orchestrator.sh` | 单元自动安装 + 控制面(`/health`、401 路径)+ 构建 API 生命周期(register/trigger/status、跨 key 归属 404)+(有 KVM 时)bare create/list/kill | `test-e2e-orchestrator` |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器(纯用户态,无 root/systemd/KVM):pidfile 锁/双起拒绝、HTTP 取 LaunchSpec、execve、`TASK_*` 剥除;`config` CLI 往返 | `test-e2e-runtask` |
| `e2e_run_builder.sh` | 三阶段构建流水线(KVM + vswitch + store-ctl + zot,guest 经 mgmt VIP 拉取):fromImage → e2b-img;fromTemplate(img)+steps+startCmd → e2b-snp(manifest:// base、配置合并、snapshot.cfg metadata 断言);fromTemplate(snp)+steps → e2b-snp(start/ready 继承);再从产物模板 create/list/kill;COPY 与 files 端点 501 | `test-e2e-run-builder` |
| `e2e_execute.sh` | 从已建模板冷启真实 microVM、guest 内 exec、pause(snapshot)→ resume 全链路 | `test-e2e-execute` |
| `e2e_orchestrator_proxy.sh` | `proxy.mode=external` 全链路:serve + 独立 worker(SO_REUSEPORT)+ routesync + 真实 microVM/envd,数据面经 proxy 走(401/转发/wake/resume/metrics) | `test-e2e-orchestrator-proxy` |

本仓 `make test-e2e` 聚合 `test-e2e-orchestrator` + `test-e2e-proxy`(指向 umbrella
同名脚本)。

## 16. See Also

- `sandbox-runtime/docs/sandbox.md` —— sandbox-ctl:SANDBOX_CONFIG 模式、
  run/snapshot/restore/connect 原语、cgroup 模型
- `sandbox-vswitch/docs/vswitch.md` —— attach/detach/open-port、floatingip 与
  mgmt-service(MMDS VIP 转换)语义
- `sandbox-builder/docs/flatten.md` —— flatten-ctl export 与 OCI Referrers 幂等流、
  `FLATTEN_REGISTRY_*`
- `sandbox-accelerator/docs/manifest.md` —— manifest 内容键、收敛加密与去重域
  (§7 的存储侧)
- `sandbox-sentinel/docs/node.md` —— 资源控制协议
  (`sandbox.resources.control_socket` 的对端)
- `kuasar-sandbox/docs/deployment.md` —— 节点部署拓扑中本组件的位置与单元安装
- `kuasar-sandbox/test/demo/DEMO.md` —— e2b CLI/SDK 全流程演示
