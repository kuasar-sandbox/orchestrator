# orchestrator

e2b 兼容沙箱平台的**节点主机**与**集群控制面**,两个生产二进制、一个 e2e 辅助二进制、两层:

- **`node-ctl`** (节点) - 计算节点上的控制面 daemon, 与独立 Proxy 共同组成节点服务. 对外是 **e2b 兼容北向入口**
  (未改造的 e2b SDK/CLI 直接指向即可 create/exec/pause/resume/kill microVM 沙箱、构建
  自定义模板);同时提供显式 ExecAccessToken 签发和 `service=exec` native exec,
  不依赖 guest envd.节点由 API-only conductor 与独立 Proxy 组成:conductor 管理控制面,
  生命周期和路由权威,Proxy 的 `data_listen` 反代 guest envd / floatingip / native exec 并
  承载 MMDS/traffic observation.conductor 可选内置**节点资源控制器**
  (沙箱准入、内存预算分配、主动回收,在声明资源和节点安全余量范围内提高有效利用率),
  以及 **node-link 客户端**(接入集群)。既可独立运行,也可经 node-link 交由 cluster-ctl 编排。
- **`cluster-ctl`**(集群)——面向大规模部署的控制面,把机群里数千个 `node-ctl` 聚合成一个
  逻辑沙箱池,**三角色均为独立进程**(无同进程内存模式):**registry**(shardkv 状态集群 +
  节点通道枢纽)、**router**(集群级数据面入口,按 sandbox-group + route-key 会话亲和转发)、
  **placer**(group provider/importer、WATCH_LIST 消费方与放置调度器)。
  沙箱按需创建 / 恢复 / 迁移,空闲下沉到节点本机快照乃至远程快照(可移植、不绑节点)。registry
  自身按 sandbox-group 分片复制状态;整套 registry 下电但 shard 数据保留时可恢复原状态。执行 shard
  不可恢复地丢失后不得从备份构造 node 执行态;存活 node 的投影恢复和 migration-token 持久 route
  分别由 [#34](https://github.com/kuasar-sandbox/orchestrator/issues/34) 与
  [#33](https://github.com/kuasar-sandbox/orchestrator/issues/33) 跟踪。

是 [Kuasar Sandbox 项目](https://github.com/kuasar-sandbox/kuasar-sandbox) 的北向入口、节点
资源仲裁与集群编排器,独立演进。两类沙箱 profile:**e2b**(guest 内 envd,完整数据面)与
**bare**(无 envd,仅 floatingip)。全部纯 Go,`CGO_ENABLED=0`,无 gRPC/protobuf(协议为
帧化 JSON over h2c)。

身份约定：`StableID` 是 sandbox 在 node-local ID 变化时保持不变的身份，`NodeSandboxID`
是当前节点实例 ID。standalone 普通 create 的 `StableID` 回退为本地 ID；显式 import 可以
改变 target 本地 ID 而保留 source `StableID`。cluster 对外 `SandboxID` 等于 `StableID`，
node 内部 `Sandbox.ID` 等于 `NodeSandboxID`。身份保持型 migration/copy 可以让多个
node-local sandbox 共享 `StableID`；它不是本地 lookup key，也没有唯一索引。KAT 的
canonical `sid` claim 绑定 `StableID`。

## 节点目录与身份

`paths.run_root` 是节点级 **RunRoot**，只承载可随重启丢失的小型运行态；
`paths.base_root` 是节点级 **BaseRoot**，承载 writable diff、checkpoint 与其他大体积本地数据。
对象的实际目录称为 **RunDir/BaseDir**，统一布局如下：

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

普通 Sandbox row 持久化精确的 `RunDir=<RunRoot>/sandboxes/<SandboxID>` 与
`BaseDir=<BaseRoot>/sandboxes/<SandboxID>`。`SandboxID` 是逻辑身份；调用 sandboxer 时
`PathID` 是相应 root 下的目录 leaf。普通 Sandbox 两者相同；Build phase 的逻辑
`SandboxID` 仍全局唯一，而 `PathID` 固定为 `a`、`b`、`c`。`RunID` 只标识 runner
execution，pidfile 位于 `runners/`。

`BuildID` 是 Build 业务身份，统一限制为 `[A-Za-z0-9_-]{1,48}` 并原样作为目录名；
`BuildRunDir`/`BuildBaseDir` 由 BuildID 与两个 root 唯一派生，不存入 Build row，也没有
hash、路径 fallback 或旧目录迁移。registered/waiting Build 不创建对象目录，execution
claim 后才创建；A/B 的本地 image carrier 与 Phase C 的本地 checkpoint capture 进入
`BuildBaseDir/checkpoint`，最终 publisher 直接消费它们。顶层 Sandbox E 不在
`BuildRunDir` 或 `BuildBaseDir` 形成完整 staging 文件，`BuildRunDir` 也不承载完整
`.image`、`.sandbox`、`.snapshot` 或 `.bundle`。本机 Sandbox snapshot/export 固定写入
`BaseDir/checkpoint`，不再配置独立的 `checkpoint.local_dir`。自定义 RunRoot 必须让最大
SandboxID 的 sandboxer socket 与最大 BuildID 的最长 phase socket 都不超过 Linux 107-byte
pathname 上限；配置加载会 fail closed。

Sandbox 显式删除先把完整 owner 原子转为内部 `deleting`,立即从节点 cache 与后续 route full
snapshot 撤下并发布 route Delete 撤销既有 live projection,再由节点 finalizer 按 exact runner
fence,network detach + durable exact-clear,RunDir,BaseDir,hard-delete 的顺序收敛.route Delete 只表示
projection withdrawal;terminal object observation 仍在 hard-delete 后发送.Detach 到 durable
clear 由 network allocation fence 包围;clear 成功后目录或 hard-delete 故障不再阻塞新网络分配,
尚未完成的 owner 仍保留在同一 row 供当前进程或下次启动重试.Pause 仍先原子提交 paused 与
checkpoint source;旧 RunID,port,RunDir 是 cleanup-pending owner,Resume/Wake/Exec 必须先
完成其清理;RunDir 删除成功后连同其 UDS 路径 exact-clear,下一次 Resume acceptance 原子恢复
canonical RunDir/UDS,BaseDir/checkpoint 始终保留.非删除终态 `dead` 不持有 unit,network,RunDir,
BaseDir 或 artifact owner.Build 同样只在 exact unit/cgroup,network 与两个派生目录均清理后
释放 execution claim;phase 子进程自清理不承担最终正确性.

cleanup 完成后的 `dead` Sandbox 与 `ready/error` Build 分别写入原子终态时间，默认保留
24 小时（`sandbox.dead_ttl`、`builder.terminal_ttl`），随后由 conductor 每轮最多 128 条地
删除；任何 runtime、network、path、artifact、result 或 execution claim ownership 都会阻止
删除，且不会自动 `VACUUM`。canonical、self-encoded TemplateID 与其 portable artifact 是长期
launch authority；Build status、transient TemplateID、name/alias 和本机 list 只在 Build row
保留期内可用，canonical TemplateID Create 与 Build `fromTemplate` 不读取旧 Build metadata。

当前保留的 Registry Build projection 由 node SQLite 派生：live `BuildUpsert/BuildDelete` 与每次
重连的 `BuildSyncBegin`/完整保留集/`BuildSyncEnd` 共同收敛丢失的 Delete。Registry 不运行独立
Build terminal TTL，并始终按 immutable registered-node binding 更新或删除 projection；这不为
#46 未来删除 post-registration projection 预留双路径。

## Build 制品发布

Build publication 明确分为两类。Image 类是 `target=image` 的 IMG、
`target=sandbox,memory=false` 的顶层 Sandbox E，以及 memory target 在 Phase C 前使用的
immutable IMG；checkpoint 类是 Phase C 捕获的增量 E、Snapshot S 和同一 checkpoint graph
的 memory/disk 增量层。顶层 E 和增量 E 都是标准 Sandbox E，但只有前者携带完整 EROFS
payload 并采用 image policy。

```yaml
checkpoint:
  mode: local # local | bundle；只控制 checkpoint 类 carrier
  remote:
    ref_location_parent: file:///mnt/shared/kuasar/checkpoints
    manifest: false
```

| 配置 | `target=image` | `sandbox,memory=false` | `sandbox,memory=true` |
|---|---|---|---|
| parent 空，`manifest=false` | IMG → Manifest store | 顶层 E → Manifest store | IMG、增量 E、S → Manifest store |
| parent 非空，`manifest=false` | IMG → Manifest store | 顶层 E → Manifest store | IMG → Manifest store；增量 E/S → named location |
| parent 非空，`manifest=true` | IMG Bundle → named location | 顶层 E Bundle → named location | IMG Bundle → named location；增量 E/S → named location |
| parent 空，`manifest=true` | 配置非法 | 配置非法 | 配置非法 |

`checkpoint.remote.manifest=true` **不表示写 Manifest store**；它表示把 manifest-backed
image 类逻辑制品直接物化为 checkpoint named location 中的 single-root Manifest Bundle。
image 类不先生成 tarstream `.image`/`.sandbox`，也不先上传 Manifest store；Bundle 只按
内容地址命名，不创建 BuildID/SandboxID alias。顶层 E 从当前 digest-qualified local IMG、
`manifest://` IMG 或 located IMG Bundle 直接组装并流向最终 publisher，没有中间 IMG
Manifest 或完整 E staging。Bundle 以 target-directory 临时文件、严格验证、独占 final、
file/directory fsync 提交；重试只复用验证通过的同 key final，损坏或不匹配的 final fail closed。
memory target 把实际返回的 portable IMG ref（包括 located Bundle）及其 location mapping 交给
Phase C，后续 checkpoint publication 保留该 ref，不把 image 复制进 Snapshot Bundle。

## 组成

| 路径 | 角色 |
| --- | --- |
| `cmd/node-ctl` | 节点主二进制:`conductor serve`(API/生命周期/路由权威 + 可选 `resource_listen` + node-link)/ `proxy serve`(唯一 sandbox data ingress 的 master + worker)/ `run-sandbox`·`run-builder`(单元内启动器)/ `resource {status,list,drain}` / `builder status` / `config` / `manifest-key` / `export-sandbox`·`import-sandbox` / `version` |
| `cmd/cluster-ctl` | 集群主二进制(三角色均独立进程):`registry`(shardkv 执行态 + node_link / route_link / node_list / placer_link)/ `router`(e2b 入口)/ `placer`(provider/importer + WATCH_LIST + Place)/ `config` / `version` |
| `cmd/node-stub-ctl` | 集群 e2e 辅助二进制:一个进程模拟多个 node-link 节点,分离 admin,API 和 Data listener,提供控制重启,清空,沙箱/build 状态和故障注入,不启动 microVM |
| `cmd/e2b-key-ctl` | 纯派生凭据工具(无 DB/config):`gen-key` / `derive-api-secret` / `gen-apikey` / `fingerprint` / `seal-pull-token` |
| `config`, `app/conductor`, `app/proxy` | 公共 declarative Config 与静态 custom conductor/Proxy App;node-ctl 用 sealed memfd + 原地 exec 交接,Hook 只在启动期调整 Config/Runtime material |
| `internal/orch` | 节点编排核心:生命周期、构建池、本节点路由权威、单元生成、重启对账 |
| `internal/nodectl` | reservation 控制器:admission、pool/水位与 grant 仲裁、inventory/StateSync 恢复、审计;不介入 sandbox balloon/cgroup 闭环 |
| `internal/nodelink` | node-link 通道(serve ↔ registry):注册 / 心跳 / 沙箱事件 / 命令,帧化 JSON over h2c |
| `internal/{registry,router,placer}` | 集群三角色:registry shardkv namespace + Reserve/Build 状态机 + node_link key cache / e2b 数据面入口与 active cache / provider/importer、WATCH_LIST、P2C 放置 |
| `internal/api` | e2b 控制面 REST(X-API-KEY 鉴权,export/import 扩展,ExecAccessToken 签发) |
| `internal/proxy` `internal/proxyshm` `internal/proxyadmission` `internal/routesync` | 数据面 L7 反代(service-addressed CONNECT,native exec gate),proxy master/worker 共享固定路由视图 + 独立 per-Sandbox inflight arena + MMDS confidential heap(park/wake,世代清扫/fail closed),路由/node-link 同步协议(注册 + bookmark + typed command result) |
| `internal/configsock` | 本机控制 socket:task(exact-run sandbox bootstrap/prepare + BuildSpec)/ admin(manifest-key + Sandbox MMDS value + Builder admission status)/ plugin(proxy 注册 + 受控路由流)/ api,SO_PEERCRED + pidfile 鉴权 |
| `internal/{apikey,secretbox,keys,regcreds}` | APISecret 派生与 api_key MAC,根凭据落盘 AES-GCM,Forward/Exec `kat1` 与数据面 token,ManifestKey 封装的镜像拉取凭据 |
| `internal/{config,clustercfg,sandboxcfg,store}` | 节点 / 集群配置加载、SANDBOX_CONFIG 渲染、节点本地 sqlite 状态(sandboxes/builds/manifest_keys 凭据对) |
| `internal/{mmds,mmdsrpc,mmdssvc,metrics,launcher,vswitch,util}` | MMDSv2 + exact route handler,Proxy worker/master 本机查询,HTTP-over-UDS service relay,Prometheus 文本,systemd D-Bus,connector-ctl vswitch 封装,内联工具 |
| `deploy/` | 每角色配置样例(`{conductor,proxy}.example.yaml`、`{registry,router,placer}.example.yaml`)与 systemd 单元(`node-ctl.service`、`node-proxy.service`、`cluster-{registry,router,placer}.service`) |
| `examples/custom-conductor` | 可编译的最小 xconductor；必须由 `node-ctl conductor serve` 进入 |
| `examples/custom-proxy` | 可编译的最小 xproxy；必须由 `node-ctl proxy serve` 进入，worker 由 master 自 reexec |

## 构建

```bash
make build                      # bin/<arch>/{node-ctl,cluster-ctl,node-stub-ctl,e2b-key-ctl};纯 Go,CGO_ENABLED=0
make build TARGET_ARCH=aarch64  # 交叉编译(别名 amd64 / arm64)
make test                       # 单元测试
make test-e2e                   # 运行 test/e2e/run_all.sh;重型用例使用项目主仓组装的完整 BIN
```

`orchestrator` 自身通过仓库 `main` 上受信任的 `Component Release` workflow
从调度器钉住的源码分支和精确 SHA 独立发布 `vX.Y.Z`。发布件
`orchestrator-vX.Y.Z-linux-x86_64.tar.gz` 包含节点与集群控制面二进制、部署模板、
不重复包含文档或 E2E。本模块文档与 `test/e2e/` 由项目主仓从所选 tag 聚合进
platform 包。平台聚合版本 `release-vX.Y.Z` 由项目主仓选择各组件版本并发布,
不从 `orchestrator/main` 推导组件组合。组件 `main` 用于主线,
`release/vX.Y.x` 用于对应组件维护线。Preview 和维护分支 Stable 不更新 GitHub
Latest;独立的幂等 Reconcile Latest 工作流按 `main` 源码提交先后协调主线 Stable,
同一提交才比较 SemVer。平台按精确 Tag 聚合组件,不依赖 Latest。当前 Release 只发布已完成全量构建与 BMS
验证的 Linux x86_64 目标。
同版本发布与删除共用完整 workflow mutation group;若 GitHub 合并 pending 请求,项目主仓
协调器会把 cancelled 状态作为未完成操作自动重跑,不会把它当作发布或 GC 已完成。

运行需要 systemd(D-Bus 管单元)与 root;沙箱本体另需 KVM、connector、sandboxer
和 guest-runtime 构建出的 `sandbox-runtime.bundle`。

## 快速开始

```bash
# 租户凭据:ManifestKey 保护内容,APISecret 认证 API;独立模式缺省从前者固定派生后成对入库
MK=$(e2b-key-ctl gen-key)
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
node-ctl manifest-key add --api-secret "$API_SECRET" --label tenant-a "$MK"
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$API_SECRET")

# 先启动 API-only conductor,再启动通过 config_socket 注册的 Proxy.
# conductor.yaml 内联 resource_listen 即内置资源控制器;配 cluster.node_link 时必须显式
# 提供不同用途的 api_endpoint 和 data_endpoint.
# 随附样例分别监听明文 :3000/:3443,proxy.yaml 也展示可选的
# traffic.max_inflight per-Sandbox 默认;生产 TLS 在 Cluster Router/LB 终止.
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml

# 可选:集群控制面——三角色各为独立进程、各自配置文件
cluster-ctl registry --config /etc/cluster-ctl/registry.yaml
cluster-ctl router   --config /etc/cluster-ctl/router.yaml
cluster-ctl placer   --config /etc/cluster-ctl/placer.yaml

# e2b SDK/CLI 直连本机(独立模式开发)
export E2B_API_URL=http://host:3000
export E2B_SANDBOX_URL=http://host:3443
python -c 'from e2b import Sandbox; s = Sandbox.create("e2b-img-<key>"); print(s.commands.run("uname -a").stdout)'
```

命令与参数详见 [docs/node.md](docs/node.md) §2,配置字段 §3,e2b API 契约 §4,集群接入 §10;
集群控制面见 [docs/cluster.md](docs/cluster.md)。

## 文档

- [docs/node.md](docs/node.md) — 节点主机设计与命令参考:架构 / e2b 契约 / 进程管理 /
  密钥模型 / 数据面装配 / 集群接入(node-link)/ 模板构建 / 可靠性 / 测试。
- [docs/node-proxy.md](docs/node-proxy.md) — 独立数据面转发层:单一 ingress / 路由判定 / master-worker/
  routesync / per-Sandbox max inflight / traffic stats / 数据面鉴权 / MMDS / service-addressed CONNECT /
  native exec gate.
- [docs/node-resource.md](docs/node-resource.md) — 节点资源控制协议(`sandbox-ctl` 拨号目标)与
  控制器(`serve` 经内联 `resource_listen` 内置):准入 / 水位额度 / 主动回收 / 无强一致状态恢复。
- [docs/cluster.md](docs/cluster.md) — 集群控制面:registry membership、shardkv 状态模型、
  node_link / route_link / node_list / placer_link、Reserve 状态机、成员变更与 e2e。
- [docs/cluster-router.md](docs/cluster-router.md) — 集群级数据面入口:e2b 头解析、调用方鉴权、
  ExecSession Reserve,两跳转发与 CONNECT,stable/NodeSandboxID 改写和 KAT 双验.
- [docs/cluster-placer.md](docs/cluster-placer.md) — 放置调度器:SandboxGroupProvider/Importer、
  placer memberlist、node_list WATCH_LIST、source lease、selector patch、PlaceSandbox / PlaceBuild。

## License

本仓库的项目原创内容采用 [Apache License 2.0](LICENSE).
贡献授权说明见 [CONTRIBUTING.md](CONTRIBUTING.md).
