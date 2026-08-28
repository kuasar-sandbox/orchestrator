# orchestrator

e2b 兼容沙箱平台的**节点主机**与**集群控制面**,两个生产二进制、一个 e2e 辅助二进制、两层:

- **`node-ctl`**(节点)——计算节点上的单实例常驻 daemon。对外是 **e2b 兼容北向入口**
  (未改造的 e2b SDK/CLI 直接指向即可 create/exec/pause/resume/kill microVM 沙箱、构建
  自定义模板);同时提供显式 ExecAccessToken 签发和 `service=exec` native exec,
  不依赖 guest envd;内含数据面 proxy(反代 guest envd / floatingip / native exec),可选的**节点资源控制器**
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

## 组成

| 路径 | 角色 |
| --- | --- |
| `cmd/node-ctl` | 节点主二进制:`serve`(daemon:控制面 + 数据面 + 可选 `resource_listen` reservation 控制器 + node-link 客户端)/ `proxy`(外置数据面 master + 内部 worker)/ `run-sandbox`·`run-builder`(单元内启动器)/ `resource {status,list,drain}` / `builder status` / `config` / `manifest-key` / `export-sandbox`·`import-sandbox` / `version` |
| `cmd/cluster-ctl` | 集群主二进制(三角色均独立进程):`registry`(shardkv 执行态 + node_link / route_link / node_list / placer_link)/ `router`(e2b 入口)/ `placer`(provider/importer + WATCH_LIST + Place)/ `config` / `version` |
| `cmd/node-stub-ctl` | 集群 e2e 辅助二进制:一个进程模拟多个 node-link 节点,提供 admin/data API 控制重启、清空、沙箱/build 状态和故障注入,不启动 microVM |
| `cmd/e2b-key-ctl` | 纯派生凭据工具(无 DB/config):`gen-key` / `derive-api-secret` / `gen-apikey` / `fingerprint` / `seal-pull-token` |
| `config`, `app/conductor`, `app/proxy` | 公共 declarative Config 与静态 custom conductor/external-proxy App；node-ctl 用 sealed memfd + 原地 exec 交接，Hook 只在启动期调整 Config/Runtime material |
| `internal/orch` | 节点编排核心:生命周期、构建池、本节点路由权威、单元生成、重启对账 |
| `internal/nodectl` | reservation 控制器:admission、pool/水位与 grant 仲裁、inventory/StateSync 恢复、审计;不介入 sandbox balloon/cgroup 闭环 |
| `internal/nodelink` | node-link 通道(serve ↔ registry):注册 / 心跳 / 沙箱事件 / 命令,帧化 JSON over h2c |
| `internal/{registry,router,placer}` | 集群三角色:registry shardkv namespace + Reserve/Build 状态机 + node_link key cache / e2b 数据面入口与 active cache / provider/importer、WATCH_LIST、P2C 放置 |
| `internal/api` | e2b 控制面 REST(X-API-KEY 鉴权,export/import 扩展,ExecAccessToken 签发) |
| `internal/proxy` `internal/proxyshm` `internal/routesync` | 数据面 L7 反代(service-addressed CONNECT,native exec gate),proxy master/worker 共享固定路由视图 + MMDS confidential heap(park/wake,世代清扫/fail closed),路由/node-link 同步协议(注册 + bookmark + typed command result) |
| `internal/configsock` | 本机控制 socket:task(exact-run sandbox bootstrap/prepare + BuildSpec)/ admin(manifest-key + Sandbox MMDS value + Builder admission status)/ plugin(proxy 注册 + 受控路由流)/ api,SO_PEERCRED + pidfile 鉴权 |
| `internal/{apikey,secretbox,keys,regcreds}` | APISecret 派生与 api_key MAC,根凭据落盘 AES-GCM,Forward/Exec `kat1` 与数据面 token,ManifestKey 封装的镜像拉取凭据 |
| `internal/{config,clustercfg,sandboxcfg,store}` | 节点 / 集群配置加载、SANDBOX_CONFIG 渲染、节点本地 sqlite 状态(sandboxes/builds/manifest_keys 凭据对) |
| `internal/{mmds,mmdsrpc,mmdssvc,metrics,launcher,vswitch,util}` | MMDSv2 + exact route handler、external worker/master 本机查询、HTTP-over-UDS service relay、Prometheus 文本、systemd D-Bus、connector-ctl vswitch 封装、内联工具 |
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

`orchestrator` 自身通过 `Component Release` workflow 独立发布 `vX.Y.Z`。发布件
`orchestrator-vX.Y.Z-linux-x86_64.tar.gz` 包含节点与集群控制面二进制、部署模板、
不重复包含文档或 E2E。本模块文档与 `test/e2e/` 由项目主仓从所选 tag 聚合进
platform 包。平台聚合版本 `release-vX.Y.Z` 由项目主仓选择各组件版本并发布,
不从 `orchestrator/main` 推导组件组合。当前 Release
只发布已完成全量构建与 BMS 验证的 Linux x86_64 目标。

运行需要 systemd(D-Bus 管单元)与 root;沙箱本体另需 KVM、connector、sandboxer
和 guest-runtime 构建出的 `sandbox-runtime.bundle`。

## 快速开始

```bash
# 租户凭据:ManifestKey 保护内容,APISecret 认证 API;独立模式缺省从前者固定派生后成对入库
MK=$(e2b-key-ctl gen-key)
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
node-ctl manifest-key add --api-secret "$API_SECRET" --label tenant-a "$MK"
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$API_SECRET")

# 启动节点 daemon(必填仅 api.domain + encryption_key;骨架: node-ctl config conductor --template)
# conductor.yaml 内联 resource_listen 即内置资源控制器;配 cluster.node_link 即接入集群
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml

# 可选:集群控制面——三角色各为独立进程、各自配置文件
cluster-ctl registry --config /etc/cluster-ctl/registry.yaml
cluster-ctl router   --config /etc/cluster-ctl/router.yaml
cluster-ctl placer   --config /etc/cluster-ctl/placer.yaml

# e2b SDK/CLI 直连本机(独立模式)
export E2B_DOMAIN=sandboxes.example.com     # dev: E2B_API_URL/E2B_SANDBOX_URL http
python -c 'from e2b import Sandbox; s = Sandbox.create("e2b-img-<key>"); print(s.commands.run("uname -a").stdout)'
```

命令与参数详见 [docs/node.md](docs/node.md) §2,配置字段 §3,e2b API 契约 §4,集群接入 §10;
集群控制面见 [docs/cluster.md](docs/cluster.md)。

## 文档

- [docs/node.md](docs/node.md) — 节点主机设计与命令参考:架构 / e2b 契约 / 进程管理 /
  密钥模型 / 数据面装配 / 集群接入(node-link)/ 模板构建 / 可靠性 / 测试。
- [docs/node-proxy.md](docs/node-proxy.md) — 数据面转发层:路由判定 / 部署模式(internal/external/off)/
  routesync / 数据面鉴权 / MMDS / service-addressed CONNECT / native exec gate.
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
