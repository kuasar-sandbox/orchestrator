# orchestrator

e2b 兼容沙箱平台的**节点主机**与**集群控制面**,两个生产二进制、一个 e2e 辅助二进制、两层:

- **`node-ctl`**(节点)——计算节点上的单实例常驻 daemon。对外是 **e2b 兼容北向入口**
  (未改造的 e2b SDK/CLI 直接指向即可 create/exec/pause/resume/kill microVM 沙箱、构建
  自定义模板);内含数据面 proxy(反代 guest envd / floatingip)、可选的**节点资源控制器**
  (沙箱准入、内存预算分配、主动回收,把固定虚拟规格下的物理密度推到单节点 3,000+ 沙箱),
  以及 **node-link 客户端**(接入集群)。既可独立运行,也可经 node-link 交由 cluster-ctl 编排。
- **`cluster-ctl`**(集群)——面向大规模部署的控制面,把机群里数千个 `node-ctl` 聚合成一个
  逻辑沙箱池,**三角色均为独立进程**:**registry**(Dragonboat/Pebble Multi-Raft Route/Build
  共识状态 + Session Holder)、**router**(集群级数据面入口,按 sandbox-group + route-key
  会话亲和转发)、**placer**(Provider/Policy 解析、RandomN 候选生成与放置调度)。
  沙箱按需创建 / 恢复 / 迁移,空闲下沉到节点本机快照乃至远程快照(可移植、不绑节点)。registry
  自身按 sandbox-group 分片复制状态;整套 registry 下电但 shard 数据保留时可恢复原状态。执行 shard
  不可恢复地丢失后不得从备份构造 node 执行态;存活 node 的投影恢复和 migration-token 持久 route
  分别由 [#34](https://github.com/kuasar-sandbox/orchestrator/issues/34) 与
  [#33](https://github.com/kuasar-sandbox/orchestrator/issues/33) 跟踪。

是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 平台的北向入口、节点
资源仲裁与集群编排器,独立演进。两类沙箱 profile:**e2b**(guest 内 envd,完整数据面)与
**bare**(无 envd,仅 floatingip)。全部纯 Go,`CGO_ENABLED=0`,无 gRPC/protobuf(协议为
帧化 JSON over h2c)。

## 组成

| 路径 | 角色 |
| --- | --- |
| `cmd/node-ctl` | 节点主二进制:`serve`(daemon:控制面 + 数据面 + 可选 `resource_listen` 资源控制器 + node-link 客户端)/ `proxy`(外置数据面 worker)/ `run-sandbox`·`run-builder`(单元内启动器)/ `resource {status,list,drain,grant,reclaim}` / `config` / `key-lease` / `export-sandbox`·`import-sandbox` / `version` |
| `cmd/cluster-ctl` | 集群主二进制:`registry`(System Group + 4096 Data Raft Groups、Route/Build API、Session Holder)/ `router`(e2b 入口与 fenced forwarding cache)/ `placer`(Provider/Policy + RandomN) / `registry-layout` / `config` / `version` |
| `cmd/node-stub-ctl` | 集群 e2e 辅助二进制:一个进程模拟多个 node-link 节点,提供 admin/data API 控制重启、清空、沙箱/build 状态和故障注入,不启动 microVM |
| `cmd/e2b-key-ctl` | 纯派生凭据工具(无 DB/config):`gen-key` / `gen-apikey` / `fingerprint` / `seal-pull-token` |
| `internal/orch` | 节点编排核心:生命周期、构建池、本节点路由权威、单元生成、重启对账 |
| `internal/nodectl` | 资源控制器:两环仲裁、四级水位 + 应急池、cgroup 真相源对账恢复、审计 |
| `internal/nodelink` | node-link 通道(serve ↔ registry):注册 / 心跳 / 沙箱事件 / 命令,帧化 JSON over h2c |
| `internal/{controlplane,raftstore,session,routeapi,routeclient,finalrouter,placer}` | 最终集群控制面:Multi-Raft 状态机、Session/Directory、可信内部 API、Router cache fencing 与 Probe/P2C 放置 |
| `internal/api` | e2b 控制面 REST(X-API-KEY 鉴权、export/import 扩展) |
| `internal/proxy` `internal/proxyshm` `internal/routesync` | 数据面 L7 反代(含 CONNECT 隧道)、proxy master/worker 共享路由视图(park/wake、世代清扫)、路由同步协议(注册 + bookmark) |
| `internal/configsock` | 本机控制 socket:task(LaunchSpec/BuildSpec)/ admin(key-lease)/ plugin(proxy 注册 + 路由流)/ api 四平面,SO_PEERCRED 鉴权 |
| `internal/{apikey,secretbox,keys,regcreds}` | AuthKey API MAC、ManifestKey 内容加密、数据面 token、镜像拉取凭据 |
| `internal/{config,clustercfg,sandboxcfg,store}` | 节点 / 集群配置加载、SANDBOX_CONFIG 渲染、节点本地 sqlite 状态(sandboxes/builds/key_leases) |
| `internal/{mmds,metrics,launcher,vswitch,util}` | MMDS 元数据(envd re-key)、Prometheus 文本、systemd D-Bus、connector-ctl vswitch 封装、内联工具 |
| `deploy/` | 每角色配置样例(`{conductor,proxy}.example.yaml`、`{registry,router,placer}.example.yaml`)与 systemd 单元(`node-ctl.service`、`node-proxy.service`、`cluster-{registry,router,placer}.service`) |

## 构建

```bash
make build                      # bin/<arch>/{node-ctl,cluster-ctl,node-stub-ctl,e2b-key-ctl};纯 Go,CGO_ENABLED=0
make build TARGET_ARCH=aarch64  # 交叉编译(别名 amd64 / arm64)
make test                       # 单元测试
make test-e2e                   # 启动真实 registry/router/placer + node-stub-ctl 做集群 stub e2e
```

运行需要 systemd(D-Bus 管单元)与 root;沙箱本体另需 KVM、connector、sandboxer
和 guest-runtime 构建出的 `sandbox-runtime.erofs`。

## 快速开始

```bash
# 租户凭据:AuthKey 只做 API 鉴权,ManifestKey 只做内容加密。
AUTH_KEY=$(e2b-key-ctl gen-key)
MANIFEST_KEY=$(e2b-key-ctl gen-key)
node-ctl key-lease put --group /tenant-a --auth-key "$AUTH_KEY" \
  --manifest-key "$MANIFEST_KEY" --label tenant-a
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$AUTH_KEY")

# 启动节点 daemon(必填仅 api.domain + encryption_key;骨架: node-ctl config conductor --template)
# conductor.yaml 内联 resource_listen 即内置资源控制器;配 cluster.node_link 即接入集群
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml

# 可选:集群控制面——三角色各为独立进程、各自配置文件
(cd /etc/cluster-ctl && cluster-ctl registry --config registry.yaml)
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
  routesync / 数据面鉴权 / MMDS / CONNECT 隧道。
- [docs/node-resource.md](docs/node-resource.md) — 节点资源控制协议(`sandbox-ctl` 拨号目标)与
  控制器(`serve` 经内联 `resource_listen` 内置):准入 / 水位额度 / 主动回收 / 无强一致状态恢复。
- [docs/cluster.md](docs/cluster.md) — 集群控制面权威设计:System Group、Multi-Raft Route/Build、
  Serve Permit、Session/Placement、node-local Admission、恢复、成员变更与验收门槛。
- [docs/cluster-router.md](docs/cluster-router.md) — 集群级数据面入口:e2b 头解析、调用方鉴权、
  Reserve 消费、两跳转发与 CONNECT、token 注入。
- [docs/cluster-placer.md](docs/cluster-placer.md) — 放置调度器:Provider/Policy、Catalog、RandomN、
  request-time Probe/P2C、typed dispatch spec 与稳定负载模型。
