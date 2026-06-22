# sandbox-orchestrator

e2b 兼容沙箱平台的**节点主机**与**集群控制面**,两个二进制、两层:

- **`node-ctl`**(节点)——计算节点上的单实例常驻 daemon。对外是 **e2b 兼容北向入口**
  (未改造的 e2b SDK/CLI 直接指向即可 create/exec/pause/resume/kill microVM 沙箱、构建
  自定义模板);内含数据面 proxy(反代 guest envd / floatingip)、可选的**节点资源控制器**
  (沙箱准入、内存预算分配、主动回收,把固定虚拟规格下的物理密度推到单节点 3,000+ 沙箱),
  以及 **node-link 客户端**(接入集群)。既可独立运行,也可经 node-link 交由 cluster-ctl 编排。
- **`cluster-ctl`**(集群)——面向大规模部署的控制面,把机群里数千个 `node-ctl` 聚合成一个
  逻辑沙箱池,**三角色均为独立进程**(无同进程内存模式):**registry**(持久状态权威 + 节点通道
  枢纽 + 租户密钥分发)、**router**(集群级数据面入口,按 sandbox-group + route-key 会话亲和
  转发)、**scaler**(独立进程,拨 registry、订阅视图,经 scaler-link 反向应答 P2C 放置建议)。
  沙箱按需创建 / 恢复 / 迁移,空闲下沉到节点本机快照乃至远程快照(可移植、不绑节点)。registry
  后端可插拔(**sqlite → etcd → raft**,持久无内存模式),sandbox-group 为天然分区键。

是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 平台的北向入口、节点
资源仲裁与集群编排器,独立演进。两类沙箱 profile:**e2b**(guest 内 envd,完整数据面)与
**bare**(无 envd,仅 floatingip)。全部纯 Go,`CGO_ENABLED=0`,无 gRPC/protobuf(协议为
帧化 JSON over h2c)。

## 组成

| 路径 | 角色 |
| --- | --- |
| `cmd/node-ctl` | 节点主二进制:`serve`(daemon:控制面 + 数据面 + 可选 `resource_listen` 资源控制器 + node-link 客户端)/ `proxy`(外置数据面 worker)/ `run-sandbox`·`run-builder`(单元内启动器)/ `resource {status,list,drain,grant,reclaim}` / `config` / `manifest-key` / `export-sandbox`·`import-sandbox` / `version` |
| `cmd/cluster-ctl` | 集群主二进制(三角色均独立进程):`registry`(持久状态权威 + 节点通道 + 密钥分发)/ `router`(e2b 入口)/ `scaler`(反向调用放置)/ `sandbox-group`(sandbox-group 配置)/ `config` / `version` |
| `cmd/e2b-key-ctl` | 纯派生凭据工具(无 DB/config):`gen-key` / `gen-apikey` / `fingerprint` / `seal-pull-token` |
| `internal/orch` | 节点编排核心:生命周期、构建池、本节点路由权威、单元生成、重启对账 |
| `internal/nodectl` | 资源控制器:两环仲裁、四级水位 + 应急池、cgroup 真相源对账恢复、审计 |
| `internal/nodelink` | node-link 通道(serve ↔ registry):注册 / 心跳 / 沙箱事件 / 命令,帧化 JSON over h2c |
| `internal/{registry,router,scaler}` | 集群三角色:注册表 + Reserve 状态机 + 密钥租约分发 + BuildStore / e2b 数据面入口 / 独立进程反向调用 P2C 放置 |
| `internal/api` | e2b 控制面 REST(X-API-KEY 鉴权、export/import 扩展) |
| `internal/proxy` `internal/routetable` `internal/routesync` | 数据面 L7 反代(含 CONNECT 隧道)、订阅者本地路由表(park/wake、世代清扫)、路由同步协议(注册 + bookmark) |
| `internal/configsock` | 本机控制 socket:task(LaunchSpec/BuildSpec)/ admin(manifest-key)/ plugin(proxy 注册 + 路由流)/ api 四平面,SO_PEERCRED 鉴权 |
| `internal/{apikey,secretbox,keys,regcreds}` | api_key 派生 MAC、manifest_key 落盘 AES-GCM、数据面 token、镜像拉取凭据 |
| `internal/{config,clustercfg,sandboxcfg,store}` | 节点 / 集群配置加载、SANDBOX_CONFIG 渲染、sqlite 状态(sandboxes/builds/manifest_keys) |
| `internal/{mmds,metrics,launcher,vswitch,util}` | MMDS 元数据(envd re-key)、Prometheus 文本、systemd D-Bus、vswitch-ctl 封装、内联工具 |
| `deps/build-runtime-{e2b,builder}.sh` | 把 envd(+构建工具链 flatten-ctl/mkfs.erofs)注入基础 runtime → `sandbox-runtime-{e2b,builder}.erofs`(确定性重打 + 2MiB 对齐) |
| `deploy/` | 每角色配置样例(`{serve,proxy}.example.yaml`、`{registry,router,scaler}.example.yaml`)与 systemd 单元(`node-ctl.service`、`node-proxy@.service`、`cluster-{registry,router,scaler}.service`) |

## 构建

```bash
make build                      # bin/<arch>/{node-ctl,cluster-ctl,e2b-key-ctl};纯 Go,CGO_ENABLED=0
make build TARGET_ARCH=aarch64  # 交叉编译(别名 amd64 / arm64)
make sandbox-runtime-e2b        # 注入 envd 的 guest runtime(需 sandbox-deps 的 envd/fsck.erofs/mkfs.erofs)
make sandbox-runtime-builder    # 构建沙箱 guest runtime(e2b flavor + flatten-ctl + mkfs.erofs)
make test                       # 单元测试;e2e 见 docs/node.md §16
```

运行需要 systemd(D-Bus 管单元)与 root;沙箱本体另需 KVM 与 vswitch(见各自仓)。

## 快速开始

```bash
# 租户凭据:根密钥入白名单,api key 给 SDK(独立模式;集群下密钥由 registry 租约预分发)
MK=$(e2b-key-ctl gen-key)
node-ctl manifest-key add "$MK" --label tenant-a
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$MK")

# 启动节点 daemon(必填仅 api.domain + encryption_key;骨架: node-ctl config serve --template)
# serve.yaml 内联 resource_listen 即内置资源控制器;配 cluster.registry 即接入集群
node-ctl serve --config /etc/node-ctl/serve.yaml

# 可选:集群控制面——三角色各为独立进程、各自配置文件(registry 持久 sqlite;router/scaler 拨 registry control_api)
cluster-ctl registry --config /etc/cluster-ctl/registry.yaml
cluster-ctl router   --config /etc/cluster-ctl/router.yaml
cluster-ctl scaler   --config /etc/cluster-ctl/scaler.yaml

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
- [docs/cluster.md](docs/cluster.md) — 集群控制面:node-link 线格式、节点 / sandbox-group / 沙箱
  注册表、Reserve 状态机、Store 接口与可扩展性。
- [docs/cluster-router.md](docs/cluster-router.md) — 集群级数据面入口:e2b 头解析、调用方鉴权、
  Reserve 消费、两跳转发与 CONNECT、token 注入。
- [docs/cluster-scaler.md](docs/cluster-scaler.md) — 放置调度器(独立进程、反向调用):nodeSelectors +
  shuffle-sharding(maglev)+ zone / 水位 / runtime 信号 + P2C、shuffle 选择器 patch-back、资源感知 PlaceBuild。
