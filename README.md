# sandbox-orchestrator

计算节点上的单实例编排 daemon,对外提供 **e2b 兼容 API**:未改造的 e2b SDK/CLI 直接
指向本机即可 create/exec/pause/resume/kill microVM 沙箱、构建自定义模板。一身兼 e2b
的 **api + orchestrator + proxy** 三角色——控制面 REST(沙箱生命周期 + 模板构建 +
鉴权)、经 systemd 模板单元驱动沙箱与构建(模板构建在**构建沙箱** microVM 内拉取
镜像、执行 steps,三阶段流水线)、把数据面流量反代到 guest 内**原版 envd**
(49983/Connect-RPC,经 UDS)或 floatingip 用户端口。是
[kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 平台的北向入口,
独立演进。

两类沙箱 profile:**e2b**(guest 内 envd,完整数据面)与 **bare**(无 envd,仅
floatingip 网络)。不感知资源仲裁(准入在 `sandbox-ctl` 内部);叶子组件,纯 Go,
`CGO_ENABLED=0`,无 gRPC/protobuf。

## 组成

| 路径 | 角色 |
| --- | --- |
| `cmd/orchestrator-ctl` | 主二进制:`serve`(daemon)/ `proxy`(外置数据面 worker)/ `run-sandbox`·`run-builder`(单元内启动器)/ `config` / `manifest-key` / `export-sandbox`·`import-sandbox` / `version` |
| `cmd/e2b-key-ctl` | 纯派生凭据工具(无 DB/config):`gen-key` / `gen-apikey` / `fingerprint` / `seal-pull-token` |
| `internal/api` | e2b 控制面 REST(X-API-KEY 鉴权、export/import 扩展) |
| `internal/orch` | 编排核心:生命周期、构建池、路由权威、单元生成、重启对账 |
| `internal/proxy` `internal/routetable` `internal/routesync` | 数据面 L7 反代、worker 本地路由表(park/wake)、serve↔worker 路由同步协议(帧化 JSON over h2c) |
| `internal/configsock` | 本机控制 socket:task(LaunchSpec/BuildSpec)/ admin(manifest-key)/ api 三平面,SO_PEERCRED 鉴权 |
| `internal/apikey` `internal/secretbox` `internal/keys` `internal/regcreds` | api_key 派生 MAC、manifest_key 落盘 AES-GCM、数据面 token、镜像拉取凭据 |
| `internal/config` `internal/sandboxcfg` `internal/store` | 配置加载、SANDBOX_CONFIG 渲染、sqlite 状态(sandboxes/builds/manifest_keys) |
| `internal/mmds` `internal/metrics` `internal/launcher` `internal/vswitch` | MMDS 元数据服务(envd re-key)、Prometheus 文本、systemd D-Bus、vswitch-ctl 封装 |
| `deps/build-runtime-e2b.sh` `deps/build-runtime-builder.sh` | 把 envd(+构建工具链 flatten-ctl/mkfs.erofs)注入基础 runtime → `sandbox-runtime-{e2b,builder}.erofs`(确定性重打 + 2MiB 对齐) |
| `deploy/` | 配置样例与 systemd 单元(`orchestrator-ctl.service`、`orchestrator-proxy@.service`) |

## 构建

```bash
make build                      # bin/<arch>/{orchestrator-ctl,e2b-key-ctl};纯 Go,CGO_ENABLED=0
make build TARGET_ARCH=aarch64  # 交叉编译(别名 amd64 / arm64)
make sandbox-runtime-e2b        # 注入 envd 的 guest runtime(需 sandbox-deps 的 envd/fsck.erofs/mkfs.erofs)
make sandbox-runtime-builder    # 构建沙箱 guest runtime(e2b flavor + flatten-ctl + mkfs.erofs)
make test                       # 单元测试;e2e 见 docs/orchestrator.md §15
```

运行需要 systemd(D-Bus 管单元)与 root;沙箱本体另需 KVM 与 vswitch(见各自仓)。

## 快速开始

```bash
# 准备租户凭据:根密钥入白名单,api key 给 SDK
MK=$(e2b-key-ctl gen-key)
orchestrator-ctl manifest-key add "$MK" --label tenant-a
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$MK")

# 启动 daemon(必填仅 api.domain + encryption_key;骨架: orchestrator-ctl config --template)
orchestrator-ctl serve --config /etc/orchestrator-ctl/config.yaml

# e2b SDK/CLI 直连本机
export E2B_DOMAIN=sandboxes.example.com     # dev: E2B_API_URL/E2B_SANDBOX_URL http
python -c 'from e2b import Sandbox; s = Sandbox.create("e2b-img-<key>"); print(s.commands.run("uname -a").stdout)'
```

命令与参数详见 [docs/orchestrator.md](docs/orchestrator.md) §2,配置字段 §3,e2b API
契约 §4。

## 文档

- [docs/orchestrator.md](docs/orchestrator.md) — 设计与命令参考:架构/e2b 契约/进程
  管理/密钥模型/数据面 proxy/构建/可靠性/测试。
