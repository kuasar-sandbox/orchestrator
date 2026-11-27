[English](README.md) | [简体中文](README_zh.md)

<a id="overview"></a>

# orchestrator

`orchestrator` 为 [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 提供 **E2B 兼容 API、节点服务和多节点控制面**。用它构建模板、创建和管理 Sandbox、接入 SDK 客户端，并运维计算节点集群。

首次部署请从主项目[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)进入：使用 **node-infra** 安装匹配的聚合版本，验证持久单节点，再构建自己的模板。聚合版本提供相互兼容的运行时、网络、存储和编排组件。本仓指南负责下列具体操作与配置。

<a id="documentation"></a>

## 推荐使用顺序

| 下一步任务 | 指南 | 完成判据 |
|---|---|---|
| 安装并启动持久节点 | 主项目[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)，再看[节点运维](docs/node_zh.md#operate-a-node) | Conductor、Proxy 正常运行，客户端可访问两个入口 |
| 构建应用模板 | [模板构建](docs/node-build_zh.md#client-workflow) | 本次 Build 达到 `ready`，保存其 canonical TemplateID |
| 运行、暂停和重新连接 Sandbox | [生命周期](docs/sandbox-lifecycle_zh.md#recommended-workflow)与 [SDK 接入](docs/node-proxy_zh.md#connect-a-client) | 命令和文件操作能通过配置的数据入口完成 |
| 开通出站访问或调整网络策略 | 主项目[网络指南](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/node-infra_zh.md#native-egress) | Guest 经所选 egress 路径访问预期目的地 |
| 增加第二个计算节点 | [首次集群](docs/cluster_zh.md#minimum-cluster) | 两个 Node 均发布目标 Group 版本，默认模板 eligible |
| 排查容量、流量或失败 | [资源管理](docs/node-resource_zh.md#operate-resources)、[可观测性](docs/observability_zh.md#diagnose-a-request) | 重试前先检查原 SandboxID/BuildID 及负责该操作的服务 |

模板 canonical ID 来自成功的 Build，不能用任意镜像名替代。Create 被接受表示生命周期操作已经开始，不表示 guest 已经就绪。对应指南说明等待条件和需要保留的身份。

### 能力参考

每篇文档拥有相应字段及行为定义。先提供用户流程，再介绍配置、恢复和实现契约。中英文版本覆盖相同能力。

| 主题 | 范围 |
|---|---|
| [节点运维](docs/node_zh.md) | 服务、配置、systemd worker、本地通道与恢复 |
| [Sandbox 生命周期](docs/sandbox-lifecycle_zh.md) | Create、Pause/Resume、workspace、Reset、Delete 和迁移 |
| [模板构建](docs/node-build_zh.md) | 构建输入、目标选择、canonical artifact、取消与保留 |
| [访问与代理](docs/node-proxy_zh.md) | SDK、凭据、native exec、转发、MMDS 与流量准入 |
| [资源与准入](docs/node-resource_zh.md) | 容量、启动策略、资源池、drain 与压力恢复 |
| [可观测性](docs/observability_zh.md) | 日志、即时统计、精确用量、OTLP 指标与历史查询 |
| [集群控制](docs/cluster_zh.md) | Registry/Router、Group 发布、Node 选点、路由与恢复 |
| [应用扩展](docs/extensions_zh.md) | 自定义 Conductor/Proxy/Telemetry 与公开 Go API |

<a id="responsibilities"></a>

## 服务与部署选择

<a id="standalone-node"></a>

### 单节点

计算节点运行 **Conductor**，负责 API 与生命周期；运行 **Proxy**，负责 Sandbox 数据流量。需要指标采集或历史查询时增加 **Telemetry**。Conductor 通过 systemd 管理 Sandbox 和 Build worker；用户使用 API 或 SDK，无需手工启动 `run-sandbox`、`run-builder`。

单节点支持文档列出的 E2B SDK/CLI 流程。API 与数据 listener 相互独立：开发 HTTP 接入需配置两个地址，TLS 部署使用对应域名。[访问与代理](docs/node-proxy_zh.md#connect-a-client)说明接入方式与已验证 SDK 版本。

<a id="multi-node-cluster"></a>

### 多节点集群

从[一个 SQLite Registry、一个 Router、两个 Node](docs/cluster_zh.md#minimum-cluster)开始。Node 保留本地生命周期和完整 Group 上下文；Registry 保存集群元数据，从 Node 已发布的 Group 支持中选点；Router 提供统一 API 与数据入口。

**Placer 可选。** 需要按来源导入 Group 或自动续租凭据时再部署。首次集群直接使用 `node-ctl group apply` 即可。SQLite 仅支持一个 Registry；部署多个 Registry 成员时选择共享 PostgreSQL 或 FoundationDB。后端选择和恢复见[集群元数据存储](docs/cluster-storage_zh.md)。

<a id="profiles"></a>

## 选择 Sandbox profile

- **`e2b`** 运行 Envd，提供支持的 SDK 命令、文件与代码解释器服务。
- **`bare`** 运行无需 Envd 的客户镜像，通过镜像自身网络服务或 native exec 访问。

Build 注册默认使用 `e2b`。输出 profile 是 artifact 身份的一部分，不能通过改模板名称切换。见 [profile](docs/node_zh.md#two-sandbox-profiles)与[Build 目标](docs/node-build_zh.md#artifact-identity)。

<a id="main-commands"></a>

## 运维命令

| 命令 | 用途 |
|---|---|
| `node-ctl --version` / `e2b-key-ctl --version` | 检查已安装节点工具版本 |
| `node-ctl config conductor --config FILE --resolve` | 部署前检查最终配置 |
| `node-ctl conductor serve` / `proxy serve` / `telemetry serve` | 服务入口，通常由部署的 unit 启动 |
| `node-ctl resource status` / `list` / `pressure` | 检查准入、reservation 和资源压力 |
| `node-ctl resource drain` / `drain --disable` | 暂停或恢复新准入 |
| `node-ctl builder status` | 检查 Build 执行 |
| `node-ctl group apply -f FILE` | 使用租户 API key 维护 Node Group |
| `run-cluster-ctl status ...` | 检查鉴权后的 Group 发布与模板可用性 |
| `run-cluster-ctl registry` / `router` / `placer` | 完整原生发布包中的集群服务入口 |
| `node-ctl export-sandbox` / `import-sandbox` | 按[导出/导入契约](docs/sandbox-lifecycle_zh.md#export-import-command)转移可移植状态 |
| `e2b-key-ctl ...` | 生成和派生租户凭据 |

资源命令 `status`、`list`、`drain` 使用资源控制器 socket；`pressure`、Group 和 manifest-key 管理使用 Conductor 控制 socket。部署修改路径后，应为各命令指定对应 `--socket`，不能统一指向同一个 socket。完整参数由各能力指南维护。

<a id="deployment-overview"></a>

## 部署与日常运维

使用 [node-infra](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/node-infra_zh.md) 安装发布版本和持久服务布局。接纳业务前，准备持久 Node 状态及其加密密钥、租户凭据、兼容 kernel/runtime、可写存储、网络 egress，以及经工作负载验证的资源预算。主项目[部署指南](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment_zh.md)维护跨组件安装、信任边界和升级流程。

现有安装应用本仓示例时，保留其服务名称、overlay 顺序、socket 和状态路径。`deploy/` 是组件配置示例，不应直接覆盖已经工作的 node-infra 配置。先验证合并后的完整配置，再使用已安装的服务 unit。配置检查不会启动服务，也不会移动现有 worker。

<a id="security-and-multi-tenancy"></a>

## 凭据与租户隔离

通过授权的本地管理入口安装每个租户的 **APISecret/ManifestKey 对**，向应用客户端提供生成的 API key。数据面 capability 分别限定用途与范围；ManifestKey 和节点数据库加密密钥都不应进入 SDK 或 guest 镜像。清除 allowlist 影响新资源准入，不会自动撤销已经绑定到现存资源的凭据。见[凭据生命周期](docs/node-proxy_zh.md#credential-model)。

将节点内部 listener 和管理 socket 保持在部署信任边界内。Registry/NodeLink TLS、Router 公共 TLS 与 Router 到 Node 的传输是不同链路；按[集群信任边界](docs/cluster_zh.md#minimum-cluster)部署。漏洞请通过项目[安全政策](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy)私下报告。

<a id="development-example"></a>

## 开发与定制

源码开发、Demo 和跨组件验证使用主项目 [Workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README_zh.md)。需要服务定制时，从[应用扩展](docs/extensions_zh.md)及可运行示例开始。组件构建、贡献要求和测试见 [CONTRIBUTING_zh.md](CONTRIBUTING_zh.md)。

<a id="contract-ownership"></a>

## 参考文档归属

各能力指南定义自己的配置与失败语义。节点目录和身份属于[节点运维](docs/node_zh.md#node-directories-and-identities)；公共 Sandbox 身份和清理属于[生命周期](docs/sandbox-lifecycle_zh.md)；凭据绑定属于[访问](docs/node-proxy_zh.md#credential-model)；artifact 发布与 Build 记录保留属于[Build](docs/node-build_zh.md)。集群投影和恢复复用节点操作，不改变这些职责。

<a id="release-model"></a>

## 发布模型

本仓发布组件 tag `vX.Y.Z`。主仓聚合 tag `release-vX.Y.Z` 选择精确组件版本并验证组合系统；两者版本号无需相同。安装完整的对应原生架构归档，包括 cluster 启动器的私有运行库。源码支持某架构，不表示所有历史版本都提供该架构资产。

版本选择、升级和回滚规则统一由主项目[发布指南](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md)维护。通过[聚合发布](https://github.com/kuasar-sandbox/kuasar-sandbox/releases)选择兼容平台，普通部署从[最新 Stable 聚合版本](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)开始。

<a id="project-boundaries"></a>

## 项目边界

| 仓库 | 职责 |
|---|---|
| [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) | 平台安装、用户流程、集成验证和聚合发布 |
| [sandboxer](https://github.com/kuasar-sandbox/sandboxer) | MicroVM 生命周期、快照与 guest 控制 |
| [accelerator](https://github.com/kuasar-sandbox/accelerator) | 镜像、artifact 与快照数据基础设施 |
| [connector](https://github.com/kuasar-sandbox/connector) | MicroVM 网络与 egress |
| [guest-runtime](https://github.com/kuasar-sandbox/guest-runtime) | Guest kernel 和 runtime 镜像 |

<a id="license"></a>

## License

原创内容采用 [Apache License 2.0](LICENSE)。生成或第三方材料须保留适用 attribution/license 声明。
见 [CONTRIBUTING.md](CONTRIBUTING.md) 与 [Organization 贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。

固定版本的 `rootless-containers/proto/go-proto` 模块归档缺少仓库根目录许可证。打包仅为该精确版本补入 [scripts/release-notices/rootless-containers-proto](scripts/release-notices/rootless-containers-proto/NOTICE) 中未修改的上游 Apache-2.0 声明，核验摘要并保留正常 source/checksum 清单。其他版本仍须提供各自的许可材料。
