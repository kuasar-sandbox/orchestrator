[English](README.md) | [简体中文](README_zh.md)

<a id="overview"></a>

# orchestrator

首次运行跨组件沙箱或 Demo，推荐从项目[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)
使用匹配聚合版本的 [workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README_zh.md)。
本组件仍可独立构建和部署，workbench 不是生产运行的强制依赖。预构建架构以所选
发布版的实际资产为准；源码支持某架构不意味着所有历史版本都提供该架构制品。

`orchestrator` 是 [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 的 **E2B 兼容节点服务与多节点控制面**。

它提供北向 API、节点本地生命周期编排、数据面 Proxy 集成、租户凭据、资源准入,以及运营集群的 Registry/Router/Placer。
仓库独立演进和发布,同时参加项目级跨组件验证与聚合发布。

<a id="documentation"></a>

## 按能力阅读

以下八个主题同时维护可操作用法与完整能力契约；公共使用在前，内部所有权和协议在后，并提供英中配对版本。跨组件安装与升级由[主项目](https://github.com/kuasar-sandbox/kuasar-sandbox)维护；源码贡献和组件打包见[贡献指南](CONTRIBUTING_zh.md)。

| 主题 | 内容 |
|---|---|
| [节点运行](docs/node_zh.md) | 服务、共享配置、systemd 池、本地通道与恢复 |
| [Sandbox 生命周期](docs/sandbox-lifecycle_zh.md) | Create 与身份、Pause/Resume、Reset/dead 保留、Delete 与迁移 |
| [模板 Build](docs/node-build_zh.md) | Build API、请求输入、不可变制品、执行、发布与取消 |
| [访问与代理](docs/node-proxy_zh.md) | SDK 访问、凭据、exec、转发、MMDS、流量准入与 routesync |
| [资源与准入](docs/node-resource_zh.md) | capacity/startup 策略、reservation、节点压力、恢复与资源协议 |
| [观测](docs/observability_zh.md) | Journal 输出、native stats、无损 usage、OTLP metrics 与历史查询 |
| [集群控制](docs/cluster_zh.md) | 公共入口、Group、角色配置、复制所有权、路由与恢复 |
| [应用扩展](docs/extensions_zh.md) | 静态 App bootstrap、公共 Go 契约、对象源、Hook 与 wrapper |

<a id="responsibilities"></a>

## 职责

<a id="standalone-node"></a>

### 独立节点

`node-ctl` 运行计算节点服务:

- **Conductor**:权威控制 API、Sandbox/Build 生命周期、本地路由、凭据管理与可选节点资源准入。
- **Proxy**:唯一沙箱数据入口,负责 Envd、floating-IP 服务、native exec、MMDS 与 traffic observation。
- **Telemetry**：独立 envd/native stats/OTLP 采集、可信沙箱身份、原生 Collector pipeline、可选本地 TSDB、独立远端写入与查询、E2B/自定义 HTTP handler。
- **Node link**:可选客户端,把节点接入集群控制面。
- **Launch helpers**:systemd 管理的 Sandbox/Build runner。
- **Administration**:资源状态/drain、模板构建状态、配置检查、manifest-key 管理与 Sandbox export/import。

独立部署的 E2B 兼容 API 可供未改造的 E2B SDK/CLI 使用。

<a id="multi-node-cluster"></a>

### 多节点集群

`cluster-ctl` 运行三个独立角色:

- **Registry**:复制的集群状态与节点通道枢纽。
- **Router**:集群级 E2B 兼容控制面与数据面统一入口,按稳定沙箱身份路由。
- **Placer**:sandbox-group provider/importer，以及创建、恢复和迁移的节点放置建议。生命周期提交与执行由 Registry 和 Node 负责。

控制面保留稳定的外部 Sandbox 身份,实际 node-local 实例可在恢复/迁移时改变。

<a id="security-and-multi-tenancy"></a>

## 安全与多租户

节点与集群区分平台凭据、内容保护密钥和有作用域的数据面 capability。API、普通数据转发与 native exec 使用各自用途的凭据,内容保护根密钥不暴露给 guest。

节点 reservation controller 拥有全节点准入、资源池记账、grant、水位与恢复,也是多租户边界的一部分。
每个 Sandbox 的 cgroup、balloon 与 VMM 执行由 sandboxer 维护;节点控制器不接管 Sandbox 内部资源闭环。

安全架构与部署信任边界见[项目系统概览](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/architecture_zh.md)和[部署指南](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment_zh.md)。漏洞通过 [Kuasar Sandbox 安全策略](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy)中的私密渠道报告。

<a id="main-commands"></a>

## 主要命令

| 命令 | 用途 |
|---|---|
| `node-ctl conductor serve` | 启动权威节点控制 API |
| `node-ctl proxy serve` | 启动数据面 Proxy master/worker |
| `node-ctl telemetry serve` | 启动沙箱指标采集与历史查询 |
| `node-ctl run-sandbox` | 在受管 unit 中启动一个 Sandbox |
| `node-ctl run-builder` | 启动一次模板构建执行 |
| `node-ctl resource ...` | 检查或 drain 节点资源 reservation |
| `node-ctl builder status` | 检查 Build 执行状态 |
| `node-ctl export-sandbox` / `import-sandbox` | 跨节点移动 portable Sandbox 状态 |
| `cluster-ctl registry` | 启动 Registry |
| `cluster-ctl router` | 启动集群控制与数据统一入口 |
| `cluster-ctl placer` | 启动放置服务 |
| `e2b-key-ctl ...` | 生成和派生租户凭据 |

move 导出成功表示源删除已被持久接纳；RunDir、BaseDir 与 checkpoint 的物理清理可能
稍后完成。同 ID Create/import 在清理完成前仍冲突。`--keep-source` 保留原源。详见
[节点文档](docs/node_zh.md)的 export/import 契约。

仓库还构建 `node-stub-ctl`,用于模拟 node-link 参与方而不启动 microVM 的 E2E helper。

<a id="profiles"></a>

## Profiles

- **e2b**:guest 运行 Envd,提供已验证的 E2B 兼容数据服务。
- **bare**:guest 不要求 Envd,通过自身网络服务与原生平台集成访问。

<a id="deployment-overview"></a>

## 部署概览

独立节点运行 Conductor 与 Proxy；按需独立启动 Telemetry。Telemetry 复用 Plugin Plane，不加入 Create/Resume barrier，其故障不改变 Sandbox lifecycle。示例配置显式启用 embedded Prometheus TSDB；本地存储必须通过 `local.enabled` 选择，未配置的 query backend 默认 `none`，写入与查询独立装配。完整选择见[观测](docs/observability_zh.md#telemetry)。集群另加 Registry、Router、Placer，配置样例与 systemd unit 位于 `deploy/`。

面向用户的发布件安装流程见项目[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)。生产部署应使用持久存储、生产 TLS、受保护凭据、明确网络策略及基于实际 workload 验证的容量设置。

<a id="development-example"></a>

## 开发示例

先按项目快速开始准备二进制、Kernel/Runtime 制品、网络与受保护配置。下面每个常驻角色分别使用服务或独立终端,
不是在一个前台 shell 中依次执行。先启动 Conductor,再调用其 manifest-key 管理 socket。
示例使用默认 config-socket;自定义路径须传对应 CLI 选项。生成凭据与 shell 环境均按敏感数据保护。

```bash
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
# 可选：独立指标采集与历史查询服务
node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
```

样例明文 API 为 `:3000`,data 为 `:3443`。配置可启用 resource_listen 与 Proxy traffic.max_inflight 默认值。
cluster 注册须区分 api_endpoint/data_endpoint;生产入口 TLS 与受信 node-link/网络边界遵循部署 owner。

```bash
# 租户根凭据:独立模式缺省从 ManifestKey 派生 APISecret。
MK=$(e2b-key-ctl gen-key)
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
node-ctl manifest-key add --api-secret "$API_SECRET" --label tenant-a "$MK"
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$API_SECRET")

# 可选集群角色,各使用独立服务/终端。
cluster-ctl registry --config /etc/cluster-ctl/registry.yaml
cluster-ctl router   --config /etc/cluster-ctl/router.yaml
cluster-ctl placer   --config /etc/cluster-ctl/placer.yaml

# 独立模式开发,替换 host 与 canonical template 占位符。
export E2B_API_URL=http://host:3000
export E2B_SANDBOX_URL=http://host:3443
python - <<'PY'
from e2b import Sandbox
sandbox = Sandbox.create("<canonical-template-id>")
try:
    print(sandbox.commands.run("uname -a").stdout)
finally:
    sandbox.kill()
PY
```

继续阅读[节点配置](docs/node_zh.md)、[Sandbox API](docs/sandbox-lifecycle_zh.md)、[访问与凭据](docs/node-proxy_zh.md)或[集群入口](docs/cluster_zh.md#client-access)。

<a id="contract-ownership"></a>

## 契约归属

节点 roots、对象 RunDir/BaseDir 与 RunID/PathID 由[节点目录与身份](docs/node_zh.md#node-directories-and-identities)维护。公共 Create 身份、冲突与重试、状态变化、Reset/dead 保留及清理由 [Sandbox 生命周期](docs/sandbox-lifecycle_zh.md)维护；凭据绑定由[访问与代理](docs/node-proxy_zh.md#credential-model)维护。Build 目录/任务/发布/恢复与保留见 [Build](docs/node-build_zh.md)，Registry 的 Build projection 与重连收敛见[集群 Build](docs/cluster_zh.md#cluster-build)。Canonical artifact 独立于保留 Build row，始终是长期 launch authority。命名发布是逻辑提交，不承诺 final/parent fsync 的断电持久性；完整矩阵和失败规则由 Build 维护。

<a id="release-model"></a>

## 发布模型

本仓库独立发布 `vX.Y.Z`。已发布原生架构的组件包包含节点/集群二进制和部署文件;
聚合 platform 包按显式文档/测试选择携带用户指南与规范 E2E 运行输入，源码专用测试与未选入的维护材料保留在源码仓。

项目聚合版本为 `release-vX.Y.Z`,选择精确组件 Tag 并在真实 KVM 基础设施验证组合。
组件版本与聚合版本通过 selection 关联,不要求版本号相同。

完整组件分支、Preview/Stable/Latest、mutation group 与取消重试规则由项目
[发布文档](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md)唯一维护;
下载见[最新 Stable 聚合发布](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)。

<a id="project-boundaries"></a>

## 项目边界

- 系统设计、共享集成测试、Demo 与聚合发布属于 [kuasar-sandbox/kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox)。
- microVM 生命周期与 guest 控制属于 [sandboxer](https://github.com/kuasar-sandbox/sandboxer)。
- image/snapshot 数据基础设施属于 [accelerator](https://github.com/kuasar-sandbox/accelerator)。
- 高密度 microVM 网络属于 [connector](https://github.com/kuasar-sandbox/connector)。
- guest kernel 与 Runtime image 属于 [guest-runtime](https://github.com/kuasar-sandbox/guest-runtime)。

<a id="license"></a>

## License

原创内容采用 [Apache License 2.0](LICENSE)。生成或第三方材料须保留适用 attribution/license 声明。
见 [CONTRIBUTING.md](CONTRIBUTING.md) 与 [Organization 贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。

固定版本的 `rootless-containers/proto/go-proto` 模块归档缺少仓库根目录许可证。打包仅为该精确版本补入 [scripts/release-notices/rootless-containers-proto](scripts/release-notices/rootless-containers-proto/NOTICE) 中未修改的上游 Apache-2.0 声明，核验摘要并保留正常 source/checksum 清单。其他版本仍须提供各自的许可材料。
