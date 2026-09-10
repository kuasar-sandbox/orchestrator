[English](README.md) | [简体中文](README_zh.md)

# orchestrator

`orchestrator` 是 [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) 的 **E2B 兼容节点服务与多节点控制面**。

它提供北向 API、节点本地生命周期编排、数据面 Proxy 集成、租户凭据、资源准入,以及运营集群的 Registry/Router/Placer。
仓库独立演进和发布,同时参加项目级跨组件验证与聚合发布。

## 职责

### 独立节点

`node-ctl` 运行计算节点服务:

- **Conductor**:权威控制 API、Sandbox/Build 生命周期、本地路由、凭据管理与可选节点资源准入。
- **Proxy**:唯一沙箱数据入口,负责 Envd、floating-IP 服务、native exec、MMDS 与 traffic observation。
- **Node link**:可选客户端,把节点接入集群控制面。
- **Launch helpers**:systemd 管理的 Sandbox/Build runner。
- **Administration**:资源状态/drain、模板构建状态、配置检查、manifest-key 管理与 Sandbox export/import。

独立部署的 E2B 兼容 API 可供未改造的 E2B SDK/CLI 使用。

### 多节点集群

`cluster-ctl` 运行三个独立角色:

- **Registry**:复制的集群状态与节点通道枢纽。
- **Router**:集群级 E2B 兼容控制面与数据面统一入口,按稳定沙箱身份路由。
- **Placer**:sandbox-group provider/importer，以及创建、恢复和迁移的节点放置建议。生命周期提交与执行由 Registry 和 Node 负责。

控制面保留稳定的外部 Sandbox 身份,实际 node-local 实例可在恢复/迁移时改变。

## 安全与多租户

节点与集群区分平台凭据、内容保护密钥和有作用域的数据面 capability。API、普通数据转发与 native exec 使用各自用途的凭据,内容保护根密钥不暴露给 guest。

节点 reservation controller 拥有全节点准入、资源池记账、grant、水位与恢复,也是多租户边界的一部分。
每个 Sandbox 的 cgroup、balloon 与 VMM 执行由 sandboxer 维护;节点控制器不接管 Sandbox 内部资源闭环。

安全架构与部署信任边界见[项目系统概览](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/kuasar-sandbox_zh.md)和[部署指南](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment_zh.md)。漏洞通过 [Kuasar Sandbox 安全策略](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy)中的私密渠道报告。

## 主要命令

| 命令 | 用途 |
|---|---|
| `node-ctl conductor serve` | 启动权威节点控制 API |
| `node-ctl proxy serve` | 启动数据面 Proxy master/worker |
| `node-ctl run-sandbox` | 在受管 unit 中启动一个 Sandbox |
| `node-ctl run-builder` | 启动一次模板构建执行 |
| `node-ctl resource ...` | 检查或 drain 节点资源 reservation |
| `node-ctl builder status` | 检查 Build 执行状态 |
| `node-ctl export-sandbox` / `import-sandbox` | 跨节点移动 portable Sandbox 状态 |
| `cluster-ctl registry` | 启动 Registry |
| `cluster-ctl router` | 启动集群控制与数据统一入口 |
| `cluster-ctl placer` | 启动放置服务 |
| `e2b-key-ctl ...` | 生成和派生租户凭据 |

仓库还构建 `node-stub-ctl`,用于模拟 node-link 参与方而不启动 microVM 的 E2E helper。

## Profiles

- **e2b**:guest 运行 Envd,提供完整 E2B 兼容数据面。
- **bare**:guest 不要求 Envd,通过自身网络服务与原生平台集成访问。

<a id="组成"></a>
## 源码职责

| 路径 | 角色 |
|---|---|
| `cmd/node-ctl` | Conductor/Proxy serve、受管 run-sandbox/run-builder、resource status/list/drain、builder status、config、manifest-key、export/import 与 version |
| `cmd/cluster-ctl` | 独立 Registry/Router/Placer、config 与 version |
| `cmd/node-stub-ctl` | node-link E2E 参与方,独立 admin/API/data listener、restart/reset、Sandbox/Build 状态与故障注入;不启动 microVM |
| `cmd/e2b-key-ctl` | 无 DB/config 的 gen-key、derive-api-secret、gen-apikey、fingerprint、seal-pull-token |
| `config`, `app/conductor`, `app/proxy` | 公共 declarative Config 与静态 custom App;sealed-memfd/原地 exec bootstrap、启动期 Config/Runtime Hook |
| `internal/orch` | 节点生命周期、Build pool、本地路由权威、单元生成与重启对账 |
| `internal/nodectl` | reservation 准入、pool/水位/grant、inventory/StateSync 恢复与审计;不接管 Sandbox balloon/cgroup 闭环 |
| `internal/nodelink` | Conductor–Registry 注册、心跳、事件与命令,帧化 JSON/h2c |
| `internal/{registry,router,placer}` | 复制 shardkv namespace、Reserve/Build 状态与 node-link key cache;E2B 统一入口/active cache;provider/importer、WATCH_LIST、P2C 放置 |
| `internal/api` | E2B 控制 REST、X-API-KEY 鉴权、export/import 与 ExecAccessToken 签发 |
| `internal/proxy`, `internal/proxyshm`, `internal/proxyadmission`, `internal/routesync` | L7 转发、service-addressed CONNECT/native exec gate、固定共享 route view、独立 inflight arena、MMDS confidential heap、park/wake 与 generation cleanup;route/node-link 注册/bookmark/typed result |
| `internal/configsock` | Run/task/admin/plugin/API、exact-run bootstrap/prepare、manifest-key/MMDS/Builder 管理与受控 route;SO_PEERCRED/pidfile 鉴权 |
| `internal/{apikey,secretbox,keys,regcreds}` | APISecret/API-key MAC、AES-GCM 根凭据存储、Forward/Exec kat1 与 data token、ManifestKey 封装的 image-pull 凭据 |
| `internal/{config,clustercfg,sandboxcfg,store}` | 节点/集群配置、SANDBOX_CONFIG 渲染与节点 SQLite Sandbox/Build/凭据对状态 |
| `internal/{mmds,mmdsrpc,mmdssvc,metrics,launcher,vswitch,util}` | MMDSv2/exact route、worker-master 查询、HTTP-over-UDS service、Prometheus、systemd D-Bus、connector-ctl 适配与工具 |
| `deploy/` | 各角色 example YAML 与 node/Proxy/Registry/Router/Placer systemd unit |
| `examples/custom-conductor` | 可编译 xconductor,由 node-ctl conductor serve 进入 |
| `examples/custom-proxy` | 可编译 xproxy,由 node-ctl proxy serve 进入,master reexec worker |

<a id="构建"></a>
## 构建与测试

Go 源码构建需要 Go 1.24+，并将 `accelerator`、`connector`、`sandboxer` 放在本仓的兄弟目录；修改范围仅在本仓也不免除构建依赖。内部 `require` 使用各组件目标正式版本（Daily Preview 去掉预发布后缀），目标 Tag 可以尚不存在，因为实际构建由本地 `replace` 选择兄弟仓源码。`GOWORK=off` 不会禁用这些替换。验证必须记录实际源码 SHA，不能把版本标签当作已编译提交。运行真实沙箱还需 Runtime/Kernel 等运行工件，不能与 Go 编译前置混同。

Go 二进制使用 `CGO_ENABLED=0` 构建。

```bash
make build                      # node-ctl, cluster-ctl, node-stub-ctl, e2b-key-ctl
make build TARGET_ARCH=aarch64  # 交叉编译;也接受 amd64/arm64 别名
make test                       # 单元测试
make test-e2e                   # 组件 owner suite,需要项目组装的完整 BIN
```

真实沙箱要求 Linux、systemd、root 权限、KVM、sandboxer、connector 及 guest-runtime 产生的 Runtime/VMLinux 制品。
变更范围可以限定在本仓，构建仍需上述依赖闭包；跨仓契约变更必须关联 companion PR 并使用精确源码组合的集成测试验证，
见 [Organization 贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。

## 部署概览

独立节点通常先启动 Conductor 再启动 Proxy;集群增加独立 Registry、Router、Placer。
配置样例与 systemd unit 位于 `deploy/`。面向用户的发布件安装流程见项目
[快速开始](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_zh.md)。
生产部署应使用持久存储、生产 TLS、受保护凭据、明确网络策略及基于实际 workload 验证的容量设置。

<a id="快速开始"></a>
## 开发示例

先按项目快速开始准备二进制、Kernel/Runtime 制品、网络与受保护配置。下面每个常驻角色分别使用服务或独立终端,
不是在一个前台 shell 中依次执行。先启动 Conductor,再调用其 manifest-key 管理 socket。
示例使用默认 config-socket;自定义路径须传对应 CLI 选项。生成凭据与 shell 环境均按敏感数据保护。

```bash
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
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
python -c 'from e2b import Sandbox; s = Sandbox.create("<canonical-template-id>"); print(s.commands.run("uname -a").stdout)'
```

完整命令、配置、API 与集群接入规则见 [Node](docs/node_zh.md) 和 [Cluster](docs/cluster_zh.md)。

<a id="节点目录与身份"></a>
<a id="build-制品发布"></a>
## 契约归属

节点 roots、对象 RunDir/BaseDir、RunID/PathID 与 cleanup 顺序由 [Node 路径与生命周期](docs/node_zh.md) 维护。
Create 身份输入、stable/node-local 区分、凭据绑定、冲突与重试见 [Node §4.1.2](docs/node_zh.md#412-create-身份)。
完整 Build 目录/任务/发布/恢复/保留契约见 [Node Build](docs/node-build_zh.md);Registry 保留 Build projection 与重连收敛见
[Cluster](docs/cluster_zh.md)。canonical artifact 是独立于保留 Build row 的长期 launch authority。
命名发布是逻辑提交,不承诺 final/parent fsync 的断电持久性;完整矩阵和失败规则由 Build 维护。

## 发布模型

发行工作流在上传前把已完成归档的 SHA-256 记录为 build job output。发布者通过
`RELEASE_ARCHIVE_SHA256` 接收这一独立值,在任何 Tag/Release 写入前核对;不能用
下载后从 bundle 重新计算的值代替。即使重算 bundle 自身的校验和,全部载荷与材料
仍须匹配该次已完成构建。本地打包和独立验证不要求这个发布输入。该记录不证明
编译器来源,也不构成对不可信候选代码的隔离。
可信发布端根据已验证请求生成标准发行正文及来源/Preview 标记。下载的
`release-notes.md` 只是本地 bundle 辅助说明,不能决定公开发行正文或对账来源。

打包从选定的 Orchestrator、Accelerator、Connector、Sandboxer commit 建立全新
checkout,以 `GOWORK=off` 和只读 module 解析重新构建 Go 载荷。不复用被忽略的开发
文件或预制二进制,拒绝 `RELEASE_BIN_DIR`。构建命令使用私有 home/缓存,不继承云/
发布凭据;可保留无凭据的 HTTPS module/network proxy 路由。
构建保留 `GOSUMDB`(包括无凭据的 HTTPS checksum mirror)与 `GOTOOLCHAIN`,
不会把发行工作流的 `local` 策略静默改为自动下载工具链。未显式设置时,打包使用
`sum.golang.org` 和本地 Go 工具链。

发行打包记录全新构建上下文实际选定的 Go 编译器,在构建前后将其分发输入与匹配的
`golang.org/toolchain` 归档逐项比较;归档由配置的 checksum database 认证。这覆盖
编译器、标准库源码及该分发中的其他文件。完整 Go 安装中额外的非构建 `api`、
`doc`、`misc`、`test` 文件不在认证范围,也不作为发行许可来源;核对时处理标准的
`go.mod`/`_go.mod` 安装转换。Go 许可/NOTICE 正文来自已验证归档,包括编译器和
标准库内嵌依赖的材料,保留各自相对路径。独立验证还会
重新核对其字节、来源 URL 和 module h1。版本字符串或重算 bundle 校验和不能替代
来源核对。验证要求启用 checksum database 并取得匹配的归档/缓存;即使采用
`GOTOOLCHAIN=local`,也可能获取核验材料,但不切换构建编译器或静默启用工具链
自动选择。这些检查以可信构建主机为前提,不证明已失陷主机可信。

许可证收集拒绝不可读子目录和不完整遍历,不会仅发布可读的材料。官方组件包
不支持没有已认证 module 校验和的第三方本地 Go 替换,应选择带版本的 module
替换。现有 Kuasar 兄弟仓本地替换和普通源码开发不变。

四个 Go 可执行文件必须分别标识自身的 Orchestrator 命令 main package、module
及 Linux/amd64 目标。即使来自同一 commit,相互替换的可执行文件也会被拒绝。
部署文件从全新选定 checkout 复制,验证时与该 commit 的 Git blob 逐字节比较。
独立验证需要本地具备该精确 commit;可信发布端获取源码历史以供检视,不执行
候选部署文件。
验证还将完整项目和内部依赖许可树与选定的 Git blob 比较,包括嵌套 `LICENSES`。
本地验证沿用 `RELEASE_*_SOURCE_DIR` 和可选的 `RELEASE_*_SOURCE_SHA` 选择,
默认使用兄弟 checkout。发布端只按已验证请求获取三个依赖 Tag,使用的只读源码
Token 在验证前撤销;不会执行这些依赖 checkout。选定 Tag 必须解析为声明的来源
commit、URL 和完整性值。即使重算校验和,内容变化、缺失或额外许可及改动的依赖
来源仍会失败。

本仓库独立发布 `vX.Y.Z`。x86_64 组件包包含节点/集群二进制和部署文件;
文档与 E2E 从所选组件 tag 收集进项目 platform 包,不在组件包重复携带。
包内来源记录只有在本地 Git Tag 与所选源码 commit 一致时才保留项目或内部依赖的
发行版本。未打 Tag 的源码记录 `git:<commit>`,归档名称仍标识请求的发行目标。
验证器将所有项目 Go 二进制及项目来源 URL/摘要绑定到同一 commit。发布者传入其
预期 commit,在任何 Tag/Release 写入前拒绝不同源码产生的包。本地打包不要求创建
目标发行 Tag。
提供 `RELEASE_DEPENDENCIES` 时,验证要求 accelerator、connector、sandboxer
的发行版本与请求完全一致;绑定缺失、重复、包含其他组件或发生冲突时,发布前
即失败。这不会给普通的本地 replace 源码构建增加远端 Tag 前置要求。

项目聚合版本为 `release-vX.Y.Z`,选择精确组件 tag 并在真实 KVM 基础设施验证组合。
组件版本与聚合版本通过 selection 关联,不要求版本号相同。

完整组件分支、Preview/Stable/Latest、mutation group 与取消重试规则由项目
[发布文档](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release_zh.md) 唯一维护;
下载见 [最新 Stable 聚合发布](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest)。

## 文档

详细设计与参考文档提供完整英文/中文版本:

- [Node](docs/node_zh.md):节点架构、命令、配置、E2B API、生命周期、凭据与可靠性。
- [Node Build](docs/node-build_zh.md):完整 Build API、配置、执行、发布与恢复。
- [Runtime extensions](docs/extensions_zh.md):完整 Conductor/Proxy SDK 生命周期、对象源、Hook 与 wrapper。
- [Journal 身份](docs/node-journald_zh.md):独立 Sandbox/Build 输出身份、StableID、查询与验证。
- [Node Proxy](docs/node-proxy_zh.md):独立数据面、路由、鉴权、MMDS 与 native exec。
- [Node resource](docs/node-resource_zh.md):节点准入、reservation、水位、inventory 恢复与统计。
- [Cluster](docs/cluster_zh.md):Registry membership、复制状态、node link、Reserve 与集群 E2E。
- [Cluster Router](docs/cluster-router_zh.md):控制/数据统一入口、路由与 stable/node-local 身份转换。
- [Cluster Placer](docs/cluster-placer_zh.md):provider/importer、WATCH_LIST、source lease、selector patch 与放置建议。

使用每对规范开头的语言选择器。共享契约只有一个完整 owner,其他指南引用它,不维护平行 schema。

## 项目边界

- 系统设计、共享集成测试、Demo 与聚合发布属于 [kuasar-sandbox/kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox)。
- microVM 生命周期与 guest 控制属于 [sandboxer](https://github.com/kuasar-sandbox/sandboxer)。
- image/snapshot 数据基础设施属于 [accelerator](https://github.com/kuasar-sandbox/accelerator)。
- 高密度 microVM 网络属于 [connector](https://github.com/kuasar-sandbox/connector)。
- guest kernel 与 Runtime image 属于 [guest-runtime](https://github.com/kuasar-sandbox/guest-runtime)。

## License

原创内容采用 [Apache License 2.0](LICENSE)。生成或第三方材料须保留适用 attribution/license 声明。
见 [CONTRIBUTING.md](CONTRIBUTING.md) 与 [Organization 贡献指南](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md)。
