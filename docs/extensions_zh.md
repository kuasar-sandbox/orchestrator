[English](extensions.md) | [简体中文](extensions_zh.md)

# 运行期扩展

Create 身份在 Hook 前已选定，`SandboxOperation.SandboxID` 不可变；把 `kuasar-sandbox.identity` 重新加入已清理的可变 metadata 会被拒绝。完整契约见 [Node Create 身份](node_zh.md#412-create-身份)。

Conductor 和独立 Proxy 支持静态链接的运行期扩展，使部署能在进程内集成而不必长期维护 fork。扩展是编入 xconductor 或 xproxy 的受信代码，共享 core 的地址空间、UID、生命周期、文件系统及网络权限。API 组织 ownership 与并发，不构成安全沙箱。

每个 Conductor/Proxy master 进程持有一个扩展对象，每个 Proxy worker epoch 新建一个对象。框架不在运行期发现或加载插件，不维护扩展 registry，不设 priority、DI container 或 URL namespace。私有项目可在各角色单一对象后自行组合模块。

## 组件 bootstrap 与进程内材料

### 公共配置入口

公共 `config` 包提供 `LoadConductor(path string) (*Conductor, error)`、
`DecodeConductor(io.Reader) (*Conductor, error)`,以及返回 `*Proxy` 的
`LoadProxy`/`DecodeProxy`。Load 打开文件;Decode 接受最多 4 MiB 的单个严格 YAML document,
应用默认值并检查显式字段,不解析 provider、不检查 executable/material 文件、不要求所有最终运行声明。
`ValidateConductorFinal(*Conductor) error` 与 `ValidateProxyFinal(*Proxy) error`
执行最终声明校验,不重新默认化或调用 provider。

`(*Conductor).Clone() *Conductor`、`(*Proxy).Clone() *Proxy` 深拷贝声明并保持 nil;
两者都有 `ParkTimeoutDur() time.Duration`。`(*ResourceAllocatable).SetMemory(string)`
标记显式内存值;`InheritMemory()` 恢复 nil,表示继承默认值/裁剪,不同于显式填写 `256MiB`。
字段语义由 [Node](node_zh.md)、[Build](node-build_zh.md)、[Proxy](node-proxy_zh.md) 与
[资源策略](node-resource_zh.md) 维护;导出类型/helper 见 [config](../config/config.go)。

`app/conductor` 和 `app/proxy` 重新导出各自 `app/*/extension` 叶包的类型、常量和 sentinel,
调用方可选任一公共 import 入口,无需导入 internal 包。两个 App 包均提供
`New(Hooks) *App`、`(*App).Run() error`、`(*App).RunContext(context.Context) error`,
生命周期见下文。`Runtime.MarshalJSON() ([]byte, error)` 与
`(*Runtime).UnmarshalJSON([]byte) error` 始终拒绝序列化/反序列化。

### Conductor bootstrap

运维入口保持不变：`node-ctl conductor serve --config ...` 先做环境无关的严格解析、默认化和
declarative validation。`paths.conductor_executable` 为空时 node-ctl 显式 final validate 并在
runtime resolution 解析 key/TLS/credential 材料；非空时 node-ctl 打开 protected absolute
executable，依据该 FD 校验 runtime owner/mode/file identity，并通过 `/proc/self/fd` 执行同一文件，
不在校验后重新解析可替换的 pathname；sealed bootstrap 同时记录该已打开文件的 device/inode，
xconductor 只将它与 `/proc/self/exe` 比较，部署期间 pathname 被替换或删除不会改变已验证身份。
随后 node-ctl 用 `exec` 原地替换为 xconductor。
bootstrap 环境变量只含 FD 编号；配置正文/摘要在 memfd 中，FD 禁止 write/grow/shrink 并
最终 seal。xconductor 直接运行、bootstrap 缺失/截断/超限/version/digest/component 不匹配
均 fail closed。这个交接用于进程组织和防误用，不宣称抵抗同 UID 恶意进程。

custom main 只需要公共包；完整可编译版本见 `examples/custom-conductor`：

```go
app := conductor.New(conductor.Hooks{
    Configure: func(ctx context.Context, cfg *conductor.Config, rt *conductor.Runtime) error {
        // 替换/调整 declarative Config；绑定启动期 Runtime provider。
        rt.Extension = myExtension
        return nil
    },
})
if err := app.Run(); err != nil {
    log.Fatal(err)
}
```

`New` 无副作用，`Run` one-shot 并处理 SIGINT/SIGTERM；托管方可用 `RunContext`。App 不调用
`os.Exit`。执行顺序固定为：decode bootstrap → clone Config → `Configure` exactly once →
校验 `paths.conductor_executable` 未改变 → final declarative validation → 再 clone/freeze →
解析 Runtime 材料 → 启动共享 conductor core。Configure、启动期 material provider 或 final-validation 失败时尚未打开
durable store、listener、systemd launcher/unit 或 node-link。Hook 可整体替换 Config，但必须
恢复最初冻结的 executable；Hook 后不会重新应用默认值。

`App` 必须由 `conductor.New` 构造；零值或 nil receiver 的 `Run` / `RunContext` 在安装 signal
handler、读取 bootstrap FD 或启动 goroutine 之前返回明确错误。`node-ctl config conductor`
只做 declarative/bootstrap 与 executable metadata 诊断，不执行 custom App/provider，也不以
诊断进程 EUID 代替实际 service owner policy；custom 模式明确提示 runtime owner 与 final
validation 均延后到 component startup。

`Config` 只含可序列化声明；`Runtime` 是禁止 JSON 序列化的进程对象，开放 logger、
TLS material、AES-256 ordered key set、builder files-storage neutral credentials provider，
以及一个可信、静态编译的 `Extension`。
TLS provider 返回 DER certificate chain、`crypto.Signer` 与 root/client CA pool，不能返回任意
`*tls.Config`；最低 TLS 版本、ALPN、mTLS/client verification 仍由 core 固定。TLS 与 encryption
provider 在启动时解析。Object-store credential 还会按下文所述按需刷新;这是请求路径上的工作,
不是仅在后台执行的回调。provider 非 nil 即为权威来源,
任何错误都不回退文件、环境或静态 credential。V1 不支持配置或 TLS/encryption material 热更新。

Extension 的 `Start(ctx, Host)` 在 store/launcher/core 构造后、InstallUnits/reconcile/pool/
node-link/listener 之前恰好调用一次；失败会中止启动，`ctx` 取消通知 Extension 自有 goroutine
退出。`Host` 提供 Sandbox/Build `Get+Watch` 非秘密深拷贝视图。Watch 使用
`sync_begin → snapshot → sync_end → live` generation；慢 watcher 只使自己的 generation
失效并自动 full resync，不保证观察每个中间变化，也不是 durable audit。详细的对象源与回调契约在下文定义。

同一 Extension 可选实现 `SandboxHook`、`BuildHook` 与 `APIWrapper`；这些能力只在 Start
成功后检查一次并冻结。生命周期 Hook 在认证后、本次新准入操作的执行副作用前调用;
强制 ownership cleanup 可能先执行或绕过 Hook(例如 Resume 先清理旧 paused owner)。回调采用“锁内捕获 precondition → 锁外 Hook → 锁内权威重读/重验 → commit”。普通显式
Delete 可拒绝，TTL/rollback/reconcile/shutdown 等 mandatory cleanup 永远绕过 Hook。
Build Register Hook 位于 capacity transaction 前，Trigger Hook 位于最终 registry credential
解析和 registered→waiting CAS 前；waiting→building claim 无 Hook。cluster BuildRegister 的
exact replay 不重复执行可变 Hook。`APIWrapper` 可增加、改写或覆盖任意 core API route；同一
wrapped handler 同时服务公网 API 与 config-socket API fallback，config-socket internal route
不经过它。Hook 的 `ErrRejected` 映射固定 policy rejection，其他错误映射固定 503，不回显私有
细节。

xconductor 从 bootstrap 得到最初 node-ctl 的精确路径。生成的 runner/builder systemd unit
仍执行该 node-ctl；`sandbox-ctl`、`connector-ctl`、`flatten-ctl`、`manifest-ctl` 的相邻目录
解析也以 node-ctl 发行目录为准，不以 xconductor 目录为准。custom component 与 node-ctl
必须来自兼容版本。该 API 不开放 store/launcher/vswitch/orch/Router；API 只以
`http.Handler` next 形式交给可信 wrapper，不暴露 internal 类型。它不引入 Go plugin、运行时
发现,全局 registry,动态 middleware 注册或 DI container.conductor 始终只服务 control API.

Conductor 的精确公共材料接口见 [app/conductor](../app/conductor/conductor.go):

| 类型 | 字段或方法 |
|---|---|
| `Config` | `config.Conductor` 的 alias |
| `Runtime` | `Logger *slog.Logger`, `TLS TLSMaterialProvider`, `EncryptionKeys EncryptionKeyProvider`, `ObjectStoreCredentials ObjectStoreCredentialsProvider`, `Extension extension.Extension` |
| `TLSMaterial` | `CertificateChain [][]byte`, `PrivateKey crypto.Signer`, `RootCAs *x509.CertPool`, `ClientCAs *x509.CertPool` |
| `TLSMaterialProvider` | `TLSMaterial(context.Context, TLSPurpose) (TLSMaterial, error)` |
| `EncryptionKeyProvider` | `EncryptionKeys(context.Context) ([][]byte, error)` |
| `ObjectStoreCredentials` | `AccessKeyID`, `SecretAccessKey`, `SessionToken` 为 string;另有 `Expires time.Time`, `CanExpire bool` |
| `ObjectStoreCredentialsProvider` | `RetrieveObjectStoreCredentials(context.Context) (ObjectStoreCredentials, error)` |

`TLSMaterialProviderFunc`、`EncryptionKeyProviderFunc`、`ObjectStoreCredentialsProviderFunc`
把相同参数/返回值签名的函数适配成对应 provider。先解析 `TLSPurposeAPI = "api"`;
只有 `Cluster.NodeLink.Endpoint` 非空才解析 `TLSPurposeNodeLinkClient = "node-link-client"`。
证书链为 leaf-first DER,signer 须匹配,chain 与 signer 必须成对提供;空材料关闭该用途 TLS。
API `ClientCAs` 要求 server certificate 并启用必须通过验证的客户端证书;
node-link 使用 `RootCAs`。Core 固定最低 TLS 1.2 与 ALPN(API 为 `h2`/`http/1.1`,node-link 为 `h2`)。

加密 provider 必须返回非空、有序的原始 32-byte AES key 集。索引零用于加密新记录,
key 集支持解密此前记录;进程加密 key 集与租户凭据分发表更新是不同概念。
自定义 object-store provider 要求已配置 `Builder.FilesStorage`、非空 access/secret key;
`CanExpire` 为 true 时 `Expires` 必须非零且在未来。Core 启动前预取一次。
此后,Build 的 S3 presign 或 HEAD 操作需要更新凭据时,AWS SDK credentials cache 会调用该 provider;
刷新可以阻塞该操作或使其失败。实现须考虑请求延迟,不能假定回调只在启动时执行。
错误不得回退 YAML 或环境默认凭据链。参见拥有该行为的 [filestore adapter](../internal/filestore/filestore.go)。

### Proxy bootstrap

运维入口仍只有 `node-ctl proxy serve --config ...`。公共 Load/Decode 只做环境无关的 strict
decode、defaults 与 provided-value validation；内置路径由 node-ctl 显式 final validate，custom
路径则延后到 master `Configure` 后。`paths.proxy_executable` 非空时，node-ctl 校验 protected
absolute executable 的 runtime owner/mode/identity，再把公共 `config.Proxy` 的
bootstrap snapshot 写入有大小上限且禁止 write/grow/shrink 的 sealed memfd；环境变量只传
FD 编号，配置正文与 TLS 材料不进入 argv 或环境。node-ctl 从已验证的同一打开文件原地 exec
xproxy，失败不回退内置实现。xproxy 直接运行、bootstrap 缺失/损坏或 component/file identity
不匹配均 fail closed；这是进程组织和防误用，不是抵抗同 UID 恶意进程的密码学认证。

可编译示例见 [examples/custom-proxy（英文）](../examples/custom-proxy/README.md)：

```go
app := proxy.New(proxy.Hooks{
    Configure: func(ctx context.Context, cfg *proxy.Config) error {
        // master-only declarative override
        return nil
    },
    BindRuntime: func(ctx context.Context, process proxy.Process, rt *proxy.Runtime) error {
        // bind process-local logger / TLS provider
        if process.Role == proxy.RoleMaster {
            rt.MasterExtension = newMasterExtension()
        } else if process.Role == proxy.RoleWorker {
            // Construct a fresh instance for this worker epoch.
            rt.WorkerExtension = newWorkerExtension()
        }
        return nil
    },
})
if err := app.Run(); err != nil {
    log.Fatal(err)
}
```

`New` 无副作用；`Run` one-shot、处理 SIGINT/SIGTERM且不调用 `os.Exit`，上层托管可用
`RunContext`。零值/nil App 在 signal、component bootstrap 或 worker bootstrap 处理前返回
必须由 `proxy.New` 构造的明确错误。master 固定执行 bootstrap decode → clone → `Configure` exactly once → 校验
`paths.proxy_executable` 未改变 → final validation → 再 deep-clone/canonical serialize/digest
冻结 EffectiveConfig → `BindRuntime(master)` → 启动 core。Configure Hook、provider 或 final
validation 失败时尚未创建 SHM、listener、routesync session 或 worker。若设置
`MasterExtension`，core 创建共享路由表与进程内 traffic aggregate 后调用其
`Start(ctx, MasterHost)` 恰好一次；Start 失败时尚未绑定 listener、启动 routesync 或 worker，
已建 SHM 会清理。Start 成功后同一对象的可选 `ManagementWrapper` 能包装
`stats_socket` handler；能力只检查一次并冻结，返回 nil handler 会中止启动。

worker 固定由 master 的 `/proc/self/exe` reexec：内置 master 得到 node-ctl worker，custom master
得到 xproxy worker，配置中的 executable 不参与选择。master 通过另一 sealed memfd 传递 frozen
EffectiveConfig、digest、worker id/epoch、FD protocol/mapping 与当前 executable identity；listener、
wake/notify、stats、MMDS RPC、独立 admission arena 等作为继承 FD 传入,并同时校验 worker
index/epoch、arena version/size/layout。worker 严格验证后调用
`BindRuntime(worker)`。每个 worker epoch 都得到新的 `Runtime`，不得复用上一 epoch 的
`WorkerExtension` 实例。worker 完成 stats hello/ready、构造 Host 并等待初始 route-table sync，
随后调用 `WorkerExtension.Start` 恰好一次、冻结同一对象的可选 `IngressWrapper`，成功后才
Serve.Start 错误或 nil wrapper 不开放 data listener,由既有 master supervisor 重启
worker。worker 从不读取 `proxy.yaml`，也不调用 `Configure`；因此配置文件被替换或删除不影响
replacement worker。

Worker 是专用、单次运行的子进程：内置与定制入口在 `Run` 返回后必须退出 worker 进程。成功建立的 route SHM 与 admission 映射存活到进程退出；`PreparedWorker.Close` 只关闭继承描述符，不卸载仍可能被 handler/extension/traffic GC 引用的映射。Supervisor 仍须确认进程退出后才清理共享计数。普通 HTTP 与 HTTP/2 CONNECT 的取消会关闭 backend，与有效流量限制无关；HTTP/1 hijacked CONNECT 保留半关闭语义。详见 [worker 生命周期与转发取消](node-proxy_zh.md#91-worker-生命周期与传输取消)。

公共 `Config` 仅含可序列化声明。`Runtime` 是拒绝 JSON 编解码的进程对象，开放 logger、
启动期 TLS material provider，以及按 process role 使用的可信、静态编译
`MasterExtension`/`WorkerExtension`。provider 返回
certificate chain、`crypto.Signer` 与可选 client CA pool，不能替换任意 `*tls.Config`。
provider 非 nil 即为权威来源，错误不回退 cert/key 文件；最低 TLS version、HTTP/2 ALPN 与
client-auth 策略仍由 core 固定。V1 不支持配置、材料或 Extension 热更新，custom component
与 node-ctl 必须来自兼容版本。

`MasterHost.Routes()` 提供 applied route 的 `Get`、generation-based `Watch` 与
`SyncState(initializing|syncing|synced|stale)`。完整 generation 是
`sync_begin → snapshot upsert* → sync_end`，之后按发布顺序发送 live upsert/delete；断线发送
`sync_lost`。慢 watcher 只使自身 generation 失效并自动 full resync；允许重复、不保证观察到
每个中间变化，也不是 durable audit。View 复制身份、profile/template/state/RunID、当前 endpoint、
artifact location、fingerprint 与 route revision，不复制原始 secret/token，也不增加 route metadata
或 SHM schema。observer 只在 core SHM apply 成功后非阻塞发布，绝不影响 routesync、barrier ACK、
Wake 或 worker notification。

`MasterHost.Traffic().Get` 以当前 route identity 直接读取 master 的进程内 worker aggregate，
不经 stats UDS 回环，返回 map/pointer 副本；V1 没有 Traffic Watch。断线期间保留 route 仍可查询，
要求新鲜度的调用者同时检查 Route `SyncState`。Management wrapper 可添加、覆盖或透传任意本地
route；框架不保留 namespace、不做 route conflict 检测，也不规定认证。

`WorkerHost.Process()` 返回当前 worker id/epoch；`GetRoute(sid)` 只做当前 SHM 点查并返回
独立的非秘密 `RouteView` 副本，不暴露 raw record/Router/可变指针，也不提供 worker Route
Watch.`IngressWrapper` 在 canonical Host/Header 与 CONNECT parser 之前接收 raw request;
wrapped handler 只服务节点 `data_listen`,MMDS listener 不使用它.Extension
可自行定义 Header/path/auth、覆盖或本地响应；未匹配请求调用 `next` 即保留 core token 与
native exec 语义。

私有认证完成后，`WorkerHost.ForwardAuthorized` 可复用 core 的
`LookupRoute → TryBeginParking → ActivateRoute/Wake/binding revalidation → optional Revalidate →
dial → ordinary HTTP/CONNECT → traffic close`。该 helper 拥有 `ResponseWriter`，返回后调用方
不得再写错误；它不校验 Kuasar `X-Access-Token`。`Revalidate` 在 activation 后、dial 前执行；
普通 HTTP 的 `Rewrite` 只收到 guest-facing clone，失败时不写任何 guest request bytes；CONNECT
不调用 Rewrite。generic helper 拒绝 native exec，后者继续经 `next` 走 KAT + per-command CEL。
本接口不增加 WebSocket transport；WebSocket 仍由 [#269](https://github.com/kuasar-sandbox/orchestrator/issues/269) 独立跟踪。

该 API 只对应独立 Proxy,不为 conductor 增加数据面 factory;也不开放原始 Router,
SHM、listener、routesync、stats、dial target 或 credential records。除同一 master Extension 的
可选 management wrapper 和同一 worker Extension 的可选 ingress wrapper 外，不引入 Go plugin、
运行时发现、多 Extension registry、通用 lifecycle hook、secret resolver 或 DI container。
`node-ctl config proxy` 只做 declarative/bootstrap 与
executable metadata 诊断，绝不执行 xproxy、调用 Runtime provider，或用诊断命令 EUID 代替实际
启动的 runtime owner 校验。详细 Extension 契约在下文定义。

Proxy 的 [公共材料类型](../app/proxy/proxy.go) 与 Conductor 有明确区别:

| 类型 | 字段或方法 |
|---|---|
| `Config` | `config.Proxy` 的 alias |
| `Runtime` | `Logger *slog.Logger`, `TLS TLSMaterialProvider`, `MasterExtension extension.MasterExtension`, `WorkerExtension extension.WorkerExtension`;忽略另一角色的 extension 字段 |
| `TLSMaterial` | `CertificateChain [][]byte`, `PrivateKey crypto.Signer`, `ClientCAs *x509.CertPool`;没有 `RootCAs` |
| `TLSMaterialProvider` | `TLSMaterial(context.Context) (TLSMaterial, error)`;没有 purpose 参数 |
| `TLSMaterialProviderFunc` | 与该方法签名完全一致的函数适配器 |
| `Process` | `Role Role`, `WorkerID string`, `WorkerEpoch uint64`;master 的 worker 字段为零值 |

`RoleMaster = "master"`,`RoleWorker = "worker"`。证书/signer 匹配与成对提供、空材料关闭 TLS、
配置 `ClientCAs` 后要求通过验证的客户端证书,遵循相同 server-material 约束。
Core 固定最低 TLS 1.2 与 `h2`/`http/1.1` ALPN。
`Hooks.Configure` 签名为 `(context.Context, *Config) error`,
`Hooks.BindRuntime` 为 `(context.Context, Process, *Runtime) error`,如上例。
映射与传输清理统一见 [Proxy worker 生命周期](node-proxy_zh.md#91-worker-生命周期与传输取消);
下述 source、Hook 与 wrapper 接口不构成另一套数据面实现。

## Conductor 对象源

单点读取与 Watch 的精确签名为:

```go
type SandboxSource interface {
    Get(context.Context, string) (SandboxView, bool, error)
    Watch(context.Context, func(SandboxEvent) error) error
}
type BuildSource interface {
    Get(context.Context, string) (BuildView, bool, error)
    Watch(context.Context, func(BuildEvent) error) error
}
```

对象不存在时返回 `false, nil`;nil Watch callback 非法。Event 含 `Generation uint64`、
typed `Kind`、`SandboxID`/`BuildID string` 与 `View *SandboxView`/`*BuildView`;
`BuildEvent` 另含 `Reason string`。Sandbox Delete 可缺少 View,Build Upsert/Remove 均携带 View。
完整非秘密字段定义见 [SandboxView 与枚举](../app/conductor/extension/sandbox.go) 和
[BuildView、BuildResources、BuildOptions](../app/conductor/extension/build.go)。BuildView 包含
不可变 resources、requested Builder target、source/steps/commands、注册/排队/执行时间、
当前 RunID/claim/enforcement/phase/network、cluster group 和凭据指纹。两类 source 都不暴露
原始凭据、MMDS secret value 或可变 core 对象;下述快照/收敛规则适用于每个字段。

在启动期 `Configure` hook 中设置 `conductor.Runtime.Extension`。该对象实现稳定的基本契约：

```go
type Extension interface {
    Start(context.Context, Host) error
}

type Host interface {
    Sandboxes() SandboxSource
    Builds() BuildSource
}
```

`Start` 在 store、launcher 和 conductor core 已创建后精确调用一次，但先于安装 systemd unit、持久状态对账、启动 pool/node-link 以及暴露外部 listener。返回错误会终止启动，并关闭已创建的资源。传入 context 的取消通知扩展自行启动的 goroutine 退出；`Start` 不应等待这些长期运行的 goroutine。

`Start` 成功后，Conductor 对同一对象仅检查一次可选 `SandboxHook`、`BuildHook` 和 `APIWrapper` 能力，结果在进程生命期内冻结。这些接口不是注册机制，不能在运行中安装、替换或重排。

`SandboxSource`、`BuildSource` 各提供单点 `Get` 和回调式 `Watch`。返回视图是独立深拷贝，包含丰富的持久和运行状态以及不可逆凭据指纹，省略原始凭据、access token、沙箱环境变量值、MMDS secret 值和 Build cleanup/runtime-prepare 内部数据。这是在日常事件中减少敏感数据及误日志，不限制受信进程内代码原本能访问的内容。

`SandboxView` 直接暴露 typed lifecycle 投影：`ResumeSourceKind`、`ResumeSourceRef`、`ArtifactLocation`、`AutoPauseMemory`、`LaunchMode`。`ArtifactLocation` 与 E/S 类型正交；running 行也可能保留 source 以持有节点本地工件。`LaunchMode` 仅在 starting 时非空，它是已持久化、已解析的启动决定，不是请求该行为的 trigger。内部 `deleting` 表示持久 cleanup ownership，绝不是路由或激活状态；core finalizer 完成前，它保留 exact runner、RunDir、BaseDir。网络 tuple 在 allocation fence 下 connector Detach 和持久清除均成功前保持原值；清除操作原子移除全部四个网络字段。

`SandboxSource.Watch` 覆盖全部持久 Sandbox 行。`BuildSource.Watch` 覆盖当前 registered、waiting、building、ready 集合；live 转入 error 时发出包含最终视图与原因的 `BuildRemove`。这个 removal 不是持久事件：重同步后，失败 Build 只是缺席于当前集合，`BuildSource.Get` 仍能读取其持久 error 行。

### Watch 代际

Watch 是最终收敛的状态流，不是持久事件日志：

1. `sync_begin` 开启一代。
2. 快照 `upsert` 按稳定顺序枚举持久行。
3. `sync_end` 标记这一代完整快照。
4. 后续 live 对象事件按发布顺序交付。

Core 先订阅，再查询 SQLite 快照，避免修改永久落入 snapshot/live 边界空隙。每个订阅者有自己的有界队列；滞后、溢出或 source generation 失效只会放弃该订阅者的一代，自动重新发起 `sync_begin` 与完整快照。没有 `sync_end` 的一代不完整，绝不能替换消费者的权威状态。

消费者使用最新完整 generation 替换本地状态，再应用其 live 事件。允许重复，不保证看到所有中间变化，但后续 full resync 会收敛到持久状态。回调在调用 `Watch` 的 goroutine 中串行运行，不在 core commit 线程执行。回调返回错误会停止 Watch 并返回该错误；context 取消返回 `ctx.Err()`。

不能用 Watch 实施审计、计费或 exactly-once 交付；这些需求需要另行设计持久 outbox。

显式删除一旦成功持久进入 deleting，就从 node cache 移除 Sandbox 并发布 route delete。该事件仅撤销投影，不证明 unit、网络、目录或数据库行已经 finalize。Conductor 对象源只有在 exact 本地 cleanup 与 hard delete 完成后才发出终态 Sandbox removal。因此启动时快照可能包含等待 cleanup 的 deleting 视图，其网络 tuple 可能仍待清理，也可能已经清除。扩展只能将其视作诊断状态，不得尝试恢复、路由或独立清理它。

对路由消费者而言，live delete 和重新连接后的完整快照中缺席，都会撤销旧投影。持久本地清理独立于这两条路由收敛路径，从保留的行继续执行。

## Conductor 生命周期 Hook

扩展可在公共的、按操作区分的副本上实现以下一个或两个准入回调：

```go
type SandboxHook interface {
    PrepareSandbox(context.Context, *SandboxOperation) error
}

type BuildHook interface {
    PrepareBuild(context.Context, *BuildOperation) error
}
```

operation 是独立可变副本，精确有一个 request 字段非 nil。`ID`、`Kind`、`Origin`、`SandboxID` 或 `BuildID` 和已分配的协议身份仍由 core 持有。Conductor 拒绝对 envelope 的修改，再对每个可变字段重新归一化和验证。存在 `Current` 时，它与对象源返回的非秘密深拷贝投影相同。

公共 [Sandbox request 类型与枚举](../app/conductor/extension/sandbox_hook.go) 定义完整输入:

| Request | 字段与约束 |
|---|---|
| `SandboxCreateRequest` | `TemplateID string`、不可变 `Profile Profile`、`TimeoutSeconds int`、`Metadata`/`Env map[string]string`、`Secure bool`、`AutoPauseMemory *bool`、`MMDS *string`;MMDS 保留顶层输入 presence。请求级 MMDS/metadata 可含 secret 或 credential,不得当作非秘密对象源投影记录日志 |
| `SandboxPauseRequest` | 不可变 `CaptureKind CaptureKind`(`snapshot`/`sandbox`);`CheckpointMergeRef`/`CheckpointDropCaches *bool`,nil 继承 Sandbox/node policy |
| `SandboxResumeRequest` | `RequestedDeadlineUnix *int64`,nil 使用 core resume-deadline policy;不可变 `Mode ResumeMode`(`auto`/`memory`/`cold`) 与 `Trigger ResumeTrigger`(`connect`/`wake`/`route`/`exec`/`exec-session`) |
| `SandboxDeleteRequest` | `Reason string` 仅诊断,修改不改变 cleanup 行为 |

直连 Create 可在不可变 profile 内更改模板;canonical cluster Create 必须保留模板及 cluster metadata,
并让顶层 `MMDS` 保持 nil,不因此取得直连 Create 的覆盖权限。

Sandbox 操作准入位置如下：

- `create`：调用方鉴权、严格解析、初步模板解析、metadata 归一化与 ID 分配之后；launch claim、持久 starting、目录、network attach、路由发布和 runner 分配之前。Hook 可改模板引用、timeout、metadata、environment、secure 及支持的请求 MMDS 输入，也可改 `AutoPauseMemory`；最终模板需重新解析，并保留 core-owned profile。
- `pause`：捕获 ownership、running incarnation 和 checkpoint 前置条件之后，快照或持久状态修改之前。Hook 可改本次动作的 merge-ref/drop-cache 策略；`CaptureKind` 可观察但由 core 持有，不能修改。
- `resume`：仅用于 API Connect、Proxy Wake、native exec 和 canonical cluster command 共用的真实 paused → starting 准入。running/starting 的幂等 join 不调用 Hook。Hook 可改请求 deadline；`Mode` 和 `Trigger` 可观察但由 core 持有。
- `delete`：仅普通显式 API Delete 或 canonical cluster Delete，可拒绝该请求。失败 create/resume 的回滚、对账、shutdown、恢复和其他强制 cleanup 不调用 Hook，不能被扩展可用性阻止。

Build `register` 在解析调用方身份与 ID 后、registration capacity 事务前运行。可以改 `Names`、`Aliases`、`Profile`、`Resources`、`Metadata`(含 routes-only MMDS 声明)、`Env`、`Secure` 和受支持的 `Builder` options;`BuildID`、`TemplateID` 保持 core-owned。不存在可变请求 `kind`:终态 `BuildKind`(`img`/`sbx`/`snp`) 只在成功时推导,此前为空。原始 MMDS secret、registry credentials 和 pull token 不复制到 operation。Core 在 Hook 后把保留的初始 MMDS 值重新绑定到最终 routes 声明，再解析 registry 凭据。Canonical cluster BuildRegister 仅在新 ownership 时调用 Hook。对已持久 BuildID 的精确重放验证原始 tenant-keyed 请求身份和凭据指纹，不重复调用可变 Hook，因此后续策略变化不会破坏丢失 ACK 后的重放。

`Register.Resources` 是注册执行向量的唯一权威(`CPU` 为 milli-CPU,`Memory`/`Storage` 为 bytes)。
Hook 必须让 `Builder.Resources` 保持 nil;重新引入重复权威会被拒绝。
可变 Builder 控制为 `Target *BuildTarget {Kind BuildTargetKind; Memory bool}`、
`Referer *BuildRefererOptions {Enabled, Writeback *bool}`、`Registry *BuildRegistryOptions`,
后者的 `TLS *BuildRegistryTLSOptions` 含 `CABundlePEM string` 与 `InsecureSkipVerify bool`。
Target kind 是 `image` 或 `sandbox`;nil 按最终 effective start/ready 自动解析,
不是按任意配置是否存在推导,见 [Build target 契约](node-build_zh.md#31-请求级-builder-输入)。

公共 [Build request 类型](../app/conductor/extension/build_hook.go) 中,`BuildTriggerRequest` 包含
`FromImage`、`FromTemplate`、`StartCommand`、`ReadyCommand` string,`Steps []BuildStep` 与
`ResourceAssertion BuildResourcePatch`。每个 [BuildStep](../app/conductor/extension/build.go) 包含
`Type string`、`Args []string`、`FilesHash string`、`Force bool`。
`BuildResourcePatch` 的 `CPU`、`Memory`、`Storage` 均为 `*int64`,单位与注册 execution vector 相同;
nil leaf 不作断言,显式 leaf 必须等于不可变注册资源。

Build `trigger` 在 ownership/state 检查与初步纯解析后、source 解析、registry credential 解析及 registered → waiting CAS 前运行。最终 `fromImage`/`fromTemplate`、steps、commands 和 resource assertion 均重新验证，凭据按最终 source 解析。waiting → building 的 execution claim 明确没有 Hook。

回调不在持有 Sandbox lifecycle lock、SQLite transaction 或 CAS callback 内运行。Sandbox pause/resume/delete 和 Build trigger 先捕获前置状态、解锁、执行 Hook，再在 mutation fence 下权威重读后提交。陈旧结果被拒绝，绝不应用到新的 run、snapshot、ownership 或 Build state。Context deadline 只是协作取消，Core 不在 detached goroutine 中丢弃一个进程内 Go 回调。

匹配 `extension.ErrRejected` 的错误产生固定策略拒绝：直连 API 为 403，集群为相应 rejected ACK。其他错误只在日志保留私有细节，对外固定为 temporary-unavailable 503。修改后候选的 core validation 仍保持普通 400/409 契约。

## Conductor API wrapper

同一对象还可实现：

```go
type APIWrapper interface {
    WrapAPI(http.Handler) http.Handler
}
```

`WrapAPI` 在 Start 后调用一次，返回 nil 会终止启动。Wrapper 可增加私有路由、在调用 next 前改写请求、覆盖 core 路由、反代其他服务或直接返回本地响应。框架不恢复 panic、不保留前缀、不检测路由冲突、不规定私有鉴权，也不要求必须调用 next。

公共 `api.<domain>` listener 与 conductor config socket 的 API fallback 使用同一个被包装的 handler。Config socket 的 task、run、admin、plugin、secret-management 路由不经过 wrapper。Conductor 没有数据面 handler。扩展为 nil 或没有 APIWrapper 时，直接使用现有 core handler 对象。

## Proxy master 扩展

独立 Proxy 可在 master 的 `BindRuntime` 调用中设置 `proxy.Runtime.MasterExtension`，使用不依赖 `internal/*` 的公共叶子契约：

```go
type MasterExtension interface {
    Start(context.Context, MasterHost) error
}

type MasterHost interface {
    Routes() RouteSource
    Traffic() TrafficSource
}
```

Master 进程持有一个对象。Master 创建共享路由表、独立跨 worker admission arena 和进程内 traffic aggregate，调用 Start 精确一次，然后才绑定 listener、启动 routesync 与 worker。Start 错误终止启动并移除已创建的 SHM/socket。Master 退出时取消传入 context；与 Conductor 相同，Start 应启动而不是等待长期扩展 goroutine。

Start 成功后，同一对象仅检查一次可选 management 能力：

```go
type ManagementWrapper interface {
    WrapManagement(http.Handler) http.Handler
}
```

`WrapManagement` 包装 stats_socket 上的既有 handler，可增加/覆盖任意本地路径、反代或调用内置 traffic handler。没有保留 namespace 或冲突 registry。返回 nil 会终止启动，不恢复 panic。扩展为 nil 或不提供该接口时，直接使用原 handler。

### Master 路由源

```go
type RouteSource interface {
    Get(context.Context, string) (RouteView, bool, error)
    Watch(context.Context, func(RouteEvent) error) error
    SyncState() RouteSyncState
}
```

对象缺席时单点读取返回 `false, nil`;nil Watch callback 被拒绝。
`RouteEvent` 含 `Generation uint64`、`Kind RouteEventKind`、`SandboxID string` 与
`View *RouteView`,Upsert/Delete 均有 View。[RouteView 与枚举](../app/proxy/extension/route.go)
定义本地/稳定身份、profile/template/lifecycle/RunID、Envd/CI socket、FloatingIP、artifact
location、完整凭据指纹与 `Revision uint64`。

`RouteSource.Get` 返回 master 成功应用到 core table 的 route 的独立非秘密投影。包括 Sandbox 与授权身份、profile、template、state、RunID、当前本地 endpoint、E/S artifact location、凭据指纹和 core route revision；省略原始 secret、access token、MMDS 值和内部 SHM record。这用于减少日常事件数据与误日志，不是受信同进程代码的权限边界，也不为扩展增加 metadata 字段或新 SHM schema。Proxy 投影特意只暴露 `ArtifactLocation`，不含 ResumeSource kind 或 launch-mode gate，不能据此判定 paused 路由是否可以 Wake。

`SyncState` 返回 initializing、syncing、synced 或 stale。Routesync 断开将 source 改为 stale 并产生 sync_lost；服务中的 SHM 保留既有重连行为。后续 sync 在 bookmark 时替换投影，使 source 回到 synced。

`RouteSource.Watch` 遵循上面的 generation 规则。完整的一代为 sync_begin、排序后的 applied-route 快照、sync_end，之后是有序 live upsert/delete。Source reset 或有界队列滞后只放弃该 watcher 未完成的一代，source synced 后开始新的全量快照。允许重复，不保证观察所有中间态，也不是持久审计流。

Observing sink 仅在 core SHM 操作成功后更新扩展投影。发布有界且非阻塞，回调仅在调用 Watch 的 goroutine 运行。扩展滞后、回调错误或 resync 不能终止 routesync、阻塞 SHM 修改和 worker notification、推迟 RouteBarrier ACK，或影响 Wake/activation。

### Master traffic 源

`TrafficSource.Get(context.Context, string) (TrafficView, error)` 没有 found boolean 或 Watch。
`ErrTrafficUnavailable` 表示 route identity 或完整 worker contribution 不可用;
`ErrTrafficConflict` 表示当前 route state 不能生成请求的观测。
[TrafficView](../app/proxy/extension/traffic.go) 含 `SandboxID`、`RunID`、`Profile`、`State`、
有效 `MaxInflight config.MaxInflight`、`Inflight TrafficInflight`、`IdleSince *time.Time` 和
`Services map[string]ServiceTrafficView`。Inflight 含 `Parking`/`Egress uint64`,每 service
另含 `IdleSince *time.Time`。`config.MaxInflight` 含 `Total`、`Forward`、`E2BEnvd`、
`E2BCodeInterpreter`、`Exec uint32`(JSON 为 `total`、`forward`、`e2b:envd`、
`e2b:code-interpreter`、`exec`);零表示无限,`Unlimited() bool` 检查整个向量。

`TrafficSource.Get` 在进程内直接组合当前 applied route 身份、有效的 per-Sandbox maxInflight policy 和 MasterStats；它不查询 stats UDS，也不从 conductor Sandbox 行推导限额。`TrafficView.MaxInflight` 使用 canonical `config.MaxInflight` 结构；返回的 map 与时间指针是独立副本。V1 明确没有 traffic Watch。重连期间保留的 route 仍可查询，需要新鲜状态的调用方还必须检查 `Routes().SyncState()`。

## Proxy worker 扩展

每次 worker re-exec 都以新的 Runtime 调用 BindRuntime，定制 Proxy 可在此为该 epoch 构造一个新的扩展对象：

```go
type WorkerExtension interface {
    Start(context.Context, WorkerHost) error
}

type WorkerHost interface {
    Process() Process
    GetRoute(string) (RouteView, bool)
    ForwardAuthorized(http.ResponseWriter, *http.Request, ForwardRequest)
}
```

Worker 重建 SHM table、listener、update/wake channel 和进程内客户端，构造 Host，等待所需初始 routesync，然后精确调用一次 Start。Start 错误使 data ingress listener 不进入服务，由既有 master supervisor 重启该 worker epoch。Epoch 结束时取消传入 context。

Start 成功后，同一对象仅检查一次可选 raw ingress 能力：

```go
type IngressWrapper interface {
    WrapIngress(http.Handler) http.Handler
}
```

最终 handler 在该 epoch 内冻结，原样用于节点的 data_listen。它先于 ParseSandbox、ParseConnect 和 core token 准入收到请求，因此可定义私有 Header/path/authentication 契约、改写 canonical E2b-Sandbox-* 输入再调用 next、直接响应、访问其他上游或有意覆盖标准请求。需要内置行为时，未匹配请求应调用 next。MMDS 不使用该 wrapper。返回 nil 终止 worker epoch，不恢复 panic；worker extension 为 nil 或不含 IngressWrapper 时直接使用原 core handler。

### Worker 路由单点查询

`GetRoute` 从 worker 当前 SHM point lookup 复制独立公共 RouteView，暴露与 master 投影相同的既有非秘密字段和 revision，而非原始 SHM record、router、token 或可变指针。Worker V1 不提供 route Watch；动态观察仅在 master。这个小接口面是 API 约束，不是受信同进程代码的安全边界。

### 已授权 core 转发

`ConnectTarget {Service ConnectService; Port int}` 选择后端。Service 常量为
`ConnectServiceLegacy = ""`、`ConnectServiceForward = "forward"`、
`ConnectServiceE2BEnvd = "e2b:envd"`、`ConnectServiceE2BInterpreter = "e2b:code-interpreter"`、
`ConnectServiceExec = "exec"`。Legacy/forward 要求 port 为 `1..65535`;
`ForwardAuthorized` 拒绝 Exec,后者必须走内置 KAT/逐命令 CEL 路径。
见 [公共 target 类型](../app/proxy/extension/worker.go)。

`ForwardAuthorized` 供已完成私有鉴权、但需要复用 core 路由和传输流程的 wrapper 使用：

```go
type ForwardRequest struct {
    SandboxID string
    Target    ConnectTarget
    Revalidate func(context.Context) error
    Rewrite    func(*http.Request) error
}
```

它拥有 http.ResponseWriter，并完成所有成功或失败路径的响应；调用方必须返回，不能在它之后再写一个错误。它明确不验证 Kuasar 的 X-Access-Token；需要该 token 契约的 wrapper 应先 canonicalize 请求，再调用 next。

固定顺序为：无副作用 route lookup 和 target validation、共享 per-Sandbox admission 与 traffic parking、ActivateRoute/Wake 和 admission/route binding 重验、可选 Revalidate、backend dial、普通 HTTP/CONNECT 传输、traffic close/idle 记账。Revalidate 在 activation 之后、dial 之前运行，使私有 registration、policy generation 或 lease revision 能对恢复后的路由施加 fence。其错误细节只写日志，客户端收到固定 stale-policy 响应，不连接旧 backend。

一个逻辑请求无论直接 ForwardAuthorized，还是 canonicalize 后调用 next，admission 都只发生一次；wrapper 不能嵌套这两条路径。拒绝先于 Wake、activation 或 dial，使用 core 的 429 max_inflight_reached。Admission lease 和 parking/egress 记账共享普通响应、CONNECT relay、取消及失败 cleanup 的单一生命周期。Generic helper 仍拒绝 native exec，因此不能绕过 KAT/CEL/首帧 gate，也不能把 exec admission 提前到 gate 之前。

普通 HTTP 的 Rewrite 收到独立的 Guest 请求副本。回调后 core 完成最终 hop-header/transport 归一化，回调失败则不写任何 Guest 请求字节。CONNECT 没有 Guest HTTP 请求，绝不调用 Rewrite。Generic helper 拒绝 native exec target，标准 exec 继续走 next 与 KAT、逐命令 CEL 路径。Helper 复用仓库现有普通 HTTP 和 CONNECT 传输，不实现 WebSocket。

## Ingress 边界

Worker IngressWrapper 只在 Proxy data_listen 可达。Conductor 公共 listener 与 config-socket API fallback 均使用被包装的控制 API handler，绝不将普通 HTTP/CONNECT 转发给 worker。Cluster router、registry、placer 不增加 extension 或 private ingress；私有部署必须先在外层 canonicalize 集群请求，再进入既有 canonical 集群路径。

## 边界

Conductor、Proxy master 或 worker 扩展为 nil 时保留内置启动与请求行为，不创建 event hub、watcher goroutine、lifecycle callback 或 wrapper。Cluster router、registry、placer 没有扩展对象；节点本地 Hook 和观察不改变 node-link ACK、exact replay、幂等或 stable SandboxID → NodeSandboxID 的权威关系。

此 API 没有 namespace、固定 extension URI、动态加载、热更新或 component compatibility version。WebSocket 不属于 #256，由 #269 独立跟踪。

可构建程序见 [`examples/custom-conductor`](../examples/custom-conductor) 与 [`examples/custom-proxy`](../examples/custom-proxy)。
