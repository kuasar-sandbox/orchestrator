[English](README.md) | [简体中文](README_zh.md)

# 定制独立 Proxy

构建示例：

```sh
go build -o /opt/kuasar/bin/xproxy ./examples/custom-proxy
```

程序须由 root 或非 root node-ctl 服务 UID 拥有，禁止 group/world 写入。
在 `paths.proxy_executable` 配置绝对路径，仍通过
`node-ctl proxy serve --config ...` 启动；xproxy 不是独立 CLI。

每个 master 实例从受理时的原始输入执行一次 Configure。BindRuntime 在 master 与
各 worker epoch 中执行，重新绑定进程 logger 和 TLS/HSM 材料。Worker 从 sealed memfd
接收冻结配置，从不读取 proxy.yaml。

示例为 RoleMaster 创建一个受信 MasterExtension，为每个 RoleWorker epoch 创建新的
WorkerExtension。Master Start 启动可收敛的路由 Watch，管理 wrapper 在既有 0600
stats socket 增加 `GET /private/route?sandbox_id=...`，其余请求交给核心。
返回公共非秘密路由投影，不将原始凭据复制到普通 Watch 事件。SandboxID 是节点本地
查找键，StableID 是跨节点本地替换保持的身份。

业务排空后核心取消扩展 context，master 的可选 Shutdown 等待 Watch 退出。
Worker 没有保留的后台任务，Shutdown 立即返回。因此两者满足 InplaceRestart 的
结束能力预检，包括 `inplace_restart: false` 的程序调用路径。

Worker wrapper 在核心解析规范 `E2b-Sandbox-*` Host/header 前接收原始请求，示范两种私有格式：

- `X-Sandbox-Id` 加 `X-Sandbox-Port`；
- `/private/sandboxes/{sandbox-id}/{port}/{guest-path...}`。

`X-Private-Authorization: example-only` 仅为演示占位符，不能用于生产鉴权。
实际扩展必须验证自己的凭据并绑定 sandbox 与 target。示例验证后读取本地路由注册、
记住 revision，再调用 ForwardAuthorized。激活时的 Revalidate 检查该 revision，
Rewrite 删除私有 header 并按需映射 Guest 路径。未匹配请求调用 next，保留内置解析、
X-Access-Token、parking、native exec KAT/CEL 和错误行为。

ForwardAuthorized 采用 handler 形式，拥有成功或失败响应，不返回普通 error，也不验证
Kuasar 的 X-Access-Token。它支持普通 HTTP、CONNECT 和协商成功的 HTTP/1.1 WebSocket；
不支持 HTTP/2 extended CONNECT 或任意 Upgrade 协议，见
[传输边界](../../docs/node-proxy_zh.md#51-http11-websocket-转发)。Wrapper 仅作用于
data_listen 的 sandbox 入口，MMDS 不经过它。

这是静态链接、相同 UID 的受信代码组织方式。私有管理路由缺少鉴权只是最小示例，
实际部署应定义并执行本地管理授权。公共 App 仅定制独立 Proxy；conductor 是 API-only，
没有数据面定制入口。Runtime provider 为权威来源，失败不回退；TLS 版本、ALPN 和
client-auth 策略由核心拥有。Cluster ingress 没有 Extension，需在外层规范化请求。
不提供进程内配置/材料热更新、worker route Watch、namespace 或动态 plugin registry。
xproxy 与 node-ctl 应来自兼容 orchestrator 版本。

示例使用 App.Run：SIGINT/SIGTERM 请求整个服务停机；`inplace_restart: true` 时仅主协调
入口注册 SIGUSR2，且必须设置正值 `shutdown_timeout`。默认 false 不接管该信号。
timeout 为正时，两种配置都可在 WaitReady 后直接调用 `app.InplaceRestart(ctx)`。
timeout 省略或为零时，InplaceRestart 不可用，自动停机立即收尾；显式
`app.Shutdown(ctx)` 仍可优雅排空，不补隐含期限，取消 context 只结束调用者等待。
方法返回受理 ID，随后通过 `app.Status()` 或 `GET /status`
观察完成；原 Go 调用栈不能跨 exec 保留。自定义信号使用 RunContext，由入口只在主协调
实例安装触发器；B 与 worker 拒绝嵌套动作。

核心在 stats_socket 保留 `/readyz` 与 `/status`，`/private/route` 保持原行为。
Ready 依赖初始 Bookmark 和 worker 实际接纳，包含已启用的 MMDS。
Master BindRuntime 可交付 `Runtime.ListenerFiles`，成功后由 App 负责关闭。
普通 node-ctl → xproxy 激活由最终 master 自动接收。FD 名称、systemd unit、目标程序
原子投放、期限和自动恢复见[Proxy 部署与有界停机](../../docs/node-proxy_zh.md#92-业务就绪与服务状态)。
