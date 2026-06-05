# sandbox-sentinel

A node-level resource guardian for high-density microVM sandboxes.

节点级资源守护进程 `node-ctl`，是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox)
单节点 3,000+ 沙箱密度目标的准入与资源仲裁者：在固定 vCPU + cgroup 限额模型下做内存/CPU
预算的准入控制、分配、回收，并维护每沙箱状态、持久化与审计。

## 角色

`sandbox-ctl`（`sandbox-runtime`）是**客户端**：冷启动/恢复前向 sentinel 申请准入与初始预算，
运行中上报 settled/heartbeat、按需 request-budget，结束时 release。`node-ctl` 是**服务端/控制器**：

| 组件 | 职责 |
|---|---|
| admission | 准入：容量/水位/floor 校验，admitted / queued / rejected |
| allocator | 内存-CPU 预算分配与 watermark 管理 |
| state | 每沙箱状态机 + reservation 记账 |
| reclaim | 压力下的预算回收（带 deadline） |
| persister / audit | 状态持久化 + 审计 |

## 与 sandbox-runtime 的关系

资源控制协议（wire 格式 + `Client`）定义在 `sandbox-runtime/pkg/resource`（client 与 server 共用）。
本仓的控制器 import 它实现 server 侧 —— 即 **`sandbox-sentinel` 依赖 `sandbox-runtime`**，
`internal/nodectl/resource_alias.go` 把协议符号按本地名再导出，控制器与 CLI 无改动引用。

纯 Go、`CGO_ENABLED=0`；除 `sandbox-runtime`（`replace => ../sandbox-runtime`）外无跨仓依赖。

## 构建

```bash
make node-ctl            # 构建控制器 daemon(= make build / make all)
make vet test            # 静态检查 + 单元测试
make bench               # 基准(-bench=. -benchmem)
make test-e2e            # e2e(= test-e2e-node-ctl;经 sandbox-runtime/pkg/resource inline-Go client)
make clean               # 清理 bin/ build/
```

协议契约与每沙箱状态机见 `docs/node.md`。
