# sandbox-sentinel

节点级资源守护进程 `node-ctl`,是 [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox)
单节点 3,000+ 沙箱密度目标的准入与资源仲裁者:在固定 vCPU + cgroup 限额模型下,
对超分内存做准入控制、burst 预算仲裁、settled 主动收缩与回收,并维护每沙箱
reservation 记账、持久化与审计。

`sandbox-ctl`(`sandbox-runtime`)是客户端:冷启动/恢复前申请准入与初始预算,
运行中上报 settled/heartbeat、按需 request-budget,结束时 release。`node-ctl`
是服务端/控制器;任何遵循协议(`docs/node.md` §5)的实现都可作为对端,
`node-ctl` 是参考实现。

## 组成

| 路径 | 角色 |
| --- | --- |
| `cmd/node-ctl` | CLI:`daemon`(systemd 单元入口)/ `status` / `list` / `drain` / `grant` / `reclaim` |
| `internal/nodectl` | 控制器实现:RPC server、admission worker、memory allocator、active reclaimer、idle sweeper、state persister(布局见 `docs/node.md` §7) |
| `internal/nodectl/resource_alias.go` | 把 `sandbox-runtime/pkg/resource` 的协议符号按本地名再导出,控制器与 CLI 无改动引用 |

## 与 sandbox-runtime 的关系

沙箱资源控制协议(wire 格式 + `Client`)的唯一定义点是
`sandbox-runtime/pkg/resource`(client/server 共用),本仓 import 它实现 server
侧——即 `sandbox-sentinel` 依赖 `sandbox-runtime`(`replace => ../sandbox-runtime`),
除此之外无跨仓依赖。纯 Go,`CGO_ENABLED=0`。

## 构建

```bash
make node-ctl            # 构建控制器 daemon(= make build / make all)
make vet test            # 静态检查 + 单元测试
make bench               # 基准(-bench=. -benchmem)
make test-e2e            # e2e(= test-e2e-node-ctl;经 sandbox-runtime/pkg/resource inline-Go client)
make clean               # 清理 bin/ build/
```

## 文档

- [docs/node.md](docs/node.md) — 设计与命令参考:资源预算与水位、沙箱资源控制
  协议规范、创建期管控、可靠性、工作负载模型。
