# kuasar-sandbox

面向大规模 Serverless / Agent 场景的 microVM 沙箱平台：亚秒级冷启动与快照恢复、
跨镜像/快照去重的存储加速、单节点数千沙箱的资源管控。

本仓是平台的**主项目**：承载系统级设计文档、跨仓 e2e/perf 套件，以及把各子项目制品
聚合成"下载即用"的大版本发布包。各能力拆分为独立演进的子仓，边界处只暴露薄的、命名
具体的导入面（如 `pkg/manifest`、`pkg/tapfd`、`pkg/image`、`pkg/resource`）。

## 子项目

| 仓 | 角色 | 导出面 / 产物 |
|---|---|---|
| **kuasar-sandbox**（本仓） | 系统文档 + 发布聚合 + 跨仓 e2e/perf | `scripts/release.sh`、`docs/`、`test/` |
| **sandbox-runtime** | microVM 生命周期引擎（host `sandbox-ctl` + guest `sandbox-init`）+ vhost 块后端 | `pkg/resource`（资源控制协议+Client） |
| **sandbox-accelerator** | 存储加速：内容寻址存储 + 分层缓存 + 收敛加密 | `pkg/manifest`、`pkg/{cache,store}/client` |
| **sandbox-builder** | 镜像构建：OCI → EROFS 确定性展平 | `pkg/image`（读取展平镜像）+ `flatten-ctl` |
| **sandbox-vswitch** | eBPF/TC 虚拟交换机 + tapfd 交接 | `pkg/tapfd`（fd 交接规约）+ `vswitch-ctl`/`tapfd-get` |
| **sandbox-sentinel** | 节点级资源守护（准入/分配/回收，3,000+ 密度） | `node-ctl` 守护进程 |
| **sandbox-deps** | 原生依赖：vmlinux / cloud-hypervisor / mkfs.erofs | 构建脚本 + patches + configs |

## 依赖关系（DAG，发布拓扑序）

```
 sandbox-accelerator   sandbox-vswitch   sandbox-deps        (T0: 无内部依赖)
        ▲   ▲                ▲
        │   │ pkg/manifest   │ pkg/tapfd
        │   └────────────────┼──────────────┐
        │ pkg/manifest       │              │
 sandbox-builder ────────────┤              │              (T1)
        ▲                    │              │
        │ pkg/image          │ pkg/tapfd    │ pkg/manifest
 sandbox-runtime ────────────┴──────────────┘              (T2)
        ▲
        │ pkg/resource
 sandbox-sentinel                                          (T3)
```

各导出面均为 **纯 Go、无 CGO**：`sandbox-runtime` 不会因依赖 accelerator/vswitch 而引入
rocksdb / eBPF（Go module-graph pruning + 后端隔离在各仓 `server`/`rocks`/`internal` 内）。
唯一的 CGO 二进制是 accelerator 的 `cache-ctl`（静态链 librocksdb）。

## 构建

**单仓（私网/离线，推荐的发布路径）**：每个仓 `go.mod` 用 `replace` 指向兄弟目录，clone
全组织为兄弟目录后即可离线构建，无需 GOPROXY 或版本 tag：

```bash
cd sandbox-runtime && GOWORK=off make build     # 同理各仓
```

**统一开发（go.work）**：根 `go.work` 把所有 module 纳入一个工作区（IDE/跨仓改动友好）。
首次需用内网 GOPROXY 拉取第三方依赖（工作区合并 MVS 会选取各仓要求的最高版本）：

```bash
go work sync          # 一次性，按内网 GOPROXY 对齐 go.sum
go build ./...        # 任意子目录
```

**发布包（下载即用）**：

```bash
kuasar-sandbox/scripts/release.sh v0.1.0               # 聚合 Go 二进制（已有原生件一并收集）
kuasar-sandbox/scripts/release.sh v0.1.0 --with-natives # 连带构建 vmlinux/cloud-hypervisor/cache-ctl
# → kuasar-sandbox/dist/kuasar-sandbox-v0.1.0-linux-<arch>.tar.gz
```

## 文档

系统级文档在本仓 `docs/`（`PROPOSAL.md` 系统设计、`deployment.md` 部署拓扑、`perf.md` 性能基线）；
模块文档随各自仓（如 `sandbox-runtime/docs/sandbox.md`、`sandbox-accelerator/docs/manifest.md`）。

## 跨仓测试

`test/`（e2e + perf）是系统级集成套件（shell 驱动）。其构建步骤源自单体仓，迁入多仓后需对接
`scripts/release.sh` 聚合出的 `dist/.../bin/` 作为被测二进制目录 —— 详见 `test/README.md`。
