# kuasar-sandbox

面向大规模 Serverless / Agent 场景的 microVM 沙箱平台:亚秒级冷启动与快照恢复、
跨镜像/快照内容级去重的存储加速、单节点数千沙箱的密度与资源管控,并以单机
**e2b 兼容**的沙箱编排/ingress(未改造的 e2b SDK 可直连本机)对外提供北向入口。

本仓是平台的**主项目**:承载系统级设计文档、跨仓 e2e/perf 套件,以及把各子项目
制品聚合成"下载即用"大版本发布包的完整构建入口。各能力拆分为独立演进的子仓,
边界只暴露薄的、命名具体的纯 Go 导入面;子仓分工、依赖 DAG 与导出面规则见
[docs/kuasar-sandbox.md](docs/kuasar-sandbox.md) §2。

> **快速体验(e2b demo)**:`make demo`(发布包内:先 `bash test/demo/demo_prep.sh`
> 再 `bash test/demo/demo_e2b.sh`)——用**未改造的 e2b Python SDK** 在本机走通
> "构建模板 → 启真实 microVM → guest 内执行 → 端口转发/出网 → 暂停/恢复 →
> 快照转模板扇出 → 迁移 → 销毁"全链路;`DEMO_PAUSE=1` 逐步暂停、可另开终端用
> SDK 手动操作。前置工具与说明见 [test/demo/DEMO.md](test/demo/DEMO.md)。

## 子项目

| 仓 | 角色 | 导出面 / 产物 |
|---|---|---|
| **kuasar-sandbox**(本仓) | 系统文档 + 发布聚合 + 跨仓 e2e/perf | `scripts/release.sh`、`docs/`、`test/` |
| **sandbox-runtime** | microVM 生命周期引擎(host `sandbox-ctl` + guest `sandbox-init`)+ vhost 块后端 | `pkg/resource`(资源控制协议+Client) |
| **sandbox-orchestrator** | 单机 e2b 兼容沙箱编排/ingress(控制面 + envd-in-guest 反代 + 模板构建)+ 节点级资源守护(准入/分配/回收,3,000+ 密度) | `orchestrator-ctl` + `e2b-key-ctl` + `node-ctl`、`sandbox-runtime-{e2b,builder}.erofs` |
| **sandbox-accelerator** | 存储加速 + 镜像构建:内容寻址存储 + 分层缓存 + 收敛加密 + OCI → EROFS 确定性展平 | `pkg/manifest`、`pkg/image`、`pkg/{cache,store}/client` + `flatten-ctl` |
| **sandbox-vswitch** | eBPF/TC 虚拟交换机 + tapfd 交接 | `pkg/tapfd`(fd 交接规约)+ `vswitch-ctl`/`tapfd-get` |
| **sandbox-deps** | 原生依赖:vmlinux / cloud-hypervisor / mkfs.erofs | 构建脚本 + patches + configs |

## 构建

**完整构建入口(本仓 Makefile)**:`make build` 按依赖序编排全部子仓构建,按
`scripts/artifacts.list` 收集制品到 `bin/$(TARGET_ARCH)/`,并装配出注入 envd 的
`sandbox-runtime-e2b.erofs`:

```bash
make -C kuasar-sandbox all        # = build:全部子仓 + 装配 bin/
make -C kuasar-sandbox release    # 打包 dist/kuasar-sandbox-<ver>-linux-<arch>.tar.gz
make -C kuasar-sandbox help       # 全部目标(test-e2e / perf / bench / demo / ...)
```

发布包聚合 `bin/` + 精选 `docs/` + 跨仓 e2e/perf/demo 脚本 + `deploy/` 样例,
解包即用(脚本经相对路径自动定位 `bin/`,见 `test/QUICKSTART.md`)。

**单仓(私网/离线,发布路径)**:每个仓 `go.mod` 用 `replace` 指向兄弟目录,
clone 全组织为兄弟目录后即可离线构建,无需 GOPROXY 或版本 tag:

```bash
cd sandbox-runtime && GOWORK=off make build     # 同理各仓
```

**统一开发(go.work)**:org 根 `go.work` 把所有 module 纳入一个工作区,首次
`go work sync` 对齐第三方依赖后任意子目录可 `go build ./...`。

## 跨仓测试

`test/`(e2e + perf + demo)是系统级集成套件,覆盖需要多仓二进制协作的路径
(真实 microVM 启动、快照/恢复、去重、密度、e2b 编排):

```bash
make -C kuasar-sandbox test-e2e          # 全部跨仓 e2e + 各子仓自有 e2e
make -C kuasar-sandbox test-e2e-<name>   # 单个,如 test-e2e-sandbox-cold
make -C kuasar-sandbox perf              # 性能 harness 全套
```

脚本清单、前置条件与排错见 [test/QUICKSTART.md](test/QUICKSTART.md)。

## 文档

- [docs/kuasar-sandbox.md](docs/kuasar-sandbox.md) — 系统设计总览:业务目标与
  指标、子系统分工与依赖、端到端数据流、关键机制、安全模型、规模推算。
- [docs/deployment.md](docs/deployment.md) — 部署拓扑与组件清单:进程归属、
  端口、启停依赖、故障域。
- [docs/perf.md](docs/perf.md) — 实测性能基线、回归 checklist 与调优杠杆。
- 模块设计文档随各自仓(如 `sandbox-runtime/docs/sandbox.md`、
  `sandbox-accelerator/docs/manifest.md`、`sandbox-orchestrator/docs/orchestrator.md`)。
