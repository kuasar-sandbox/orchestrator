# test — release 包内的 e2e / perf 入口指南

本文件随 release tarball 一并下发，位于 `<release>/test/QUICKSTART.md`。
适用于已解开 `kuasar-sandbox-<ver>-linux-<arch>.tar.gz` 的用户，目的是把
"如何把这堆脚本跑起来"压缩为一页。

## 1. 解包后的布局

```
<release>/
├── bin/                       平台全部二进制（cloud-hypervisor / vmlinux /
│                              mkfs.erofs / manifest-ctl / store-ctl / cache-ctl /
│                              flatten-ctl / sandbox-ctl / sandbox-init /
│                              sandbox-runtime.erofs / node-ctl / vswitch-ctl /
│                              tapfd-get / *.erofs）
├── README.md                  项目入口
├── docs/                      系统设计 + 模块设计 + 部署 + 性能基线
└── test/
    ├── QUICKSTART.md          本文件
    ├── e2e/                   18 个跨仓 e2e 脚本（一键跑，零环境变量）
    └── perf/                  7 个性能/分析脚本
```

`bin/` 的绝对路径会被所有 `e2e/*.sh`、`perf/*.sh` 通过相对路径
`SCRIPT_DIR/../../bin` 自动定位——**无需 export `BIN`**。如需把 `bin/` 复制到
其他目录运行（如 `/tmp` 规避 WSL2 drvfs ~500ms/exec 开销），加 `BIN=...` 覆盖。

## 2. 前置条件

每个 e2e 脚本头部都内置 `skip()` 探测，缺什么就打印什么并以 0 退出。设
`REQUIRE_KVM=1` 把"跳过"改为"硬失败"。常见前置：

- `/dev/kvm` 可读写（嵌套 KVM 亦可）
- pre-existing TAP 设备已 up（默认名 `sb-tap0`，env `TAP_NAME` 覆盖）
- `docker` 可用（用于 `docker pull` 拉镜像；可用 `BLK0_IMAGE=...` 提供预制 erofs 跳过）
- 部分脚本需要 root（cgroup、network namespace、uffd）

`bin/cloud-hypervisor` 和 `bin/vmlinux` 已在包内；脚本会自动指向它们。

## 3. 三十秒上手

```bash
tar xzf kuasar-sandbox-<ver>-linux-<arch>.tar.gz
cd kuasar-sandbox-<ver>-linux-<arch>
bash test/e2e/e2e_sandbox_cold.sh      # 冷启 python:3.12-slim 并验证退出
```

跑全套（顺序执行，每个独立）：

```bash
for f in test/e2e/*.sh; do bash "$f" || break; done
```

## 4. e2e 脚本清单（18 个）

### 沙箱生命周期

| 脚本 | 验证内容 |
|---|---|
| `e2e_sandbox_cold.sh` | python:3.12-slim 冷启动，含 image-config 自动提取 |
| `e2e_sandbox_cold_manifest.sh` | blk0 base 来自 manifest:// 加载 |
| `e2e_sandbox_cold_target.sh` | 指定 launch target 的冷启动 |
| `e2e_sandbox_diff_template.sh` | 无显式 overlay.diff 时模板退化路径 |
| `e2e_sandbox_launchspec.sh` | 完整 launch spec（env/cwd/cmd 合并）|
| `e2e_sandbox_proto.sh` | host↔guest 双向 launch 协议 |
| `e2e_sandbox_stdio.sh` | sandbox-ctl stdio 转发模型 |
| `e2e_sandbox_tapfd.sh` | 网络来自 tapfd handoff 的沙箱启动 |

### 快照 / 恢复

| 脚本 | 验证内容 |
|---|---|
| `e2e_sandbox_snapshot.sh` | 长跑沙箱执行 snapshot |
| `e2e_sandbox_restore.sh` | snapshot → restore 链路 |
| `e2e_sandbox_restore_files.sh` | restore 时按实例注入文件 |
| `e2e_sandbox_upload_restore.sh` | 快照 upload 并通过 manifest:// restore |

### 存储 / 镜像 / 去重

| 脚本 | 验证内容 |
|---|---|
| `e2e_manifest.sh` | manifest-ctl + flatten-ctl 真实 Docker 镜像 |
| `e2e_obs.sh` | store-ctl OBS backend 与华为云 OBS 往返 |
| `e2e_warmpool_dedup.sh` | warm pool 跨沙箱去重率 |
| `e2e_cache.sh` | cache-ctl + manifest-ctl 集成 |
| `e2e_cluster_rolling.sh` | EC 集群成员滚动变更（SIGHUP）|

### 密度 / 资源

| 脚本 | 验证内容 |
|---|---|
| `e2e_density.sh` | agent 间歇式三模式密度 e2e |

## 5. perf / 分析脚本清单（7 个）

| 脚本 | 用途 |
|---|---|
| `perf/sandbox-perf.sh` | sandbox-ctl 冷启动性能跨时序追踪 |
| `perf/sandbox-perf-manifest.sh` | manifest:// 加载路径性能矩阵 |
| `perf/density-perf.sh` | sandbox-resource-control 密度压测 |
| `perf/workload.py` | agent 工作负载发生器（cycles / pareto / idle 模式） |
| `perf/bench_cache.sh` | cache-ctl 性能 smoke |
| `perf/bench_cache_remote.sh` | 多机 cache-ctl 远端 bench driver |
| `perf/dedup_report.sh` | N 个容器镜像的去重分析报告 |

跑法与 e2e 相同：`bash test/perf/<name>.sh`。环境变量见各脚本头部 `# Modes` /
`# Env` 注释。

## 6. 留在源仓的 e2e

`sandbox-sentinel/test/e2e/e2e_node_ctl.sh` **未打包**。该脚本通过 heredoc 内
联生成 Go driver 文件并 `go run` 执行（驱动 import
`github.com/kuasar-sandbox/sandbox-runtime/pkg/resource`），运行时需要源码工
作区。如需跑：

```bash
git clone https://github.com/kuasar-sandbox/sandbox-sentinel
git clone https://github.com/kuasar-sandbox/sandbox-runtime  # 兄弟目录
cd sandbox-sentinel && GOWORK=off make build
bash test/e2e/e2e_node_ctl.sh
```

## 7. 排错速查

- "skipping (...)" 退出 0 — 缺前置条件；按提示补齐或 `REQUIRE_KVM=1`
- "permission denied /dev/kvm" — `sudo usermod -aG kvm $USER` 后重登
- TAP 缺失 — `sudo ip tuntap add sb-tap0 mode tap user $USER && sudo ip link set sb-tap0 up`
- WSL2 上慢 — 把 `bin/` 复制到 `/tmp/kbin`，`BIN=/tmp/kbin bash test/e2e/...`
- docker pull 失败 — `BLK0_IMAGE=/path/to/prebuilt.erofs` 跳过 flatten 步骤
