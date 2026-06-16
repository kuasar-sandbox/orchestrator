# test — release 包内的 e2e / perf 入口指南

本文件随 release tarball 一并下发,位于 `<release>/test/QUICKSTART.md`。
适用于已解开 `kuasar-sandbox-<ver>-linux-<arch>.tar.gz` 的用户,目的是把
"如何把这堆脚本跑起来"压缩为一页。

## 1. 解包后的布局

```
<release>/
├── bin/                       平台全部二进制(cloud-hypervisor / vmlinux /
│                              mkfs.erofs / fsck.erofs / envd / manifest-ctl /
│                              store-ctl / cache-ctl / flatten-ctl / sandbox-ctl /
│                              sandbox-init / node-ctl / vswitch-ctl / tapfd-get /
│                              orchestrator-ctl / e2b-key-ctl /
│                              sandbox-runtime.erofs / sandbox-runtime-e2b.erofs)
├── README.md                  项目入口
├── docs/                      系统设计 + 模块设计 + 部署 + 性能基线
├── deploy/                    运维配置样例 + systemd 单元(config.example.yaml /
│                              orchestrator-ctl.service / orchestrator-proxy@.service)
└── test/
    ├── QUICKSTART.md          本文件
    ├── e2e/                   跨仓 e2e 脚本(一键跑,零环境变量)
    ├── perf/                  性能 / 分析脚本
    └── demo/                  e2b 端到端演示(demo_e2b.sh + DEMO.md,最直观的"试一下")
```

`bin/` 的绝对路径会被所有 `e2e/*.sh`、`perf/*.sh` 通过相对路径
`SCRIPT_DIR/../../bin` 自动定位——**无需 export `BIN`**。如需把 `bin/` 复制到
其他目录运行(如 `/tmp` 规避 WSL2 drvfs ~500ms/exec 开销),加 `BIN=...` 覆盖。

## 2. 前置条件

每个 e2e 脚本头部都内置 `skip()` 探测,缺什么就打印什么并以 0 退出。设
`REQUIRE_KVM=1` 把"跳过"改为"硬失败"。常见前置:

- `/dev/kvm` 可读写(嵌套 KVM 亦可)
- pre-existing TAP 设备已 up(默认名 `sb-tap0`,env `TAP_NAME` 覆盖)
- `docker` 可用(用于 `docker pull` 拉镜像;可用 `BLK0_IMAGE=...` 提供预制 erofs 跳过)
- 部分脚本需要 root(cgroup、network namespace、uffd)

`bin/cloud-hypervisor` 和 `bin/vmlinux` 已在包内;脚本会自动指向它们。

## 3. 三十秒上手

```bash
tar xzf kuasar-sandbox-<ver>-linux-<arch>.tar.gz
cd kuasar-sandbox-<ver>-linux-<arch>
bash test/e2e/e2e_sandbox_cold.sh      # 冷启 python:3.12-slim 并验证退出
```

跑全套(顺序执行,每个独立):

```bash
for f in test/e2e/*.sh; do bash "$f" || break; done
```

## 4. e2e 脚本清单

### 沙箱生命周期

| 脚本 | 验证内容 |
|---|---|
| `e2e_sandbox_cold.sh` | python:3.12-slim 冷启动,含 image-config 自动提取 |
| `e2e_sandbox_cold_manifest.sh` | blk0 base 来自 manifest:// 加载 |
| `e2e_sandbox_cold_target.sh` | 指定 launch target 的冷启动 |
| `e2e_sandbox_diff_template.sh` | 无显式 overlay.diff 时模板退化路径 |
| `e2e_sandbox_launchspec.sh` | 完整 launch spec(env/cwd/cmd 合并)|
| `e2e_sandbox_proto.sh` | host↔guest 双向 launch 协议 |
| `e2e_sandbox_stdio.sh` | sandbox-ctl stdio 转发模型 |
| `e2e_sandbox_tapfd.sh` | 网络来自 tapfd handoff 的沙箱启动 |
| `e2e_sandbox_placeholder.sh` | launch.placeholder 空跑锚点:exec 驱动 + kill 锚点原地重启不 reboot |
| `e2e_sandbox_disks.sh` | boot.disks[] 多数据盘(单盘 + overlay)冷启挂载 + 快照/恢复数据存活 |

### 快照 / 恢复

| 脚本 | 验证内容 |
|---|---|
| `e2e_sandbox_snapshot.sh` | 长跑沙箱执行 snapshot |
| `e2e_sandbox_restore.sh` | snapshot → restore 链路 |
| `e2e_sandbox_restore_files.sh` | restore 时按实例注入文件 |
| `e2e_sandbox_local_merge.sh` | 本地快照层合并不变量(restore→再 snapshot 合并为单层)+ 本地→manifest:// 晋升回环 |
| `e2e_sandbox_upload_restore.sh` | 快照 upload 并通过 manifest:// restore |

### 存储 / 镜像 / 去重

| 脚本 | 验证内容 |
|---|---|
| `e2e_manifest.sh` | manifest-ctl + flatten-ctl 真实 Docker 镜像 |
| `e2e_obs.sh` | store-ctl OBS backend 与华为云 OBS 往返 |
| `e2e_warmpool_dedup.sh` | warm pool 跨沙箱去重率 |
| `e2e_cache.sh` | cache-ctl + manifest-ctl 集成 |
| `e2e_cluster_rolling.sh` | EC 集群成员滚动变更(SIGHUP)|

### 密度 / 资源

| 脚本 | 验证内容 |
|---|---|
| `e2e_density.sh` | agent 间歇式三模式密度 e2e |

### 沙箱编排 / e2b

**最直观:先跑 demo 看全链路** —— `bash test/demo/demo_prep.sh` 后
`sudo bash test/demo/demo_e2b.sh`:用**未改造的 e2b Python SDK** 走通
"构建模板 → 启真实 microVM → guest 内执行 → 端口转发/出网 → 暂停/恢复 →
快照转模板扇出 → 迁移 → 销毁";`DEMO_PAUSE=1` 可逐步暂停,另开终端 source
脚本写出的凭据文件后用 SDK 手动操作。详见 [`demo/DEMO.md`](demo/DEMO.md)。

下表 e2e 为分项断言(回归用),全部用 curl 驱动原生 API,不需要 e2b SDK/CLI:

| 脚本 | 验证内容 |
|---|---|
| `e2e_orchestrator.sh` | orchestrator-ctl 单元自动安装 + e2b 控制面(`/health`、`X-API-KEY` 401)+ 构建 API |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器 + `config`/`info` CLI(纯用户态,无 root/systemd/KVM)|
| `e2e_run_builder.sh` | 三阶段构建流水线(KVM):guest 内拉取展平 → steps → 模板快照;fromImage/fromTemplate 三链 + 从产物模板 create |
| `e2e_execute.sh` | 启真实 microVM(KVM)→ envd 内执行 → 暂停/恢复状态存活 → kill |
| `e2e_orchestrator_proxy.sh` | external proxy(SO_REUSEPORT + routesync)+ 数据面 X-Access-Token + auto-resume |

> **前置(比其他 e2e 重)**:这组脚本另需 systemd 为 PID1 + root、`zot`、
> `docker`;`e2e_run_builder`/`e2e_execute`/`e2e_orchestrator_proxy` 还需
> `/dev/kvm` 与 `mkfs.ext4`(run_builder 另需 `bin/sandbox-runtime-builder.erofs`,
> `make sandbox-runtime-builder` 产出);demo 另需 e2b Python SDK
> (`pip install e2b e2b-code-interpreter`)、`openssl`、`sqlite3`、`iptables`。
> 脚本会自检,缺失即 skip。

## 5. perf / 分析脚本清单

| 脚本 | 用途 |
|---|---|
| `perf/sandbox-perf.sh` | sandbox-ctl 冷启动性能跨时序追踪 |
| `perf/sandbox-perf-manifest.sh` | manifest:// 加载路径性能矩阵 |
| `perf/density-perf.sh` | sandbox-resource-control 密度压测 |
| `perf/workload.py` | agent 工作负载发生器(cycles / pareto / idle 模式) |
| `perf/bench_cache.sh` | cache-ctl 性能 smoke |
| `perf/bench_cache_remote.sh` | 多机 cache-ctl 远端 bench driver |
| `perf/dedup_report.sh` | N 个容器镜像的去重分析报告 |

跑法与 e2e 相同:`bash test/perf/<name>.sh`。环境变量见各脚本头部 `# Modes` /
`# Env` 注释。

## 6. 留在源仓的 e2e

`sandbox-orchestrator/test/e2e/e2e_node_ctl.sh` **未打包**。该脚本通过 heredoc 内
联生成 Go driver 文件并 `go run` 执行(驱动 import
`github.com/kuasar-sandbox/sandbox-runtime/pkg/resource`),运行时需要源码工
作区。如需跑:

```bash
git clone https://github.com/kuasar-sandbox/sandbox-orchestrator
git clone https://github.com/kuasar-sandbox/sandbox-runtime  # 兄弟目录
cd sandbox-orchestrator && GOWORK=off make node-ctl
bash test/e2e/e2e_node_ctl.sh
```

## 7. 排错速查

- "skipping (...)" 退出 0 — 缺前置条件;按提示补齐或 `REQUIRE_KVM=1`
- "permission denied /dev/kvm" — `sudo usermod -aG kvm $USER` 后重登
- TAP 缺失 — `sudo ip tuntap add sb-tap0 mode tap user $USER && sudo ip link set sb-tap0 up`
- WSL2 上慢 — 把 `bin/` 复制到 `/tmp/kbin`,`BIN=/tmp/kbin bash test/e2e/...`
- docker pull 失败 — `BLK0_IMAGE=/path/to/prebuilt.erofs` 跳过 flatten 步骤
