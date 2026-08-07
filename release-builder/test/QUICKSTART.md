# test — release 组件包内的 e2e / perf 入口指南

本文件随 orchestrator 组件包一并下发,位于 `<release-dir>/test/QUICKSTART.md`。
适用于把 `release-v*` 下发的多个组件包解到同一个目录后的用户,目的是把
"如何把组件包合并后的脚本跑起来"压缩为一页。

## 1. 解包后的布局

```
<release-dir>/
├── bin/                       平台全部二进制(cloud-hypervisor / vmlinux /
│                              mkfs.erofs / manifest-ctl /
│                              store-ctl / cache-ctl / flatten-ctl / sandbox-ctl /
│                              sandbox-init / node-ctl / connector-ctl /
│                              e2b-key-ctl / sandbox-runtime.bundle)
├── docs/                      平铺语义文档(kuasar-sandbox.md / sandboxer.md /
│                              cloud-hypervisor.md / vmlinux.md / ...)
├── deploy/                    运维配置样例 + systemd 单元(config.example.yaml /
│                              node-ctl.service / node-proxy.service)
├── release/                   组件包元数据
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

每个 e2e 脚本头部都内置前置探测。直接运行单个脚本时,部分重型前置缺失会
以 0 退出,方便在开发机上做局部验证;统一入口 `test/e2e/run_all.sh` 默认设置
所有 `REQUIRE_*` 标志,除 OBS 凭据场景外,缺前置即失败。常见前置:

- `/dev/kvm` 可读写(嵌套 KVM 亦可)
- pre-existing TAP 设备已 up(默认名 `sb-tap0`,env `TAP_NAME` 覆盖)
- `docker` 可用(用于 `docker pull` 拉镜像;可用 `BLK0_IMAGE=...` 提供预制 erofs 跳过)
- `zot` 可用(本地 OCI registry;安装到 PATH 或设置 `ZOT_BIN=/path/to/zot`)
- `versitygw` 可用(COPY/files_storage e2e;安装到 PATH 或设置 `VGW_BIN=/path/to/versitygw`)
- 部分脚本需要 root(cgroup、network namespace、uffd)

`bin/cloud-hypervisor` 和 `bin/vmlinux` 已在包内;脚本会自动指向它们。

## 3. 三十秒上手

```bash
mkdir kuasar-sandbox-release && cd kuasar-sandbox-release
for f in \
  ../accelerator-v*-linux-<arch>.tar.gz \
  ../connector-v*-linux-<arch>.tar.gz \
  ../sandboxer-v*-linux-<arch>.tar.gz \
  ../orchestrator-v*-linux-<arch>.tar.gz \
  ../sandbox-runtime-<arch>-runtime-v*.tar.gz \
  ../vmlinux-<arch>-vmlinux-v*.tar.gz; do
  tar xzf "$f"
done
bash test/e2e/e2e_sandbox_cold.sh      # 冷启 python:3.12-slim 并验证退出
```

跑全套(顺序执行,每个独立):

```bash
bash test/e2e/run_all.sh
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
| `e2e_sandbox_placeholder.sh` | launch.placeholder 无 NIC 冷启动:CH 无 `--net`,guest 仅 `lo`,vsock exec + kill 锚点原地重启不 reboot |
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
| `e2e_density.sh` | agent 间歇式密度 e2e:自动分配、静态 guest self-cap 收敛与动态主动 grant 对照、创建反压 |

### 沙箱编排 / e2b

**最直观:先跑 demo 看全链路** —— `bash test/demo/demo_prep.sh` 后
`sudo bash test/demo/demo_e2b.sh`:用**未改造的 e2b Python SDK** 走通
"构建模板 → 启真实 microVM → guest 内执行 → 端口转发/出网 → 暂停/恢复 →
快照转模板扇出 → 迁移 → 销毁";`DEMO_PAUSE=1` 可逐步暂停,另开终端 source
脚本写出的凭据文件后用 SDK 手动操作。详见 [`demo/DEMO.md`](demo/DEMO.md)。

下表 e2e 为分项断言(回归用),全部用 curl 驱动原生 API,不需要 e2b SDK/CLI:

| 脚本 | 验证内容 |
|---|---|
| `e2e_orchestrator.sh` | node-ctl 单元自动安装 + e2b 控制面(`/health`、`X-API-KEY` 401)+ 构建 API |
| `e2e_runtask.sh` | run-sandbox/run-builder 启动器 + `config`/`info` CLI(纯用户态,无 root/systemd/KVM)|
| `e2e_run_builder.sh` | 三阶段构建流水线(KVM):guest 内拉取展平 → steps → 模板快照;fromImage/fromTemplate 三链 + 从产物模板 create;Build Register MMDS real guest、Trigger 禁止覆盖、终态 secret cleanup/制品隔离 |
| `e2e_execute.sh` | 启真实 microVM(KVM)→ local Pause 三态 policy(node/Create/Pause/reaper)→ B 本地恢复→`W -> local B`→独立 export/promote→portable W 恢复 + self-only prefetch→kill |
| `e2e_sandbox_disks.sh` | `merge_ref=false` working-set:memory self/parent 分层,root + data disk 仍合并并可本地恢复 |
| `e2e_orchestrator_proxy.sh` | external proxy(master routesync + shm route view + worker fd inheritance)+ 数据面 X-Access-Token + auto-resume |
| `e2e_mmds_routes_internal.sh` | 在 `e2e_execute.sh` 的真实 guest 上追加 internal static、initial/unresolved/PUT/rotate/DELETE secret 与 conductor local UDS service |
| `e2e_mmds_routes_external.sh` | 在 external worker 真实 guest 上覆盖同一合同,并重启空 heap proxy 验证 full resync/fail-closed 恢复;`proxy.yaml` 无 services |
| `e2e_cluster_real.sh` | cluster-ctl registry/router/placer + 真实 node-ctl + 真实 microVM;阶段一 N=1 registry,阶段二 N=3 registry + node-link redirect |

> **前置(比其他 e2e 重)**:这组脚本另需 systemd 为 PID1 + root、`docker`、
> `zot`(安装到 PATH 或设置 `ZOT_BIN`);`e2e_run_builder` 还需 `versitygw`
> (安装到 PATH 或设置 `VGW_BIN`);`e2e_execute`/`e2e_orchestrator_proxy` 还需
> `/dev/kvm` 与 `mkfs.ext4`;`e2e_cluster_real` 还需 `cluster-ctl` / `node-ctl`
> / `connector-ctl` / `store-ctl` 等完整 release-builder `bin/`;demo 另需 e2b Python SDK
> (`pip install e2b e2b-code-interpreter`)、`openssl`、`sqlite3`、`iptables`。
> `run_all.sh` 会打开所有 REQUIRE 标志,除 `e2e_obs.sh` 在 `OBS_E2E=1`
> 未设置时允许跳过外,缺失前置都会失败。

MMDS 两条 wrapper 的声明只使用最终 `{secrets:{...},routes:[...]}` schema。两者会扫描
sqlite、proxy mmap 和测试日志中的固定 secret marker;Build 用例还检查终态 value row 已删除、
final image config 无 MMDS namespace。它们不增加 cluster MMDS case;cluster 回归仍由现有
`e2e_cluster_real.sh` 与源仓 cluster stub 承担。

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

`orchestrator/test/e2e/e2e_node_ctl.sh` **未打包**。该脚本通过 heredoc 内
联生成 Go driver 文件并 `go run` 执行(驱动 import
`github.com/kuasar-sandbox/sandboxer/pkg/resource`),运行时需要源码工
作区。如需跑:

```bash
git clone https://github.com/kuasar-sandbox/orchestrator
git clone https://github.com/kuasar-sandbox/sandboxer  # 兄弟目录
cd orchestrator && GOWORK=off make node-ctl
bash test/e2e/e2e_node_ctl.sh
```

## 7. 排错速查

- "skipping (...)" 退出 0 — 缺前置条件;按提示补齐或 `REQUIRE_KVM=1`
- "permission denied /dev/kvm" — `sudo usermod -aG kvm $USER` 后重登
- TAP 缺失 — `sudo ip tuntap add sb-tap0 mode tap user $USER && sudo ip link set sb-tap0 up`
- WSL2 上慢 — 把 `bin/` 复制到 `/tmp/kbin`,`BIN=/tmp/kbin bash test/e2e/...`
- docker pull 失败 — `BLK0_IMAGE=/path/to/prebuilt.erofs` 跳过 flatten 步骤
