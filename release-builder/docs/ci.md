# BMS CI

五仓集成测试的唯一入口仍是:

```bash
make -C src/orchestrator/release-builder test-e2e
```

工作流不会用外部脚本代替该入口。仓库内 `scripts/ci-timed.sh` 只包裹 Makefile
中的单个构建或测试 recipe,向 `KUASAR_CI_TIMINGS` 追加耗时、CPU、最大 RSS 和
文件系统 IO 数据;未设置该变量时直接 `exec` 原命令。

## Revision set

PR 测试把 `github.sha` 对应的候选 merge commit 与其余四仓解析后的 `main` SHA
写入 `ci-metrics/revisions.tsv`,再按精确 SHA 装配源码。不要从 `src/*` 执行
`git rev-parse`:GitHub tarball 不含 `.git`,该命令会向上找到 runner checkout 并
报告无关 revision。

精确 SHA 的源码归档缓存在 `/var/cache/kuasar/sources/<repo>/<sha>.tar.gz`,命中
时同时校验 SHA256 和 tar 结构。BMS 仅在新 SHA 首次出现时访问 GitHub 下载源码;
每个仓库以单一 `flock` 串行发布和解包归档，并保留最近使用的 32 个 SHA，避免
长驻 runner 的磁盘占用无界增长。Linux 源码和容器镜像仍分别使用清华 TUNA 与
DaoCloud 国内镜像。

## Native artifacts

`scripts/native-cache.sh restore-or-build` 在完整构建前处理以下制品:

- guest `vmlinux`;
- `mkfs.erofs` 与配套 `fsck.erofs`;
- guest `envd`;
- RocksDB headers 与 `librocksdb.a`;
- patched `cloud-hypervisor`.

缓存路径为
`/var/cache/kuasar/native/v1/<arch>/<component>/<input-hash>/`.输入 hash 覆盖
构建定义和脚本、patch/config、固定的上游归档摘要、目标架构、Go 微架构、
Cargo 全局及 target-specific 编译选项、pkg-config 搜索环境及其解析到的
元数据/库、实际选择的 C/C++ 编译工具二进制/版本与系统包版本。每个条目包含
`inputs.tsv`、`provenance.txt`、`payload.tar` 和 `SHA256SUMS`。

同 key 的构建和命中恢复都持有条目 `flock`;miss 直到构建、校验和原子发布完成才
释放,等待者随后验证并恢复同一条目。发布后的目录去除写权限。命中恢复前会校验
descriptor hash、payload hash 和 tar 路径。损坏条目直接失败,不会在原路径修补。
发布 staging 目录使用独立锁；正常退出直接清理，强制取消或 runner 重启留下的
目录由后续 run 在确认锁已释放后回收，不会误删另一个 slot 正在发布的 payload。

每个架构、组件保留最近使用的 4 个 input hash。淘汰器只删除超过 1 小时保护期且
能非阻塞取得条目锁的旧目录,因此并发恢复或构建中的制品不会被删除。留存数和保护
期可分别用 `KUASAR_NATIVE_CACHE_MAX_ENTRIES`、
`KUASAR_NATIVE_CACHE_MIN_AGE_SECONDS` 调整。

cache miss 只允许在 CI 新装配、尚无原生源码目录的 workspace 中构建。脚本发现
已有 kernel/Cloud Hypervisor 等源码树时会拒绝删除,避免破坏开发者 WIP。

本地逻辑自测不下载或编译真实依赖:

```bash
make -C orchestrator/release-builder test-ci-tools
```

它覆盖热命中、环境/工具链/Kbuild 输入失效、损坏拒绝、同 key 并发 miss 只构建
一次,以及源码和原生制品归档留存上限。

## Run artifacts

每个 BMS run 上传 `ci-metadata-<run>-<attempt>`，包含:

- `run.tsv`:event candidate、merge SHA 与 PR head SHA;
- `revisions.tsv`:五仓精确 revision set;
- `source-cache.tsv`:源码归档命中与摘要;
- `native-cache.tsv`:原生制品 key、命中状态与等待/构建耗时;
- `timings.tsv`:每个构建组件、umbrella E2E 和子仓 E2E 的资源数据。
