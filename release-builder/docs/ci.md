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
Linux 源码和容器镜像仍分别使用清华 TUNA 与 DaoCloud 国内镜像。

## Native artifacts

`scripts/native-cache.sh restore-or-build` 在完整构建前处理以下制品:

- guest `vmlinux`;
- `mkfs.erofs` 与配套 `fsck.erofs`;
- guest `envd`;
- RocksDB headers 与 `librocksdb.a`;
- patched `cloud-hypervisor`.

缓存路径为
`/var/cache/kuasar/native/v1/<arch>/<component>/<input-hash>/`.输入 hash 覆盖
构建定义和脚本、patch/config、固定的上游归档摘要、目标架构、相关环境选项、
编译工具二进制/版本与系统包版本。每个条目包含 `inputs.tsv`、`provenance.txt`、
`payload.tar` 和 `SHA256SUMS`。

同 key miss 持有 `flock` 直到构建、校验和原子发布完成;等待者随后验证并恢复同一
条目。发布后的目录去除写权限。命中恢复前会校验 descriptor hash、payload hash
和 tar 路径。损坏条目直接失败,不会在原路径修补。

cache miss 只允许在 CI 新装配、尚无原生源码目录的 workspace 中构建。脚本发现
已有 kernel/Cloud Hypervisor 等源码树时会拒绝删除,避免破坏开发者 WIP。

本地逻辑自测不下载或编译真实依赖:

```bash
make -C orchestrator/release-builder test-ci-tools
```

它覆盖热命中、输入失效、损坏拒绝以及同 key 并发 miss 只构建一次。

## Run artifacts

每个 BMS run 上传 `ci-metadata-<run>-<attempt>`，包含:

- `run.tsv`:event candidate、merge SHA 与 PR head SHA;
- `revisions.tsv`:五仓精确 revision set;
- `source-cache.tsv`:源码归档命中与摘要;
- `native-cache.tsv`:原生制品 key、命中状态与等待/构建耗时;
- `timings.tsv`:每个构建组件、umbrella E2E 和子仓 E2E 的资源数据。
