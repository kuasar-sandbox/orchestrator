# Aggregate release

## 1. 概述

聚合发布把五个独立仓库的一组精确 revision、完整 BMS E2E 结果和七个可部署组件包
绑定为同一个可追溯版本。维护者只需从 `orchestrator/main` 手动触发 `BMS Release`,
并输入 `vX.Y.Z`;工作流完成 revision 固定、源码装配、构建、全量测试、打包、清单
生成和 GitHub Release 发布。

`main` 与 `release` 承担不同职责:

- `main` 保存经 PR 审查的工作流和发布脚本,不作为聚合版本 tag 的代码归属;
- `release` 是独立的 orphan 发布历史,每个提交只保存该版本的 `release.json`;
- `release-vX.Y.Z` 指向对应 `release` 提交,GitHub Release 绑定该 tag;
- 实际源码由 `release.json` 中的五个完整 commit SHA 决定,不从聚合 tag 推断。

首次发布在 `release` 上创建无父提交的根提交。后续发布以当前 `release` head 为唯一
父提交,形成只包含成功版本的线性清单历史,不会继承 `orchestrator/main` 的文件树或
提交历史。

```text
main
  └─ .github/workflows/release.yml
       │
       ├─► resolve exact five-repository revisions
       ├─► full BMS E2E ─► package ─► Actions artifact
       │
       └─► publish job (GitHub-hosted, contents:write)
              │
release       ▼
  ● release.json (v0.1.0) ◄── release-v0.1.0 ◄── GitHub Release + assets
  │
  ● release.json (v0.1.1) ◄── release-v0.1.1 ◄── GitHub Release + assets
```

## 2. CLI

在 GitHub Actions 页面选择 `BMS Release`,分支选择 `main`,输入版本后执行。也可以
使用 GitHub CLI:

```bash
gh workflow run release.yml \
  --repo kuasar-sandbox/orchestrator \
  --ref main \
  -f version=v0.1.0
```

版本必须是无前导零的 `vX.Y.Z`;聚合 tag 固定为 `release-vX.Y.Z`。工作流不自动
推导版本号,避免并发维护者基于不同预期发布同一版本。

发布 job 因网络或 GitHub API 暂时失败时,应在原 workflow run 中只重试失败的
publish job。它会复用同一个已验证 Actions artifact。不要新建同版本 workflow run
来替代部分完成的发布;新 run 的 preflight 会拒绝已经占用的 tag、Release 或当前
`release.json`。

## 3. 配置

发布复用 BMS CI 已有的仓库变量和密钥:

- `KUASAR_CI_APP_CLIENT_ID`;
- `KUASAR_CI_APP_PRIVATE_KEY`。

GitHub App token 只授予五仓 `contents:read`,只在 BMS runner 的源码装配阶段存在,
并在任何仓库代码执行前撤销。发布 job 使用本次 workflow 的 `GITHUB_TOKEN`,且仅该
GitHub-hosted job 获得 `actions:read` 和 `contents:write`。自托管 runner 不持有仓库
写凭据。

仓库 Actions 默认权限继续保持只读。`release.yml` 在 job 级别显式扩大 publish
权限,不需要长期 PAT、部署密钥或发布专用 secret。

## 4. 设计

### 4.1 Revision set

可复用的 `bms-e2e.yml` 在 release mode 中通过一次 GitHub GraphQL 请求解析以下
分支 head:

- `kuasar-sandbox/accelerator:main`;
- `kuasar-sandbox/connector:main`;
- `kuasar-sandbox/guest-runtime:main`;
- `kuasar-sandbox/orchestrator:main`;
- `kuasar-sandbox/sandboxer:main`。

工作流要求解析出的 orchestrator SHA 等于 dispatch 时的 trusted workflow SHA。
如果 `main` 在源码装配前已经变化,本次发布失败,维护者重新 dispatch。解析成功后,
五个 SHA 写入 `revisions.tsv`,后续下载、构建、测试、包内元数据和最终清单均只使用
这组值;其他仓库之后的分支变化不改变本次发布。

### 4.2 构建、测试与打包

发布调用与普通 BMS CI 使用同一个 workspace 装配、源码 cache、native artifact
cache 和 `make test-e2e` 入口。完整测试成功后,不重新解析分支,直接使用已测试的
workspace 执行 `scripts/release.sh`。

`release.sh` 从 `KUASAR_REVISION_MANIFEST` 读取完整 commit SHA,写入每个包的
`release/<component>.json`。这避免 GitHub source archive 不含 `.git` 时向父目录
误解析 revision。`prepare-release.sh` 在上传前验证:

- revision set 恰好包含预期五仓,没有缺失、重复或额外仓库;
- `dist/` 恰好包含七个组件包和 `SHA256SUMS`;
- 每个 tarball 的包内名称、版本、架构、文件名和完整 commit SHA 与 revision set
  一致;
- `SHA256SUMS` 恰好覆盖七个包,且逐文件重新计算一致;
- 最终 `release.json` 中的资产名、大小和 SHA-256 与本地文件一致。

准备完成的 Actions artifact 包含:

```text
release.json
release-notes.md
assets/
  accelerator-vX.Y.Z-linux-<arch>.tar.gz
  connector-vX.Y.Z-linux-<arch>.tar.gz
  guest-runtime-vX.Y.Z-linux-<arch>.tar.gz
  orchestrator-vX.Y.Z-linux-<arch>.tar.gz
  sandboxer-vX.Y.Z-linux-<arch>.tar.gz
  sandbox-runtime-<arch>-vX.Y.Z.tar.gz
  vmlinux-<arch>-vX.Y.Z.tar.gz
  SHA256SUMS
```

### 4.3 权限边界

```text
self-hosted BMS job                    GitHub-hosted publish job
┌─────────────────────────────┐       ┌──────────────────────────────┐
│ GitHub App: contents:read   │       │ GITHUB_TOKEN                │
│ GITHUB_TOKEN: contents:read │       │ actions:read, contents:write│
│                             │       │                              │
│ source → build → full E2E   │──────►│ verify → branch/tag → draft │
│        → package            │artifact│ → upload → digest → publish │
└─────────────────────────────┘       └──────────────────────────────┘
```

候选源码、构建脚本和测试二进制只在无写权限的 job 中执行。具备写权限的 job 只执行
trusted `main` 中的发布脚本,读取上一 job 产生的不可变 Actions artifact,不执行五仓
构建产物。

### 4.4 `release` 分支与 tag

发布脚本使用 GitHub Git Database API 创建单文件 tree 和 commit。不存在 `release`
时创建无父根提交;存在时以当前 head 为父创建下一个提交,并通过非 force ref 更新
完成 compare-and-swap。并发更新导致 ref 不再是预期父提交时,API 返回冲突,脚本
停止,不会覆盖另一个发布。

分支推进后创建 lightweight `release-vX.Y.Z` ref,使分支 head、tag 和 GitHub
Release 对应同一个 commit。该 tag 的 GitHub 自动 Source code 归档只包含
`release.json`,不会把某个 orchestrator 提交误表示为五仓聚合源码。

`release.json` 的稳定结构包含:

- `version`、`tag`、`architecture` 和生成时间;
- 五仓 repository 名称及完整 commit SHA;
- BMS workflow repository、run ID、attempt、URL;
- 八个上传资产的名称、字节数和 SHA-256。

同一份字节内容既作为 `release` commit 的唯一文件,也作为 GitHub Release 附件。

### 4.5 发布事务

publish job 按以下顺序执行:

1. 再次验证 bundle schema、资产大小和 SHA-256;
2. 验证现有 `release` head 对应已发布的上一版本;
3. 创建新的待发布 commit 和指向它的 `release-vX.Y.Z` tag,此时不推进分支;
4. 创建 draft GitHub Release,上传八个资产和 `release.json`;
5. 从 GitHub Release API 读取全部资产,逐项比较名称、大小、状态和服务器计算的
   `sha256:` digest;
6. 全部一致后以非 force compare-and-swap 推进 `release`,再把 draft 发布为 latest
   release;
7. 再次确认 `release` head、tag 和发布 commit 三者一致。

资产验证完成前发生暂时失败时,`release` 仍指向上一个成功版本,远端最多留下待发布
commit、tag 或 draft。原 publish job 重试时必须提供字节完全相同的 `release.json`,
且待发布 commit 必须以当前 `release` head 为父提交,才允许从该状态继续。脚本不会
覆盖已发布 Release,也不会用另一组 revision 或资产替换部分完成的同版本发布。

## 5. 可靠性

| 失败位置 | 远端发布状态 | 处理 |
|---|---|---|
| revision 解析、构建或 E2E | 无变化 | 修复问题后重新 dispatch |
| package 或 bundle 验证 | 无变化 | 修复打包问题后重新 dispatch |
| `release` ref compare-and-swap | 无新 Release | 检查并发发布,重新选择版本或重新 dispatch |
| tag 或 draft 创建 | `release` 不变,可能有待发布 commit/tag | 重试原 publish job |
| 资产上传或 digest 核验 | `release` 不变,draft 不对外发布 | 重试原 publish job |
| draft 发布后最终一致性检查 | Release 已发布 | 人工检查 GitHub ref;脚本不会自动改写已发布版本 |

workflow 级 `aggregate-release-<repository>` concurrency 不取消正在运行的发布,保证
同仓发布串行。preflight 同时拒绝已经存在的同名 tag 或 GitHub Release。真正的
一致性仍由 Git ref 的非 force 更新和发布前的远端 digest 核验保证,不依赖
concurrency 作为锁。

## 6. 性能

发布必须执行完整 BMS E2E,不会用已有 release 或仅有成功状态的旧 workflow run
代替。源码 archive cache 和 native artifact cache 与日常 BMS 共用,因此已验证
revision 的源码下载、kernel、EROFS、RocksDB、envd 和 Cloud Hypervisor 构建通常
命中 cache。E2E 完成后的打包直接复用当前 workspace 的构建输出,不再执行第二次
五仓构建。

Actions 中间制品保留 7 天,只用于 publish job 交接和短期失败重试。长期交付物由
GitHub Release 保存。

## 7. See Also

- [ci.md](ci.md) - BMS revision set、源码 cache、native artifact cache 与运行指标;
- [../README.md](../README.md) - 聚合构建入口和组件包布局;
- [deployment.md](deployment.md) - 发布包的部署拓扑与配置。
