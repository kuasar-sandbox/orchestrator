# Release

## 1. 概述

Kuasar Sandbox 区分组件版本与平台聚合版本。组件版本回答“某个仓库交付了什么”,
聚合版本回答“本次平台交付选择了哪些已经发布的组件版本”。两者不共享版本号,
也不从同一组 `main` head 隐式推导。

独立版本线为:

- `accelerator`、`connector`、`sandboxer`、`orchestrator`:各自在本仓发布 `vX.Y.Z`;
- `guest-runtime`:不发布通用 `vX.Y.Z` 或 `guest-runtime-*.tar.gz`,而是分别发布
  `runtime-vX.Y.Z` 与 `vmlinux-vX.Y.Z`;
- `orchestrator`:额外发布平台聚合版本 `release-vX.Y.Z`。

一个聚合版本可以选择不同的组件版本。例如 `release-v0.2.1` 可以组合
`accelerator v0.2.0`、`sandboxer v0.2.0`、`orchestrator v0.1.5`、
`runtime-v0.2.1` 和 `vmlinux-v0.1.4`。选择关系存放在 `orchestrator` 仓的
`release` 分支,不是 `main` 代码的一部分。

```text
component main                         orchestrator/release
      │                                        │
      ├─ Component Release ─► vX.Y.Z           ├─ releases/release-v0.2.1.json
      │                         │              │              │
guest-runtime main              │              │              ▼
      ├─ Runtime Release ─► runtime-vX.Y.Z ────┼────► resolve published assets
      └─ Vmlinux Release ─► vmlinux-vX.Y.Z ────┘              │
                                                               ▼
                                                   full BMS E2E on exact assets
                                                               │
                                                               ▼
                                                   release-v0.2.1 + originals
```

## 2. CLI

### 2.1 独立发布

每个仓库都提供手动触发的发布 workflow:

```bash
gh workflow run release.yml \
  --repo kuasar-sandbox/accelerator --ref main -f version=v0.2.0

gh workflow run component-release.yml \
  --repo kuasar-sandbox/orchestrator --ref main -f version=v0.1.5

gh workflow run release-runtime.yml \
  --repo kuasar-sandbox/guest-runtime --ref main \
  -f version=runtime-v0.2.1 -f sandboxer_version=v0.2.0

gh workflow run release-vmlinux.yml \
  --repo kuasar-sandbox/guest-runtime --ref main -f version=vmlinux-v0.1.4
```

`accelerator`、`connector`、`sandboxer` 的 workflow 文件均为 `release.yml`。
每次 workflow 固定 dispatch 时的 `main` commit,构建、测试、打包并发布对应 tag。
已有 tag 或已发布 Release 不会被覆盖。

### 2.2 聚合发布

先创建本地 mapping 文件,再执行一次顺序发布:

```bash
make -C orchestrator/release-builder release \
  RELEASE_MAPPING=/path/to/release-v0.2.1.json
```

等价脚本入口为:

```bash
bash orchestrator/release-builder/scripts/release-suite.sh \
  /path/to/release-v0.2.1.json
```

脚本依次执行:

1. 把 mapping 以新文件提交到远端 `release` 分支;
2. 复用已发布且完成的组件版本,顺序触发尚未发布的组件 workflow;
3. 等待每个独立 Release 完成;
4. 触发 `orchestrator` 的 `Aggregate Release` workflow;
5. 等待完整 BMS E2E 和 `release-vX.Y.Z` 发布完成。

脚本使用当前维护者的 `gh` 登录凭据。仓库内 `GITHUB_TOKEN` 只对当前仓有效,
现有 BMS GitHub App 继续保持五仓 `contents:read`;快捷入口不要求扩大 App 到跨仓
Actions 写权限。

也可以在所有组件均已发布且 mapping 已进入 `release` 分支后只触发聚合:

```bash
gh workflow run aggregate-release.yml \
  --repo kuasar-sandbox/orchestrator --ref main \
  -f version=release-v0.2.1
```

## 3. 配置

### 3.1 版本映射

`release` 是与 `main` 无父子关系的独立分支。每个版本新增一个不可变文件
`releases/release-vX.Y.Z.json`:

```json
{
  "architecture": "x86_64",
  "components": {
    "accelerator": "v0.2.0",
    "connector": "v0.1.3",
    "orchestrator": "v0.1.5",
    "runtime": "runtime-v0.2.1",
    "sandboxer": "v0.2.0",
    "vmlinux": "vmlinux-v0.1.4"
  },
  "schemaVersion": 1,
  "version": "release-v0.2.1"
}
```

`release-map.sh put` 首次运行时创建无父根提交和 `release` 分支,后续以非 force
fast-forward 追加 mapping。已存在的版本文件只能复用完全相同的内容,不能改写。
`release-vX.Y.Z` tag 最终指向首次加入该 mapping 的 `release` commit,而不是
`orchestrator/main` commit。

当前发布架构固定为 workflow 中的 `x86_64`;mapping 保留 `architecture` 字段,
使组件资产名、BMS 环境和聚合清单显式一致。未来增加其他 runner/架构时仍使用
独立 mapping 与对应资产集合。

### 3.2 凭据与权限

组件和聚合工作流复用:

- repository variable `KUASAR_CI_APP_CLIENT_ID`;
- repository secret `KUASAR_CI_APP_PRIVATE_KEY`。

GitHub App token 只有五仓 `contents:read`,用于读取依赖源码、tag、Release 元数据和
资产。自托管 runner 在执行任何仓库代码前撤销 token。具备 `contents:write` 的
`GITHUB_TOKEN` 只存在于 GitHub-hosted publish job;该 job 只验证 bundle、创建 tag、
上传资产和发布 draft,不运行组件二进制。

## 4. 设计

### 4.1 组件 Release

每个组件发布分为三个 job:

```text
preflight (read)        build (self-hosted, read)       publish (hosted, write)
tag/release 不存在 ───► exact source → test → package ───► draft → upload → digest → publish
```

组件 archive 使用可合并的共享根布局:`bin/`、`docs/`、`test/`、`deploy/`、
`release/`。包内 `release/<component>.json` 与随 Release 上传的 `release.json`
记录主仓 commit、构建依赖的精确 commit、架构和 archive 名。`SHA256SUMS`、
`release.json` 与组件 archive 是组件 GitHub Release 的三个资产。

`sandboxer` 和 `orchestrator` 的源码仍通过当前薄 `replace` 关系构建;workflow
只读装配实际依赖 revision 并写入来源清单,不把这些 revision 升级为新的版本绑定规则。

### 4.2 Runtime 与 vmlinux

`guest-runtime` 的两个版本独立演进:

| 版本 | 原始 archive | 内容 |
|---|---|---|
| `runtime-vX.Y.Z` | `sandbox-runtime-<arch>-runtime-vX.Y.Z.tar.gz` | runtime bundle、`flatten-ctl`、`mkfs.erofs`、runtime 文档和 flatten e2e |
| `vmlinux-vX.Y.Z` | `vmlinux-<arch>-vmlinux-vX.Y.Z.tar.gz` | `vmlinux` 稳定入口、版本化 kernel 文件和 kernel 文档 |

runtime 构建要求输入一个已发布的 `sandboxer vX.Y.Z`,并把该 tag/commit 写入清单,
因为 `sandbox-init` 被嵌入 runtime bundle。该记录不要求聚合选择相同的 sandboxer
版本;最终组合是否兼容由聚合 BMS E2E 验证。

vmlinux workflow 只恢复或构建 native cache 的 `vmlinux` 组件。相同 kernel 输入和
工具链命中缓存时不重新编译 Linux;runtime workflow 只处理 `erofs` 与 `envd`,不会
因为发布 runtime 顺带构建 kernel。

### 4.3 聚合解析

`Aggregate Release` 只接受 `release-vX.Y.Z`,随后从 `release` 分支定位对应 mapping
文件的提交。preflight 对六个组件逐项验证:

- tag 与已发布、非 prerelease GitHub Release 一致;
- tag 指向组件 `release.json` 记录的 commit;
- Release 恰好包含组件 archive、`SHA256SUMS`、`release.json`;
- GitHub 计算的 `sha256:` digest、大小与组件清单一致;
- archive 名、版本前缀、架构、来源和 runtime 的 sandboxer 来源记录合法。

解析结果写入 `resolved.json` 并作为短期 Actions artifact 交给 BMS job。此后即使
各仓 `main` 或 `release` 分支前进,本次运行仍只使用已经解析的 tag、commit 和资产
digest。

### 4.4 精确制品 BMS

BMS 根据组件 tag commit 装配测试所需的五仓源码,但不从源码重新生成发布二进制。
它下载六个原始组件 archive,校验 GitHub digest、包内来源元数据和安全路径,再把
archive 中的 `bin/` 安装到测试目录。`test-e2e-prebuilt` 运行完整跨仓 E2E、
accelerator e2e、connector e2e 和 guest-runtime flatten e2e,所有命令都指向这些
已发布文件。

这使测试对象与最终聚合资产完全相同,同时消除旧流程的两个问题:

- 不会把五个 `main` head 当成一个隐式版本集合;
- 聚合验证不会重新编译 kernel 或重新打包组件。

测试通过后只复制六个原始 archive,新增一份覆盖六包的聚合 `SHA256SUMS` 和
`release.json`。聚合 Release 包含八个资产:六个 archive、`SHA256SUMS`、
`release.json`。

### 4.5 发布事务

publish job 按以下顺序执行:

1. 重新验证 aggregate bundle、六个组件清单和所有本地 SHA-256;
2. 确认 mapping commit 仍位于 `release` 分支历史,远端 mapping 字节未变化;
3. 创建指向 mapping commit 的 lightweight `release-vX.Y.Z` tag;
4. 创建 draft Release,上传八个资产;
5. 从 GitHub API 读取每个资产的大小、状态和服务器 `sha256:` digest;
6. 全部一致后发布 draft,并再次确认 tag 未移动。

构建和 E2E job 没有写权限;publish job 不推进 `release` 分支。mapping 的提交与聚合
发布是两个显式阶段,因此失败不会用另一组组件版本静默替换同名 Release。

## 5. 可靠性

| 失败位置 | 远端状态 | 处理 |
|---|---|---|
| mapping 提交 | 无组件或聚合发布 | 修正 mapping/并发冲突后重试 `release-suite.sh` |
| 独立组件构建或测试 | mapping 已记录,缺少对应组件 Release | 修复组件后重跑快捷入口;已完成版本会复用 |
| 聚合解析 | 组件 Release 不完整或 digest 不一致 | 修复/重新发布新的组件版本,新增聚合 mapping;不改写旧版本 |
| BMS E2E | 六个组件 Release 保持不变,无聚合 Release | 修复组件并选择新版本,或修复测试环境后重跑同一 mapping |
| draft 上传或 digest 校验 | 可能留下 tag/draft,公开 Release 不存在 | 在原 workflow run 重试 publish job |
| 发布后最终检查 | Release 已公开 | 人工核查远端 ref;脚本不会覆盖已发布资产 |

组件和聚合 workflow 都按仓库串行,但并发正确性不依赖 Actions concurrency。tag
存在检查、不可变 mapping、非 force `release` 更新和服务器 digest 校验共同构成最终
一致性边界。

## 6. 性能

组件 workflow 只构建自己的交付物和必要依赖。native 产物使用 BMS runner 的内容
寻址 cache;vmlinux、EROFS、envd、RocksDB 和 Cloud Hypervisor 输入不变时直接恢复。

聚合 BMS 不调用平台 `build`,只下载并运行已发布资产。因此一次由六个已有版本组成的
新聚合不会重复编译 Go 二进制、Cloud Hypervisor 或 Linux kernel。源码 archive cache
仍用于快速取得对应版本的测试配置和测试数据。

preflight 与 BMS 间、BMS 与 publish 间的 Actions artifact 保留 7 天,仅用于本次发布
和失败 job 重试;长期制品保存在各自 GitHub Release。

## 7. See Also

- [ci.md](ci.md) - 日常 BMS PR/main 验证、源码 cache 和 native cache;
- [../README.md](../README.md) - 聚合构建、测试与发布入口;
- [deployment.md](deployment.md) - 六类发布 archive 的部署布局;
- `guest-runtime/docs/sandbox-runtime.md` - runtime 镜像内容与独立版本契约。
