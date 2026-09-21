# RFC-142 配对 checkpoint 身份

[English](rfc-142-pairs.md) | [简体中文](rfc-142-pairs_zh.md)

`ResumeSource` 保留 `Kind` 和 `Ref`。Snapshot 还必须包含 `SandboxRef`，即 S 实际选择
的 E。E-only source 将 E 存在 `Ref` 中，`SandboxRef` 为空。此对作为一个持久值参与
SQLite、cache、CAS 比较、恢复及加密迁移 token，绝不是尽力填充的缓存。
`removedRefs` 仅属于操作报告。

runtime capture 返回生产者的实际身份。`snapshot --json` 返回
`{snapshotRef,sandboxRef,removedRefs}`；`export --json` 返回
`{sandboxRef,removedRefs}`。capture 的差集为 `[]`。Bundle 在同一物理文件中使用两个
selector。本地引用保留身份限定符，只公开 basename；checkpoint 目录属于执行上下文。
原有默认人类可读输出和 upload-key stdout 保持兼容。

外部 S-only 模板是显式初始 preparation 输入，此时 starting 行尚无已受理的持久 source。
现有 tenant task 解析 S/E，在 bounded summary 中返回配对身份。RunID、根、launch mode
以及两个身份都参与 resolution digest。相同摘要可重放，E 不同的提交会冲突。只有匹配的
starting run 才能将完整对与 running 状态原子提交。preparation 不得替换已受理的对。
完整配置及依赖密钥的解析始终留在 tenant task。Builder recovery 也在现有 runtime
preparation 记录中冻结已受理的对，每次 replay 都比较完整对。

发布在报告和 token 中返回最终 S1/E1；keep-source 保留本地 S0/E0。portable Export
仅使用已保存的对，即使工件或存储不可访问也不读取工件、不启动元数据子进程。Import
认证后原子插入完整对，不打开工件。目标精确要求 S1/E1 时，Expectations 拒绝 S1/E2。

move Export 在源删除被持久接纳后返回，由正常的可重试 finalizer 负责清理。local 与
portable 源拥有的 BaseDir/checkpoint 最终都会删除，共享发布工件保持不变。接纳失败
保留 paused S0/E0；接纳后的清理失败保留 `deleting` 行与有效 S1/E1 结果。同 ID
Create/import 在源行最终删除前继续冲突。取消、重启与 observer 时序见[节点生命周期
契约](node_zh.md)。

升级前停止旧 conductor，排空或退役其管理的 sandbox。保留需要的记录和工件，再由
操作员明确清空旧本地数据库，使用匹配版本的 sandboxer 与 orchestrator 重新创建记录。
此配对 schema 不提供迁移、ALTER、回填或双读兼容。进程拒绝旧 schema，绝不为升级
删除用户数据。缺少必填 `resumeSandboxRef` 字段的旧 token（包括缺少 E 的 Snapshot token）会被拒绝；应废弃并从完整配对记录重新导出。
Export 绝不通过读取 S 修复缺失的 E。

验证覆盖真实 capture 生产者与 CLI 响应、SQL 原子回滚及重开、生命周期 CAS、preparation
身份与重试、严格认证 token，以及 S/E × token/template × keep/move × local/portable
共 16 种真实 CLI/API 组合。将 `KUASAR_TEST_SANDBOX_CTL` 指向配套二进制以执行集成
用例。portable 组合先导入 token，再使源工件不可用，最后重新导出。
