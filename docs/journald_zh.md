[English](journald.md) | [简体中文](journald_zh.md)

# 沙箱与 Build 的 journal 身份

本文补充[节点参考文档](node_zh.md)的日志章节。
运行时目标语法见 [sandboxer journal 指南](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/journald_zh.md)。

## 职责与输出目标

业务身份字段的含义及选择由 orchestrator 负责。sandboxer 接收相互独立的
`journald=TAG[,FIELD=VALUE...]` 输出目标，不从环境变量读取这些字段，也不从
unit 名称、目录、模板或快照推导它们。没有进程级 `--journal-field` 参数，
各输出之间也不继承字段。

每次托管沙箱运行时，Conductor 生成四个完整目标：

| 参数 | identifier | 身份字段 |
| --- | --- | --- |
| `--log-to` | `sandbox-ctl` | `KUASAR_STABLE_ID`、`KUASAR_SANDBOX_ID`、`KUASAR_RUN_ID` |
| `--stdout-to` | `sandbox` | 同样三个字段，分别显式传入 |
| `--stderr-to` | `sandbox` | 同样三个字段，分别显式传入 |
| `--console` | `console` | 同样三个字段，分别显式传入 |

字段值直接来自 `Sandbox.StableID()`、`Sandbox.ID` 与当前 `Sandbox.RunID`。
因此普通单机沙箱也有 StableID，缺省时回退到本地 ID。独立传入的 StableID 或
import 保留的 StableID 不要求沙箱属于集群。同一个 artifact 创建的新沙箱使用自身
身份，artifact 内容不是日志字段的来源。

同节点恢复保留逻辑身份，并使用新分配的 RunID。保持身份的 import/migration 可在
改变本地 SandboxID 和 RunID 时保留 StableID。StableID 是聚合键，不是本地唯一
lookup key：保持身份的复制可能让多个实例同时存在。

## Build 输出

每个 Build phase 的 `run` 目标显式携带当前 `BuildSpec` 的 `KUASAR_BUILD_ID` 与
`KUASAR_RUN_ID`。组件 identifier 为 `sandbox-ctl`，应用为 `build`，console 为
`console`。Build 输出不合成沙箱 StableID，也不把 phase 沙箱 ID 当作 Build 身份。

四个原有写 journal 的 flatten/export/referrer `exec` 调用点同样接收完整 `build`
目标。artifact/文件目标和 stderr 捕获保持不变；`exec` 进程诊断仍可被现有错误尾部
捕获逻辑读取。SDK 仍只消费 `build` 流，不包含 console 和运行时组件诊断。
run-builder 自身里程碑及 Envd RUN 回放保留现有原生 Build 字段。

## 编码与查询

格式化函数按键排序，对每个不透明值使用 Go `url.PathEscape`。运行时先分割字段，
再且仅再执行一次 `url.PathUnescape`。逗号及百分号不能注入字段，`+` 保持为字面加号，
显式空值不表示从环境变量读取。例如 `worker,42%2C+east` 编码为
`worker%2C42%252C+east`。

```bash
# 此主机上同一稳定逻辑身份的全部沙箱原生日志流。
journalctl KUASAR_STABLE_ID=worker-42

# 一次本地运行，包括组件诊断。
journalctl KUASAR_SANDBOX_ID=worker-42-g1 KUASAR_RUN_ID=run-17

# 仅应用或仅组件视图。
journalctl SYSLOG_IDENTIFIER=sandbox KUASAR_STABLE_ID=worker-42
journalctl SYSLOG_IDENTIFIER=sandbox-ctl KUASAR_STABLE_ID=worker-42

# SDK 可见的 Build 日志流。
journalctl SYSLOG_IDENTIFIER=build KUASAR_BUILD_ID=build-17
```

查询只作用于正在读取的 journal；跨节点汇聚由平台运营方负责。本修改不增加
exporter 或中心日志存储。

## 边界与验证

不改变 guest 环境、StdioSpec/MUX 协议、公共 API、metadata/header、快照格式或认证
身份。`MANIFEST_KEY` 保留现有秘密传递路径，绝不写入输出目标。
`--log-to` 只选择运行时组件诊断出口，不替换进程描述符，不接管 Cloud Hypervisor
stderr、runtime panic、node-ctl pre-exec 信息或 systemd 事件。

原生发送是同步且尽力而为的。成功后不再复制到 stderr；失败则回退到原始出口。
不承诺恰好一次持久化日志或零阻塞。

测试覆盖实际生成的 runner/Build 参数、StableID 回退及独立值、恢复/import/新对象
隔离、转义和文件/捕获行为保留。owner E2E runner 用 journal cursor 包围真实沙箱、
集群与 Build 用例，并按精确 sandbox-ctl 可执行文件筛选，然后验证应用、console 和
组件输出中的原生 JSON 字段。源码工作区还执行完整模块单测、race 和 vet；二进制包
不要求 Go 工具链。sandboxer 配套测试覆盖 cold/restore/exec 原生输出、尾部半行、
同 tag 独立字段，以及启动失败时不主动重复写 stderr。
