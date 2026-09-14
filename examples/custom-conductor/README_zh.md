[English](README.md) | [简体中文](README_zh.md)

# 自定义 conductor

构建示例:

```sh
go build -o /opt/kuasar/bin/xconductor ./examples/custom-conductor
```

二进制由 root 或非 root 的 node-ctl 服务 UID 持有, 禁止 group/world 写入.
在 `paths.conductor_executable` 设置其绝对路径, 继续通过
`node-ctl conductor serve --config ...` 启动服务. 此二进制不是独立 CLI.

`Configure` 在任何 conductor listener、持久 store、systemd unit 或 worker
启动之前执行一次. 声明式覆盖属于 `Config`; logger 和 material provider 属于
`Runtime`, 不参与序列化.

示例绑定一个可信的静态链接 runtime Extension. `Start` 保存 conductor `Host`,
启动随进程 context 退出的 Sandbox Watch. `Get` 和 Watch View 是深拷贝的非秘密
投影. Watch 通过 generation 最终收敛, 不是持久审计流; 未到达 `sync_end` 的
generation 必须整体丢弃. `SandboxView.ID` 是节点本地身份; 保持身份的 import
或 cluster re-place 改变本地 ID 时, `SandboxView.StableID` 保持不变.

同一对象实现可选 `APIWrapper` 和 `SandboxHook`. Wrapper 添加
`/private/extension/health`, 未匹配请求交给 canonical handler. Hook 添加示例
metadata, 并在 core 最终规范化之前提高过短的 Create timeout. 私有路由没有生产
认证, 仅用于展示组装. Wrapper 可以覆盖 canonical route, 因而生产代码需要负责
最终 URL 与认证合同. 必须执行的 cleanup 不依赖 Hook. 包装后的 handler 同时用于
公开 API listener 和 config-socket API fallback; sandbox data 和 CONNECT ingress
仅属于独立 Proxy, 不进入此 App.

不存在动态 plugin loader 或 extension registry. Extension 为 nil 时保持 built-in
行为, 不创建观察 hub 或 watcher goroutine. 信任和一致性合同参见
[`docs/extensions_zh.md`](../../docs/extensions_zh.md).

`config.DecodeConductor` 是环境无关的声明式解析器: 第三方文档不必重复本机 dispatch
executable 或携带加密材料. 替换 Config 时须保留不可变 bootstrap executable,
然后在启动前通过 Runtime 绑定加密材料.

`cfg.Sandbox.Resources.Allocatable.SetMemory("512MiB")` 将 memory 标为显式值;
直接赋指针等价. `InheritMemory()` 恢复省略时的 `256MiB` 默认值及其 capacity
clamp 行为. Config snapshot 和 `Clone` 保留这一差别.

原生计量策略位于 `cfg.Sandbox.Usage`; 例如在 conductor YAML 的 `sandbox.usage`
下声明 `enabled: true`、`sample_interval: 1s`、`flush_interval: 5m`. 公共 config
经 bootstrap 和全部三条原生启动路径传递此策略. Configure hook 后的最终校验
拒绝无效周期, usage 关闭时也一样.

可信私有任务可以在 Start 后读取 `e.host.Stats()`. Core 启动及 provider 装配完成
之前, 读取立即返回 unavailable; 应使用任务 context 重试, 不在 Start 内等待. 使用精确 `SandboxView.ID`
和可取消 context, 在 Watch callback 之外调用:

```go
rows, err := e.host.Stats().ReadStats(ctx, conductor.StatsRequest{
    SandboxIDs: []string{sandboxID},
    Sections: []string{"usage"},
    Usage: conductor.UsageQuery{View: "saved"},
})
// On success, rows[0].Usage is lossless native JSON, including for a paused VM.
```

它与公开 stats API 和可信 telemetry plugin 调用相同的 conductor 领域 Reader.
固定请求数、并发、时间、大小上限和整批错误也适用于进程内调用. StableID 不能替代
SandboxID. 保留原始 JSON 或解码为原生整数类型; 不经过通用 float64 map 舍入,
也不要通过未认证 wrapper 暴露此可信 Reader. 参见[原生 usage](../../docs/node-usage_zh.md).
