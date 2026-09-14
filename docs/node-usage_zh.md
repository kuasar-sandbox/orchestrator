[English](node-usage.md) | [简体中文](node-usage_zh.md)

# 原生沙箱 usage 与本机 stats 读取

## 1. 概述

Conductor 在 `/stats/resource` 和 `/stats/traffic` 之外发布
`GET /sandboxes/{SandboxID}/stats/usage`. Usage 读取 sandboxer 已有的原生生命周期
计量. Resource stats 描述当前生效规格和 VMM 观测; traffic stats 描述既有 Proxy
ingress 观测. 这些接口独立于 telemetry 及其
`/sandboxes/{SandboxID}/metrics` 历史查询.

每次公开读取均认证 API key 并核对精确 SandboxID 的归属. StableID 只是关联标签,
不能作为查询别名. Conductor 定位当前对象, 调用与 `sandbox-ctl usage` 共用的
sandboxer [`pkg/usagereader.Read`](https://github.com/kuasar-sandbox/sandboxer/blob/main/pkg/usagereader/read.go).
它不复制原生 codec、恢复、锁或历史读取算法. Telemetry 只经 conductor 获取原生
section, 不打开沙箱 `ctl.sock` 或 `.usage` 文件.

## 2. CLI 与 HTTP

```sh
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage"
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage?view=saved"
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage?view=history&cursor=0&limit=10"
```

| 参数 | 合同 |
|---|---|
| `view` | `current` (默认)、`saved` 或 `history` |
| `cursor` | 仅 history; 非负十进制字节位置, 默认 `0`. 原样保留返回的 `next_cursor` 字符串, 不经过浮点转换 |
| `limit` | 仅 history; 整数 `1`–`100`, 默认 `10`. 超出原生 1 MiB 页面上限时应减小 |

未知、重复、空值或格式错误的参数返回 400. 不提供 `/usage` 别名. 响应包含
`Cache-Control: no-store`. 普通认证错误保持不变; 对象不存在或 SandboxID 不属于
调用方时返回 404. 文件缺失、活动 writer 锁冲突、owner 不可用、owner 身份无效、
读取失败、超时和并发 runtime 替换返回 503. 合法 View 中的原生
`read_error`/`save_error` 仍完整保留, 不丢弃或转换为零.

`current` 返回原生 View; `saved` 返回省略 `live` 的同一 View. 例如 usage 关闭且
没有 saved record 的 owner 可以返回:

```json
{"enabled":false,"saved_end":"0","saving":false,"unknown_tail":false}
```

这不是测得零用量的记录, 也不能证明其他位置不存在历史用量. `history` 返回原生
Record 和 `next_cursor`; 验证为空的文件返回
`{"records":[],"next_cursor":"0"}`. 原生 counter、大小、时间戳、位置和 128-bit
积分保持十进制字符串表达. Coverage、completeness、status、`live`、`saved`、
`saving`、`unknown_tail`、错误和原生 `run_epoch` 元数据均保留. 不向 stats 模型
加入 orchestrator RunID.

## 3. 节点配置

```yaml
sandbox:
  usage:
    enabled: true
    sample_interval: 1s
    flush_interval: 5m
```

默认值分别为 false、`1s` 和 `5m`. 周期必须是正的 Go duration, flush 不小于
sample, sample 的两倍不得超出原生有符号 duration 范围. 关闭时也执行校验.
显式 null、错误类型、未知/重复字段和 YAML merge key 均拒绝. Clone、配置输出、
外部 conductor bootstrap 和 Configure hook 后的最终校验保留同一策略.

Conductor 将节点策略下发到 image cold start、`run --from` 和 `run --restore`.
Usage 是宿主策略, 不进入 portable E/S artifact; 恢复使用当前节点策略. 启用
telemetry 不会启用 usage, 停止 telemetry 不会停止原生计量. `flush_interval`
调度原生 append, 不触发 fsync 或设备缓存 flush. 参见经实际解析器校验的
[conductor 部署示例](../deploy/conductor.example.yaml).

## 4. 在线、saved 与离线 ownership

运行中的 owner 对 live 状态、自己接纳的 saved 基线和已确认 history 具有权威.
仅 owner socket 不存在或拒绝连接时允许离线回退. 成功连接之后, 协议错误、EOF、
owner 错误、取消或超时均不能回退到看似可读的文件. 每个响应都证明精确 owner
SandboxID, 包括空/关闭的 View 和空 history. Reader 与 sandboxer owner 必须使用
该 ctl envelope 的兼容版本.

Paused 对象可以在不唤醒的情况下读取. 离线读取取得原生非阻塞共享锁、验证普通
文件, 然后使用 Recover 和 ReadHistory. 活动 writer 阻止调用方绕过其接纳的
saved 边界. 离线 recovery 找到完整存活记录, 不证明前一 writer 已确认该 append,
也不证明数据经历掉电后仍可靠保存.

读取不采样、不积分、不 append、不 flush、不通过写入进行 recovery、不 resize,
也不调用 guest. 它不会将尚未保存的 live 值保存, 或消除不确定尾部. 原生 memory
积分 coverage 与 CPU 来源 reset 保持原有语义. 重复读取同一累计记录得到的是同一
总量, 不是新增区间消耗. 浮点 telemetry 投影不能恢复无损原生账本, 也不能证明
不完整尾部的完整性. 不新增 usage 持久化、sync、ACK、finalizer、删除后保留或
生命周期行为.

## 5. 可信 conductor 读取面

静态链接的 conductor extension 使用 `Host.Stats().ReadStats(ctx, StatsRequest)`.
对应的内部 `POST /internal/plugin/telemetry/stats` 复用现有
`paths.config_socket`, 对完整 RouteEntry 流已发现的 SandboxID 显式选择原生
section:

```json
{"sandboxIDs":["exact-sid"],"sections":["usage"],"usage":{"view":"saved"}}
```

按请求顺序返回的结果包含 `sandboxID`、可选 `stableID` 和每个已选 section 的原生
body. 不返回凭据、路径或 RunID. 任一已选 section 缺失或读取失败都会使整批失败,
不会以旧样本或零补齐. Paused 对象无需 active guest 即可提供 usage; 若同批还请求
不可用的 resource section, 则该批可能失败.

能够连接 UDS 不自动获得此操作权限. 服务端验证真实 SO_PEERCRED PID、既有可选
`plugin_pidfile` 白名单, 以及固定 Plugin ID `telemetry` 当前已 ready 的有效注册.
Stats 连接必须属于已注册进程. Lease 替换或断开会取消正在执行的读取. 注册不要求
暴露 query UDS, 因而 write-only telemetry 仍能消费原生 stats. 未配置白名单时,
mode 0600 保持既有可信本机 plugin 边界; 普通 API 请求仍需要 API 认证和归属校验.
不收集租户 API key, 不引入新的权限模型或 socket.

## 6. 可靠性与性能

每批包含 1–64 个不重复的非空 SandboxID, 并从 `resource`、`traffic`、`usage` 选择
1–3 个不重复 section. 仅选中 usage 时接受 usage 参数. 请求 body 上限 64 KiB,
响应上限 4 MiB, 总超时 5 秒; 底层原生 Reader 保留各自更小的上限. 所有 batch
调用之间最多并发读取八个对象. 父 context 取消和 lease 撤销会传递到底层读取,
并释放槽位和连接. 无效请求返回 400, 对象不存在返回 404, 生命周期状态冲突返回
409, 读取不可用返回 503, 与公开 API 共用同一领域错误体系.

这些值按需读取, 不进入 RouteEntry 订阅流. 公开读取、本机 batch 和进程内
extension 共用 conductor 领域实现及最终当前绑定校验. Stats 和 telemetry 均不
创建 Create/Resume barrier. 密度 benchmark 测量有界读取及 FD/goroutine 行为,
不设置机器相关门槛; 原生采样和 history append 算法保持不变.

```sh
go test ./internal/orch -run '^$' -bench '^BenchmarkNativeUsageBatch$' -benchmem -count=3
```

此 benchmark 在每批 1/16/64 个对象下读取真实 SQLite 对象和原生 saved 文件,
报告分配量及保留 FD/goroutine 的变化; 不代表 guest 采样或远端导出吞吐.

## 7. See Also

- [Node API 与本机平面](node_zh.md)
- [节点资源](node-resource_zh.md)
- [Telemetry](telemetry_zh.md)
- [Conductor extension](extensions_zh.md)
- [Sandboxer 原生 usage](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/usage_zh.md)
