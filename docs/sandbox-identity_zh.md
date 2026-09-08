[English](sandbox-identity.md) | [简体中文](sandbox-identity_zh.md)

# Create 时指定沙箱身份

直连 conductor 的 `POST /sandboxes` 可以指定节点本地 SandboxID，并可选指定独立的
StableID。这是创建时的配置输入，不是身份预约接口，也不提供幂等结果重放。

## 两种等价输入

使用现有 namespaced metadata 入口，其中值是一个 JSON **字符串**：

```json
{
  "templateID": "<canonical-template-id-or-alias>",
  "timeout": 300,
  "metadata": {
    "kuasar-sandbox.identity": "{\"id\":\"worker-42-instance-3\",\"stable_id\":\"worker-42\"}",
    "application": "worker"
  }
}
```

也可以通过配置 Header 提供相同的身份对象：

```http
X-Kuasar-Sandbox-Identity: {"id":"worker-42-instance-3","stable_id":"worker-42"}
```

不增加 Create body 顶层字段。SDK 调用方可以使用已有的 `metadata` 参数。Header
**整对象覆盖** metadata 中的身份配置，不逐字段合并。两个显式输入层都必须合法，
因此合法 Header 不能掩盖非法身份 metadata。

例如，metadata 为 `{"id":"body-instance","stable_id":"body-stable"}`，Header 为
`{"id":"header-instance"}` 时，本地 ID 和有效 StableID 都是 `header-instance`。
Header `{}` 清除两个低优先级选择，改用正常默认值。

## 字段契约

| 输入 | 节点本地 SandboxID | 有效 StableID |
|---|---|---|
| 未提供身份或 `{}` | 新生成的 UUIDv7 | 本地 ID |
| 仅 `id` | 指定的 `id` | 本地 ID |
| 仅 `stable_id` | 新生成的 UUIDv7 | 指定的 `stable_id` |
| 两个字段 | 指定的 `id` | 指定的 `stable_id` |

字段空字符串表示未指定。未指定 StableID 时，其可选持久化字段仍为空，
由 `Sandbox.StableID()` 回落为本地 ID。

本次新增 Create 配置的两个非空字段都使用现有 `ValidLocalSandboxID` 契约：
1..57 字节，只允许小写 ASCII 字母、数字和连字符，首尾必须是字母或数字。
精确模式是 `^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$`。大写、点、斜线、空白、NUL
和超长值直接拒绝，不做自动转换。这不改变历史迁移 token 或可信集群 StableID 的契约。

对象只接受 `id` 和 `stable_id`。空 Header、非对象 JSON、null（包括字段值 null）、
重复 Header、重复或未知 JSON 字段、尾随 JSON、非法 ID 字符串和非字符串字段返回 400。

Create 响应的 `sandboxID` 仍是**本地 ID**。节点生命周期 URL、Proxy Host/Header
寻址、本地路径和路由表主键都使用该 ID。StableID 不是查询别名，也没有独立查询接口。
只指定 StableID 而不指定 `id`，不能按本地 ID 去重，也不能预知本地访问 URL。
两个 ID 不要求具有字符串上的派生关系；`-g0` 不是 conductor 对输入规定的格式。

## 适用范围与所有权

身份只针对本次 Create 解释一次，随后从传给 Hook 和存入 Sandbox 的 metadata 中移除。
之后以专用的 `Sandbox.ID`、`StableIDValue` 字段为事实源。普通应用 metadata 保留。
模板、组默认配置和 placement 默认配置不能提供继承身份；`MergeCreateMetadata`
只允许当前请求显式提供这一命名空间。

指定 ID 不授予权限，也不产生集群归属。即使 conductor 已连接 Registry，直连 Create
仍是 `OriginDirect`，且 `Cluster == nil`。现有凭据对 allowlist 与 APIKey 所有权检查不变。

Create Hook 从受保护的操作 envelope 看到最终本地 ID，不能修改它，也不能向可变
metadata 重新加入 `kuasar-sandbox.identity`。其他受支持请求字段的修改仍会重新校验。
最终 StableID 必须在生成沙箱凭据前确定。

默认 ServiceSecret 从 APISecret 和 StableID 派生，forward token 绑定该 StableID。
因此，使用相同默认凭据输入复用 StableID，不会自动使所有旧凭据失效。
StableID 是参与认证的稳定身份，不是可以随意重复的显示标签。

| 入口 | 身份配置处理 |
|---|---|
| 直连 conductor Create | 接受 metadata 和 Header |
| 单机或集群 Build 注册 | 显式身份返回 400；不保存到 Build 或模板 |
| 集群 Router 公共 Create | 拒绝两种输入；身份分配权仍属于 Registry |
| 可信 node-link Create | 使用已有 typed `SID` 和集群 StableID 字段；拒绝 `Config` 中的身份命名空间 |
| Import / Connect | 保持现有目标 ID 和迁移 token 语义；不增加身份覆盖能力 |

## 冲突、重试和删除

Create 保持 insert-only。本地 ID 已有任意状态的保留记录，或仍有活动 launch owner
时，都不允许第二次创建。请求到达身份冲突阶段后，两种冲突统一归类为 409，沿用现有
message 响应形态：

```json
{"message":"sandbox already exists"}
```

失败方不能替换既有记录、取消成功方、清理其资源或返回其凭据。请求仍可能更早因凭据、
配置错误或 Proxy 准入不可用而失败；冲突检测不改变这些检查顺序。

409 不是成功重放，也不说明既有对象与本次请求的定义相同或由本次请求创建。
响应丢失后，调用方可以使用已知本地 ID 和已有凭据查询对象并按状态处理；服务端不保存
Create 结果供重放。这是单节点冲突保护，不是跨节点的全局唯一性保证。

创建失败可能保留 `dead` 行，例如 Proxy 路由屏障在 durable starting 准入后失败。
此时 ID 仍被占用。发出删除请求不等于清理已完成；只有现有 finalizer/retention 真正
释放数据库行和活动 launch ownership 后才能复用。Create 不隐式恢复、替换或清理
既有对象。列表游标仍按本地 ID 排序，因此调用方指定的 ID 不保证按创建时间排列。

## 共享实现

直连 API 与可信 node-link 适配器保留各自的认证、模板所有权、timeout 和 MMDS 边界，
共用纯配置规范化、最终 Sandbox 构造和 `acceptFreshLaunch`。集群预检查返回独立的
解析结果，不再改写 `Command.Config`。单机模板别名规则与集群 canonical 模板限制
仍然不同；不增加公开 Hook 或 wire 协议。

共同准入仍按顺序取得 launch ownership、原子插入 Sandbox 与可选 MMDS 值、发布
starting、等待 Proxy 路由应用屏障，再调度异步启动。单机 HTTP 201 表示 durable
acceptance，不表示 guest 已就绪。node-link 在同一边界返回 accepted ACK；
`CreateCluster` 仍只是等待本次具体 attempt 的同步包装。

周边契约参见[节点规范](node_zh.md)、[集群 Router](cluster-router_zh.md)与
[扩展指南（英文）](extensions.md)。
