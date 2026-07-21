# Cluster Placer

`cluster-ctl placer` 是无执行权威的独立策略服务。它读取 Provider/Policy 和 Registry 提供的已提交 Node
Catalog，返回 immutable dispatch intent 与最多四个候选；Registry 协调最终 Probe、P2C 和 Binding
commit，Admission 决策与资源状态由目标节点持久化。

## 1. Boundary

Placer 负责：

```text
GROUP Provider/Policy lookup
caller-independent Sandbox/Build input normalization
typed dispatch spec construction
catalog selector and capability filtering
optional shuffle sharding
RandomN candidate selection
```

Placer 不负责：

```text
Route/Build persistence
node session ownership
Admission or resource reservation
execution import/recovery
caller credential storage
final P2C choice
```

Placer 是可重试的纯计划步骤。Registry 必须先把其结果提交到 STARTING，之后才允许 dispatch；Leader
切换后从共识状态恢复，不能重新调用 Provider 并改写已提交 intent。

## 2. Input and output

Sandbox 输入包含：

```text
group, route_key, sandbox_id
requested config and timeout
normalized resource demand
target runtime digest
```

Build 输入包含：

```text
group, build_id, template_id, profile
names, aliases, metadata, builder options
normalized Build demand
target runtime digest
```

输出固定为：

```text
1..4 PlacementCandidate
canonical normalized demand
typed SandboxDispatchSpecV1 or BuildDispatchSpecV1
provider/policy version
```

候选池不足四个时返回实际可用的 1..4 个；零候选返回明确失败。不得用重复候选或已过滤节点填满数量。

## 3. Typed dispatch specs

Sandbox spec 固定 effective template reference、AuthKey/ManifestKey fingerprints、resolved config、随机
execution access capability、target port、timeout、runtime digest 与完整 node request envelope。Build registration
spec 固定 template handle、AuthKey/ManifestKey fingerprints、profile、names/aliases、注册 metadata、正的
CPU/memory ceiling、builder defaults、runtime digest 与完整 envelope。trigger steps、base image/template 与 pull
credential 在注册后经 Router 直接送到 bound node，不进入 Registry/Placer intent；node 拒绝 Trigger 放大注册
时 CPU/memory ceiling。

两类 spec 使用严格 JSON 解码、版本字段和大小上限。system-owned ExecutionBinding metadata key 不允许由
Provider、caller 或配置输入提供。Registry 共识状态再次解析 typed spec；仅 digest 自洽但类型错误的 intent
不会进入 STARTING。

## 4. Catalog and filtering

Node Catalog 来自 System Group 已提交 enrollment，而不是 Session Directory 或 memberlist。稳定字段包括：

```text
node_id
labels and capabilities
failure domain
runtime digest
load-model version
Sandbox slot capacity
Build slot/CPU/memory/storage capacity
draining
catalog version
```

Placer 依次应用：

1. execution-kind capability。
2. target runtime digest。
3. Provider node selector。
4. draining 和静态容量约束。
5. optional shuffle-sharding rule。

动态可用资源不在 Placer 里缓存为权威；它由当前 Holder 的 request-time Probe 判断。

Sandbox 内存 demand 不是 Router 猜测值。Placer 先合并 group `sandbox_config` 与 caller override，再从有效
`kuasar-sandbox.resource` 派生 floor/startup；allocatable 缺省为 capacity，startup 缺省为 allocatable。
未声明的内存保持 unknown，启用 resource controller 的节点会拒绝该 Probe。派生后的 canonical demand
随 dispatch intent 持久化，并由 node-local Admission 原样校验和使用。

## 5. RandomN, Probe and P2C

Placer 使用稳定随机源从过滤结果中返回四个不同候选。Registry 对 fresh pair 发起 Probe，Probe 必须验证：

```text
expected NodeEpoch and SessionSeq
data endpoint and runtime digest
load-model version
sample sequence and age
Sandbox startup memory / slot or Build resource demand
node-local Admission token availability
```

过期、tuple 不匹配或模型版本不匹配统一视为 `STALE`，不能参与 P2C。两个可用 Probe 按 rate PPM 选择较低
者；相同 rate 使用显式随机 tie-breaker。`WOULD_QUEUE` 可作为较低优先级候选，`REJECT` 只有在 node 对该
exact immutable request 提供 definitive no-side-effect 结果后才允许尝试下一候选。

## 6. Shuffle sharding

配置项：

```yaml
placement:
  candidates: 4
  shuffle_sharding:
    - selector: { pool: gpu }
      shard_by: zone
      n: 2
```

规则只缩小静态候选范围，不改变 Route shard、Raft membership 或 execution identity。`n` 必须为正，
selector 和 `shard_by` 必须完整。规则变化通过 provider/policy version 进入新的未 dispatch intent；不得修改
已提交 STARTING。

## 7. Provider sources

当前 final Placer 支持显式 `type: file` 的 GROUP source：

```yaml
group_sources:
  - source_id: groups-primary
    source_type: file
    path: /var/lib/kuasar/groups
```

每个 source ID 唯一，path 必须为绝对路径。Group 记录提供独立 AuthKey/ManifestKey、`template_ref` 默认值与
`allow_template_override` 策略。Provider 负责 caller/GROUP authentication 与期望 key lease；Registry 不复制
AuthCatalog 或秘密材料。正常运行没有 Registry execution importer 或 WATCH_LIST 权威路径。

## 8. Failure behavior

Placer timeout 或不可用发生在 selection commit 前，因此可安全重试。selection 已提交后，Placer 不可用不
影响同一 execution 的继续 dispatch。Registry 不能因 Placer、Holder 或 node 暂时不可见而构造 replacement。

Placer 响应必须有界，Registry 同时限制 aggregate Sandbox/Build launch rate。Placement snapshot 最大周期
为 500ms，并在 node Admission/launch/completion 变化时主动发送；这保证 Probe 使用新鲜软状态，但不把软
状态提升为 execution authority。

## 9. Configuration

```yaml
placer:
  id: placer-1
  listen: ":7800"
  tls: { cert: /etc/kuasar/tls/placer.crt, key: /etc/kuasar/tls/placer.key, ca: /etc/kuasar/tls/ca.crt }

group_sources:
  - { source_id: groups-primary, source_type: file, path: /var/lib/kuasar/groups }

placement:
  candidates: 4
```

Registry 通过 Registry Layout 外的显式 `placers.endpoints` mTLS 列表调用 Placer。候选数在 final protocol 中固定为
4；修改协议常量需要重新审批 RFC。
