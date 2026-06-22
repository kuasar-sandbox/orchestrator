# cluster-scaler — 放置调度器与 shuffle-sharding

scaler 是 cluster(cluster.md)的放置**调度器**:被 registry 调用时给出 `PlaceSandbox` /
`PlaceBuild` 的**节点建议**,并维护 sandbox-group ↔ node 的 shuffle-sharding 分片(把分片结果以
标签选择器 patch 回 group 的有效 nodeSelectors)。

scaler **只做调度,不碰别的**:**不联系节点**(无 node-link / 命令)、**不分发密钥**(密钥分发是
registry 的事,cluster.md §7.6)、**不处理沙箱生命周期**(pause / 提升只由节点侧或控制面显式发起)、
**无 drain**。它订阅 registry 的节点 / group 状态(维护本地视图),建议时**本地计算**;registry
**原子提交**预留(调度器 / 提交分层,cluster.md §4.3)——故 scaler 不在请求路径持锁、可独立扩展。

## 1. 概述

### 1.1 业务问题

新沙箱 / 构建该落在哪个节点,不能随机:

- **标签约束 + 爆炸半径**:一个 sandbox-group 只允许调度到特定 zone / pool / slot 的节点;更进一步,
  把每个 group 钉到一个**小的确定性子集**,使单个 group / 节点故障的影响面受限(shuffle-sharding,
  §4.4)。这个小子集还顺带带来**缓存局部性**(快照 chunk 常热,加速恢复,cluster.md §9)。
- **水位均衡**:在符合约束的节点里避开高水位节点,否则放置惊群把热点越堆越高。
- **构建资源**:构建任务吃 CPU / 内存 / 存储,与沙箱内存维度不同;需按**每节点 build 资源容量**调度。

scaler 解决:nodeSelectors + shuffle-sharding 选址(§4.2 / §4.4),P2C 均衡(§4.2),build 资源感知
调度(§4.5)。

### 1.2 设计原则

1. **只建议,不提交、不联系节点**:scaler 给节点建议,registry 提交(CAS)并下发命令。scaler 不
   持节点连接、不推密钥、不动沙箱。
2. **本地视图 + 本地计算**:订阅 registry 的节点 / group 状态维护本地视图;建议时本地算,不每次
   全表扫 / 不远程 fan-out——故建议快(cluster.md §9 冷路径)。
3. **P2C 而非全局最优**:无放回抽两个不同候选取较空者——O(1)、去相关、抗惊群(无放回避免 2 节点集时
   ~25% 抽中同一更热节点)。
4. **关联变化不迁移在跑沙箱**:nodeSelectors / shuffle 分片的变化**只影响新建沙箱落点,已启动的
   沙箱不动**(§4.3 / §4.4)。

### 1.3 边界与依赖

- 读(订阅):registry 节点注册表(labels / watermark / build_capacity / liveness)、sandbox-group
  配置(nodeSelectors / shuffle 标签,经 `GroupPlacementProvider`,cluster.md §6)。
- 写:经 registry 把 shuffle 分片选择器 patch 回 group 有效 nodeSelectors(registry-store overlay,
  §4.4);给出 `Place*` 建议(cluster.md §4.3 / §7)。
- 复用:`maglev.LocateN`(sandbox-accelerator `pkg/maglev`,§4.4)。
- **不**:不连节点、不推密钥、不碰生命周期、无 drain。

## 2. 命令行接口

```
cluster-ctl scaler [--config /etc/cluster-ctl/config.yaml] [--registry <addr>]
```

经 `--registry` 连 registry 的 op 接口(UDS 本机 / mTLS 远端,cluster.md §4.2 / §5.2)。无独立
listen——scaler 是纯调度器(订阅 + 被调建议方)。

## 3. 配置

`scaler` 配置组(完整表见 cluster.md §3):

| 字段 | 默认 | 说明 |
|---|---|---|
| `scaler.registry` | 空 | registry op 端点(scaler 拨入);空 = 同机 `op.listen` |
| `scaler.place_candidates` | `2` | P2C 抽样候选数(§4.2) |
| `scaler.zone_admit_max` | `yellow` | 放置只选水位区 ≤ 此的节点(red / critical 排除,§4.2) |
| `scaler.shuffle_sharding` | 空 | shuffle-sharding 规则列表(§4.4);空 = 仅用静态 nodeSelectors |

`shuffle_sharding` 规则形态:

```yaml
scaler:
  shuffle_sharding:
    - selector: {random-two-slots}   # 命中: nodeSelectors 含此标签的 group + labels 含此标签的 node
      shard_by: slot                  # 以 node 的 slot 标签值分桶, 每个 slot 值 = 一个分片
      n: 2                            # 每个匹配 group 分到 2 个分片(slot)
```

(密钥租约时长 / 续租是 registry 的密钥分发配置,见 cluster.md §7.6——scaler 不分发密钥。)

## 4. 放置建议

registry 在 `Reserve` 冷 / 迁移路径**同步调用** scaler(cluster.md §4.3 / §7);scaler 本地算出
建议节点回给 registry,registry 原子提交。

### 4.1 节点视图

scaler 订阅 registry 节点注册表(labels、watermark、build_capacity / build_alloc、liveness),维护
本地视图;group 的有效 nodeSelectors = 静态(`GroupPlacementProvider`)+ scaler 自管的 shuffle 分片
选择器(§4.4)。建议纯本地计算,无远程 fan-out。

### 4.2 PlaceSandbox

`PlaceSandbox(group, route_key) → node_id 建议`:

```
eligible = { node :
    matchSelectors(node.labels, group.有效 nodeSelectors)   // 静态 ∪ shuffle 分片(§4.4)
  ∧ node alive(LastHeartbeatUnix 未超 node_dead_after) ∧ ¬ node.draining
  ∧ node.zone ≤ scaler.zone_admit_max                     // red / critical 排除
  ∧ runtimeCompatible(node.runtime_digest, target)        // target 非空时(如迁移快照 runtime)
}
node_id = argmin( sample(eligible, place_candidates), load )   // P2C 无放回抽 place_candidates 个,取 load 较小者
```

- **负载信号**:首选 `allocated / pool` 水位(经心跳),回退 `counts / capacity` headroom,再回退裸 counts。
- **node alive**:scaler 视图里 `LastHeartbeatUnix` 超 `node_dead_after` 的节点排除(失联未扫前)。
- `eligible` 为空 → 建议失败 → `Reserve` 失败 → router 转 503(cluster.md §7.4)。
- scaler 只**建议**;registry 提交时 CAS(并发 / 视图滞后 / CAS 冲突则重问一次,cluster.md §4.3)。

### 4.3 nodeSelectors 与爆炸半径

`group.nodeSelectors` 是**选择器列表**,节点命中**任一**即合格(列表 OR);单选择器内标签等式
**全部**成立(AND):

```
nodeSelectors: [{slot: c01-s03}, {slot: c01-s04}]   → slot=c01-s03 或 c01-s04 的节点
nodeSelectors: [{zone: east1a, pool: c01}]          → zone=east1a 且 pool=c01 的节点
```

节点标签由通道 `register` 上报(`{zone, pool, slot, node}`)。nodeSelectors 主要**控制爆炸半径**;
**其变化只影响新建沙箱落点,不迁移已启动沙箱**——已跑沙箱由节点承载直至自然暂停 / 迁移。

### 4.4 shuffle-sharding(选择器 patch,maglev)

静态 nodeSelectors 给粗粒度可落集;`scaler.shuffle_sharding` 规则在其内把每个 group 钉到**确定性
随机的 n 个分片**——**仅按标签分组均匀分布,不按负载**。对每条规则:取 labels 含 `selector` 的节点,
按 `shard_by` 标签值分桶得分片集 `S`;对 nodeSelectors 含 `selector` 的每个 group:

```
shards = maglev.LocateN(group_path, S, n)        // n 个确定性分片(slot)
scaler 把 {shard_by ∈ shards} 选择器 patch 回 group 的有效 nodeSelectors(registry-store overlay)
```

放置(§4.2)直接读有效 nodeSelectors——shuffle 不是 Place 的额外输入,而是 scaler **维护进选择器**。

- **算法复用**:`maglev.LocateN(key, members, n)` 由 sandbox-accelerator 导出(`pkg/maglev`,与其
  缓存分片定位同款 Maglev 一致性哈希);cluster import。一致性哈希使**增删 slot 时分片重映射最小**。
- **自管选择器环**:scaler 跑独立环,**在 slot 增加 / 隔离 / 下线时**重算 `maglev.LocateN` 并 patch
  选择器(churn 最小)。隔离 / 下线一个 slot → 该 slot 退出 `S` → 命中它的 group 重分到其他 slot
  (仅影响新建,§4.3 不变量)。
- **三重收益**:爆炸半径(group 只触 n 分片)、均衡分布(maglev 均匀)、**缓存局部性**(group 的
  快照 chunk / 本机 bundle 在固定 n 节点上常热 → 恢复免远程拉取,cluster.md §9)。

### 4.5 PlaceBuild(资源感知)

`PlaceBuild(group, resources{cpu,mem,storage}) → node_id 建议`:在 build 资源余量满足的节点间 P2C。
scaler 据视图过滤 build 余量、按利用率 P2C 建议;registry 提交时按 BuildStore 已 RESERVED 之和**权威
校验**该节点余量(超订 re-Place,cluster.md §7.5)。

```
eligible = { node :
    matchSelectors(node.labels, group.有效 nodeSelectors) ∧ node alive ∧ ¬ draining
  ∧ node.build_capacity − node.build_alloc ≥ resources    // build 资源 headroom
}
node_id = argmin( sample(eligible, place_candidates), build_alloc/build_capacity )
```

- **每节点 build 资源容量 `{cpu, mem, storage}`**:源自 builder slice 配置(`builder.cpu_quota` /
  `memory_max` + scratch / `diff_template` 存储预算,node.md §12),经通道 `register`/`heartbeat`
  上报;与沙箱内存水位**独立**(构建走单独资源池)。
- **build 声明所需 `resources`**(不指定取 builder 默认配置);registry 提交 BuildStore 时 **RESERVED
  即占用**该节点 `build_alloc`,ready/error/gone 释放(cluster.md §7.5)。scaler 订阅 `build_alloc`
  维护视图。
- 节点最终 admission 仍权威(build 沙箱启动被拒 → registry re-Place)。

## 5. 可靠性

| 故障 | 影响 | 自愈 |
|---|---|---|
| scaler 崩溃 / 重启 | 冷放置停滞(建议无人给) | 无状态,重启重连 registry 增量重同步视图(cluster.md §5.3)续跑;**热路径不受影响**(router 缓存);park 超时 |
| 节点集变化触发分片重算 | 新建沙箱落点变 | maglev churn 最小;**已跑沙箱不动**(§4.3 / §4.4 不变量) |
| 建议节点已满(视图滞后) | 提交 CAS 冲突 | registry 重问一次(cluster.md §4.3) |
| `eligible` 为空 | 该次 Reserve 失败 | router 转 503;扩容 / 放宽 nodeSelectors / 调 shuffle n |

scaler 无持久状态:shuffle 选择器 patch 落 registry-store(scaler 重启重算并对账)。

## 6. 性能

- **建议快**:筛 eligible O(分配节点数)+ P2C O(1);本地视图,无远程 fan-out;shuffle 分片以 maglev
  表查 O(1)。冷路径建议 = scaler 本地计算 + registry↔scaler 一次往返(共置 UDS,cluster.md §9)。
- **抗惊群**:P2C 摊平放置负载;无全局锁 / 排序。
- **可分片**:大规模按 group 分片 scaler(每片维护相关节点视图);调度 / 提交分层使之干净
  (cluster.md §4.3)。

## 7. See Also

- [cluster.md](cluster.md) §4.3 —— 调度器 / 提交分层;§7 —— Reserve 调 `Place*` 的状态机;§7.5 ——
  ReserveBuild;§7.6 —— 密钥分发(registry owns,非 scaler);§9 —— 快照恢复 + shuffle 缓存局部性。
- [cluster-router.md](cluster-router.md) —— Place 的上游驱动者(经 Reserve)。
- [node.md](node.md) §12 —— 三阶段构建与 builder slice(build 资源容量来源);§8 —— 节点侧
  pause / 快照(scaler 不发起)。
- [node-resource.md](node-resource.md) —— 节点水位(zone / allocated / pool)语义,P2C 负载信号来源。
- `sandbox-accelerator/docs/cache.md` —— Maglev 一致性哈希(`pkg/maglev` 导出,shuffle-sharding 复用)。
