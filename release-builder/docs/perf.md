# perf — 性能基线与调优

记录系统各路径(cache、sandbox、密度控制)的实测指标、可复现 harness、调优
杠杆与已采纳/已验证的优化决策。

测试环境基线:嵌套 KVM 开发环境(nested-KVM/WSL2 类)。此类环境的 page-fault
路径有 ~2× 抖动,裸金属 native Linux + NVMe 数字应明显更稳定且更优。下列数字按
技术事实记录,绝对量级以裸金属复测为准。

测量入口(本仓 umbrella 聚合目标):

```bash
make bench           # Go 微基准(各 Go 子仓)
make perf            # 系统级 harness 全套:accelerator perf-cache + perf-sandbox/-manifest/-density
make perf-sandbox
make perf-density
```

`perf-cache` 由 `make perf` 内部委派给 `accelerator`(本仓无同名目标);
单独跑用 `make -C accelerator perf-cache`。

## 1. cache 子系统

### 1.1 基线与目标

**场景**:`BENCH_SCENARIO=tiered-shard-l2 PREFILL=500 VALUE=512KB DURATION=30s`,
6 server core / 2 client core,loopback TCP。

**端到端(c=1,3 次中位数,value=512 KiB)**:

| 指标 | 优化前 | 目标 | 优化后 |
|---|---:|---:|---:|
| p50 | 1083 µs | < 500 µs | 773 µs |
| p99 | 2760 µs | — | 1486 µs |
| p99.9 | 13325 µs | < 5 ms | 2730 µs ✓ |
| throughput | 832 op/s | — | 1228 op/s |

**并发扫描(最终构建)**:

| conc | throughput | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 1 | 1,235 op/s | 770 µs | 1.47 ms | 2.86 ms |
| 2 | 1,949 op/s | 972 µs | 1.96 ms | 3.52 ms |
| 4 | 2,604 op/s | 1.42 ms | 3.52 ms | 7.12 ms |

p99.9 目标达成。p50 离 500 µs 仍有 ~270 µs 差距,来源是 loopback TCP 131 KiB
单次 RTT 的物理下限(150–500 µs);要进一步降需要改 wire 传输层(UDS / 共享
内存 / pipeline),属传输层重设计。

口径说明:此处 < 5 ms 是 cache-ctl 单进程 loopback 压测的 p99.9 目标;L2 集群
对外读尾延迟的设计目标为 < 4 ms(见 cache.md 与 kuasar-sandbox.md §7.2)。

### 1.2 已采纳的优化(按 p50 改善顺序)

| 步 | 改动 | 评估手段 |
|---|---|---|
| 1 | `cache.NewPool(512<<10)` 透传给 EC tier + bench client,替代 `DefaultPool` | heap `alloc_space -top`:`defaultPool.Alloc` 49.75% / 16.6 GB → 移除 |
| 2 | EC.Get 的 Decode 使用 sync.Pool 复用 ~1 MiB joined buffer(被 7 取代) | heap:`ec.encoder.Decode` 71% → 4% |
| 3 | `net.Conn.SetDeadline` 1 syscall 替代 `SetReadDeadline + SetWriteDeadline` 2 syscall | 噪声内略优;不劣化即接受 |
| 4 | `wire.readResponseFast`:header 走 bufio、body 超过 bufio 容量时直接 raw → pool,跳过 bufio 拷贝 | CPU `bufio.Read` 22% → ~0 |
| 5 | 缺失 shard 槽位预填池化 buf,让 `reedsolomon.Reconstruct` 复用 | heap `reedsolomon.AllocAligned` 72% / 4.2 GB → 8.6% |
| 6 | `ec.segmentedBlob` + `cache.Segmenter` 接口:EC.Get 直接返回持有 4 个 shard 引用的多段 blob;`wire.WriteResponse` 用 `net.Buffers` writev 发 hdr+shards,完全跳过 Decode 的 join memcpy | p50 832 → 773,decode 段从 ~80 µs 消失 |

合计:堆分配 5700 MB → ~700 MB(-88%),p99.9 13.3 ms → 2.7 ms,p50 1083 →
773 µs。

### 1.3 已验证不收益的候选

- `ReconstructData` 替代 `Reconstruct`:-17% throughput,library 的 ReconstructData
  在需要重建时路径略慢
- 去掉 cancel watcher goroutine:-10% throughput,server 会白做 1 个 131 KiB
  shard 的 rocks.Get + 回传

### 1.4 观测基础设施

**Daemon pprof 端点**(yaml `pprof_listen` 留空则不启用,生产默认):

```yaml
mode: tiered
listen: 127.0.0.1:7070
health_listen: 127.0.0.1:7071
pprof_listen: 127.0.0.1:6060    # /debug/pprof/{profile,heap,goroutine,...}
```

```bash
curl -sSo cpu.pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30
curl -sSo heap.pprof http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -top -cum cpu.pprof
go tool pprof -alloc_space -top heap.pprof
```

**bench 端 profile flag**:`cache-ctl bench` 支持 `--cpu-profile`、
`--heap-profile`、`--trace`,对应 `pprof.StartCPUProfile`、
`runtime.GC + WriteHeapProfile`、`runtime/trace`。

**bench 端 MemStats 差值**:bench 结尾自动打印 `allocs/op` + `bytes/op` +
`gc-count` + `gc-pause-ms`,配合 heap profile 定位分配热点。

**EC.Get 分阶段计时**:`CACHE_CTL_TIMING=1` 环境变量(启动 daemon 时设置),
每 100 次 EC.Get 采样一次,打到 stderr,含 `locateN` / `fan_out first` /
`fan_out last` / `decode` / `total`。

**bench_cache.sh 自动抓双侧 pprof**:`test/scripts/bench_cache.sh` 每次运行
都会在 `$WORKDIR` 里生成 bench client + bench target 的 cpu.pprof + heap.pprof。
设 `KEEP_WORKDIR=1` 保留;设 `PROFILE_TRACE=1` 额外采 `runtime/trace`。

**微基准**:

- `pkg/cache/ec/ec_bench_test.go`:`BenchmarkDecode_AllHit_512KB`(全命中无
  重建的 decode 上限)
- `pkg/cache/ec/tier_bench_test.go`:`BenchmarkECGet_AllHit_InProcess`
  (mock shard client,剥离 TCP 评估 EC 逻辑开销)
- `pkg/cache/wire/codec_bench_test.go`:`BenchmarkWireCodec_RoundTrip_512KB`
  (loopback 单次往返基线)

```bash
CGO_ENABLED=1 go test -run=^$ -bench=. -benchmem ./pkg/cache/ec/ ./pkg/cache/wire/
```

### 1.5 再优化入口(架构选择,非局部优化)

把 p50 进一步压到 500 µs 以下需要架构层面改造:

1. **Unix domain socket 或共享内存替代 loopback TCP**。当前 43% target CPU
   在 `syscall.Syscall6`(TCP recvmsg),loopback 128 KiB 单程的物理下限约
   100-200 µs,其中相当一部分是 TCP 协议栈
2. **Request pipelining**。c=1 bench 是串行的,pipeline 需要 wire 协议支持
   多请求交错。对 c=1 的单端到端延迟没帮助,但对 throughput 显著
3. **批量 Get API**。多 key 合并成一条 request,减少 per-op 的 wire 开销
4. **fan-out goroutine 池化**。`runtime.mcall/schedule/futex` 合计 ~60% CPU,
   说明 5 个 one-shot goroutine + channel 的调度有可观开销

四条都需要成规模的改造,不应该在不改变 wire 层的前提下继续抠 per-op µs。

## 2. sandbox 子系统

### 2.1 完整 manifest:// 矩阵(N=5 中位数,实测)

四个核心场景 × 两个缓存状态(cold L1 = cache-ctl rocksdb 刚清空;hot L1 = 同
cache-ctl 实例累积)+ snapshot upload 两个 dedup 状态。

**wallclock + uffd**:

| 场景 | wallclock T0→app(median) | uffd faults | uffd batch_avg | 懒加载比 |
|---|---:|---:|---:|---:|
| 冷启动 file:// (基线) | **419 ms** | 65 | 194 | 9.66% |
| 冷启动 manifest:// **cold L1** | **875 ms** | 66 | 195 | 9.82% |
| 冷启动 manifest:// **hot L1** | **547 ms** | 65 | 194 | 9.66% |
| restore manifest:// **cold L1** | **472 ms** | 84 | 106 | 6.81% |
| restore manifest:// **hot L1** | 158 / **904** / 1214 ms | 85 | 105 | 6.82% |

restore hot-L1 的中位数偏高的原因:python TICK counter stdout flush + 嵌套 KVM
vCPU resume 抖动 + RocksDB compaction;min=158ms 反映真正的"chunk 全命中"
路径(< file:// restore baseline 750ms)。

**snapshot --upload**:

| 场景 | wallclock(median) | snapshot chunks(total/dedup) | disk chunks(total/dedup) |
|---|---:|---:|---:|
| no-dedup baseline(每次 fresh sandbox) | **2256 ms** | 76 / 26(34%) | 7 / 0(0%) |
| max-dedup(同 sandbox 重复 snapshot) | **2015 ms** | 76 / 48(63%) | 7 / 7(100%) |

dedup 解读:
- "no-dedup baseline" 5 个 fresh sandbox 顺次启动 → snapshot,后面的 iter 仍能
  从 store 命中前面 iter 写入的 image-段 chunks(同一个 python image),所以
  dedup ≠ 0
- "max-dedup" 同进程持续,snapshot 内容 ≈ 不变,memory 段 dedup 63%、disk
  dedup 100%
- 跨多 sandbox 共享同 image:image 段(代码 / 库 / 文件系统结构)是确定的,
  dedup ≥ 60%

### 2.2 cold L1 vs hot L1 解读

manifest:// 冷启动 **cold L1 → hot L1 ↓328 ms (37%)**:cache-ctl 从空到含
全部 ~80 chunks 后,`Get(chunk)` 不再走 store-ctl origin。差额 ≈ 213 chunks
× ~1.5 ms/chunk 的网络往返 + RocksDB Get,与单机 RocksDB BlockCache 命中延迟
一致。

manifest:// 唯一可量化的"成本"是 cold L1 状态下额外 ~330 ms 启动延迟和
~250 ms restore 延迟。生产部署 cache-ctl 持续运行,这个成本仅命中**首个**
用了某 manifest key 的 sandbox;之后所有同 image 的 sandbox 都走 hot L1。

| 维度 | file:// 路径 | manifest:// 路径 |
|---|---|---|
| 跨 sandbox 共享 | 否 | 是,同 image hash → 同 chunks |
| 跨节点共享 | 否 | 是,所有节点共享 OBS / fs store |
| 节点本地存储 | 必须 | 不必须(cache-ctl tiered 命中即返) |
| restore 不依赖原 host 文件 | 否(必须 `<sha256>.overlay` + `<sid>.snapshot`) | 是,只需 manifest key |
| sandbox snapshot 上传 → 别处 restore | 手工 rsync | 一条 manifest key |

### 2.3 sandbox-ctl 内部断面

| 指标 | `file://` | `manifest://`(cache-ctl tiered:rocksdb L1 + store origin) |
|---|---:|---:|
| **wallclock T0→exit (median)** | **416 ms** | **698 ms** |
| sandbox-ctl 内部 wallclock | 254 ms | 524 ms |
| **uffd faults_absent** | 65 | 66 |
| **uffd batch_avg / batch_max** | 194 / 256 | 194 / 256 |
| **uffd wakes** | **0** | **0** |
| **uffd 懒加载比** | 9.66% (12660 / 131072 pages) | 9.74% (12769 / 131072) |
| pages_zeroed / pages_copied | 12660 / 0 (cold-start) | 12769 / 0 (cold-start) |
| **blk0 reqs / p50 / p99** | 212 / 4.8 µs / 91.7 µs | 212 / 924 µs / 3966 µs |
| blk0 load coverage | 7.02% (8.3 MiB / 117.8 MiB) | 7.02% |
| **Go: numGC / pause_total / total_alloc** | 1 / 0.12 ms / 9.3 MiB | 3 / 0.62 ms / 19.2 MiB |
| Go: goroutines / mallocs | 16 / 6341 | 30 / 11450 |

**冷启动**走 `ZeroSource`,所有 fault 都是 ZEROPAGE,batch_avg ~194 页/次几乎
跑满 1 MiB 上限。guest 实际 fault 只占声明 RAM 的 9.7% —— 懒加载有效,
512 MiB 中 ~50 MiB 才被实际触碰。

**wakes = 0 的语义**(单 uffd 架构):
- handler 没碰到 EEXIST 路径(uffd_C 上 ZEROPAGE/COPY 创建 folio 都首次成功)
- backend 对 backendVA 的访问全部由 kernel shmem 缺页 path 直接装 PTE(folio
  已被 handler 装入 inode),不投 uffd 事件

**GC 压力门槛**:manifest:// 路径的 alloc 受两条规则约束(实施在 `pkg/cache`
与 `pkg/manifest/crypto`):
- 解密走原地 XOR(`DecryptInPlace`),不为每个 chunk 分配新 plaintext 缓冲
- wire 客户端通过 `cache.NewPool` 复用 ciphertext 缓冲区

`make perf-sandbox` 把以下指标做成回归门:任意 PR 引入 `numGC > 8 /
total_alloc > 30 MiB` 应被发现。

### 2.4 Snapshot 本地落盘(基线)

启动 + python TICK 计数 ≥ 5 → snapshot → 落 `<out>/<sid>.snapshot` +
`<out>/<sha256>.overlay`:

| 阶段 | 时间 | 数据量 |
|---|---:|---|
| pause + Quiesce | 3-6 ms | 0 |
| CH dump (config.json + state.json) | 30-50 ms | ~90 KB |
| overlay sparse copy + 跳空洞 hash | ≤ 8 ms | 600 KiB 物理 / 1 GiB 逻辑 |
| memfd sparse copy + ZIP append | 10-20 ms | 47-54 MiB 物理 / 512 MiB 逻辑 |
| **总 dump 时间** | **43-72 ms** | |

输出:`<sid>.snapshot` 逻辑 513 MiB / 物理 48-52 MiB(91% 稀疏);
`<sha256>.overlay` 逻辑 1 GiB / 物理 600 KiB(99.9% 稀疏)。
overlay / snapshot 文件名内嵌的 sha256 是跳空洞的 extent 摘要(见
`sandboxer/docs/sandbox.md` §6.1;发布包平铺名:`docs/sandbox.md`),host 多次
snapshot 同内容 → 同名覆盖。

**对比"CH 写 memory-ranges + sandbox-ctl 读再上传"路径**:该路径 = 24 GiB
I/O,~10 s 量级。本设计的 sandbox-ctl 持有 memfd + SEEK_DATA/HOLE 扫驻留页 =
47 MiB 单次写,~1 GB/s 顺序写盘 = ~50 ms,**~200× I/O 减少**。

`--upload`(入 store 而非落盘)路径的瓶颈在 chunk ingest 的网络往返,不在
本地写盘:ingest 的 derive/encrypt/`Put` 现按 store 客户端连接池(`store.pool`)
并发,而非串行单 `Put`(机制见 `accelerator/docs/manifest.md` §4.7;发布包
平铺名:`docs/manifest.md`)。多 GiB
内存段上传由此从"串行往返累加"变为"池并发",壁钟随 `store.pool` 近似线性
下降,直到打满 store-ctl 或带宽;`sandbox-ctl snapshot` 期间 stderr 打 ingest
进度 + 收尾吞吐 profile 行(见 `sandboxer/docs/sandbox.md` §2.3;发布包
平铺名:`docs/sandbox.md`)。

### 2.5 Restore 本地文件(基线)

| 阶段 | 时间 |
|---|---:|
| 解 ZIP + config.json 重写 | <5 ms |
| memfd + va_report 准备 | <5 ms |
| spawn CH + uffd 协商(单 uffd) | ~150 ms |
| /vm.resume + vCPU 跑到第一个 TICK | ~250 ms |
| 端到端(snapshot 时 TICK=10 → 接续到 TICK 13) | ~750 ms 实测窗口 |

uffd 统计(restore 模式 = StreamSnapshotSource):

```
faults_absent        92
copy_calls           92
pages_copied         8887
batch_calls          92
batch_avg_pages      96      ≈ 384 KiB / batch
batch_max_pages      256     (1 MiB cap 命中)
wakes                0
errors               0
```

restore 比 cold-start 平均批小一半(96 vs 196):snapshot 文件 SEEK_HOLE 分布
更碎(混合 hole 和 data 段),每次 IsZero 切换都打断 batch。9000 页(35 MiB)
在 92 次 ioctl 内完成。

### 2.6 优化机会(按收益预估)

**StreamSnapshotSource 提供 RunLength 接口**(中,~30%):现 `extendBatch`
对每个候选页调一次 `IsZero(off)`,256 次 bit lookup。StreamSnapshotSource
持有 `holeMap []uint64`,可以一次性算出"从 off 起同类连续多少页"
(查 word 内 `bits.TrailingZeros64` / `LeadingZeros64`)。预计 restore 路径下
batch_avg 从 96 提到 ~150-200。

**cache-ctl chunk 预取**(中,~30% manifest restore cold-L1):现
ManifestSnapshotSource 一次 fault 拉一个 chunk(经 cache-ctl)。可以加
sequential 预取:每次 fault 拿到 chunk N,在异步 prefetch chunk N+1, N+2。
预计 manifest:// restore cold-L1 从 472 ms 降到 ~330 ms。

**vhost-user-blk read 预取**(中,~20% 冷启动):blk0 的 manifest:// p99 ~3.9ms
主要来自顺序 erofs 块读 + chunk 拉取叠加。加 `posix_fadvise(POSIX_FADV_WILLNEED)`
或 manifest 路径下预取下一 chunk。

**启动期 backendVA prefault on madvise(MADV_POPULATE_READ)**(低):违背懒加载
的核心收益,**不推荐**。

**reader 单 goroutine 拆分**(低):reader epoll 两个 fd 单线程。当前不是
瓶颈。

**EEXIST 路径精确统计**(低):wakes=0,这条路径几乎不走。

**Quiesce 并行化**(低):pause 窗口已经很小(4-6 ms)。

**batch 上限调优**(待定):提高到 512(2 MiB)/ 1024(4 MiB)可能进一步减少
ioctl 数,但每个 ioctl 时延增长。需要参数 sweep。

## 3. 沙箱密度与控制器

### 3.1 工作负载模型

`test/perf/workload.py` 是单一可复用 guest 负载脚本,通过环境变量切换三种
模式:

**cycles 模式**(确定性,默认):每 cycle 以 4 MiB chunk 增长到 R MiB → rest
→ 释放,共 N cycles 均匀分布在 WL_DURATION 内。R 在 [RMIN, RMAX] 之间 PRNG
采样(seed 固定)。

**pareto 模式**(近真实流量):idle→active 转换为 Poisson 过程(rate λ),
active 持续时间为重尾 Pareto(α=1.5)。需要较长测量窗口(≥5 min)才能观察
稳态。

**idle 模式**(密度基线):仅 boot 后 sleep,不产生内存压力。

### 3.2 测量入口

```bash
sudo bash test/perf/density-perf.sh
```

主要环境变量:

| 类别 | 变量 | 默认 | 含义 |
|---|---|---|---|
| 拓扑 | `N` | 8 | 并发沙箱数 |
| 拓扑 | `MEM_MIB` | 256 | 每沙箱 memory-zone 大小 |
| 拓扑 | `FLOOR_MIB` | 64 | allocatable.memory(floor) |
| 拓扑 | `BURST_MIB` | =FLOOR_MIB | startup_burst.memory |
| 拓扑 | `STAGGER_S` | 0 | 连续启动间隔 |
| 负载 | `MODE` | cycles | cycles \| pareto \| idle |
| 控制器 | `PHYS_MEM` | 主机 MemTotal | node_budget.memory |
| 控制器 | `HOST_RES_MEM` | 1GiB | host_reserved.memory |
| 控制器 | `HIGH_FACTOR` `LOW_FACTOR` `EMERG_FACTOR` | 0.85,0.70,0.05 | water-mark 比例 |
| 控制器 | `GRANT_PER_SEC_FACTOR` | 0.20 | 每秒 grant 总速率 / pool |
| 控制器 | `RECOVER_DUR` | 30s | dampening 恢复时长 |

输出 `$WORK/density-perf-N{N}.json`,含:每沙箱 admit/settled/grant/reject
计数,cgroup `memory.events.local` 累积 high/oom,host MemAvailable 时间序列
采样,launch wallclock。

### 3.3 实测基线

主机 8 GiB,1 GiB host_reserved → pool=6.9 GiB。所有数字含 controller 在闭环。

**idle 占用**(workload mode=idle):

| N | memory-zone | floor | guest 工作负载 | Δ MemAvailable / sandbox |
|---:|---:|---:|---|---:|
| 16 | 256 MiB | 64 MiB | sleep | **57 MiB** |
| 40 | 256 MiB | 64 MiB | sleep | **57 MiB** |
| 8 | 1024 MiB | 128 MiB | sleep | **72 MiB** |

**关键观察**:idle 状态下,memory-zone 大小几乎不影响占用——256 MiB 与
1024 MiB 的 zone 差别只有 ~15 MiB(主要是 CH 内部为更大 memfd 维护的元数据)。
memfd 是**lazy-allocated**,host 物理内存只为已 fault 的 guest 页买单。
idle 沙箱 ~60 MiB 约束 = CH 进程 RSS(~50 MiB)+ boot/init 已 fault 的 guest
页(~5–10 MiB)。

**active cycles 占用**(mode=cycles):

| N | memory-zone | floor | burst | R 区间 | grants | oom | Δ MemAvail/sb | 备注 |
|---:|---:|---:|---:|---|---:|---:|---:|---|
| 4 | 256 MiB | 64 | 64 | 64–128 | 10 | 0 | 168 MiB | 单沙箱获更多 grant |
| 16 | 256 MiB | 64 | 64 | 64–128 | 22 | 0 | 102 MiB | 进入水位竞争 |
| 8 | 1024 MiB | 128 | 128 | 128–256 | 16 | 0 | 189 MiB | 大 R → 大占用 |

active 状态下,Δ MemAvail/sb **由 workload 实际 fault 的页决定**,而不是
memory-zone size。R=64–128 MiB → 100 MiB/sb;R=128–256 MiB → 190 MiB/sb,
近似线性。

active 阶段的内存增长曲线(N=8, 1024 MiB zone, R=128–256 MiB):

```
t=0s:  avail=4116 MiB (baseline)
t=5s:  avail=2780 MiB (-1336, 初次 fault-in 主导)
t=10s: avail=2646 MiB (-1470)
t=20s: avail=2647 MiB (稳态)
t=60s: avail=2615 MiB (基本不再下降)
```

第一次 active 周期会把高水位 fault 进 host 内存,后续 idle 段即使 Python 释放,
balloon **不会立即回收**——host 端 BalloonController 的反馈环周期 5 s + 单步
≤ 256 MiB,guest kernel page reclaim 周期 + glibc 堆碎片再叠加一层延迟,实测
60 s 内回收量 ~30 MiB(占释放量 <20%)。所以 steady-state Δ MemAvail/sb ≈
**此前观测到的 active 峰值**,而非平均。

**controller 事件延迟特征**:

| 事件 | 中位数(典型) | 说明 |
|---|---:|---|
| admit RPC roundtrip | < 5 ms | 同机 UDS,无 IO |
| HelloDone → Settled RPC 到达 daemon | ~1 s | guest cold-start 主导 |
| Active reclaimer sweep 间隔 | 10 s | 默认 `Interval` 常量 |
| Sensor → grant ack | <100 ms | sensor 100 ms tick + UDS RPC |
| Reject 5th admit (zone red) | <50 ms | 同步,无 backoff |

### 3.4 密度上限分析:peak working set 主导

memory-zone 只是 memfd 的虚拟上界(用作 guest 物理地址空间和 cgroup
`memory.max`),本身**不消耗** host 物理页;实际占用 = guest 已 fault 进
memfd 的页数 - balloon 已经 report 给 host 回收的页数。所以密度估算应该围绕
**peak working set**,而不是 memory-zone size:

```
N ≈ (host_total - host_reserved - host_buffer)
    / (CH_overhead + peak_resident_per_sandbox)

CH_overhead              ≈ 50 MiB  (CH 进程 private RSS,与 memory-zone 无关)
peak_resident_per_sandbox 与 workload 行为耦合,见下表
```

对 8 GiB 主机(host_reserved=1 GiB,buffer=512 MiB,可用 pool ≈ 6.5 GiB):

| 工作负载剖面 | peak_resident / sandbox | N 上限 |
|---|---:|---:|
| idle (sleep) | ~60 MiB | **~110** |
| 轻 active (R≤64 MiB,稀疏 active) | ~110 MiB | ~60 |
| 中 active (R≤128 MiB,中等 duty cycle) | ~150 MiB | ~45 |
| 重 active (R≤256 MiB,持续 active) | ~250 MiB | ~25 |

**实测验证**:
- 40 个 idle 沙箱(memory-zone=256 MiB)与 8 个 idle 沙箱(memory-zone=1024 MiB)
  在 host 上的 Δ MemAvail/sb 都是 ~57–72 MiB,**zone size 4× 不同但占用近似
  相等**——因为 idle guest 不去 fault memfd,扩大的 zone 是死虚拟地址空间
- 8 个 active 沙箱(memory-zone=1024 MiB,R=128–256 MiB)Δ MemAvail/sb ≈
  189 MiB,与 R 区间一致,与 zone size 无关

**balloon 回收的局限**(为什么不能让占用回到 idle 水平):

1. 回收链路长:Python `del held` → glibc 不一定立即 munmap(堆碎片) → 即使
   munmap,guest kernel 也不会立即把页加入 free_list → guest /proc/meminfo
   的 MemAvailable 才上升 → guest 5 s 推 mem_report → host 端 BalloonController
   `MaxStep ≤ 256 MiB` 单步推 inflate target → guest balloon 驱动响应 inflate
   → CH 在 memfd 上 punch_hole + madvise
2. 实测 60 s 内回收 <20% 释放量;如果 active 周期间隔 < 反馈环完整 cycle,
   实际接近**永远不回收**
3. 即使全程 idle 等几分钟后回收效率高(50–70%),也要求 BalloonController 把
   target 持续抬到 `Capacity − TargetFreeBuffer`,该过程受 stale-guard 与
   Slack(默认 32 MiB)节流

**结论**:memory-zone size 是单沙箱 cap(防失控),不是密度筹划单位。密度
规划应基于**该应用类型的 peak working set 实测值**(从 cgroup `memory.current`
P99 读),不要看 memory-zone。

### 3.5 调优杠杆

按效益从高到低排列。

**3.5.1 客户应用 peak working set**(业务侧调优)

密度的首要决定项是**客户应用的实际峰值 RSS**——平台层无法让一个真要吃
256 MiB 的应用变成 64 MiB。但平台可以提供两类杠杆:

- **指导客户填写合理的 capacity**:capacity 太大不会"奖励"密度(memory-zone
  不耗物理页),但会撑大单沙箱失控时的破坏面;太小会真 OOM。建议取观察到的
  P99 RSS × 1.2
- **让客户应用尽快释放峰值后的内存**:active 周期结束后立刻 `del`/`gc.collect()`
  + 调用 `malloc_trim(0)` 提示 glibc 归还页给 kernel;否则 balloon 看不到这些页

**3.5.2 host-driven balloon + `deflate_on_oom=on`**

平台 CH 命令行采用 `--balloon size=0[,deflate_on_oom=on]`,host 端
BalloonController 通过 `/vm.resize` 周期推 inflate target(详见 sandbox.md
§9.3);`deflate_on_oom` 让 guest 在内部 OOM 时先释放 balloon 抢救。两者缺
一不可:
- 只开 host-driven inflate:guest 内部 OOM 时 balloon 不让步 → 应用进程被 kill
- 只开 deflate_on_oom:host 没有 inflate 信号 → 沙箱永不让出空闲内存 → 密度
  提不上

balloon 是密度的**唯一**弹性来源,但它不快。两个层次的对策:
- **调度层面**:不要把 active 周期密集排布——让相邻 active 之间有 ≥1 min 的真
  idle gap,反馈环才能把上一轮峰值收回
- **参数层面**:BalloonController 的 `Interval` / `MaxStep` / `TargetFreeBuffer`
  可调,缩短 Interval / 加大 MaxStep 让回收更激进,代价是 mmu_notifier
  广播突发增大(详见 sandbox.md §9.3 的 stale-guard / Slack 设计)

**3.5.3 controller 水位**(`high_factor` / `low_factor` / `emergency_factor`)

| 参数 | 默认 | 调小的影响 | 调大的影响 |
|---|---:|---|---|
| `high_factor` | 0.85 | 提前进入 yellow(更早降速) | 延后 yellow,提高名义密度但增加 OOM 风险 |
| `low_factor` | 0.70 | yellow 持续更久(更保守) | 更早回 green |
| `emergency_factor` | 0.05 | 紧急池更小,常规 grant 更宽松 | 紧急池更大,头部沙箱(urgency=high)抗 OOM 能力强 |

调优思路:**先看负载形态**——突发型(短 active 高峰)适合 high=0.90 +
emerg=0.10,让常规 grant 紧一点但留更大紧急池;持续型(长尾 active)适合
high=0.80 + emerg=0.05,让常规 grant 更顺畅。

**3.5.4 `grant_per_sec_factor`**(rate limit)

默认 0.20 表示每秒最多分配 pool 的 20%(8 GiB pool → 1.6 GiB/s)。这是抗
"突发惊群"的核心杠杆。调小到 0.10 让 grant 更分散,代价是单个突发 active
沙箱的扩容延迟从 ~50 ms 上升到 ~500 ms。

**3.5.5 active reclaimer**(`recover_duration` / 安全边际)

控制器侧的 active reclaimer 默认每 10 s sweep 一次,把 settled 沙箱
allocatable 向 `last_reported_rss × 1.25` 收敛(yellow zone 1.10,red zone
1.05,critical 1.00)。

调优:负载抖动大时调到 1.50(给应用 50% 缓冲),抖动小时 1.10(更激进回收)。

`recover_duration` 是 grant 后冷却窗口——刚 grant 过的沙箱不会立即被 reclaim,
避免 thrash。30 s 是一个保守值;紧凑节点(高密度)可调到 60 s。

**3.5.6 `host_reserved` 与 `operational_margin_factor`**

`host_reserved` 给 host kernel + 系统进程 + cache-ctl 等留出**绝对**空间;
`operational_margin_factor`(默认 0.10)在 pool 里再扣 10% 作为日常波动 buffer
(突发 page cache、驱动 alloc 等)。

调优:cache-ctl 长期常驻、image cache 大时,`host_reserved` 应明确加上
cache-ctl 预算(典型 1–2 GiB),否则 cache 增长会挤掉沙箱内存。

### 3.6 摸底新工作负载的工作流

针对一类新工作负载找 (capacity, floor, burst, water-mark) 的最优组合:

1. **测单沙箱 peak working set** —— 这是密度估算的输入

   ```bash
   sudo N=1 MEM_MIB=<上界> MODE=cycles WL_DURATION=300 \
        WL_RMIN_MIB=<业务低水> WL_RMAX_MIB=<业务高水> \
        PERF_KEEP=1 bash test/perf/density-perf.sh
   ```

   从 cgroup `memory.current` 历史取 P99 RSS。`capacity = MEM_MIB = P99 × 1.2`
   (防失控的 cap),`peak_resident = P99` 用于密度公式估算 N 上限

2. **验证 idle baseline 不依赖 zone size**

   ```bash
   sudo N=20 MEM_MIB=<上一步 capacity> MODE=idle WL_DURATION=180 \
        bash test/perf/density-perf.sh
   ```

   预期 Δ MemAvail/sb ≈ 60 MiB,与 zone size 解耦——若否,排查 sandbox-init
   是否引入了不该有的常驻内存

3. **测目标密度下控制器收敛**

   ```bash
   sudo N=<目标密度> MEM_MIB=<同上> MODE=cycles \
        WL_DURATION=300 WL_CYCLES=10 \
        bash test/perf/density-perf.sh
   ```

   断言:`cgroup_oom_total = 0`,`daemon_grants > 0`(控制器活跃)。Δ MemAvail
   后期不再下降说明系统进入稳态;若持续下降说明 active 周期太密,balloon 还没
   来得及回收上一轮峰值就进入下一轮——需要业务侧错峰或加 idle gap

4. **测拒绝速率反压**

   ```bash
   sudo N=<目标密度+5> MEM_MIB=<同上> PHYS_MEM=<紧凑> \
        bash test/perf/density-perf.sh
   ```

   观察 `daemon_rejects > 0`,确认水位生效

5. **抖动注入**(可选)

   ```bash
   sudo N=<目标> MODE=pareto WL_DURATION=600 \
        WL_LAMBDA=0.3 WL_ALPHA=1.2 \
        bash test/perf/density-perf.sh
   ```

   重尾分布能暴露 `recover_duration` 与 `safety_margin` 的不当——表现为 grant
   抖动放大、cgroup high 累积

### 3.7 已知噪声源(嵌套 KVM 开发环境)

- vCPU resume / cold-start 有 ±300 ms 抖动 → settled latency 中位数稳定但 P99
  偏长,要 N≥10 取中位数才稳
- 嵌套虚拟化下宿主 `MemAvailable` 受外层宿主 working-set 影响,采样应在隔离的
  专用环境内进行
- shared memfd 的 `Δ used` 在 `free -m` 中表现保守(只算 private),真实占用
  要看 `MemAvailable` 下降量
- guest 内核 dmesg(`console=hvc0`)与应用 stdout 是两条独立的道:dmesg 经 CH
  `--console tty` 落到 sandbox-ctl 给 CH 的匿名管道(`run --console` 决定去向),
  应用 stdin/stdout/stderr 经 vsock stdio MUX 转发(默认 pipe 模式:应用 stdout →
  sandbox-ctl stdout)。perf harness 仍主要依赖 audit.log + cgroup events 做断言,
  应用 stdout 可用作辅助信号

### 3.8 cluster 控制面冷路径

cluster 性能口径应区分热路径与冷路径。热路径是 router 已有活动连接缓存后的
数据转发,不应进入 registry Reserve;冷路径是首次连接、沙箱创建、build 创建、
node 状态变化、group 导入与成员切换。这些路径进入 registry/placer,目标是
保证可靠性和扩展线性,而不是把所有 QPS 都压到 registry 上。

```
hot path:

client ── data stream ──► router active connection cache ──► node/proxy/envd
          no Reserve while same route connection is alive

cold path:

router ── Reserve(group,sandbox) ──► registry route_link ── Place ──► placer
node   ── state/heartbeat/events ──► registry node_link/node_list
placer ── group import / key selector ──► registry route_link/node_link
```

性能回归应覆盖:

- registry N=1 与 N>1 两种模式:单成员必须退化为无网络复制路径;多成员验证
  shardkv CAS、WATCH 与成员健康变化。
- `membership_version` 变更:配置 reload 后不同 label 的成员集群并存,旧版本
  grace 期间继续服务已有分片;新版本 READY 后新写进入新逻辑分片。业务无损的
  目标是避免把所有记录全局扫描迁移,由节点心跳、group import 与分片内复制
  自然补齐活动数据。
- node_link 重定向/转发:任意 registry 接入 node 时,若自身不是 node owner,
  应逐个尝试 owner;节点支持 redirect 时可重连到 owner,否则在 relay 链路上
  订阅并复制状态。
- router cache:同一 route 的活动连接存在时,新连接不走 Reserve;活动连接过期
  或路由失效后才回到冷路径。
- placer import:同一个 `source_id` 的导入任务由 placer_link 中的 lease 串行
  执行;文件源只用于开发/e2e,生产源通过 provider/importer 接口实现。

`orchestrator` 的 cluster stub e2e 应作为当前主要回归入口:由
`make build` 产生真实 `cluster-ctl`/`node-stub-ctl` 二进制,启动 registry、
router、placer 和指定数量 stub node,覆盖 N=1、多成员、成员变更、node
重启、sandbox/build 创建删除、route 查询与 WATCH_LIST。

## 4. 已知测量局限

- **vCPU 并发**:当前数据是 1 vCPU,fault 几乎无并发竞态;4-8 vCPU 下的 batch
  行为与 EVENT_REMOVE 在 host-driven balloon inflate 场景下的突发速率
  (每 5 s tick + MaxStep 256 MiB)未覆盖
- **环境口径**:本数据来自嵌套 KVM 开发环境,有 ~2× 抖动;绝对数量级以裸金属
  native Linux + NVMe 为准,尤其 hot-L1 restore 中位数稳定后会接近 file:// 基线
- **去重口径**:本数据是单节点;跨多节点 cache-ctl 集群 + EC L2 + 共享 OBS 的
  跨 sandbox image-段 dedup(预期 >70%)未实测

## 5. 回归 checklist

改 `pkg/cache/{ec,client,wire,blob.go,server,tiered.go}` 时:

```bash
go vet ./...
make test
bash test/e2e/e2e_cache.sh                # cache-ctl + manifest-ctl 集成(脚本在 accelerator)
BENCH_SCENARIO=tiered-shard-l2 CLIENT_CORES=0-1 SERVER_CORES=2-7 \
    CONCS='1 2 4' PREFILL=500 DURATION=30s bash test/scripts/bench_cache.sh
```

对照 §1.1 的指标,p50/p99.9/throughput 回归超过 5% 需要查明原因或回退。

改 `pkg/sandbox/{uffd,vhost,memory,snapshot,restore}` 时:

```bash
make test
make test-e2e-sandbox-cold
make test-e2e-sandbox-cold-manifest
make perf-sandbox
```

对照 §2.3 的指标,uffd wakes/errors 必须 0,numGC > 8 / total_alloc > 30 MiB
需要查明原因。

改 `pkg/nodectl/*` 或 `pkg/sandbox/*` 中的资源控制路径时:

```bash
make -C orchestrator test-e2e-node-ctl   # node-ctl 资源协议 e2e(orchestrator 仓;umbrella test-e2e 经其调用)
make test-e2e-density
make perf-density
```

`test-e2e-density` 的 Phase B 不以 guest OOM 作为静态模式基线。生产
vmlinux 在 infeasible balloon target 下会由 guest sticky self-cap 主动归还内存，
因此 B1 直接验证 self-cap 日志和 workload 最终存活；B2 在相同 floor/workload
下验证 node controller admit/grant，并要求 workload 在没有 self-cap、guest OOM
或 cgroup OOM 的情况下完成。host cgroup pressure 只作为诊断信息，不能替代
guest 内的直接证据。

对照 §3.3 的实测基线;`cgroup_oom_total > 0` 必须查明原因。

## 6. See Also

- `accelerator/docs/cache.md`(发布包:`docs/cache.md`) —— cache-ctl 架构,本文 §1 关注其运行特征
- `sandboxer/docs/sandbox.md`(发布包:`docs/sandbox.md`) —— sandbox-ctl 架构,本文 §2 关注其运行特征
- `orchestrator/docs/node-resource.md`(发布包:`docs/node-resource.md`) —— 节点资源控制器架构与协议规范
- `orchestrator/docs/cluster.md`(发布包:`docs/cluster.md`) —— cluster registry/router/placer 设计
- [`kuasar-sandbox.md`](kuasar-sandbox.md) §1.3 / §7 —— 系统级 SLO 与规模推算的来源
