[English](node-resource.md) | [简体中文](node-resource_zh.md)

<a id="overview"></a>

# Node resources and admission

`node-ctl conductor serve` optionally embeds the node resource controller. It owns node-side reservation, admission, pools, watermarks, recovery inventory, pressure episodes and statistics projection. The pressure worker coordinates the existing full memory Pause/Resume lifecycle through the orchestrator. Sandboxer owns the sandbox-local loop for guest memory observations, Cloud Hypervisor ballooning, `memory.high` and cold/restore/snapshot lifecycle; those operations do not belong to the node controller.

Begin with units and configuration, then request patches. Capacity, allocatable memory, startup target and host reservations have different owners. The model, wire protocol and recovery sections preserve the implementation contract needed to extend or diagnose the controller.

**Read by task:** [Terms](#units-and-terms) · [Configure](#resource-policy) · [Request resources](#request-resources) · [Resource pools](#resource-pools) · [Model](#resource-model) · [Protocol](#controller-protocol) · [Admission](#admission-and-pressure) · [Recover](#recovery)

<a id="units-and-terms"></a>

## Terminology

| Name | Definition | Owner |
|---|---|---|
| Capacity | Fixed maximum VM memory. | Sandbox config/snapshot. |
| Headroom | `resources.allocatable.memory`, settled guest headroom. | Sandbox policy. |
| StartupHeadroom | `resources.startup.memory`, headroom before the first trusted cold-start report. | Sandbox policy. |
| BudgetAtSnapshot | `Capacity - min(snapshot target,snapshot current)`. | Sandbox snapshot. |
| NodeReservation | Absolute memory amount reserved by the node for one sandbox. | Node controller. |
| HostMemoryCurrent | Host VMM cgroup `memory.current`; diagnostic only. | Reported by sandbox, recorded by node. |
| reservedMemory | Sum of all live `NodeReservation` values. | Node state. |

Headroom is not the total Budget. CPU `allocatable` expresses the relative scheduling specification mapped to `cpu.weight`, without a hard fractional-core quota or unconditional performance guarantee. Its meaning differs from memory headroom.

Sandbox-local state also includes `TargetBudget`, `CurrentBudget`, `ObservedBudget` and `DemandMemory`; the node neither needs nor stores them. On the normal controlled path, the sandbox obtains enough NodeReservation before increasing Budget and releases reservation only after shrink converges. Emergency guest `deflate_on_oom` is an exception to the phased soft guarantee: it changes neither target nor reservation. Existing `memory.high` continues throttling host VMM charges above its threshold, while the sandbox marks target/current unstable and prohibits shrink. Snapshot still computes BudgetAtSnapshot from the safe upper bound of both sides.

<a id="resource-command"></a>

## Command-line interface

Conductor starts the controller through `resource_listen`; there is no standalone controller daemon subcommand. Administration provides observation and admission drain:

```text
node-ctl resource status [--socket PATH]
node-ctl resource list   [--socket PATH]
node-ctl resource drain  [--socket PATH] [--disable]
node-ctl resource pressure [--socket CONTROL_SOCKET]
```

`status` shows node budget, host reserved, operational margin, allocatable pool, reserved memory, startup in-flight, zone and recovery counts. `list` shows each sandbox reservation. `drain` prevents new admission without altering live reservations.

`pressure` queries `GET /internal/admin/resource-pressure` on the conductor control socket (or `NODE_CTL_SOCKET`), using the existing local admin authentication. It reports effective/RawZone, transition reason/version/time, R/P/E, hold remaining, protected funds, Q by phase, cleanup barriers, oldest wait, per-sandbox pause reason/running interval, and a bounded recent operation history. It also projects `pools`, `runner_pools`, unattributed runner counts, per-dimension utilization, capacity/startup usage and source/destination pools for recovery obligations; see [resource pools](#resource-pools). The resource reservation protocol remains unchanged. The former `host_safety_blocked` diagnostic field is no longer reported; this query does not measure whole-host memory safety.

`worker_blocked=no_eligible_running_sandbox` explains a pressure episode with
no safe running candidate; the worker waits without releasing another owner's
charge.

There is no `resource grant` or `resource reclaim`. Headroom is a sandbox-policy input: select it through supported sandbox configuration/lifecycle inputs, and let the sandbox's existing loop converge from observations. This does not introduce a node command for live policy reload or direct balloon/cgroup adjustment.

<a id="resource-policy"></a>

## Configuration

<a id="sandbox-resource-policy"></a>

### Sandbox resource policy

```yaml
sandbox:
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable:
      memory: 256MiB
    startup: { memory: 2GiB }
    overhead: { memory: 32MiB }
    watermark_high: { ratio: 0.875 }
    diff_cow: { cache_size: 64MiB }
```

- `capacity.memory` is Capacity.
- `diff_cow` budgets one host cache across the sandbox's writable disks. An omitted cache size defaults to 32 MiB. An omitted dirty limit follows half the final cache size, rounded down to 4 KiB with a 4 KiB minimum. The example resolves to 64 MiB of cache and a 32 MiB dirty limit. Explicit limits must be positive 4 KiB multiples and dirty must fit in cache.
- `allocatable.memory` is settled headroom, default `256MiB`. If an inherited default exceeds final Capacity, the resolver clamps it to Capacity. Explicit out-of-range request values are rejected.
- `startup.memory` is cold-start headroom, defaulting to final Capacity in both static and dynamic modes. It is independent of settled headroom and need not be greater or smaller.
- `overhead.memory` is node-owned host VMM overhead. Sandbox-ctl occupies a separate control cgroup and does not consume that allowance. `watermark_high.ratio` also belongs to node policy. Neither may be set by requests/templates.
- `0 < allocatable.memory <= capacity.memory`.
- `0 < startup.memory <= capacity.memory`.
- `0 < watermark_high.ratio < 1`, default `0.875`.

Restore obtains Capacity from the snapshot. StartupHeadroom does not participate in restore admission: sandboxer's BudgetAtSnapshot is the sole initial reservation.

A synchronous restore request validates only the portable patch structure. After the runner binds the exact run-id, its task reads the root snapshot.cfg in-process. For a Manifest Bundle root, it first reads only the metadata prefix and supplements located-source mappings from flat bundle/refs metadata; it does not open or recursively scan refs Bundles. The task returns Capacity as a nonsecret summary to the unique launch worker; conductor does not open the snapshot. Explicit request Capacity can act as an assertion. A mismatch or unreadable snapshot becomes an asynchronous `resource_resolve` failure, before network Attach, sandbox YAML, controller Admit or VM startup, with no fallback to node defaults. Sandboxer still computes BudgetAtSnapshot from CH snapshot target/current; orchestrator does not probe or derive it.

`ResourceAllocatable.Memory` exposes presence as `*string`: nil inherits the internal resolver's `256MiB` default; non-nil, including explicit `256MiB`, is operator/custom Configure policy and must fit capacity. `SetMemory`/`InheritMemory` are conveniences; direct pointer assignment has identical semantics. Clone and component bootstrap JSON preserve presence.

<a id="resource_listen"></a>

### resource_listen

This example uses the implicit single `default` resource pool and its legacy
physical-resource calculation. For independent net budgets, use the explicit
[resource-pool configuration](#pool-configuration); do not combine multi-pool
input with the physical-resource or operational-margin fields shown below.

```yaml
resource_listen:
  enabled: true
  socket: /run/sandbox-resource.sock
  cgroup_scan_paths:
    - /sys/fs/cgroup/sandbox.slice/sandbox-runner.slice
    - /sys/fs/cgroup/sandbox.slice/sandbox-builder.slice
  resources:
    physical_memory: auto
    physical_cpu: auto
    host_reserved: { memory: 16GiB, cpu: 1.5 }
  watermarks:
    operational_margin_factor: 0.10
    high_factor: 0.85
    low_factor: 0.70
    emergency_factor: 0.05
    startup_factor: 0.50
  rate_limits:
    memory_grant_per_sec_factor: 0.05
  admission:
    rate: 4
    burst: 16
    startup_ttl: 30s
    queue_ttl: 30s
    queue_max_depth: 256
  pressure:
    interval: 1s
    failure_interval: 500ms
    critical_after_rounds: 3
    pause_after_rounds: 3
    critical_exit_hold: 5s
    red_to_yellow_hold: 30s
    yellow_to_green_hold: 30s
    minimum_run_time: 30s
```

`operational_margin_factor` reserves a node safety margin from the post-host budget. `emergency_factor` reserves pool capacity for high-urgency growth. `startup_factor` bounds aggregate reservations for concurrent creation/restoration. Admission rate/burst form a request token bucket, not memory Budget.

Preflight requires `host_reserved.memory < physical_memory` and validates `0 <= operational_margin_factor < 1`, `0 <= low_factor < high_factor < 1 - emergency_factor <= 1`, `0 <= emergency_factor < startup_factor <= 1` and `0 < memory_grant_per_sec_factor <= 1`. The final pool must be positive and byte-rounded thresholds must satisfy `Ty < Tr < Tc`. Pressure intervals and holds must be positive, failure rounds must be positive, failure interval must be at least 250ms, and minimum run time must be nonnegative. Invalid values fail before serving. These defaults are initial tuning values; validate capture/restore latency and net host release on the target node.

`resource_listen.socket` is the sole endpoint configuration. Node-ctl binds an absolute path and canonicalizes parent-directory symlinks into the identity shared by owner lock, lease inventory and sandbox client. A symlink at the final socket, dangling path or ambiguous alias fails closed. Sandbox YAML does not carry `control.cgroup_path`; the runner injects that host capability through an inherited cgroup FD. `resource_listen.state_path` is deprecated and ignored; it does not enable state.json recovery ([Inventory and controller restart](#inventory-and-controller-restart)).

<a id="requesttemplate-ownership"></a>

### Request/template ownership

Tenant resource patches allow `capacity`, `allocatable`, `startup` and the `diff_cow` cache limits and cold-start discard policy. The node resolver owns `control`, `overhead`, `watermark_high`, sensors and deflate_on_oom. The parser rejects unknown or unauthorized fields.

<a id="conductor-resource-configuration"></a>

## Conductor resource configuration

These fields belong to Conductor configuration. Shared loading, path and environment rules are defined in [node operation](node.md#conductor-configuration).

| Field | Default | Meaning |
|---|---|---|
| `sandbox.resources.capacity.cpu` / `.memory` | `2` / `2GiB` | Guest-visible VM ceiling/SKU; E2B cpuCount/memoryMB still mean capacity. img cold starts accept create/group overrides; artifact capacity constrains restore |
| `sandbox.resources.allocatable.cpu` / `.memory` | Final capacity CPU / inherited `256MiB` when absent | CPU is relative scheduling weight, not a hard fractional-core guarantee; memory is settled guest headroom, not total Budget. Omitted node memory can clamp to final capacity, but explicit operator/custom values, even `256MiB`, must not silently clamp. Out-of-range values fail; request pointer semantics are unchanged |
| `sandbox.resources.startup.memory` | Final capacity memory | Headroom before the first trusted cold report, in static and dynamic modes; independent of settled headroom and unused for initial restore Budget |
| `sandbox.resources.overhead.memory` | `32MiB` | Node-owned host VMM overhead; sandbox-ctl is outside VMM cgroup. Requests/templates cannot override |
| `sandbox.resources.watermark_high.ratio` | `0.875` | Node-owned memory.high pressure ratio in `(0,1)`; requests/templates cannot override |
| `sandbox.resources.diff_cow.cache_size` | `32MiB` | Host cache shared by the sandbox's writable disks |
| `sandbox.resources.diff_cow.max_dirty_size` | Half the final cache size, rounded down to 4 KiB, minimum 4 KiB | An explicit limit must fit within the cache |
| `sandbox.resources.diff_cow.discard` | `auto` | Writable ext4 cold-mount policy: `auto` mounts with `discard`; `manual` uses `nodiscard` while retaining explicit trim support |
| `resource_listen` | Absent/not embedded | Sole controller endpoint source: socket resolves to absolute bind Listen and canonical SocketIdentity for ownership/inventory/lease/Sandbox YAML. Clients reach the same socket inode through its canonical path. enabled and tuning are in node-resource [resource_listen](#resource_listen). Omitted/disabled means static cgroups |

<a id="request-resources"></a>

## Request resource schema and precedence

Resource and traffic merge by leaf. Public resource JSON permits only:

```json
{
  "capacity": {"cpu": 2, "memory": "8GiB"},
  "allocatable": {"cpu": 0.5, "memory": "256MiB"},
  "startup": {"memory": "1GiB"},
  "diff_cow": {"cache_size": "64MiB", "max_dirty_size": "16MiB", "discard": "auto"}
}
```

The value must be one JSON object. Unknown fields, null, arrays/scalars, trailing JSON, explicit zero/negative CPU, empty/invalid/nonpositive memory return 400 with full kuasar-sandbox.resource path. Requests/templates/groups/headers/migration tokens cannot carry allocatable.deflate_on_oom, overhead, watermark_high, control or sensor. Node/runtime own them, including injected watermark_high.ratio. Valid patches persist as compact canonical JSON.

Resource leaf priority is fixed:

```text
node resource policy
  < cluster group defaults
  < create / reserve body resource
  < X-Kuasar-Sandbox-Resource
  < E2B first-class cpuCount / memoryMB (Override only the corresponding capacity leaf)
  < portable artifact capacity constraint
```

Each resource leaf overlays independently. For example, group capacity.memory and Create allocatable.memory both survive. Every layer is validated before merging. Group/reserve, standalone/cluster and Build registration share the helper. Other namespaces follow their own rules below.

`X-Kuasar-Sandbox-Resource: {"diff_cow":{"cache_size":"64MiB"}}` gives the sandbox a 32 MiB dirty limit when the node has omitted it. Use `"max_dirty_size":"16MiB"` inside `diff_cow` to set it explicitly. Sizes must be positive multiples of 4 KiB. Raw policy, request metadata and migration tokens retain only configured leaves. Restore resolves cache limits on the target node independently of portable artifact capacity. Build execution inherits the node policy, and the target Sandbox also applies the target request.

To defer filesystem discard on a new cold start, set
`sandbox.resources.diff_cow.discard: manual` on the node, or send
`X-Kuasar-Sandbox-Resource: {"diff_cow":{"discard":"manual"}}` for the
instance. `auto` preserves the default automatic discard mounts; `manual`
explicitly uses `nodiscard` on the writable ext4 root and data disks, including
overlay upper filesystems. The value must be exactly `auto` or `manual`.
Both modes retain the block device's DISCARD capability. Manual mode schedules
no trim job; explicit trim requires access to the underlying ext4 filesystem,
not its visible overlayfs mount. Until blocks are trimmed or reused, deleted
payload can continue to occupy local storage and new exports. See the
[sandboxer mount-policy contract](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/configuration.md#diff-cow-policy)
for the mount scope and runtime compatibility requirement: a manual cold launch
requires a Guest runtime that advertises this capability, and fails explicitly
with an older runtime.

This leaf applies to image boots and cold launches from Sandbox E. Memory
restore from Snapshot S preserves the captured mounts: the renderer omits
`discard` from the restore host document while retaining both cache limits.
Changing node policy or a retained resource patch does not remount a restored
Guest. Request metadata and migration tokens retain the explicitly requested
mode for a later cold launch; node defaults come from the node performing that
launch. This leaf does not enter Portable E as a resource field. Runtime cache
budgets and their ownership are unchanged.

<a id="resource-resolution"></a>

### Resolving capacity, startup and policy

Resolver determines capacity, then allocatable/startup, then adds node-only overhead/watermark/deflate/controller. Inherited node allocatable memory may clamp to final capacity; explicit node/request values cannot. Startup is independent: explicit request startup must be in `(0,capacity.memory]`; node startup exceeding final capacity clamps; if both absent, use capacity. Static/dynamic both render startup, with static omitting controller only. When settled policy needs ballooning (capacity memory exceeds allocatable), YAML writes deflate_on_oom=true. A nonzero cold startup target alone also creates a balloon device using sandboxer's effective-true default.

<a id="resource-pools"></a>

## Resource pools and placement

Resource pools extend the existing in-process resource controller. Each runner
or Builder pool belongs to one resource pool. Several logical pools can share a
resource pool, including its unit template; a template cannot span resource
pools. All pools continue to use the shared `paths.base_root`.

<a id="pool-configuration"></a>

### Configuration and capacity

The two pool levels have distinct configuration and accounting:

| Configuration | Default/presence | Contract |
| --- | --- | --- |
| `resource_listen.pools[].name` | Required, unique | Nonempty name matching `[a-zA-Z0-9][a-zA-Z0-9_.-]*`; identifies one resource budget |
| `resource_listen.pools[].capacity.{cpu,memory}` | Omitted dimension is unlimited | Positive declared-Capacity scheduling envelope; CPU may use fractional cores, memory a positive size |
| `resource_listen.pools[].allocatable.memory` | Required with multiple pools | Positive net runtime memory; a sole pool may use the legacy derived budget |
| `resource_listen.pools[].allocatable.cpu` | Inherits capacity CPU; both omitted means no finite denominator | Positive CPU accounting denominator for utilization/watermarks; no separate runtime CPU admission gate |
| `resource_listen.pools[].watermarks.{cpu,memory}` | Per-leaf inheritance | Optional low/high/emergency factors override the node policy; other dimensions are rejected |
| `resource_listen.pools[].unit_dropins` | None | Absolute paths to externally supplied unit-placement fragments copied into the managed namespace |
| `units.{runner,builder}_pools[].name` | `runner-N` / `builder-N`, zero-based | Unique within its kind; use explicit stable names before reordering |
| `units.{runner,builder}_pools[].resource_pool` | `default` | Must name a configured resource pool; a shared template cannot cross resource pools |
| `units.{runner,builder}_pools[].unit` / `size` | Required in explicit entries | Template and nonnegative idle target; shared unit-installation/legacy rules remain in [Node configuration](node.md#conductor-configuration) |
| `units.{runner,builder}_pools[].capacity` | Omitted is unlimited | Nonnegative total instance/start-reservation limit; `size <= capacity`; zero permits no new supply |
| `units.{runner,builder}_pools[].priority` | `0` | Higher numeric priority wins within a selected resource pool; equal priorities select randomly |

Explicit empty or null `resource_listen.pools`, duplicate/invalid names, unknown
pool references and templates spanning resource pools fail validation. Explicit
CPU/memory limits must be finite, positive and representable. Dimension thresholds
must satisfy `0 <= low < high < 1-emergency <= 1`; final runtime resolution also
checks positive net memory, byte-rounded thresholds and nonzero grant rate.

This fragment assumes two memory-bearing NUMA nodes. Adjust budgets, CPU masks
and paths to the actual host; allocatable memory is a **net runtime budget**.

```yaml
resource_listen:
  enabled: true
  pools:
    - name: numa0
      capacity: {cpu: 32, memory: 128GiB}
      allocatable: {cpu: 8, memory: 24GiB}
      watermarks:
        cpu: {low_factor: 0.60, high_factor: 0.80}
      unit_dropins: [/etc/kuasar/pools/numa0.conf]
    - name: numa1
      capacity: {cpu: 32, memory: 128GiB}
      allocatable: {cpu: 8, memory: 24GiB}
      unit_dropins: [/etc/kuasar/pools/numa1.conf]
units:
  install: true
  runner_pools:
    - {name: interactive0, resource_pool: numa0, unit: runner-numa0@.service, size: 2, capacity: 32, priority: 100}
    - {name: batch0, resource_pool: numa0, unit: runner-numa0@.service, size: 0, capacity: 16}
    - {name: interactive1, resource_pool: numa1, unit: runner-numa1@.service, size: 2, capacity: 32, priority: 100}
  builder_pools:
    - {name: build0, resource_pool: numa0, unit: builder-numa0@.service, size: 0, capacity: 4}
    - {name: build1, resource_pool: numa1, unit: builder-numa1@.service, size: 0, capacity: 4}
```

`capacity` bounds the sum of declared sandbox Capacity, including accepted
launch reservations. Omitted dimensions are unlimited. Runtime `allocatable`
memory accounts actual reservation, including cold-start headroom; CPU accounts
each sandbox's allocatable CPU, falling back to its Capacity. Runtime CPU has no
additional hard admission check. It contributes to utilization and watermarks
and does not change at startup/Settled. StartupPool and sandbox startup are
memory-only. Omitted runtime CPU inherits scheduling capacity CPU; if both are
omitted its utilization is zero while absolute usage remains visible.

Each `watermarks.memory` / `watermarks.cpu` entry can override `low_factor`,
`high_factor`, or `emergency_factor` independently. Missing values inherit the
existing `resource_listen.watermarks` settings. Pool utilization and raw water
level are the maximum across dimensions. The node is critical if any pool is
critical; otherwise it reports the lowest pool level. Local admission evaluates
the selected pool, so an unhealthy pool cannot seal off another healthy pool.

Runner/Builder `size` remains the idle prewarm target. Optional `capacity` limits
all starting, idle, bound, stopping and restored instances, plus effective start
reservations; zero disables new supply. A stop failure retains the slot until
confirmed cleanup. Within the selected resource pool, higher `priority` wins,
with random ties. Resource selection first filters capacity, operation permission
and runner supply, then chooses the lowest runtime utilization, with random ties.
Create permits green/yellow; normal Resume also permits red; resource Recovery
permits every level subject to its complete memory budget and startup checks.

<a id="pool-growth-limits"></a>

### Growth limits and numeric validation

Normal/low memory growth has a separate token bucket per resource pool. Its rate
`G` is the pool's effective runtime memory times
`rate_limits.memory_grant_per_sec_factor`, rounded down; a zero-byte rate is
invalid. The bucket holds `max(G, 4 MiB)` and starts with `G` tokens. Each grant
is bounded by this bucket, remaining demand, available memory and the existing
512 MiB maximum. Small positive rates can accumulate the minimum partial grant;
a smaller complete tail is granted exactly. Waiting for tokens is not memory
pressure. High urgency retains the existing emergency behavior.

The admission request token bucket remains node-wide. Missing or explicit zero
`admission.rate` / `burst` retain their defaults; negative effective values fail
at the shared final resource resolver used by Conductor, public Apps and
`config conductor --resolve`.

Legacy `physical_cpu` accepts `auto` or a complete decimal integer core count
that fits after conversion to millicores. `host_reserved.cpu` accepts finite,
nonnegative fractional cores and retains truncation to millicores and its
explicit-zero default. Pool CPU fields accept fractional cores and round up to
millicores; omitted pool capacity remains unlimited.

<a id="pool-systemd-placement"></a>

### Systemd placement

Conductor copies each referenced file to every associated runner/Builder
template's drop-in directory, using `50-kuasar-resource-pool-NNNN.conf`, then
reloads systemd. It removes stale files in this managed namespace and preserves
operator files. Reserve that namespace for Conductor. Files must be absolute
paths. With `units.install: false`, all template and drop-in installation remains
externally managed and referenced files are not read.

For example, after inspecting `lscpu -e` and
`/sys/devices/system/node/node0/cpulist`, a node-0 file might contain:

```ini
[Service]
CPUAffinity=
CPUAffinity=0-7
AllowedCPUs=
AllowedCPUs=0-7
NUMAPolicy=bind
NUMAMask=
NUMAMask=0
AllowedMemoryNodes=
AllowedMemoryNodes=0
```

CPU numbering is host-specific. Ancestor cgroups must allow the requested sets.
These constraints belong on the unit template before prewarming. Reloading does
not relocate existing processes. Sandbox enforcement stays in sandbox-ctl's VMM
cgroup; Conductor adds no Builder outer CPUQuota/MemoryMax/MemoryHigh limits.

<a id="pool-lifecycle-and-pressure"></a>

### Lifecycle, pressure and balancing

Pool identity is immutable while an execution owns resources. Pause fences the
old execution and releases resources; Resume can then select another pool.
Artifact preparation may precede knowing authoritative Capacity. If the prepared
task belongs elsewhere, the old preparation worker is stopped before clearing
its assignment, a replacement prepares in the selected pool, and the summaries
must agree before final launch. No VM starts on the preparation-only worker.

Builder API business quotas remain separate. The prelaunch reservation covers
the largest prospective serial phase; each actual phase replaces that claim
with its own Capacity in the same resource pool. The existing release fence
prevents the next phase from overlapping the old phase's runtime reservation.

Real memory-demand escalation, protected headroom, minimum running duration and
backoff remain per pool. A pause scan tries `Pause(ifIdle=true)` from oldest to
newest continuous running interval. Explicit busy advances the scan; if no idle
candidate succeeds, ordinary Pause retries the oldest eligible busy candidate.
Proxy failures, lifecycle changes and conflicts do not count as busy. The scan
consumes a pressure opportunity only after a candidate accepts the handshake;
the recovery obligation and placement intent commit before capture.

Periodic balancing additionally requires a **raw critical** source and a feasible
noncritical destination. Its persistent intent excludes the source during
recovery, including after restart. Losing destination supply leaves the sandbox
paused. Effective critical held solely by recovery obligations cannot keep
draining a pool whose raw pressure has fallen. If every pool is critical, only
new balancing stops; ordinary pressure work and budget-feasible recovery continue.

Outstanding recovery obligations prevent an already-critical pool from
downgrading; they never promote healthy pools. Background recovery checks the
full budget, available memory, target-pool competing/protected demand and startup
concurrency. It need not project below red and cannot create eviction demand for
itself. Legacy saved sources lacking a preflight budget still undergo exact
runtime Admit before starting a VM.

<a id="pool-compatibility-and-recovery"></a>

### Compatibility and recovery

Without `resource_listen.pools`, one `default` resource pool is used. A sole pool
without `allocatable.memory` retains the original physical-memory minus host
reservation and operational-margin formula. Multiple pools require explicit net
runtime memory and reject explicit old physical-memory/CPU/host-reserved/margin
fields. Automatically inserted defaults do not count as explicit old input,
including across Clone and YAML/JSON round trips or Configure hooks.

Explicit memory startup works with the resource controller omitted or disabled
as well as enabled. IMG and cold SBX launches use startup headroom; memory SNP
restores and memory Resume use sandboxer's authoritative snapshot budget.
Artifact preparation still validates startup against final artifact Capacity.

Omitted runner names are `runner-0`, `runner-1`, etc.; Builder names are
`builder-0`, `builder-1`, etc. Use explicit stable names before reordering pools.
Legacy unit/size fields remain supported when their corresponding list is absent.
Pool identity and actual unit names are journaled before unit start. Restart
reconstructs runner slots and declared-capacity claims before admission opens,
then reuses lease inventory and Sync to recover runtime reservations. Existing
over-limit instances remain accounted when configuration limits are lowered.
Templates and resource identities of live executions must remain configured.

A legacy live unit shared by multiple logical pools has no recoverable logical
pool identity. With unlimited runner capacity it is shown as unattributed;
enabling finite capacity in that ambiguous configuration fails startup rather
than guessing. Drain those instances before enabling strict limits. Unknown
managed consumers conservatively occupy their target runtime pool and its finite
scheduling envelope until verified or fenced.

`node-ctl resource pressure` includes `pools`, `runner_pools`, unattributed
runner counts, per-dimension utilization, capacity usage, startup usage, pressure
demands/holds and each obligated sandbox's source/destination. The resource wire
protocol and portable artifacts carry no host-specific pool IDs.

<a id="pool-regression"></a>

### Real-environment regression

With the usual prepared E2E `BIN`, `E2E_LIB`, `WORK`, `OUT`, execute image and
Zot inputs, run `test/e2e/manual/resource-pools.sh` with
`RESOURCE_POOL_NUMA_NODES="0 1"` on two actual memory-bearing NUMA nodes. It uses
the existing isolated E2E topology and checks Builder release, both pool ledgers,
VMM CPU/memory masks, conductor restart, durable balancing intent, guest process
and disk preservation, and ordinary cross-pool Resume. Use an isolated workbench
with KVM, systemd, cgroup v2 and disk-backed temporary storage. This manual test
requires two NUMA nodes; the source-gate systemd fixture also supports one node.

`test/e2e/cases/orchestrator.static-startup.sh` exercises controller-omitted and
disabled IMG, fresh SNP/SBX, memory Resume and cold Resume with explicit startup.
Allocator tests use virtual time for byte-per-second rates, including the actual
sandboxer reservation adapter and node wire server, without waiting days.

<a id="controller-design"></a>

## Overview

<a id="responsibility-boundaries"></a>

### Responsibility boundaries

Memory control comprises two independent flows:

```text
sandbox-local loop
  guest MemAvailable + CH target/current
    -> RequestedBudget
    -> optional RequestBudget reservation RPC
    -> memory.high / vm.resize / convergence

node reservation loop
  Admit / RequestBudget / StateSync / Release
    -> NodeReservation
    -> node aggregate / pool / zone / recovery
```

The normal reservation loop neither samples nor writes sandbox cgroups, calls CH APIs, receives guest `MemReport`, nor sends balloon targets. Recovery inventory does read process/cgroup identity, liveness and conservative limits ([Inventory and controller restart](#inventory-and-controller-restart)); it does not use those reads to implement the guest Budget loop. The controller atomically handles sandbox-initiated reservation requests. Heartbeat returns a reservation echo, not an execution command.

Builder registration/execution ledgers remain separate from these per-Sandbox reservations. Each actual Build phase uses ordinary sandbox-ctl admission and release. Build.Resources configures A/B resources; final Sandbox resources resolve independently. Generated Builder service/slice supplies ownership, delegation and group cleanup without deriving outer CPU/memory limits from Build business quotas. Explicit pool-placement drop-ins remain governed by [systemd placement](#pool-systemd-placement). The existing VMM leaf policy and conservative recovery inventory remain unchanged.

<a id="memory-multiplexing-model"></a>

### Memory multiplexing model

The controller is designed for a node where sandbox memory demand is typically uneven:
many sandboxes remain idle or lightly used for long periods while a smaller set becomes
memory-intensive for shorter intervals. The goal is to reuse memory released by idle
sandboxes across the node without treating every sandbox Capacity as continuously
resident or reserved, while keeping enough reaction margin that a burst can grow before
guest OOM causes workload loss.

The mechanism deliberately separates three roles:

- **Balloon/Budget is the sandbox memory-sizing actuator.** Balloon inflation reduces the
  guest Budget and is the operation that actually makes an idle sandbox relinquish guest
  memory. Balloon deflation increases Budget after growth has been authorized.
- **NodeReservation is the cross-sandbox allocation ledger.** Memory is reusable by other
  sandboxes only after a completed local shrink is committed as a smaller reservation.
  Host RSS or a lower `memory.high` does not by itself release node capacity.
- **`memory.high` is primarily a host-side throttling boundary, not the reclaim
  owner.** When VMM host charge exceeds `memory.high`, Linux subjects the cgroup to
  reclaim and throttling; resulting stalls and `memory.events.local` provide pressure
  feedback. Merely approaching the threshold does not trigger its throttling or `high`
  event. Slowing further growth is intended to give the Budget-growth loop time to obtain
  reservation and deflate the balloon. Kernel reclaim on this path is not the node's
  accounting event and does not substitute for balloon shrink plus reservation release.
  `memory.high` is not a hard cap and can be exceeded; see the
  [Linux cgroup v2 memory interface](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files).

This makes growth and shrink intentionally asymmetric. On the normal controlled path,
growth is safety-prioritized: reserve node capacity first, relax `memory.high`, then
 deflate the balloon. Shrink is conservative: inflate the balloon, observe safe current
progress, lower `memory.high`, then release reservation. Reclaiming idle memory may
therefore lag. Guest emergency `deflate_on_oom` is the explicit exception described in
[Terminology](#units-and-terms), not a normal density mechanism or an implicit node grant.

`Headroom` is the sandbox-local reaction margin, while the node's emergency pool is a
shared safety margin. Per-sandbox headroom helps cover detection and control latency;
the shared pool is reserved for high-urgency growth ([Runtime grants](#runtime-grants)), not ordinary growth. It does
not replace local headroom or guarantee that every burst obtains memory in time.
OOM/high-urgency handling is a last safety path, not the normal growth signal.

Under concurrent growth, sandboxes do not transfer memory directly to one another.
Each sandbox retains its own growth objective and requests reservation from the common
node pool. State serializes aggregate reservation changes so concurrent requests cannot
oversell the same headroom. Normal new grants are additionally shaped by the resource pool's
runtime grant token bucket; red/critical zones preserve node safety and the emergency
pool. Runtime growth has no node-side FIFO wait queue: a sandbox that receives a partial
or zero grant keeps its local objective and retries subject to the returned cooldown as
the local controller processes observations, pressure events and pending transactions.

The steady-state feedback model is therefore:

```text
idle sandbox
  guest MemAvailable rises
    -> balloon inflate
    -> observe safe smaller Budget
    -> memory.high down
    -> release NodeReservation
    -> node pool becomes reusable

bursting sandbox
  guest report or host pressure
    -> RequestBudget
    -> node atomically charges grant
    -> memory.high up
    -> balloon deflate
    -> guest receives more Budget

host pressure fast path
  VMM charge exceeds memory.high
    -> throttling (with kernel reclaim) + PSI/events feedback
    -> sandbox-local growth request
    -> reservation grant/cooldown
```

The design does not promise that every sandbox can reach Capacity simultaneously.
Capacity is the per-sandbox ceiling; NodeReservation is the amount currently promised
from the shared node pool. Avoiding guest OOM is the design objective, not an unconditional
guarantee when capacity, available grants or control-loop response time are insufficient.

<a id="core-invariants"></a>

### Core invariants

- `0 < NodeReservation <= Capacity`.
- `reservedMemory = sum(live NodeReservation)`.
- Memory pool usage, memory RawZone, ResourceProbe allocated memory and recovery replacement aggregate NodeReservation only. CPU utilization uses allocatable CPU, falling back to Capacity. Pool RawZone takes the maximum dimension; effective Zone also records qualified pressure and outstanding recovery obligations.
- Only the sandbox initiates growth; the node accounts for a grant before returning it.
- Only the sandbox submits shrink, after balloon current converges and memory.high is handled in the required order.
- `Settled` is a lifecycle fact; it does not derive or rewrite reservation from memory.current.
- Controller restart provisionally charges Capacity, then atomically replaces that charge after StateSync.
- Stale heartbeats, host charge and node administrative commands cannot alter an individual sandbox's reservation.

<a id="resource-model"></a>

## Node reservation model

<a id="pool"></a>

### Pool

For a sole pool without explicit net runtime memory, the legacy memory formula is:

```text
Physical           = NodeBudget status field
PostHostBudget      = saturating_sub(Physical, HostReserved)
OperationalMargin  = PostHostBudget * operational_margin_factor
AllocatablePool    = saturating_sub(PostHostBudget, OperationalMargin)
Reserved           = sum(NodeReservation)
MainHeadroom        = saturating_sub(AllocatablePool, Reserved + EmergencyPool)
StartupPool         = AllocatablePool * startup_factor
```

In this legacy mode, NodeBudget is the current status/wire name for configured or discovered physical resources. Do not treat it as an amount from which HostReserved was already subtracted. With explicit pool memory, each `allocatable.memory` is already net: it is not reduced again by the host reservation or operational margin. Multi-pool node summaries aggregate those net budgets; the legacy physical formula is not their interpretation.

On a shared host using the legacy single-pool mode, configure `resources.physical_memory` and `host_reserved` to
define this controller's assigned budget and leave room for other services.
`physical_memory: auto` reads host `MemTotal` during configuration; capacity
discovery does not establish exclusive ownership of the machine.
OperationalMargin remains statically excluded from P for operational headroom.
It is neither granted to sandboxes nor a threshold for host-wide availability.
The runtime node loop does not sample host `MemAvailable` or infer its resource
pool from memory left idle by unrelated services. Guest observations and the
sandbox-local budget loop remain separate and unchanged.

Resource subtraction saturates instead of underflowing. Individual reservation and aggregate updates occur in the same State critical section. Insertion/recovery replacement validates aggregate-addition overflow before modifying indices.

<a id="rawzone-and-effective-zone"></a>

### RawZone and effective Zone

For the memory dimension of one selected resource pool, let `R=Reserved`, `P=AllocatablePool`, `E=floor(P*emergency_factor)`,
`Ty=floor(P*low_factor)`, `Tr=floor(P*high_factor)` and `Tc=P-E`.
RawZone is green below Ty, yellow from Ty to below Tr, red from Tr to below
Tc, and critical at or above Tc. Zero pool fails preflight and is critical
if encountered defensively at runtime.

The pool's raw level is the maximum of its memory and CPU levels. CPU uses its
runtime accounting denominator and resolved dimension thresholds. The node
projection is critical if any pool is critical; otherwise it reports the lowest
pool level. Local operation admission uses the selected pool, so one unhealthy
pool does not deny admission into another healthy pool.

Effective Zone is the stateful authority of that resource pool. Reservation threshold
upgrades are immediate. Valid sustained memory failures may also upgrade
straight from green/yellow/red to critical. Request origin has no role:

| Effective Zone | Create, including snapshot templates | Ordinary resume | Resource-paused recovery | Resource Pause |
|---|---|---|---|---|
| green | Eligible | Eligible | Eligible | No |
| yellow | Eligible | Eligible | Eligible | No |
| red | No | Eligible | Eligible | No |
| critical | No | No | Eligible | Further qualified pressure, or eligible raw-critical balancing |

Eligibility still requires actual full initial memory, startup coordination,
rate tokens, and the existing authentication/ownership checks. Direct API,
Proxy, internal launch paths and node command execution use this same rule.
Cluster placement preferences belong to #46; yellow is not a node-side
cluster-create rejection mode. NodeList has no dynamic zone fields or aliases.

A failure round is a time-progressed recheck of the same valid demand and
launch/token identity. Concurrent duplicates, credential/shape errors,
impossible capacity, drain, transport failures, and startup-slot/rate-token
waits do not count. Three qualified rounds enter critical; three further
rounds authorize one Pause. Another Pause needs fresh rounds. Reservation
increments that do not increase sandboxer's executable Budget retain the
still-blocked demand and protection for its next executable step.
Demand freshness is independent of polling: its bounded
window is the maximum of 30 seconds, three failure intervals and two scan
intervals. This retains the guest's
five-second report/retry history; observations never add failure rounds.
Expired history restarts on a later request, and unused expired protection
cannot block admission even before the next scan. Expiration never clears a
reservation or recovery obligation. The local pressure query includes each
demand's nonsecret age, amount and failure/critical round counts.
Only valid memory-blocked demands in the managed pool advance pressure rounds.
Whole-host availability does not create a demand or authorize a resource Pause;
a critical Zone without a qualifying demand does not itself authorize pressure
Pause. [Periodic balancing](#pool-lifecycle-and-pressure) additionally
allows a raw-critical source with a feasible noncritical destination.

Q includes accepted capture intent, resource-pressure paused rows and their
starting resumes. Q>0 holds an already-critical pool even when RawZone is green;
it does not promote healthy pools. Exit requires
Q=0, no related unverified cleanup, RawZone below critical, and no active
qualified memory blockage throughout the exit hold. It exits only to red.
Red requires RawZone below red continuously for its own hold before yellow;
yellow requires continuously green RawZone for its hold before green. Rebound
resets the hold. One evaluation performs at most one downgrade. Restart loads
the journal and obligations before admission and restarts monotonic hold clocks.
Historical transition reasons such as `host_operational_margin` remain labels,
not reconstructed demands. Existing Q, cleanup barriers and reservation charges
survive the policy change and follow the same recovery and stepwise exit rules.

<a id="runtime-grants"></a>

### Runtime grants

RequestBudget is a reservation transaction with an absolute baseline and delta:

```text
request:  CurrentReservation, RequestedDelta, Urgency
response: GrantedDelta, NewReservation, Cooldown

NewReservation = CurrentReservation + GrantedDelta
0 <= GrantedDelta <= RequestedDelta
```

Normal new growth uses actual `P-R-E`, less another selected waiter's unused protection; high urgency can use E but cannot exceed P. Effective red/critical does not itself deny runtime growth. Already-charged replay is reused without another charge or rate debit. One selected resume/grow demand protects released headroom until admission, executable progress, cancellation or expiration of an unused hold. Resume protection has priority over grow; a protection never subtracts a live reservation on cancellation.

Starting capture or committing a paused row does not release grantable funds.
Only a safe shrink or confirmed Release reduces R; the freed budget can then
serve an eligible grow even while Q or an exit hold keeps effective critical.

Partial grants are allowed. Sandboxer accumulates reservation first and deflates the balloon only when it can represent a larger Budget with a 64 MiB-aligned balloon target, so rounding cannot create unreserved memory. Budget boundaries are relative to Capacity: at Capacity 1056 MiB they are 32/96/160/224/... MiB. A grant from 160 to 192 MiB retains the demand and protects the remaining 32 MiB; reaching 224 MiB is executable progress and clears that demand. Capacity itself need not be aligned.

Shrink uses the same message with RequestedDelta=0. After local balloon inflation, current convergence and ordered memory.high adjustment, the sandbox submits a smaller absolute baseline to release reservation. The node neither polls CH nor decides whether shrink has completed.

<a id="controller-protocol"></a>

## Reservation protocol

<a id="transport-and-authentication"></a>

### Transport and authentication

Sandboxer `pkg/resource` defines length-prefixed JSON over Unix sockets. Each sandbox uses a persistent connection and token. Controller restart/reconnect rebuilds sessions with lifecycle leases, SO_PEERCRED, managed pidfile/cgroup identity and StateSync.

<a id="current-messages"></a>

### Current messages

| Message | Direction | Node action |
|---|---|---|
| Admit | Sandbox → node. | Admit the full initial reservation. |
| Settled | Sandbox → node. | Change lifecycle stage only. |
| RequestBudget | Sandbox → node. | Reconcile absolute baseline and optionally grant growth. |
| OOMReport | Sandbox → node. | Count diagnostics; no direct reservation change. |
| Heartbeat | Sandbox → node. | Record liveness/host charge and echo reservation. |
| StateSync | Sandbox → node. | Replace provisional state with the sandbox's safe absolute baseline. |
| Release | Sandbox → node. | Delete reservation and aggregate charge. |
| AdminDrain/Status/List | Admin → node. | Admission drain and observation. |

There are no node-to-sandbox balloon/cgroup commands or administrative grant/reclaim operations.

<a id="existing-wire-field-semantics"></a>

### Existing wire-field semantics

The current implementation retains the existing reservation message shape; it does not add a separate Budget protocol. Interpret wire names as follows:

| Wire/Go name | Current meaning |
|---|---|
| `FloorMemoryBytes` | Settled HeadroomMemoryBytes. |
| `StartupBudgetMemory` | Cold InitialBudget aligned through the target calculation. |
| `AllocatableAtSnapshot` | Restore BudgetAtSnapshot. |
| `GrantedInitialAlloc` | Full InitialBudget. |
| `CurrentAlloc` | Sandbox's safe absolute NodeReservation baseline. |
| `NewAllocatable` | NodeReservation after the node transaction / heartbeat echo. |
| `AppliedAllocatableMemory` | Safe NodeReservation baseline in StateSync. |
| `CurrentRSS` | Diagnostic HostMemoryCurrent, the host VMM cgroup charge. |

These fields do not mean guest RSS, demand, balloon current or a memory.high target. Keeping the message shape is the architecture boundary, not two old/new semantic paths. The implementation has no negotiation, version gate, alias decoder or mixed-version branch for this model; this does not establish an arbitrary cross-version compatibility guarantee.

<a id="lifecycle"></a>

## Lifecycle

<a id="cold-admission"></a>

### Cold admission

```text
sandbox resolve H/Hs/C
  -> InitialBudget = aligned Hs
  -> Admit exact InitialBudget
  -> node atomically charges it or queue/reject
  -> sandbox starts CH with command-line balloon target
  -> launch_ack -> Settled
  -> fresh report drives sandbox-local steady policy
```

The node cannot partially admit initial memory. Before the first report, host memory.current does not determine Budget. Settled clears startup-in-flight charge without changing NodeReservation.

<a id="restore-admission"></a>

### Restore admission

```text
sandbox reads Capacity,target,current from snapshot
  -> BudgetAtSnapshot = Capacity - min(target,current)
  -> Admit exact BudgetAtSnapshot
  -> CH spawn/resume -> restore ACK -> MUX
  -> sandbox-local normalization/new epoch/steady loop
```

The node neither uses startup headroom nor reads snapshot balloon state, and it does not trigger adjustment before ACK/MUX. It sees only the exact initial reservation submitted by the sandbox.

<a id="runtime-shrinkgrow"></a>

### Runtime shrink/grow

Cross-boundary growth ordering:

```text
sandbox RequestBudget -> node charge/grant -> response
sandbox memory.high up -> balloon deflate
```

Cross-boundary shrink ordering:

```text
sandbox balloon inflate -> wait current -> memory.high down
sandbox RequestBudget(smaller baseline, delta=0) -> node release
```

Node and sandbox do not share a state machine. Reservation requests/responses are their sole coordination boundary.

<a id="resource-pause-recovery-and-explicit-adoption"></a>

### Resource Pause, recovery and explicit adoption

One capture worker scans oldest to newest current continuous-running interval,
not CreatedUnix, trying Pause(ifIdle=true). Explicit busy continues the scan;
if no idle candidate succeeds, ordinary Pause retries the oldest eligible busy
candidate. Errors/conflicts are not busy. Running commit resets the interval.
It uses the existing
memory Pause: native exec quiesce/cleanup, Snapshot capture, old VM exit and
runner/network/resource cleanup. It adds no retained-VMM pause, page swapper,
exec preservation or new snapshot format. Checkpoint storage must be disk
backed; tmpfs/ramfs is rejected. Only observed Release/reconciliation makes
memory reusable. Capture failure backs off without discarding a saved source.

One background recovery worker services the oldest outstanding obligation,
using the common SID launch group also used by external Wake. A bounded settling
interval at worker startup and after its own capture/recovery completions paces
preparation. Unrelated reservation changes do not restart that interval: a busy
node must not require global allocation silence to recover an unattended sandbox.
`headroom_stable` remains a diagnostic, not a recovery-admission prerequisite.
Sandboxer subsequently supplies exact I, and final resource admission waits for
its complete budget before VM start; the pacing timer does not grant memory.
Background recovery checks available memory, target-pool demand/protection and
startup concurrency, without requiring projected RawZone below red. It cannot
trigger a new eviction for its own budget wait. See [resource pools](#resource-pools)
for configuration, multi-dimensional watermarks and cross-pool balancing.
Starting remains in Q until actual running commit. The minimum running window
limits immediate re-eviction. If all otherwise eligible candidates remain in
that window, selection waits for it to expire; host-wide availability cannot
bypass the configured protection.
A valid demand can coordinate further Pause while effective Zone stays critical;
recovery never waits for a downgrade that Q itself prevents. Missing/corrupt
artifacts, full/slow storage and capacity failures retain the obligation/source
and appear in operation diagnostics. Existing deadline termination and explicit
Delete cancel recovery intent through the same lifecycle owner.

An authenticated default memory Pause of an already resource-paused sandbox
adopts its existing Snapshot as ordinary explicit paused state. The Hook runs
and its full-record precondition is rechecked after lock reacquisition. One
narrow durable CAS changes reason/obligation/version, preserving IDs, source,
credentials and remaining cleanup. It neither captures again nor starts a VM.
It cancels unused resource-recovery holds and the critical exemption exactly
once, then publishes even though state is still paused. Explicit capture options
(including explicit false values) or filesystem-only capture are rejected:
the retained source cannot certify a new capture action. Hooks cannot silently
remove such an action to turn it into adoption. Ordinary repeated Pause still
returns 409; starting still conflicts. A later real Wake follows ordinary
resume rules, with no permanent user-paused lock. Adoption is not Release and
cannot bypass the critical exit conditions.

Physical release checks distinguish the old VMM's shared-memory charge from
Snapshot file cache. After the old process exits, disk-backed Snapshot pages
may remain cached or dirty; `MemFree` need not rise by the old charge. Observe
the old process/cgroup identity, confirmed reservation release, host
`MemAvailable` and storage writeback separately during isolated validation.
These whole-host measurements are test diagnostics, not node control inputs.
The node never treats a Snapshot's file length or a predicted guest working set
as released headroom.

Sandbox fields and the small transition journal share the existing SQLite
owner. The node restores them with resource inventory before serving. Resource
hot paths perform no capture, filesystem/network I/O or SQLite calls under
State.mu. The admission queue precedes State; lifecycle callbacks never run
under State. Low-frequency lifecycle/zone commits persist; grants/heartbeats
do not rewrite whole durable state. Local routesync, SHM and extension
projections carry reason, obligation, version and running interval as facts.

<a id="admission-and-pressure"></a>

## Admission and scheduling projection

<a id="admission"></a>

### Admission

Cold starts use StartupBudgetMemory; memory restore uses the unchanged
AllocatableAtSnapshot wire field, whose value is sandboxer's authoritative
BudgetAtSnapshot. No RSS, compressed length, guest peak, or expected task
completion substitutes for that value. Initial memory is always admitted in
full under the same State mutex as aggregate mutation and runtime grants.

The orchestrator assigns operation class from the final durable source and
resolved launch mode. Its in-process launch lookup verifies the resource
connection's peer PID against the current runner pidfile and its live POSIX
lock on the same open file. RPC fields,
metadata, template kind, urgency and Origin cannot grant recovery authority.
A fresh snapshot-template create remains Create. A durable starting operation
retains its accepted identity and asynchronous lifecycle if Zone changes; a
later resource wait is not reported as a no-effect rejection of that operation.

Ordinary creates require `I <= StartupPool`, sufficient `P-R-E`, available
startup budget and a rate token. Legitimate memory resumes retain a serialized
complete-budget lane: no other startup may be in flight, the actual I is
charged without truncation, and no concurrent startup enters until Settled
or confirmed Release. This permits `I > StartupPool` and, when necessary,
`I > P-E`, provided `R+I <= P` after other protections. Host/operational margin
is outside P and is never granted. The capability belongs to a validated saved
source and survives explicit Pause adoption; the critical exemption belongs
only to the current resource memory-recovery obligation. An explicit cold
resume follows ordinary resume policy and receives no saved-memory budget
exception. `I>P` remains an explicit
capacity failure with the saved source retained.

Memory budgets, startup concurrency, competing demands and protections in these
checks belong to the selected resource pool. The request admission token bucket
remains node-wide. A scheduling-capacity claim and an available worker slot do
not replace exact runtime memory Admit.

The bounded admission queue revisits all entries in FIFO age order rather
than stopping behind a newly ineligible create or a memory-blocked head.
Every recheck resolves the current launch identity. Timeout or stale work can
cancel only that identity's unused protection. Startup/rate waits preserve
resume protection without accumulating memory-failure rounds. Replay and
StateSync replace the existing charge atomically; disconnect, TTL or ambiguous
cleanup cannot turn a charged consumer into free memory.

<a id="resourceprobe-and-cluster-load"></a>

### ResourceProbe and cluster load

ResourceProbe.Allocated and cluster projected memory load use reservedMemory, not host charge. E2B memoryMB still means Capacity/SKU.

Per-sandbox resource statistics use the current sandbox-ctl owner independently of the controller's heartbeat/reservation path:

- `cpuCapacity`: effective capacity in cores; `cpuAllocatable`: the relative scheduling specification mapped to `cpu.weight`, without a hard fractional-core quota or performance guarantee.
- `memoryCapacity`: effective Capacity in bytes; `memoryHeadroom`: final `resources.allocatable.memory`, the balloon headroom, not Budget, guest free memory or NodeReservation.
- `memoryReserved`: current NodeReservation when the dynamic controller has an observation; omitted otherwise.
- `memoryUsed`: host VMM `memory.current`; `cpuSeconds`: the same cgroup's `cpu.stat.usage_usec / 1e6`. No ctl/guest CPU addition, inactive-file/balloon subtraction or guest-capacity clipping occurs.
- `timestampUnix`: actual observation time; missing host fields are independently omitted and valid zeros remain visible. A source rebuild may reset CPU seconds; native usage owns lifecycle accumulation.

These reads work in static/dynamic mode with usage or telemetry disabled. They do not alter RequestBudget, admission, recovery charge, heartbeat sampling or the existing build-phase reservation release fence. The native API removes `cpuCount`/`memTotal`/`memAllocatable`/`memUsed`; reservation and headroom now have separate explicit names. [Instantaneous resource and traffic stats](observability.md#native-stats-api) defines response validity, errors and numeric precision. E2B compatibility metrics keep their own field names and meanings.

<a id="recovery"></a>

## Reliability

<a id="inventory-and-controller-restart"></a>

### Inventory and controller restart

Lifecycle leases retain sandbox identity, Capacity, headroom, cold-start headroom, cgroup identity and feature information. At startup, inventory scans leases, managed pidfiles and configured cgroup roots:

Before this inventory, Conductor restores durable logical/resource-pool identities,
worker slots and declared-capacity claims. Inventory then charges the appropriate
runtime pool; ambiguous or unknown managed consumers cannot be treated as free.
The full compatibility rules are in [pool recovery](#pool-compatibility-and-recovery).

1. Provisionally charge known leases at Capacity.
2. Charge live cgroups with incomplete identity at a provable conservative upper bound.
3. Receive StateSync when each sandbox reconnects.
4. Within one State critical section, remove provisional charge and insert the reported NodeReservation.

The controller does not read the old persistent state schema or reconstruct guest Budget from memory.current. Read-only recovery checks include cgroup.events, cgroup.procs, process cgroup identity and memory.max; these establish liveness/conservative accounting, not a second guest-memory controller.

<a id="lost-responses-and-reconnects"></a>

### Lost responses and reconnects

- If a growth response is lost, the sandbox has not raised high/deflated based on that response and retains its old local baseline. StateSync atomically replaces provisional node charge without undercounting.
- If a shrink-commit response is lost, balloon current has already converged and memory.high is lower, so the sandbox uses the completed smaller baseline. If the node committed, both agree; otherwise StateSync completes the release. Reusing the old larger baseline could let the sandbox reuse headroom the node already released and reassigned.
- A heartbeat mismatch triggers reconnect/StateSync; its echo is not an execution command.
- Stale heartbeats and host charge can update diagnostic times/values only, never reservation.

<a id="reservation-lifecycle"></a>

### Reservation lifecycle

Startup TTL cleans failed creations that never reached Settled. Heartbeat timeout, Release and inventory cleanup atomically remove indices/aggregate charge by reservation identity/token. Unknown or provisional reservations cannot use ordinary growth transactions.

<a id="performance"></a>

## Performance and observability

The node-wide admission token bucket bounds creation bursts; each resource pool's startup budget bounds memory reserved for creating/restoring sandboxes, and its runtime-grant token bucket bounds normal growth. High urgency can use that pool's emergency allowance. The allocator may return cooldown, with retries driven by later sandbox observations/pressure events.

Observe reservation/recovery state with resource status/list, the local `resource pressure` query, and existing node status production. ResourceProbe returns effective Zone and the same reservation R/P; consumers must not reconstruct effective Zone from R/P. Abnormal queue diagnostics use conductor's standard logs, collected and retained by the deployment. Key measures are:

- Reserved memory / pool / zone.
- Startup in-flight.
- Provisional/unknown reservation counts.
- Per-sandbox HostMemoryCurrent and last-report time.
- Grant/cooldown and admission queue latency.

Do not combine these into one undifferentiated memory-usage value: reservation, host VMM charge and guest demand are separate measures.

Lifecycle cumulative usage is read through [native stats/usage](observability.md#native-usage), with node policy applied to all three launch paths. Current VMM counters, lifecycle totals and historical E2B gauges retain separate meanings.

<a id="related-contracts"></a>

## See also

- [node](node.md).
- [sandboxer sandbox](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/runtime.md).
- [cluster](cluster.md).
- [sandboxer/pkg/resource](https://github.com/kuasar-sandbox/sandboxer/tree/main/pkg/resource).
- [internal/nodectl](../internal/nodectl).
- [internal/sandboxcfg/resource.go](../internal/sandboxcfg/resource.go).
