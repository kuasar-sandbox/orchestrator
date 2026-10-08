[English](node-resource.md) | [简体中文](node-resource_zh.md)

# node-resource — node reservation controller

`node-ctl conductor serve` optionally embeds the node resource controller. It owns node-side reservation, admission, pools, watermarks, recovery inventory, pressure episodes and statistics projection. The pressure worker coordinates the existing full memory Pause/Resume lifecycle through the orchestrator. Sandboxer owns the sandbox-local loop for guest memory observations, Cloud Hypervisor ballooning, `memory.high` and cold/restore/snapshot lifecycle; those operations do not belong to the node controller.

## 1. Overview

### 1.1 Responsibility boundaries

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

The normal reservation loop neither samples nor writes sandbox cgroups, calls CH APIs, receives guest `MemReport`, nor sends balloon targets. Recovery inventory does read process/cgroup identity, liveness and conservative limits (§8.1); it does not use those reads to implement the guest Budget loop. The controller atomically handles sandbox-initiated reservation requests. Heartbeat returns a reservation echo, not an execution command.

Builder registration/execution ledgers remain separate from these per-Sandbox reservations. Each actual Build phase uses ordinary sandbox-ctl admission and release. Build.Resources configures A/B resources; final Sandbox resources resolve independently. Builder service/slice only supplies ownership, delegation and group cleanup, with no orchestrator CPU/memory enforcement. The existing VMM leaf policy and conservative recovery inventory remain unchanged.

### 1.2 Memory multiplexing model

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
§1.3, not a normal density mechanism or an implicit node grant.

`Headroom` is the sandbox-local reaction margin, while the node's emergency pool is a
shared safety margin. Per-sandbox headroom helps cover detection and control latency;
the shared pool is reserved for high-urgency growth (§4.3), not ordinary growth. It does
not replace local headroom or guarantee that every burst obtains memory in time.
OOM/high-urgency handling is a last safety path, not the normal growth signal.

Under concurrent growth, sandboxes do not transfer memory directly to one another.
Each sandbox retains its own growth objective and requests reservation from the common
node pool. State serializes aggregate reservation changes so concurrent requests cannot
oversell the same headroom. Normal new grants are additionally shaped by the node-wide
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

### 1.3 Terminology

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

### 1.4 Core invariants

- `0 < NodeReservation <= Capacity`.
- `reservedMemory = sum(live NodeReservation)`.
- Pools, RawZone, ResourceProbe allocated memory and recovery replacement aggregate NodeReservation only. Effective Zone also records qualified pressure and outstanding recovery obligations.
- Only the sandbox initiates growth; the node accounts for a grant before returning it.
- Only the sandbox submits shrink, after balloon current converges and memory.high is handled in the required order.
- `Settled` is a lifecycle fact; it does not derive or rewrite reservation from memory.current.
- Controller restart provisionally charges Capacity, then atomically replaces that charge after StateSync.
- Stale heartbeats, host charge and node administrative commands cannot alter an individual sandbox's reservation.

## 2. Command-line interface

Conductor starts the controller through `resource_listen`; there is no standalone controller daemon subcommand. Administration provides observation and admission drain:

```text
node-ctl resource status [--socket PATH]
node-ctl resource list   [--socket PATH]
node-ctl resource drain  [--socket PATH] [--disable]
node-ctl resource pressure [--socket CONTROL_SOCKET]
```

`status` shows node budget, host reserved, operational margin, allocatable pool, reserved memory, startup in-flight, zone and recovery counts. `list` shows each sandbox reservation. `drain` prevents new admission without altering live reservations.

`pressure` queries `GET /internal/admin/resource-pressure` on the conductor control socket (or `NODE_CTL_SOCKET`), using the existing local admin authentication. It reports effective/RawZone, transition reason/version/time, R/P/E, hold remaining, protected funds, Q by phase, cleanup barriers, oldest wait, per-sandbox pause reason/running interval, and a bounded recent operation history. The resource reservation protocol remains unchanged. The former `host_safety_blocked` diagnostic field is no longer reported; this query does not measure whole-host memory safety.

`worker_blocked=no_eligible_running_sandbox` explains a pressure episode with
no safe running candidate; the worker waits without releasing another owner's
charge.

There is no `resource grant` or `resource reclaim`. Headroom is a sandbox-policy input: select it through supported sandbox configuration/lifecycle inputs, and let the sandbox's existing loop converge from observations. This does not introduce a node command for live policy reload or direct balloon/cgroup adjustment.

## 3. Configuration

### 3.1 Sandbox resource policy

```yaml
sandbox:
  resources:
    capacity: { cpu: 2, memory: 2GiB }
    allocatable:
      memory: 256MiB
    startup: { memory: 2GiB }
    overhead: { memory: 32MiB }
    watermark_high: { ratio: 0.875 }
```

- `capacity.memory` is Capacity.
- `allocatable.memory` is settled headroom, default `256MiB`. If an inherited default exceeds final Capacity, the resolver clamps it to Capacity. Explicit out-of-range request values are rejected.
- `startup.memory` is cold-start headroom, defaulting to final Capacity in both static and dynamic modes. It is independent of settled headroom and need not be greater or smaller.
- `overhead.memory` is node-owned host VMM overhead. Sandbox-ctl occupies a separate control cgroup and does not consume that allowance. `watermark_high.ratio` also belongs to node policy. Neither may be set by requests/templates.
- `0 < allocatable.memory <= capacity.memory`.
- `0 < startup.memory <= capacity.memory`.
- `0 < watermark_high.ratio < 1`, default `0.875`.

Restore obtains Capacity from the snapshot. StartupHeadroom does not participate in restore admission: sandboxer's BudgetAtSnapshot is the sole initial reservation.

A synchronous restore request validates only the portable patch structure. After the runner binds the exact run-id, its task reads the root snapshot.cfg in-process. For a Manifest Bundle root, it first reads only the metadata prefix and supplements located-source mappings from flat bundle/refs metadata; it does not open or recursively scan refs Bundles. The task returns Capacity as a nonsecret summary to the unique launch worker; conductor does not open the snapshot. Explicit request Capacity can act as an assertion. A mismatch or unreadable snapshot becomes an asynchronous `resource_resolve` failure, before network Attach, sandbox YAML, controller Admit or VM startup, with no fallback to node defaults. Sandboxer still computes BudgetAtSnapshot from CH snapshot target/current; orchestrator does not probe or derive it.

### 3.2 resource_listen

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

`resource_listen.socket` is the sole endpoint configuration. Node-ctl binds an absolute path and canonicalizes parent-directory symlinks into the identity shared by owner lock, lease inventory and sandbox client. A symlink at the final socket, dangling path or ambiguous alias fails closed. Sandbox YAML does not carry `control.cgroup_path`; the runner injects that host capability through an inherited cgroup FD. `resource_listen.state_path` is deprecated and ignored; it does not enable state.json recovery (§8.1).

### 3.3 Request/template ownership

Tenant resource patches allow only `capacity`, `allocatable` and `startup`. The node resolver owns `control`, `overhead`, `watermark_high`, sensors and deflate_on_oom. The parser rejects unknown or unauthorized fields instead of silently ignoring them.

## 4. Node reservation model

### 4.1 Pool

```text
Physical           = NodeBudget status field
PostHostBudget      = saturating_sub(Physical, HostReserved)
OperationalMargin  = PostHostBudget * operational_margin_factor
AllocatablePool    = saturating_sub(PostHostBudget, OperationalMargin)
Reserved           = sum(NodeReservation)
MainHeadroom        = saturating_sub(AllocatablePool, Reserved + EmergencyPool)
StartupPool         = AllocatablePool * startup_factor
```

NodeBudget is the current status/wire name for configured or discovered physical resources. Do not treat it as an amount from which HostReserved was already subtracted.

On a shared host, configure `resources.physical_memory` and `host_reserved` to
define this controller's assigned budget and leave room for other services.
`physical_memory: auto` reads host `MemTotal` during configuration; capacity
discovery does not establish exclusive ownership of the machine.
OperationalMargin remains statically excluded from P for operational headroom.
It is neither granted to sandboxes nor a threshold for host-wide availability.
The runtime node loop does not sample host `MemAvailable` or infer its resource
pool from memory left idle by unrelated services. Guest observations and the
sandbox-local budget loop remain separate and unchanged.

Resource subtraction saturates instead of underflowing. Individual reservation and aggregate updates occur in the same State critical section. Insertion/recovery replacement validates aggregate-addition overflow before modifying indices.

### 4.2 RawZone and effective Zone

Let `R=Reserved`, `P=AllocatablePool`, `E=floor(P*emergency_factor)`,
`Ty=floor(P*low_factor)`, `Tr=floor(P*high_factor)` and `Tc=P-E`.
RawZone is green below Ty, yellow from Ty to below Tr, red from Tr to below
Tc, and critical at or above Tc. Zero pool fails preflight and is critical
if encountered defensively at runtime.

Effective Zone is the single stateful node authority. Reservation threshold
upgrades are immediate. Valid sustained memory failures may also upgrade
straight from green/yellow/red to critical. Request origin has no role:

| Effective Zone | Create, including snapshot templates | Ordinary resume | Resource-paused recovery | Resource Pause |
|---|---|---|---|---|
| green | Eligible | Eligible | Eligible | No |
| yellow | Eligible | Eligible | Eligible | No |
| red | No | Eligible | Eligible | No |
| critical | No | No | Eligible | After further qualified pressure |

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
a critical Zone without a qualifying demand does not itself authorize Pause.

Q includes accepted capture intent, resource-pressure paused rows and their
starting resumes. Q>0 keeps critical even when RawZone is green. Exit requires
Q=0, no related unverified cleanup, RawZone below critical, and no active
qualified memory blockage throughout the exit hold. It exits only to red.
Red requires RawZone below red continuously for its own hold before yellow;
yellow requires continuously green RawZone for its hold before green. Rebound
resets the hold. One evaluation performs at most one downgrade. Restart loads
the journal and obligations before admission and restarts monotonic hold clocks.
Historical transition reasons such as `host_operational_margin` remain labels,
not reconstructed demands. Existing Q, cleanup barriers and reservation charges
survive the policy change and follow the same recovery and stepwise exit rules.

### 4.3 Runtime grants

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

## 5. Reservation protocol

### 5.1 Transport and authentication

Sandboxer `pkg/resource` defines length-prefixed JSON over Unix sockets. Each sandbox uses a persistent connection and token. Controller restart/reconnect rebuilds sessions with lifecycle leases, SO_PEERCRED, managed pidfile/cgroup identity and StateSync.

### 5.2 Current messages

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

### 5.3 Existing wire-field semantics

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

## 6. Lifecycle

### 6.1 Cold admission

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

### 6.2 Restore admission

```text
sandbox reads Capacity,target,current from snapshot
  -> BudgetAtSnapshot = Capacity - min(target,current)
  -> Admit exact BudgetAtSnapshot
  -> CH spawn/resume -> restore ACK -> MUX
  -> sandbox-local normalization/new epoch/steady loop
```

The node neither uses startup headroom nor reads snapshot balloon state, and it does not trigger adjustment before ACK/MUX. It sees only the exact initial reservation submitted by the sandbox.

### 6.3 Runtime shrink/grow

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

### 6.4 Resource Pause, recovery and explicit adoption

One capture worker selects the longest current continuous-running interval,
not CreatedUnix. Running commit resets that interval. It uses the existing
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

## 7. Admission and scheduling projection

### 7.1 Admission

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

The bounded admission queue revisits all entries in FIFO age order rather
than stopping behind a newly ineligible create or a memory-blocked head.
Every recheck resolves the current launch identity. Timeout or stale work can
cancel only that identity's unused protection. Startup/rate waits preserve
resume protection without accumulating memory-failure rounds. Replay and
StateSync replace the existing charge atomically; disconnect, TTL or ambiguous
cleanup cannot turn a charged consumer into free memory.

### 7.2 ResourceProbe and cluster load

ResourceProbe.Allocated and cluster projected memory load use reservedMemory, not host charge. E2B memoryMB still means Capacity/SKU.

Per-sandbox resource statistics use the current sandboxer lifecycle owner independently of the controller's heartbeat/reservation path:

- `cpuCapacity`: effective capacity in cores; `cpuAllocatable`: the relative scheduling specification mapped to `cpu.weight`, without a hard fractional-core quota or performance guarantee.
- `memoryCapacity`: effective Capacity in bytes; `memoryHeadroom`: final `resources.allocatable.memory`, the balloon headroom, not Budget, guest free memory or NodeReservation.
- `memoryReserved`: current NodeReservation when the dynamic controller has an observation; omitted otherwise.
- `memoryUsed`: host VMM `memory.current`; `cpuSeconds`: the same cgroup's `cpu.stat.usage_usec / 1e6`. No ctl/guest CPU addition, inactive-file/balloon subtraction or guest-capacity clipping occurs.
- `timestampUnix`: actual observation time; missing host fields are independently omitted and valid zeros remain visible. A source rebuild may reset CPU seconds; native usage owns lifecycle accumulation.

These reads work in static/dynamic mode with usage or telemetry disabled. They do not alter RequestBudget, admission, recovery charge, heartbeat sampling or the existing build-phase reservation release fence. The native API removes `cpuCount`/`memTotal`/`memAllocatable`/`memUsed`; reservation and headroom now have separate explicit names. [Node §4.1.1](node.md#411-instantaneous-resource-and-traffic-stats) defines response validity, errors and numeric precision. E2B compatibility metrics keep their own field names and meanings.

## 8. Reliability

### 8.1 Inventory and controller restart

Lifecycle leases retain sandbox identity, Capacity, headroom, cold-start headroom, cgroup identity and feature information. At startup, inventory scans leases, managed pidfiles and configured cgroup roots:

1. Provisionally charge known leases at Capacity.
2. Charge live cgroups with incomplete identity at a provable conservative upper bound.
3. Receive StateSync when each sandbox reconnects.
4. Within one State critical section, remove provisional charge and insert the reported NodeReservation.

The controller does not read the old persistent state schema or reconstruct guest Budget from memory.current. Read-only recovery checks include cgroup.events, cgroup.procs, process cgroup identity and memory.max; these establish liveness/conservative accounting, not a second guest-memory controller.

### 8.2 Lost responses and reconnects

- If a growth response is lost, the sandbox has not raised high/deflated based on that response and retains its old local baseline. StateSync atomically replaces provisional node charge without undercounting.
- If a shrink-commit response is lost, balloon current has already converged and memory.high is lower, so the sandbox uses the completed smaller baseline. If the node committed, both agree; otherwise StateSync completes the release. Reusing the old larger baseline could let the sandbox reuse headroom the node already released and reassigned.
- A heartbeat mismatch triggers reconnect/StateSync; its echo is not an execution command.
- Stale heartbeats and host charge can update diagnostic times/values only, never reservation.

### 8.3 Reservation lifecycle

Startup TTL cleans failed creations that never reached Settled. Heartbeat timeout, Release and inventory cleanup atomically remove indices/aggregate charge by reservation identity/token. Unknown or provisional reservations cannot use ordinary growth transactions.

## 9. Performance and observability

The admission token bucket bounds creation bursts; startup pool bounds memory reserved for creating/restoring sandboxes; the runtime-grant token bucket bounds aggregate normal growth rate. High urgency can use emergency pool. The allocator may return cooldown, with retries driven by later sandbox observations/pressure events.

Observe reservation/recovery state with resource status/list, the local `resource pressure` query, and existing node status production. ResourceProbe returns effective Zone and the same reservation R/P; consumers must not reconstruct effective Zone from R/P. Abnormal queue diagnostics use conductor's standard logs, collected and retained by the deployment. Key measures are:

- Reserved memory / pool / zone.
- Startup in-flight.
- Provisional/unknown reservation counts.
- Per-sandbox HostMemoryCurrent and last-report time.
- Grant/cooldown and admission queue latency.

Do not combine these into one undifferentiated memory-usage value: reservation, host VMM charge and guest demand are separate measures.

Lifecycle cumulative usage is read through [native stats/usage](node.md#native-usage), with node policy applied to all three launch paths. Current VMM counters, lifecycle totals and historical E2B gauges retain separate meanings.

## 10. See also

- [node](node.md).
- [sandboxer sandbox](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox.md).
- [cluster](cluster.md).
- [sandboxer/pkg/resource](https://github.com/kuasar-sandbox/sandboxer/tree/main/pkg/resource).
- [internal/nodectl](../internal/nodectl/).
- [internal/sandboxcfg/resource.go](../internal/sandboxcfg/resource.go).
