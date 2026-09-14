[English](node-resource.md) | [简体中文](node-resource_zh.md)

# node-resource — node reservation controller

`node-ctl conductor serve` optionally embeds the node resource controller. It owns node-side reservation, admission, pools, watermarks, recovery inventory and statistics projection. Sandboxer owns the sandbox-local loop for guest memory observations, Cloud Hypervisor ballooning, `memory.high` and cold/restore/snapshot lifecycle; those operations do not belong to the node controller.

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

### 1.2 Terminology

| Name | Definition | Owner |
|---|---|---|
| Capacity | Fixed maximum VM memory. | Sandbox config/snapshot. |
| Headroom | `resources.allocatable.memory`, settled guest headroom. | Sandbox policy. |
| StartupHeadroom | `resources.startup.memory`, headroom before the first trusted cold-start report. | Sandbox policy. |
| BudgetAtSnapshot | `Capacity - min(snapshot target,snapshot current)`. | Sandbox snapshot. |
| NodeReservation | Absolute memory amount reserved by the node for one sandbox. | Node controller. |
| HostMemoryCurrent | Host VMM cgroup `memory.current`; diagnostic only. | Reported by sandbox, recorded by node. |
| reservedMemory | Sum of all live `NodeReservation` values. | Node state. |

Headroom is not the total Budget. CPU `allocatable` still expresses scheduling weight/guarantee; it is not structurally identical to memory headroom.

Sandbox-local state also includes `TargetBudget`, `CurrentBudget`, `ObservedBudget` and `DemandMemory`; the node neither needs nor stores them. On the normal controlled path, the sandbox obtains enough NodeReservation before increasing Budget and releases reservation only after shrink converges. Emergency guest `deflate_on_oom` is an exception to the phased soft guarantee: it changes neither target nor reservation. Existing `memory.high` continues bounding host VMM charge, while the sandbox marks target/current unstable and prohibits shrink. Snapshot still computes BudgetAtSnapshot from the safe upper bound of both sides.

### 1.3 Core invariants

- `0 < NodeReservation <= Capacity`.
- `reservedMemory = sum(live NodeReservation)`.
- Admission, pools, zones, ResourceProbe, cluster projected load and recovery replacement aggregate NodeReservation only.
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
```

`status` shows node budget, host reserved, operational margin, allocatable pool, reserved memory, startup in-flight, zone and recovery counts. `list` shows each sandbox reservation. `drain` prevents new admission without altering live reservations.

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
```

`operational_margin_factor` reserves a node safety margin from the post-host budget. `emergency_factor` reserves pool capacity for high-urgency growth. `startup_factor` bounds aggregate reservations for concurrent creation/restoration. Admission rate/burst form a request token bucket, not memory Budget.

Preflight requires `host_reserved.memory < physical_memory` and validates `0 <= operational_margin_factor < 1`, `0 <= low_factor < high_factor < 1`, `0 <= emergency_factor < startup_factor <= 1` and `0 < memory_grant_per_sec_factor <= 1`. Invalid values fail before pool calculation, unsigned subtraction or cluster-load projection.

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

Resource subtraction saturates instead of underflowing. Individual reservation and aggregate updates occur in the same State critical section. Insertion/recovery replacement validates aggregate-addition overflow before modifying indices.

### 4.2 Zone

Zone derives solely from Reserved / AllocatablePool:

| Zone | Meaning |
|---|---|
| green | Normal admission and growth. |
| yellow | Conservative operation; policy can still grant. |
| red | Reject new admission and defer non-high-urgency growth. |
| critical | Retain only safety/high-urgency request paths. |

Zone does not consume MemAvailable, balloon current, memory.current or sandbox lifecycle details.

### 4.3 Runtime grants

RequestBudget is a reservation transaction with an absolute baseline and delta:

```text
request:  CurrentReservation, RequestedDelta, Urgency
response: GrantedDelta, NewReservation, Cooldown

NewReservation = CurrentReservation + GrantedDelta
0 <= GrantedDelta <= RequestedDelta
```

Partial grants are allowed. Sandboxer accumulates reservation first and deflates the balloon only when it can represent a 64 MiB-aligned Budget, so rounding cannot create unreserved memory.

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

## 7. Admission and scheduling projection

### 7.1 Admission

Cold uses StartupBudgetMemory; restore uses AllocatableAtSnapshot. The selected InitialBudget must satisfy all of the following:

- It does not exceed sandbox Capacity.
- It fits the per-request limits of AllocatablePool and startup pool.
- Current main headroom and startup headroom suffice.
- Node is not drained and zone is not red/critical.
- An admission token is available.

Temporary shortages can enter the FIFO queue. Requests beyond node limits, or disallowed by node-protection conditions, are rejected. No path proceeds with a smaller initial grant for startup/restore.

### 7.2 ResourceProbe and cluster load

ResourceProbe.Allocated and cluster projected memory load use reservedMemory, not host charge. E2B memoryMB still means Capacity/SKU.

Per-sandbox resource statistics use the current sandbox-ctl owner independently of the controller's heartbeat/reservation path:

- `cpuCapacity`: effective capacity in cores; `cpuAllocatable`: the relative scheduling specification mapped to `cpu.weight`, without a hard fractional-core quota or performance guarantee.
- `memoryCapacity`: effective Capacity in bytes; `memoryHeadroom`: final `resources.allocatable.memory`, the balloon headroom, not Budget, guest free memory or NodeReservation.
- `memoryReserved`: current NodeReservation when the dynamic controller has an observation; omitted otherwise.
- `memoryUsed`: host VMM `memory.current`; `cpuSeconds`: the same cgroup's `cpu.stat.usage_usec / 1e6`. No ctl/guest CPU addition, inactive-file/balloon subtraction or guest-capacity clipping occurs.
- `timestampUnix`: actual observation time; missing host fields are independently omitted and valid zeros remain visible. A source rebuild may reset CPU seconds; native usage owns lifecycle accumulation.

These reads work in static/dynamic mode with usage or telemetry disabled. They do not alter RequestBudget, admission, recovery charge, heartbeat sampling or the existing build-phase reservation release fence. The native API removes `cpuCount`/`memTotal`/`memAllocatable`/`memUsed`; reservation and headroom now have separate explicit names. [Node §4.1.1](node.md#411-即时-resource--traffic-stats) defines response validity, errors and numeric precision. E2B compatibility metrics keep their own field names and meanings.

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

Observe reservation/recovery state with resource status/list and cluster heartbeats. Abnormal queue diagnostics use conductor's standard logs, collected and retained by the deployment. Key measures are:

- Reserved memory / pool / zone.
- Startup in-flight.
- Provisional/unknown reservation counts.
- Per-sandbox HostMemoryCurrent and last-report time.
- Grant/cooldown and admission queue latency.

Do not combine these into one undifferentiated memory-usage value: reservation, host VMM charge and guest demand are separate measures.

## 10. See also

- [node](node.md).
- [sandboxer sandbox](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox.md).
- [cluster](cluster.md).
- [sandboxer/pkg/resource](https://github.com/kuasar-sandbox/sandboxer/tree/main/pkg/resource).
- [internal/nodectl](../internal/nodectl/).
- [internal/sandboxcfg/resource.go](../internal/sandboxcfg/resource.go).
