# Resource pools

[中文](resource-pools_zh.md) · [Resource protocol](node-resource.md) · [Runner units](node.md)

Resource pools extend the existing in-process resource controller. Each runner
or Builder pool belongs to one resource pool. Several logical pools can share a
resource pool, including its unit template; a template cannot span resource
pools. All pools continue to use the shared `paths.base_root`.

## Configuration

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

## systemd placement

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

## Lifecycle and pressure

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

## Compatibility and recovery

Without `resource_listen.pools`, one `default` resource pool is used. A sole pool
without `allocatable.memory` retains the original physical-memory minus host
reservation and operational-margin formula. Multiple pools require explicit net
runtime memory and reject explicit old physical-memory/CPU/host-reserved/margin
fields. Automatically inserted defaults do not count as explicit old input,
including across Clone and YAML/JSON round trips or Configure hooks.

Legacy `physical_cpu` accepts `auto` or a complete decimal integer core count
that fits after conversion to millicores. `host_reserved.cpu` accepts finite,
nonnegative fractional cores and retains truncation to millicores and its
explicit-zero default. Pool CPU fields accept fractional cores and round up to
millicores; omitted pool capacity remains unlimited.

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

## Real-environment regression

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
