# Worker-owned admission generations

[简体中文](proxy-admission-generations_zh.md)

## Ownership, not cross-process lock recovery

The admission arena is independent of the master-write / worker-read route SHM.
The route record carries the effective `traffic.max_inflight` and `{slot,
generation}`. Arena v2 stores only a master-written active generation per slot
and one generation-tagged row of four absolute counters per worker. There is no
second mutable limit policy, identity hash, draining state, or shared row guard.

A live worker alone writes its column. A process-local mutex **per slot**, not
merely per Sandbox ID, serializes acquire, initialization and old releases. The
master never acquires these locks and never clears a live worker's rows. Delete
revokes the active generation; reuse publishes a fresh generation. Neither waits
for a worker to run, including a worker stopped in a critical section. A worker
initializes its own row on first use of a new generation, publishing the tag only
after all counters have been reset. Scans bracket each row's counters with tag
reads and only include matching generations.

A late release either runs before the same worker initializes the new generation,
or sees the new tag and does nothing. It cannot decrement a new Sandbox's count.
An acquire racing with revocation rechecks generation before and after publishing
its increment; failure undoes the increment under the same local slot mutex.
Existing full RouteBinding/ExecIdentity equality still fences credentials and
policy around activation; arena Valid checks generation liveness only.

The master's allocation transaction retains one spare slot. Identity replacement
revokes the old binding and allocates the new one before route publication. Commit
retires the old slot; rollback retires the unpublished slot and reactivates the old
one without clearing its outstanding rows. Ordinary starting/running/paused
updates and unchanged full-sync replay preserve generation and counts.

## Worker exit

The supervisor must reap the exact worker epoch before clearing its column and
starting a replacement at that index. Process exit alone leaves stale-high shared
counts; clearing early would omit live connections. ClearWorker needs no shared
lock because the previous writer is gone and the replacement does not yet exist.
Master route updates only touch slot generations, so cleanup and route apply
cannot form a worker-guard wait cycle. No watchdog, lock timeout, kill-on-contention
policy, per-flow master RPC or global limiter is introduced.

## Bounded approximation

For one current generation and a stable positive limit M, each worker serializes
all services under the same slot mutex and publishes its increment before returning
a grant. For a history with no releases, after the increment first reaching M,
only the at-most-one already-started acquisition in each other worker can still
publish based on an older observation: at most N-1 more. Later scans see at least
M. Thus the bound is M+N-1 for total and target service independently.

For an arbitrary observation time T, remove the complete positive-count intervals
of flows released before T. This only lowers other scans' observations, preserving
all successful acquisitions still active at T and their per-worker serialization.
The resulting no-release history has exactly the active flows at T, so the same
bound applies. A reaped worker's closed flows are releases; the replacement starts
after cleanup and cannot overlap the old writer. A fresh generation counts only
its own tagged rows. This is not a promise to evict existing flows when policy
changes or to give old and new Sandbox identities one shared quota.

Unlimited routes retain the existing no-scan/no-IPC hot path. Worker placement is
not a quota: a single worker can use all available M, rather than a static M/N
share, while multiple workers cannot each independently consume M.

## Layout and validation

For route_capacity=65536 and two workers, arena v2 uses 5,767,320 shared mmap bytes
(previously 8,388,800). Counter payload remains 4,194,304 bytes. Each worker also
has 65,537 process-local mutexes (524,296 bytes with an 8-byte sync.Mutex); these
are not process-shared guards. Layout size, version, strides, indices and atomic
alignment are validated. This changes the admission arena version from 1 to 2,
not the user configuration or routesync protocol. Master and workers must use the
same executable/bootstrap source set.

Tests retain the 1/2/4/8-worker threshold interleavings, service/total checks, skew,
release waves, rollback, and real child-process crash cleanup. Additional tests
stop a real child with SIGSTOP inside acquire and require master route changes
and a surviving worker to progress without waking or killing it first.
