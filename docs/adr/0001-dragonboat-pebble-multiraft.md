# ADR 0001: Dragonboat and Pebble for Registry Multi-Raft

- Status: Accepted; final cutover gated by the benchmarks below
- Date: 2026-07-18
- RFC: [orchestrator#46](https://github.com/kuasar-sandbox/orchestrator/issues/46)

## Context

The Registry needs one three-replica System Group and an initial 4,096
three-replica data-shard groups. The runtime must preserve the authority,
fencing, revision, read-safety, snapshot, catch-up, and configuration-change
contracts frozen in RFC #46. Library choice must not change those semantics.

The implementation targets Go 1.24 and a same-region deployment. One Registry
process must host thousands of groups without opening one transport, WAL, or
state database per group.

## Decision

Use these pinned modules:

| Role | Module | Version | License |
| --- | --- | --- | --- |
| Multi-Raft runtime | `github.com/lni/dragonboat/v4` | `v4.0.0-20250723143628-076c7f6497dc` | Apache-2.0 |
| Shared state engine | `github.com/cockroachdb/pebble` | `v0.0.0-20221207173255-0f086d933dac` | BSD-3-Clause |

Use one Dragonboat `NodeHost` per Registry process. It owns shared transport,
scheduling, and LogDB/WAL facilities for the System Group and all local data
replicas. Every group uses an `IOnDiskStateMachine` backed by one process-local
Pebble database. Pebble keys are scoped by Raft shard and replica identity.

The state engine uses:

- deterministic, strictly decoded commands;
- committed Raft log indexes as Route and Build revisions;
- one atomic Pebble batch for every Dragonboat update batch;
- serialized sync generations for Dragonboat's `Sync` durability boundary;
- two snapshot slots and one atomically switched control record per replica;
- bounded 16 MiB synced snapshot-recovery batches;
- one Pebble snapshot for metadata and row reads in each lookup;
- per-bucket Route snapshots and a durable 10,000-revision compact Route
  invalidation feed that returns an explicit reset after compaction;
- explicit replica-range deletion only after committed membership removal.

Dragonboat groups enable `CheckQuorum`, `PreVote`, ordered configuration
changes, bounded in-memory logs, Snappy snapshots, and configured snapshot,
send-queue, receive-queue, and worker limits. Initial bootstrap, join, restart,
promotion, removal, and local data deletion are driven by durable enrollment
and the signed Registry Layout, never by memberlist or leader visibility.

Every Registry process must start with `deploy/dragonboat-soft-settings.json`
in the Registry config directory as its current working directory. The profile
bounds per-replica queues as follows:

```text
InMemEntrySliceSize=128          MinEntrySliceFreeSize=32
PendingProposalShards=4         IncomingReadIndexQueueLength=128
IncomingProposalQueueLength=128 ReceiveQueueLength=128
TaskQueueInitialCap=16          TaskQueueTargetLength=64
TaskBatchSize=128
```

Dragonboat reads this fixed file name only during package initialization.
`cluster-ctl registry` therefore validates the exact profile, launch directory,
regular-file type, and non-group-writable permissions before creating a
`NodeHost`; the systemd unit fixes `WorkingDirectory=/etc/cluster-ctl`.

## Correctness Mapping

| RFC requirement | Implementation evidence |
| --- | --- |
| Stable HardState/log and restart | Dragonboat LogDB plus explicit enrollment; ambiguous first start and missing enrolled history fail closed |
| Deterministic apply | `ApplySystemCommand` and `ApplyDataCommand` use committed indexes and strict bounded command envelopes |
| Quorum commit before success | synchronous Dragonboat proposals return only after apply; ambiguous outcomes are resolved by a strong read |
| Replica-local positive reads | Dragonboat stale reads expose only locally applied READY/positive rows; local miss and non-ready rows return leader outcomes |
| Strong reads | Dragonboat `SyncRead` supplies the leader/read-index path and applied barriers |
| List and watch | each `(group, route_bucket)` range is read from one Pebble snapshot with its shard revision; durable change rows use committed indexes and a compacted cursor returns reset |
| Snapshot and catch-up | on-disk snapshot stream plus learner catch-up proof from an exact target replica's linearizable read |
| Safe membership change | signed next Registry Layout, learner add, applied barrier, promotion confirmation, target-readiness proof while predecessor voters remain reachable, epoch activation, old-Permit drain, data-epoch retirement, and old-replica removal |
| Registry History Generation fencing | signed anti-rollback Registry Layout guard, explicit enrollment, System closure proof, and bounded Serve Permit identities |
| Fence compaction | full monotonic retention wait, durable outbox ACK or permanent NodeEpoch fence, and exact-voter applied-index probes |
| Encrypted storage | runtime startup requires a platform storage attestor for NodeHost, WAL, Pebble, Registry Layout guard, and enrollment paths |

The runtime deliberately rejects generic submission of bootstrap, epoch-change,
membership-progress, Registry-History-Generation-closure, and fence-compaction commands. Those
commands are reachable only through their proof-collecting workflows.

## Operational Constraints

- A Raft endpoint hosts at most one replica ID of a given shard.
- A retained member keeps its replica ID and Raft endpoint during one Registry Layout
  transition. Replacing either uses a new member/replica target.
- A member removed from `next_registry_layout` may restart from the signed active
  artifact while the durable anti-rollback guard remains at `next_registry_layout`.
- Old replicas may serve only the draining old epoch. They cannot serve the new
  epoch unless the new Registry Layout places and promotes that exact local replica.
- Phases 1-4 are review order only and are never deployed separately. The
  final delivery has no old-store migration, dual write, or intermediate
  production path.

## Validation

Completed locally on a 4-vCPU, 3.6 GiB host:

- deterministic System/data state-machine and snapshot tests;
- restart, anti-rollback, Registry History Generation rollover, and Permit fencing tests;
- real three-member Registry History Generation rollover and #34 recovery, including an open
  recovery-epoch restart of all Registry replicas, node SessionSeq renewal,
  digest-CAS Binding rebind, durable event ACK, and old-Binding proxy fence;
- real three-node Dragonboat proposal and local-read integration;
- real snapshot transfer to an empty learner after leader compaction;
- learner promotion and old-voter removal;
- shared-Pebble crash/reopen, snapshot slot switch, and replica cleanup tests;
- Route bucket snapshot, changefeed pagination, retention reset, restart, and
  snapshot-transfer tests;
- five consecutive real snapshot/catch-up integration runs;
- `GOWORK=off go test ./...`;
- `GOWORK=off go vet ./...`.

The local host is intentionally too small for the acceptance-scale gates. The
following commands run the gates on a qualified persistent filesystem; the
Dragonboat test binary starts from `deploy/` so the production soft-settings
profile is loaded during process initialization:

```bash
GATE_ROOT=/mnt/kuasar-raft-gates/kuasar-gates
GOWORK=off go test -c -o /tmp/kuasar-raftstore-gates ./internal/raftstore
env -C deploy KUASAR_RAFT_SCALE_GATE=1 KUASAR_RAFT_GATE_ROOT="$GATE_ROOT" \
  /tmp/kuasar-raftstore-gates -test.run='^TestDragonboat4097GroupScaleGate$' -test.count=1 -test.v
GOWORK=off KUASAR_RAFT_STATE_GATE=1 KUASAR_RAFT_GATE_ROOT="$GATE_ROOT" \
  go test ./internal/raftstore -run '^TestPebbleMillionReadyRouteGate$' -count=1 -v
```

The Dragonboat gate starts 4,097 live groups, performs 20,000 reads, requires
read p99 at or below 5 ms, starts within two minutes, and limits RSS to 8 GiB.
The state-engine gate loads 1,000,000 READY rows within five minutes, performs
50,000 reads, requires read p99 at or below 5 ms, and limits RSS to 16 GiB.
The gates passed on `bms.tmp` with 88 logical CPUs (Intel Xeon Gold 6266C),
375.9 GiB RAM, openEuler 24.03 LTS-SP4, kernel
`6.6.0-159.4.6.157.20260713.a4e2472763b2.oe2403sp4.x86_64`, and the dedicated
`/dev/sdb1` ext4 VBS volume:

| Gate | Result |
| --- | --- |
| 4,097 groups / 12,291 replicas | startup 60.816 s; total 69.98 s; READY read p50 58.9 us, p99 108.0 us, p99.9 233.8 us; max RSS 4.70 GiB |
| 1,000,000 READY rows / 4,096 shards | 191..302 rows/shard; load 37.023 s; total 40.75 s; read p50 54.0 us, p99 72.7 us, p99.9 181.6 us; max RSS 0.57 GiB |

The upstream Dragonboat defaults were also measured and rejected: the live
group gate consumed 25.54 GiB RSS. The bounded profile reduced the same tmpfs
cross-check to 4.53 GiB RSS with 10.333 s startup. The system `/dev/sda2` ext4
VBS volume missed the two-minute startup limit at 127.34 s, while the dedicated
volume passed; Registry storage therefore requires the same pre-cutover gate
and cannot be placed on an unqualified system volume.

## Fallback Gate

Replace Dragonboat with `etcd-io/raft` plus the same Pebble state model only if
the intended BMS host cannot satisfy one of these after bounded tuning:

- 4,097 live groups cannot start within two minutes or remain within 8 GiB RSS;
- stable local READY read p99 exceeds 5 ms;
- snapshot catch-up, crash recovery, or ordered membership changes violate the
  frozen correctness tests;
- the pinned dependency cannot build with Go 1.24 or creates an unacceptable
  security or license obligation.

A fallback changes only the runtime adapter and requires a replacement ADR. It
must not change RFC #46 authority or protocol semantics.

## Consequences

Dragonboat supplies the mature Raft lifecycle and shared process runtime, while
the application retains explicit control of the Registry state model and read
contract. Pebble avoids one database per group and permits atomic row updates,
but makes compaction, snapshot concurrency, memory budgets, and disk latency
operationally significant. Those limits remain Registry Layout/runtime tuning, must be
observable, and must pass the stated BMS gates before cutover.
