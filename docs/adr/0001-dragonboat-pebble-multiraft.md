# ADR 0001: Dragonboat and Pebble for Registry Multi-Raft

- Status: Accepted for dormant implementation; cutover gated by the benchmarks below
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
- explicit replica-range deletion only after committed membership removal.

Dragonboat groups enable `CheckQuorum`, `PreVote`, ordered configuration
changes, bounded in-memory logs, Snappy snapshots, and configured snapshot,
send-queue, receive-queue, and worker limits. Initial bootstrap, join, restart,
promotion, removal, and local data deletion are driven by durable enrollment
and the signed manifest, never by memberlist or leader visibility.

## Correctness Mapping

| RFC requirement | Implementation evidence |
| --- | --- |
| Stable HardState/log and restart | Dragonboat LogDB plus explicit enrollment; ambiguous first start and missing enrolled history fail closed |
| Deterministic apply | `ApplySystemCommand` and `ApplyDataCommand` use committed indexes and strict bounded command envelopes |
| Quorum commit before success | synchronous Dragonboat proposals return only after apply; ambiguous outcomes are resolved by a strong read |
| Replica-local positive reads | Dragonboat stale reads expose only locally applied READY/positive rows; local miss and non-ready rows return leader outcomes |
| Strong reads | Dragonboat `SyncRead` supplies the leader/read-index path and applied barriers |
| Snapshot and catch-up | on-disk snapshot stream plus learner catch-up proof from an exact target replica's linearizable read |
| Safe membership change | signed next manifest, learner add, applied barrier, promotion confirmation, old-replica removal, epoch activation, old-Permit drain, and epoch retirement |
| Generation fencing | signed anti-rollback manifest guard, explicit enrollment, System closure proof, and bounded Serve Permit identities |
| Fence compaction | full monotonic retention wait, durable outbox ACK or permanent NodeEpoch fence, and exact-voter applied-index probes |
| Encrypted storage | runtime startup requires a platform storage attestor for NodeHost, WAL, and Pebble paths |

The runtime deliberately rejects generic submission of bootstrap, epoch-change,
membership-progress, generation-closure, and fence-compaction commands. Those
commands are reachable only through their proof-collecting workflows.

## Operational Constraints

- A Raft endpoint hosts at most one replica ID of a given shard.
- A retained member keeps its replica ID and Raft endpoint during one manifest
  transition. Replacing either uses a new member/replica target.
- A member removed from `manifest_next` may restart from the signed active
  artifact while the durable anti-rollback guard remains at `manifest_next`.
- Old replicas may serve only the draining old epoch. They cannot serve the new
  epoch unless the new manifest places and promotes that exact local replica.
- Phase 1-4 code remains dormant. No old-store migration, dual write, or
  intermediate release is authorized by this ADR.

## Validation

Completed locally on a 4-vCPU, 3.6 GiB host:

- deterministic System/data state-machine and snapshot tests;
- restart, anti-rollback, generation rollover, and Permit fencing tests;
- real three-node Dragonboat proposal and local-read integration;
- real snapshot transfer to an empty learner after leader compaction;
- learner promotion and old-voter removal;
- shared-Pebble crash/reopen, snapshot slot switch, and replica cleanup tests;
- five consecutive real snapshot/catch-up integration runs;
- `GOWORK=off go test ./...`;
- `GOWORK=off go vet ./...`.

The local host is intentionally too small for the acceptance-scale gates. Run
these exact gates on `bms.tmp` before the Phase 4 PR can be approved:

```bash
GOWORK=off KUASAR_RAFT_SCALE_GATE=1 go test ./internal/raftstore -run '^TestDragonboat4097GroupScaleGate$' -count=1 -v
GOWORK=off KUASAR_RAFT_STATE_GATE=1 go test ./internal/raftstore -run '^TestPebbleMillionReadyRouteGate$' -count=1 -v
```

The Dragonboat gate starts 4,097 live groups, performs 20,000 reads, requires
read p99 at or below 5 ms, starts within two minutes, and limits RSS to 8 GiB.
The state-engine gate loads 1,000,000 READY rows within five minutes, performs
50,000 reads, requires read p99 at or below 5 ms, and limits RSS to 16 GiB.
Record CPU, memory, storage, filesystem, kernel, and elapsed results in the PR.

The BMS gates are currently pending because the existing reverse SSH endpoint
at `localhost:52222` refuses connections. This is an infrastructure blocker,
not a passed benchmark and not dependency-download work.

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
operationally significant. Those limits remain manifest/runtime tuning, must be
observable, and must pass the stated BMS gates before cutover.
