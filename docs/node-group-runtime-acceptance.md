# Node Group and snapshot runtime acceptance

This report accompanies [orchestrator #570](https://github.com/kuasar-sandbox/orchestrator/issues/570)
and [sandboxer #472](https://github.com/kuasar-sandbox/sandboxer/issues/472).
It separates source tests, native database tests, product processes with simulated
Node execution, and actual Node/KVM experiments. Results from one level do not
stand in for another. Final merge, release and aggregate receipts must be attached
before either proposal is closed.

## Delivered behavior

- Node owns Group context, revisions and deletion records. HTTP, UDS and trusted
  NodeLink maintenance use the same authorization and conditional writes. A
  combined credential installation/renewal and Group write commits in one Node
  SQLite transaction. The existing standalone credential API keeps its behavior.
- Normal Node reports are the only GroupSupport source. A complete directory,
  subsequent ordered changes, authentication references and cleanup bind to the
  original registration. Shared metadata uses the existing SQLite, PostgreSQL
  and FoundationDB engines; drivers have no Group-specific transaction API.
- Registry intersects Group, complete authentication, the selected template's
  runtime declaration and request Placement on one Node, then rechecks the
  decision in its transaction. Explicit templates do not inherit default sandbox
  configuration. Each waiter checks its own permission against the final object.
- The optional Placer maintains Node Groups through the same business operation.
  It no longer supplies create-time context or a second authorization directory.
  File removal and selector shrink do not silently delete persistent Groups.
- A valid old SNP remains usable. A new declaration requires the same basename
  and declared digest; malformed declarations are rejected. Registry does not
  inspect artifacts or decrypt KMT. Node retains its actual runtime validation.
- Snapshot capture records the runtime from the same portable capture before
  committing its result. Existing reference fields propagate it through holder
  saving, publication, Build and export. Bundle and tarstream remain supported
  formats. Content keys, other Bundle members and immutable internal parent
  references retain their original content identities.
- Existing orphan recovery and cross-Node migration remain available. Unknown
  results retain exact ownership and authentication responsibilities. Recovery
  can advance a non-reused local generation while preserving logical identity.
  Only the precise initial Group-version rejection can use the special rollback
  path. No physical exactly-once guarantee or persistent command journal was added.

## Source and delivery identities

| Unit | Reviewed source / merged source | Delivery evidence |
|---|---|---|
| Shared Ref reader, accelerator #238 | merged `127afc27958d2721beb4b36c135ee1b1fce86390` | SDK `v0.2.0-preview.20261124`; official run `45201145110` passed |
| Sandboxer reader, #473 | head `d2c67d40d7db27801550c6055d41fb9dad349f7d`; merged `347b3fe657b66f69b0a86ce2c428bdb484471663` | exact-head run `45262391782` passed; component release `45271271036` passed |
| Orchestrator reader, #571 | head `ccc4cd82d40d40d343f36aa083ba795c2c7124a7`; merged `a0cf03d51ae9e8d15b7bc877d43c4afb351f11d0` | exact-head run `45274746152` passed; actual reader release pending |
| Node Group foundation, #572 | `187e04a98d67fc4dfe9a96bc6c1aaa0d8bd1f45a` | includes reviewed cursor escaping fix; full Store race and native ARM regression passed; final main CI pending |
| Shared support repository, #573 | `75f38bb1c863939eaeec7da8c029e2ced9c85e37` | final main rebase/CI pending |
| Snapshot producer, sandboxer #474 | `ecfd3ea66efb1968d6435f344e555decd2e873ca` | same tree as tested `5bd60b2`; waits for actual Orchestrator reader delivery |
| Cluster activation, #574 | production changes through `85d1ce7`; failure/carrier tests through `1c6ff22` | final main rebase/CI/release pending |

Sandboxer SDK `v0.2.0-preview.20261124.1` resolved to
`h1:lp8cYCRnPyyyNbzd7V7aRYS9OT3aLI/hN3UiQgVlNIE=`;
its go.mod checksum is `h1:vTA14bPZ9LCIszuCot7yWPz3FoXb7QVtU02BsBY8Ij8=`.
Published `sandbox-ctl` SHA-256:

- x86_64: `7c72ee57e8776b5d0e0c96f177b7d2b9cde4357ac3b04ce31fd8fe7011bced99`.
- aarch64: `3832fc253276b2e3fd63369fb999d5980f6c08160650279ee8c9f9b26ba2aba2`.

Both published readers restored snapshots from the updated producer on their
native KVM hosts. The updated producer hashes are
`b78bfa29b45dc9857157c00eb3087dec8766b7090c27af1e7db6f0d82ac8ab94`
(x86_64) and
`2e328e92f8c5194aec79922155b9179d41c7f1b838e3df358447bde1455fbf4b`
(aarch64). These use the `5bd60b2` tree, identical to rebased `ecfd3ea`.

The manual cluster migration used a `cluster-ctl` built from `1822c68` plus the
subsequently committed `85d1ce7` patch, SHA-256
`5acdefdb599f718d405fc58a6eb6bd5eb143ccc746fa8a8904e0aed48d92e835`.
Its patch receipt is retained. This is source-workspace evidence, not a claim
that these manual runs used the final published cluster binary.

## Functional and failure evidence

Native transaction and connection tests execute real SQLite, PostgreSQL 16.15
and FoundationDB 7.3.77 under the Go race detector on x86_64 and aarch64. The child-process commit
experiment kills an actual Registry test process after its native commit and
before returning to command dispatch. Its Node execution after recovery is
represented by report/ACK fixtures. The manual VM suite complements this with
actual Node SQLite, HTTP/2 NodeLink, KVM and a private FDB instance.

| ID | Observation | Execution scope |
|---|---|---|
| F01 | Abort after eight staged native writes leaves no intent, ownership or auth reference and sends no command. | Three native backends, new connection reread. |
| F02 | Physical commit followed by a lost result keeps the exact decision. SIGKILL before dispatch preserves the logical SID and advances g0 to g1 without releasing old ownership/auth. After socket write but before Node acceptance, killing the real owner also restores to g1; only g1 is forwarded to Node. | Three native backends plus child process; separate real VM/relay window. |
| F03 | Drop accepted Create ACK; a real running report converges the correct logical/local identity with one observed Create delivery. | Real Node DB/VM/NodeLink. |
| F04 | Deliver running report before a delayed generic error ACK; the materialized object remains accessible. | Real Node DB/VM/NodeLink. |
| F05 | Two actual HTTP/2 streams deliberately reuse CmdID. Old ACK/report cannot complete the fresh waiter or overwrite its directory/ownership. Fresh ACK/report still works. | Real streams and all three native databases; execution represented by report fixtures. |
| F06 | A precise initial stale Group version gives 409 before a Node row or RunDir exists; the next request can obtain a fresh decision. | Real Node/VM harness and exact rollback predicate tests. |
| F07 | Removing the structured proof turns the reply into ordinary 409. A Node GET returning 404 does not release the waiting decision. Timeout retires the logical slot but retains old ownership/auth. | Real Node/relay and three native databases. |
| F08 | SIGKILL the Registry after Node accepted a VM while withholding ACK/reports. New owner/process and full NodeLink snapshot recover the same instance and credentials with one observed Create. | Real FDB/VM owner restart; PG/FDB multi-Registry process tests; SQLite single-Registry restart. |
| F09 | Capture source, make target Group default runtime unavailable, kill source Node and migrate through opaque KMT. Guest marker, logical ID and credentials survive; local identity advances. | Real source/target Node and VM/SNP. Registry never reads the token's source reference. |
| F10 | Returning source g0 is cleaned by its exact ownership; target g1 remains usable immediately. | Real Node/VM migration. A stale Router data target found by this experiment was fixed and regression-tested. |
| F11 | Update and delete after Node has copied Group inputs do not cancel the accepted invocation. Later requests see the new revision/deletion; an existing VM stays accessible after Group deletion. | Real Node SQLite/create core with gated launcher, plus VM deletion scenario and durable Build regression. |
| F12 | Two initially authorized callers join one flight; the caller with another valid credential or incompatible Placement is denied by final-object validation. Two valid concurrent public callers share one VM creation. | Three native backends plus real public VM requests; existing replacement/routeKey tests remain covered. |

F07 observes **one Registry invocation**. Router may retry a 503 after a logical
slot retires, creating a later invocation; that is not evidence of resending the
old execution identity. A negative query, timeout or missing report never proves
that all earlier deliveries had no effects.

| ID | Compatibility / selection coverage |
|---|---|
| C01 | Old SNP remains accepted by template, Group and import paths; old snapshots restore on both native architectures and new consumers. |
| C02 | Legacy selection adds no runtime filter or artifact inspection. On both architectures the published reader accepted an unannotated old SNP, then its actual restore preflight rejected a wrong-digest primary runtime and missing v1 fallback. This is real restore-boundary evidence, combined with Registry selection tests; it is not a new full cluster incompatibility scenario. |
| C03 | Runtime selection checks basename and digest together; another basename with the same digest does not match. |
| C04 | Group/auth/runtime/Placement are checked on one candidate; empty OR branch cannot remove hard conditions. |
| C05 | Shared parser, Group validation and request tests reject malformed, repeated and out-of-order declarations before installing a combined credential pair. |
| C06 | Explicit/default template origins, no-default Group, private sandbox configuration and durable-first ordinary Build covered by Node/Registry suites. |
| C07 | Real v1 captures restored with host default v2 retain v1 and guest state in tarstream and Bundle; capture/holder result persistence is covered by sandboxer suites. |
| C08 | Old source output need not acquire invented metadata; a new capture gains its actual runtime. Actual opaque-token migration succeeds with an incompatible target default. |
| C09 | Publication/rewrite tests preserve the same Snapshot declaration; member selection, physical carriers, internal parents and content keys exclude it. The review-found rewrite loss was fixed before #473 merge. |
| C10 | Actual old-to-new and new-to-new restore pass on x86_64/aarch64. A genuinely old reader rejects new annotated refs. An intermediate reader accepted tarstream but rejected annotated Bundle, demonstrating why the complete reader was required. |
| C11 | Actual module resolution and published binary tests precede producer enablement; source merge alone is insufficient. Orchestrator reader release remains a producer gate until its receipt is attached. |
| C12 | Base/suffix/ref/TemplateID limits 256/336/592/799 bytes; native x86/ARM parser and actual mmap read/write pass. Build capture JSON, publication, route/Token and persistence suites preserve the reference. A 592-byte ref / 799-byte template also traversed selection, framed NodeLink command/report, actual native DB commit/readback, RouteLink response and mmap on all three backends on both architectures. Encrypted KMT and maximum runtime basename in Build capture JSON passed separately on x86 and ARM. |

The full activation Go race suite passed (Node package 662.134 s). Follow-up
Registry race passed in 58.381 s; the native commit/stream/waiter suite passed
after later additions. Full ARM native race coverage also passed at `1c6ff22`
([receipt](evidence/node-groups-20261124/native-arm64-databases/validation-receipt.json),
[log](evidence/node-groups-20261124/native-arm64-databases/native-arm-databases.log)):
FDB driver 1.332 s, metadata 10.254 s, SQL engine 4.014 s, Registry 28.985 s,
cluster command 4.903 s and HTTP cluster stub 3.174 s. All 3,259 archived source
files were verified before execution. This uses real native PG/FDB/SQLite and
simulated Node execution where the test calls for it, not an ARM VM cluster.
An initial ARM dependency setup lacked the verified workspace sums and could not
reach the public checksum service. After copying the original sums, a later run
exposed a test injector that consumed its commit fault on a definite database
retry, plus an overlong Unix-socket temporary path. The injector now deliberately
rolls back one real transaction before losing the successful commit reply; x86
and ARM regressions passed. The final runner uses a short temporary path. Failed
attempts remain in the local archive and are not counted as passing.
Source checks include Go vet and 359 Python helper tests.
The real cluster lifecycle run covered Build, Group import, Create, exec, cold
Wake, WebSocket/envd, stats, List/Delete and retained Build status after Router
restart. Separate real recovery/reset runs covered owner failover, conductor
restart, checkpoint/reset/autoreset and parent reclamation.

## Performance

### Existing product operations, before and after activation

Same WSL host (Intel i7-8565U), Go 1.26.8, GOMAXPROCS=4, private FoundationDB
7.3.77 single-SSD database. Baseline `75f38bb1` and candidate `8310ef3737` were
measured in three alternating pairs without concurrent heavy local work.
Registry, Router and Placer are real processes; **one execution-stub process
represents four logical Nodes**. FDB is separate. This measures the control/data
path, not VM startup or production HA.

Values are medians of three run-level results. Historical latency quantiles use
the recorded order statistic `floor((n-1)*p)`. The older runs did not retain
individual request samples, so these have not been relabelled as a different
quantile estimator. The committed benchmark driver now retains raw samples.

| Operation | P50 ms before -> after | P95 ms | P99 ms | Throughput ops/s |
|---|---:|---:|---:|---:|
| Existing Ensure | 10.383 -> 11.175 | 15.811 -> 15.394 | 19.129 -> 18.676 | 88.22 -> 86.30 |
| Control GET | 0.931 -> 5.081 | 1.315 -> 6.216 | 1.621 -> 6.886 | 937.45 -> 187.66 |
| Cached data GET | 1.158 -> 1.179 | 1.451 -> 1.871 | 1.755 -> 2.293 | 791.27 -> 745.38 |
| First Create, serial | 60.098 -> 61.245 | 91.868 -> 80.763 | 147.537 -> 96.259 | 14.97 -> 15.72 |
| First Create, concurrency 8 | 94.259 -> 88.276 | 152.017 -> 143.045 | 164.711 -> 175.308 | 76.80 -> 78.91 |
| Delete, concurrency 8 | 14.281 -> 16.543 | 23.398 -> 26.363 | 30.211 -> 30.379 | 511.46 -> 444.21 |

Per-request control authorization costs approximately 1.35 -> 5.71 ms CPU per
operation. The P50 increase is 445.48%; this is a real regression, not an inferred
improvement from removing a Placer RPC. Avoidable sequential candidate rereads
were batched through the existing transaction API before the final series.
The initial slower series is retained alongside the optimized results.

After Delete, summed RSS of the four product processes was 217.49 -> 235.68 MiB.
FDB is excluded and shared pages can be counted more than once. The sum of each
process's historical HWM is only an upper bound, not a simultaneous physical
memory peak. Per-request allocation counts were not collected from these
processes; allocation measurements below are Go benchmarks. Additional sampled
FDB conflict and cleanup observations are recorded separately, not retroactively
inserted into these six historical runs.

A separate three-pair measurement retained individual request latencies and
sampled FDB server counters outside the timed phases. The median counter delta
was: started transactions 19,645 -> 25,190; committed 1,775 -> 1,565; conflicted
0 -> 18; queue-time rejection 0 -> 0. These are isolated-server totals including
background/status traffic, not per-request transaction or conflict attribution.
Each run created 400 measured instances. The observer first verified all 400
existed; after Delete, all six runs observed zero remaining measured instances
on their first query (roughly 1–2 ms query time). That is ordinary simulated
execution cleanup; it does not erase or measure unresolved ownership obligations
from failure cases. These runs use the later production code including the
migration cache fix and are kept separate from the earlier latency table.

### New Group operations

No equivalent Node Group CRUD existed in the baseline. Three 200-operation Go
benchmark repetitions measure actual Node SQLite on the same WSL host:

| Operation | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Get | 56,000 | 6,646 | 99 |
| Update | 6,972,544 | 11,312 | 185 |
| Delete + explicit recreate | 13,814,302 | 17,561 | 312 |
| Combined renewal | 7,113,261 | 14,539 | 237 |
| Minimal catalog snapshot, 128 Groups | 1,132,006 | 604,519 | 7,324 |

Candidate/directory benchmarks separately cover real SQLite, PostgreSQL and FDB
at 4/128/256 supporting Nodes and full/incremental reports. The combined initial
run had a stopped PostgreSQL container; its failed PG samples are excluded. The
PG-only retry passed. PostgreSQL candidate selection measured median 1.541 ms
(4), 9.844 ms (128), and 22.019 ms (256). These are synthetic metadata sizes, not
that number of live VM Nodes.

### Snapshot capture and restore

Actual native KVM hosts; same 1-vCPU/512-MiB guest, kernel/root and hypervisor
within each comparison. Two warmups then 20 samples per version/format,
alternating old/new order. P50 is the sample median, P95 uses nearest rank, and
maximum is reported instead of claiming a reliable P99 from 20 observations.
The performance candidate predates the later publication-rewrite-only fix; the
updated final writer was separately functionally retested on both architectures.

| Architecture / format | Capture P50 ms before -> after | Restore P50 ms before -> after |
|---|---:|---:|
| x86_64 tarstream | 181.139 -> 179.284 | 212.024 -> 205.401 |
| x86_64 Bundle | 151.106 -> 149.722 | 212.762 -> 212.852 |
| aarch64 tarstream | 58.501 -> 57.758 | 43.295 -> 42.813 |
| aarch64 Bundle | 77.316 -> 77.223 | 64.238 -> 63.745 |

Shared parser benchmarks retain allocation counts (manifest: 192 B/4 allocations;
located Bundle/tarstream: 32 B/1 allocation). Median old-input parsing time rose
from 296.0 to 342.9 ns for manifest, 666.7 to 767.6 ns for located Bundle, and
670.7 to 753.8 ns for located tarstream.

Actual mmap schema 10 records are 3,112 bytes instead of 2,664. At the default
65,536 slots the mapping grows by 28 MiB. This is mapping capacity, not measured
resident memory. Both architectures executed the layout and long-template tests.

## Reproduction and limits

- `test/e2e/manual/cluster-faults.sh` and `cluster-migration.sh` exercise real
  Node/VM windows; their relay only controls stream delivery and ordering.
- `TestRegistryNative*` under native `scripts/cluster-native.py test` covers real
  transaction, child-process, stream and waiter boundaries. PostgreSQL requires
  an explicit isolated DSN; a skipped backend is not counted as passing.
- `test/e2e/lib/cluster_benchmark.py`, invoked by `CLUSTER_BENCH_OUTPUT` from
  `test/source/cluster_stub.sh`, retains request samples, CPU ticks and process
  RSS. Its execution topology is stated in each result.
- Two Nodes on the same physical test host used distinct task-owned runner
  templates. The manual fixture only reused byte-identical pre-existing slice
  units and excluded them from cleanup. All twelve pre-existing host unit/drop-in
  hashes were unchanged. The fixture patch and before/after hashes are retained.
- Ordinary Delete cleanup and late g0 cleanup were observed. Unknown ownership
  can legitimately remain until terminal facts arrive. This does not establish
  zero unresolved responsibility, bounded outage recovery time or exactly-once
  external side effects under arbitrary partitions.
- SQLite remains a single-Registry mode. Shared owner switching uses PG/FDB.
  Opaque-token migration has no new Registry runtime prefilter. No production
  scale, HA SLA or rolling mixed-version protocol compatibility is claimed.
- Aggregate `45226273027` failed at published-download validation with GitHub
  403 after retry; it is not a successful full aggregate receipt. Later aggregate
  and final exact-source receipts must be recorded independently.

## Final closeout checklist

- [x] Reader changes reviewed and merged through normal protection.
- [x] Native database, process, stream and real VM evidence collected.
- [x] Before/after product and snapshot performance, including regressions.
- [ ] Actual Orchestrator reader SDK/tools delivered before producer release.
- [ ] Remaining PRs rebased onto main, reviewed, exact-head CI passed and merged.
- [ ] Final trusted component/aggregate releases and both-architecture identities.
- [ ] Final F/C matrix and scope limitations attached to both original proposals.
- [ ] Proposals closed only after the preceding delivery receipts are complete.

## Retained evidence

The [receipt index](evidence/node-groups-20261124/index.json) records original
and published SHA-256 values. The [evidence directory](evidence/node-groups-20261124)
contains raw benchmark observations, summaries, native test results, actual VM
fault/migration receipts and both-architecture reader results. Failed initial
PG benchmark setup remains visible and is excluded from the successful PG-only
measurement. Snapshot capability references are omitted from the public VM
result summaries; log whitespace is normalized and exact patches are JSON-encoded
where stated in the index. Configs, databases and complete process output stay in the
local task archive. The index names those original task files.

- [Control/data/Create/Delete measurements](evidence/node-groups-20261124/product-benchmark-final-summary.json)
- [Additional FDB counters and ordinary cleanup](evidence/node-groups-20261124/product-counters-summary.json)
- [Real VM failure cases](evidence/node-groups-20261124/vm-faults-v10.json)
- [Real VM migration](evidence/node-groups-20261124/vm-migration-v3.json)
- [Native commit, stream, caller and carrier tests](evidence/node-groups-20261124/activation-native-final-carriers.log)
- [x86 snapshot performance](evidence/node-groups-20261124/vm-runtime-x86/performance-summary.json)
- [ARM snapshot performance](evidence/node-groups-20261124/vm-runtime-arm64/performance-summary.json)
