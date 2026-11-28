# Storage refactor acceptance, 2026-11-22

Historical acceptance evidence at the source revisions and environments recorded below. Use [cluster metadata storage](cluster-storage.md) for current deployment and recovery. Measurements and delivery receipts here do not describe every later release.

This report covers the shared metadata storage engine and its three native
backends. Node Group publication and request-acceptance validation are recorded
separately in [Node Group and runtime acceptance](node-group-runtime-acceptance.md).

## Implementation and source identity

The implementation under test is `f1cd0c5e99b170c04094a785c3d07699f31041d5`
(tree `36a645311cd6faae920f853c1a2064f06a939b2e`). It follows merge baseline
`5c72e418c49bd333e312b10fba1000d7c0d918b3`, which combines the previous PR head
`22314ae768a1cb87895395a4c3f155ba0d4dd6b8` and main
`e1e34f99f7421bbb1ab3ff5226eeb8f7f1b45026`. The final code under test is
`377de1e327575d563406a690c44a5128a6332519`, which also adds an
observation-admission regression and fixes SQLite handle identity when a new
database is opened through a symlinked directory. Documentation and evidence
updates do not change these tested production sources.

| Requirement | Delivered behavior |
|---|---|
| One metadata implementation | Route, Build, Node, ownership, catalog, KeyPair and import repositories moved to `internal/cluster/metastore` |
| Three generic drivers | Real SQLite, PostgreSQL and FoundationDB use the same byte transaction and record contracts |
| Driver isolation | Architecture test forbids business-package imports in storage code; Registry still receives typed repositories |
| Durable identity and CAS | Per-key ten-byte revisions survive delete/recreate; long identifiers, chunk integrity and conditional writes share one codec |
| Retry and uncertainty | Only definitely aborted transactions retry; original read revisions remain pinned; Unknown never automatically replays or publishes a successful version |
| Observation | Persistent revision conditions, bounded shared admission, cancellation/Close reclamation, FDB watches and SQL coalesced rereads |
| PostgreSQL notifications | Missed NOTIFY still invalidates through durable polling; terminated LISTEN connection invalidates consumers and reconnects |
| Deployment | Backend configuration, SQLite singleton validation, PostgreSQL DSN environment variable, restart-only backend changes, pure-Go CLI support |
| Existing checkpoint delivery | `MaxCheckpoints` remains in the route record and report reconstruction; Registry request/retry/replacement regressions remain green |

The same native Go workspace was used on both sides of the benchmark:

| Dependency | Source revision |
|---|---|
| accelerator | `3a7740e28327f0001c17d37b9c7da25fba53b65d` |
| connector | `6b48c8f450dac707b708a08980a463c1d66ff847` |
| sandboxer | `a63a8df3e5acdb156a8895da0ede4cc86ee99b92` |
| guest-runtime | `a6329f958319ebea4a389d99a34d3bb63f72ed1b` |

Sandboxer's tested tree `a3780530bd6633f99f4c9e2cf5752d83a56b2efb` is identical
to published `v0.2.0-preview.20261121.2`, commit
`3d546021e3b84b1a0acf7ecca9aa320aec1db79d`. The local workspace dependencies
were not edited. These identities describe the local acceptance inputs, not a
claim that hosted CI has selected this vector.

## Functional and build verification

| Gate | Result |
|---|---|
| WSL x86_64 `make test-core` | Passed, including the complete CGO=0 Go suite and source/tooling checks |
| WSL x86_64 `CGO_ENABLED=0 go vet ./...` | Passed |
| Registry/cluster/router/placer/nodelink/routesync race suites | Passed |
| Real three-backend engine and metadata conformance with race detector | Passed |
| Native FDB command and Registry HTTP suite | Passed |
| Real PostgreSQL Registry HTTP suite | Passed |
| Pure-Go SQLite and PostgreSQL binaries | Built; each started, returned HTTP 200 membership and exited on SIGTERM without FDB runtime |
| FDB-capable native package and bundled launcher | Built; bundled `run-cluster-ctl version` passed |
| Shared observation admission and reservation regression | Passed with race detector |
| ARM aarch64 native engine/metadata/command/HTTP race suites | Passed, including real FDB, SQLite and PostgreSQL |
| ARM PostgreSQL HTTP and observation admission | Passed with race detector |
| ARM pure-Go and FDB native binaries | Built; bundled native launcher passed |

The shared suite exercises explicit absence versus empty values, duplicate batch
keys, binary ranges ending in `0xff`, bounded page progress, serializable range
claims, atomic rollback, over-budget rollback, reopen persistence, large chunked
payloads, corrupted chunks, long identities, stale CAS after delete/recreate,
read/observe races and shared handle cleanup. Unknown outcomes with and without
a committed effect are injected at the generic boundary around real database
transactions; this does not claim a real network partition during COMMIT.

The original chat's handoff was checked against test-only extension examples:
bidirectional Group/Node indices, competing claims, registration-guarded bounded
cleanup that preserves a new registration, a stable snapshot latch unaffected by
an unrelated delta cursor, and auth references retained while a live instance
still needs them. These show that the upper design can use shared record
transactions without adding business methods to the drivers. Production Group
authority, canonical request digests, accepted-result binding and CloseCreate
remain the next stage.

An initial new lifecycle test omitted the Node report's resolved IdleTimeout;
its fixture was corrected and the complete affected native suite rerun. The
existing report behavior was not changed to satisfy that assertion. Earlier
failed logs are retained in the task evidence.

On ARM, the default Docker bridge was absent and the public Go module proxy
timed out. The task database was isolated on a loopback port; verified module
files and readable prior caches supplied an offline file proxy, with normal
Go checksum verification retained and a new writable task cache. No shared
Docker configuration or original checkout was changed. The final ARM run
executes on the target architecture; it is not a cross-build-only result.
All 2,377 Go/module/native-harness input files match the WSL source manifest
for the final code. After the SQLite alias fix, the complete native three-backend
race suite passed again on x86_64 and aarch64; both ARM binary variants and the
bundled launcher were rebuilt and checked.
Native and pure-Go binary hashes are recorded separately; these workspace
builds do not embed a VCS revision, so provenance comes from the source
manifests, recorded build inputs and artifact hashes.

## Historical shardkv to first FDB comparison

The historical paired business-path measurements ran on 2026-11-20. The old
side is `c4a6f534ec412710aaf0f24ef35815a449c8cdbe` plus benchmark fixtures; the
FDB binary was built from `dc2d06b9d517403f2ff658bc404d8aca349298fa` plus fixtures.
A preserved source-equivalence receipt associates its 862 tracked Go files with
the then-final `5841d20e43f670f31e0dbd485c130cd636bcbb54` without mismatches.
Both benchmark builds report modified workspaces. These are historical
measurements, not a retest of `22314ae`, `377de1e` or the current shared engine.

The task re-read all 16 raw results, verified their hashes against their run
receipts, and verified both binary hashes and the source-equivalence receipt.
The compact [historical receipt](evidence/storage-20261122/historical-performance-receipt.json)
retains parameters, successes, failures, drops, latency statistics and original
result hashes. Full raw records and invocation logs are retained in the task
evidence; the original remote files were not modified.

Both sides used Go 1.26.4, `GOMAXPROCS=16`, real Registry/Router HTTP paths and
fixed Placer/auth/Node ACK/READY fixtures. No real VM creation or full image
build was measured. The old protocol used volatile memory with a 2/2 quorum
between x86 and ARM. FDB 7.3.77 used eight processes across those two hosts,
`double` redundancy and `ssd-redwood-1` storage. Its before/after status recorded
two storage and two log roles, full recovery and two remaining replicas. This
is not an equal-durability or equal-resource pure-engine comparison. FDB storage
processes had 32 GiB memory / 24 GiB cache limits; other processes had 8 GiB /
512 MiB limits. The test used a 0.02 available-space ratio with a separate
15 GiB free-space guard. No fsync/AIO-disabling option appeared in the saved
startup arguments. These settings are not measured RSS or a hardware IOPS claim.

The closed-loop comparison used concurrency 8, 20 sequential warmup requests,
one excluded trace request and 100 measured requests per path, all successful.
READY delay was 2 ms. Latencies below are milliseconds.

| Path | Old p50 / p99 | First FDB p50 / p99 |
|---|---:|---:|
| Hot resolve | 0.817 / 2.134 | 0.680 / 3.067 |
| Cold resolve | 19.542 / 27.294 | 4.271 / 16.256 |
| Create | 383.032 / 471.290 | 38.258 / 48.947 |
| Build registration | 351.754 / 415.634 | 28.518 / 47.300 |

Fixed-arrival runs used 20 seconds of warmup, a 60-second measured window,
at most 256 requests in flight, no queue and a 10 ms launch-lag limit.

| Offered workload | Old successes | First FDB successes | Old successful scheduled p99 | First FDB scheduled p99 |
|---|---:|---:|---:|---:|
| Cold resolve, 1,000/s | 46,032 / 60,000 | 60,000 / 60,000 | 720.119 ms | 5.781 ms |
| Create, 100/s | 0 / 6,000 | 6,000 / 6,000 | No successful sample | 50.073 ms |
| Build, 100/s | 45 / 6,000 | 6,000 / 6,000 | 19,995.122 ms; only 45 samples | 34.725 ms |

The old cold run dropped 44 late and 13,924 capacity-limited arrivals. Old Create
had 768 failed calls, 14 late drops and 5,218 capacity drops; old Build had 846
failed calls, one late drop and 5,108 capacity drops. FDB cold warmup also dropped
44 of 20,000 arrivals for capacity, despite its successful measured window.
Neither the historical 5 ms cold-read comparison line nor the 20 ms write line
was met. The hot-path p99 also increased. These outcomes must not be described
as an unconditional performance or SLO pass.

For the separate single-Group preload case, the old side completed 1,064 Put
operations and failed on attempt 1,065 before measurement. FDB completed 10,000
Put/CAS operations and then 100 successful Create requests with p99 54.172 ms.
Those preloaded objects were Reserved Route rows, not running sandboxes or a
complete set of ownership references. The old snapshot-size boundary explains
the observed failure path, but the original underlying HTTP status was not
retained. This is not a ten-million-object capacity test.

## First FDB to shared storage comparison

The control is the typed FDB implementation at `5c72e41`, with only the identical
public-repository benchmark fixture added. The new side is clean `f1cd0c5`.
The fixture SHA-256 is
`0af57fa723a1e603113342c83fab342b70bdc3b0a29120aa1d1e8f0ab609a178`.

Both run on the same WSL x86_64 host, Go 1.26.8, six reported CPUs,
`GOMAXPROCS=8`, race detector disabled, fresh FDB 7.3.77 single-SSD databases
and ordinary transaction priority. Each workload runs 500 serial operations in
each of three rounds. Tables show the median of the three per-round statistics;
latencies are milliseconds. Every measured operation succeeded.

| Workload | Before mean | After mean | Change | Before p99 | After p99 | Before / after bytes per operation |
|---|---:|---:|---:|---:|---:|---:|
| Read resolution | 1.492 | 1.325 | -11.2% | 2.620 | 2.269 | 4,018 / 6,808 |
| Intent and ownership | 11.068 | 10.728 | -3.1% | 16.814 | 16.670 | 8,284 / 14,122 |
| List 128 records | 63.798 | 6.832 | -89.3% | 96.524 | 9.641 | 384,426 / 496,045 |
| Read, observe, write, wake | 15.488 | 16.245 | +4.9% | 24.183 | 23.409 | 9,821 / 18,221 |

The first measurement exposed unnecessary revision-cell reads on the ordinary
read path and excessive small FDB range batches. The final implementation uses
the committed header revision for ordinary reads and adaptive FDB range batches.
The initial measurements are retained; the table above uses a fresh paired run
after those changes. The write/observe mean remains slower and allocations are
higher across all four workloads. The durable revision layer has a measurable
cost; this report does not claim universal latency or memory improvement.

The other backends use the same workload and durable settings: SQLite WAL with
FULL synchronization; PostgreSQL 16.15 with fsync and synchronous commit enabled,
serializable transactions. They are backend measurements, not an old/new SQL
comparison.

| Workload | SQLite mean / p99 | PostgreSQL mean / p99 |
|---|---:|---:|
| Read resolution | 0.116 / 0.347 | 2.793 / 6.166 |
| Intent and ownership | 7.817 / 15.222 | 10.517 / 28.710 |
| List 128 records | 1.815 / 3.392 | 3.790 / 6.181 |
| Read, observe, write, wake | 7.746 / 12.044 | 13.781 / 28.850 |

These serial microbenchmarks do not establish production concurrency,
high-availability behavior, scheduled-arrival tail latency or ten-million-object
capacity. The historical comparison above and this same-fixture comparison
answer different questions; their percentages must not be composed into an
old-shardkv-to-current-head performance claim.

## Release-binary qualification

See [storage configuration and contracts](cluster-storage.md) for schema v2,
limits and deployment rules. There is no silent migration from the experimental
FDB layout. Hidden revision cells grow with distinct identities and require an
explicit history boundary before reclamation.

Platform source `73e65ed7b08e39f15935525074703780eb45cd4e` includes the native
cluster launcher and its private runtime.
The previous trusted platform main `d0273490cf5551abce945d1a7ad6ee5c38173e4a`
did not contain `ci/integration/cluster-native-runtime.json`.
[Hosted run 44987177560](https://github.com/kuasar-sandbox/orchestrator/actions/runs/44987177560)
failed all three cluster E2E cases on both architectures because the prepared
products lacked `run-cluster-ctl`. One x86 MMDS recovery case also encountered a
Proxy port collision; the occupying process was not retained. Both source gates
and architecture builds/preparations passed. This is failed delivery acceptance,
not a storage-conformance failure or a passing combined run.

Aggregate `release-v0.2.0-preview.20261122` supplies the SDK baseline used for
release-binary qualification.

[Platform run 44992663426](https://github.com/kuasar-sandbox/kuasar-sandbox/actions/runs/44992663426)
passed both source gates, both builds/preparations, all 95 selected E2E cases on
each architecture, the x86 performance smoke and `ci / finalize`. It tested
platform head `4fdcbfe89614266dcb14677c9185ae0ac6033886` through candidate/framework
`f0a9be6007d29f181643d41351f539beb6e713a3`, with orchestrator head `377de1e` through
candidate `6517130798f5856a881e1bf4038faa2c322a4c11`. The three cluster cases and
MMDS recovery passed on both architectures. Exact per-case results, selected
sources, provenance hashes and declared exclusions are retained in
[x86 results](evidence/storage-20261122/platform-hosted-x86.json) and
[ARM results](evidence/storage-20261122/platform-hosted-arm.json).

This source-lane plan excludes four node-infra cases that require staged release
assets and credentialed OBS. Local node-infra library staging verification below
does not claim a full deployment run. Node Group/request-admission protocols,
production HA/scale qualification and automated data migration are outside
these measurements.

Component run `44992656729` retained the same old trusted platform framework.
After both source gates and architecture builds/preparations passed, it was
cancelled to release shared ARM capacity for platform qualification. Its retained
logs again show all three cluster cases missing `run-cluster-ctl` on both
architectures before cancellation; cancelled jobs and their failing aggregate
checks are not counted as a pass.

Premerge review reproduced a SQLite handle-alias bug: creating a database through
a symlinked parent and reopening its real path split the shared admission budget
(`first=1`, `second=0`). Canonicalization now happens after private file creation.
The regression verifies shared admission, reservation reclamation and a surviving
handle after closing its alias. The complete native suites passed on both
architectures after this startup-only change; the measured transaction paths and
the FDB before/after benchmark inputs remain unchanged.

Platform `4fdcbfe89614266dcb14677c9185ae0ac6033886` also passed local
`test-ci-tools`, `test-release-tools` and `test-perf-tools`. The CI-tools suite
used the pinned EROFS 1.9.1 readers; initial attempts exposed missing Rust and
an older system reader without `--cat`, and passed after matching the documented
toolchain. No product assertions were weakened. An actual x86 native package
with 19 runtime attachments passed ELF/ABI/hash admission, component and
aggregate extraction, source collection, sealed public E2E preparation, the
public runner's bundled launcher and Node infrastructure library staging.
The [delivery receipt](evidence/storage-20261122/native-delivery-artifacts.json)
records all artifact hashes and gate names.

Final benchmark logs and receipts are under
[`evidence/storage-20261122`](evidence/storage-20261122). Detailed local run logs,
the full original-chat handoff and earlier failed attempts are retained at
`D:\CodexTaskData\storage-engine-pr563-20261122`.
