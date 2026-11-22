# Storage refactor acceptance, 2026-11-22

This report covers the shared storage stage of orchestrator PR #563. It does not
claim that the subsequent Node-owned Group/create-accept-close redesign is
implemented, or that the PR has passed combined hosted delivery acceptance.

## Implementation and source identity

The implementation under test is `f1cd0c5e99b170c04094a785c3d07699f31041d5`
(tree `36a645311cd6faae920f853c1a2064f06a939b2e`). It follows merge baseline
`5c72e418c49bd333e312b10fba1000d7c0d918b3`, which combines the previous PR head
`22314ae768a1cb87895395a4c3f155ba0d4dd6b8` and main
`e1e34f99f7421bbb1ab3ff5226eeb8f7f1b45026`. Later changes in this delivery add
this report, evidence and an observation-admission regression test.

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
All 2,376 Go/module/native-harness input files match the WSL source manifest.
Native and pure-Go binary hashes are recorded separately; these workspace
builds do not embed a VCS revision, so provenance comes from the source
manifests, recorded build inputs and artifact hashes.

## Paired performance results

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
capacity. The earlier custom-shardkv comparison belongs to an older source
snapshot and is not reused as evidence for this head.

## Delivery and remaining acceptance boundary

See [storage configuration and contracts](cluster-storage.md) for schema v2,
limits and deployment rules. There is no silent migration from the experimental
FDB layout. Hidden revision cells grow with distinct identities and require an
explicit history boundary before reclamation.

The independent platform native-runtime PR
[#426](https://github.com/kuasar-sandbox/kuasar-sandbox/pull/426) remains open.
Trusted platform main `d0273490cf5551abce945d1a7ad6ee5c38173e4a` does not contain
`ci/integration/cluster-native-runtime.json`. Its former exact #563 cutover also
names an older base/head/merge and dependency vector. Those identities must be
reconciled with this candidate before combined hosted delivery can be accepted.
The historical failed component run and the platform's old successful run are
not results for this implementation. Neither PR nor an issue is closed by this
report.

Final benchmark logs and receipts are under
[`evidence/storage-20261122`](evidence/storage-20261122). Detailed local run logs,
the full original-chat handoff and earlier failed attempts are retained at
`D:\CodexTaskData\storage-engine-pr563-20261122`.
