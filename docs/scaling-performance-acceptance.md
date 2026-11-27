# Cardinality performance validation

Historical performance evidence for the revisions and environment recorded below. Use [cluster operations](cluster.md) and [resource operations](node-resource.md) for current user workflows; these measurements are not production capacity recommendations.

This change addresses repeated or unrelated work as sandbox, Group and authentication-root counts grow. The baseline is `2cc052363d0dd5646a41606f9a77ce14b04053e5`. It does not establish a 100,000-VM capacity limit or a production latency SLO.

## Scope and invariants

| Path | Change | Preserved behavior |
| --- | --- | --- |
| Heartbeat key renewal | Read the Node profile and key leases without enumerating sandbox or Build ownership. | Existing command ACK, lease, stream-registration and pruning rules; aggregate `GetNode` still includes all children. |
| Node SQLite lists | Replace state and owner-candidate single-column indexes with `(state,id)` and `(substr(api_secret_hash,1,24),id)`. | Existing SQL predicates, row validation/decryption, ordering, cursor and public-state semantics. |
| Node key expiry | Add an expiry index containing positive expirations. | Permanent roots remain permanent; only the original expiry predicate deletes rows. |
| File Group import | Validate one directory snapshot after acquiring each source lease; reuse it for all pages and reads in that pass. | Original conditional updates, unknown-outcome handling, pending commands, target selection and source fences. |
| Group reports | Encode each record once for batch sizing, adding exact array delimiters. | 128 rows, 128 authentication entries, 256 KiB array limit, message validation and report sequence. |
| Group authentication | Seek the candidate fingerprint in the existing fixed-width full-fingerprint directory. | Retained pre-upgrade roots, candidate collisions, full root-identity validation and full token MAC checks. |
| SQL metadata scans | Use the stronger inclusive/exclusive lower bound as the actual SQL seek predicate. | Generic byte-range semantics, namespace and end bounds, row/byte budgets and lookahead; no business logic in drivers. |
| Public sandbox lists | Bound each request's scanned candidates, propagate pagination through Router and use encrypted continuation tokens. | Every page rechecks current authentication; state and per-object authorization can produce a short or empty page with a continuation. |

The Group authentication change needs no secondary-index backfill. A candidate page contains at most 128 roots; the storage engine may fetch one additional row to determine whether more data exists. Searches stop when the candidate prefix ends. A real candidate collision is paginated, never truncated or accepted without a MAC check.

## Environment and measurement limits

The measurements use Go 1.26.4 on Linux/amd64, an AMD EPYC 9V74 host, `GOMAXPROCS=1`, and one benchmark operation per selected case. Node SQLite measurements use the production `modernc.org/sqlite v1.40.1` driver and file-backed databases opened through `Store.Open`. Exact dependency revisions are in [environment.json](evidence/scaling-20261125/environment.json).

These are paired observations on a shared machine, not statistically sampled p50/p99 or concurrent-load results. Fixture creation is excluded from timing. Allocation figures are cumulative Go allocations, not peak resident memory. The complete local Go benchmark output is retained beside this document.

## Node SQLite results

Each sandbox dataset contains two owners and interleaved running, paused and pausing records, inserted in descending ID order. Both index layouts use the same data, production insertion validation/encryption and unchanged `Store.List` query/decoding. Pages contain 100 rows. The full inventory verifies strictly increasing IDs and the exact expected total.

| Rows | Owner page at midpoint, previous → composite | Full default-state inventory, previous → composite |
| ---: | ---: | ---: |
| 1,000 | 4.475 → 1.743 ms | 57.82 → 39.30 ms |
| 10,000 | 28.665 → 1.947 ms | 3.316 → 0.409 s |
| 100,000 | 259.111 → 2.365 ms | 304.359 → 4.214 s |

At 100,000 rows, a single explicit-state midpoint page changed from 191.399 to 2.069 ms; the default three-state midpoint page changed from 294.018 to 3.680 ms. The three-state query still merges ranges into global ID order. Its remaining temporary sort is not evidence of a full-prefix rescan.

For key expiry, half the roots are permanent and half have future expirations. No rows are removed in either variant. At 1,000 / 10,000 / 100,000 roots, the previous predicate took 0.214 / 2.584 / 28.503 ms; with the positive-expiry index it took 0.043 / 0.057 / 0.073 ms.

Sources: [sandbox inventory](evidence/scaling-20261125/sqlite-inventory.log), [key expiry](evidence/scaling-20261125/sqlite-key-expiry.log).

The 4.214-second inventory does not prove that telemetry always fits a five-second window: metrics calculation, export and competing writes are excluded. Full inventories still decode every returned row and remain proportional to their total size.

## Group results

The file benchmark invokes the real `importSource` loop with one JSON file per Group. Only lease/checkpoint HTTP replies are supplied in memory, and there are no target Nodes. It measures source processing and command preparation, excluding Node RPCs and writes.

| File Groups | Previous full pass | Snapshot full pass |
| ---: | ---: | ---: |
| 1,000 | 18.574 s | 28.787 ms |
| 10,000 | Not run | 253.375 ms |
| 100,000 | Not run | 2.942 s |

At 1,000 Groups, cumulative allocation decreased from 8.885 GB to 12.669 MB. The 10,000 and 100,000 quadratic baselines were not executed, and no extrapolated elapsed times are presented. The 100,000-Group snapshot pass allocated 1.262 GB cumulatively; this is not its retained heap size.

The NodeLink benchmark invokes real `streamGroups`, including `ValidateMessage`, with an in-memory snapshot and output. It excludes database snapshot reads, network transport and Registry publication.

| Report Groups | Previous | Linear sizing |
| ---: | ---: | ---: |
| 1,000 | 25.534 ms | 1.617 ms |
| 10,000 | 261.255 ms | 11.371 ms |
| 100,000 | 2.838 s | 108.738 ms |

At 100,000 Groups, cumulative allocation decreased from 1.748 GB to 57.868 MB.

Sources: [file before](evidence/scaling-20261125/file-before.log), [file after](evidence/scaling-20261125/file-after.log), [report before](evidence/scaling-20261125/nodelink-before.log), [report after](evidence/scaling-20261125/nodelink-after.log). The new production-loop regression also [fails on the original source](evidence/scaling-20261125/before-regression.log): changing a file between pages interrupted the old pass, whereas the new pass finishes from its validated snapshot and revalidates the changed file on the next pass.

## Authentication and generic SQL seek verification

The candidate-directory regression checks existing-layout roots at the beginning, middle and end, absent candidates, full-zero suffixes, cross-scope/cross-prefix cursors, and synthetic candidate collisions across 128-row page boundaries. The Registry regression requires a later candidate page and verifies the complete token MAC.

Independent benchmarking found a second issue after the business-layer range change: SQLite selected `Begin` for `SeekGE` and evaluated `After` as a per-row filter. A bounded decoded page therefore did not yet guarantee a bounded seek. The generic SQL fix replaces the redundant lower bounds with the stronger one. `ValidateRange` already requires `After >= Begin`, so the exclusive `After` predicate subsumes the inclusive `Begin` predicate.

The production modernc VM now uses `SeekGT` with the cursor argument on later pages. The regression also exercises 1,331 binary Begin/End/After combinations: 340 valid ranges, including 74 empty results, and 991 invalid ranges that retain the existing error contract. PostgreSQL executes the same range-contract test in the required source gate.

| Authentication roots | Candidate lookup before SQL seek fix | Candidate lookup after SQL seek fix |
| ---: | ---: | ---: |
| 1,000 | 0.503 ms | 0.702 ms |
| 10,000 | 1.582 ms | 0.578 ms |
| 100,000 | 13.382 ms | 1.140 ms |

Both columns already include the authentication candidate-range optimization; the first column is **not** the original full-directory authentication baseline. These single-iteration timings include cache and shared-host noise. The stronger evidence is the actual VM seek argument and bounded page/identity checks, rather than a claim of perfectly constant measured latency.

Sources: [before SQL seek](evidence/scaling-20261125/auth-before-sql-seek.log), [after SQL seek](evidence/scaling-20261125/auth-after-sql-seek.log), [VM comparison](evidence/scaling-20261125/sql-seek-bytecode.log), [binary-range and VM regressions](evidence/scaling-20261125/sql-range-regressions.log).

## Pagination and upgrade behavior

The public list remains a JSON array, but its default is now a page: `limit=100`, maximum `1000` scanned candidates, with `nextToken` and the `x-next-token` response header. Complete inventories must continue until that header is absent, including after an empty page. This is an intentional behavior change from the previous unbounded cluster list. The maintained cluster inventory callers are updated accordingly.

The public token encrypts the internal storage cursor using XChaCha20-Poly1305 and a random 192-bit nonce. Its authenticated context binds the operation/version, Group, normalized state filter and complete API-token hash. The internal cursor is not exposed in base64 to clients, because it can contain the name of a filtered object. Cursor encryption does not conceal response sizes, empty pages or the number of requests needed to scan a Group.

One server-only 32-byte key is initialized atomically in the metadata namespace and cached only after success. Existing keys are read-only. Concurrent initialization reads the winning committed key; an unknown commit is returned without replay or caching an unconfirmed key. A malformed key, or an absent key with durable deletion history, fails closed and is never silently replaced. All Registry peers using that namespace can resume a cursor after reconnect or a new Registry instance. Namespace backups must retain the key. Restoring a backup from before its first initialization produces a different key, so previous tokens fail and clients must restart listing. If initialized key material is lost or corrupt, restore the matching namespace/key rather than deleting its history. A cursor is a position, not a multi-page MVCC snapshot.

Node databases build the new indexes during `Open`, then remove the superseded single-column indexes. Index construction requires time and temporary disk space proportional to existing records. No sandbox records or authentication roots are rewritten, and both terminal-retention indexes are retained. Reopening an upgraded database is idempotent.

## Reproducing the measurements

Use the normal multi-repository development workspace, with the dependency revisions from the environment receipt. The project-owned modules are selected from source, as in the source CI gate; do not replace their unpublished workspace versions with unrelated module releases.

```sh
GOMAXPROCS=1 go test ./internal/store -run '^$' \
  -bench '^BenchmarkSandboxQueryScaling$' -benchtime=1x -count=1 -cpu=1 -benchmem
GOMAXPROCS=1 go test ./internal/store -run '^$' \
  -bench '^BenchmarkKeyExpiryScaling$' -benchtime=1x -count=1 -cpu=1 -benchmem
GOMAXPROCS=1 go test ./internal/placer -run '^$' \
  -bench '^BenchmarkFileGroupImportPass$' -benchtime=1x -count=1 -benchmem
GOMAXPROCS=1 go test ./internal/nodelink -run '^$' \
  -bench '^BenchmarkGroupStreamSnapshot$' -benchtime=1x -count=1 -benchmem
GOMAXPROCS=1 go test ./internal/cluster/metastore -run '^$' \
  -bench '^BenchmarkGroupAuthCandidates$/sqlite' -benchtime=1x -count=1 -cpu=1 -benchmem
```

The SQLite benchmark reconstructs the previous index layout only inside its temporary database. For the Group baseline, copy the identical added benchmark files into a separate checkout of the baseline commit. Limit the old file-import benchmark to its `/1000` case; the two larger old cases intentionally do not form part of this evidence.

## Validation boundary

Regression coverage includes old-database reopen and exact decoded-record preservation; mixed owners/states and cross-page completeness; production query plans; key renewal without any ownership reads; missing/cancelled/partial key reads; file snapshot isolation and next-pass visibility; exact JSON size limits and report ordering; fingerprint-bound seeks and full-MAC checks; and public pagination, cursor privacy, current authorization, namespace-key concurrency and unknown outcomes.

The required merge gate is `ci / finalize`. The source workflow runs the full Go suite, race detector, vet, real PostgreSQL and FoundationDB repository contracts, source fixtures and the native integration suite. Final gate status belongs to the PR's exact submitted head.

This change does not remove every linear task: target selection still considers Group–Node bindings, key renewal still visits that Node's key leases, complete inventories still visit all records, and background reaping, idle scheduling, reconnect snapshots and observation fan-out have separate cardinality costs. No native port-capacity increase, revision-cell deletion, recovery/fencing redesign or 100,000-live-VM claim is included.
