# Cluster metadata storage

Registry uses one shared metadata implementation on SQLite, PostgreSQL or
FoundationDB. This storage stage preserves the existing Registry/Router/Placer
business protocols. Node-owned Group context, Group support reporting and the
revised create/accept/close protocol are a separate upper-layer change.

## Configuration and deployment

```yaml
storage:
  backend: sqlite                 # fdb (default), sqlite, postgres
  namespace: example/records-v2/   # identical for Registry peers
  sqlite_path: /var/lib/cluster-ctl/metadata.sqlite
  postgres_dsn_env: CLUSTER_STORAGE_POSTGRES_DSN
foundationdb:
  cluster_file: /etc/foundationdb/fdb.cluster
  prefix: example/                # legacy namespace base, if namespace omitted
```

Only the selected backend is opened. PostgreSQL credentials come from the named
environment variable, not YAML or the membership API. For example, set
`CLUSTER_STORAGE_POSTGRES_DSN` to the deployment's PostgreSQL connection string
and select `backend: postgres`. All peers must reach the same database and
namespace. Backend, namespace and connection changes require restart.

SQLite requires a persistent local file and a single Registry member in every
configured membership version. It uses WAL, `synchronous=FULL` and immediate
write transactions. PostgreSQL uses serializable transactions and
`synchronous_commit=on`; startup rejects `fsync=off`. Database replication,
failover, backup and restoring a consistent history remain deployment concerns.
The tested FoundationDB client/server version is 7.3.77 with API 730.

SQLite and PostgreSQL need no FDB headers, libraries, build tag or CGO:

```sh
CGO_ENABLED=0 go build -o cluster-ctl ./cmd/cluster-ctl
./cluster-ctl registry --config registry.yaml
```

The existing native release still includes the FDB-capable binary and bundled
runtime. `make build-cluster-native` / `run-cluster-ctl` remain its supported
build/launch path. Selecting FDB in a pure-Go build fails with an explicit error.

## Shared implementation and engine contract

`internal/cluster/metadata` defines business records and repository contracts.
`internal/cluster/metastore` implements them once. `internal/storage/record`
provides record identities, revisions, integrity, chunks and conditional writes.
`internal/storage` defines ordered byte keys, `View`, `Update`, `Get`, `GetMany`,
bounded `Scan`, `Put` and `Delete`; observation is a separate capability.
`internal/storage/sqlengine` and `internal/fdbdriver` contain backend adaptation.
An architecture test rejects business-package imports in the storage packages.
Registry receives repositories, never a generic transaction closure.

Engine callbacks execute once. Reads use a consistent snapshot; writes are
serializable, include absent keys/range predicates, and read their own writes.
`GetMany` preserves order, duplicates and absence versus an empty value. Range
ordering is byte-lexicographic and cursors are exclusive. Pagination across
transactions does not retain one snapshot.

Only a definitely aborted transaction returns `ErrRetry`. The record layer may
retry it within a four-second operation deadline while retaining initially read
record revisions. A changed guard returns `Conflict`; it does not authorize a
new selection. A range scan on a retry is a new range read: callers needing a
specific candidate or snapshot must keep its explicit identity/version or use a
business snapshot protocol. Unknown commit outcomes are never automatically
replayed. Results and Node RPC side effects are published only after confirmation.

Versions retain the ten-byte / 20-hex representation. They monotonically order
one logical key in one namespace, including deletion and recreation; they are
not native versionstamps, wall clocks or a global commit order. An atomic record
mutation assigns `max(previous revisions of mutated keys)+1` to its writes.
Missing visible records still use zero for expected absence. Hidden revision
cells retain the original logical identity and survive deletion to prevent ABA.
They grow with the number of distinct identities ever mutated and cannot be
garbage-collected by an ordinary TTL. Namespace replacement/restoration requires
discarding old process caches and defining a new history boundary.

Long equality-key components are hashed in the common codec; original identities
remain in headers and revision cells and are checked. Scope prefixes survive.
This does not promise user-string lexicographic ordering of hashed components.
Fields requiring ordering use short, fixed-width encodings. Hashing is shared
by all backends, not a hidden SQL-driver behavior.

## Bounds and observation

| Bound | Contract |
|---|---|
| Physical key | 2,048 bytes, independent of namespace |
| Physical value | 100,000 bytes |
| Record chunks | 64 KiB with length, count, identity and SHA-256 checks |
| Logical transaction budget | Below 9,000,000 accounted bytes; staging is bounded too |
| Native transaction budget | Also enforced; FDB includes namespace/conflict-range overhead under 10 MB |
| Scan request | 1–1,024 rows, 1 byte–2 MiB byte budget |
| Large first item | One item may exceed the requested page byte budget, allowing progress |
| Batch point read | At most 1,024 keys |
| One observation | 1–256 durable conditions |
| Observation owner budget | 8,192 slots, including 256 reserved for catalogs |

A route read returns its durable revision condition. Arming compares that
condition again, so a change between read and registration immediately
invalidates. FDB watches durable revision cells. SQL owners coalesce duplicate
keys and poll in batches of 256 with a 100 ms fallback; local writes wake the
poller immediately. PostgreSQL LISTEN/NOTIFY accelerates the same reread and
carries no business payload. LISTEN is established before its state recheck;
connection loss invalidates active observations. Missing/coalesced notifications
cannot replace or lose the persistent revision check. Cancel and Close wait for
resource reclamation; reservations survive sequential rearming.

## Schema and upgrade boundary

This is record schema v2. SQL uses the private `kuasar_storage_v2` KV table with
a namespace hash; FDB uses the configured namespace plus private record keys.
When `storage.namespace` is omitted, it defaults to
`foundationdb.prefix + "records-v2/"`. With the default legacy prefix this is
`kuasar-sandbox/cluster/records-v2/`.

There is no automatic in-place conversion of the prior experimental FDB schema.
Do not point the new codec at old record bytes or treat an empty new namespace
as a migrated database. Retain old snapshots/evidence separately; any deployment
holding old data needs an explicit export/import and cache-history cutover.
The PR's existing native release layout is unchanged.

## Validation and future metadata extensions

`storagetest` runs the same transaction, record, ABA, retry, unknown-outcome,
observation and representative repository tests on real backends. The extension
example adds Group/Node indices, registration guards, selection competition and
bounded cleanup above the engine. It is an isolation proof, not the production
Group protocol. Snapshot latch/cursor, immutable auth references and accepted
result binding belong in shared metadata; they do not require Group/Create
methods in a driver. `MaxCheckpoints` remains part of current business regression.

```sh
go test -race ./internal/storage/... ./internal/cluster/metastore
# Set STORAGE_TEST_POSTGRES_DSN to a real isolated database for PostgreSQL cases.
STORAGE_TEST_BACKEND=postgres go test -race ./test/e2e/cluster_stub
python3 scripts/with-test-fdb.py -- python3 scripts/cluster-native.py test --arch x86_64 -- -race
```

PostgreSQL tests skip without their explicit DSN. Native FDB tests require an
explicit test cluster and do not silently skip. The shared benchmark harness
reports latency percentiles, throughput and Go allocations for resolution,
intent/ownership, 128-record listing and observation wakeup. Paired FDB runs use
the same workload, dependencies and durable database settings. These tests do
not establish production high-availability behavior or ten-million-object SLOs.
