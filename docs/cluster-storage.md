[English](cluster-storage.md) | [简体中文](cluster-storage_zh.md)

# Cluster metadata storage

Registry uses one shared metadata implementation on SQLite, PostgreSQL or
FoundationDB, including execution ownership and completed Node Group support.
Node owns private Group context; Registry authenticates and selects from published
support. Router and the optional Placer use Registry protocols and do not open
the database. Start with the [minimum two-node cluster](cluster.md#minimum-cluster).

## Configuration and deployment

Choose a backend before first startup:

| Backend | Deployment | Required durable input |
|---|---|---|
| SQLite | One Registry serving multiple compute Nodes | Private persistent local database file; do not share it between Registry processes or use a network filesystem |
| PostgreSQL | One or more Registry peers | One database, the same namespace, and a service environment variable containing the DSN |
| FoundationDB (default) | One or more Registry peers | Reachable database, readable cluster file, and the same namespace |

Generate the full role configuration with `run-cluster-ctl config registry
--template`; choose the backend in that file. For example, the storage portion of
a **single-Registry** setup is:

```yaml
storage:
  backend: sqlite                 # fdb (default), sqlite, postgres
  namespace: example/records-v2/   # identical for Registry peers
  sqlite_path: /var/lib/cluster-ctl/metadata.sqlite
  # postgres_dsn_env: CLUSTER_STORAGE_POSTGRES_DSN  # used only with backend: postgres
foundationdb:
  cluster_file: /etc/foundationdb/fdb.cluster
  prefix: example/                # legacy namespace base, if namespace omitted
```

Only the selected backend is opened. PostgreSQL credentials come from the named
environment variable, not YAML or the membership API. For example, set
`CLUSTER_STORAGE_POSTGRES_DSN` to the deployment's PostgreSQL connection string
and select `backend: postgres`. For the shipped systemd unit, create a root-owned
mode-0600 `/etc/cluster-ctl/storage.env` containing
`CLUSTER_STORAGE_POSTGRES_DSN=postgresql://...` with the actual database, user,
password and TLS requirements. Add a service drop-in:

```ini
# /etc/systemd/system/cluster-registry.service.d/storage.conf
[Service]
EnvironmentFile=/etc/cluster-ctl/storage.env
```

Run `systemctl daemon-reload` and start/restart this Registry through its normal
maintenance procedure. An `export` in an operator's shell does not configure an
already managed systemd service. Do not print the DSN in diagnostics or pass it
as a command-line argument. All peers must reach the same database and namespace.
Backend, namespace and connection changes require restart. A membership
`https://` advertisement requires the matching listener certificate/key and peer
trust; it does not enable TLS by itself.

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
as a migrated database. There is no supported execution-state export/import API
and no tool that rebuilds Registry ownership from Node databases, route events,
Group definitions or portable snapshots. Do not fabricate old SID/Node/ready rows.

For disposable pre-release state, stop new admission, finish or explicitly remove
test-owned work through the old version, stop the old roles and retain any needed
evidence. Start the new release with a fresh Registry database/namespace and
fresh Node state, reinstall roots and Groups, then create new Sandboxes from
compatible retained canonical artifacts. This is a rebuild with new execution
identities, not an in-place migration or same-ID recovery. Keep old data isolated
until its retention decision has been made; do not delete unrelated state.

For a same-schema database restore, stop all Registry/Router/Placer processes
that could write or cache that history and preserve the corresponding Node
ownership evidence. Restore a database-consistent backup by that backend's
native procedure, including SQLite WAL consistency. A database backup alone
cannot roll running Nodes back to that point in time. Keep admission closed
until ownership reconciliation is demonstrated; a new empty namespace does not
repair mismatches. Node DB/encryption keys, tenant roots, canonical artifacts and
their complete dependencies are separate backup inputs. The [recovery contract](cluster.md#registry-state-ownership-and-disaster-recovery-boundaries)
defines the current limit; loss of required ownership state cannot promise
identity-preserving recovery.

## Validation and future metadata extensions

`storagetest` runs the same transaction, record, ABA, retry, unknown-outcome,
observation and representative repository tests on real backends. The production
Group-support repository also tests registration guards, completed publication,
selection competition, retained object authentication and bounded cleanup. These
business contracts remain above the engine; they do not require Group/Create
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
