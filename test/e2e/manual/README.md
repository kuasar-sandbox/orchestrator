# Real cluster failure acceptance

These scripts extend the prepared product E2E fixture with destructive **test
process** failures. Run them on an isolated Linux KVM test host, sequentially.
They use real Node SQLite, NodeLink streams, FoundationDB and guest execution.
They are manual acceptance cases, not automatic source-only tests.

Prepare `BIN`, `E2E_LIB`, `ORCHESTRATOR_BASE_IMAGE`, `ZOT_BIN`,
`FDB_SERVER_TEST_BIN` and `FDB_CLI_TEST_BIN` exactly as for
`orchestrator.cluster-lifecycle.sh`. The base image needs Python 3 for the normal
WebSocket fixture. Copy this revision's `test/e2e/lib` into
`$E2E_LIB/orchestrator`. Supply a fresh `WORK` and `OUT` for each run. The regular
fixture refuses to overwrite existing systemd templates and cleans only its own
instances. The two-Node migration uses separate target unit templates so a
source conductor's orphan reconciliation cannot stop the target Node's VMs.

Build the test-only ordering relay with the selected source/dependency workspace:

```sh
CGO_ENABLED=0 go build -trimpath -o /your/test/tools/nodelink-fault-relay ./test/e2e/nodelink-fault-relay
FAULT_RELAY=/your/test/tools/nodelink-fault-relay bash test/e2e/manual/cluster-faults.sh
bash test/e2e/manual/cluster-migration.sh
```

The failure script exercises lost Create ACK, running report before generic
error ACK, Registry SIGKILL both before and after Node acceptance, initial
Group version conflict, generic rejection with a negative Node query, concurrent
callers with different credentials/Placement, and existing-object authorization
after Group deletion. It checks Node rows, exact local identity, actual VM execution, public
credentials and Create delivery counts. The relay stores only frame types,
command/Sandbox IDs and states; it excludes command bodies and credentials.

The generic-rejection case observes one Registry decision directly because the
public Router can retry a temporary response after logical-slot retirement.
It does not classify that later invocation as a second delivery of the old
execution identity. Native Registry tests separately check retained ownership
and authentication after timeout on SQLite, PostgreSQL and FoundationDB.
They also inject rollback/Unknown at physical transaction boundaries and
replay old ACK/report frames across two real HTTP/2 NodeLink streams.

The migration script exports a real annotated Snapshot, makes the target Group
default refer to an unavailable runtime, and passes only the opaque KMT to
Connect after the source Node disappears. It checks that Node-decoded captured
input restores the guest, logical identity and public credentials remain stable,
the local generation changes, and a returning source is cleaned without harming
the target. This also covers invalidation of an old Router data-cache target.

Keep the JSON receipts, complete command/source/binary identities, and logs.
Record failures and the exact injection boundary. These cases do not by
themselves establish every F01–F12/C01–C12 case, physical exactly-once execution,
multi-host availability, or production performance.

## Product-process measurements

The optional benchmark uses the regular cluster-stub acceptance fixture. Four
logical Nodes share one execution-stub process; Registry, Router and Placer are
real processes. The storage server is separate. This does not measure VM startup
or multi-host HA. With the normal native binaries and isolated FDB helper:

```sh
CLUSTER_STUB_CASE=registry-n1 NODES=4 SCALERS=1 \
  CLUSTER_BENCH_COUNT=200 CLUSTER_BENCH_OUTPUT=/your/evidence/candidate.json \
  bash test/source/cluster_stub.sh
```

Use the same driver and optional hook on the baseline, match dependencies and
hardware, and alternate three before/after pairs without concurrent load. The
JSON records request latencies, throughput, status counts and product-process
CPU/RSS. CPU ticks require the recorded clock frequency; RSS excludes FDB and
can double-count shared pages. Quantiles use the documented empirical order
statistic. Historical acceptance samples preceded raw-latency retention; their
recorded quantiles must not be presented as recomputed nearest-rank estimates.
Group CRUD/renewal and directory costs, including Go allocation counts, are
separately measured by BenchmarkNodeGroup and BenchmarkGroupDirectory.
