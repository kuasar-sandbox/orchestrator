# Group support repository foundation

Implementation reference for GroupSupport publication. For current setup and operator checks, use [cluster operations](cluster.md#minimum-cluster) and [publication diagnostics](cluster.md#groups-and-placement). This foundation does not add a deployment step or a feature switch.

This is the shared metadata foundation for [proposal
#570](https://github.com/kuasar-sandbox/orchestrator/issues/570). The typed
repository uses the existing record/short-transaction layer. No storage driver
understands Group business semantics. Normal Node reports publish support here;
Registry request admission reads the completed directory.

Each full report has an original registration, a synchronization ID, a consistent
Node sequence and a record count. Batches have offsets and are checked for
duplicates, gaps and the snapshot boundary. A batch whose result is unknown can
be explicitly retried with its original identity and offset; the engine does
not replay an unknown commit. A new registration cannot borrow an old stream's
authority. All mutations revalidate the original registration inside the short
transaction.

Full-report completion cleans removed support in bounded pages before publishing
the catalog. A selection reads the support, its stable publication marker and
the live Node registration together. Incremental reports update a separate
sequence cursor; unrelated changes do not rewrite the publication marker or
the selected Group row. Gaps require a new complete snapshot.

Support rows contain the Group revision, optional default TemplateID and routing
per authentication reference. Full private Group context is never stored here.
Authentication material has separate support/object reference records. Adding
an object reference does not write a shared authentication counter. Removing
the last reference uses a bounded serializable prefix check in the same
transaction; concurrent references conflict with reclamation. Removing support
therefore preserves authentication retained by an existing sandbox or Build.
Sandbox/Build route and ownership changes retain or release these object
references inside their existing lifecycle transactions.

Report batches are limited to 128 rows, 128 authentication entries and 256 KiB
of encoded payload. Cleanup also bounds total authentication work per
transaction, including when one Group has many entries. Reverse indexes,
material references, support and progress commit together. Node reaping cannot
finish with support rows still present; an old cleanup claim cannot remove a
new registration's state.

Validation uses the same repository contract on real SQLite, PostgreSQL 16.15
and FoundationDB 7.3.77, with Go race instrumentation. Cases cover incomplete
snapshots, stable publication, ordered deltas, reconnect/reap races, unknown
commits, retained object authentication and bounded cleanup. Existing repository
conformance also runs on all three backends. End-to-end Create/Build/Router,
failure and performance results are recorded in the
[activation acceptance report](node-group-runtime-acceptance.md).
