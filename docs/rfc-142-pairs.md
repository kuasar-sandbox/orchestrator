# RFC-142 paired checkpoint identity

[English](rfc-142-pairs.md) | [简体中文](rfc-142-pairs_zh.md)

`ResumeSource` keeps `Kind` and `Ref`. A Snapshot additionally requires
`SandboxRef`, the exact E selected by that S. An E-only source holds E in `Ref`
and has an empty `SandboxRef`. The pair is one durable value in SQLite, cache,
CAS comparisons, recovery and encrypted migration tokens. It is never a
best-effort cache. `removedRefs` belongs only to operation reports.

Runtime capture returns the producer's actual identities. `snapshot --json`
returns `{snapshotRef,sandboxRef,removedRefs}`; `export --json` returns
`{sandboxRef,removedRefs}`. Capture removals are `[]`. A Bundle carries two
selectors in the same physical file. Local refs retain their identity qualifiers
but expose only basenames; the checkpoint directory is execution context.
Default human output and upload-key stdout remain compatible.

An external S-only template is an explicit initial preparation input. Its
starting row has no accepted durable source. The existing tenant task parses
S/E and returns the paired identity in its bounded summary. RunID, root, launch
mode and both identities participate in the resolution digest. Identical
summaries replay; different E submissions conflict. Only the matching starting
run can atomically commit the pair with running state. An already accepted
pair cannot be replaced by preparation. Full configs and key-dependent parsing
remain in the tenant task. Builder recovery also freezes the accepted pair in
its existing runtime preparation record and compares it on every replay.

Publication returns final S1/E1 in both report and token. Keep-source retains
local S0/E0. Portable Export uses only the stored pair, even with inaccessible
artifacts or storage; it performs no artifact reads or metadata subprocesses.
Import authenticates and atomically inserts the complete pair without opening
artifacts. Exact target expectations reject S1/E2 when they require S1/E1.

Move Export returns after accepting source deletion durably, with cleanup owned
by the normal retrying finalizer. Both local and portable sources lose their
owned BaseDir/checkpoint eventually; shared published artifacts remain. A failed
acceptance preserves paused S0/E0, while a later cleanup failure retains the
`deleting` row and valid S1/E1 result. Same-ID Create/import conflicts until the
row is finalized. See the [node lifecycle contract](node.md#81-artifact-capture-templates-and-migration)
for cancellation, restart and observer ordering.

Before upgrading, stop the old conductor and drain or retire managed sandboxes.
Preserve needed records and artifacts, then have the operator explicitly clear
the old local database and recreate records with the matching sandboxer and
orchestrator versions. There is no pair-schema migration, ALTER, backfill or
dual-read compatibility. The process refuses the old schema and never deletes
user data to upgrade it. Old tokens without the required `resumeSandboxRef`
field, including Snapshot tokens missing E, are rejected; discard them and re-export from complete paired records. Export never repairs missing E
by reading S.

Validation includes real capture producers and CLI responses, atomic SQL
rollback/reopen and lifecycle CAS, preparation identity/replay tests, strict
authenticated token tests, and the 16-case S/E × token/template × keep/move ×
local/portable CLI/API matrix. Set `KUASAR_TEST_SANDBOX_CTL` to the companion
binary to execute the integration cases. Portable matrix cases import the token
and make source artifacts unavailable before re-exporting.
