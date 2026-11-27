# Node Group internal foundation

Implementation reference for Node Group persistence. For the supported user workflow, use [Group operations](cluster.md#groups-and-placement); no direct database editing is required.

The Node owns Group persistence. HTTP/UDS and trusted NodeLink maintenance use
the same transaction core; [cluster operations](cluster.md) describes reporting,
import and request admission. Group persistence requires no runtime feature switch.

`node_groups` stores each active name, revision, owner fingerprint, fixed
API/manifest-root binding and the complete private context. Root material and
context use the existing Node encryption box. Deletion clears all context and
root material; only name, revision and owner fingerprint remain. Names preserve
slashes and are limited to 1,024 UTF-8 bytes without control characters.

The transaction core authenticates real API tokens. Active-object management
uses its retained root, even after allowlist expiry or removal. Initial creation,
explicit recreation and new sandbox/build admission require an allowed pair;
trusted combined maintenance may install it within the same transaction. A
different owner cannot read or take over a name, including a deleted name.

Expected version zero means the name never existed. A stale write changes
neither the Group nor the allowlist. A complete equivalent normalized context
returns the existing version. Combined key renewal uses an absolute expiry and
only extends the same pair; it cannot shorten a newer lease, downgrade a
permanent pair or change an active Group's fixed manifest binding after TTL GC.
The existing standalone `AddKeyPair` continues replacing expiry as before.

Each actual Group change advances a Node-wide SQLite catalog sequence in the
same transaction. A consistent snapshot returns that boundary and only the
minimal GroupSupport projection: optional default TemplateID plus authentication
and routing. Key-only renewal does not fabricate a catalog change. This sequence
is independent of the shared storage engine's per-key revisions and is not an
execution/acceptance log.

Private context leaves 8 KiB within the existing 1 MiB frame budget for the
command envelope. Minimal support also satisfies the shared record budget before
commit. Context replacement is complete, not a patch; absent/null templateID
means no default, while an explicitly empty or invalid template fails. Nonempty
sandbox_config requires a default; image_repo and resolved registry_auth remain
valid without one. Placement is a request field and is rejected in Group defaults.

The SQLite tests cover real transaction rollback after pair insertion, competing
CAS writers, no-op renewal, expiry and binding failures, authenticated tombstone
handling, owner-scoped pagination, encryption, and reading a committed result
after closing/reopening the database without consuming its reply. Native
transaction, transport and real VM results are recorded in the
[activation acceptance report](node-group-runtime-acceptance.md).
