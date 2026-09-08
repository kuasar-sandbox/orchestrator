[English](sandbox-identity.md) | [简体中文](sandbox-identity_zh.md)

# Sandbox identity on Create

A direct conductor `POST /sandboxes` can select its node-local SandboxID and,
optionally, an independent StableID. This is a creation-time configuration input,
not an identity reservation API or idempotent result replay.

## Two equivalent inputs

Use the existing namespaced metadata carrier (the value is a JSON **string**):

```json
{
  "templateID": "<canonical-template-id-or-alias>",
  "timeout": 300,
  "metadata": {
    "kuasar-sandbox.identity": "{\"id\":\"worker-42-instance-3\",\"stable_id\":\"worker-42\"}",
    "application": "worker"
  }
}
```

Alternatively, supply the same identity object through the configuration Header:

```http
X-Kuasar-Sandbox-Identity: {"id":"worker-42-instance-3","stable_id":"worker-42"}
```

No new top-level Create body field is introduced. SDK callers can use their
existing `metadata` option. The Header overrides the **entire** metadata identity
object; it does not merge individual fields. Both supplied input layers must be
valid, so a valid Header cannot hide malformed identity metadata.

For example, metadata `{"id":"body-instance","stable_id":"body-stable"}`
with Header `{"id":"header-instance"}` selects `header-instance` for both the
local ID and effective StableID. Header `{}` clears both lower-priority choices
and uses normal defaults.

## Field contract

| Input | Node-local SandboxID | Effective StableID |
|---|---|---|
| Absent identity or `{}` | New UUIDv7 | Node-local ID |
| `id` only | Supplied `id` | Node-local ID |
| `stable_id` only | New UUIDv7 | Supplied `stable_id` |
| Both fields | Supplied `id` | Supplied `stable_id` |

An empty string field means unspecified. An absent StableID remains empty in the
stored optional field; `Sandbox.StableID()` supplies the local-ID fallback.

Both nonempty fields in this new Create configuration use the existing
`ValidLocalSandboxID` contract: 1..57 bytes, lowercase ASCII letters, digits and
hyphens, starting and ending with a letter or digit. The exact pattern is
`^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$`. Uppercase, dots, slashes, whitespace,
NUL and longer values are rejected rather than normalized. This does not change
the historical migration-token or trusted cluster StableID contracts.

The object accepts exactly `id` and `stable_id`. Empty Header values, non-object
JSON, null (including null field values), duplicate Headers, duplicate or unknown
JSON fields, trailing JSON, invalid ID strings and non-string fields return 400.

The Create response's `sandboxID` remains the **local** ID. Node lifecycle URLs,
Proxy host/header addressing, local paths and route-table keys use that ID.
StableID is not a lookup alias or a separate endpoint. Merely selecting StableID
without `id` does not provide local-ID deduplication or a predictable local URL.
There is no required string relationship between the two IDs; `-g0` is not an
input format imposed by conductor.

## Scope and ownership

Identity is interpreted once for this Create request and removed from the
metadata passed to the Hook and stored on the Sandbox. Dedicated `Sandbox.ID`
and `StableIDValue` fields are authoritative afterwards. Ordinary application
metadata is preserved. Templates, group defaults and placement defaults cannot
supply inherited identity; `MergeCreateMetadata` admits it only from the current
request.

Selecting an ID grants no permission and does not create cluster ownership.
Direct conductor Create remains `OriginDirect` with `Cluster == nil`, even when
the conductor is connected to a Registry. Existing credential-pair allowlisting
and API-key ownership checks are unchanged.

Create Hooks see the final local ID in the protected operation envelope. They
cannot change it or reintroduce `kuasar-sandbox.identity` into mutable metadata.
Other supported request changes are revalidated as before. The final StableID is
set before generating any sandbox credentials.

Default ServiceSecret is derived from APISecret and StableID; the forward token
is bound to that StableID. Reusing a StableID with the same default credential
inputs therefore does not automatically invalidate all old credentials. StableID
is a security-relevant stable identity, not an arbitrary display label.

| Entry point | Identity configuration |
|---|---|
| Direct conductor Create | Accepts metadata and Header |
| Direct or cluster Build registration | Rejects explicit identity with 400; never stores it in a Build or template |
| Cluster Router public Create | Rejects both carriers; Registry retains identity allocation authority |
| Trusted node-link Create | Uses existing typed `SID` and cluster StableID fields; rejects an identity namespace in `Config` |
| Import / Connect | Existing target-ID and migration-token semantics remain unchanged; no new identity override |

## Conflict, retry and deletion

Create is insert-only. A retained row in any state, or an active launch owner for
the local ID, prevents a second creation. At the identity-conflict stage both
sources are classified as 409 with the existing message response shape:

```json
{"message":"sandbox already exists"}
```

The loser never replaces the existing record, cancels the winner, tears down its
resources or returns its credentials. Requests can still fail earlier for invalid
credentials/configuration or unavailable Proxy admission; conflict detection does
not reorder those checks.

409 is not a successful replay and does not assert that the existing object has
the same definition or originated from this request. After a lost response the
caller can use the known local ID and its existing credentials to query the
object and handle its state. There is no stored Create-result replay. This is
node-local conflict protection, not cross-node global uniqueness.

A failed Create can retain a `dead` row: for example, the Proxy route barrier can
fail after durable starting admission. That ID remains occupied. Deleting or
requesting deletion is not automatically equivalent to having completed cleanup;
reuse is possible only after the existing finalizer/retention has actually
released the row and active launch ownership. Create never implicitly resumes,
replaces or cleans an existing object. The unchanged list cursor orders by local
ID, so caller-selected IDs do not imply creation-time order.

## Shared implementation

Direct API and trusted node-link adapters retain their own authentication,
template ownership, timeout and MMDS boundaries. They share pure configuration
normalization, final Sandbox construction and `acceptFreshLaunch`. Cluster
prechecks return independent parsed output instead of modifying `Command.Config`.
The direct template alias policy and the cluster canonical-template restriction
remain distinct; no new public Hook or wire protocol is introduced.

Common admission still claims launch ownership, atomically inserts the Sandbox
and optional MMDS values, publishes starting, waits for the Proxy route-applied
barrier, and only then schedules the asynchronous launch. Direct HTTP returns 201
for this durable acceptance, not guest readiness. Node-link returns its accepted
ACK at the same boundary; `CreateCluster` remains a synchronous wrapper waiting
for that exact attempt.

See [Node](node.md), [Cluster Router](cluster-router.md) and
[Extensions](extensions.md) for the surrounding contracts.
