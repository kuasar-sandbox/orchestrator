[English](sandbox-lifecycle.md) | [简体中文](sandbox-lifecycle_zh.md)

<a id="overview"></a>

# Sandbox lifecycle

Use this contract to create, inspect, pause, resume, reset, delete, export and import a Sandbox. Public acceptance, durable identity and retained data are defined here; host service setup belongs to [node operation](node.md), credentials and guest access to [access and proxy](node-proxy.md), and template production to [Build](node-build.md).

**Read by task:** [API](#sandbox-api) · [Identity](#create-identity) · [Pause](#capture-and-pause) · [Resume](#resume) · [Reset and retention](#reset-and-retention) · [Export and migrate](#publication-and-migration) · [Request inputs](#request-configuration) · [State ownership](#state-and-ownership)

<a id="reading-path"></a>

## Reading the lifecycle

For API use, read the operation table and identity rules, then the capture/resume and export/import sections. `running`, `starting`, `pausing`, `paused`, `dead` and `deleting` are distinct management states. Successful Create/Connect acceptance can precede runtime readiness. A memoryless pause retains disk artifact E; resume with `memory=false` cold-starts from E. A dead Sandbox retains its data until explicit Delete or `sandbox.dead_ttl` drives final deletion; `autoReset` is a separate policy, defined below. Implementation ownership and launch preparation follow the public contracts.

<a id="public-api"></a>

## e2b API contract

Base URL is `https://api.<domain>`. Authentication accepts **X-API-KEY** for SDKs or **Authorization: Bearer** for CLI builds, parsing both identically. APISecret signs keys through e2b-key-ctl gen-apikey; conductor verifies their MAC to identify tenants without a static api_keys table ([Credentials and ownership: encrypted APISecret/ManifestKey pairs](node-proxy.md#credential-model)).

**Ownership:** ID-based operations verify the API-key MAC against the resource row's decrypted APISecret. Mismatch returns **404**, hiding another tenant's existence. Create/build/import additionally require the complete APISecret/ManifestKey pair to be allowlisted, otherwise **403**.

<a id="sandbox-api"></a>

## Control plane: sandbox lifecycle

| Operation | Method and path | Contract |
|---|---|---|
| Create | POST /sandboxes → 201 | Body templateID/timeout/metadata/envVars/optional idleTimeout/autoPauseMemory/autoReset/max_checkpoints (fixed at 1) plus optional X-Kuasar-Sandbox-* headers. Omitted/null/true autoPauseMemory captures S at TTL; false captures E, without changing explicit Pause's default. 201 is durable starting acceptance and does not wait for runner/runtime/envd. e2b returns Envd/Traffic/Forward tokens; bare returns Forward only |
| Get | GET /sandboxes/{id} | Includes state/startedAt/endAt/metadata, autoReset, optional deadAt/executionResult/autoResetError ([Termination, Reset and retained data](#reset-and-retention)) |
| Usage stats | GET /sandboxes/{id}/stats/usage | Shared native current/saved/history reader; lossless integers and coverage, online owner/offline locks, no Wake or sampling |
| Resource stats | GET /sandboxes/{id}/stats/resource | Read-only effective resource specification and host VMM counters, with observed node reservation; sparse JSON, no guest access |
| Metrics history | GET /sandboxes/{SandboxID}/metrics?start=...&end=... | Exact SandboxID ownership, opaque live telemetry UDS forwarding; 503 when unavailable, no Wake/Resume; [E2B contract](observability.md#e2b-metrics-history) |
| Traffic stats | GET /sandboxes/{id}/stats/traffic | Final node Proxy's current parking/connected and conservative idleSince; no Wake/Resume |
| List | GET /v2/sandboxes | Tenant-scoped state/limit/nextToken query. Omitted state lists running/pausing/paused; explicit states support diagnosis. x-next-token pagination; items include cpuCount/memoryMB/diskSizeMB and ISO-8601 startedAt/endAt. CPU/memory retain capacity/SKU meaning, not headroom |
| Kill | DELETE /sandboxes/{id} → 204 | Non-owner returns 404. Atomically transfer complete ownership into deleting, withdraw business routing and publish a deleting management projection before response. Finalizer cancels launch, fences runner/source users, detaches under allocation fence, removes all owned RunDir/BaseDir data, then hard-deletes the parent and management location. Pending repeats are idempotent |
| Reset | POST /sandboxes/{id}/reset → 204 | Wait for durable E-only paused; already E-only paused is idempotent. Other non-dead states conflict (409), owner mismatch is 404, unavailable/invalid current disks fail with 422 and retain dead. No VM launch ([Termination, Reset and retained data](#reset-and-retention)) |
| Resume | POST /sandboxes/{id}/connect | Body timeout in seconds and optional bool/null memory. Kuasar maps nil to auto, true to memory, false to cold. Paused atomically becomes starting with durable launch_mode before response. Missing targets may synchronously import paused state from X-Kuasar-Migration-Token, then use the same admission. Response does not wait for launch |
| Exec session | POST /sandboxes/{id}/exec-sessions → 201 | X-API-KEY only. Mint execAccessToken; optional ttlSeconds, CEL conditions and X-Kuasar-Migration-Token. Does not create a guest process |
| Pause | POST /sandboxes/{id}/pause → 204 or 200 with checkpointID | checkpoint/ifIdle default to false ([Capture and Pause](#capture-and-pause)). Omitted/null/true memory saves Snapshot S; false saves Sandbox E. False with snapshot-only merge/drop fields returns 400; ordinary already-paused or starting returns 409; default memory Pause adopts resource-paused state without recapture |
| Timeout | POST /sandboxes/{id}/timeout | Body timeout in seconds resets TTL; starting permits a narrow-field update |

Compatibility is bounded by the upstream API/SDK versions and actual tests. The upstream [Create](https://docs.e2b.dev/api-reference/sandboxes/create-sandbox), [Pause](https://docs.e2b.dev/api-reference/sandboxes/pause-sandbox) and [Connect](https://docs.e2b.dev/api-reference/sandboxes/connect-to-sandbox) references checked on 2026-09-07 document autoPauseMemory and memory selection, including Connect memory=false. Thus the field is not exclusively a Kuasar extension. Kuasar's nil→source-dependent auto behavior and its existing authorized-traffic Wake of both E and S are specific local semantics; upstream documents restrictions on traffic-triggered resume of filesystem-only snapshots. Kuasar does not claim the complete E2B autoResume policy.

Create templateID accepts a persistent ID ([Template IDs and artifact authority](node-build.md#artifact-identity)), registration-time transient ID, or a ready Build's name/alias. The latter two resolve to persistent ID before the common path. envdVersion is `0.6.1` for e2b and compatibility stub `0.1.0` for bare (SDKs require at least 0.1.0). Bare has no envd or Envd/Traffic tokens. Both profiles return a separate forwardAccessToken signed at creation and persisted with the Sandbox.

Create preserves the existing response body without adding state; response/cache/background worker use distinct copies. Get can show starting or cleanup-pending deleting. Default List selects running/pausing/paused; explicit starting/deleting/dead supports diagnosis. Running Connect is idempotent. Starting Connect applies only explicit timeout updates and starts no duplicate attempt. Paused Connect durably accepts resume before returning, so success may correspond to starting but never the old paused state.

Every successful standalone Connect, including Running, Starting and imported targets, waits for the current Proxy registrations to apply the preceding route updates and acknowledge an ordered barrier. It does not wait for runtime or envd readiness; the first data request can park on Starting. The existing park timeout, caller cancellation and node shutdown bound this wait. A route-barrier failure returns 503 without undoing timeout updates or resume acceptance that already committed; accepted launch work continues independently. This response fence does not change the cluster CmdConnect acceptance/ownership protocol.

An explicit deadline intent on paused/starting survives a failed resume back to paused within one conductor process, and is consumed only after exact-run commit to running. V1 has no persistent intent discriminator. Reconcile can infer an interrupted starting resume and conservatively restore intent in that process, preventing a following ordinary Wake/Connect from overwriting it with node defaults. If it had already returned to paused before another conductor restart, the database cannot distinguish that intent from ordinary paused/default-rearm state. Exact preservation across multiple restarts needs the separately approved schema work in #139; #135 does not hide it in an implicit encoding or sidecar.

The complete Exec session request, CEL conditions, credentials and failure semantics are defined in [access and proxy](node-proxy.md#exec-session-api).

<a id="create-identity"></a>

## Create identity

The internal Build MMDS route cache uses a disjoint namespace, so legal caller IDs beginning with `build-` remain supported.

A direct conductor `POST /sandboxes` can select its node-local SandboxID and,
optionally, an independent StableID. This is a creation-time configuration input,
not an identity reservation API or idempotent result replay.

**Two equivalent inputs**

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

**Field contract**

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
NUL and longer values are rejected rather than normalized. Migration-token
encoding retains its historical StableID contract; node Import admission
independently enforces the same 1..57-byte format ([Credentials and ownership: encrypted APISecret/ManifestKey pairs](node-proxy.md#credential-model)).

The object accepts exactly `id` and `stable_id`. Empty Header values, non-object
JSON, null (including null field values), duplicate Headers, duplicate or unknown
JSON fields, trailing JSON, invalid ID strings and non-string fields return 400.

The Create response's `sandboxID` remains the **local** ID. Node lifecycle URLs,
Proxy host/header addressing, local paths and route-table keys use that ID.
StableID is not a lookup alias or a separate endpoint. Merely selecting StableID
without `id` does not provide local-ID deduplication or a predictable local URL.
There is no required string relationship between the two IDs; `-g0` is not an
input format imposed by conductor.

**Scope and ownership**

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

**Conflict, retry and deletion**

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

**Shared implementation**

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

See [Cluster Router](cluster.md#client-access) and
[Extensions](extensions.md) for the surrounding contracts.

<a id="stable-identity"></a>

## Stable and node-local identity

- **Stable and local identity:** Sandbox.ID is the node-local store/runtime/route lookup key. StableID() preserves sandbox identity across local-ID changes. Ordinary standalone Create leaves StableIDValue empty, so the helper falls back to Sandbox.ID. Standalone import with another target ID changes only Sandbox.ID and preserves source StableID/service credentials. Identity-preserving migration/copy may produce several local rows sharing StableID; stable_id is neither UNIQUE nor a reverse-lookup index. In a cluster, local ID is NodeSandboxID and StableID is Registry's public SandboxID. Same-node resume retains both; cross-node migration/replacement changes only NodeSandboxID. Forward/Exec KAT sid always binds StableID; a copy is not thereby an independently authorized fork. Publication locations are also keyed by StableID ([Publication, templates and migration](#publication-and-migration)): a re-export after an import changed the target ID lands in the same directory, and every cluster generation shares one directory. Import admission validates a token-carried StableID as an opaque id (types.ValidLocalSandboxID), rejecting malformed identities at import instead of failing export-time Resolve.
- SQLite directly uses stable_id without probing/migrating legacy identity columns or dual reads/writes. Incompatible old preview databases must be explicitly rebuilt according to the schema boundary; this is not a claim that no Stable Release exists.

<a id="create-acceptance"></a>

## Create acceptance and asynchronous launch

Synchronous Create performs request validation and pure parsing, selects the requested or generated identity and materializes tokens, claims in-process launch ownership, then inserts `starting, run_id=""` with empty network fields and caches/publishes starting. It sends `route_barrier` on that same ordered routesync stream, waits for the current Proxy master to apply the preceding Upsert and ACK, and revalidates the registration lease before scheduling launch and returning HTTP 201. This means durable acceptance plus enough applied Proxy identity data to authenticate and park traffic. It does not mean a runner is assigned, runtime/backend is ready, or e2b `/init` completed. No Proxy, disconnection, apply failure or timeout returns 503. Before starting resources, cleanup removes this object's RunDir while retaining BaseDir and any formed data, then exact-owner CAS commits dead plus automatic responsibility and publishes a non-routable dead management Upsert.

The cold-image background path retains its single-stage fast path: prepare resources/network/YAML, then allocate a RunID from the runner pool. Artifact launch first creates directories and binds `ready.sock`; the pool commit callback binds an exact RunID with `starting AND run_id=''` CAS. The assigned parent immediately connects readiness and obtains bootstrap using its already-locked RunID pidfile. After authentication, ManifestKey overrides its environment. Based on E/S kind and LaunchMode, the task opens the root, selects PreparedSource and submits a non-secret capacity/network/ref-closure summary. The sole launch worker then resolves resources/network, attaches networking, persists ownership with `starting AND run_id=<exact>` CAS, writes root-credential-free YAML and returns the final LaunchSpec. The resident runner appends its retained ref-locations and starts `sandbox-ctl run` as a direct child. While the parent waits for that child, conductor independently consumes the strict runtime readiness wire, performs e2b `POST /init` for environment/default user and commits `running` with an exact-RunID CAS before opening traffic. Only when the child later exits does the parent's single Wait return and its bounded result report begin.

<a id="capture-and-pause"></a>

## Workspace storage

A workspace gives one named data disk one or more paths inside the guest. An application can write to these paths, pause sandbox A and pass the disk to sandbox B. B can use different guest paths. Each consumer gets its own writable layer.

Declare workspaces in the existing `POST /sandboxes` or `POST /v3/templates` request. Send a JSON array in `X-Kuasar-Sandbox-Workspace`, or encode the same array as the string value of `metadata["kuasar-sandbox.workspace"]`. The header takes precedence. For example, send this array as a single-line header to reserve a device before assigning work:

```json
[{"name":"project","mounts":[{"path":"/data","subpath":"src"},{"path":"/work/results","subpath":"results"}],"slot":{}}]
```

Each `path` is an absolute guest directory. Each `subpath` is relative to the disk root. Omitting `subpath` mounts the root. Choose non-overlapping guest paths across disks and non-overlapping subpaths within each disk. A sandbox supports eight extra disks in total. Public sandbox and template metadata retain the names and mounts.

| Create choice | Result |
|---|---|
| `"slot":{}` | Reserve an unattached device and its guest paths. The disk supplied on attach determines its size. `"slot":{"size":"8GiB"}` requires exactly that capacity. |
| `"disk":{}` | Initialize a private disk from the node's configured default. |
| `"disk":{"migrationToken":"…"}` | Initialize a private writer over the disk exported by another sandbox with the same tenant credentials. |
| `"disk":{…DiskV1…}` | Use a complete authorized disk with `kind:"disk"` and `version:1`. Include `base` and any `base_from_refs` in newest-to-oldest order, or use an authorized `diff_template`. The optional `size` asserts capacity in bytes. |
| Neither field | On an image, reserve the device. On a sandbox or snapshot template, inherit the captured device and its data. |

On an SBX or SNP template, select an existing empty device by name, for example `[{"name":"project","disk":{}}]`. Omit `mounts` to inherit its topology. The conductor waits for the bind acknowledgement before reporting `running`. Use a captured attached device as it stands. For a new template, automatic target selection yields SBX when it carries workspaces and no start/ready command, or SNP when it has such a command.

| Operation | API | node-ctl |
|---|---|---|
| Inspect current devices | `GET /sandboxes/{id}/workspaces` | `node-ctl workspace list B --json` |
| Attach to an empty device in a running sandbox | `POST /sandboxes/{id}/workspaces/attach?name=project` with `{"disk":{}}`, a complete disk in `disk` or `{"disk":{"migrationToken":"…"}}` | `node-ctl workspace attach B --name project`; use `--disk-spec FILE` or `--token-file FILE` to select another source |
| Export an attached device from a paused sandbox | `POST /sandboxes/{id}/workspaces/export?name=project` with `{}` returns `{"token":"…"}` | `node-ctl workspace export A --name project`; `--json` returns the token envelope |

Use the owner's `E2B_API_KEY`. Once A has synced application writes and paused, B can receive the disk through stdin:

```bash
set -o pipefail
node-ctl workspace export "$A" --name project |
  node-ctl workspace attach "$B" --name project --token-file -
```

Export captures the complete disk, including unmounted directories. A stays paused and can resume or be deleted after export. For memory pauses, the selected content comes from the paired disk artifact. B's guest paths become available after the attach acknowledgement (204). The guest mounts the filesystem on first access. An unattached path rejects access.

The list reports `unattached`, `attaching` or `attached`, and `sizeBytes` when known. While running it also returns `runGeneration`. Pin a retry to this value with `--if-generation G` or `X-Kuasar-Run-Generation: G`. The API returns 412 if the generation changed, 409 for a conflicting disk or sandbox state, and 503 when it cannot confirm the owner's state.

## Capture and Pause

Create autoPauseMemory has tri-state parsing:

```text
omitted/null -> true
true         -> true
false        -> false
```

The independent durable auto_pause_memory field selects TTL CaptureKind only; it does not enter checkpoint namespace or snapshot.cfg. Standalone and cluster share a typed chain: public Router → route-link reserve → Registry reservation → node-link command → node CreateReq → Sandbox row, without metadata tunneling.

Create `idleTimeout` is a nullable instance policy, in integer seconds. Omission
or `null` disables it; `0` schedules immediately after a trusted idle observation;
a positive value waits from Conductor receipt. Negative, fractional, or values
above 9223372036 seconds return 400. For example:

```json
{"templateID":"<template-id>","idleTimeout":5}
```

Get/List and extension views expose the effective nullable value. Store, cluster
reservation/create, Resume, Reset and migration preserve it, including zero.
There is no runtime policy-update API. Each new RunID starts fresh observation,
including a newly running Sandbox that has never received a business request.

The automatic path requires complete, normal single-Proxy observation. It records
receipt t0 with BOOTTIME, waits the policy duration, then uses the common Pause
path with `memory=true`, `checkpoint=false`, `ifIdle=true`, `ifIdleSince=t0`.
At most one automatic Pause attempt per SID may wait for lifecycle/source access;
new observations replace the candidate without adding waiters, and are evaluated
after the old attempt retires. Duplicate idle notifications do not extend the
deadline; new idle, source changes,
missing statistics and restarts rebuild it. The timer holds no lifecycle lock.
While waiting it reads the immutable lifecycle cache; a due attempt rereads
authoritative policy and state from the Store under the SID lock.
The final synchronous Worker check catches short requests hidden by delayed
statistics. Rejected attempts can rearm without another business request; repeated
failures use bounded internal backoff. An unresolved durable pausing state remains
owned by the existing lifecycle recovery.

A later request uses existing park/Wake. Requests arriving after the Worker sample
may wait through capture and Resume, subject to the original `park_timeout`;
delivered business is never replayed. Streams, WebSockets and Native Exec remain
busy until final relay closure. Guest background work and traffic bypassing Proxy
are outside this definition and can be frozen with the VM.

`idleTimeout` does not refresh `DeadlineUnix`, change TTL's `autoPauseMemory`, create
a retained checkpoint, or acquire resource recovery responsibility. A running
Sandbox with `ResourceObligation` is deferred until that responsibility converges,
then receives a fresh idle interval. TTL, resource pressure and explicit Pause
retain their own behavior. Upgrade Conductor, Proxy and cluster peers together
(routesync v11) before enabling this policy. Migration tokens with non-null
`idleTimeout` require a receiver that supports the field; omitted/null policy
keeps the existing token shape.

Explicit Pause body may be empty or:

```json
{"memory":false}
```

Omitted/null/true memory selects CaptureSnapshot; false selects CaptureSandbox. Explicit Pause always defaults to S independently of autoPauseMemory. Thus Create(autoPauseMemory=false) plus Pause({}) captures S, while TTL captures E. SnapshotPolicy alone owns checkpoint_merge_ref/drop_caches. With memory=false, presence of either field, even with a null value, returns 400 before freeze/capture/lifecycle side effects. Explicit merge_ref/drop_caches fields in the Pause Header have the same restriction. Header/metadata checkpoint input still permits only merge_ref/drop_caches, never memory.

`ifIdleSince` optionally adds a continuous-idle threshold to `ifIdle=true`.
Use an RFC 3339 timestamp with a timezone (subsecond precision is retained).
Omitted/null keeps the existing behavior. A missing `ifIdle=true`, malformed
value, or timestamp later than receipt at the final Conductor returns 400 before
installing the gate. This condition is evaluated only for running instances;
starting/paused, including resource-paused, returns 409 without adoption.

The gate's synchronous Worker samples must prove zero parking and connected,
and `effectiveIdleSince <= ifIdleSince`. Busy or a later known idle baseline
returns 409; missing samples, empty participants, failed ACKs or indeterminate
historical clock evidence return 503. Rejection withdraws the temporary gate
without capture or durable pausing. Extension hooks cannot change this condition.
The cluster Router forwards the timestamp unchanged to the executing node.

`checkpoint` and `ifIdle` default to false. Every Pause first gates new traffic at the online
Proxy registrations and synchronously samples parking/connected counts from every worker.
IfIdle requires all reported counts to be zero (409 otherwise); ordinary Pause permits busy
traffic. No Proxy inventory is persisted. Registrations disconnected before the handshake
are ignored; disconnect/replacement/join during it or an incomplete ACK rejects Pause with
503 and republishes the authoritative state. The idle guarantee covers only these participants;
an empty online set does not wait for disconnected proxies.

The publishable `pausing` route also appears in full synchronization during preparation.
Proxy parks authenticated requests and sends Wake for both pausing and paused. Existing
transports remain valid during the handshake so canceling them cannot fabricate idle.
Conductor holds the SID lifecycle lock through capture, atomic pause/checkpoint commit and
cleanup before processing queued Wake. pausing→paused is normal progress, whereas
starting→paused remains a failed Resume.

Runner exit and Pause completion are independent facts. An exit report for the
current RunID is persisted and acknowledged immediately, including during
pausing or while paused cleanup still owns that RunID. It does not turn an
admitted Pause route into dead or veto a successful capture/paused commit.
The paused row retains the exit diagnostic; the finalizer rechecks state and
RunID under the SID lock and only finishes paused cleanup. Resume retires the
old result in both storage and live views, and a valid paused source remains
exportable. Reports from a retired RunID cannot change a subsequent run.
If capture succeeds but the paused transaction fails, the uncommitted operation
remains pausing for existing recovery; it must not be advertised as running.
Restart never adopts an interrupted durable pausing
row merely because its runner is live: it fences that exact runner and uses existing terminal
cleanup because no new source was committed.

`checkpoint=true` atomically retains the immutable E or S/E pair with paused/source and returns
HTTP 200 `{"checkpointID":"<UUID-v7>"}`. Ordinary Pause still returns 204. Sandbox option
`max_checkpoints` is currently fixed at 1; Create accepts omission or 1. There is no node retention
setting. A retained capture, including one being exported, rejects the next checkpoint Pause
with 409 before handshake/capture. Ordinary Pause neither occupies this quota nor replaces it.

Resume releases only obsolete versions not protected by the retained ResumeSource or a user checkpoint; a new no-ID Export after Resume
returns 400. A retained checkpoint survives Resume. Export accepts `checkpointID` in
`POST /sandboxes/{id}/export`; CLI uses `export-sandbox <sid> --checkpoint <id>`.
This selects the immutable capture independently of current runtime state. Concurrent Resume
does not preempt it. Successful export consumes the checkpoint, failure retains it, and
`keepSource=true` is invalid with an ID. Export leaves the sandbox intact. Alternatively,
`DELETE /sandboxes/{id}/checkpoints/{checkpointID}` consumes it directly (204; missing ID 404).
Consumption releases quota; files still required by the current ResumeSource remain, while
existing sandbox cleanup collects obsolete files and DIFF versions.

Checkpoints always belong to their sandbox and disappear with parent deletion.
Termination retains current disks, dependencies, the original ResumeSource and user checkpoints ([Termination, Reset and retained data](#reset-and-retention)).
The existing checkpoint schema still rejects databases missing `max_checkpoints`;
the Reset metadata upgrade is additive and defaults existing objects to autoReset=false ([SQLite state](node.md#sqlite-state)).

checkpoint.mode accepts local/bundle for both CaptureKinds:

```text
CaptureSnapshot:
  sandbox-ctl snapshot --json --path-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root> --checkpoint <UUID-v7> [--merge-ref=...] [--drop-caches=...]
  -> ResumeSource{kind:snapshot, ref:<actual S basename + identity>, sandboxRef:<actual E basename + identity>}

CaptureSandbox:
  sandbox-ctl export --json --path-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root> --checkpoint <UUID-v7>
  -> ResumeSource{kind:sandbox, ref:<actual E basename + identity>, sandboxRef:""}
```

Capture returns the producer's actual identities: `snapshot --json` returns
`{snapshotRef,sandboxRef,removedRefs}` and `export --json` returns
`{sandboxRef,removedRefs}`. Capture removals are `[]`. A Snapshot Bundle carries
the S and E selectors in the same physical file. Local refs retain their identity
qualifiers but expose only basenames; the checkpoint directory is execution
context. Default human output and upload-key stdout are unchanged. The adapter
validates the complete structured result before committing pausing→paused;
Snapshot stores the actual S/E pair, while E-only capture clears the old Snapshot
and its association. Capture failure never commits a partial pair.

Ordering is resolve request/quota → accept operation → online Proxy handshake → running→pausing → capture runtime → CommitPaused(id, exact RunID, source, checkpointID) → stop/reset exact runner → detach exact network → selectively clean checkpoint → remove RunDir → publish paused. State/source commit is atomic; remaining RunID/port/RunDir means cleanup pending. Successful stop/detach clears its field by exact CAS. Checkpoint or RunDir cleanup failure blocks new Resume/Wake/Exec ownership and is retried at admission or startup. BaseDir/checkpoint remains. Capture failure restores running only while the same RunID still owns the pausing row and no exit result has been accepted; an accepted exit follows existing terminal cleanup. Failed attempts retain the old source and create no success alias, never downgrading S to E.

<a id="local-disk-checkpoints"></a>

### Local disk Checkpoints

Every actual managed business sandbox start, including cold/memory restore, restart and a task assigned
to an idle runner, passes `--enable-checkpoint`. Writable DIFF files remain under
`<BaseRoot>/sandboxes/<SandboxID>/checkpoint/diffs`; RunDir cleanup does not remove
them. The existing BaseRoot and PathID determine this directory, without a separate
tenant option. Build stages use single-layer writable disks without this flag ([Build execution](node-build.md#execution-and-publication)).

Each actual named capture generates a fresh UUID v7 shared by the root and all
writable data disks. UUID generation failure stops before starting the capture
command. Repeated requests that do not capture again create no new version.
`checkpoint.mode` still selects local or Bundle output, and `memory`, `merge_ref`
and `drop_caches` retain their own meanings. E records fixed disk versions in
`checkpoint.json`; the ordinary disk payload is not copied during this capture.

Old running instances are not converted in place. Only sandbox-ctl's explicit
unsupported result (exit 78) permits one retry without `--checkpoint`, using fresh
arguments and output buffers and validating that retry's complete report. I/O,
space, corruption and incomplete-report failures never trigger this fallback.
New starts must support the enabled mode. Deploy matching sandboxer source and
binaries containing [sandboxer #336](https://github.com/kuasar-sandbox/sandboxer/pull/336)
(`7cd3ed4c5a8807b75b175bdb845867effb1c5018` or a successor); the selected Sandboxer SDK
module must also match. Standalone builds use the version committed in `go.mod`; coordinated builds select local modules through
an explicit Go workspace ([Build and test](../CONTRIBUTING.md#build-and-test)).

Task-local Open, Clone and Validate preserve the Checkpoint layout. Its root and
data disks already contain active data even when the ordinary active-base field
is empty, so preparation does not inject a blank disk. Ordinary direct EROFS inputs
still need their writable upper. Resume passes the committed source unchanged:
sandboxer may reopen the original DIFF when directory, disk identity, capacity,
base and latest active version match. Older versions, changed active contents or
another directory require materialization. Source artifacts resolve relative to
their actual carrier, independently of the target RunDir/BaseDir.

This removes historical disk-payload copying from named local capture; it does
not make the complete Pause or Resume constant-time. Drain, memory capture and
business cleanup still contribute. DIFF reopening performs required initial
checks and queries presence on first access, so evaluate guest readiness and the
first working set together with Open time. Retained versions can increase space
and shared-write cost; ordinary active-DIFF export still traverses its index.
Storage-layer measurements alone are not platform pause/resume measurements.

For platform validation, the public `orchestrator.checkpoint.sh` case requires
named versions, same-directory DIFF reuse, correct guest data across memory/cold
resume and portable publication, export-time retention, and actual allocated-byte
reclamation after release. It allows bounded background-GC grace; `List=[]` alone
does not satisfy reclamation. Its Build stages must use single-layer disks, managed business
launches must enable Checkpoint, and the final template capture remains ordinary.

For an optional comparison, prepare independent baseline-copy and candidate
workspaces through the central E2E preparation process, both including the current
`orchestrator.checkpoint.sh` dependencies/helpers. Select the baseline `node-ctl`
from its own source and the candidate from its source; keep sandboxer, other
products, execute image, architecture and host resources identical. From the
orchestrator repository, run sequentially with fresh output directories:

```sh
sudo python3 test/perf/run_checkpoint_platform.py --workspace /path/to/baseline-prepared \
  --mode baseline-copy --sizes 0,1 --out /path/to/baseline-observations
sudo python3 test/perf/run_checkpoint_platform.py --workspace /path/to/candidate-prepared \
  --mode checkpoint --sizes 0,1 --out /path/to/candidate-observations
```

This separate measurement driver calls the prepared public `workspace.verify`,
capability probe, `load_images` and `case_environment`, and verifies inputs again
after execution. It changes no prepared product and has no host-product fallback;
its result is not an E2E acceptance result. It records full prepared provenance,
product SHA-256 values, image identity, actual argv and capture layouts. The
baseline must omit Checkpoint startup/capture flags and metadata, while the
candidate must use the new mode and distinct UUID v7 identities. Both modes publish
through the same public UDS export API with a 1,200-second client budget; this
avoids the older CLI's fixed 30-second timeout without changing its server.

Both runs use the same Build request and filesystem capacity (the largest selected
payload plus 2 GiB). The default samples are 0/1 GiB dense data and a 1 GiB sparse
file with 64 MiB mapped payload; zero still includes guest/filesystem writes.
`--sizes 10` explicitly selects a larger run after memory and free-space checks.
The serial test configures the controller's startup factor to 0.8 so its 6 GiB
Build fits developer hosts; production defaults and physical-memory detection
are unchanged. Startup metadata and encryption can change template identity.

Build preparation is timed separately from Create, Pause, hot/cold resume,
publication and Create from a published template. Each restore verifies data,
including after deleting the source. Full SHA-256 verification separately records
cache preparation, hashing and host elapsed time, with a size-based bounded
budget. After hashing, a separately timed guest
sync/cache-drop fixes the file-cache condition before capture; normal Pause
policy remains unchanged. Direct reads separately measure the first payload
page and a 256-page working set after readiness. A second cold Pause has no
explicit intervening payload write, although guest housekeeping can still write.
These timings do not isolate internal Drain, index registration or DIFF Open.

A fully initialized 128 MiB workload compares a real publisher retaining a fixed
version with the same guest after publication, version release and observed
physical reclamation. It performs sequential/random 4 KiB writes, 2 MiB hot-set
writes, sector-aligned 512-byte partial-page writes and mixed reads/writes with
`O_DIRECT|O_DSYNC`, QD1. Retained and released stages use different 64 MiB halves;
the retained half alternates between rounds. Each half contains five disjoint
12 MiB regions. Thus the released stage first overwrites previously untouched
payload pages, rather than repeating writes to pages already privatized while
retained. Recorded offsets distinguish this `fresh-v3` protocol from earlier
same-address samples, which must not be pooled with it.

Unsupported direct I/O fails explicitly. The default 1,024 operations per pattern
retain raw syscall latency, read/write p50/p95/p99 and throughput. Throughput also
includes deterministic buffer generation. Both states use the same relative
address seed and different content to force writes. Retention always runs first;
cache, index, allocator, ordering and background-GC effects remain, so this is not
an isolated causal measurement of Delete. Use `--rounds 3` for repeated samples;
`--skip-lifecycle` or `--skip-workloads` selects one group.

`checkpoint-platform.jsonl` reports BaseDir/RunDir and DIFF allocation separately.
Workload observations sample runtime-unit RSS/cgroup memory and `/proc/PID/io`/
cgroup `io.stat` every 250 ms, retaining counter resets and before/after deltas.
The sampled memory maximum is not a process peak. Shared caches, Store output
and the node daemon are outside these scopes. Short workload I/O deltas are not
a physical-media write-amplification ratio. Report source revisions, filesystem,
cache conditions, raw samples and these limits with any comparison.

<a id="managed-checkpoint-history-and-selective-cleanup"></a>

### Managed checkpoint history and selective cleanup

`merge_ref=false` records the current resident working set separately. Starting with
`S1 -> S0`, the next capture streams a new immutable historical Snapshot
`S1′ = S1(memory) over S0(memory)` and writes `S2(current working set) -> S1′`.
Further local captures repeat this composition: at most one owned local memory
lower remains beneath the newest S. `merge_ref=true` absorbs the current resident
memory and the entire eligible local prefix; switching false → true → false has
the same bound. Restore still accepts old multi-layer inputs. Only the newest S
supplies execution state and `sandbox_ref`; historical S contributes memory only.

The prefix belongs to the current sandbox's canonical `BaseDir/checkpoint`.
Selection uses complete source bindings and physical Bundle membership before
comparing paths. A named location or external template is a boundary even if its
files are on this host, including a mapping to the same pathname. The boundary
and remaining lower refs preserve their order. Local tarstream, Bundle, and mixed
carriers follow the same rules. Ordinary copied writable disk captures absorb their eligible
same-device local prefix, independently of the memory flag; immutable EROFS bases
remain separate from ext4 uppers. Data and opaque Zero override lower layers;
Hole falls through. Historical reads use host artifact streams, never the guest
memfd, and therefore do not enlarge the captured working set.

History is composed before guest freeze and streamed into the final sink. It
uses `fetch.NewLayered` and sparse runs without a full-image buffer or intermediate
image copy. Local output reuses dependencies already owned by that checkpoint through their
physical selectors. Bundle output keeps its existing member-copy publication path. New history has a new content
identity; the old source remains valid through sink commit/close and database
commit. Failed capture or database commit cannot authorize old-file deletion.

After a managed Pause commits its exact S/E pair (or E-only root), the lifecycle
owner fences the old runner and all readers/writers, then invokes selective
cleanup. Generic FileSink output, arbitrary `--output`, independent
`snapshot --resume`, and shared build inputs confer no cleanup rights. The
sandboxer artifact library interprets the keep set; conductor supplies ownership,
paths, the committed pair, and the existing lifecycle fences through the
short-lived `node-ctl checkpoint-cleanup` tool. Portable Export continues to use
its stored pair without reading artifacts. No token, public result, base format,
or persistent cleanup schema changes.

The keep set unions current and explicitly retained S/E roots, their memory-history carriers, and disk,
upper and immutable-base carriers, including reused files. A historical S's old
E is not a disk dependency. Any needed Bundle member retains its whole physical
carrier. Source/location binding precedes basename comparison. Selection reads
only bounded current metadata and Bundle indexes, not old candidate payloads or
full-image digests; Snapshot CPU/state bodies are not needed by this operation.
Any keep-plan or reader-close error prevents all deletions. Kernel/runtime basename
identities bind host-supplied boot files and are not checkpoint payload edges.

Only direct entries in the verified exclusive checkpoint directory are eligible:
64 lowercase hexadecimal digest/key names with `.snapshot`, `.sandbox`,
`.overlay`, `.image`, or `.bundle` must be regular files; capture partials must
exactly match `<producer-SandboxID>.<kind>.<uint32-decimal>.partial` (including
`bundle`, no leading zeros except `0`); fixed `<sid>.snapshot`/`<sid>.sandbox`
aliases and `.<sid>.<role>.<32-lowercase-hex>.tmp` must be symlinks with the
producer's basename target convention. SandboxID is not PathID or StableID.
Unknown names, other SIDs, prefix collisions, malformed lookalikes, directories,
and unexpected symlinks remain untouched. Incomplete partial contents need no
validation. A fixed alias is current only when it names the corresponding committed
S or E carrier. An E-only root retires its old S alias even when E retains the
same Bundle. Snapshot capture commits the S alias only; its E identity comes from
the stored pair, without requiring an E alias. Cleanup unlinks recognized aliases themselves and never follows
symlinks or recursively removes the directory; directory-fd operations keep
unlink confined if paths race.

Managed checkpoint cleanup requires a canonical physical `paths.base_root`:
its checkpoint path and all ancestors must contain no symbolic links. Configure
the resolved directory itself rather than a symlink alias. This is an existing
cleanup precondition; a successful Pause does not imply path validation happened
at node startup. Published artifact retention and deletion remain the caller's
responsibility, separate from owned checkpoint cleanup.

A committed Pause remains successful if cleanup fails. The last durable RunDir
ownership marker remains until checkpoint cleanup and ordinary paused cleanup
succeed. The existing worker retries with a freshly loaded current source under
the SID lifecycle lock, including after restart. Active or detached exports keep
their read fence; cleanup never waits for an export while holding the lock it
needs. Resume/Wake/Exec obey the existing pending-cleanup admission contract.
Missing candidates count as finished; permission, I/O, and identity errors retain
retry ownership. Ordinary Kill and terminal BuildBaseDir deletion retain their
existing finalizers. Shared/external artifacts are never reclaimed by this step.

<a id="releasing-disk-versions-after-resume"></a>

### Releasing disk versions after Resume

Running disk-version release is separate from the paused artifact-file cleanup
above. Successful running admission, actual export completion and startup adoption
request a shared release check. It reloads current state and exact runtime/path
ownership, and only a running sandbox without a terminal execution result or a
source operation still using its checkpoint may release versions. Starting,
failed resume rolled back to paused, and an unresumed `keepSource` retain them.

The check uses sandboxer's complete List result and shared SDK CheckpointVersions graph validation,
excludes the complete ResumeSource version set and retained checkpoint IDs, and deletes
the remaining exact `{DiffID, ID}` references. Partial or uncertain Delete retries keep the same set;
a different runtime cannot inherit that selection. If List explicitly reports
`checkpoint_unsupported`, the current check ends successfully without Delete or
retry. This does not cache capability by SID: an independent later or concurrent
trigger still starts another check. Other List errors, incomplete lists and all
Delete errors are logged and retried
with bounded calls and backoff, without rolling back a successful launch or
publication. This does not run `checkpoint-cleanup`, remove the DIFF directory or
delete memory dependencies. Delete makes old versions reclaimable; physical space
returns through sandboxer's subsequent GC rather than synchronously. A retained
`ResumeSource` and all versions it needs remain available for failed Resume rollback
and terminal retention. A graph validation error prevents version deletion.

<a id="resume"></a>

## Resume admission, Connect and Wake

Connect accepts a bounded strict JSON object with timeout and optional bool/null memory. The upstream Connect reference checked on 2026-09-07 also exposes memory; Kuasar's exact source-dependent rules below define local behavior rather than claiming complete upstream policy equivalence. Omitted/null maps to ResumeAuto, true to ResumeMemory, false to ResumeCold:

| Paused source | Request | LaunchMode | sandbox-ctl |
|---|---|---|---|
| S | Omitted/null/auto | memory | run --restore S |
| S | true/memory | memory | run --restore S |
| S | false/cold | cold | Resolve S's sandbox_ref, then run --from E |
| E | Omitted/null/auto | cold | run --from E |
| E | false/cold | cold | run --from E |
| E | true/memory | Conflict | 409 memory unavailable |

Running Connect remains idempotent; false does not restart it. A starting resume accepts omitted/null by joining the current attempt. An explicit matching mode joins; mismatch returns 409 without changing durable choice. Router/route-link/Registry/node-link retain pointer presence.

BeginResume(id, deadline, LaunchMode, RunDir, EnvdUDS, CiUDS) requires fully cleaned runner/network/RunDir ownership and atomically writes starting, mode and new canonical RunDir/UDS. CommitStartingRunning/RollbackStartingPaused/RollbackStartingDead clear mode. For an existing local S, a failed cold attempt returns to the original paused S, allowing later memory selection. Conductor restart reconstructs an accepted starting resume from source plus durable mode, so lost cache cannot turn S+cold into memory restore.

Cold launch from E uses the target node's configured kernel and runtime, preserves the source workload/disk graph and derives a new C0 with those boot identities. Selecting cold from an existing S uses its paired E without rewriting S or requiring an E-only export. Memory restore retains the original C0, needs a runtime with the required digest type and value, and does not require an external kernel file.

If Connect must import a missing target, its memory selector also chooses the imported durable source: false stores E-only before cold launch. A later launch failure leaves paused E. For subsequent paused resumes, auto remains cold and true returns 409 until a new S is captured. This differs from one cold attempt on an existing local paused S. Import never rewrites the original token or deletes its S/E artifacts; see [Import selection](#import-selection).

Ordinary Proxy ingress and native exec activation call OnWake with ResumeTriggerWake; ExecSession uses ResumeTriggerExecSession. Public Route/Exec trigger constants remain extension contracts, although conductor no longer owns those data adapters. All these entries request ResumeAuto: S→memory, E→cold. SHM carries neither source kind nor cold gate. An authenticated first E request stays parked until cold launch, running route and successful backend dial. A future autoResume=false must be separate traffic policy, not inferred from E/S kind; complete E2B autoResume is not implemented.

<a id="reset-and-retention"></a>

## Termination, Reset and retained data

Create accepts `autoReset: boolean` (default false). It is immutable parent state, preserved across
subsequent runs and projected by Get/List; pre-upgrade objects also default to false. `deadAt` is
optional and marks the current first durable terminal commit. `executionResult` retains the original
known result; unknown exits remain absent, never synthetic exit code zero. `autoResetError` records
only automatic preparation failure, separately from that result. `endAt` remains the run deadline.

Normal/abnormal termination, fresh Create failure and confirmed missing runs fence the exact runner,
release network/reservations and unneeded RunDir, retain the parent/current disks/dependencies/original
ResumeSource/user checkpoints, then commit dead. Creating E is not a prerequisite for terminal cleanup.
Normal Pause owns its exit. Failed Resume returns to its original paused source. Accepted Delete/Kill
continues deletion without autoReset; delayed old-run events cannot clean or overwrite a new run.

Before guest access, shared sandboxer startup persists portable C0, actual complete disk graph and
source binding, root/named disk-to-active-DIFF identities and dependencies in BaseDir/checkpoint,
bound to parent instance and RunID. Every run refreshes that binding, including DIFF reuse. The SDK
retains local materialized lowers; callers that did not opt into retention keep temporary cleanup.

`POST /sandboxes/{id}/reset` uses existing authentication/ownership. It returns 204 only after E-only
paused is committed; repeat E-only paused returns 204. Memory-bearing paused, starting, running,
pausing and deleting return 409; foreign/missing parents return 404. Unavailable/incomplete current
disks fail with 422 and retain dead. Reset never substitutes an empty disk, template, old checkpoint
or previous run description, and never waits for an exited runtime to initialize storage. Request
cancellation stops waiting, not an accepted Reset operation. Retries join the same durable operation
ID until success or a definitive failure has committed.

The parent persists one Reset operation ID per automatic terminal opportunity in the same dead
transaction. The worker uses the shared SDK through `sandbox-ctl reset --checkpoint-dir ...
--sandbox-id ... --instance-id ... --run-id ... --operation-id ... --json`. Under exclusive source
ownership it validates every disk's identity/capacity/readiness/dependencies, fixes all versions with
one operation identity, validates/replays partial output and creates standard Sandbox E. Only an
exact parent/terminal/operation commit publishes E-only ResumeSource and paused. Outputs always
belong to the parent; Reset neither consumes a user checkpoint nor occupies its long-term quota.
The configured sandbox-ctl must come from the declared sandboxer dependency version.

Manual requests join pending automatic work. Definitive failure durably marks failed and keeps original
deadAt/result/data; automatic operations also record autoResetError. Duplicates, scans and restart never automatically retry
it. A later manual request may create a new operation. Unknown interruption resumes the same operation,
including after partial multi-disk capture or E creation. Recovery recognizes an already committed
paused result. Success clears deadAt/current result/automatic error. Only a new run ending grants a new
automatic opportunity. Reset launches no VM; subsequent Connect/Wake cold-starts the same SandboxID
with a new RunID. Explicit memory restore from E-only fails; failed cold Resume retains E-only paused.

Reset columns are an additive SQLite upgrade: immutable instance_id, auto_reset=false, dead_run_id,
dead_token and reset_id/state/automatic/error plus auto_reset_error. Existing retained dead rows get
an identity token but no retroactive automatic operation. Missing current disks fail clearly; their
parents can still be deleted or expire. In running state dead_run_id may first record a confirmed end
before cleanup without fabricating executionResult. The same transaction/CAS boundaries and inherited
source lock protect Reset, checkpoint export, deletion and TTL across conductor process restart.

This retains the existing guest FLUSH/capture durability contract. It adds no cross-node storage
recovery, automatic VM restart loop, paused TTL, run archive or remote artifact ownership.

<a id="publication-and-migration"></a>

## Publication, templates and migration

Both local E/S use:

```text
sandbox-ctl publish --json --quiet --manifest-config <cfg> [--to-ref-location <name>=<uri>] <artifact>
```

The publication adapter consumes and validates the JSON report while preserving ResumeSource kind. Without a named parent, it publishes to Manifest Store. With checkpoint.remote.ref_location_parent, name is the entity id — the sandbox StableID for E/S publication, the BuildID for build publication (see reflocation.PublicationName) — and URI is `<parent>/<sha256(name)[0:2]>/<sha256(name)[2:4]>/<name>`. Tarstream results are located sandbox/snapshot refs using digest or hmac identity; Bundle results use manifest root selectors. Manifest config is supplied even for named publication because exact Bundle publication validates its Manifest graph. Bundle→Store verifies recorded admission, physical digest and salt domain and commits root last.

For a local disk Checkpoint source, sandboxer materializes the fixed versions into
ordinary portable disk artifacts and removes `checkpoint.json` from the published
E. The original local source is unchanged. Publish before moving the result to
another node or rolling back to a reader that does not understand Checkpoint E;
copying its metadata alone does not transfer the local DIFF contents.

The name itself is the directory key: every publication of one logical entity — retries, process restarts, re-exports after an import changed the target id, cluster generations — converges on one directory where content-addressed `<digest>.<role>` files accumulate as versions and same-content files are deduplicated by the publisher. Two SHA-256 fan-out levels limit entries. Callers track portable refs, TemplateIDs and migration tokens and own the retention and deletion policies for published artifacts. Publication age alone does not establish that an artifact is no longer referenced. The project provides references and their changes; external retention and deletion are outside node lifecycle. Conductor/tasks share deterministic internal/reflocation resolution; the location name is self-sufficient without side tables.

The authenticated `POST /sandboxes/{id}/export` request remains
`{"toTemplate":false,"keepSource":true}`. Its successful response preserves the
existing `result` field and adds exactly the corresponding publication roots and
`removedRefs`. A Sandbox response has three fields:

```json
{"result":"kmt1.…","sandboxRef":"manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","removedRefs":[]}
```

A Snapshot response has four:

```json
{"result":"kmt1.…","snapshotRef":"manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","sandboxRef":"manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","removedRefs":["file://old.snapshot"]}
```

For `toTemplate:true`, only the meaning of `result` changes to the corresponding
`sbx`/`snp` TemplateID. Snapshot always returns the E actually referenced by final
S, including a selected located Bundle member. The publication adapter parses
one strict `sandbox-ctl publish --json` document and checks expected S/E role,
portable final roots, required final E, sorted unique removals and safe file
basenames **before** source finalization. It never treats diagnostic stderr as a
successful result. Builder checkpoint publication consumes the same contract.
The Core method returns one `types.ExportResult`; the HTTP `result` field remains
compatible with existing clients. Deploy sandboxer and orchestrator from the
matching source set when adopting the new internal CLI contract.

Local publication returns S1/E1 in both report and token; keep-source retains
S0/E0 in SQLite and the cache. Capture and its producer result are specified in
[Capture and Pause](#capture-and-pause).

**Upgrading the paired-source schema:** stop the old conductor and drain or retire its managed
sandboxes, preserving any artifacts and records you need before proceeding.
The operator must explicitly clear the old local sandbox database before
starting this version; there is no ALTER, migration, backfill or dual-read path
for the pair schema. The process rejects the old schema and never deletes user
data to upgrade it. Deploy the matching sandboxer/orchestrator versions and
recreate local records. Old migration tokens without the required `resumeSandboxRef` field, including
Snapshot tokens missing E, are invalid; discard stale tokens and re-export from a
complete paired record.
Do not use an old token or an S metadata read to reconstruct a durable pair.

An already portable source is not republished and has `removedRefs:[]`. A
portable Snapshot returns the exact S/E pair already stored in SQLite. Export
does not open artifacts, configure storage, inspect metadata or start a metadata
subprocess, even when both artifacts are offline. An incomplete paused pair is
invalid and is never repaired by reading S. The same roots and removals
accompany normal, keep-source, move and accepted detached-template returns.
Resume/preemption, ownership checks, lifecycle cancellation, idempotence and
concurrency retain the ordering below.

Removals describe the old known topology minus retained final references, not
field rewrite events or deletion commands. They include local roots and internal
refs that actually leave; shared refs retained elsewhere in the final topology
are excluded. With skip verification, replaced old refs are leaves even if
readable; explicit native lower lists remain known. No new payload scan, digest
or staging file is introduced by JSON reporting. All unlocated public file refs
are basenames with their existing `@digest`, `@hmac` or `@manifest` identity;
named locations keep their legal representation. Full source scope is used for
comparison before projection. The caller supplies the checkpoint directory
externally; the response contains no path/context/identifier/field-mapping
objects. A leaving Bundle selector does not imply deleting its physical carrier.
`keepSource` retains the original source and checkpoint despite reported removals.
The report itself triggers no artifact deletion or GC. A move uses the sandbox-owned cleanup described below.

TemplateID is `<profile>-<kind>-<base64url(canonical-portable-ref)>`:

```text
img: manifest ref,or located .image/.bundle
sbx: manifest ref,or located .sandbox/.bundle
snp: manifest ref,or located .snapshot/.bundle
```

Paused E promotes to sbx, S to snp; E is not rejected for lacking memory. Build Snapshot also yields snp, publishing through the same sandbox-ctl publish rather than a special Snapshot-only publisher.

The authenticated KMT V1 payload carries optional `autoReset` when true. Import restores this policy. Existing tokens omit it and import with the default false. An older strict reader rejects tokens carrying the new field.

KMT V1 directly contains resumeSourceKind, resumeSourceRef, resumeSandboxRef and autoPauseMemory. Snapshot requires its exact associated E in resumeSandboxRef; E-only requires that field to be empty. Missing fields, duplicate/unknown JSON and invalid combinations are rejected, including old Snapshot tokens without E. Import authenticates the original token and compares target expectations against its original S/E pair before selecting the new durable source. Destination retrieves tenant roots from the trusted local allowlist and verifies fingerprints/profile. It atomically inserts the selected source, deadline, portable env/metadata and existing ServiceSecret/Envd/Traffic/Forward credentials. Token contains no raw tenant roots, host paths, MMDS secret values, cluster group/route key or generation.

Export admission completes outstanding paused cleanup before registering the
attempt or starting publication. A private source-operation lock outside the
checkpoint directory is shared by capture, cleanup, export, disk-version release
and terminal resource removal. Publisher, capture CLI and `checkpoint-cleanup`
inherit its descriptor, so conductor cancellation or exit does not release their
protection before the actual child exits. The lock file stays with BaseDir until
terminal directory removal; it must not be independently unlinked or replaced.
Contention is retried outside the SID lifecycle lock with current state and paths.
An already clean source can resume while publication runs, but release and
terminal deletion wait for source use to finish. Cancellation alone is not proof
of completion. Normal completion, confirmed runtime disappearance and explicit
deletion share the source-protected resource cleanup; retained dead history and
execution results keep their existing business meaning.

Publish does not hold a long lifecycle lock. After validating the result, Export
checks the complete original S/E pair, source instance and export attempt under
the per-SID lifecycle lock. Resume winning `BeginResume` cancels KMT export with
409; template publication can finish detached and return its ID without source
cleanup. If Export wins finalization, `keepSource` preserves the original
ResumeSource and its necessary checkpoint artifacts after admission has completed
pending paused cleanup and durably cleared that ownership. Publication finalization
does not rewrite the retained source. A later move from that
retained source uses the same deletion path as any other move.

Without `keepSource`, Export validates local path ownership and calls the normal
lock-held delete acceptance entry. Success means a valid token/template result
and a durable `deleting` row; it may precede physical cleanup. The source is
removed from the runtime cache, and a non-routable deleting management Upsert is
published before Export returns. Full snapshots retain it until hard deletion. The existing finalizer fences the runner, releases the
network, removes RunDir and BaseDir (including all owned checkpoint files), then
hard-deletes the row and emits the terminal observer notification. This applies
to local and portable S/E sources alike, including portable sources with stale
local checkpoints. Published/shared output is outside source cleanup ownership;
remote/located artifacts are never deleted by this finalizer.

Failure to persist delete acceptance returns an error and leaves the paused
source, its S/E pair and checkpoint intact. After acceptance, directory or
hard-delete failure retains `deleting` ownership and the valid export result;
it never restores paused state. The worker retries independently of the HTTP
request. Shutdown leaves unfinished ownership in the same row for startup
reconciliation. Request/lifecycle cancellation can stop Export before it wins
finalization; after that point a bounded `context.WithoutCancel` critical section
finishes acceptance. Same-ID Create/import keeps the existing 409 conflict until
cleanup hard-deletes the source row; clients may retry after completion or import
under a different node-local ID.

Standalone import defaults to source NodeSandboxID; an explicit target changes only local ID while retaining StableID/service credentials. Atomic insert-only conflicts with 409. Connect can synchronously import a missing target from X-Kuasar-Migration-Token; existing targets ignore it. Paused routes project only kind-independent artifact_location local/remote for trusted migration consumers. Proxy does not inspect artifact kind; migration tokens are sensitive Sandbox credentials and never route-broadcast.

<a id="import-selection"></a>

### Import selection and runtime compatibility

`POST /sandboxes/import` accepts optional `memory: boolean|null` alongside `token` and optional `sandboxID`. Import creates paused state; it does not start a VM:

| Authenticated token source | Import memory | New durable source | Subsequent resume |
|---|---|---|---|
| S + paired E | Omitted/null or true | Original S + E, after runtime admission | Auto/true restores memory; false cold-starts |
| S + paired E | false | E-only, using the token's paired E | Auto/false cold-starts; true returns 409 |
| E-only | Omitted/null or false | Original E-only | Auto/false cold-starts |
| E-only | true | No row inserted; 409 memory unavailable | No resume accepted |

E-only selection changes only the new row's ResumeSource. It does not create a replacement snapshot, modify the token or delete/rewrite the original S/E. Authentication and expectations still apply to the complete original pair. E-only Import does not open artifacts or check target boot files; successful import does not establish that E is readable or cold-bootable. Those checks belong to the later launch.

The platform adds runtime admission at exactly two entries: fresh Create from an snp template, inside its existing asynchronous tenant preparation; and Import selecting S, synchronously before insertion. Import uses a short-lived tenant helper to read S and its paired E metadata and check the runtime identity actually required by that source. It does not restore memory, scan the disk graph or allocate a sandbox runner. For Import, a definite runtime mismatch returns 409; metadata, access, I/O and cancellation failures remain check errors. Existing local Connect/Resume/Wake add no platform precheck; sandboxer still validates the runtime at actual memory restore.

Import selecting S uses an internal four-second admission context, or the caller's earlier deadline, for the metadata/helper check and passes it to insertion. Normal helper output drains within that context; cancellation closes the parent pipe endpoints, with a separate 250 ms bound on process cancellation cleanup. The existing same-sandbox lifecycle fence does not support cancellation, so waiting for that lock can exceed the admission budget. Import checks the context again after acquiring it and rejects an expired insertion. A failed or timed-out metadata check inserts no row. The budget is therefore not a hard deadline for the entire Import request, a user option or sandbox TTL; fresh snp Create keeps its existing task deadline. These checks do not add component-aware scheduling.

The token remains KMT V1. `RuntimeDigest` still means the whole-file SHA-256 of the node's default runtime when the token is minted, but Import no longer compares it with the target default file. Minting a migration token during Export therefore still needs that default runtime file and retains its hashing cost. Old receivers can parse V1 but retain their old compatibility rules. Upgrade the receiving Conductor and matching tools before relying on Import memory selection: older HTTP handlers can silently ignore the unknown `memory:false` field. See [the upgrade compatibility proposal](https://github.com/kuasar-sandbox/orchestrator/issues/519).

Running means runtime readiness and mandatory e2b /init, not application-port health. Generic backend health/dial retry is separate work. Cluster Create/Connect reuse node primitives. Secrets-only CONNECT import remains standalone-only; node-link carries no MMDS route-value plaintext.

<a id="export-import-command"></a>

## `node-ctl export-sandbox` / `import-sandbox`

These clients use conductor's local **API plane** for `POST /sandboxes/{id}/export` and `POST /sandboxes/import`. `E2B_API_KEY` must authenticate the owner. See [Artifact capture, templates and migration](#capture-and-pause).

```
node-ctl export-sandbox <sid> [--checkpoint ID] [--to-template] [--keep-source] [--json] [--timeout D] [--socket S]
node-ctl import-sandbox <token> [--memory=true|false] [--socket S]
```

- `--timeout` bounds the complete export request (default `20m`, positive Go duration). Publication materializes disk contents and can exceed short administrative deadlines. A timeout cancels this client request; it is not proof that an already accepted server operation rolled back. Check the source/result before retrying.
- `export-sandbox <sid>` prints one opaque `kmt1.` migration token. By default, success means the valid result is ready and source deletion is durably accepted; physical cleanup may finish after the response; `--keep-source` retains it paused with its original ResumeSource and local checkpoint — the source resumes from exactly where it would have resumed without the export (#336).
- `export-sandbox <sid> --to-template` publishes paused E or S and prints persistent `sbx`/`snp` templateID for fan-out. Source retention is identical: `--keep-source` retains it; omission accepts its deletion with the same eventual cleanup semantics.
- Resume remains allowed during local-artifact upload. If Resume wins first, KMT Export cancels upload and returns 409, whereas Template Export continues and returns templateID. Both abandon the source finalizer without updating/deleting source or local artifact.
- `import-sandbox <token>` defaults to source NodeSandboxID, inserts a paused row without overwriting and prints SID; an existing target, including a source still in `deleting`, returns 409. Reusing the same node-local ID requires cleanup to finish; import does not wait for it. API body may choose another node-local `sandboxID`, retaining logical authentication identity and service credentials. A subsequent Connect accepts asynchronous resume.
- Omit `--memory` to preserve the token's source. `--memory=false` permanently imports only E, including from an S token; `--memory=true` requires S. The client sends the field only when explicitly specified. The [Import selection table](#import-selection) defines compatibility and failure behavior; using this option requires a server that supports it.

`--json` prints the complete successful Export API response. Default output remains
one `result` line (token or template ID); quiet publication progress never enters
stdout. Invalid/trailing JSON, polluted stdout, missing result or roots, wrong
root role, unsafe local refs and incoherent template/root pairs fail explicitly.
The client also checks response read errors. No empty credential is printed as
success. The full response contract is documented in [Publication, templates and migration](#publication-and-migration).

<a id="request-configuration"></a>

## Sandbox configuration propagation

Each instance receives configuration through reserved e2b metadata namespaces `kuasar-sandbox.<ns>`, each encoded as a JSON object string, without SDK/API changes. These typed tenant-facing schemas define the permitted subset and are rendered into sandboxer configuration; conductor does not blindly import a complete runtime YAML as tenant policy.

| Namespace | Destination |
|---|---|
| resource | Strict partial patch for capacity, allocatable, startup, DiffCOW cache limits and cold-start discard policy ([resource schema](node-resource.md#request-resources)) |
| traffic | Host-only explicit per-Sandbox max_inflight total/forward/e2b:envd/e2b:code-interpreter/exec; never guest configuration |
| network | hostname/nexthop to guest; inner_ip/transit_* to vswitch Attach; dns to resolv.conf |
| launch | exec/args/env/workdir/restart/user/stop_signal/plugin/cgroup_control; bare only, since e2b reserves launch for envd |
| init / mounts / files | Typed init[]/mounts[]/files[] forwarding |
| metadata | Runtime SANDBOX_CONFIG metadata, e.g. e2b.start_cmd |
| restore | Current host prefetch policy: optional, explicit off/memory only |
| credentials | Create-time ServiceSecret and Envd/Traffic overrides, separated from ordinary metadata and never guest config |
| checkpoint | Host-only Create-local Pause defaults: merge_ref/drop_caches true/false/null; Sandbox row only, not runtime YAML/snapshot.cfg |
| workspace | Portable data-disk names and mounts; create selectors are private. See [Workspace storage](#workspace-storage). |
| mmds | Portable exact routes plus request-scoped initial secrets; split before persistence so metadata retains routes only |

Field definitions follow their capability: [resource patches and precedence](node-resource.md#request-resources), [traffic limits](node-proxy.md#request-traffic), [MMDS](node-proxy.md#request-mmds), [credential overrides](node-proxy.md#request-credentials), and [cluster system context](cluster.md#node-system-context). This section defines shared input assembly, restore and checkpoint policy.

<a id="request-restore"></a>

### Restore policy input

Equivalent per-Sandbox prefetch inputs are:

```http
X-Kuasar-Sandbox-Restore: {"prefetch":"memory"}
```

```json
{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}"}}
```

Restore accepts only a strict JSON object; unknown fields, nonstring prefetch or invalid enum fails before lifecycle side effects. Omission defaults off; Header overrides metadata in that request. Orchestrator does not decide local/remote, single/multiple layers or backend capability. It renders explicit memory unchanged; sandboxer executes or skips it.

<a id="request-checkpoint"></a>

### Checkpoint policy input

Checkpoint policy accepts Create Header or metadata:

```http
X-Kuasar-Sandbox-Checkpoint: {"merge_ref":false,"drop_caches":null}
```

```json
{"metadata":{"kuasar-sandbox.checkpoint":"{\"merge_ref\":true,\"drop_caches\":false}"}}
```

Both use the same strict object parser allowing only merge_ref/drop_caches and true/false/null. Unknown fields, wrong types or trailing/second values return 400. Header overrides by field; null/absence means no override, not clearing a lower value. If all fields remain null/absent, remove the namespace. Source templates do not supply it. Build checkpoint input is defined in [Request-scoped Builder input](node-build.md#request-scoped-builder-input). Local/Bundle use identical parsing, precedence and side-effect boundaries.

<a id="configuration-rendering"></a>

### Configuration resolution and persistence

The complete request-scoped `kuasar-sandbox.builder` input and target rules are in [Builder input](node-build.md#request-scoped-builder-input). The rules below continue to govern shared Sandbox configuration.

- **Rendering:** cold images use a pure resolver to create complete ResourcesConfig before Attach/Assign. Artifact launch resolves capacity from the task summary in the sole launch worker; conductor never opens the snapshot. Renderer installs the canonical object without reinterpreting fields. YAML has no control.cgroup_path; run-sandbox injects the trusted cgroup-FD capability at child Start. Dynamic controller comes only from resolved resource_listen.SocketIdentity; node injects watermark_high.ratio and leaves sensor absent. Boot/tapfd/resolved network and allowed tenant fields follow the config-socket handoff.
- **Two injection surfaces:** e2b metadata and X-Kuasar-Sandbox-* Headers normalize at the API edge. Resource/traffic use leaf precedence; MMDS/checkpoint use the special merge rules above; remaining namespaces generally use whole-object Header precedence. Create runtime inputs use sandboxes.metadata_json. The shared parser, target admission and Build persistence rules are defined completely in [Request-scoped Builder input](node-build.md#request-scoped-builder-input). Cluster Sandbox ownership uses separate system context ([Cluster integration (node-link)](cluster.md#node-attachment)).
- **Precedence:** resources follow the fixed leaf chain; remaining inputs follow their declared namespace rules across node/group/current Create. Canonical TemplateID Create never reads builds.metadata_json. Build resources and final Sandbox resources are independent, without mutual defaults/comparisons/derivation. Trigger cpuCount/memoryMB only assert immutable Build resources.
- **Capacity:** img Create uses request/group/node defaults. sbx/snp Create, paused resume and migration take portable E capacity as authority. Synchronous acceptance checks patch structure only; after runner preparation, explicit matching capacity is an assertion, while any differing leaf causes asynchronous resource_resolve failure. Unreadable capacity fails rather than falling back. Runner is already assigned, but networking/YAML/controller reservation/VM side effects have not begun; the runner delegation cgroup may already exist. Initial restore reservation exactly equals sandboxer's BudgetAtSnapshot derived from captured CH target/current; startup headroom and partial grants do not apply.
- **Network travels with E:** cold rendering records logical network in SANDBOX_CONFIG.metadata[kuasar-sandbox.network], which enters E's sandbox.runtime.cfg. S refers to E; snapshot.cfg itself contains no metadata. Artifact tasks strictly parse/validate it and submit typed bounded network summaries, never raw metadata to conductor. Explicit current fields win over inherited artifact fields. Build template network projection is in [Request-scoped Builder input](node-build.md#request-scoped-builder-input). MigrationToken also carries metadata. Other runtime namespaces apply at cold start or are captured in memory; host restore/checkpoint policies stay separate.
- **Host policy stays outside artifacts/templates:** restore/checkpoint originate in Create metadata. Image cold boot does not render them into runtime YAML. snp Create, later resume and migration rerender restore policy when restoring S. Checkpoint is read only by host Pause and never enters SANDBOX_CONFIG/snapshot.cfg. Connect/resume offers no temporary override.
- **Persistence:** sandboxes.metadata_json holds launch input. Build metadata/builder JSON are retention-bounded execution records, never a second template authority.

<a id="node-defaults"></a>

## Node lifecycle defaults

These fields belong to Conductor configuration. Shared loading, path and environment rules are defined in [node operation](node.md#conductor-configuration).

| Field | Default | Meaning |
|---|---|---|
| `sandbox.timeout_sec` | `300` | Default sandbox TTL in seconds |
| `sandbox.dead_ttl` | `24h` | Positive Go duration retaining a dead parent and all its owned data, from its first durable dead commit ([Termination, Reset and retained data](#reset-and-retention)) |
| `sandbox.network.switch` | `sw0` | Vswitch name |
| `sandbox.network.hostname` | `sandbox` | Guest sethostname and `/etc/hosts` entry ([Guest profiles: envd and the toolchain](node.md#guest-runtime)) |
| `sandbox.network.dns` | `[169.254.169.253]` | Guest `/etc/resolv.conf` nameserver; deployment must route it to actual DNS |
| `sandbox.network.e2b` / `.bare` | `169.254.0.21/30` + `169.254.0.22` / `169.254.1.1/31` + `169.254.1.0` | Per-profile `{inner_ip,nexthop}` reused inside guests; floating IP uniquely identifies a sandbox. e2b's /30 and gateway support envd port forwarding |
| `checkpoint.mode` | `local` | Local paused capture: role tarstream or multi-Manifest ZIP Bundle; output stays in Sandbox BaseDir/checkpoint ([Node directories and identities](node.md#node-directories-and-identities), [Artifact capture, templates and migration](#capture-and-pause)) |
| `checkpoint.merge_ref` / `.drop_caches` | Unset | Node tri-state Pause policy. Explicit true/false reaches sandbox-ctl snapshot; omitted/YAML null uses sandboxer's default |
| `checkpoint.remote.ref_location_parent` | Empty | Optional absolute hostless file URI. Named-location parent for Build checkpoint graphs/export-sandbox and located Build image Bundle resolution; does not change Pause mode |
| `checkpoint.remote.manifest` | `false` | False sends image-class outputs to Manifest Store. True instead materializes single-root Manifest Bundles at the required named parent; it does not change checkpoint mode or checkpoint-class policy ([Target-aware execution and publication](node-build.md#execution-and-publication)) |

<a id="state-and-ownership"></a>

## Lifecycle and state machine

Four fixed rules govern lifecycle:

```text
Pause decides what is saved.
Resume decides what is used.
Wake only triggers Resume.
LaunchMode records what will actually run.
```

CaptureKind, ResumeSource, ResumeMode, LaunchMode and ResumeTrigger are separate:

| Concept | Values | Responsibility |
|---|---|---|
| CaptureKind | snapshot / sandbox | Whether this Pause captures S or E |
| ResumeSource | kind/ref plus SandboxRef for Snapshot | Exact S/E pair, or E-only root, owned by the row |
| ResumeMode | auto / memory / cold | Caller selection for this resume |
| LaunchMode | image / memory / cold | Resolved mode actually executed by starting |
| ResumeTrigger | connect / wake / route / exec / exec-session | Low-cardinality observation source, not mode or permission |

`ResumeSource` retains `Kind` and `Ref`. A Snapshot also requires `SandboxRef`,
the exact E selected by S; an E-only source holds E in `Ref` and leaves
`SandboxRef` empty. The complete pair is one atomic durable identity in SQLite,
cache, lifecycle CAS comparisons, restart recovery and encrypted migration tokens,
not a best-effort cache. `removedRefs` belongs only to operation reports and is
never persisted in SQLite or tokens.

An image is a read-only filesystem root. E contains portable sandbox.runtime.cfg and disk state, without guest memory. S holds memory and references authoritative E through snapshot.cfg.sandbox_ref. S always has real memory state; its config does not represent E with a memory toggle. Template mapping is:

```text
img -> LaunchImage  -> sandbox-ctl run
sbx -> LaunchCold   -> sandbox-ctl run --from <E>
snp -> LaunchMemory -> sandbox-ctl run --restore <S>
```

The state machine is:

```text
Create(img|sbx|snp) --> starting --success--> running
                         +--fresh failure--> dead

running --Proxy ACK--> pausing --capture + commit--> paused(source=E|S)
paused --Resume(auto|memory|cold)--> starting(launch_mode=cold|memory)
                                      +--success--> running
                                      +--failure--> paused(original E|S)

running --termination + cleanup--> dead --Reset + commit--> paused(source=E)
Import --source selection + insert--> paused(source=E|S)

running|starting|paused|dead --Kill/Delete acceptance--> deleting
deleting --finalizer success--> absent
(dead retention expiry also requests deletion)
```

Starting is durable and spans admission, artifact preparation, resources/network, pool assignment, runtime readiness and mandatory e2b /init. Fresh Create stores image/cold/memory launch_mode. Paused resume stores cold/memory in the same atomic paused→starting update. Running/paused/deleting/dead clear launch_mode. Running may retain latest ResumeSource to track local artifact ownership; a successful Pause atomically replaces it.

Create/Connect/Wake/route activation/native exec/exec-session/migration import share one SID launch group. Under its lifecycle lock, admission rereads durable state, authorizes, resolves mode and performs Store CAS, then creates/joins an attempt. Attempt mode only caches durable launch_mode. Kill/Delete exact-CAS into deleting under that lock, retaining RunID/unit/port/RunDir/BaseDir/artifact ownership, cancels attempt, excludes it from the runtime cache and publishes deleting in live/full management projections. Business-route withdrawal is not proof of cleanup.

After durable acceptance, finalizer waits for late launch ownership to finish, then stops/resets exact unit with inactive readback, detaches inside allocation fence, full-owner-CAS clears network tuple, removes RunDir and BaseDir and exactly hard-deletes. Only successful durable network clear releases the detached-port fence. Later directory/row failures retain path/runner ownership without blocking new Attach. Hard-delete publishes terminal Extension/object observation and the final route Delete. Failures retain unfinished ownership and retry in-process or after startup. Fresh Create's route-applied barrier precedes resource launch; failed acceptance cleans runtime ownership while retaining formed data, then exactly commits dead with automatic responsibility and publishes its management projection.

CommitPaused atomically stores state/source. RunID, port and RunDir clear individually only after successful Stop/Reset fencing, Detach, selective checkpoint cleanup and RemoveAll. RunDir CAS also clears its envd/CI UDS paths. Each failure retains remaining retry fields. Fully cleaned paused state retains BaseDir with checkpoint data and its source-operation lock, plus the source. Resume/Wake/Exec finish cleanup backlog before taking a new runtime owner and atomically restore canonical RunDir/UDS on paused→starting. Runtime cleanup never removes paused BaseDir; explicit Delete removes it and then the row.

When `resource_listen` is enabled, local admission uses the selected resource pool's
source-independent [effective-zone matrix](node-resource.md#rawzone-and-effective-zone).
Green/yellow admit Create and ordinary Resume; red admits ordinary Resume;
critical retains eligibility for current resource-paused recovery. Snapshot
template Create is still Create. All paths classify the final resolved operation,
and accepted starting work retains its asynchronous identity if the zone changes.

Resource pressure invokes the existing full memory Pause, including native exec
quiesce and cleanup. It records `resource-pressure`, the current recovery
obligation, a version, and the continuous-running start time in the existing
SQLite row. The single recovery worker and real Proxy Wake share launch
singleflight. Actual running commit clears the obligation and resets the running
interval; runner creation or snapshot opening does not. Cleanup ownership and
resource Release remain separate facts.

For an authenticated resource-paused sandbox, default memory Pause atomically
adopts its retained source as ordinary explicit paused state after the Hook and
its full-record CAS check. IDs/source/cleanup do not change, and there is no new
capture or VM start. This cancels resource auto-recovery and unused holds, and
publishes the changed reason even though state remains paused. Explicit capture
options or filesystem-only Pause return 400 because compatibility with a new
capture action cannot be certified. Ordinary repeated Pause and starting retain
409. Adoption survives restart; later real Wake uses ordinary eligibility, while
the legitimate saved snapshot retains its complete-budget recovery capability.
It creates no permanent user-paused lock and cannot restore killed exec children
or replay requests already delivered to the old VM. See
[resource lifecycle](node-resource.md#resource-pause-recovery-and-explicit-adoption)
for Q, critical exit, storage and accounting rules.

<a id="task-local-preparation"></a>

## Task-local preparation and three configuration types

Preparation may discover Capacity after the first worker is assigned. If the
resource-pool decision changes, Conductor stops that preparation-only worker
before clearing its assignment, prepares again on a replacement and verifies
matching summaries before publishing final launch configuration. No VM starts
on the old preparation-only worker. [Resource pools](node-resource.md#pool-lifecycle-and-pressure)
own placement, durable recovery intent and capacity accounting.

Artifact launch runs internal/taskartifact inside its tenant task. ArtifactPrepareSpec v5 includes RunID, source kind/ref, its already accepted E if present, durable mode, manifest config, location parent, relative directory, max refs and absolute deadline. With its MANIFEST_KEY, the task uses sandboxer's real readers to open/decrypt artifacts; conductor does not.

```text
E + cold   -> open E, parse sandbox.runtime.cfg, compute disk closure, prepared=E
S + memory -> open S, open S.sandbox_ref E, compute memory+disk closure, prepared=S
S + cold   -> open S, resolve/open S.sandbox_ref E, compute disk closure, prepared=E
E + memory -> reject during request mode admission; invalid task spec fails before VM launch
```

For remote Manifest S+cold, selected source is manifest://E. Local tarstream resolves relative content-identified E refs. Bundle preparation preserves the reader's actual current/sibling carrier and E selector, or its remote Manifest fallback; it never assumes E is in the S carrier. PreparedSource for runtime argv, ref-location URIs, carrier/Bundle binding and full config remain task-local. The summary additionally returns the exact root S/E identities. Network metadata is strictly decoded there, rejecting unknown/duplicate/malformed fields. Only the paired root identity, typed network, capacity, disk topology, required-ref count and resolution_digest reach conductor. Digest covers RunID, the complete root pair, launch mode, selected source, closure, locations, carrier binding and capacity/network summary. An external S-only template is an initial preparation input while the durable source is empty; it is not a valid paused source. The existing tenant task read resolves E, and the exact starting worker commits S/E together with running state. Failure leaves the initial source unaccepted; restart does not recover an incomplete pair. Preparation cannot replace E in an imported or persisted pair. Same RunID/digest replay returns the same result; conflicting replay fails. taskrun appends --from E or --restore S only from local PreparedSource.

Runtime uses three explicit DTOs:

- ImageColdConfig for run --config may contain image root/diff template, launch/env/files, mounts/init/plugin, resources/network/metadata.
- SandboxHostConfig for run --from E --config includes only ApplyFromRules host/instance fields: actual kernel/runtime bindings, active diff, resource controller/host policy, network provider/identity, timeouts, allowed persistent overrides, ephemeral_files and launch.ephemeral_env. It cannot declare immutable root/data graph. Paused E or S+cold treats E C0 as authoritative without replaying old row launch/env/files; only fresh KindSbx Create can deliberately apply current persistent overrides.
- SnapshotHostConfig for run --restore S --config renders the allowed runtime candidate, active diff, controller/allocatable, network provider/identity, restore policy and timeouts. The renderer clears and omits the kernel binding because S already contains the running guest kernel. The type omits launch/files/ephemeral files/init/plugin/mounts/metadata/cmdline/disk graph.

Node-generated hosts/resolv.conf use ephemeral_files during cold launch, not portable C0; memory restore does not reinject them. Persistent env/files follow deliberate C0 overrides; ephemeral input affects this cold invocation only. Rendered YAML must pass LoadMergedWithPresence and the corresponding ApplyFromRules/ApplyRestoreRules, retaining E's data-disk name/order/mount authority.

<a id="application-startup-policy"></a>

## Application startup policy

Node-local `sandbox.timeouts.app_start` is an optional SDK post-ACK overall
deadline. Omission, empty and `0` add no deadline; positive durations opt into a
bound, and negative/invalid values are errors. Existing clone, bootstrap,
workorder and Builder rendering carry this host policy; portable E/S artifacts
never do. Socket deadlines, API/Proxy waits, assignment budgets, running TTL and
Builder total/step/readiness deadlines retain their existing responsibilities.

Ordinary accepted launches follow the attempt owner through prepare, readiness
and /init, without the former implicit 60-second limit. Sandbox bootstrap version
7 retains zero/omitted `absolute_deadline_unix_nano`; positive means an explicit
absolute cutoff and negative is invalid. Version 7 additionally requires fresh snp
tasks to honor runtime admission. Deploy matching Conductor and sandbox runner
versions; bootstrap version 6 is rejected instead of silently omitting that check.
Shared prepare summary stays version 5; Builder version 7 and its positive work
deadline/reporting reserve, the cluster protocol and KMT V1 are unchanged.

Manifest opens reuse Sandboxer's `OpenManifestStream`/readretry with the original
scoped fetcher, Bundle identity and caller context. Builder benefits within its
own work deadline; file carriers keep their existing recovery boundary.
Store/Cache Get/GetShard remain single attempts. Recovery never replays
Prepare/Create/Resume or allocates another runner/network.

Upgrade conductor and idle node-ctl runners together. Reconcile stops each old
orphan's exact unit and confirms inactive before pool refill; failure reports the
RunID/unit and prevents startup. A new pool only accepts RunIDs it created.
Generated units retain the resolved node-ctl executable; self-managed unit
operators must install the compatible binary at their ExecStart path. Running
VMs are not replaced. Conductor restart continuation of all starting attempts
is outside this contract.

New initial Launch requires Guest `initial_launch_state_v1` before apply.
Rebuild runtime Bundle with matching sandbox-init, including prewarmed Guests
intended for a new Launch. Host upgrade cannot replace init inside an existing
snapshot. Ordinary restore retains restore semantics and does not consume an old
token as a new startup fact. Initial notification and verified Pong confirm the
same token; PID presence or reachability alone cannot confirm success.

An unavailable dependency can retain a `starting` runner, network and directories
indefinitely. Inspect existing state, launch phases, accepted duration and exact
RunID; running TTL does not reap starting. Kill/Delete, owner cancellation, real
runner/VM exit, permanent read errors, readiness EOF/malformed wire and /init
non-204 remain terminal. Cleanup stops the exact runner, confirms inactive, then
detaches/removes resources. Positive app_start or explicit PingFatal policy opts
into bounded termination; default PingFatalThreshold stays zero. See the
[conductor example](../deploy/conductor.example.yaml).

The prepared [launch recovery case](../test/e2e/cases/orchestrator.launch-recovery.sh)
observes actual Manifest Get failures for more than 60 seconds on image, E and S
startup paths. It compares SID, RunID, exact runner/process identities and network
ownership, checks one runtime spawn, and deletes a still-blocked accepted attempt.
E/S preparation precedes VM creation; the Sandboxer recovery case separately
proves VM survival across post-ACK and ordinary-read failures. Record the actual
selected SDK and Guest Bundle, not just a successful local relative replace build.
