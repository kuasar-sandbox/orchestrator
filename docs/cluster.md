[English](cluster.md) | [简体中文](cluster_zh.md)

<a id="cluster--registry-自聚簇路由与放置控制面"></a>
# cluster — Registry clustering, routing and placement control plane

`cluster-ctl` is the cluster control plane for large deployments, with three independent roles:

- `registry`: a reliable, stateful cluster that maintains execution state for nodes, routes and placer imports.
- `router`: a unified e2b-compatible entry point that locates the route owner by sandbox group and forwards the hot path directly to the node.
- `placer`: the group provider/importer and placement scheduler; it consumes `node_list` and offers Place / verify-key to Registry.

Registry's foundation is a consistent KV organized by `namespace + shard key + recordSet + record key`. All `node_link`, `route_link`, `node_list` and `placer_link` records share this model: no traversal across shards, full replication within a shard, and convergence through quorum reads/writes, CAS, WATCH and read repair.

<a id="1-概述"></a>
## 1. Overview

<a id="11-总体拓扑"></a>
### 1.1 Overall topology

```text
                         client / e2b SDK
                                │
                                ▼
                         ┌────────────┐
                         │   router   │
                         │ ingress +  │
                         │ route cache│
                         └─────┬──────┘
                               │ route_link Reserve/Resolve
                               ▼
┌────────────┐          ┌──────────────────────┐          ┌────────────┐
│ node-ctl   │ node_link│      registry        │ placer_link│   placer   │
│ serve      │◄────────►│   state cluster      │◄────────►│ placement  │
│ sandbox VM │          │ shardkv namespaces   │          │ importer   │
└────────────┘          └──────────────────────┘          └────────────┘
                               ▲
                               │ node_list WATCH_LIST
                               └────────────── placer consumes one owner
```

The steady-state data plane bypasses Registry. Explicit create/connect/exec-session operations and data requests with a missing target or typed stale fallback require Registry:

- `POST /route-link/reserve` distinguishes four operations using `operation=create|connect|exec-session|data`. Create calls Reserve directly; Registry completes connect/exec-session through node-link; data calls Reserve only when the route lacks a complete node target or the node proxy reports typed stale state. Unknown routes do not implicitly create sandboxes. Explicit build registration calls `ReserveBuild`.
- Registry calls the placer's `PlaceSandbox` / `PlaceBuild`.
- Registry sends create/connect/exec_session/delete/build/key commands through the node owner.
- Nodes report sandbox/build state and the low-frequency node directory through node_link.

<a id="12-设计原则"></a>
### 1.2 Design principles

1. **Group is the business shard key**: every northbound cluster request must include `X-Kuasar-Sandbox-Group` or equivalent group identity. A cluster data-plane entry without a group is currently unsupported.
2. **Registry is a reliable state cluster**: Registry members directly replicate execution state. With three owners, a logical owner set tolerates one member failure; a single-owner configuration has no redundancy. Automatic recovery of running sandboxes is not required after the entire Registry cluster loses its state.
3. **No traversal across shards**: Registry must not scan across group/node shards or combine partial results from unrelated shards into an authoritative view.
4. **Full replication within a shard**: owners of the same `namespace + shard key + recordSet` hold the complete view and can serve point reads, CAS and WATCH. A non-owner may coordinate point reads/CAS, but cannot provide a complete local Snapshot/WATCH.
5. **Member health does not determine sharding**: the member list comes from versioned configuration; `LocateN` takes only membership members as input. Memberlist detects health and propagates metadata.
6. **Nodes are the authority for runtime state**: node reports determine whether sandboxes/builds still exist. A host reboot loses running processes; surviving durable rows are reconciled against actual units and checkpoint/cleanup ownership. A conductor process restart can adopt surviving units. Neither event means blindly deleting all durable rows or automatically recreating every old sandbox; see §13–14.
7. **Placer does not own lifecycle**: it imports groups, patches selectors, applies shuffle sharding/P2C and proposes placements. The node owner checks current connectivity/usage; the node's durable transaction makes the authoritative Build registration admission decision.
8. **Router does not subscribe to huge numbers of groups**: create/data Reserve returns READY or an error; connect/exec-session Reserve returns after synchronous node preparation, without waiting for asynchronous resume. Router keeps only a bounded route cache; in-flight requests do not provide routes for new requests.

<a id="13-角色边界"></a>
### 1.3 Role boundaries

| Role | Responsibility |
|---|---|
| Registry member | Forms the Registry cluster and hosts `route_link` / `node_link` / `node_list` / `placer_link` execution state and membership |
| Router | Unified e2b entry; locates route owners by group; Resolve on a cache miss, Reserve for create/connect/exec-session/data; local route cache on the hot path |
| Placer | Consumes `node_list` WATCH_LIST; imports groups through providers/importers; maintains placement and selector patches; offers Place / verify-key |
| Node | Runs sandboxes/builds; reports full inventories and events through node_link; receives create/connect/exec_session/delete/build/key commands |

<a id="2-配置与监听"></a>
## 2. Configuration and listeners

Registry uses one control-plane listener by default. A separate node_link listener can isolate long-lived node connections without changing owner rules. The following three-member example shows membership and timing fields; `https://` advertisements also require the matching `member.tls` configuration and peer trust. They do not enable TLS by themselves. The built-in default has one member and one owner per namespace.

```yaml
member:
  id: A
  listen: "0.0.0.0:7700"       # Unified Registry control-plane listener

membership:
  active: 1
  # next: 2                    # Target version during joint phase
  # old_grace: 1               # Retain old members as read-only shardkv certificate/snapshot sources after cutover
  reload_ready_timeout: 10s
  versions:
    - version: 1
      members:
        - { id: A, advertise: "https://A:7700", node_advertise: "A:7700" }
        - { id: B, advertise: "https://B:7700", node_advertise: "B:7700" }
        - { id: C, advertise: "https://C:7700", node_advertise: "C:7700" }
  owners:
    route_link: 3
    node_link: 3
    placer_link: 3
    node_list: 3

node_link:
  # listen: ""                 # Empty reuses member.listen; nonempty provides a separate long-lived node listener
  heartbeat_interval: 10s
  node_dead_after: 30s          # Wait after node-link disconnect before cleaning stored state

route_link:
  park_timeout: 30s

node_list:
  watch_retention: 10000

placer_link:
  placer_label: placer.default
  placer_replica_count: 3      # Number of Registry-to-placer failover candidates
  min_ready_placers: 1
  place_timeout: 2s
```

Default paths:

```text
/cluster/membership
/node-link/*
/route-link/*
/placer-link/*
/internal/registry-member/shardkv
/internal/node-owner
/internal/node-link/relay
/internal/memberlist/packet
/internal/memberlist/stream
```

Registry's advertised address is `membership.versions[].members[].advertise`; redirect-capable nodes use `membership.versions[].members[].node_advertise`.

<a id="3-membership-与健康检测"></a>
## 3. Membership and health detection

<a id="31-版本化成员表"></a>
### 3.1 Versioned membership

Registry membership comes only from an operator-distributed configuration file. Each version has a stable label:

```text
registry.<version>.<sha256(sort(member_ids))>
```

Registry can hold three views simultaneously:

- `active`: the version clients use to locate route/node/node_list/placer_link owners.
- `next`: the target version during the joint phase. Writes must satisfy both active and next quorums.
- `old_grace`: the previous version retained after cutover. Old members may still accept peers/node_link or receive node-owner RPCs, and provide old-head snapshots/certificates as a read-only shardkv set. They are excluded from write owner sets and cannot commit writes using the old view.

```text
stable(v1)
  -> load_config(active=v1,next=v2)
  -> wait_memberlist_ready(v2)
  -> joint owner set: owners(v1) ∪ owners(v2)
  -> cutover active=v2, old_grace=v1
  -> retire(v1)
```

Reload can load/cancel `next` from a stable state, or promote an already configured `next` to the new `active`. It cannot jump directly from stable(v1) to stable(v2).

<a id="32-memberlist-边界"></a>
### 3.2 Memberlist boundaries

```text
membership config                       memberlist
  defines member ids                      observes liveness
  defines advertise URLs                  propagates tiny meta
  input to LocateN                        does not reshard
  controlled by reload                    does not own member list
```

Each Registry membership label has its own memberlist domain. Memberlist transport reuses control-plane HTTP:

```text
registry A /internal/memberlist/*  ◄────►  registry B /internal/memberlist/*
label=registry.1.hash(A,B,C)
```

Memberlist is used only to:

- Determine whether configured members are reachable at runtime.
- Publish Registry/placer readiness metadata.
- Provide signals for RPC fail-fast/cooldown.

Memberlist does not:

- Maintain the Registry member inventory.
- Change `LocateN` inputs.
- Replicate data.
- Turn suspect/dead states into resharding.

<a id="33-placer-memberlist-域"></a>
### 3.3 Placer memberlist domain

Placer uses a separate label, `placer.default` by default. Registry has no configured placer list; it joins the placer memberlist with `role=observer`. `POST /placer-link/register` supplies only a seed for Registry's initial join. Placer memberlist metadata expresses readiness:

```json
{"role":"placer","id":"s1","advertise":"https://s1:7800","ready":true,"ready_label":"registry.2.hash"}
```

Registry considers only members satisfying `role=placer && alive && ready=true && ready_label==active_registry_label` for Place / verify-key.

<a id="4-registry-状态模型"></a>
## 4. Registry state model

<a id="41-数据层级"></a>
### 4.1 Data hierarchy

```text
namespace
  └── shardKey
        └── recordSet
              ├── commit Rev
              ├── recordKey -> value
              └── tombstone(recordKey)
```

- `namespace`: a logical domain, such as `route_link` or `node_link`.
- `shardKey`: the member-sharding key, such as group or node_id.
- `recordSet`: the replication, Rev and WATCH domain.
- `recordKey`: a record key within the recordSet.
- `Rev`: the recordSet commit version. A record's `rev` is the recordSet Rev at that record's last modification.

A shard is the unit of member sharding; a recordSet is the unit of data replication. Rev does not belong at the shard level, which would unnecessarily couple independently evolving profile/sandbox/build/key domains within that shard.

<a id="42-owner-解析"></a>
### 4.2 Owner resolution

```text
(namespace, shardKey)
        │
        ▼
ShardResolver
        │  LocateN(shardKey, versioned members, owner_count)
        ▼
owner set
   ┌──────────┬──────────┬──────────┐
   ▼          ▼          ▼
 registry A  registry B  registry C
 full copy   full copy   full copy
```

`membership.owners.route_link/node_link/placer_link/node_list` controls the owner count for each namespace. Owner count is Registry's internal replication factor. `placer_link.placer_replica_count` controls only the number of ready-placer failover candidates called by Registry; it is not the owner count of the `placer_link` namespace.

<a id="43-namespace-schema"></a>
### 4.3 Namespace schema

| Namespace | Shard key | RecordSet | Record key | Contents |
|---|---|---|---|---|
| `route_link` | group | `sandbox` | route_key | Route record |
| `route_link` | group | `build` | build_id | Build execution state |
| `node_link` | node_id | `profile` | `profile` | Node profile, labels, liveness, link_owner and low-frequency capacity |
| `node_link` | node_id | `sandbox` | node_sandbox_id | Per-node sandbox ownership; value contains sandbox_id + sandbox_generation + group + route_key + profile + api_secret_fingerprint |
| `node_link` | node_id | `build` | build_id | Per-node Build ownership; value contains group |
| `node_link` | node_id | `key_pair` | api_secret_fingerprint | Node APISecret+ManifestKey pair cache |
| `node_list` | `node_list` | `nodes` | node_id | Low-frequency node directory and WATCH_LIST |
| `placer_link` | `import/source/<source_id>` | `import` | `state` | Import-source lease/cursor |

A fixed schema defines the current recordSet collection. If a namespace introduces dynamic recordSet names in the future, its recordSet directory must itself be a reserved recordSet in the same shard, following the same CAS/WATCH rules.

<a id="44-读写协议"></a>
### 4.4 Read/write protocol

Shardkv must let any receiving Registry member perform CAS, reads and WATCH within the target shard's owner set without introducing a primary for every group/node. It must also preserve a monotonically consistent recordSet committed history across a member failure, joint membership, partial repair and tombstone collection.

The approach separates member sharding from data replication:

- `shardKey` determines only the owner set.
- `recordSet` is the replication unit for CAS, Rev, WATCH and commit certificates.
- Writes use a unique ballot `(round, writer_id)` and two-phase prepare/accept.
- Accepted state is initially invisible; only a quorum-chosen committed snapshot can enter the committed view.
- A committed snapshot carries a `CommitCertificate`, allowing later readers/writers to prove that a quorum decided that Rev even if they see only one up-to-date replica.

Shardkv distinguishes two collections within each shard view:

- `WriteSets`: sets allowed to prepare/accept/install. Active in stable mode; active+next in joint mode; active after cutover with old_grace.
- `ReadSets`: sets allowed for reads/snapshots/certificate validation. Active in stable mode; active+next in joint mode; active+old_grace after cutover.

A stable commit requires the current `WriteSets` quorum; a joint commit requires active quorum + next quorum. Old_grace participates only in reads and proof of the old head, not new-write commits.

```text
coordinator
   │ prepare(ballot)
   ├──────────────► owner A
   ├──────────────► owner B
   └──────────────► owner C
          quorum promise
   │ fetch committed snapshot from read members
   │ install current head on write owners
   │ accept(record, new_rev)
   ├──────────────► owner A
   ├──────────────► owner B
   └──────────────► owner C
          quorum accepted
   │ install(snapshot, commit_certificate)
   ├──────────────► owner A
   ├──────────────► owner B
   └──────────────► owner C
          quorum installed, laggards repaired best-effort
```

<a id="441-不变量"></a>
#### 4.4.1 Invariants

Shardkv correctness rests on these invariants:

1. **The replication unit is a recordSet**. Each `(namespace, shardKey, recordSet)` has one monotonically increasing `Rev` sequence. A commit changes at most one `recordKey`, but consumes the next `Rev` of the entire recordSet.
2. **Member sharding and data replication are separate**. `shardKey` determines the owner set; `recordSet` determines the Rev, CAS and WATCH domain. Different recordSets within a shard advance their Revs independently.
3. **A ballot applies to the entire recordSet**. `prepare` raises the recordSet-level promise. Concurrent writes to different keys are therefore ordered in one recordSet Rev sequence instead of independently allocating the same `Rev+1`.
4. **Accept is invisible**. On `accept`, an owner stores only a pending accepted record. It does not change committed records, trigger WATCH or expose the record to ordinary reads. Only an install with a commit certificate, or a quorum-verifiable snapshot, enters the committed view.
5. **Each owner holds a complete committed view**. A ready owner can serve local Snapshot/WATCH. A non-ready local view can participate in quorum protocols but cannot act as a complete local read source.

The system assumes crash/fail-stop Registry members that do not forge peer responses. Communication can time out, disconnect or duplicate requests, but request bodies are not Byzantine-tampered. `UpdatedAt` serves only TTL/GC; it does not order consistency.

<a id="442-提交证书"></a>
#### 4.4.2 Commit certificates

An accept quorum has already decided the value of a Rev. Keeping only in-memory accepted state would create an availability problem: if an accepting member then fails, the surviving quorum may see just one current replica and one older replica, making it impossible to recover the latest commit by requiring an identical snapshot from a quorum.

The coordinator therefore creates a `CommitCertificate` after accept quorum:

```text
CommitCertificate {
  rev      = committed recordSet Rev
  digest   = hash(rev + canonical live records)
  ballot   = accepted ballot
  labels   = membership labels whose owner set accepted quorum
  members  = accept quorum member ids
}
```

`canonical live records` includes only undeleted records. A delete still advances recordSet `Rev` and therefore changes the commit's `digest`; whether an owner locally retains an expired tombstone does not change the logical committed state at that Rev.

Install writes the complete committed snapshot and certificate to owners. A later quorum snapshot read can establish the committed head in either way:

```text
case A: same (rev,digest) snapshot is returned by quorum
case B: one snapshot carries valid certificate, and certificate.members proves one ReadSet quorum
```

Case B permits this sequence: m1/m2 commit while m3 is down; m1 subsequently fails, yet m2+m3 can continue writing. The certificate from m2 proves that the m1/m2 accept quorum decided the Rev, so the coordinator can repair the snapshot onto m3 and then allocate `Rev+1`.

`labels` identifies old heads during membership transitions. A certificate committed in stable V1 has `labels=[V1]`. On the first access to a cold recordSet after entering V1+V2 joint mode, V2 owners may not yet have that head. A snapshot certificate satisfying a V1-owner quorum still proves a committed head. The coordinator first installs/repairs it onto the joint owner set, then performs the next write. A new joint-phase certificate carries both V1/V2 labels because new writes require old quorum + new quorum.

After cutover to `active=V2,old_grace=V1`, V1 leaves `WriteSets` but remains in `ReadSets`. On V2's first access to a cold recordSet, a V1 certificate can still prove its old head; Registry repairs that head onto V2 write owners before continuing. Once V2 write owners hold a certified committed head, subsequent operations need only an online V2 write quorum, not an online V1 old_grace quorum as well. V1 certificates stop being accepted as proof for the current shard view only when `old_grace` is retired. Concurrent old-only and new-only writes that bypass the joint view remain forbidden throughout the transition.

If the coordinator fails after accept quorum but before install quorum, the next writer's prepare sees the pending accepted record. The new coordinator must complete that record under a new ballot and install its certificate before processing its own write. For callers, a CAS returning `ErrQuorum` at such a stage has an unknown outcome: reread/retry using `(recordKey, expectRev)` instead of assuming the write did not happen.

Install must also obey local monotonicity:

- A locally certified view cannot be overwritten by a lower `Rev`.
- At the same `Rev`, snapshots may replace each other only if their canonical digests match; this allows retained/compacted tombstone representations.
- Different canonical digests at the same `Rev` represent different committed histories and must be rejected so the caller can retry/report a conflict.
- An uncertified local view is only a cache awaiting repair and can be replaced by the quorum-chosen committed snapshot.

<a id="443-写正确性"></a>
#### 4.4.3 Write correctness

A successful CAS linearizes when the accept quorum decides that Rev. Returning success additionally requires an install quorum to have saved the committed snapshot/certificate, so the commit remains recoverable after another member fails.

Two different values cannot both commit at the same Rev because:

- Each write uses a unique ballot `(round, writer_id)`.
- Any two quorums in the same member set intersect. A joint view requires both old and new quorums, so it also intersects old-only/new-only operations. Membership transitions must not permit concurrent old-only and new-only writes that bypass the joint view.
- After prepare, an intersecting owner rejects accepts with a lower ballot.
- If that owner has a pending accepted record, a later writer with a higher ballot sees it in prepare and completes it first. A new write cannot skip an accepted `head+1`.
- An owner rejects an accept with `rec.rev > local_rev+1`, preventing the coordinator from skipping intermediate Revs.

The recordSet's committed history is therefore a linear sequence. CAS `expectRev` matches the target record's last-modified Rev. If other keys advance the recordSet Rev while the target key stays unchanged, those unrelated writes do not invalidate its `expectRev`.

<a id="444-读正确性"></a>
#### 4.4.4 Read correctness

Point reads use a quorum by default, without always fetching a full snapshot:

```text
read(key) from all ReadSet members
  ├─ if same record version is visible on WriteSet quorum: return it and repair write laggards
  ├─ if ReadSet quorum reports not found and no ReadSet member reports a record: return not found
  └─ otherwise fetch committed snapshot, choose committed head, install/repair, then read key
```

A record version can be returned only when that version itself is quorum-visible. If the key was modified/deleted at a higher Rev, the successful write installed its new version on a `WriteSets` quorum. The read's `ReadSets` intersects the current/old commit-certificate sets and cannot mistake an old version for a quorum-visible one. Not-found also needs a `ReadSets` quorum; during old_grace, empty active replicas alone cannot prove that an old recordSet is absent. A higher version on one replica or conflicting replicas requires fallback to committed-snapshot selection.

Snapshot/EnsureReady always chooses the committed head first, installs it locally and marks `(label, Rev)` ready. When the local recordSet view is ready, callers can use `ReadOptions`:

| Option | Behavior |
|---|---|
| `ReadDefault` | Quorum read |
| `ReadDefault + MinRev` | Read the ready local view directly if its Rev satisfies MinRev; otherwise fall back to quorum |
| `ReadLocal` | Read only the ready local view; return local-view-behind if not ready |
| `ReadLocal + MinRev` | Return local-view-behind if the ready local view's Rev is insufficient |

Only `EnsureReady` / `Snapshot` / `Watch` establishes a complete local view. Ordinary accept stores only pending accepted state. Read repair can install one committed record without marking that recordSet complete and ready.

<a id="445-watch-正确性"></a>
#### 4.4.5 WATCH correctness

WATCH uses only committed records:

- Pending accepts neither enter the watch log nor wake subscribers.
- Contiguous single-record commits append put/delete deltas to the watch log.
- Installing a snapshot that is not a single-record commit at local `rev+1` resets subscribers, requiring another full snapshot.
- The token contains epoch, membership label, recordSet identity and recordSet Rev. An epoch/label/recordSet mismatch or compacted log requires resubscription/reset.

WATCH is therefore an incremental cache of the committed view, not the replication protocol itself. Quorum read/CAS/install still provides replication and repair.

<a id="446-效率边界"></a>
#### 4.4.6 Efficiency boundaries

The current implementation prioritizes very many shards with small-to-medium recordSets:

| Operation | RPC rounds | Payload | Notes |
|---|---:|---|---|
| Point read of a quorum-visible record | 1 | O(1) record | Hot path; can also repair a lagging replica |
| Point read missing on the complete quorum | 1 | O(1) record | Direct not-found if no ReadSet member returns the key |
| Conflicting/lagging point read | read + snapshot/install | O(recordSet) snapshot | Establishes the committed head |
| CAS | prepare + snapshot + head install + accept + commit install | O(recordSet) snapshot/install | Cold path; owner calls run in parallel |
| Initial Snapshot/Watch | snapshot + install | O(recordSet) | Establishes a complete ready local view |
| WATCH delta | 0 additional RPCs | O(1) event | Broadcast only after local committed install |

With `N=3`, a stable write normally takes five sequential phases, with calls to owners parallelized within each phase (prepare, snapshot read, current-head install, accept and new-commit install). Reserve/create/build/import leases are cold paths, whose cost is acceptable relative to sandbox startup/build time; Router's hot data path does not write shardkv. Cost grows mainly with the records in one recordSet, not the global group/node count, because Registry does not scan across shards.

Design constraints:

- RecordSets should not hold unbounded tables. Group sandbox/build tables, node sandbox/build/key tables and the low-frequency node_list directory should remain pageable, evictable or reprojectable from their authority.
- If a future recordSet needs frequent large-table writes, use a delta certificate based on the preceding digest or split the recordSet; do not continue relying on full-snapshot install.
- Member readiness provides only fail-fast/liveness signals and does not change quorum calculation. With owner count 3, the runtime guarantee is one member failure from the logical shard's perspective.

<a id="45-watch"></a>
### 4.5 WATCH

WATCH operates at recordSet level. Initial frames provide reset/snapshot, followed by a bookmark marking completion of the initial view, then live deltas.

```text
watch(from token)
  -> reset(records, rev, token)
  -> bookmark(rev, token)
  -> put/delete(..., rev, token)
```

The watch token encodes local epoch, shard-view label, recordSet identity and Rev. A mismatched epoch/label/recordSet or compacted changelog requires resubscription for reset + full snapshot.

The route owner also uses the shardkv watch log to wake its own process's waiters. This is not a Router watch: Router does not subscribe to routes.

<a id="46-gc"></a>
### 4.6 GC

GC addresses local storage and WATCH reset-snapshot growth, not data migration. It must preserve two boundaries:

- Do not enumerate across shards.
- Do not change recordSet committed history.

A tombstone is retained after deletion for short-term `GetRecord`, WATCH reset and debugging. Logical reads/writes care only whether the key currently exists, so expired tombstones can be deleted locally. The commit certificate's canonical digest excludes deleted records: at the same Rev, an owner retaining tombstones and one that compacted them have equivalent committed snapshots, without a same-Rev conflict.

Compactor rules:

- Delete a local tombstone only after retention expires and the owner member is ready.
- Before compaction, repair the local view through recordSet committed-head selection so collection does not run on a lagging replica.
- Release expired idle local shards in non-pinned namespaces.

GC does not trigger migration. Active records are still reached through the namespace's authority and reads/writes to the same shard.

<a id="5-membership-变更"></a>
## 5. Membership changes

For one shard key:

```text
oldOwners   = LocateN(shardKey, V1)
newOwners   = LocateN(shardKey, V2)
jointOwners = oldOwners ∪ newOwners

write/read quorum = quorum(oldOwners) + quorum(newOwners)
repair            = best-effort to jointOwners
```

Example:

```text
V1 owners for group G: A,B,C
V2 owners for group G: B,D,E

joint write sets: A,B,C + B,D,E
commit requires: quorum(A,B,C) + quorum(B,D,E)
```

A simple majority of the union is unsafe because it may miss a committed value held only by an old quorum. An overlapping member can count toward both the old and the new quorum.

A membership transition is not a global migration job. Registry does not scan every group/node. Data reaches new owners naturally through these authorities:

- Active node connections, heartbeats and sandbox/build events continuously write to the current node_link owner set.
- Node owners continuously project low-frequency profiles to node_list under the current membership.
- Group requests, node reports and group-scoped list/export operations catch up/read-repair the route_link recordSet they access. This does not introduce operator export/import of execution state; §13 defines that boundary.
- Placer import/source leases, cursors and selector patches use the current `placer_link` owner set.

On first access to an old recordSet, its old membership certificate can prove the old head; that same operation then repairs it to current `WriteSets`. Joint/cutover therefore needs no cross-shard scan. After cutover, `old_grace` remains a read-only `ReadSets` source so empty active replicas cannot incorrectly declare a cold recordSet absent. Cutover must still be controlled: all new writes during active/next joint mode must use the joint view. If some members write using only next early, the protocol no longer guarantees intersection across membership quorums.

<a id="6-node_link"></a>
## 6. node_link

<a id="61-接入redirect-与-relay"></a>
### 6.1 Connection, redirect and relay

A node can connect to any Registry member. The receiving member first resolves its node owner set with `LocateN(node_id,N)`.

```text
node X connects registry A

hash(node X) -> node owners = B,C,D

case 1: A ∈ owners
node X ─────► A
              │ subscribe node stream
              ├── replicate profile/sandbox/build/key ─► C
              └── replicate profile/sandbox/build/key ─► D

case 2: A ∉ owners, node supports redirect
node X ─────► A
              │ hello{redirect:[B,C,D]}
              ▼
node X reconnects B/C/D

case 3: A ∉ owners, no redirect target
node X ─────► A ── relay ──► first successful owner in B,C,D
```

Relay must try owners sequentially in owner order; the first successful responder accepts the subscription. The node profile's `link_owner` identifies the Registry member actually holding the h2 stream. Route-owner create/connect/delete/build/key commands reach `link_owner` through node-owner RPC.

During `old_grace`, old members are excluded from owner sets but can remain `link_owner` and receive forwarded commands. When the node disconnects and reconnects, it resolves owners using current membership.

<a id="62-node-记录"></a>
### 6.2 Node records

Node_link maintains these recordSets:

- `profile`: node_id, labels, runtime_digest, api_endpoint, data_endpoint, Build registration/execution capacity and durable usage, draining, liveness and link_owner. Heartbeat `allocated` memory is the sum of all sandbox NodeReservations on that node; `pool` is the node allocatable pool, not host `memory.current`, VMM charge or guest demand. The low-frequency `node_list` projection includes both endpoints and capacity; usage stays in the node owner's live profile.
- `sandbox`: the complete per-node sandbox ownership map, `node_sandbox_id -> {sandbox_id,sandbox_generation,group,route_key,profile,api_secret_fingerprint}`.
- `build`: the complete per-node Build ownership map, `build_id -> group`.
- `key_pair`: the APISecret+ManifestKey pair cache refreshed by selector patches.

Heartbeats update only runtime/liveness fields in `profile`; they must not rewrite the `sandbox`, `build` or `key_pair` recordSets. The cluster writes sandbox/build ownership before dispatch. Registration usage counts only nonterminal Build rows and is released on the ready/error transition; an executed Build commits that terminal state and releases execution ownership only after exact host cleanup. Retaining terminal history does not retain admission usage. The ownership record stays until node terminal retention deletes that Build row and emits `BuildDelete`; Registry then exactly deletes the corresponding Build projection and owner ref. Selector patches update key_pair. Nodes do not generate group/route-key, but validate and separately persist sandbox system context delivered by node-link. A Build's cluster group is a separate system field of the node Build row, outside portable metadata. High-frequency heartbeats therefore do not slow unrelated recordSet CAS queues.

Within a node, `node_sandbox_id` ownership is written by CAS: replaying the same complete ownership is an idempotent refresh; different ownership returns a conflict without overwriting the old value. If create encounters that conflict before sending the node command, it rolls back only this attempt's RESERVED record, preserves stable `sandbox_id`, consumes the next `sandbox_generation`, generates a new `node_sandbox_id` and retries. The node synchronously rejects an existing or currently-being-created node-local ID before asynchronous launch.

Node_link processes streams by event importance:

- Route `upsert/delete`, `cmd_ack` and `BuildUpsert/BuildDelete` are essential convergence events and are processed immediately in the read loop.
- Heartbeats have latest-value semantics. The Registry read loop sends the latest heartbeat to one asynchronous coalescing updater per node; a slow updater may skip superseded heartbeats.
- Node-side transmission is also prioritized: command ACKs and Build deltas enter a high-priority outbox; only the latest heartbeat is retained.
- The writing side of `StreamAuthority` flushes route events first so heartbeats do not delay route READY/DEAD.

Reserve's READY report is therefore not blocked by heartbeat persistence. A node-local `starting` upsert exposes launch identity only to the node proxy/MMDS. The node_link owner includes it in the seen set during full sync, but does not add a route_link business state or overwrite existing RESERVED/PAUSED. Later READY/PAUSED/Delete advances route_link. If a failed create candidate's Delete still matches the current in-flight RESERVED fence, Registry uses that fence to restore the pre-Reserve route, or deletes the reservation for a new create. It must not delete RESERVED first and destroy the old route's rollback anchor. Sandbox events carry NodeSandboxID, profile and node-owned execution state. The nodelink owner looks up `(node_id,NodeSandboxID)` in that node's ownership map to recover stable SandboxID, SandboxGeneration and group/route_key, then updates route_link. A READY arriving after park timeout finds no ownership entry, is classified as orphaned, and triggers cleanup of the orphan sandbox on the node.

`deleting` belongs only to node-local durable cleanup and is not a sandbox upsert projection. Once the exact owner durably transitions to `deleting`, the node immediately excludes it from its local cache and subsequent full-sync route sets, and sends Delete on an existing live node-link to revoke the projection. That Delete does not prove completion of unit, network, RunDir/BaseDir cleanup or hard deletion. If the process exits between the durable transition and incremental publication, the old stream dies with it; the next full route snapshot withdraws the old projection because that SID is excluded. On restart, the node retries unfinished owners recorded in the durable `deleting` row. An allocation-fenced network tuple that was detached and exactly cleared is skipped, rather than reacquired because directory cleanup failed. Correctness does not depend on Registry still retaining a projection; finalizer completion sends no second route Delete.

The node returns CmdCreate `cmd_ack` only after claiming the unique launch attempt, inserting durable `starting,run_id=""`, and caching/publishing starting. The ACK means node-local launch acceptance, not READY. Subsequent resource preparation, runner or runtime failure drives reservation rollback through a matching Delete. CmdConnect completes cleanup of old runner/network/RunDir ownership before ACK; atomic paused→starting acceptance restores canonical RunDir/UDS and commits the deadline before publishing starting. Restore failure publishes a paused Upsert and must not enter the fresh-create Delete path. CmdDelete ACK means the node durably accepted `deleting`; it does not wait for the local finalizer. Pending replay is idempotent. Standalone and cluster Delete share one finalizer: node-link owns neither another cleanup implementation nor different path derivation. Route Delete is published before ACK to revoke the existing projection; terminal object observation is sent only after hard deletion.

High-frequency usage and liveness are not projected to node_list. That directory contains registration-time labels/capacity/endpoints/runtime and draining changes. The node owner's current node-link connection is the sole liveness authority. Before create/build dispatch, the route owner checks connectivity; failed candidates enter the current placement's exclusion set before reselection. Failure to persist the node_link profile rejects the subscription. A node_list projection failure does not disconnect node_link; later register/heartbeat/resync retries the pending directory projection.

<a id="63-增量订阅"></a>
### 6.3 Incremental subscriptions

When starting a subscription, the node owner can supply an opaque revision string. The current format is `source_fingerprint:seq`, parsed privately by the node; callers must treat the token as opaque. Matching fingerprints and available changelog history permit incremental replay; otherwise a full resync occurs. A randomly staggered 1–6 hour full-resync cycle was a design recommendation, not an implemented periodic timer. Current full sync is driven by connection/resume-token and replay-window conditions.

Before full subscription starts, the nodelink owner captures that node's sandbox-ownership baseline. At the completion bookmark, it cleans only baseline entries absent by node_sandbox_id in this snapshot. Before cleanup, it rereads and verifies that the complete current stable/node-generation ownership still matches the baseline, protecting work newly dispatched or rebound during synchronization. An incremental-replay bookmark only advances the resume token and does not clean missing entries.

Build projections do not use the route replay window. Every node-link session requires `BuildSyncBegin → BuildUpsert* → BuildSyncEnd`. The snapshot contains all cluster Builds still retained in node SQLite: registered/waiting/building and ready/error within retention. The node subscribes to live deltas before ranging the snapshot, so Upsert/Delete during synchronization follows End without being lost. The nodelink owner likewise cleans only post-registration baseline refs captured before connection establishment, still bound to the exact `(NodeID, BuildID)` at End, and absent from this snapshot. A Registry-owned `BuildStarting` ambiguous dispatch intent is not a node projection and cannot be deleted by an empty snapshot. Reconnection repairs a lost live Delete, while newly registered/rebound work remains protected from an older snapshot. Missing Begin/End, duplicate brackets or Bookmark before End fails closed. Registry has no independent Build terminal TTL.

<a id="7-node_list"></a>
## 7. node_list

Node_list is a special namespace with a fixed shard key:

```text
namespace = node_list
shardKey  = node_list
recordSet = nodes
recordKey = node_id
```

```text
node_link owner
  │ labels/capacity/endpoints/runtime/draining low-frequency projection
  ▼
node_list owner set: LocateN("node_list", M)
  ┌─────────────┬─────────────┬─────────────┐
  ▼             ▼             ▼
 owner A       owner B       owner C
 full view     full view     full view
  ▲
  │ WATCH_LIST
  ▼
placer consumes one owner at a time
```

The node_list owner shard is fully replicated. Placer neither needs nor is allowed to merge results from multiple owners as if they were different shards. If the current owner disconnects, placer clears that source view and switches to another owner for a new reset + bookmark.

When node_list is not ready, it may list + repair only from the same node_list owner set. It cannot scan across node shards or create a second authoritative propagation path by rebuilding from node_link.

<a id="8-route_link"></a>
## 8. route_link

<a id="81-身份"></a>
### 8.1 Identity

- Route lookup key: `(group, route_key)`.
- Stable public identity: `sandbox_id`. Registry generates it on first create; same-node resume, cross-node migration and re-placement do not change it. Public APIs, Host and Router cache keys use this ID.
- Stable sandbox identity: `stable_id`. The cluster invariant fixes `stable_id == sandbox_id`; it survives NodeSandboxID changes and binds ServiceSecret and KAT `sid`. This duplicate projection is existing protected credential state and is not deduplicated here. StableID is not a node-local lookup key and has no additional unique index; identity-preserving migration/copy can give multiple node-local sandboxes the same StableID.
- Registry durable JSON state uses `stable_id` directly, without a legacy-field fallback. Pre-release deployments must clear and rebuild old preview state.
- Node execution identity: `node_sandbox_id = <sandbox_id>-g<sandbox_generation>`. The first candidate is g0; candidate conflict/failure or cross-node migration consumes the next generation. Same-node resume keeps the current NodeSandboxID. NodeSandboxID is an opaque node-local ID; Registry ownership maps are authoritative, and components do not reverse-parse the string.
- Node events carry no group/route_key/SandboxGeneration. Registry looks up `(node_id,node_sandbox_id)` to recover stable identity and group context. There is no cross-group SandboxID index.

<a id="82-route-记录"></a>
### 8.2 Route record

| Field | Meaning |
|---|---|
| `group` | Shard key |
| `route_key` | Route lookup key within the group |
| `sandbox_id` | Stable public SandboxID |
| `node_sandbox_id` | Current node-local execution ID |
| `sandbox_generation` | Registry-owned generation of the current NodeSandboxID |
| `next_sandbox_generation` | Next allocatable generation; persisted only by Registry, never sent to the node |
| `state` | `reserved` / `ready` / `paused` / `dead` |
| `node_id` | Current hosting node |
| `profile` | Sandbox profile fixed by create intent, consistent with node ownership and event facts |
| `api_secret_fingerprint` | Complete APISecret fingerprint bound to the sandbox business record; lifecycle commands/ownership cleanup use it to prevent operations across bindings |
| `manifest_key_fingerprint` | Complete fingerprint of the paired ManifestKey; routes neither store nor project raw ManifestKey |
| `stable_id` | Sandbox identity retained across NodeSandboxID changes; equals `sandbox_id` in the cluster and binds ServiceSecret/KAT |
| `api_secret` | APISecret currently bound to the sandbox; exists only in protected route storage and trusted router/proxy projections |
| `service_secret` | Persisted per-sandbox service credential for signing/verifying purpose-specific KAT tokens |
| `envd_access_token` | Data-plane token for the e2b envd port |
| `traffic_access_token` | Token used by external gateways and e2b data-plane components; not consumed by the cluster platform layer |
| `forward_access_token` | Data-plane token for other bare/e2b forwarding targets |
| `target_port` | Mandatory data-plane port returned by group/provider; if zero, the request must explicitly specify a port |
| `updated_at` | Used by timeout/reconciliation |

State machine:

```text
none -> reserved -> ready
ready -> paused -> reserved -> ready
ready/paused/reserved -> dead/tombstone
```

An actually emptied node, a killed sandbox, or a sandbox missing after restart converges through dead-route cleanup. A later explicit create/recovery Reserve can place it again. A host/conductor restart alone does not mean every durable paused or recovering sandbox is missing; the node's reconciled report and exact ownership determine cleanup.

<a id="83-reserve"></a>
### 8.3 Reserve

```text
POST /route-link/reserve
  ?operation=create|connect|exec-session|data
  &group=<group>&route_key=<route-key>
  [&sid=<stable-sandbox-id>][&port=<effective-port>]
  [&timeout=<seconds>]
```

Each operation has its own typed Reserve body: create accepts `{"config":{...},"auto_pause_memory":true|false}` with optional fields (at most 16 MiB); connect accepts an optional `{"memory":true|false}` (at most 64 KiB), preserving absent/null selection; exec-session carries `{"ttl_seconds":N,"conditions":["..."]}`; data has an empty body. Credentials and completion conditions differ among the four operations. Registry strictly validates exec-session schema/bounds again; it accepts neither the legacy `ttl_seconds` query nor conditions hidden in headers/metadata/config maps:

- `create`: query carries only group/route_key, header carries `X-API-KEY`, and the config map permits only `kuasar-sandbox.restore`, `kuasar-sandbox.credentials` and `kuasar-sandbox.checkpoint`; optional `auto_pause_memory` is a separate typed body field, and `memory` is rejected. Before placement or route writes, Registry validates the API key through the group provider, generates stable SandboxID and the first NodeSandboxID, and sends CmdCreate. Node ACK means only durable starting + active attempt. Registry still waits for the node READY event before returning `Route` to northbound create. Concurrent creates are coalesced inside Registry.
- `connect`: query must carry expected stable `sid`, with optional `timeout`; headers carry `X-API-KEY` and optional `X-Kuasar-Migration-Token`. The body accepts only the optional memory selector (no config/auto_pause_memory). Registry validates the API key with the APISecret bound to the route business record and preserves that selector in CmdConnect to the exact NodeSandboxID. If that node is unavailable and a migration token is supplied, Registry excludes it, allocates a new generation and sends CmdConnect to a new node. The node synchronously validates, optionally imports, cleans old runner/network/RunDir ownership, restores canonical RunDir/UDS during paused→starting acceptance, persists the deadline and reads credentials. ACK returns typed `ConnectResult`. Registry checks its NodeSandboxID/TemplateID/Profile/three public tokens against the route, then returns `Route + Connect`. Resume is asynchronous; connect does not wait for READY, and Router does not coalesce different requests.
- `exec-session`: query must carry expected stable `sid`; the typed body carries nonnegative int64 `ttl_seconds` and a normalized array of CEL source strings. Headers carry the original `X-API-KEY` and optional `X-Kuasar-Migration-Token`. Registry validates the API key with the route business record's bound APISecret and checks expected stable SID. The raw key terminates at the Registry verifier; it does not enter commands, ACKs, routes, logs or error text. Registry sends `CmdExecSession{APISecretFingerprint,Profile,TTLSeconds,ExecConditions,MigrationToken?,Cluster?}` to the current/new candidate NodeSandboxID. Any other command kind carrying `ExecConditions` must be rejected. The node synchronously performs optional import, object/credential/context validation, authoritative CEL compilation and KAT signing. ACK carries only `ExecSessionResult{ExecAccessToken}`, followed by asynchronous resume under the existing contract. Compilation/minting failure precedes any resume mutation. Conditions exist only in this public request, Reserve body, Command and token; they are never persisted in Route, SandboxRecord, events or metadata, and their source is not logged. MigrationToken likewise lives only in this Reserve/command wire exchange, is never stored in route/SandboxRecord or included in logs/errors, and is discarded after synchronous import. Registry validates the typed result, rereads the current route and returns `Route + ExecSession` without waiting for READY. Router exposes only `execAccessToken` to the client. Token issuance on a READY/RESERVED route neither rewrites its revision nor takes another workflow's rollback fence; only PAUSED transitions to RESERVED under existing activation semantics. Every API call uses its own CmdID and mints a fresh KAT; exec-session Reserve calls are not coalesced.
- `data`: query must carry expected stable `sid`; a legacy target may supply an effective `port`, which Registry combines with route `target_port` for authentication. The logical exec service does not require a port. Headers carry `X-Access-Token`, and exec additionally carries `E2b-Sandbox-Service: exec`. For ordinary targets, Registry selects EnvdAccessToken or ForwardAccessToken by profile/port; exec validates a KAT with `StableID + ServiceSecret`. Authentication precedes any route CAS/CmdConnect. READY returns directly; PAUSED first CASes to RESERVED and sends CmdConnect; RESERVED waits for events in the current stable lineage. `Route` returns only after READY and a valid DataEndpoint. A nonexistent route returns not-found without creating a sandbox.

`Route` is a protected result containing stable SandboxID, current NodeSandboxID, `APIEndpoint`, `DataEndpoint` and `route_revision`. Both endpoints come from the current node runtime/profile looked up by NodeID, rather than being copied into SandboxRecord. Control/build always uses APIEndpoint; data/exec always uses DataEndpoint. Router→node currently uses plaintext HTTP/CONNECT, so each value must point to its reachable plaintext internal listener. External TLS terminates at Router; node-link mTLS is separate. `route_revision` is the current committed revision of the group route recordSet, allowing Router to reject late results for an old node. Router does not subscribe to route_link updates.

CmdConnect/CmdExecSession `CmdID` correlates only the current Command and ACK waiter; it is not a durable idempotency key. Registry does not automatically redeliver the same Command/`CmdID` after ACK timeout, link loss or node restart. The call returns a temporary failure, and an API retry creates a new operation/CmdID. Connect remains retryable through insert-only targets, object-binding validation and node-local launch ownership; an Exec Session retry can issue another KAT. The system persists no command digest, ACK/result or temporary deduplication state.

The nodelink owner and route owner jointly converge orphan cleanup. They first look up `(node_id,node_sandbox_id)`. If the ownership entry is absent, or its `(group,route_key)` is gone/replaced by another instance, they send delete/kill to that node. This does not traverse the data plane or depend on access tokens or global sandbox-ID lookup. Full sync, orphan cleanup and node reclamation compare complete `(group,route_key,sandbox_id,node_sandbox_id,sandbox_generation,profile,api_secret_fingerprint)` ownership so late events cannot delete new records across generations or credential bindings.

<a id="9-placer_link-与-placer"></a>
## 9. placer_link and placer

<a id="91-placer-发现"></a>
### 9.1 Placer discovery

```text
placer S1
  │ POST /placer-link/register {id, advertise, memberlist_label}
  ▼
registry observer joins placer.default memberlist
  │
  ▼
ready placer view from memberlist meta
```

Registry uses deterministic failover by group:

```text
readyPlacers = placer memberlist nodes where role=placer and alive and ready=true
               and ready_label == active_registry_label
candidates   = LocateN(group, readyPlacers, placer_link.placer_replica_count)
try candidates in order until success
```

Registry does not apply P2C to placers. P2C belongs inside placer, where it chooses a target from node candidates.

<a id="92-importsource"></a>
### 9.2 Import/source

```text
source_id = file-prod-a

ready placers ── LocateN(source_id, readyPlacers, import_source_owner_count)
       │
       ▼
candidates race CAS lease:
  namespace = placer_link
  shardKey  = import/source/file-prod-a
  recordSet = import
  recordKey = state

lease winner
  │ Range(cursor, limit)
  │ GetPlacementHint/GetKey/GetAPISecret(group)
  │ selector patch with lease fencing
  │ cursor checkpoint after page success
  ▼
node_link key_pair cache refreshed
```

`source_id` is the importer's sole execution unit. Placers configured with the same `source_id` compete for one `placer_link` source-execution record. Provider point lookups may deduplicate across sources; an Importer Range does not combine multiple sources into one view.

<a id="93-group-provider-边界"></a>
### 9.3 Group provider boundary

Registry does not implement `SandboxGroupProvider` / `SandboxGroupImporter`. Group configuration, placement hints, APISecret and ManifestKey belong to placer/provider. Registry retains only execution state and the credential-pair cache needed by each node.

Once a group disappears from its provider, new Place/verify-key calls treat it as nonexistent. Credential pairs already cached in node_link are not proactively deleted; Registry/node TTLs evict them. Credential pairs already copied into existing sandbox/build records are unaffected.

<a id="10-router"></a>
## 10. Router

Router is a stateless northbound entry with local caches:

- Route-resolution cache: `(group, route_key, stable sandbox_id)` → NodeSandboxID / APIEndpoint / DataEndpoint / profile / StableID / APISecret / both root fingerprints / ServiceSecret / EnvdAccessToken / TrafficAccessToken / ForwardAccessToken / RouteRevision.
- Build-forwarding cache: `(group, build_id)` → APIEndpoint; build_id alone is insufficient.
- In-flight requests hold only their own route copy and counters. They neither provide a routing cache for new requests nor prevent a new RouteRevision from replacing an old NodeSandboxID.

```text
request(group, route_key, sandbox_id)
  │
  ├─ cache hit with node target ───► node proxy (ready/paused/starting)
  ├─ cache hit without target ─────► data Reserve ──► node proxy
  │
  └─ miss ─────────────────────────► route owner Resolve
                                      │
                                      ├─ complete target ───► node proxy
                                      └─ missing target ────► Reserve ──► node proxy
```

When a node proxy returns typed `not_found`/`unauthorized` during the CONNECT handshake, Router evicts the old target, calls `ReserveData` once with the same credential to revalidate/refresh the route, then retries only once. Ordinary connection failures only evict the cache entry.

Data requests for unknown routes return not-found after Resolve and do not create sandboxes. Once a route is found, Router translates public SandboxID to NodeSandboxID at the node boundary: control rewrites the path and dials only APIEndpoint; outer data-plane CONNECT, Host and existing sandbox-identity headers use NodeSandboxID and dial only DataEndpoint. Either missing endpoint fails closed, with no cross-plane fallback. Public create/connect/list/get results continue to expose only stable SandboxID.

A newly received route cannot overwrite the cache with a lower RouteRevision or replace NodeSandboxID at an equal RouteRevision. An old-generation in-flight failure may evict only if the cache still points to that NodeSandboxID, protecting a newer generation already installed there.

All requests must carry group. Router bootstraps `/cluster/membership` and resolves the route owner using active membership. Membership refresh tries bootstrap and known active/next/old_grace members, selecting the response with the newest active version.

Reserve always validates the client's raw API key for create/connect/exec-session. Create's group admission uses provider APISecret through a ready placer; connect/exec-session uses the APISecret bound to the sandbox business record. For other control operations, Router asks the route owner's verify-key endpoint, which fails over only among ready placers. Once the sandbox is READY, its node projects the business record's bound APISecret, ServiceSecret and purpose-specific access tokens into the protected route for trusted Registry/router/proxy use. Raw ManifestKey does not enter this path and is not used for API authentication.

Cluster native exec control does not reverse-proxy the public node API to the current node:

```text
POST /sandboxes/<stableSID>/exec-sessions
  X-Kuasar-Sandbox-Group + X-Kuasar-Route-Key + X-API-KEY
  empty / {} / {"ttlSeconds":N,"conditions":[{"expr":"..."}]} + optional MigrationToken
    ↓ Router strict 64 KiB decode
Reserve(operation=exec-session, expected stableSID, typed TTL + conditions body)
    ↓ Registry verifies API key and sends CmdExecSession
Node validates/imports, compiles conditions, signs, accepts async resume
    ↓ Route + ExecSessionResult
201 Cache-Control:no-store {"execAccessToken":"kat1..."}
```

Router neither signs tokens, sends raw API keys through node-link, nor exposes NodeSandboxID. Its body shares the direct Node endpoint's strict decoder: at most 64 KiB for the entire raw body, with an empty body allowed. Missing `conditions` and `[]` normalize to unrestricted nil. Explicit `null`, unknown/duplicate fields, an empty expr, negative values, a trailing second JSON value and out-of-bounds TTL are rejected. Internal Registry/node failures map to fixed, sanitized exec-session errors without NodeSandboxID, socket path, fingerprint, ServiceSecret or token payload.

The data plane interprets `E2b-Sandbox-Service` for backend selection only on CONNECT. Ordinary HTTP uses legacy-port forwarding and preserves the application header, except that exact `service=exec` is rejected with 405 before activation. Without a CONNECT service, raw-port behavior remains. The node selects the final backend using its locally trusted profile. In particular, bare legacy 49983/49999 are ordinary TCP forwarding targets and do not return 501. Cluster Router currently has a service-aware branch only for `service=exec`; the other three explicit service values are not claimed as supported. [#63](https://github.com/kuasar-sandbox/orchestrator/issues/63) tracks the complete cluster passthrough boundary.

`service=exec` accepts only CONNECT and always enforces KAT. Router first performs a side-effect-free Resolve/cache lookup, validates the original `X-Access-Token` with `StableID + ServiceSecret`, and compiles conditions only after successful HMAC verification. After public CONNECT 200, Router strictly reads the first ExecRequest, rechecks expiry and evaluates conditions. Failure neither calls `Reserve(operation=data)` nor connects to the node. After successful request admission, a complete node target connects directly to the node proxy; a missing target calls `Reserve(operation=data)` and revalidates stable lineage/credentials against the fresh route. It then constructs the second-hop CONNECT:

```text
E2b-Sandbox-Id:      <current NodeSandboxID>
E2b-Sandbox-Service: exec
E2b-Sandbox-Port:    <original port, if present>
X-Access-Token:      <same KAT>
```

Only stable SID is rewritten to NodeSandboxID between hops. Service/port/token values and the token header stay unchanged; Router sends the first frame's Raw bytes unchanged exactly once. The final node does not trust Router: it validates the same KAT against its local route, rereads and evaluates the complete ExecRequest gate, and only then permits parking, resume and connection to `ctl.sock`. A complete-target cache hot path adds no Registry RPC, while retaining both Router and node validation. KAT binds StableID, not NodeSandboxID/generation, so same-node resume, cross-node migration or re-placement of one logical sandbox needs no client re-signing; a new CONNECT always targets current NodeSandboxID. Typed stale retry is allowed only before Raw has been written to any node. After node CONNECT 200 and Raw transmission, retry/reroute/replay is forbidden and ctl errors are relayed unchanged.

Ordinary non-exec traffic follows each hop's own effective authentication policy. Router `log`/`off` can admit invalid ordinary credentials; enforcing Router policy does not alter node policy. Preventing invalid ordinary credentials from parking, waking or reaching a backend at the final node requires that node's effective `enforce` policy. Native exec always enforces both token and request gates regardless of ordinary policy.

<a id="11-密钥与鉴权"></a>
## 11. Keys and authentication

A group/tenant has two root credential domains with separate purposes:

| Name | Holders | Purpose |
|---|---|---|
| `APISecret` | provider/placer/node/registry/router/proxy | Signs/verifies API keys and later purpose-specific authentication material; its complete SHA-256 fingerprint identifies the credential pair |
| `ManifestKey` | provider/placer/node | Decrypts manifest/image/snapshot contents and wraps pull tokens; Router never receives it |

Both are 32 bytes / 64 lowercase hex characters. If APISecret is omitted, placer derives it while materializing inline ManifestKey using the fixed KDF:

```text
APISecret = HMAC-SHA256(decodeHex(ManifestKey), "kuasar-api-secret-v1")
```

Only APISecret signs/verifies API keys. The API key's `SHA256(APISecret)[:12]` is only a candidate prefilter; node-link, lifecycle commands and business records use full 64-hex `SHA256(APISecret)`. ManifestKey has its own full 64-hex SHA-256 fingerprint to validate the paired content key.

APISecret and ManifestKey are typed secrets supporting inline or out-of-band ref delivery. A selector patch either carries a complete pair—both typed carriers and both complete fingerprints—or no credential fields at all. Half-pairs must be rejected. Credential distribution occurs only through shuffle-sharding/import/selector-patch paths:

```text
placer selector patch
  │ target node set + APISecret/ManifestKey material/ref
  ▼
registry/node owner
  │ CAS node_link key_pair cache (record key = APISecretFingerprint)
  ▼
node_link heartbeat refresh
  │ key_put complete pair only when missing or near lease expiry
  ▼
node encrypted local key store
```

The node atomically validates and installs the complete pair on `key_put`. One complete APISecret fingerprint cannot bind different pair material. `key_drop` locates a pair by full APISecret fingerprint, but correctness does not depend on it. Node leases evict entries not renewed before TTL. Distribution is a prerequisite for create/build; drop, TTL and provider updates do not alter pairs already copied into existing sandbox/build business records.

ServiceSecret is not a third group root; it is an independent service credential for each node Sandbox business record. Its default derives from that Sandbox's bound APISecret and `StableID()` using a fixed domain, or the current create's `kuasar-sandbox.credentials` object can explicitly supply it. Before route/ref/command side effects, Registry validates and normalizes that object against placement Profile. The node validates and separates it again, then encrypts ServiceSecret and Envd/Traffic/Forward tokens into the Sandbox business row. Ordinary metadata, guest configuration and node-stub observation retain no credentials object.

ForwardAccessToken and ExecAccessToken both use `kat1`, with separate audiences. ExecAccessToken's canonical payload fields are `v,session_id,sid,aud[,exp][,conditions]`: session ID is UUIDv7, `sid=StableID`, `aud=exec`, and there is no `iat`; normalized CEL source strings appear only when restricted conditions were requested. ServiceSecret is decoded to a 32-byte key and used directly for HMAC-SHA256, with no extra exec-specific derivation. Create/get/list does not return a default ExecAccessToken. Every exec-session API call signs independently, without a server-side session row, revocation or single-use/replay state.

When Registry materializes a protected route from a node route event, it accepts APISecret, both root fingerprints, StableID, ServiceSecret and Envd/Traffic/Forward tokens. It does not accept raw ManifestKey or the existing node-link wire's MmdsSecret intended for node proxy/MMDS. Registry currently stores protected route credentials as plaintext structured fields without an extra encryption layer. They may be returned only by protected Reserve/Resolve to trusted routers, never by ordinary route watch/list, public create/get/list, logs or observation interfaces. Reserve separates `kuasar-sandbox.credentials` from ordinary config at entry, freezes it only with the RESERVED record, temporarily encodes it before CmdCreate, and clears it after READY.

<a id="12-build"></a>
## 12. Build

Build records live in the group-scoped `route_link` `build` recordSet. The node owns durable registration/execution claims and usage; its node-link owner holds the current connectivity/profile projection for Registry checks.

```text
router build register
  │ stable build_id/template_id + explicit profile
  ▼
route owner ReserveBuild → PlaceBuild
  │ placer filters configured capacity and suggests a node
  ▼
node owner: live connection + current registration usage check
  │
  ▼
route_link BuildStarting intent CAS + exact node_link build ref
  │
  ▼
node_link build_register → node durable registration admission
  │ ACK accepted: retain exact target; definitive rejection: clean/reselect
  │ ambiguous dispatch: keep intent pinned to this node/BuildID
  ▼
node BuildUpsert projects state; heartbeat projects durable usage
  │ exact execution cleanup precedes terminal commit / execution release
  │ ready/error no longer counts toward registration; history remains
  ▼
node TTL deletes history; BuildDelete removes projection/ref
```

`ReserveBuild` returns the current node's `APIEndpoint`. Router caches and dials only that address for later build status/trigger/files/log control HTTP. The result retains no old `DataEndpoint` alias, and BuildRecord does not copy endpoints.

Northbound `/v3/templates` resolves omitted profile to `e2b` according to that endpoint's semantics. Internally, profile must be explicit. `X-Kuasar-Sandbox-Builder.target` is a registration-only immutable definition: Image, top-level Sandbox E and memory Sandbox targets travel with canonical builder JSON, Registry replay identity, `build_register` and the node Build row. When omitted, only the worker resolves it automatically from effective start/ready. The route owner persists profile, target/config identity and placement's `APISecretFingerprint` into BuildRecord and passes them through `build_register`. The node uses the full fingerprint to copy the same credential pair into the local Build and writes profile into BuildSpec. Missing/invalid values are rejected, never silently rewritten. Bare builds can choose all three targets but still cannot use e2b-only start/ready commands.

Build Register and ordinary Create share strict header/metadata normalization for resource/network/launch/init/mounts/files/metadata, and also pass `envVars`. Registry's replicated BuildRecord retains non-secret config/env/secure plus irreversible digests of credentials/MMDS values. Raw credentials and initial MMDS values live only in the current dispatch envelope; after validation, the node encrypts them into task-local Build state. Router, Registry, node and worker must each fail closed on instance-only inputs for explicit Image/top-level Sandbox E, unconditional rejection of restore, and target/output-ref mismatches. Filtering by one hop does not replace another hop's trust boundary.

Placer filters low-frequency configured capacity. Registry validates the node's live connection and current registration-usage headroom, then persists immutable `BuildStarting` dispatch intent and the `(node_id,build_id)` ownership ref before `build_register`. The node's durable registration transaction is the sole authoritative admission decision; Registry has no `AdmitBuild` lease or terminal `ReleaseBuild` RPC. Insufficient known headroom or a definitive admission rejection permits exclusion/reselection after exact intent cleanup. Timeout, reset, disconnect, ACK loss or an invalid/uncertain acceptance leaves the operation bound to the selected node and BuildID. A retry must replay the identical stored definition and credential/MMDS digests to that same target; it cannot silently move an ambiguous registration to another node. Equal build_id values on different nodes remain distinct node-local bindings.

BuildUpsert/Delete carries only a node-local projection. The nodelink owner obtains group from the immutable `(node_id,build_id)` ref. Terminal Upsert updates the query projection; durable node heartbeat reports claim usage. After exact execution-owner cleanup, the terminal commit releases execution ownership and the row no longer counts toward registration usage. Node TTL deletion later removes the retained history and sends `BuildDelete`, allowing Registry to remove the exact projection/ref. See the [durable usage query](../internal/store/build_admission.go) and [registration transaction](../internal/store/mmds_route_secret_values.go). Northbound queries and Router caches always include group. Every node Build durable transition and its corresponding live publication is serialized under an unconditional per-Build fence, independent of conductor Extension enablement, so waiting cannot arrive after building/terminal and overwrite Registry's projection.

Ready/error status, transient TemplateID, name/alias and local/Registry Build lists remain available only within the node's `builder.terminal_ttl` retention window. A canonical TemplateID self-encodes profile, kind and portable artifact ref, so it can be reused for img/sbx/snp Create after the Build row is deleted. Create does not recover IMG config from old Build projections or metadata; canonical `fromTemplate` likewise needs no old projection. Registry has no separate timer and does not extend node retention. During overlapping connections, Registry accepts Build frames only from the current active node-link session. Session replacement and Build store mutation share one NodeID fence so an old connection cannot delete a new generation's same-ID Build. During reconnect snapshot, the node still interleaves bounded command ACKs/heartbeats through the same stream writer. Live Build changes stay buffered in their independent subscription until after `build_sync_end`.

<a id="13-状态所有权与灾备边界"></a>
## 13. State ownership and disaster-recovery boundaries

The node is the authority for sandbox/build execution state. Sandbox/build records in `route_link`, reverse ownership in `node_link`, and admission-related projections derive from node facts for routing, queries or scheduling; operator files cannot create or transfer them. A terminal Build remains a projection of one node execution. Its produced template/manifest is durably reusable; the BuildRecord is not.

Registry's normal control plane offers no execution-state import/export. In particular, it must not import existing SID, NodeID, READY/PAUSED state, Build execution, node refs, admission or local checkpoints. Nodes persist separate system context for cluster sandboxes, but current node-link full reports do not return Registry-owned identity. If an execution shard is completely lost, route events alone cannot rebuild ownership, and preinstalling ownership to make old events appear legitimate is forbidden. The explicit recovery/bootstrap protocol is not yet implemented and is tracked by [#34](https://github.com/kuasar-sandbox/orchestrator/issues/34); manually writing execution rows must not bypass that limitation.

The disaster-recovery object for a portable paused sandbox is an **unbound durable route** with a migration token, not SandboxRecord. It contains no old SID/NodeID/admission. Recovery performs fresh placement; the target node validates the remote snapshot and creates a new identity, and READY establishes the runtime route. [#33](https://github.com/kuasar-sandbox/orchestrator/issues/33) tracks this recovery-only workflow, outside normal route_link APIs.

Manifest Bundle does not change migration-token/KMT wire format: the token still carries only the root Bundle ref. Target-node task-local preflight reads only the root Bundle's contiguous metadata prefix, gathers located sources from flat `bundle/refs`, and deterministically derives the complete mapping using `checkpoint.remote.ref_location_parent`. Same-directory siblings need no mapping. Orchestration does not open or recursively scan referenced Bundles. Export/promote still delegates to sandboxer, publishing the root Bundle and its unlocated siblings unchanged while retaining the addresses of externally located dependencies.

Sandbox-group configuration, placement hints, APISecret and ManifestKey remain the placer/provider's responsibility for persistence and disaster recovery. Before #33/#34 is implemented, complete Registry execution-state loss has no operator runtime-import fallback. It must be reported as unrecoverable through the current protocol, instead of constructing route/build ownership that could conflict with nodes.

<a id="14-可靠性"></a>
## 14. Reliability

| Event | Behavior |
|---|---|
| Router crash | Local cache is lost; after restart, a miss resolves again |
| Placer crash | Registry fails over to the next ready placer for that group; the hot path is unaffected |
| node_link disconnect | Node owner immediately rejects new submissions to that node; route owner excludes/reselects it. After node_dead_after, node profile/node_list and associated execution projections are cleaned. Reconnection reports full/incremental state |
| Conductor process restart | Reconciles durable rows against systemd units: adopts live sandboxes/builders, retries deleting/paused cleanup, resumes already accepted interrupted restores, and cleans/fails missing fresh executions. This is not whole-host reboot |
| Node host reboot | Running processes are gone. If node SQLite/artifacts survive, reconciliation still processes paused rows, accepted interrupted restores, cleanup owners and retained Builds. Missing executions converge through node reports/exact route cleanup; no blanket durable-row deletion or automatic recreation of every old sandbox |
| One Registry member fails | Service continues while the owner-set quorum is available; access to the same shard or authority reports read-repair recovered members. A single-owner deployment cannot tolerate losing its owner |
| Multiple Registry failures lose quorum | Affected shards stop writes, without unsafe degraded writes |
| Registry execution shard is completely lost while nodes survive | Do not import backup execution rows. Explicit recovery/bootstrap is needed to rebuild projections from node facts (#34; currently unimplemented) |
| Membership change | active/next joint write quorums + old_grace read-only certificates/snapshots |
| Whole-cluster power loss | Running processes are lost. Surviving node durable rows/checkpoints can be reconciled only with intact storage and required Registry ownership; this is not a guaranteed cluster recovery protocol. If Registry execution state is lost, provider data, portable templates/manifests and future migration-token routes (#33) are the disaster-recovery inputs, not an execution-row import |

<a id="15-性能"></a>
## 15. Performance

- Hot data path: a complete node-target cache hit forwards directly to the node, including paused/starting, without Registry access.
- Cold Place path: group → route_link owner → ready-placer failover → node-owner connectivity/current-usage check → authoritative node admission. Definitively failed candidates are excluded before reselection; ambiguous Build dispatch stays pinned.
- Very many groups: Router does not subscribe to groups; Registry does not scan across them; placer import pages independently by source_id.
- Very many nodes: node_link shards by node_id; node_list holds only a low-frequency directory, not high-frequency usage.
- Membership changes: no global promoter; node reports, group requests, source imports and read repair drive convergence.

<a id="16-集群-stub-e2e"></a>
## 16. Cluster stub e2e

`node-stub-ctl` is this repository's cluster e2e node stub. It connects to Registry using real node_link; one process can simulate multiple nodes without starting microVMs. Each process has distinct admin, API and Data listeners. Ready JSON returns `admin`, `api`, `data`, and each node registers APIEndpoint and DataEndpoint. It simulates node control behavior except microVM/application execution:

- Node registration, heartbeats, draining, usage and Build budgets.
- Receiving `key_put/key_drop/create/connect/exec_session/delete/build_register` commands and returning ACKs.
- Publishing READY/dead route events according to configured sandbox behavior.
- Publishing full Build snapshots, BuildUpsert and BuildDelete; an empty reconnect snapshot can converge a lost Delete.
- The admin listener offers only node-stub-ctl management queries; the API listener simulates conductor control APIs; the Data listener simulates ordinary data, CONNECT and exec.
- Node actions including `restart-link`, `reboot-empty` and `crash/start`.

`make test-e2e` does not build binaries: it passes `E2E_BIN` (defaulting to the sibling project repository’s assembled binary directory) to `test/e2e/run_all.sh` and requires that multi-repository artifact set beforehand. For the local stub-only flow, run `make build` followed by `make test-e2e-cluster-stub`; this target uses the local `BINDIR` to start real `cluster-ctl registry/router/placer` and `node-stub-ctl`. See [Makefile](../Makefile). `test/e2e/e2e_cluster_stub.sh` covers N=1 and multi-member Registry, joint/old_grace membership cutover, group import, key distribution, explicit create/Reserve, stable-SandboxID CmdConnect, SandboxID↔NodeSandboxID translation, control/build reaching the API listener, data/exec reaching the Data listener, ExecSession Reserve/CmdExecSession issuance, `service=exec` KAT rejection/two-hop validation and the second-hop buffered tunnel, route cache, BuildRegister, orphan-route cleanup, reconnect full-sync convergence after a lost Build Delete, and emptied-node convergence. The stub's `reboot-empty` deliberately empties simulated state; it is not proof that real conductor startup deletes durable SQLite rows.

<a id="17-see-also"></a>
## 17. See also

- [Cluster router](cluster-router.md) — Router ingress, route cache and data forwarding.
- [Cluster placer](cluster-placer.md) — Group provider/importer, WATCH_LIST, Place and key distribution.
- [Node](node.md) — node-ctl host and node-side node-link behavior.
- [Node proxy](node-proxy.md) — Node data proxy, routesync and CONNECT.
- [Deployment](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md) — Deployment topology, ports, startup/shutdown and failure domains.
