[English](cluster-placer.md) | [简体中文](cluster-placer_zh.md)

<a id="cluster-placer--group-导入watch_list-与放置"></a>

# cluster-placer — group import, WATCH_LIST, and placement

`cluster-ctl placer` is an independent placement scheduler. It holds no node connections, owns no sandbox lifecycle, and does not replicate registry state. It obtains group configuration and key material through `SandboxGroupProvider` / `SandboxGroupImporter`, consumes the registry's `node_list` WATCH_LIST, and provides `PlaceSandbox` / `PlaceBuild` / `verify-key` to the registry.

<a id="1-概述"></a>

## 1. Overview

<a id="11-职责"></a>

### 1.1 Responsibilities

```text
SandboxGroupProvider / Importer
          │
          ▼
      placer
          │  selector patch / Place / verify-key
          ▼
      registry
          │ node owner commands
          ▼
        node
```

The placer is responsible for:

- Integrating group providers/importers.
- Consuming the `node_list` WATCH_LIST.
- Selector patches and APISecret/ManifestKey pair-cache refresh.
- Shuffle sharding, static selectors, runtime matching, and P2C.
- `PlaceSandbox` / `PlaceBuild` recommendations.
- Provider-side API-key verification.

The placer does not:

- Connect to nodes.
- Dispatch `key_put`, create, delete, or build commands.
- Maintain route/build execution state.
- Store the registry's group/route records.

<a id="12-原则"></a>

### 1.2 Principles

1. **The registry does not implement a group provider**: Provider/Importer belongs to the placer or an external platform.
2. **WATCH_LIST serves the placer only**: the router does not consume node_list.
3. **High-frequency load does not travel through WATCH_LIST**: WATCH_LIST carries only low-frequency directory fields; final resource confirmation occurs at node-owner admission.
4. **source_id identifies the import execution unit**: placers configured with the same `source_id` compete for the same source lease.
5. **Shuffle affects new placement only**: it does not migrate running sandboxes.
6. **Credential-pair distribution affects create/build prerequisites only**: dropping a pair, lease expiry, or provider updates do not modify pairs already copied into existing sandbox/build records.

<a id="2-命令行"></a>

## 2. Command line

```text
cluster-ctl placer --config /etc/cluster-ctl/placer.yaml
```

A standalone placer must configure at least one `import_groups` source. Embedded or production integrations can directly inject custom `SandboxGroupProvider` / `SandboxGroupImporter` implementations.

<a id="3-配置"></a>

## 3. Configuration

```yaml
placer:
  id: placer-1
  listen: ":7800"
  advertise: "https://placer-1.example:7800"
  memberlist_label: placer.default

registry:
  bootstrap: registry-1.example:7700

import_groups:
  - source_id: example-file-source
    source_type: file
    path: /var/lib/kuasar/groups

placement:
  candidates: 2
  zone_admit_max: yellow
  import_source_owner_count: 3
  import_source_lease_ttl: 15s
  selector_patch_refresh_interval: 1m
  # shuffle_sharding:
  #   - selector: { pool: gpu }
  #     shard_by: zone
  #     n: 2
```

| Field | Meaning |
|---|---|
| `registry.bootstrap` | Registry bootstrap endpoint for fetching membership. |
| `placer.id` | Placer instance ID. |
| `placer.listen` | Placer HTTP API listener. |
| `placer.advertise` | Address used by the registry to call the placer, also its memberlist HTTP transport address. |
| `placer.memberlist_label` | Placer memberlist label; defaults to `placer.default`. |
| `import_groups[]` | Standalone placer group sources; built-in type: `source_type=file`. |
| `placement.candidates` | Number of P2C candidates inside the placer. |
| `placement.zone_admit_max` | Highest eligible placement water-level zone. |
| `placement.import_source_owner_count` | Number of placer candidates allowed to compete for each source lease. |
| `placement.import_source_lease_ttl` | Registry-side source lease TTL. |
| `placement.selector_patch_refresh_interval` | Refresh interval for unchanged selector patches. |
| `placement.shuffle_sharding` | Shuffle-sharding rules. |

On the registry, `placer_link.placer_label` selects the placer memberlist domain; `placer_link.placer_replica_count` selects the number of failover candidates used to call a placer for one group. It is not the owner count of the registry's internal `placer_link` namespace.

## 4. Provider / Importer

The placer uses these common interfaces:

```text
SandboxGroupProvider:
  Get(group)
  GetPlacementHint(group)
  GetKey(group)       # typed manifest_key
  GetAPISecret(group) # typed APISecret

SandboxGroupImporter:
  Range(cursor, limit)
```

The built-in file source is for local development and e2e use:

```yaml
import_groups:
  - source_id: example-file-source
    source_type: file
    path: /path/to/dir
```

It enumerates `*.json` files in the directory. Each file contains one `SandboxGroupRecord` JSON object. The expected file count is small, so `Get/GetPlacementHint/GetKey/GetAPISecret` can scan and parse the directory directly. Production deployments should integrate their actual group source through the interfaces.

Example:

```json
{
  "group": "/cell/project/app/group",
  "template_ref": "tmpl-1",
  "target_port": 49983,
  "api_secret": {"type": "inline", "value": "<64-lowercase-hex>"},
  "manifest_key": {"type": "inline", "value": "<64-lowercase-hex>"},
  "node_selectors": [{"pool": "default"}]
}
```

An explicit `api_secret` takes precedence. When an inline `manifest_key` omits `api_secret`, the placer derives the default using `HMAC-SHA256(decodeHex(ManifestKey), "kuasar-api-secret-v1")`. A referenced ManifestKey cannot be materialized locally by the placer and requires an explicit APISecret. Both roots and the complete fingerprints of references must be 64 lowercase hexadecimal characters.

`target_port` is the enforced data-plane port returned with placement to route_link/router. It does not filter nodes; node selection still uses `node_selectors` and shuffle-sharding configuration.

Distinguish these semantics when using multiple sources:

- Provider point lookups can search sources in order; defining the same group in multiple sources is an error.
- Importer Range runs independently for each `source_id`; it does not combine sources into one Range view.
- Each `source_id` maintains its own cursor and lease.

After a group disappears from the provider, new Place requests have no matching group and verify-key rejects missing credentials; neither can use the removed group. Pairs already cached in node_link are not actively deleted; registry/node TTL eviction removes them. Durable credential copies in existing sandbox/build records remain unchanged.

## 5. node_list WATCH_LIST

Registry node owners project these low-frequency node fields into node_list:

- node_id
- labels
- runtime_digest
- api_endpoint
- data_endpoint
- Configured registration/execution Build capacity; usage/headroom does not enter this low-frequency view.
- draining
- Liveness timestamp

```text
registry node owners
  │ low-frequency projection
  ▼
node_list owner set
  │ reset + delta + bookmark
  ▼
placer local node view
```

WATCH_LIST semantics:

1. The placer derives node_list owner candidates from active membership and consumes one owner's complete WATCH_LIST at a time.
2. The first frame is reset, followed by deltas; bookmark marks completion of the initial view.
3. A watch token encodes the registry-local epoch, node_list shard-view label, and Rev.
4. An epoch/label mismatch or compacted changelog requires a full resubscription.
5. Ordinary heartbeats do not trigger WATCH_LIST. Draining changes and coarse liveness refresh do update node_list.

The node_list shard is fully replicated across its owners. The placer neither needs nor may merge results from multiple node_list owners. If its current owner disconnects, the placer clears that source view, fails over to another candidate owner, and repeats reset + bookmark. It must not declare ready before bookmark completes.

<a id="6-placer-memberlist-域"></a>

## 6. Placer memberlist domain

At startup, the placer joins the memberlist selected by `placer.memberlist_label` and periodically registers a seed with active / next registry members:

```json
{"id":"s1","advertise":"https://s1:7800","memberlist_label":"placer.default"}
```

```text
placer S1 ── register seed ──► registry A
    │                            │
    └──── placer.default memberlist ◄──── registry observer
```

Placer readiness requires:

- Current registry membership has been fetched.
- One active node_list WATCH_LIST has completed reset/bookmark.

Group import is not a global readiness gate. Each Place resolves only its requested group. A missing group returns NoNode / unavailable without blocking other groups.

The registry selects ready placers from memberlist metadata only:

```json
{"role":"placer","id":"s1","advertise":"https://s1:7800","ready":true,"ready_label":"registry.2.hash"}
```

`placer_link/register` is neither the readiness source nor a placer directory store.

<a id="7-import-reconcile"></a>

## 7. Import reconciliation

<a id="71-source-owner-选择"></a>

### 7.1 Source-owner selection

Each source runs independently:

```text
readyPlacers = placer memberlist nodes where ready_label == active_registry_label
sourceOwners = LocateN(source_id, readyPlacers, import_source_owner_count)

sourceOwners race:
  POST /placer-link/import-source-lease
```

The source lease lives in registry shardkv:

```text
namespace = placer_link
shardKey  = import/source/<source_id>
recordSet = import
recordKey = state
```

A lease request carries `source_id`, `owner_id`, `run_id`, and `ttl_ms`. Only the lease winner executes that source's `Importer.Range(cursor, limit)`.

<a id="72-page-处理"></a>

### 7.2 Page processing

```text
lease winner
  │ Range(cursor, limit)
  ▼
groups in page
  │ for each group:
  │   GetPlacementHint
  │   GetKey / GetAPISecret
  │   calculate effective selectors
  ▼
selector patch with fencing
  │
  ▼
registry refreshes node_link credential-pair cache
  │
  ▼
cursor checkpoint after whole page succeeds
```

A selector patch must carry `import_source_id/import_owner_id/import_run_id/import_term`. The registry accepts it only when it matches the current source lease. Missing fencing fields or a mismatched term/run_id cause rejection.

`NextCursor==""` ends the round: the cursor is cleared and the round increments. Failure or owner crash does not advance the cursor; the next lease owner resumes from the last successful cursor or replays the same page.

### 7.3 Selector-patch refresh

At `placement.selector_patch_refresh_interval`, the placer resends unchanged selector patches to keep the node_link credential-pair cache alive. A group disappearing from the provider does not actively delete the cache; entries expire by TTL.

## 8. Placement

<a id="81-registry-选择-placer"></a>

### 8.1 Registry selection of a placer

For each group, the registry performs deterministic failover only:

```text
readyPlacers = placer memberlist nodes where role=placer and alive and ready=true
               and ready_label == active_registry_label
candidates   = LocateN(group, readyPlacers, placer_link.placer_replica_count)
try candidates in order until success
```

The registry does not use P2C to choose a placer. P2C is used only inside a placer to choose a placement target from node candidates.

### 8.2 PlaceSandbox

Input:

```text
group, route_key, sandbox_id, target_runtime_digest?, config?, exclude_node_ids?
```

`sandbox_id` is the stable public SandboxID owned by the registry. The placer does not allocate, parse, or receive NodeSandboxID or SandboxGeneration.

Flow:

```text
provider.GetPlacementHint(group)
  │
  ▼
filter node_list:
  selector match
  not draining
  not in exclude_node_ids
  zone <= admit max
  runtime digest match
  shuffle slot match
  │
  ▼
P2C over candidates
  │
  ▼
return node_id + create_spec + APISecret fingerprint + runtime/template hints
```

`node_list` is a low-frequency directory, not an online-status authority. Before committing, the route owner asks the node owner for the current node-link connection. Disconnected candidates, admission rejections, or command rejections add the candidate to `exclude_node_ids`, followed by another Place, until an online candidate is selected or the placer reports no candidate. The node owner is the sole liveness authority.

### 8.3 PlaceBuild

Input:

```text
group, build_id, template_id, resources
```

Build placement resembles sandbox placement, but the placer's low-frequency view uses configured registration capacity only to exclude nodes that cannot fit the single Build even in isolation. It receives no heartbeat usage. Current headroom is read only at the Holder/node boundary:

1. The placer recommends a node whose static capacity can fit the request.
2. Through the current Holder, the registry confirms that node-link is online and reads the latest durable registration usage.
3. If headroom is insufficient, the registry excludes that candidate and reschedules before creating a registration intent.
4. The registry persists a STARTING intent for the selected node/build, then sends `build_register`.
5. The node performs final registration admission in a SQLite transaction. Only a definitive rejection without side effects permits excluding the candidate. Timeout, disconnect, or lost ACK pins queries/retries to that node/BuildID.

## 9. Key distribution

The placer owns selector patches and APISecret/ManifestKey pair-cache refresh:

1. Obtain the group's typed `manifest_key` and `api_secret`. A missing APISecret can be derived only from an inline ManifestKey using the fixed derivation.
2. Compute the target node set from placement selectors and shuffle results.
3. A selector patch carries either the complete pair or no credential fields. Each member includes a typed carrier and full 64-hex SHA-256 fingerprint; half-pairs are rejected.
4. Registry/node owners use the full `APISecretFingerprint` as the record key, write the desired pair to node_link's `key_pair` recordSet, and replicate it by CAS across the owner set.
5. A node_link heartbeat performs atomic `key_put` only for entries lacking `AckAccepted` or entering the renewal window. Delivery state advances only for an ACK matching the entire current desired pair and lease.
6. One full APISecret fingerprint can bind only identical pair material; conflicts must not overwrite it.
7. Nodes evict unrenewed pairs by TTL. `key_drop` addresses the full APISecret fingerprint and is not a correctness dependency.

Credential pairs are create/build prerequisites. Cache removal, key_drop, lease expiry, or later provider updates do not affect pairs already copied into existing sandbox/build business records.

<a id="10-可靠性"></a>

## 10. Reliability

| Event | Behavior |
|---|---|
| Placer crash | The registry fails over to the next ready placer for the group; the hot path is unaffected. |
| WATCH_LIST disconnect | The placer clears its node_list view and fully resubscribes through another owner; it remains not-ready until bookmark completes. |
| Provider unavailable | Place/verify-key for affected groups return unavailable. |
| Stale node labels | Node-owner admission/create provides the final rejection. |
| Source-owner crash | After lease expiry, another candidate resumes from the committed cursor; fencing rejects old-owner patches/cursor updates. |
| Key-renewal delivery failure | Timeout, disconnect, or rejection does not advance delivery state; create/build rejects at the node and the next heartbeat refresh retries. |
| Build registration timeout or lost ACK | Keep the persisted selected-node/BuildID intent and query/retry that same node. Durable registration usage remains node-owned; the placer has no admission-lease timer that frees it. The node releases registration usage on its ready/error transition; terminal-history deletion later removes the Registry projection. These are separate events. See the [durable usage query](../internal/store/build_admission.go). |

See the authoritative [registration dispatch and projection reconciliation](../internal/registry/build.go) and [placement predicates](../internal/placer/scaler.go). In particular, ambiguous delivery cannot be treated as proof of a side-effect-free rejection.

<a id="11-性能"></a>

## 11. Performance

- WATCH_LIST carries low-frequency fields only, avoiding high-frequency water-level fan-out.
- Group `LocateN` distributes Place QPS across ready placers.
- Inside the placer, P2C compares only sampled candidates using the current local directory view; it does not synchronously poll nodes for live load.
- Import pagination is independent per source; sources are not fully merged.
- Selector patches are a slow path and can be rate-limited; unchanged patches refresh TTL only.

## 12. See also

- [cluster.md](cluster.md) — overall registry membership, shardkv, node_link, route_link, and placer_link design.
- [cluster-router.md](cluster-router.md) — router Reserve consumption, route cache, and data-plane forwarding.
- [node.md](node.md) — node-side node-link registration, heartbeats, command execution, and key TTL.
