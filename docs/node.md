[English](node.md) | [简体中文](node_zh.md)

# node — An e2b-compatible sandbox host and cluster member

`node-ctl` runs one resident conductor per compute node and exposes its microVM sandboxes through an **e2b-compatible API**. The supported operations can be used by unmodified Python/JavaScript `e2b`, `@e2b/code-interpreter` SDKs and the e2b CLI, subject to the compatibility boundaries and pinned versions below. `node-ctl conductor serve` provides the **API** (control-plane REST for sandbox lifecycle, template builds and authentication), **host orchestration** (systemd template units for `sandbox-ctl`/builder tasks and `connector-ctl vswitch` networking), an optional **resource controller** (node arbitration through `resource_listen`; see [node-resource.md](node-resource.md)), and the **node-link client** that joins cluster-ctl orchestration (§10). The separately deployed `node-ctl proxy serve` owns guest data-plane forwarding; conductor does not serve it.

A node can run **standalone**, serving e2b SDK/CLI clients on one machine, or **join a cluster** managed by Registry, Router and Placer ([cluster.md](cluster.md)). Both modes share the same e2b control plane and lifecycle primitives. **Create, pause, kill and template execution remain node-local**; cluster commands reuse these primitives (§10).

`sandbox-ctl` runs the sandbox microVM with Cloud Hypervisor. An e2b-profile guest runs upstream envd from the single `sandbox-runtime.bundle` (§11); the independent Proxy forwards its single-port protocol (49983/Connect-RPC) through a host UDS. A tenant's root credentials are **APISecret + ManifestKey**: APISecret signs/verifies API keys and derives each Sandbox's ServiceSecret; ManifestKey protects Manifest content and image-pull tokens. If omitted, APISecret is derived from ManifestKey by a fixed KDF. The pair is stored encrypted, never as plaintext root credentials in the database (§7).

The outputs are **`node-ctl`** (daemon, launchers, resource controller and administration CLI, §2) and **`e2b-key-ctl`** (credential derivation without DB/config/orchestration state, §2.8).

## 1. Overview

### 1.1 The service problem

The runtime/accelerator/builder/vswitch stack exposes individual CLI primitives: start a microVM, flatten an image, attach a port. Clients need a northbound service that composes them: an existing e2b SDK/CLI and API key should let them create, execute in, pause, resume and kill sandboxes and build custom templates, without managing microVMs, Manifests or the eBPF switch.

node-ctl supplies that layer using **e2b protocol compatibility**. The SDK ecosystem, including code interpreters and agent integrations, can use the supported contract without client modifications. Endpoints, tokens and state machines have reference implementations, and real SDK/CLI E2E tests check compatibility. At scale, node-link connects many nodes to cluster-ctl for session-affine routing and on-demand placement (§10 and [cluster.md](cluster.md)); one node can also serve independently.

### 1.2 Design principles

1. **Compose at explicit boundaries.** Lifecycle and networking use subprocess CLIs such as `sandbox-ctl` and `connector-ctl vswitch`; units use systemd D-Bus and envd forwarding uses UDS. The code also imports sibling repositories' public packages for typed artifact preparation, configuration, assembly and publication (§13), so this is not a CLI-only dependency graph. The binaries support pure-Go `CGO_ENABLED=0` builds.
2. **Keep resource arbitration separable.** `sandbox-ctl`'s `pkg/resource` client negotiates sandbox admission and quota with the node resource controller. `node-ctl conductor serve` embeds it through `resource_listen`, with inline configuration ([node-resource.md](node-resource.md)). It remains separate from API, host and Proxy logic. Conductor manages the Build admission pool itself ([Build §5](node-build.md#5-target-aware-execution-and-publication)).
3. **Delegate process supervision to systemd.** Runner/builder template instances use RunID and may start early to wait for config-socket assignment. Runner `ctl/vmm` subgroups separate the supervisor from sandbox resources; the entire builder unit is accounted. StopUnit and its completion checks reclaim the unit; conductor does not replace systemd as a process supervisor.
4. **Keep root credentials encrypted at rest.** Tenant APISecret/ManifestKey pairs use AES-256-GCM in SQLite. Authenticated task bootstrap supplies authoritative ManifestKey environment values to single-tenant runner/builder processes. Conductor's managed sandbox/build preparation does not open or parse tenant artifacts with that key, and ManifestKey does not enter the guest (§6–7). Pairs received over node-link are also stored encrypted (§10).
5. **Separate local and cluster routing authority.** Conductor owns local routes and lifecycle; independent Proxy is the sole sandbox data ingress (§9). Registry owns cluster routes. Node-link reports node events and accepts cluster commands (§10) without creating competing cluster authority.
6. **Reconcile after restart.** SQLite and systemd unit inventory preserve enough state to adopt or clean up execution after a conductor restart (§14). In cluster mode, node-link reconnects and reports the node's sandboxes so Registry converges.

### 1.3 Two sandbox profiles

| Profile | Meaning | Data plane | Exposed services |
|---|---|---|---|
| **e2b** | Upstream envd in the guest for agent exec, file access and code execution | envd fs/process/pty/runCode plus platform native exec | envd/CI, floating-IP user ports and native exec |
| **bare** | Run a customer image as a networked microVM without envd | Platform native exec; no envd API | Floating-IP networking and native exec |

`bare` reuses `sandbox-runtime.bundle` and exposes the base sandbox through the northbound API. Build registration defaults to **e2b**, but explicitly accepts **bare** ([Build §1](node-build.md#1-build-api)); the output profile is immutable. The templateID prefix encodes profile ([Build §2](node-build.md#2-template-ids-and-artifact-authority)), which selects guest launch behavior and data services within the shared runtime Bundle.

### 1.4 Boundaries and dependencies

- Northbound clients use e2b SDK/CLI directly. In a cluster, cluster-ctl Router forwards traffic and node-link carries control commands. Platform administration can also use the e2b API.
- Standalone and cluster modes keep create/pause/kill/template execution on the node. Joining adds node-link (§10) without replacing the e2b contract.
- The data plane forwards upstream guest envd protocols rather than implementing envd (§4.2).
- The independent Telemetry component implements envd/OTLP collection and E2B `/sandboxes/{SandboxID}/metrics` history; Conductor only authenticates, checks ownership and forwards to a live registered query UDS. See [Telemetry](telemetry.md). Native `/stats/resource`, `/stats/traffic` and `/stats/usage` remain independent; see §4.1.1 and [Native usage](node-usage.md).
- The server does not parse Dockerfiles. It pulls/flattens existing images and executes the supported structured Build steps supplied by clients inside phase microVMs ([Build §5](node-build.md#5-target-aware-execution-and-publication)).
- Routing, storage and units are node-local. Cross-node snapshots/templates use canonical portable refs in Manifest Store or uniformly mounted named locations (§8.1); cluster-ctl orchestrates through node-link (§10).
- Dependencies include the standard library, pure-Go `modernc.org/sqlite`, `golang.org/x/net/http2` for config-socket/node-link h2c, `golang.org/x/sys` for pidfile locks/SO_PEERCRED/mmap, `coreos/go-systemd`, `google/uuid` v7 and `gopkg.in/yaml.v3`. The module also includes CEL/protobuf, AWS SDK and sibling public packages; see [go.mod](../go.mod). The hand-written envd client and node-link use JSON rather than a gRPC wire protocol.

### 1.5 Architecture and data paths

Alongside the application data path below, `node-ctl telemetry serve` subscribes
to the same full Plugin Plane RouteEntry stream. It scrapes envd over UDS and
directly accepts FloatingIP-identified guest OTLP in the management namespace.
Its Collector pipelines use standard exporter configuration; the local TSDB opens
only when explicitly enabled. Query backends and HTTP handlers are selected independently,
and query-only requires no Collector graph. Conductor forwards authenticated metrics queries over the
independent registered API UDS; no telemetry work enters lifecycle barriers.
The complete topology, failure and identity contracts are in [Telemetry](telemetry.md).

```
             client / cluster router
                    │ control
                    ▼
             APIEndpoint
                    │
      ┌─ conductor ─┴────────────────────────┐
      │ API, lifecycle, route authority      │
      │ sandbox/build ownership and systemd  │
      └──────────────┬───────────────────────┘
                     │ config_socket
                     │ route sync / Wake / barrier / MMDS / stats registration
                     ▼
      ┌─ proxy master + workers ─────────────┐
      │ DataEndpoint: HTTP / CONNECT / exec  │◄── client / cluster router
      │ MMDS and traffic observation         │
      └──────────────┬───────────────────────┘
                     │ UDS / TCP / ctl.sock
                     ▼
              sandbox backend
```

Synchronous Create performs request validation and pure parsing, selects the requested or generated identity and materializes tokens, claims in-process launch ownership, then inserts `starting, run_id=""` with empty network fields and caches/publishes starting. It sends `route_barrier` on that same ordered routesync stream, waits for the current Proxy master to apply the preceding Upsert and ACK, and revalidates the registration lease before scheduling launch and returning HTTP 201. This means durable acceptance plus enough applied Proxy identity data to authenticate and park traffic. It does not mean a runner is assigned, runtime/backend is ready, or e2b `/init` completed. No Proxy, disconnection, apply failure or timeout returns 503. Before starting resources, cleanup removes this object's RunDir/BaseDir, then exact-owner CAS converts `starting,run_id=""` into owner-free `dead` history and publishes route Delete.

The cold-image background path retains its single-stage fast path: prepare resources/network/YAML, then allocate a RunID from the runner pool. Artifact launch first creates directories and binds `ready.sock`; the pool commit callback binds an exact RunID with `starting AND run_id=''` CAS. The assigned task immediately connects readiness, locks its task pidfile and obtains bootstrap. After authentication, ManifestKey overrides its environment. Based on E/S kind and LaunchMode, the task opens the root, selects PreparedSource and submits a non-secret capacity/network/ref-closure summary. The sole launch worker then resolves resources/network, attaches networking, persists ownership with `starting AND run_id=<exact>` CAS, writes root-credential-free YAML and returns the final LaunchSpec. The runner appends its retained ref-locations and replaces itself with `sandbox-ctl run` under the same PID. After microVM startup and the strict runtime readiness wire, e2b performs direct `POST /init` for environment/default user. Exact-RunID CAS commits `running` and opens traffic.

Cluster Create arrives as a node-link `create` command. Profile, group, route-key and optional authentication identity use structured system context and separate durable fields. Events report profile, node-owned execution facts and protected routing credentials; Registry recovers cluster identity from its existing node ownership records (§10 and §4.4).

Legacy data routing parses `(sid,port)` from `Host` (`<port>-<sid>.<domain>`) or `E2b-Sandbox-Id`/`E2b-Sandbox-Port`. For e2b, ports 49983/49999 authenticate with EnvdAccessToken and dial sandbox-ctl `--connect` host UDS endpoints for envd/CI. For bare, these numbers are ordinary legal ports: ForwardAccessToken authorizes `floatingip:port`. CONNECT may explicitly carry `E2b-Sandbox-Service: forward|e2b:envd|e2b:code-interpreter|exec`; this is the authoritative backend selector. Ordinary HTTP otherwise uses legacy routing, but explicitly naming `exec` returns 405. Native exec uses an explicitly minted ExecAccessToken, verifies KAT before any resume side effects and ultimately reaches `<RunRoot>/sandboxes/<NodeSandboxID>/ctl.sock`.

TrafficAccessToken is for external gateways/e2b data components; node's platform layer does not consume it. Authorized requests to paused sandboxes trigger automatic resume through the shared launch owner (§8). Independent Proxy is the sole node data plane; see [node-proxy.md](node-proxy.md). In a cluster, Router maps the stable public SandboxID to current NodeSandboxID, injects `E2b-Sandbox-Id` and `X-Access-Token`, and forwards to node Proxy ([cluster-router.md](cluster-router.md)).

### 1.6 Node directories and identities

`paths.run_root`/`paths.base_root` are node-level **RunRoot/BaseRoot**; an object's actual directories are **RunDir/BaseDir**. RunRoot holds pid/lock files, Unix sockets, readiness, Sandbox YAML, runtime config, bounded CH snapshot staging state and small temporary JSON. BaseRoot holds writable diffs, local checkpoints, Build images and Sandbox/Snapshot artifacts, and other large data:

```text
<RunRoot>/
├── node-level files
├── runners/<RunID>.pid
├── sandboxes/<SandboxID>/
└── builds/<BuildID>/
    ├── builder.pid
    ├── a/
    ├── b/
    └── c/

<BaseRoot>/
├── node-level persistent files
├── sandboxes/<SandboxID>/checkpoint/
└── builds/<BuildID>/
    ├── checkpoint/
    ├── a/
    ├── b/
    └── c/
```

An ordinary Sandbox row stores exact `RunDir=<RunRoot>/sandboxes/<SandboxID>` and `BaseDir=<BaseRoot>/sandboxes/<SandboxID>`. SandboxID is always logical identity. sandboxer's PathID selects only the leaf beneath invocation-level roots; for ordinary sandboxes they are equal. Build phases retain globally unique logical SandboxIDs but use fixed PathID `a`, `b` or `c`. Resources, logs, memfd and artifact identity still use logical SandboxID. RunID identifies runner execution; its pidfile lives only under `runners/`.

BuildID is restricted to `[A-Za-z0-9_-]{1,48}` and used verbatim as `builds/<BuildID>`. BuildRunDir/BuildBaseDir derive uniquely from the roots and BuildID; they are not stored in Build rows, hashed or sanitized, and have no legacy-path fallback/migration. Registered/waiting Builds create no directories; execution claims precede creation. All Build images and Sandbox/Snapshot artifacts use `BuildBaseDir/checkpoint`; ordinary local captures use `BaseDir/checkpoint`. Validation requires custom RunRoot to fit sandboxer's longest socket under the maximum SandboxID and the longest phase socket under a 48-byte BuildID, within Linux's 107-byte pathname limit. The identity-length contract does not vary with the chosen root.

## 2. Command-line interface

### 2.1 Subcommands

**`node-ctl`:**

| Subcommand | Purpose |
|---|---|
| `conductor serve` | Control plane, local control socket, reaper and Build pool; `cluster` enables node-link (§10), and `resource_listen` embeds the resource controller ([node-resource.md](node-resource.md)) |
| `proxy serve` | Independent data-plane master/workers (§2.3 and [node-proxy.md](node-proxy.md)) |
| `run-sandbox` / `run-builder` | Launchers inside systemd units, not interactive commands (§2.4 and §6) |
| `resource` | `status`/`list`/`drain`: inspect reservations and drain admission ([node-resource.md](node-resource.md) §2) |
| `builder status` / `builder cancel <build-id>` / `builder delete <transient-template-id> [--cancel]` | Inspect durable usage, transient IDs, operation intent and claims; cancel execution or delete one Build record ([Build §1.1](node-build.md#11-cancel-and-delete-a-build-record)) |
| `config` | Normalize/validate configuration or print a commented template |
| `manifest-key` | `add`/`remove`/`check`/`list`: manage credential-pair allowlisting for create/build/import (§7); Registry also writes leased entries (§10) |
| `export-sandbox` / `import-sandbox` | Promote paused sandboxes to templates or migrate them (§8.1) |
| `version` | Print version |

**`e2b-key-ctl`**, which derives credentials without DB/config/daemon access:

| Subcommand | Purpose |
|---|---|
| `gen-key` | Generate a random 32-byte root credential (64 hex digits) |
| `derive-api-secret [<MANIFEST_KEY>]` | Derive default APISecret with the fixed KDF (§7) |
| `gen-apikey [<API_SECRET>]` | Sign an e2b API key with APISecret (`e2b_` plus hex, §7) |
| `fingerprint [<API_SECRET>]` | Print APISecret's full 64-hex SHA-256 fingerprint |
| `seal-pull-token [<MANIFEST_KEY>] …` | Seal an opaque image-pull token (`kpt_`, [Build §5](node-build.md#5-target-aware-execution-and-publication)) |
| `version` | Print version |

A typical standalone-node setup is:

```bash
# 1) Generate a content root, derive APISecret, register the pair and issue the SDK API key
MK=$(e2b-key-ctl gen-key)
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
node-ctl manifest-key add --api-secret "$API_SECRET" --label tenant-a "$MK"
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$API_SECRET")

# 2) Point e2b SDK/CLI at this node
export E2B_DOMAIN=sandboxes.example.com        # Production (TLS, §12)
# dev: E2B_API_URL=http://host:3000  E2B_SANDBOX_URL=http://host:3443
```

In cluster mode, Registry distributes leased keys through node-link (§10 and [cluster.md](cluster.md)); manual `manifest-key add` is unnecessary.

### 2.2 `node-ctl conductor serve`

```
node-ctl conductor serve [--config /etc/node-ctl/conductor.yaml]
```

| Flag | Default | Meaning |
|---|---|---|
| `--config` | `/etc/node-ctl/conductor.yaml` | Conductor configuration (§3) |

Startup opens SQLite with file mode 0600, generates/installs systemd templates (§5), reconciles restart state (§14), starts the 5-second TTL reaper and optional embedded resource controller, binds and confirms the local control socket (§6), starts runner/builder pools and Build admission ([Build §5](node-build.md#5-target-aware-execution-and-publication)), optionally dials `cluster.node_link.endpoint` (§10), then listens on `api.listen`. That listener serves only the wrapped control API; sandbox data Host requests and CONNECT do not reach backends. With no TLS certificates it serves plaintext h2c; development SDK control uses `E2B_API_URL`.

The supplied systemd service is [deploy/node-ctl.service](../deploy/node-ctl.service).

### 2.3 `node-ctl proxy`

This is the independent data-plane master, deployed separately on the conductor's node. Its `proxy.yaml` has its own schema ([node-proxy.md](node-proxy.md) §2). Master internally reexecs and supervises workers using its current executable; workers neither enter the node-ctl CLI dispatcher nor reread configuration:

```
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

`proxy.yaml` configures process endpoints/bootstrap policy: `config_socket`, `data_listen`, `proxy_netns`, `stats_socket`, `shm_path`, `workers`, `tls`, `auth` and `park_timeout`. Only conductor configures MMDS listening and the service registry. Master registers once on the plugin plane, receives MMDS policy in the handshake, maintains the shared route view and passes the sole Data listener FD to workers. See [node-proxy.md](node-proxy.md) §2 and §5; conductor and Proxy must be deployed together.

### 2.3.1 `node-ctl telemetry`

```sh
node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
```

This is an independent component with its own strict config schema and
`paths.telemetry_executable` static bootstrap option. It registers fixed Plugin
ID `telemetry`, subscribes with `Kind: route`, and registers a separate HTTP
query UDS only when a Reader and HTTP handler are selected. Writes and reads
are independent; query-only requires neither Collector nor local TSDB. It does not join Create/Resume
readiness or call Wake. Native Collector configuration selects envd, sandboxstats
and sandboxotlp receivers, standard processors/exporters/connectors/extensions
and multiple pipelines. Sandboxstats uses only the current telemetry lease on
conductor config_socket; native stats remain independent of telemetry. Local TSDB, Prometheus and ClickHouse readers, direct
sandbox OTLP networking, E2B steps/MAX behavior and bounds are specified in
[Telemetry](telemetry.md). Use `deploy/node-telemetry.service` alongside, not as
a required dependency of, conductor/Proxy.

### 2.4 `node-ctl run-sandbox` / `run-builder`

These are systemd ExecStart launchers. Their common entry uses `--run-id` as the unit instance name and `--pidfile=<RunRoot>/runners/<RunID>.pid`. An exclusive `fcntl(F_SETLK)` lock prevents duplicate execution; the launcher writes its PID, then calls WaitAssignment over `--config-socket` to obtain its business ID (§6). They then diverge:

- **run-sandbox:** immediately connect `<RunRoot>/sandboxes/<SandboxID>/ready.sock` with `FD_CLOEXEC` still set, lock `<RunDir>/<SandboxID>.pid`, and request bootstrap using SID plus exact RunID. Cold-image bootstrap returns final LaunchSpec in one RPC. E/S bootstrap returns task-local ArtifactPrepareSpec and authoritative `MANIFEST_KEY`; the runner overrides inherited environment, opens E/S according to kind and durable LaunchMode, submits the completion and waits for final LaunchSpec. It retains PreparedSource, carrier bindings and sorted ref-locations locally, explicitly closes readers/fetchers, then changes to LaunchSpec.Workdir, strips `TASK_*` and merges task/spec environment. Only immediately before final `execve` does it clear readiness FD's `FD_CLOEXEC`, append actual `--ready-fd=<fd>` and replace itself with `sandbox-ctl run`. The new program inherits PID, unit cgroup, pidfile lock FD and readiness FD. Any pre-exec/prepare failure closes readiness and conductor immediately observes EOF.
- **run-builder:** complete task preparation, resident execution, deadlines and result handoff are in [Build §4](node-build.md#4-task-handoff).

```
node-ctl run-sandbox --pidfile=<f> --config-socket=<uds> --run-id=<rid>
node-ctl run-builder --pidfile=<f> --config-socket=<uds> --run-id=<rid>
```

Missing flags fall back to `TASK_PIDFILE`, `TASK_CONFIG_SOCKET` and `TASK_RUN_ID`, used for systemd `%i` wiring.

### 2.5 `node-ctl config`

Configuration generation and diagnosis are **role-specific** (`conductor`/`proxy`/`telemetry`, separate files and schemas):

```
node-ctl config <conductor|proxy|telemetry> --template            # Print the role-specific commented template
node-ctl config <conductor|proxy|telemetry> --config <file>       # Load, default, validate and normalize
node-ctl config <conductor|proxy|telemetry> --config <file> --resolve   # Also resolve auto/derived effective values
                              -o <file>             # Write a file (default stdout)
```

The first argument selects schema: conductor uses `conductor.yaml` (§3), proxy uses `proxy.yaml` ([node-proxy.md](node-proxy.md) §2), telemetry uses `telemetry.yaml` ([Telemetry](telemetry.md)). Conductor `--resolve` additionally expands `resource_listen` auto memory/CPU and deeply validates watermarks; for other roles it is equivalent to `--config`. Templates correspond to `deploy/{conductor,proxy,telemetry}.example.yaml`.

The command performs strict declarative decoding, defaulting and diagnosis. It never executes `conductor_executable`/`proxy_executable`/`telemetry_executable` or accesses custom App runtime materials. With a custom executable, the first output line explicitly marks bootstrap-only validation; that App validates final configuration before startup side effects.

### 2.6 `node-ctl manifest-key`

This thin client manages create/build credential-pair allowlisting (`manifest_keys`) through conductor's local **admin plane** (§6). Daemon is the table's sole writer; CLI opens no DB and reads no config. It needs only `--socket`, or `NODE_CTL_SOCKET`, defaulting to `/run/sandbox/node-ctl.socket`.

ManifestKey comes from a positional argument or `MANIFEST_KEY`. APISecret can be supplied through `--api-secret`/`API_SECRET`; otherwise §7's KDF derives it. The pair is written atomically. Output includes only their full fingerprints, never root credentials. Registry also installs leased pairs over node-link (§10 and [cluster.md](cluster.md)), coexisting with manual entries.

```
node-ctl manifest-key add    [--api-secret S] [--label L] [--ttl 24h]
                             [--registry-auth <docker.json> |
                              --registry-username U --registry-password P |
                              --registry-token T]      [--socket S] <KEY>…
node-ctl manifest-key remove [--api-secret S] [--socket S] <KEY>…
node-ctl manifest-key check  [--api-secret S] [--socket S] <KEY>…
node-ctl manifest-key list   [--socket S]
```

- `--api-secret` explicitly pairs an APISecret with one `<KEY>` only; omission derives the default. `add/remove/check` always operate on the complete pair.
- `--ttl` sets expiry duration (`0`/omitted means never). Repeated add refreshes expiry only for an identical complete pair. Binding the same full APISecret fingerprint to different credential material conflicts. Expired pairs are treated as absent and lazily removed by the reaper; `list` shows `expires`.
- `--registry-auth`/`--registry-*` configure default tenant pull credentials encrypted in `registry_auth_enc`, selected by fromImage host during Build ([Build §5](node-build.md#5-target-aware-execution-and-publication)).
- `add/remove/check` prints `STATUS api=<64-hex> manifest=<64-hex>`; `list` prints both full fingerprints per row.
- Authentication uses `SO_PEERCRED`: with `paths.admin_pidfile`, peer PID must be listed; otherwise socket mode 0600 is the boundary (same UID/root).

### 2.7 `node-ctl export-sandbox` / `import-sandbox`

These clients use conductor's local **API plane** for `POST /sandboxes/{id}/export` and `POST /sandboxes/import`. `E2B_API_KEY` must authenticate the owner. See §8.1.

```
node-ctl export-sandbox <sid> [--to-template] [--keep-source] [--socket S]
node-ctl import-sandbox <token> [--socket S]
```

- `export-sandbox <sid>` prints one opaque `kmt1.` migration token. After successfully acquiring the source finalizer, default behavior deletes the source; `--keep-source` retains it paused with its original ResumeSource and local checkpoint — the source resumes from exactly where it would have resumed without the export (#336).
- `export-sandbox <sid> --to-template` publishes paused E or S and prints persistent `sbx`/`snp` templateID for fan-out. Source retention is identical: `--keep-source` retains it; omission deletes it.
- Resume remains allowed during local-artifact upload. If Resume wins first, KMT Export cancels upload and returns 409, whereas Template Export continues and returns templateID. Both abandon the source finalizer without updating/deleting source or local artifact.
- `import-sandbox <token>` defaults to source NodeSandboxID, inserts a paused row without overwriting and prints SID; an existing target returns 409. API body may choose another node-local `sandboxID`, retaining logical authentication identity and service credentials. A subsequent Connect accepts asynchronous resume.

### 2.8 `e2b-key-ctl`

See §2.1 for the command list. Full `seal-pull-token` syntax:

```
e2b-key-ctl seal-pull-token [<MANIFEST_KEY>] {--registry-username U --registry-password P |
                                              --registry-token T}
```

The opaque `kpt_` token contains image-pull credentials sealed with AES-GCM using a tenant ManifestKey-derived key. SDK `api_headers` sends it as `X-Kuasar-Pull-Token` with the Build request; conductor opens it using the tenant's stored key ([Build §5](node-build.md#5-target-aware-execution-and-publication)). ManifestKey comes from the first positional argument or `MANIFEST_KEY`.

## 3. Configuration

Conductor uses `conductor.yaml`. The fully commented [deploy/conductor.example.yaml](../deploy/conductor.example.yaml) matches the shape of `node-ctl config conductor --template`. The authoritative type is `config.Conductor` in public package `github.com/kuasar-sandbox/orchestrator/config`; internal code reuses that schema. It provides strict `LoadConductor`/`DecodeConductor`, `LoadProxy`/`DecodeProxy`, final validators that do not reapply defaults after hooks, and genuinely deep `Clone` operations. Decode/Load perform bounded strict decoding, declarative defaulting and enum/duration/range/format validation of supplied values. They do not inspect environment, component files or executable-dependent startup requirements. Identical input produces identical results regardless of `NODE_CONFIG_ENCRYPTION_KEY`. Unknown YAML fields and multiple documents fail. Config JSON/YAML contains only serializable declarations, never loggers, providers or runtime handles.

`ResourceAllocatable.Memory` exposes presence as `*string`: nil inherits the internal resolver's `256MiB` default; non-nil, including explicit `256MiB`, is operator/custom Configure policy and must fit capacity. `SetMemory`/`InheritMemory` are conveniences; direct pointer assignment has identical semantics. Clone and component bootstrap JSON preserve presence.

Groups are `api`, `proxy`, `paths`, `units`, `sandbox` (instance defaults under `resources`/`network`/`boot`), `builder`, `checkpoint`, `mmds`, `cluster` (node-link, §10) and `resource_listen` (embedded controller with all tuning inline; [node-resource.md](node-resource.md)). `encryption_key` and `manifest_config` are top-level values. Built-in node-ctl explicitly performs final declarative validation, then runtime resolution reads the environment/YAML encryption key. Custom conductor validates after Configure; Runtime providers may supply key/TLS/object-store material. A provider overrides environment/YAML and never falls back after failure. Helper executable paths (`sandbox-ctl`, `connector-ctl`, `flatten-ctl`) are outside public Config. Resolution starts at the original node-ctl's exact adjacent release directory, retaining PATH fallback when a helper is absent.

| Field | Default | Meaning |
|---|---|---|
| `api.domain` | Required | Service domain, e.g. `sandboxes.example.com`; control plane is `api.<domain>` |
| `api.listen` | `:443` | Northbound listener; development can use plaintext h2c on `:3000` |
| `api.tls.cert/key` | Empty | Wildcard certificate for `*.<domain>` and `api.<domain>` (§12); empty means plaintext. A non-nil custom Runtime TLS provider is authoritative; core still fixes TLS version/ALPN/client-auth policy |
| `proxy.park_timeout` | `30s` | Request parking budget while waiting for route synchronization or paused-sandbox resume ([node-proxy.md](node-proxy.md) §4) |
| `proxy.auth` | `enforce` | Data authentication policy: `off`/`log`/`enforce`, checking `X-Access-Token` ([node-proxy.md](node-proxy.md) §6) |
| `proxy.metrics_listen` | Empty/off | Conductor's global Prometheus text endpoint; Proxy's separately configured `metrics_listen` aggregates worker data-plane metrics |
| `encryption_key` | Required in built-in mode | AES-256 keys for stored credential pairs: colon-separated 64-hex values, first active, others retained for old records. Priority: custom Runtime provider > `NODE_CONFIG_ENCRYPTION_KEY` > YAML; provider failure never falls back |
| `manifest_config` | `/opt/sandbox/manifest.yaml` | Shared remote Manifest Store configuration; leave `manifest.key` empty because each task receives its tenant key through environment |
| `paths.conductor_executable` | Empty | Absolute custom conductor executable; empty selects built-in. Config diagnosis checks regular/executable, not group/world-writable and not the same file as node-ctl, without applying diagnostic EUID ownership policy. Runtime root node-ctl accepts only root-owned executables; non-root accepts root or its own EUID. Dispatch executes the same opened/validated FD and never falls back |
| `paths.run_root` | `/run/sandbox` | Small node files plus `runners/`, `sandboxes/`, `builds/`, usually tmpfs; must leave the 107-byte Linux socket pathname budget for the longest phase path at maximum BuildID (§1.6) |
| `paths.base_root` | `/var/lib/sandbox` | Persistent node files and large Sandbox/Build data (§1.6) |
| `paths.db_path` | `<base_root>/node-ctl.db` | SQLite path (§14) |
| `paths.config_socket` | `/run/sandbox/node-ctl.socket` | Local run assignment/result and task/admin/plugin/API planes (§6); connects manifest-key/export/import CLI, Proxy and platform agents |
| `paths.admin_pidfile` | Empty | Multiline PID allowlist for admin, allowing `#` comments; otherwise socket mode 0600 alone |
| `paths.plugin_pidfile` | Empty | Multiline PID allowlist for Proxy/agent plugin registration; otherwise socket mode 0600 alone |
| `units.dir` | `/etc/systemd/system` | Template-unit installation directory |
| `units.runner_pools` / `units.builder_pools` | Absent | Independent ordered pools of `{unit, size}`; see below |
| `units.runner` / `units.builder` | `sandbox-runner@.service` / `sandbox-builder@.service` | Template-unit names |
| `units.runner_pool_size` / `units.builder_pool_size` | `0` / `0` | Idle prestarted RunID unit counts; zero still starts on-demand units through WaitAssignment. Finite execution admission resources permit a nonzero Builder pool; idle units hold no Build claim |
| `units.pool_wait_timeout` | `5s` | Positive budget from StartUnit through entry into WaitAssignment; timeout cleans that RunID and replenishes the pool |
| `units.install` | `true` | False delegates unit installation to operations |
| `sandbox.timeout_sec` | `300` | Default sandbox TTL in seconds |
| `sandbox.usage.enabled` / `.sample_interval` / `.flush_interval` | `false` / `1s` / `5m` | Native lifecycle accounting policy for image cold, `run --from` and `run --restore`; strict validation, excluded from portable artifacts and independent of telemetry. See [Native usage](node-usage.md) |
| `sandbox.dead_ttl` | `24h` | Positive Go duration retaining completely cleaned, owner-free dead diagnostic rows |
| `sandbox.resources.capacity.cpu` / `.memory` | `2` / `2GiB` | Guest-visible VM ceiling/SKU; E2B cpuCount/memoryMB still mean capacity. img cold starts accept create/group overrides; artifact capacity constrains restore |
| `sandbox.resources.allocatable.cpu` / `.memory` | Final capacity CPU / inherited `256MiB` when absent | CPU is relative scheduling weight, not a hard fractional-core guarantee; memory is settled guest headroom, not total Budget. Omitted node memory can clamp to final capacity, but explicit operator/custom values, even `256MiB`, must not silently clamp. Out-of-range values fail; request pointer semantics are unchanged |
| `sandbox.resources.startup.memory` | Final capacity memory | Headroom before the first trusted cold report, in static and dynamic modes; independent of settled headroom and unused for initial restore Budget |
| `sandbox.resources.overhead.memory` | `32MiB` | Node-owned host VMM overhead; sandbox-ctl is outside VMM cgroup. Requests/templates cannot override |
| `sandbox.resources.watermark_high.ratio` | `0.875` | Node-owned memory.high pressure ratio in `(0,1)`; requests/templates cannot override |
| `sandbox.network.switch` | `sw0` | Vswitch name |
| `sandbox.network.hostname` | `sandbox` | Guest sethostname and `/etc/hosts` entry (§11) |
| `sandbox.network.dns` | `[169.254.169.253]` | Guest `/etc/resolv.conf` nameserver; deployment must route it to actual DNS |
| `sandbox.network.e2b` / `.bare` | `169.254.0.21/30` + `169.254.0.22` / `169.254.1.1/31` + `169.254.1.0` | Per-profile `{inner_ip,nexthop}` reused inside guests; floating IP uniquely identifies a sandbox. e2b's /30 and gateway support envd port forwarding |
| `sandbox.boot.kernel` | — | vmlinux path |
| `sandbox.boot.runtime` | — | Single guest runtime Bundle: offset-zero EROFS plus digest-marker ZIP, containing envd, flatten-ctl and mkfs.erofs (§11) |
| `sandbox.boot.overlay_diff_template` | — | Preformatted empty ext4, sparsely copied to img cold-start writable upper. An unformatted diff is rejected; deployment supplies a sparse file formatted with mkfs.ext4. Restore obtains the layer graph from artifacts |
| `checkpoint.mode` | `local` | Local paused capture: role tarstream or multi-Manifest ZIP Bundle; output stays in Sandbox BaseDir/checkpoint (§1.6, §8.1) |
| `checkpoint.merge_ref` / `.drop_caches` | Unset | Node tri-state Pause policy. Explicit true/false reaches sandbox-ctl snapshot; omitted/YAML null uses sandboxer's default |
| `checkpoint.remote.ref_location_parent` | Empty | Optional absolute hostless file URI. Named-location parent for Build checkpoint graphs/export-sandbox and located Build image Bundle resolution; does not change Pause mode |
| `checkpoint.remote.manifest` | `false` | False sends image-class outputs to Manifest Store. True instead materializes single-root Manifest Bundles at the required named parent; it does not change checkpoint mode or checkpoint-class policy ([Build §5](node-build.md#5-target-aware-execution-and-publication)) |
| `mmds.enabled` | `false` | envd authentication posture (§9.2, node-proxy §7): false uses -isnotfc and the Proxy gate; true uses FC mode and MMDS re-key |
| `mmds.listen` | `127.0.0.1:19254` | MMDS listener, targeted by vswitch --mgmt-service translation |
| `mmds.routes.enabled` | `false` | Accept tenant static/secret/service exact routes; requires mmds.enabled |
| `mmds.routes.max_routes_per_sandbox` | `32` | Maximum routes for each Sandbox or Build |
| `mmds.routes.max_namespace_bytes` | `65536` | Maximum bytes in one Header/metadata MMDS JSON document |
| `mmds.routes.max_static_body_bytes` | `16384` | Maximum UTF-8 bytes in one static data value |
| `mmds.routes.max_secret_value_bytes` | `16384` | Maximum initial secret string/admin PUT opaque body |
| `mmds.routes.reserved_path_prefixes` | `[/latest/api/, /internal/]` | Exact-path prefixes reserved from tenant routes; built-in `/` is also reserved |
| `mmds.services` | Empty | Conductor-only local service registry, name.endpoint; V1 requires absolute unix:// endpoints, with no duplicate proxy.yaml registry |
| `cluster.node_link.endpoint` | Empty | Registry node-link address (§10); empty means standalone |
| `cluster.node_link.tls` | Empty | Node-link mTLS cert/key/CA; production requires it (§10 and cluster.md) |
| `cluster.node_id` | Required in cluster mode | Unique node registration identity |
| `cluster.labels` | Empty | zone/pool/slot/node labels matched by Placer nodeSelectors |
| `cluster.api_endpoint` | Required in cluster mode | Explicit advertised conductor host:port; never inferred from api.listen |
| `cluster.data_endpoint` | Required in cluster mode | Explicit advertised Proxy host:port; never inferred from its bind listener |
| `resource_listen` | Absent/not embedded | Sole controller endpoint source: socket resolves to absolute bind Listen and canonical SocketIdentity for ownership/inventory/lease/Sandbox YAML. Clients reach the same socket inode through its canonical path. enabled and tuning are in node-resource §3.2. Omitted/disabled means static cgroups |

The primary use case is [NUMA deployment (§5.3)](#numa-deployment): distinct templates carry per-node CPU/memory placement, and new executions are assigned round-robin. The repeated-template example below illustrates configuration semantics, not distinct NUMA bindings.

Each `units.runner_pools` / `units.builder_pools` entry contains only `unit` and
`size`. Each entry creates an independent pool, including repeated templates and
completely identical entries. `size >= 0` is the target number of idle prestarted
workers, not execution capacity or round-robin weight; zero participates and
starts on demand. `dir`, `install` and `pool_wait_timeout` are shared.

```yaml
units:
  dir: /etc/systemd/system
  install: true
  pool_wait_timeout: 5s
  runner_pools:
    - {unit: sandbox-runner@.service, size: 8}
    - {unit: sandbox-runner@.service, size: 4}
    - {unit: sandbox-runner-special@.service, size: 0}
  builder_pools:
    - {unit: sandbox-builder@.service, size: 2}
    - {unit: sandbox-builder@.service, size: 0}
```

When a list is absent, that kind uses its legacy `runner` / `runner_pool_size` or
`builder` / `builder_pool_size` as one pool, retaining the template and size-zero
defaults. Runner and Builder can independently use old or new input. Explicit
empty lists, invalid entries and explicit old/new fields for the same kind
(including an explicit old size of zero) are rejected; inserted defaults are not
mixed input. Compatibility covers reading old configurations in the new binary.

### 3.1 Statically customized conductor

The complete custom Conductor construction, protected configuration, runtime binding and provider contracts are maintained in [Extensions](extensions.md#conductor-bootstrap). Authorization, persisted records and lifecycle invariants remain core-owned and cannot be bypassed by configuration hooks.

### 3.2 Statically customized Proxy

The complete Proxy configuration/binding lifecycle, Master/Worker hooks, public route sources and authorized-forwarding SDK are maintained in [Extensions](extensions.md#proxy-bootstrap). Core process, shared-memory and cancellation invariants remain in [node-proxy.md](node-proxy.md).

Build configuration and its legacy-schema boundary are defined in [Build configuration](node-build.md#3-build-configuration).

There is no node-global remote-memory prefetch switch. Each Sandbox's kuasar-sandbox.restore namespace selects it (§4.4).

Configuration consistency requires proxy.auth=enforce when mmds.enabled=false, since nonsecure envd relies on Proxy's data gate. mmds.routes.enabled requires mmds.enabled; service endpoints must be absolute Unix socket URIs. Only proxy.yaml configures proxy_netns and worker count. Cluster endpoint requires node_id plus separate api_endpoint/data_endpoint. With resource_listen.enabled, startup resolves one canonical controller identity and writes it into every dynamic Sandbox YAML; there is no second sandbox.resources.control_socket source. Startup headroom has identical static/dynamic semantics, independent of controller enablement.

## 4. e2b API contract

Base URL is `https://api.<domain>`. Authentication accepts **X-API-KEY** for SDKs or **Authorization: Bearer** for CLI builds, parsing both identically. APISecret signs keys through e2b-key-ctl gen-apikey; conductor verifies their MAC to identify tenants without a static api_keys table (§7).

**Ownership:** ID-based operations verify the API-key MAC against the resource row's decrypted APISecret. Mismatch returns **404**, hiding another tenant's existence. Create/build/import additionally require the complete APISecret/ManifestKey pair to be allowlisted, otherwise **403**.

### 4.1 Control plane: sandbox lifecycle

| Operation | Method and path | Contract |
|---|---|---|
| Create | POST /sandboxes → 201 | Body templateID/timeout/metadata/envVars/optional autoPauseMemory plus optional X-Kuasar-Sandbox-* headers. Omitted/null/true autoPauseMemory captures S at TTL; false captures E, without changing explicit Pause's default. 201 is durable starting acceptance and does not wait for runner/runtime/envd. e2b returns Envd/Traffic/Forward tokens; bare returns Forward only |
| Get | GET /sandboxes/{id} | Includes state/startedAt/endAt/metadata |
| Usage stats | GET /sandboxes/{id}/stats/usage | Shared native current/saved/history reader; lossless integers and coverage, online owner/offline locks, no Wake or sampling |
| Resource stats | GET /sandboxes/{id}/stats/resource | Read-only effective resource specification and host VMM counters, with observed node reservation; sparse JSON, no guest access |
| Metrics history | GET /sandboxes/{SandboxID}/metrics?start=...&end=... | Exact SandboxID ownership, opaque live telemetry UDS forwarding; 503 when unavailable, no Wake/Resume; [E2B contract](telemetry.md#6-e2b-history-query) |
| Traffic stats | GET /sandboxes/{id}/stats/traffic | Final node Proxy's current parking/connected and conservative idleSince; no Wake/Resume |
| List | GET /v2/sandboxes | Tenant-scoped state/limit/nextToken query. Omitted state lists running/paused; explicit states support diagnosis. x-next-token pagination; items include cpuCount/memoryMB/diskSizeMB and ISO-8601 startedAt/endAt. CPU/memory retain capacity/SKU meaning, not headroom |
| Kill | DELETE /sandboxes/{id} → 204 | Non-owner returns 404. Atomically transfer complete ownership into deleting, exclude from cache/full snapshots and publish route Delete before response. Finalizer cancels launch, fences runner, detaches under allocation fence, exactly clears durable network ownership, removes RunDir/BaseDir and hard-deletes row. Route Delete only withdraws projection; pending repeats are idempotent |
| Resume | POST /sandboxes/{id}/connect | Body timeout in seconds and optional bool/null memory. Kuasar maps nil to auto, true to memory, false to cold. Paused atomically becomes starting with durable launch_mode before response. Missing targets may synchronously import paused state from X-Kuasar-Migration-Token, then use the same admission. Response does not wait for launch |
| Exec session | POST /sandboxes/{id}/exec-sessions → 201 | X-API-KEY only. Mint execAccessToken; optional ttlSeconds, CEL conditions and X-Kuasar-Migration-Token. Does not create a guest process |
| Pause | POST /sandboxes/{id}/pause → 204 | Omitted/null/true memory saves Snapshot S; false saves Sandbox E. False with snapshot-only merge/drop fields returns 400; already paused or starting returns 409 |
| Timeout | POST /sandboxes/{id}/timeout | Body timeout in seconds resets TTL; starting permits a narrow-field update |

Compatibility is bounded by the upstream API/SDK versions and actual tests. The upstream [Create](https://docs.e2b.dev/api-reference/sandboxes/create-sandbox), [Pause](https://docs.e2b.dev/api-reference/sandboxes/pause-sandbox) and [Connect](https://docs.e2b.dev/api-reference/sandboxes/connect-to-sandbox) references checked on 2026-09-07 document autoPauseMemory and memory selection, including Connect memory=false. Thus the field is not exclusively a Kuasar extension. Kuasar's nil→source-dependent auto behavior and its existing authorized-traffic Wake of both E and S are specific local semantics; upstream documents restrictions on traffic-triggered resume of filesystem-only snapshots. Kuasar does not claim the complete E2B autoResume policy.

Create templateID accepts a persistent ID ([Build §2](node-build.md#2-template-ids-and-artifact-authority)), registration-time transient ID, or a ready Build's name/alias. The latter two resolve to persistent ID before the common path. envdVersion is `0.6.1` for e2b and compatibility stub `0.1.0` for bare (SDKs require at least 0.1.0). Bare has no envd or Envd/Traffic tokens. Both profiles return a separate forwardAccessToken signed at creation and persisted with the Sandbox.

Create preserves the existing response body without adding state; response/cache/background worker use distinct copies. Get can show starting or cleanup-pending deleting. Default List still selects running/paused; explicit starting/deleting/dead supports diagnosis. Running Connect is idempotent. Starting Connect applies only explicit timeout updates and starts no duplicate attempt. Paused Connect durably accepts resume before returning, so success may correspond to starting but never the old paused state.

An explicit deadline intent on paused/starting survives a failed resume back to paused within one conductor process, and is consumed only after exact-run commit to running. V1 has no persistent intent discriminator. Reconcile can infer an interrupted starting resume and conservatively restore intent in that process, preventing a following ordinary Wake/Connect from overwriting it with node defaults. If it had already returned to paused before another conductor restart, the database cannot distinguish that intent from ordinary paused/default-rearm state. Exact preservation across multiple restarts needs the separately approved schema work in #139; #135 does not hide it in an implicit encoding or sidecar.

Exec session is explicit authorization, not a server session object or guest process. Its strict JSON object accepts int64 ttlSeconds and conditions objects containing expr:

```json
{
  "ttlSeconds": 3600,
  "conditions": [
    {"expr": "request.argv == ['/usr/bin/python3', '/workspace/task.py']"},
    {"expr": "request.cwd == '/workspace'"},
    {"expr": "request.user == '1000:1000' && !request.stdio.tty"}
  ]
}
```

Omitted conditions or [] means unrestricted, normalized to nil and omitted from token payload. Explicit null, non-arrays, unknown/duplicate fields, non-object elements, unknown/duplicate element fields or empty expr return 400. Conditions combine with AND; use `||` within one CEL expression for OR. The entire raw body, including trailing whitespace, is capped at 64 KiB. A second JSON value, negative/out-of-range TTL fails before lifecycle side effects; 64 KiB+1 returns 413. Omitted/zero ttlSeconds gives no expiry. Positive values calculate exp at actual mint time; Unix-second or time.Time overflow returns 400.

Before compiling caller-controlled CEL, standalone node performs side-effect-free credential preflight: an existing target checks resource-bound APISecret; a missing KMT-import target checks an allowlisted pair. After compilation, prepareStandaloneTarget authoritatively validates again against concurrent changes before possible import/resume. CEL exposes only typed request: argv list(string), env map(string,string), cwd/user strings and stdio tty/stdin/stdout/stderr booleans. It exposes no route, claims, metadata, clock, filesystem, network, secrets or I/O/side-effect functions. Before minting or paused→starting, node compiles, requires boolean type and applies source/AST/cost bounds. Unknown fields, non-booleans and excessive bounds fail. Existing eager exec-session activation remains; deferred activation is separate #240 work.

Compilation precedes target preparation and lifecycle mutation. The SID fence then waits for the previous attempt's terminal cleanup, mints using actual current time, and only afterward permits a new paused→starting transition. Fence waiting does not consume token TTL; compilation/signature failure starts no new launch.

Existing targets ignore migration token. Missing targets may synchronously import, validating the object, credential binding and profile; node then signs KAT with the row's ServiceSecret. MigrationToken is transient input, never stored in Sandbox business rows, logs or errors, and is discarded after use. Paused targets accept asynchronous resume without waiting for READY. Successful response sets Cache-Control: no-store and contains exactly:

```json
{"execAccessToken":"kat1.<payload>.<signature>"}
```

It returns no session ID, expiry, ServiceSecret or other route/credential fields. Authentication retains existing 401/403; missing/non-owned sandboxes return 404. Synchronous import, credential read/mint or asynchronous-resume admission failures expose sanitized 503, omitting fingerprints, ServiceSecret, NodeSandboxID, socket paths and KAT payload.

#### 4.1.1 Instantaneous resource and traffic stats

Both endpoints first read the Sandbox business row and verify API-key ownership; failure returns 404. They observe without Connect/Wake/Resume/Pause/envd calls and set Cache-Control: no-store.

Resource stats read the current sandbox-ctl owner's effective resource specification and host VMM cgroup through its existing control socket. Conductor adds the embedded resource controller's reservation when available:

```json
{
  "cpuCapacity": 2,
  "cpuAllocatable": 0.5,
  "memoryCapacity": 2147483648,
  "memoryHeadroom": 268435456,
  "memoryReserved": 1073741824,
  "memoryUsed": 536870912,
  "cpuSeconds": 12.345678,
  "timestampUnix": 1786482600
}
```

`cpuCapacity` is `capacity.cpu` in cores. `cpuAllocatable` is the existing relative scheduling specification mapped to `cpu.weight`, not a fractional-core hard quota or performance guarantee. `memoryCapacity` is `capacity.memory` in bytes. `memoryHeadroom` is the final effective `resources.allocatable.memory`: the balloon controller's headroom, distinct from Budget, guest free memory and NodeReservation. `memoryReserved` is the observed reservation actually charged to the node; it is omitted when the dynamic controller or its observation is absent.

`memoryUsed` reads `memory.current` and `cpuSeconds` reads `cpu.stat.usage_usec / 1e6` from the same pinned host VMM cgroup. These values do not add the ctl process or guest CPU, subtract inactive file/balloon memory, or cap host memory at guest capacity. CPU seconds are cumulative for that current source and can reset when it is rebuilt; lifecycle accumulation belongs to native usage. The native JSON preserves integer bytes and emits CPU seconds as an exact decimal with microsecond precision. Consumers converting these numbers to binary floating point may lose precision.

Each host observation has independent validity: a valid zero is returned, while missing memory or CPU observations are omitted. `timestampUnix` is the actual read time and is omitted if neither host value exists. It is not a refreshed heartbeat/cache timestamp. Effective specifications remain readable from an owner without a live VMM, but no current host observation is fabricated. Starting/running without a reachable owner returns 503; paused and other states without a current runtime return 409. Concurrent runtime or binding replacement invalidates the read.

This works with static or dynamic resource control, with usage disabled and with telemetry stopped. Reading does not Wake, sample/save usage, call the guest or Cloud Hypervisor, or change control policy. It creates no resource history. The former native `cpuCount`, `memTotal`, `memAllocatable` and `memUsed` fields are removed; the old reservation-valued `memAllocatable` is replaced by `memoryReserved`, while `memoryHeadroom` is a separate explicit concept. E2B `/metrics` and list/SKU compatibility fields retain their existing meanings.

Traffic stats report authenticated logical ingress accepted by final node Proxy:

```text
ingress = parking + connected
parking = Authenticated, but activation/final backend dial has not completed
connected  = Final node proxy→Sandbox backend is established and not finally closed
```

```json
{
  "state": "running",
  "maxInflight": {
    "total": 128,
    "forward": 96,
    "e2b:envd": 0,
    "e2b:code-interpreter": 0,
    "exec": 8
  },
  "inflight": {
    "parking": 0,
    "connected": 0
  },
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "forward": {
      "parking": 0,
      "connected": 0,
      "idleSince": "2026-08-12T14:03:21.123456789Z"
    },
    "exec": {
      "parking": 0,
      "connected": 0,
      "idleSince": "2026-08-12T14:00:00Z"
    }
  },
  "platform": {
    "rxPackets": 57,
    "rxBytes": 5108,
    "txPackets": 39,
    "txBytes": 8042
  },
  "transit": {
    "rxPackets": 2,
    "rxBytes": 196,
    "txPackets": 3,
    "txBytes": 294
  },
  "egress": {}
}
```

maxInflight comes from master’s currently applied route and destination-node effective policy, not conductor's row. An all-zero object means unlimited. Configured M is an approximate whole-node per-Sandbox ceiling; with N workers the transient theoretical bound is M+N-1, not M per worker.

e2b services are forward/e2b:envd/e2b:code-interpreter/exec; bare uses forward/exec. Top-level inflight sums services. Each service includes idleSince only when both counters are zero. Top-level idleSince appears only for running with all-zero counts and is the maximum of all applicable service idle times. Starting/paused returns inflight but no top-level idle. The API omits idle boolean/duration, last open/close, cumulative connection counts, rates, latency, port details and worker information.

Conductor queries the current trusted Proxy registration's stats_socket master cache, without worker fan-out. Unregistered master, unsynchronized route, mismatched RunID/profile/state, failed worker stream or unready replacement returns 503. This stats window does not alter master route/admission authority or Create barrier. The complete shared-admission algorithm, error proof, worker state machine, absolute snapshots and failure windows are in [node-proxy.md](node-proxy.md) §8.

The response is flat: `state`, `maxInflight`, `inflight`, `idleSince`, `services`, `platform`, `transit`, and `egress`. Conductor owns this native API and composes the existing Proxy observation with the current Sandbox-to-switch/port binding. `platform` maps connector Mgmt RX/TX packets/bytes; `transit` maps Transit RX/TX. Both use the sandbox viewpoint. Each configured current port supplies all four unsigned integer counters, including valid zero. A sandbox without an attached port has empty `platform`/`transit` objects, meaning no applicable current observation. `egress: {}` always means no publishable egress statistics; it does not mean observed zero traffic. There are no per-service packet counters, source groups or network API alias.

`connected` replaces the former native `inflight.egress` and `services[*].egress`, preserving the established-backend-until-final-Close meaning. Top-level `idleSince` describes only admitted Proxy ingress; management monitoring packets do not refresh it and it makes no claim about sandbox computation or network idleness. Packets are packet counts, not application request counts; bytes are observed frame bytes, not throughput. Management uses the existing port/management ingress frame length; transit uses the frame before encapsulation or after removal of outer GENEVE headers, retaining Ethernet. Different observation points are not added into a traffic total. The cumulative counters belong to the current attachment and can reset on reuse; the API creates no network history.

Conductor batches at most 64 current ports per switch through connector's Go `Stats(ports)` API, with no per-sandbox CLI processes or second lifecycle authority. It reuses its allocation/detach fence; connector reuses the pin-directory shared lock, current pinned-map ID and reset-confirmation flag. The existing Proxy stats socket also accepts a bounded batch. Configured-source read errors, unconfirmed reset, control contention, incomplete results or changed bindings return 503 for the whole read, never zero or an older complete-looking response. The same domain reads serve the public API, trusted local batch and conductor extension. Stats remains available independently of telemetry and does not participate in Create/Resume readiness.

Native lifecycle accounting is available at `/sandboxes/{id}/stats/usage`, including paused objects. It has its own current/saved/history selection and preserves the native lossless record rather than replacing it with resource counters. See [Native usage and trusted batch reads](node-usage.md).

#### 4.1.2 Create identity

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
independently enforces the same 1..57-byte format (§7).

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

See [Cluster Router](cluster-router.md) and
[Extensions](extensions.md) for the surrounding contracts.


### 4.2 Data protocols: envd and native exec

envd uses port **49983**, HTTP/1.1 plus h2c and unversioned Connect-RPC process.Process/filesystem.Filesystem packages. Filesystem RPC handles metadata; contents use GET/POST /files with signed query. Other endpoints include /health, /init and /metrics. Per-operation user uses `Authorization: Basic base64("user:")`. Code-interpreter POSTs NDJSON to `https://49999-<sid>.<domain>/execute`, reaching guest FastAPI on 49999 and Jupyter on 8888. Envd exec is POST /process.Process/Start with X-Access-Token. Tenant traffic is forwarded, not reimplemented; Build supplies a minimal hand-written connect+JSON process.Start client for steps/startCmd/readyCmd, without generated protobuf/gRPC stubs ([Build §5](node-build.md#5-target-aware-execution-and-publication)).

sandbox-ctl --connect maps guest envd/CI control ports to host `<rundir>/envd.sock` and `ci.sock`. Proxy reaches these UDS paths directly without floating-IP routing or a cross-sandbox path.

Native exec uses a separate ctl protocol. Clients obtain ExecAccessToken (§4.1), then establish service-addressed CONNECT:

```http
CONNECT sandbox:443 HTTP/1.1
E2b-Sandbox-Id: <NodeSandboxID>
E2b-Sandbox-Service: exec
X-Access-Token: kat1.<payload>.<signature>
```

Authority port 443 is a transport placeholder, not a guest port. Even an accompanying E2b-Sandbox-Port cannot select exec backend. service=exec requires CONNECT and always enforces KAT regardless of ordinary off/log/enforce policy. After CONNECT 200, final Proxy reads and authorizes the complete exec_request first frame. Only successful conditions permit paused activation, ctl.sock dial and exact raw-frame forwarding; see [node-proxy.md](node-proxy.md) §5.

### 4.3 SDK/CLI integration and protocol pins

- Production TLS uses E2B_DOMAIN and E2B_API_KEY. Development uses HTTP/h2c E2B_API_URL/E2B_SANDBOX_URL. Control Host must match api.*.
- API keys are `e2b_` plus 72 hex digits, 76 characters total. SDK syntax checking uses `/^e2b_[0-9a-f]+$/`; server separately checks MAC (§7).
- Envd is pinned through e2b-dev/infra release tarball ENVD_TARBALL in guest-runtime/native-deps, default tag 2026.22 and envd 0.6.x. The recorded SDK compatibility baseline is JavaScript e2b 2.27.x and Python 2.25.x; this is not a claim about every later version.
- X-Access-Token carries envdAccessToken. Secure Sandbox behavior is enabled by default from SDK v2.0.0, which attaches it to data requests.
- Routesync for Proxy/observers is version 8: four-byte little-endian length followed by JSON. Messages include register/hello/upsert/delete/bookmark/wake/route_barrier/route_barrier_ack, over PUT /internal/plugin/{id}/register on config-socket's plugin plane (§6; node-proxy §4).

### 4.4 Sandbox configuration propagation

Each instance receives configuration through reserved e2b metadata namespaces `kuasar-sandbox.<ns>`, each encoded as a JSON object string, without SDK/API changes. These typed tenant-facing schemas define the permitted subset and are rendered into sandboxer configuration; conductor does not blindly import a complete runtime YAML as tenant policy.

| Namespace | Destination |
|---|---|
| resource | Strict partial resources patch: capacity CPU/memory, allocatable CPU/memory and startup memory |
| traffic | Host-only explicit per-Sandbox max_inflight total/forward/e2b:envd/e2b:code-interpreter/exec; never guest configuration |
| network | hostname/nexthop to guest; inner_ip/transit_* to vswitch Attach; dns to resolv.conf |
| launch | exec/args/env/workdir/restart/user/stop_signal/plugin/cgroup_control; bare only, since e2b reserves launch for envd |
| init / mounts / files | Typed init[]/mounts[]/files[] forwarding |
| metadata | Runtime SANDBOX_CONFIG metadata, e.g. e2b.start_cmd |
| restore | Current host prefetch policy: optional, explicit off/memory only |
| credentials | Create-time ServiceSecret and Envd/Traffic overrides, separated from ordinary metadata and never guest config |
| checkpoint | Host-only Create-local Pause defaults: merge_ref/drop_caches true/false/null; Sandbox row only, not runtime YAML/snapshot.cfg |
| mmds | Portable exact routes plus request-scoped initial secrets; split before persistence so metadata retains routes only |

Resource and traffic merge by leaf. Public resource JSON permits only:

```json
{
  "capacity": {"cpu": 2, "memory": "8GiB"},
  "allocatable": {"cpu": 0.5, "memory": "256MiB"},
  "startup": {"memory": "1GiB"}
}
```

The value must be one JSON object. Unknown fields, null, arrays/scalars, trailing JSON, explicit zero/negative CPU, empty/invalid/nonpositive memory return 400 with full kuasar-sandbox.resource path. Requests/templates/groups/headers/migration tokens cannot carry allocatable.deflate_on_oom, overhead, watermark_high, control or sensor. Node/runtime own them, including injected watermark_high.ratio. Valid patches persist as compact canonical JSON.

Resource leaf priority is fixed:

```text
node resource policy
  < cluster group defaults
  < create / reserve body resource
  < X-Kuasar-Sandbox-Resource
  < E2B first-class cpuCount / memoryMB (Override only the corresponding capacity leaf)
  < portable artifact capacity constraint
```

The five leaves overlay independently: group capacity.memory and Create allocatable.memory both survive. Build-row metadata is not another Create layer. Other namespaces follow their own rules below, generally whole-namespace replacement. Every layer is parsed strictly before merging, so a valid higher layer cannot hide an invalid lower one. Group/reserve, standalone/cluster and Build registration share the helper.

Build traffic is request-scoped to the Build runtime and its synthetic route; it is not inherited by Sandboxes created from the output artifact. Canonical TemplateID Create never queries retained Build metadata, a Template catalog, or artifact metadata for traffic defaults. Traffic leaf priority is:

```text
cluster group explicit metadata (when present)
  < create / reserve body metadata
  < X-Kuasar-Sandbox-Traffic
```

JSON is shaped as `{"max_inflight":{"total":32,"exec":2,"forward":0}}`. Leaves may be absent; explicit zero clears lower-priority patches or destination Proxy defaults. An absent metadata key remains absent; destination master fills from local traffic.max_inflight without storing defaults in Sandbox rows, MigrationToken or Registry. Migration retains absence or exact explicit patches and uses destination defaults for remaining leaves. Unknown/duplicate/null/negative/noninteger/uint32-overflow values return 400. Bare rejects explicit envd/CI service limits, though node defaults may include them; bare consumes only total/forward/exec. This limits inflight concurrency, not QPS/global capacity. See [node-proxy.md](node-proxy.md) §8 for algorithm and error bounds.

Resolver determines capacity, then allocatable/startup, then adds node-only overhead/watermark/deflate/controller. Inherited node allocatable memory may clamp to final capacity; explicit node/request values cannot. Startup is independent: explicit request startup must be in `(0,capacity.memory]`; node startup exceeding final capacity clamps; if both absent, use capacity. Static/dynamic both render startup, with static omitting controller only. When settled policy needs ballooning (capacity memory exceeds allocatable), YAML writes deflate_on_oom=true. A nonzero cold startup target alone also creates a balloon device using sandboxer's effective-true default.

MMDS uses this shared Create/Build Register schema. Headers contain JSON directly; metadata values contain JSON strings:

```json
{
  "secrets": {
    "key1": "data1",
    "key2": "data2"
  },
  "routes": [
    {
      "path": "/path/to/secret",
      "type": "secret",
      "secret": "key1",
      "content_type": "application/json"
    },
    {
      "path": "/path/to/relay",
      "type": "service",
      "service": "external-mmds"
    },
    {
      "path": "/path/to/data",
      "data": "value",
      "content_type": "application/json"
    }
  ]
}
```

Omitted type means static. Static requires path, permits data/content_type and forbids secret/service. Secret requires path/secret, permits content_type and forbids data/service. Service requires path/service and forbids data/secret/content_type. Static/secret response Content-Type defaults to text/plain only at response time; service uses the validated local service response type. Initial values must be JSON strings and each name must be referenced by a secret route; a route may initially lack a value.

Header and metadata are independently strictly parsed as partial documents, then merged by top-level key:

```text
effective.secrets = Header.secrets if present, else metadata.secrets
effective.routes  = Header.routes  if present, else metadata.routes
```

A key is neither concatenated nor recursively/path-merged. Explicit empty routes/secrets counts as present and clears lower input. Both documents reject unknown/duplicate fields, trailing values, malformed JSON and oversized input.

Validation immediately separates portable routes from confidential values. An exact absolute path must be usable byte-for-byte as an HTTP request target. Query/fragment/percent escapes, characters requiring percent encoding, empty/dot segments, backslash, trailing slash, wildcard, duplicate and reserved-prefix collisions are forbidden. Persistence uses stable minimal JSON: remove explicit static type; preserve absent type/content_type; add no runtime defaults or external version:

```text
input:     {"routes":[{"path":"/data","type":"static","data":"x"}]}
metadata:  {"routes":[{"path":"/data","data":"x"}]}
```

Metadata kuasar-sandbox.mmds never contains secrets. Build-specific scope and terminal cleanup are in [Build §3.1](node-build.md#31-request-scoped-builder-input).

Equivalent per-Sandbox prefetch inputs are:

```http
X-Kuasar-Sandbox-Restore: {"prefetch":"memory"}
```

```json
{"metadata":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}"}}
```

Restore accepts only a strict JSON object; unknown fields, nonstring prefetch or invalid enum fails before lifecycle side effects. Omission defaults off; Header overrides metadata in that request. Orchestrator does not decide local/remote, single/multiple layers or backend capability. It renders explicit memory unchanged; sandboxer executes or skips it.

Credential overrides likewise have two entries:

```http
X-Kuasar-Sandbox-Credentials: {"service_secret":"<64 lowercase hex>","envd_access_token":"...","traffic_access_token":"..."}
```

```json
{"metadata":{"kuasar-sandbox.credentials":"{...}"}}
```

A complete Header credentials object overrides metadata wholesale. Only service_secret/envd_access_token/traffic_access_token are allowed; unknown/duplicate/null/nonstring/trailing values fail. Bare rejects Envd/Traffic override with 400. Each opaque token must be valid UTF-8 and at most 256 bytes. Absent ServiceSecret derives from APISecret and StableID(); Forward token is always generated from final ServiceSecret and cannot be caller-supplied. Credentials are removed from ordinary metadata immediately after validation.

Checkpoint policy accepts Create Header or metadata:

```http
X-Kuasar-Sandbox-Checkpoint: {"merge_ref":false,"drop_caches":null}
```

```json
{"metadata":{"kuasar-sandbox.checkpoint":"{\"merge_ref\":true,\"drop_caches\":false}"}}
```

Both use the same strict object parser allowing only merge_ref/drop_caches and true/false/null. Unknown fields, wrong types or trailing/second values return 400. Header overrides by field; null/absence means no override, not clearing a lower value. If all fields remain null/absent, remove the namespace. Source templates do not supply it. Build checkpoint input is defined in [Build §3.1](node-build.md#31-request-scoped-builder-input). Local/Bundle use identical parsing, precedence and side-effect boundaries.

kuasar-sandbox.cluster is not tenant configuration. Cluster Build group uses a separate durable system field, outside portable metadata. Ordinary Sandbox Profile/Group/RouteKey/optional StableID arrive through structured node-link context and persist separately. Node events do not echo Registry-owned group/route key/authentication identity; Registry restores them from ownership records.

The complete request-scoped `kuasar-sandbox.builder` input and target rules are in [Builder input](node-build.md#31-request-scoped-builder-input). The rules below continue to govern shared Sandbox configuration.

- **Rendering:** cold images use a pure resolver to create complete ResourcesConfig before Attach/Assign. Artifact launch resolves capacity from the task summary in the sole launch worker; conductor never opens the snapshot. Renderer installs the canonical object without reinterpreting fields. YAML has no control.cgroup_path; run-sandbox adds inherited cgroup-FD capability at final exec. Dynamic controller comes only from resolved resource_listen.SocketIdentity; node injects watermark_high.ratio and leaves sensor absent. Boot/tapfd/resolved network and allowed tenant fields follow the config-socket handoff.
- **Two injection surfaces:** e2b metadata and X-Kuasar-Sandbox-* Headers normalize at the API edge. Resource/traffic use leaf precedence; MMDS/checkpoint use the special merge rules above; remaining namespaces generally use whole-object Header precedence. Create runtime inputs use sandboxes.metadata_json. The shared parser, target admission and Build persistence rules are defined completely in [Build §3.1](node-build.md#31-request-scoped-builder-input). Cluster Sandbox ownership uses separate system context (§10).
- **Precedence:** resources follow the fixed leaf chain; remaining inputs follow their declared namespace rules across node/group/current Create. Canonical TemplateID Create never reads builds.metadata_json. Build resources and final Sandbox resources are independent, without mutual defaults/comparisons/derivation. Trigger cpuCount/memoryMB only assert immutable Build resources.
- **Capacity:** img Create uses request/group/node defaults. sbx/snp Create, paused resume and migration take portable E capacity as authority. Synchronous acceptance checks patch structure only; after runner preparation, explicit matching capacity is an assertion, while any differing leaf causes asynchronous resource_resolve failure. Unreadable capacity fails rather than falling back. Runner is already assigned, but networking/YAML/controller reservation/VM side effects have not begun; the runner delegation cgroup may already exist. Initial restore reservation exactly equals sandboxer's BudgetAtSnapshot derived from captured CH target/current; startup headroom and partial grants do not apply.
- **Network travels with E:** cold rendering records logical network in SANDBOX_CONFIG.metadata[kuasar-sandbox.network], which enters E's sandbox.runtime.cfg. S refers to E; snapshot.cfg itself contains no metadata. Artifact tasks strictly parse/validate it and submit typed bounded network summaries, never raw metadata to conductor. Explicit current fields win over inherited artifact fields. Build template network projection is in [Build §3.1](node-build.md#31-request-scoped-builder-input). MigrationToken also carries metadata. Other runtime namespaces apply at cold start or are captured in memory; host restore/checkpoint policies stay separate.
- **Host policy stays outside artifacts/templates:** restore/checkpoint originate in Create metadata. Image cold boot does not render them into runtime YAML. snp Create, later resume and migration rerender restore policy when restoring S. Checkpoint is read only by host Pause and never enters SANDBOX_CONFIG/snapshot.cfg. Connect/resume offers no temporary override.
- **Persistence:** sandboxes.metadata_json holds launch input. Build metadata/builder JSON are retention-bounded execution records, never a second template authority.

## 5. Process management through systemd template units

At startup, conductor generates each configured template and the fixed sandbox-runner.slice/sandbox-builder.slice under units.dir; identical generated content for the same template is written once, while conflicting content under one name is rejected before writing; it calls D-Bus Reload only when content changes. With units.install=false, operations manages them; conductor neither writes files nor reads or validates operator resource properties. Generated ExecStart uses the exact original node-ctl path retained by runtime resolution/bootstrap, including custom conductor mode (§3).

**Runner unit** (`%i` is RunID):

```ini
# sandbox-runner@.service (Generated; paths rendered from config)
[Service]
Type=exec
WorkingDirectory=/run/sandbox
ExecStart=<node-ctl> run-sandbox --pidfile=/run/sandbox/runners/%i.pid \
          --config-socket=/run/sandbox/node-ctl.socket --run-id=%i
ExecStopPost=/bin/rm -f /run/sandbox/runners/%i.pid
# One process per Sandbox; no retry after a stateful crash
Restart=no
# Default SIGTERM, then SIGKILL after TimeoutStopSec; includes CH (§5.1)
KillMode=control-group
TimeoutStopSec=20
Slice=sandbox-runner.slice
# Delegate CPU/memory controllers (§5.1)
Delegate=yes
```

The complete Builder unit lifecycle and resource ownership rules are in [Build §4.1](node-build.md#41-builder-unit-and-process-lifecycle).

Both ExecStart programs lock the RunID pidfile, then wait for business-ID assignment over config-socket. Runner connects readiness immediately after obtaining SID, locks `<RunDir>/<SandboxID>.pid` and requests exact-run bootstrap. Artifact tasks prepare E/S locally and complete the second stage before obtaining final LaunchSpec, then execve sandbox-ctl run with the same unit PID/cgroup. Type=exec requires no sd_notify. Builder process/child-VM and result behavior is defined in [Build §4](node-build.md#4-task-handoff).

Runner and Builder each select the next list position at the existing Assign
boundary and advance an independent cursor. Only selection holds the cursor
lock; waiting for Assign does not. Registration, rejected Build admission and
already bound RunIDs do not select again. Resume selects when it needs a new
runner. There is no idle preference, failed-pool skipping, cross-pool fallback,
weighting or resource scheduling. One Build stays with one Builder for all phases;
its global registration/execution ledgers, FIFO claims and release order remain
unchanged. NUMA and CPU affinity belong to externally managed units; conductor
does not interpret them or create pool-specific slices or resource limits.

Before StartUnit can run, conductor records RunID → creating pool for exact
WaitAssignment routing, even when pools share a template. A separate RunID →
actual unit association survives assignment until lifecycle cleanup. Durable
binding still precedes publishing assignment. Builder retries first consult the
durable Build binding. Neither RunID format nor the pidfile/config-socket,
Delegate, ctl/vmm and trusted-FD contracts change.

Conductor maintains target idle counts for each pool. Assignment consumes a unit already waiting in WaitAssignment and schedules asynchronous replenishment. With no idle unit, it creates a RunID and uses the same StartUnit/WaitAssignment path. A fixed control loop serializes Start/Stop requests received over a channel; there is no newly spawned start goroutine for every replenishment. pool_wait_timeout covers the full StartUnit-to-WaitAssignment interval. Start failure, wait timeout or canceled waiting connection triggers StopUnit plus ResetFailedUnit, then a new UUIDv7 RunID to refill the pool.

Runner assignment budget begins at pool Assign and covers queuing, on-demand StartUnit and WaitAssignment. After selecting an idle runner, the pool calls the commit callback to bind RunID; only success publishes task ID and returns success. This is assignment's linearization point: later caller cancellation cannot reverse it. Pool loop alone answers pending cancellation/shutdown, exactly once per request.



- **Kill:** under the SID fence, exact-CAS full ownership into deleting, cancel active launch, withdraw cache and publish route Delete. Asynchronous finalization stops the unit and all CH descendants, resets failed state, releases TAPFD or detaches vswitch, removes directories and hard-deletes row. Route withdrawal does not await cleanup. The old launch claim remains until its attempt finishes local cleanup, preventing late CAS resurrection or an early same-SID successor. Guest restart policy cannot block host unit termination.
- **Readiness:** before assigning a runner, conductor binds `<RunDir>/ready.sock` with directory 0700/socket 0600. Runner immediately connects. Conductor waits for artifact completion, readiness EOF, cancellation and absolute deadline together, so a task exiting during root reads fails immediately. The one-shot stream must be `control_ready\nready\nEOF`. Bare is ready then; e2b next sends mandatory POST /init as the first envd request, without /health as a startup gate. Health is for external checks after initialization. Artifact-launch absolute budget begins at successful Assign and covers root read, completion RPC, host resource/network preparation, final-spec wait, exec/startup, runtime wire and /init; final spec does not reset it. Cold fast path retains its post-handoff runtime budget. /init starts immediately, retries only connection/transport errors at 1ms/2ms/4ms/capped-5ms backoff, with at most 50ms per request. Only 204 succeeds; other statuses expose the status code without potentially sensitive response body. Protocol errors, early EOF, cancellation or total timeout fail launch: fresh starting becomes dead; resume returns paused. After durable acceptance but before runner assignment, failure uses the empty-RunID fence. Only /init success commits starting→running with the exact RunID and publishes running route.
- **Liveness authority:** ListUnitsByPatterns over distinct configured runner templates obtains the authoritative active RunID set for reconciliation with stored sandboxes.run_id (§14).
- Host Restart=no and guest envd restart=always under sandbox-init are separate layers.

### 5.1 Cgroup separation and FD capability

The runner unit root is an empty delegation boundary:

```text
sandbox-runner@<run-id>.service/
├── ctl/   node-ctl, then sandbox-ctl after exec
└── vmm/   cloud-hypervisor
```

Before assignment, run-sandbox accepts only membership in its matching RunID unit root or ctl subgroup. From the root it creates ctl, moves its whole process with cgroup.procs and rereads /proc/self/cgroup to confirm identity. Existing ctl supports externally installed units. Both paths verify an empty unit root and cpu/memory delegation, enable controllers there, idempotently create vmm and open its directory FD with CLOEXEC. This requires Delegate=yes, not systemd v254's DelegateSubgroup. After LaunchSpec, the launcher makes that FD inheritable only at final exec and locally appends --cgroup-path=fd=N; LaunchSpec carries no host cgroup path or FD.

sandbox-ctl restores CLOEXEC immediately, configures limits and creates CH atomically in vmm with clone3(CLONE_INTO_CGROUP). memory.high therefore constrains VMM while sandbox-ctl can service UFFD/vsock/signals/reaping under guest pressure. Sandboxer owns high, balloon target/current and grow/shrink ordering; node controller only handles reservation requests. Linux 5.7+ and seccomp permission for clone3 are required. Unsupported hosts fail closed, with no post-start migration/shared-cgroup fallback.

KillMode=control-group covers ctl/vmm recursively. systemd reclaims the delegated subtree after StopUnit; conductor need not move processes or remove cgroups itself. Delegation/hierarchy/controller/FD failures precede CH startup.

### 5.2 Journald and log labels

Conductor constructs independent journald targets for runtime component diagnostics, app stdio and guest console. Every managed Sandbox target explicitly carries `KUASAR_STABLE_ID`, `KUASAR_SANDBOX_ID` and `KUASAR_RUN_ID`; Build phase targets carry `KUASAR_BUILD_ID` and `KUASAR_RUN_ID`. Fields are not inferred from environment, units or artifacts and do not inherit across outputs. Complete identity, encoding, query and delivery rules live in the [journal identity guide](node-journald.md); runtime argument syntax belongs to [sandboxer's journal guide](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/journald.md).

| Label | Writer | Content | Audience |
|---|---|---|---|
| `sandbox-ctl` | Runner/Build phase `--log-to` | Runtime component diagnostics | Host only |
| `sandbox` | Runner `--stdout-to`/`--stderr-to` | Sandbox app stdio | Host only |
| `build` | Build phases/run-builder | App stdio, milestones, envd RUN replay and flatten progress | SDK and host; complete reading/pagination/failure rules in [Build §5](node-build.md#5-target-aware-execution-and-publication) |
| `console` | Runner/Build phase `--console` | Guest kernel dmesg | Host only; excluded from SDK Build logs |

Native streams write directly to journal without temporary log files. Run-builder milestones and envd RUN replay use pure-Go `go-systemd/journal`. `--log-to` does not replace original process stderr: early CLI output, CH stderr, panics and failed-send fallback retain their existing sinks. Builder units set `LogRateLimitIntervalSec=0`; runners keep default rate limits. Neither policy guarantees lossless storage under journal/storage failure, exactly-once durability or nonblocking sends.

<a id="numa-deployment"></a>

### 5.3 NUMA deployment with multiple pools

The primary deployment use case for multiple pools is to keep a Sandbox's host
execution within a NUMA node while distributing new executions across nodes.
Conductor selects a pool and its systemd template; the operator configures CPU
placement and memory allocation on that template. This is host-side placement,
not guest vNUMA topology or a NUMA-aware capacity scheduler.

**NUMA isolation is an upper-layer application pattern built on multiple pools,
not a new isolation capability implemented by Conductor.** The following is a
deployment configuration example, not a project test specification. Project
acceptance covers pool configuration, independent prewarming, simple round-robin,
actual-unit lookup and the existing lifecycle, plus static example/documentation
checks. Real NUMA hosts, page-distribution checks, NUMA E2E and performance tests
are neither required nor scheduled for this feature; they are not merge
prerequisites or outstanding delivery work.

For a host with online, memory-bearing NUMA nodes 0 and 1, use distinct template
names for distinct placement policies. The following replaces the `units` block
in an otherwise complete Conductor configuration; omit the legacy single-pool
fields. The sizes are illustrative idle-worker targets, not recommended capacity:

```yaml
units:
  dir: /etc/systemd/system
  install: true
  pool_wait_timeout: 5s
  runner_pools:
    - {unit: sandbox-runner-numa0@.service, size: 4}
    - {unit: sandbox-runner-numa1@.service, size: 4}
  builder_pools:
    - {unit: sandbox-builder-numa0@.service, size: 1}
    - {unit: sandbox-builder-numa1@.service, size: 1}
```

**Inspect the actual host topology first.** Node IDs and logical CPU numbering
are machine-specific, including SMT siblings; never infer a CPU range from the
node number. Confirm cgroup v2 and the `cpuset` controller are available, and that
the ancestor slices allow both target nodes/CPU sets:

```sh
lscpu -e=CPU,NODE,SOCKET,CORE
numactl --hardware
cat /sys/fs/cgroup/cgroup.controllers
cat /sys/devices/system/node/node{0,1}/cpulist
```

**Install operator-owned placement drop-ins before starting Conductor or any
prewarmed worker.** On the example two-node host, run this as root. Each selected
node must have online CPUs and memory. The example uses that node's entire CPU
list; adjust `cpus` to the deployment's intended subset when reserving host CPUs:

```sh
set -eu
unit_dir=/etc/systemd/system
for node in 0 1; do
  cpus=$(cat "/sys/devices/system/node/node${node}/cpulist")
  test -n "$cpus"
  for kind in runner builder; do
    dropin="$unit_dir/sandbox-${kind}-numa${node}@.service.d"
    install -d -m 0755 "$dropin"
    cat > "$dropin/20-numa.conf" <<EOF
[Service]
CPUAffinity=
CPUAffinity=$cpus
AllowedCPUs=
AllowedCPUs=$cpus
NUMAPolicy=bind
NUMAMask=
NUMAMask=$node
AllowedMemoryNodes=
AllowedMemoryNodes=$node
EOF
  done
done
systemctl daemon-reload
```

`CPUAffinity` sets the initial execution mask; `AllowedCPUs` bounds the entire
unit subtree through cgroup v2. `NUMAPolicy=bind` with `NUMAMask` sets allocation
policy before the worker creates threads/children; `AllowedMemoryNodes` bounds
the unit's permitted memory nodes. These are placement settings, **not**
`CPUQuota`, `MemoryMax`, `MemoryHigh`, a per-pool reservation, or exclusive CPU
ownership. Keep runtime Sandbox resource enforcement with `sandbox-ctl`.

With `install: true`, Conductor generates each named base template and the two
existing fixed slices; the separately managed `.service.d/20-numa.conf` files
supply placement. Do not put custom properties into generated base files, which
Conductor rewrites. With
`install: false`, operators instead install four independent real template files
based on this version's generated runner/Builder templates (§5 and Build §4.1),
plus `sandbox-runner.slice` and `sandbox-builder.slice`, then reload systemd.
Preserve the deployment's executable paths, `%i` **RunID**, pidfile/config socket,
`Delegate=yes`, `ctl/vmm`, cleanup, and existing slice contracts. Do not use unit
aliases to represent different placements. Conductor does not install or reload
anything in that mode.

Runner assignments alternate between the two runner pools; Builder assignments
independently alternate between the two Builder pools. A prewarmed process is
already placed before assignment. Runner exec-replacement and Builder child
processes remain under the selected unit; the Builder's A/B/C phases that actually
run use that same Builder, not fresh pool selections. In-guest work runs through
those phase VMMs. Do not bind only the Conductor daemon: systemd starts the units,
so placement belongs on their templates. See [Build §4.1](node-build.md#41-builder-unit-and-process-lifecycle).

The deployment retains these boundaries:

- Two entries sharing one template share its placement policy; duplication does
  not automatically bind them to different nodes. `size` does not weight the
  round-robin sequence or limit active workers. Equal assignment counts are not
  equal CPU/memory usage or per-NUMA admission budgets; existing node-global
  admission and Build FIFO/claims remain unchanged.
- A bound execution stays with its actual unit, including Conductor restart.
  Pause/resume requiring a new runner selects again and may use the other node;
  the feature supplies no sticky NUMA placement, live migration or host-node
  identity in portable artifacts. Keep templates needed by active/pending-cleanup
  executions configured. Changing/reloading drop-ins does not re-exec existing
  prewarmed or assigned workers with the new process policy; plan an explicit
  lifecycle-safe worker turnover rather than assuming hot migration.
- Strict single-node binding can cause local reclaim or allocation failure/OOM
  despite free memory elsewhere. Conductor does not retry another pool. A
  deliberate `NUMAPolicy=preferred` fallback configuration must also remove or
  widen single-node `AllowedMemoryNodes`; cpuset restrictions take precedence.
  That trades strict memory locality for fallback, not NUMA-aware scheduling.

Setting semantics: [systemd.exec(5)](https://manpages.debian.org/trixie/systemd/systemd.exec.5.en.html),
[systemd.resource-control(5)](https://manpages.debian.org/trixie/systemd/systemd.resource-control.5.en.html),
[Linux NUMA memory policy](https://docs.kernel.org/admin-guide/mm/numa_memory_policy.html),
and [cgroup v2 cpuset](https://docs.kernel.org/admin-guide/cgroup-v2.html#cpuset).

## 6. Local control socket: run, task, admin, plugin and API planes

Conductor serves h2c/HTTP1.1 on paths.config_socket, default `/run/sandbox/node-ctl.socket`, mode **0600**. One socket multiplexes five independently authenticated planes. SO_PEERCRED injects peer PID into request context; same UID/root can connect, with additional plane checks. SDKs do not use /internal/*, which is separate from API paths.

**Run plane** handles POST /internal/run/assignment, /internal/run/build-result and /internal/run/build-phase. Prestarted units wait for assignment and builders report phases/results. SO_PEERCRED peer PID must match the locked RunID pidfile under runners/, with exact task ownership checks on reports. This differs from the task plane's business-ID pidfile authentication. See [configsock/server.go](../internal/configsock/server.go).

**① Task plane:** launchers obtain work specifications. Artifact-backed Sandbox and Build use exact-run two-stage endpoints; cold/image retains one-RPC bootstrap:

- POST /internal/task/sandbox/bootstrap takes sandbox_id/run_id/version. It first obtains non-secret exact-run pidfile identity and authenticates SO_PEERCRED+pidfile before the secret-bearing provider. Cold-image returns final LaunchSpec; artifact returns ArtifactPrepareSpec plus environment. The task submits sandbox_id/run_id/summary to POST /internal/task/sandbox/prepare and waits for LaunchSpec `{exec,args,workdir}`. Identical-digest replay waits for/returns the same result without duplicate host effects; conflicting replay returns 409. A disconnected HTTP request cancels only its wait. Exec is sandbox-ctl with run, sandbox-id/path-id, config, shared manifest config and invocation roots `<RunRoot>/sandboxes`/`<BaseRoot>/sandboxes`, plus selected --from E or --restore S and --connect mappings. PathID/roots fix ch.sock/ctl.sock/staging to stored RunDir/BaseDir; pause/snapshot/exec use that same locator. Final exec forcibly adds the local cgroup FD, which LaunchSpec cannot override.
- Complete Build bootstrap/prepare versions, payloads and recovery rules are in [Build task handoff](node-build.md#4-task-handoff); the identity and shared-plane rules below apply to both Sandbox and Build.
- Sandbox/Build authentication first resolves business ID plus exact RunID to non-secret task identity, then verifies peer PID against locked `<RunDir>/<sid>.pid` or `<BuildRunDir>/builder.pid`. Only then may providers decrypt ManifestKey/registry credentials. Stale runs or unauthorized peers cannot reach them.
- Root-credential-free bulk config uses files: Sandbox YAML (0600) or phase YAML (0600) in phase RunDir (0700). Tenant root/pull credentials use authenticated spec/environment instead. This does not make arbitrary caller-supplied workload files/env nonsensitive; their declarations retain normal config/artifact semantics.

**② Admin plane:** /internal/admin/manifest-keys uses GET for list and POST `{op:add|remove|check,manifest_key,api_secret?,label,ttl_seconds,registry_auth}` for pair allowlisting (§7). Responses contain fingerprints only. Optional admin_pidfile checks peer PID; otherwise socket 0600 applies. manifest-key CLI uses this plane. Cluster key_put writes/refreshes atomic leased pairs through node-link; expiry without renewal evicts entries, while key_drop is best-effort (§10).

The same plane supports Sandbox-local MMDS secret values:

```http
PUT    /internal/admin/sandboxes/{id}/mmds/secrets/{name}
DELETE /internal/admin/sandboxes/{id}/mmds/secrets/{name}
```

Name must be referenced by a current secret route. PUT replaces opaque bytes within max_secret_value_bytes; DELETE is idempotent. Success is 204 without plaintext. There is no TTL/expiry input or Content-Type-derived value metadata; response content type belongs to the route. Only successful store CAS publishes Upsert. Logs record SID/name/outcome, never body. Authentication remains socket 0600, SO_PEERCRED and optional admin pidfile.

**③ Plugin plane:** PUT /internal/plugin/{id}/register registers Proxy master or an observer and holds the h2c connection as both lease and routesync stream ([node-proxy.md](node-proxy.md) §4). First body frame is register{caps}; route_wake subscriptions can later send wake/route_barrier_ack. Response is hello(policy) → initial upserts → bookmark → live upsert/delete/barrier. Wake/ACK share a serialized upstream writer.

The same plugin plane exposes bounded native stats at `POST /internal/plugin/telemetry/stats`. Only the actual PID of the current ready telemetry registration may read it, subject to the existing plugin allowlist; UDS access alone does not grant ordinary API or native-batch rights. Lease revocation cancels active reads. See [Native reading bounds and ownership](node-usage.md#5-trusted-conductor-reading-surface).

Capabilities are independent: subscribe route/route_wake, proxy marker with optional stats_socket, and mmds. Barrier participation requires exact proxy ID, route_wake subscription and proxy marker, not stats_socket. Disconnect unregisters; a new same-ID registration unregisters/disconnects its predecessor. Optional plugin_pidfile checks peer PID, otherwise socket 0600 applies. Proxy and agents subscribe locally, independently of cross-network node-link mTLS.

Confidential MMDS projection cannot be self-granted by arbitrary plugins. Only exact ID proxy with route_wake, non-nil proxy and mmds=true receives service registry, mmds_routes and mmds_route_secret_values. Ordinary observers and cluster/node-link do not receive route values; other IDs claiming mmds=true are rejected. Master fails closed before full Bookmark and clears mutable route/value/service views on disconnect rather than indefinitely serving stale secrets.

**④ API plane:** remaining paths use the same wrapped e2b control http.Handler as api.listen, including export/import, over local plaintext h2c with API-key authentication. Export/import CLI uses it (§8.1).

Host root/daemon UID are trusted; tenant code runs inside guests and cannot reach host UDS.

## 7. Credentials and ownership: encrypted APISecret/ManifestKey pairs

- **Stable and local identity:** Sandbox.ID is the node-local store/runtime/route lookup key. StableID() preserves sandbox identity across local-ID changes. Ordinary standalone Create leaves StableIDValue empty, so the helper falls back to Sandbox.ID. Standalone import with another target ID changes only Sandbox.ID and preserves source StableID/service credentials. Identity-preserving migration/copy may produce several local rows sharing StableID; stable_id is neither UNIQUE nor a reverse-lookup index. In a cluster, local ID is NodeSandboxID and StableID is Registry's public SandboxID. Same-node resume retains both; cross-node migration/replacement changes only NodeSandboxID. Forward/Exec KAT sid always binds StableID; a copy is not thereby an independently authorized fork. Publication locations are also keyed by StableID (§8.1.4): a re-export after an import changed the target ID lands in the same directory, and every cluster generation shares one directory. Import admission validates a token-carried StableID as an opaque id (types.ValidLocalSandboxID), rejecting malformed identities at import instead of failing export-time Resolve.
- SQLite directly uses stable_id without probing/migrating legacy identity columns or dual reads/writes. Incompatible old preview databases must be explicitly rebuilt according to the schema boundary; this is not a claim that no Stable Release exists.
- **Separate tenant roots:** both APISecret and ManifestKey are 32 bytes represented as 64 lowercase hex digits. APISecret authenticates API requests and derives ServiceSecret. ManifestKey protects Manifest content through MANIFEST_KEY/manifest.key and seals image-pull tokens. A pair may specify APISecret; otherwise use the fixed domain-separated KDF:

  ```
  APISecret = HMAC-SHA256(decodeHex(ManifestKey), "kuasar-api-secret-v1")
  ```

  **Only APISecret signs/verifies API keys** through e2b-key-ctl gen-apikey:

  ```
  api_key = "e2b_" + hex( fp(12) ‖ ts(4) ‖ nonce(4) ‖ mac(16) )      # 76 characters
  fp  = SHA256(APISecret)[:12]                                       # Candidate prefilter, not unique identity
  mac = HMAC-SHA256(APISecret, fp‖ts‖nonce)[:16]                     # Unforgeable without APISecret
  ```

  SDK syntax uses /^e2b_[0-9a-f]+$/, while internal/apikey separately checks MAC. Neither root credential is sent to SDK clients.
- **Encrypted storage:** manifest_keys/sandboxes/builds each store the complete pair. api_secret_enc and manifest_key_enc use AES-256-GCM in internal/secretbox: keytag(4)‖nonce(12)‖ciphertext+tag; keytag selects a decryption key. Each root has a full 64-hex SHA-256 fingerprint. The API key's 24-hex prefix only prefilters candidates. encryption_key/NODE_CONFIG_ENCRYPTION_KEY takes colon-separated keys, first active, others retained for rotation; custom Runtime providers have the priority described in §3.
- **Authentication:** prefilter by APISecret fingerprint prefix, decrypt candidate APISecret, recompute HMAC. Create/build/import requires its complete pair in manifest_keys (daemon-only writes, manual CLI or Registry lease), otherwise 403. Existing ID/list operations check the resource's own APISecret without allowlist lookup, so clearing allowlist does not revoke existing resources before their lifecycle ends. List verifies MAC per candidate after prefix filtering, preventing hash-collision tenant crossover.
- **Copied business credentials:** Create/Build/import copies the complete pair into its row. Later allowlist add/drop/expiry or provider changes affect new records, never rebind existing ones.
- **Convergent storage:** ManifestKey seals the Manifest key table; chunk encryption keys derive from SHA256(salt‖plaintext). Writers sharing a Store salt belong to one content-sharing domain, allowing identical plaintext to reuse objects. Isolation requires extra_salt or separate Store/salt domains. Content equality never shares tenant ManifestKeys/APISecrets/templates or expands their authorization boundaries.
- **No plaintext stored roots:** roots reside encrypted in SQLite and only in necessary trusted process memory/protected route projections/environment at runtime. Authenticated exact-run bootstrap delivers sandbox ManifestKey; runner replaces inherited same-name entries so final exec has one authoritative MANIFEST_KEY. Conductor can store/deliver it but managed host preparation does not call CustomerKey, construct key-bound readers or open/decrypt/parse tenant artifacts. Sandbox YAML contains no root credentials.

  Builder likewise gets authoritative environment only after authentication. run-builder installs MANIFEST_KEY as process-wide reader authority and removes inherited registry credential keys. Registry credentials remain in local BuildSpec and go only to the host flatten operation or guest import command that needs them, not phase sandbox-ctl descendant environments. APISecret stays with conductor API authentication/ServiceSecret derivation and protected trusted Router/Proxy projections, never guest workloads. Auto-resume decrypts ManifestKey only for authenticated task delivery. Node-link installs both roots atomically with encrypted storage and runtime-only plaintext.
- Each Sandbox durably stores ServiceSecret, deriving it by default as HMAC-SHA256(APISecret, `kuasar-service-secret-v1:`+StableID()), then never rebuilding it after storage. e2b EnvdAccessToken/TrafficAccessToken may be overridden, otherwise are random; bare keeps both empty. EnvdAccessToken serves e2b 49983/49999. TrafficAccessToken is for external gateways/e2b data components, not node platform authentication. Each opaque token is valid UTF-8 and at most 256 bytes, checked before persistence. Both profiles' ForwardAccessToken is strict kat1 signed by ServiceSecret, with stable SID and aud=forward. All four credentials persist encrypted and cannot be rebound by lifecycle Upsert.

  ExecAccessToken is not generated by default in create/get/list and never stored in the Sandbox row. Each exec-sessions call mints a UUIDv7 session_id token: `kat1.<base64url-no-padding(payload)>.<base64url-no-padding(signature)>`. Canonical fields are v,session_id,sid,aud, optional exp and optional conditions; sid is StableID, aud is exec, and there is no iat. HMAC-SHA256 uses decoded 32-byte ServiceSecret directly, without another exec key. Conditions are compact strings in API order, omitted when unrestricted, signed together with other fields without a separate digest. Token holders can decode them, so expressions must not contain confidential literals. Payload has no generation, node ID or route revision. Node mints; Router/Proxy verify protected routes. Strict validation covers three segments, canonical unpadded base64url, 32-byte signature, fixed JSON fields/order, UUIDv7, SID, audience and optional expiry. Session ID is internal to KAT, not a separate API field.

  MmdsSecret is HMAC-SHA256 using decoded ManifestKey and `kuasar-mmds-v1:` plus SID. This deterministic per-sandbox MMDS session-signing key is projected to trusted Proxy when enabled, authenticating envd (§7 of [node-proxy.md](node-proxy.md)).
- **Separate MMDS route-value storage:** MmdsSecret signs/verifies MMDSv2 tokens; MMDSRouteSecretValues contains sensitive response values. Each Sandbox/Build owner has one encrypted JSON blob containing actual name→opaque bytes only, without version, Content-Type, TTL or configured/wait state. AAD binds at least owner kind/ID, canonical route digest and revision. Updates use revision CAS; key rotation retains old secretbox decrypt keys. Create/Register transaction writes business row, routes-only metadata and initial blob together. Sandbox deletion cascades; Build terminal/cleanup removes its blob. Database contains ciphertext; plaintext exists only for bounded trusted conductor/Proxy heap lifetimes.

## 8. Lifecycle and state machine

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
| ResumeSource | kind snapshot/sandbox plus ref | Resumable artifact root owned by paused row |
| ResumeMode | auto / memory / cold | Caller selection for this resume |
| LaunchMode | image / memory / cold | Resolved mode actually executed by starting |
| ResumeTrigger | connect / wake / route / exec / exec-session | Low-cardinality observation source, not mode or permission |

An image is a read-only filesystem root. E contains portable sandbox.runtime.cfg and disk state, without guest memory. S holds memory and references authoritative E through snapshot.cfg.sandbox_ref. S always has real memory state; its config does not represent E with a memory toggle. Template mapping is:

```text
img -> LaunchImage  -> sandbox-ctl run
sbx -> LaunchCold   -> sandbox-ctl run --from <E>
snp -> LaunchMemory -> sandbox-ctl run --restore <S>
```

The state machine is:

```text
Create(img|sbx|snp)
  |
  v
starting --success--> running --Pause/TTL--> paused(source=E|S)
  |                      |                       |
  |                      +--Kill--> deleting    | Resume(auto|memory|cold)
  |                                              v
  +--fresh failure--> dead                    starting(launch_mode=cold|memory)
                                                   |
                              running <--success---+
                                                   +--failure--> paused(original E|S)

starting|paused|dead --explicit Delete--> deleting --finalizer success--> absent
```

Starting is durable and spans admission, artifact preparation, resources/network, pool assignment, runtime readiness and mandatory e2b /init. Fresh Create stores image/cold/memory launch_mode. Paused resume stores cold/memory in the same atomic paused→starting update. Running/paused/deleting/dead clear launch_mode. Running may retain latest ResumeSource to track local artifact ownership; a successful Pause atomically replaces it.

Create/Connect/Wake/route activation/native exec/exec-session/migration import share one SID launch group. Under its lifecycle lock, admission rereads durable state, authorizes, resolves mode and performs Store CAS, then creates/joins an attempt. Attempt mode only caches durable launch_mode. Kill/Delete exact-CAS into deleting under that lock, retaining RunID/unit/port/RunDir/BaseDir/artifact ownership, cancels attempt, excludes it from cache/future snapshots and publishes route Delete. Withdrawal is not proof of cleanup.

After durable acceptance, finalizer waits for late launch ownership to finish, then stops/resets exact unit with inactive readback, detaches inside allocation fence, full-owner-CAS clears network tuple, removes RunDir and BaseDir and exactly hard-deletes. Only successful durable network clear releases the detached-port fence. Later directory/row failures retain path/runner ownership without blocking new Attach. Hard-delete publishes terminal Extension/object observation, without another route Delete. Failures retain unfinished ownership and retry in-process or after startup. Fresh Create's route-applied barrier precedes resource launch; failed acceptance cleans locally, then exactly creates owner-free dead history and withdraws route.

CommitRunningPaused atomically stores state/source. RunID, port and RunDir clear individually only after successful Stop/Reset fencing, Detach and RemoveAll. RunDir CAS also clears its envd/CI UDS paths. Each failure retains remaining retry fields. Fully cleaned paused state retains only BaseDir/checkpoint and source. Resume/Wake/Exec finish cleanup backlog before taking a new runtime owner and atomically restore canonical RunDir/UDS on paused→starting. Runtime cleanup never removes paused BaseDir; explicit Delete removes it and then the row.

### 8.1 Artifact capture, templates and migration

#### 8.1.1 Capture and Pause

Create autoPauseMemory has tri-state parsing:

```text
omitted/null -> true
true         -> true
false        -> false
```

The independent durable auto_pause_memory field selects TTL CaptureKind only; it does not enter checkpoint namespace or snapshot.cfg. Standalone and cluster share a typed chain: public Router → route-link reserve → Registry reservation → node-link command → node CreateReq → Sandbox row, without metadata tunneling.

Explicit Pause body may be empty or:

```json
{"memory":false,"checkpoint_merge_ref":null,"checkpoint_drop_caches":null}
```

Omitted/null/true memory selects CaptureSnapshot; false selects CaptureSandbox. Explicit Pause always defaults to S independently of autoPauseMemory. Thus Create(autoPauseMemory=false) plus Pause({}) captures S, while TTL captures E. SnapshotPolicy alone owns checkpoint_merge_ref/drop_caches. With memory=false, presence of either field returns 400 before freeze/capture/lifecycle side effects. Header/metadata checkpoint input still permits only merge_ref/drop_caches, never memory.

checkpoint.mode accepts local/bundle for both CaptureKinds:

```text
CaptureSnapshot:
  sandbox-ctl snapshot --sandbox-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root> [--merge-ref=...] [--drop-caches=...]
  -> ResumeSource{kind:snapshot, ref:<dir>/<sid>.snapshot}

CaptureSandbox:
  sandbox-ctl export --sandbox-id <sid> --output <dir> --mode <local|bundle> \
    --run-root <run-root>
  -> ResumeSource{kind:sandbox, ref:<dir>/<sid>.sandbox}
```

Ordering is resolve request → accept operation → capture runtime → CommitRunningPaused(id, exact RunID, source) → stop/reset exact runner → detach exact network → remove RunDir → publish paused. State/source commit is atomic; remaining RunID/port/RunDir means cleanup pending. Successful stop/detach clears its field by exact CAS. RunDir removal failure blocks new Resume/Wake/Exec ownership and is retried at admission or startup. BaseDir/checkpoint remains. Capture failure keeps running state, old source, runner/network and creates no success alias; it never downgrades S to E.

#### 8.1.2 Resume admission, Connect and Wake

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

BeginResume(id, deadline, LaunchMode, RunDir, EnvdUDS, CiUDS) requires fully cleaned runner/network/RunDir ownership and atomically writes starting, mode and new canonical RunDir/UDS. CommitStartingRunning/RollbackStartingPaused/RollbackStartingDead clear mode. Failed S+cold returns to the original paused S, allowing later memory selection. Conductor restart reconstructs an accepted starting resume from source plus durable mode, so lost cache cannot turn S+cold into memory restore.

Ordinary Proxy ingress and native exec activation call OnWake with ResumeTriggerWake; ExecSession uses ResumeTriggerExecSession. Public Route/Exec trigger constants remain extension contracts, although conductor no longer owns those data adapters. All these entries request ResumeAuto: S→memory, E→cold. SHM carries neither source kind nor cold gate. An authenticated first E request stays parked until cold launch, running route and successful backend dial. A future autoResume=false must be separate traffic policy, not inferred from E/S kind; complete E2B autoResume is not implemented.

#### 8.1.3 Task-local preparation and three configuration types

Artifact launch runs internal/taskartifact inside its tenant task. ArtifactPrepareSpec includes source kind/ref, durable mode, manifest config, location parent, relative directory, max refs and absolute deadline. With its MANIFEST_KEY, the task uses sandboxer's real readers to open/decrypt artifacts; conductor does not.

```text
E + cold   -> open E, parse sandbox.runtime.cfg, compute disk closure, prepared=E
S + memory -> open S, open S.sandbox_ref E, compute memory+disk closure, prepared=S
S + cold   -> open S, resolve/open S.sandbox_ref E, compute disk closure, prepared=E
E + memory -> reject during request mode admission; invalid task spec fails before VM launch
```

For remote Manifest S+cold, selected source is manifest://E. Local tarstream resolves relative content-identified E refs. Bundle selection points to the same physical Bundle with E's Manifest selector, without assuming remote Store already contains E. PreparedSource, ref-location URIs, carrier/Bundle binding and full config remain task-local. Network metadata is strictly decoded there, rejecting unknown/duplicate/malformed fields. Only typed network, capacity, required-ref count and resolution_digest reach conductor. Digest covers root, launch mode, selected source, closure, locations, carrier binding and capacity/network summary. Same RunID/digest replay returns the same result; conflicting replay fails. taskrun appends --from E or --restore S only from local PreparedSource.

Runtime uses three explicit DTOs:

- ImageColdConfig for run --config may contain image root/diff template, launch/env/files, mounts/init/plugin, resources/network/metadata.
- SandboxHostConfig for run --from E --config includes only ApplyFromRules host/instance fields: actual kernel/runtime bindings, active diff, resource controller/host policy, network provider/identity, timeouts, allowed persistent overrides, ephemeral_files and launch.ephemeral_env. It cannot declare immutable root/data graph. Paused E or S+cold treats E C0 as authoritative without replaying old row launch/env/files; only fresh KindSbx Create can deliberately apply current persistent overrides.
- SnapshotHostConfig for run --restore S --config includes only allowed kernel/runtime, active diff, controller/allocatable, network provider/identity, restore policy and timeouts. The type omits launch/files/ephemeral files/init/plugin/mounts/metadata/cmdline/disk graph.

Node-generated hosts/resolv.conf use ephemeral_files during cold launch, not portable C0; memory restore does not reinject them. Persistent env/files follow deliberate C0 overrides; ephemeral input affects this cold invocation only. Rendered YAML must pass LoadMergedWithPresence and the corresponding ApplyFromRules/ApplyRestoreRules, retaining E's data-disk name/order/mount authority.

#### 8.1.4 Publication, templates and migration

Both local E/S use:

```text
sandbox-ctl publish --manifest-config <cfg> [--to-ref-location <name>=<uri>] <artifact>
```

promoteArtifact preserves ResumeSource kind. Without a named parent, it publishes to Manifest Store. With checkpoint.remote.ref_location_parent, name is the entity id — the sandbox StableID for E/S publication, the BuildID for build publication (see reflocation.PublicationName) — and URI is `<parent>/<sha256(name)[0:2]>/<sha256(name)[2:4]>/<name>`. Tarstream results are located sandbox/snapshot refs using digest or hmac identity; Bundle results use manifest root selectors. Manifest config is supplied even for named publication because exact Bundle publication validates its Manifest graph. Bundle→Store verifies recorded admission, physical digest and salt domain and commits root last.

The name itself is the directory key: every publication of one logical entity — retries, process restarts, re-exports after an import changed the target id, cluster generations — converges on one directory where content-addressed `<digest>.<role>` files accumulate as versions and same-content files are deduplicated by the publisher. Two SHA-256 fan-out levels limit entries. GC must be reference-aware (reachability over the directories and files pointed to by portable refs / TemplateIDs / migration tokens), never age-based: publication age is not the lifetime of its references. Retention/reachability/in-flight-publication GC and safe remote deletion are outside node lifecycle. Conductor/tasks share deterministic internal/reflocation resolution; the location name is self-sufficient without side tables.

TemplateID is `<profile>-<kind>-<base64url(canonical-portable-ref)>`:

```text
img: manifest ref,or located .image/.bundle
sbx: manifest ref,or located .sandbox/.bundle
snp: manifest ref,or located .snapshot/.bundle
```

Paused E promotes to sbx, S to snp; E is not rejected for lacking memory. Build Snapshot also yields snp, publishing through the same sandbox-ctl publish rather than a special Snapshot-only publisher.

KMT V1 directly contains resumeSourceKind/ref and autoPauseMemory and rejects legacy snapshotRef payloads. Import restores kind/ref, deadline, portable env/metadata and existing ServiceSecret/Envd/Traffic/Forward credentials. Destination retrieves roots from trusted local allowlist and verifies fingerprints/runtime digest/profile. Token contains no raw tenant roots, host paths, MMDS secret values, cluster group/route key or generation.

Publish phase does not hold a long lifecycle lock. Finalization acquires per-SID lock against the exact original ResumeSource. Resume winning BeginResume cancels KMT export with 409; template publication can finish detached and return ID without source cleanup. If the finalizer wins, keepSource returns the export result and leaves the source untouched — same ResumeSource, row, cache, route and local checkpoint (#336); without keepSource, exact teardown precedes row/cache/route/local-artifact deletion. Teardown/Store failure retains retryable durable ownership. Local cleanup never deletes remote/located artifacts.

Standalone import defaults to source NodeSandboxID; an explicit target changes only local ID while retaining StableID/service credentials. Atomic insert-only conflicts with 409. Connect can synchronously import a missing target from X-Kuasar-Migration-Token; existing targets ignore it. Paused routes project only kind-independent artifact_location local/remote for trusted migration consumers. Proxy does not inspect artifact kind; migration tokens are sensitive Sandbox credentials and never route-broadcast.

Running means runtime readiness and mandatory e2b /init, not application-port health. Generic backend health/dial retry is separate work. Cluster Create/Connect reuse node primitives. Secrets-only CONNECT import remains standalone-only; node-link carries no MMDS route-value plaintext.

## 9. Data-plane boundaries

The data-plane **forwarding layer**—L7 reverse proxy routing `(sid, port)` to guest envd/floating IP, per-request authentication, CONNECT tunnels, MMDS and the routesync wire format—is specified in [node-proxy.md](node-proxy.md). Conductor serves only control APIs, lifecycle and local route authority. It creates no proxy, listens on no data port and receives or forwards no Sandbox data. The independent `node-ctl proxy serve` process's mandatory `data_listen` is the node's sole data ingress.

### 9.1 APIEndpoint and DataEndpoint

Standalone and cluster deployments must separate the endpoints:

| Endpoint | Owner | Purpose |
|---|---|---|
| `APIEndpoint` | Conductor `api.listen` | Sandbox/Build control HTTP |
| `DataEndpoint` | Proxy `data_listen` | Ordinary HTTP, CONNECT and native exec CONNECT |

Cluster registration explicitly carries both; they are not derived from bind addresses. Router sends control/build requests only to APIEndpoint and data/exec only to DataEndpoint. A missing endpoint fails closed without cross-plane fallback. Proxy workers perform the KAT + ExecRequest gate and sandboxer ctl tunnel for native exec. They construct `<RunRoot>/sandboxes/<NodeSandboxID>/ctl.sock` locally from mandatory `paths.run_root` in the master's frozen EffectiveConfig. That path is absent from routesync Policy and shared route views.

Router→node currently uses plaintext HTTP/CONNECT. The two registered endpoints must therefore address distinct internal plaintext listeners, never TLS-enabled node listeners. Supplied deployment examples use conductor `:3000` and Proxy `:3443`, terminating external TLS at Router or a load balancer. Node-link retains its independent mTLS.

Proxy master can register an optional stats_socket. Each worker pushes Prometheus counters and per-Sandbox absolute traffic values through its own socketpair. Conductor's traffic API queries only the current trusted Proxy registration's stats endpoint. Route-barrier participation is independent of this socket. Each logical ingress is counted once, at the final node worker.

### 9.2 Route authority and broadcast

Conductor serve is this node's route and lifecycle authority. Create/resume/pause/kill update routes immediately and broadcast routesync Upsert/Delete to plugin-plane subscribers: Proxy masters and read-only observers such as platform agents. Master projects routes into shared memory; workers read them, while observers maintain read-only state caches. Full broadcast sends individual upserts followed by a bookmark, keeping sender memory bounded at high density. See node-proxy.md §4 for framed JSON over h2c, resynchronization and RouteEntry fields, including migration's artifact_location, MMDS's mmds_secret and presence-aware max_inflight patches; §6 covers plugin registration/authentication. Registry owns fleet-wide routes (cluster.md). Node-link reports local Sandbox events to Registry (§10), independently of local plugin broadcasts.

Server-side projection precedes broadcast. Trusted MMDS proxies receive mmds_routes and mmds_route_secret_values in the same Upsert for atomic replacement. Ordinary observers receive neither, and cluster/node-link never receives secret values. Conductor-owned mmds.services appears only in trusted proxies' Hello policy. Master atomically replaces that service registry; workers obtain resolved Unix-socket endpoints through local MMDS RPC instead of reading a second copy from proxy.yaml.

- **Auto-resume launch owner:** requests reaching paused Sandboxes send routesync Wake upstream. Connect, Proxy Wake, ExecSession and other launch entries for one SID share one attempt (§8).
- **Starting projection:** broadcast immediately after durable insertion of `starting,run_id=""`, before any FloatingIP or usable endpoint exists. Publish enriched starting after network ownership is durably CAS-written and YAML/ready.sock are prepared, allowing Proxy MMDS to support envd /init. Starting permits neither ordinary data access nor Wake. Each Create follows its initial Upsert with an ordered route barrier. Master validates and merges node defaults with explicit patches, then transactionally publishes admission bindings and route SHM before ACK. The barrier does not wait for worker readiness, statistics or a healthy-worker count: successful master apply can ACK with no serving workers. Admission/route apply failure neither ACKs nor leaves half-published state. Only successful apply+ACK and a lease recheck permit 201 and launch. Thus the first valid request after 201 should see starting and park, without treating the normal propagation gap as missing. Launch success broadcasts running; failed Create broadcasts Delete, while failed Resume broadcasts paused.
- **Envd authentication posture (`mmds.enabled`):** determines whether Create supplies an envd token and Proxy hosts MMDS. False means non-secure envd and one proxy gate, with `proxy.auth=enforce` required. True enables FC MMDS v2 inside Proxy and re-keys a fresh token per identity through /init, making snapshot fan-out data access possible. See node-proxy.md §7 for the posture comparison and two-stage MMDS protocol.

## 10. Cluster integration (node-link)

With cluster.node_link.endpoint (§3), `node-ctl conductor serve` dials Registry to join the cluster orchestrated by `cluster-ctl registry/router/placer`. Node-link reuses routesync's framed JSON over h2c engine (node-proxy.md §4), with reversed roles: node owns local routes/builds; Registry subscribes and sends commands.

This section defines node behavior. [cluster.md](cluster.md) defines Registry ownership selection, redirect/relay, shardkv replication and membership changes.

```text
node-ctl conductor serve
  │ dial registry node_link endpoint
  │ register node profile
  │ stream heartbeat + sandbox route events
  │ stream Build full snapshot + live BuildUpsert/BuildDelete
  │ receive create/connect/exec_session/delete/key/build commands
  ▼
registry node_link owner or relay holder
```

There are two cluster→node paths:

- **Node-link:** registration, heartbeats, Sandbox route events, Build snapshots/deltas, commands and manifest-key leases.
- **Router forwarding to local e2b control/data planes:** pause/kill/timeout and Build status/files use local e2b control; data enters local Proxy after Router injects E2b-Sandbox-Id and X-Access-Token (node-proxy.md).

### 10.1 Registration and redirect

After dialing Registry's node_link endpoint, the first node frame is:

```text
register{
  node_id,
  labels,
  capacity,
  build_registration_capacity,
  build_execution_capacity,
  api_endpoint,
  data_endpoint,
  runtime_digest,
  accept_redirect
}
```

If the accepting member does not own this node_link and the node supports redirect, Registry may return the owners' node_advertise list. Node tries owners in order, advancing on failure. With no redirect target or redirect disabled, the accepting member may relay to the first available owner.

### 10.2 Heartbeats and the low-frequency directory

Node periodically sends:

```text
heartbeat{zone, allocated, pool, build_registration_usage,
          build_execution_usage, counts, draining}
```

Both Build capacity levels travel in the initial register frame. Sandbox watermarks come from the resource controller (node-resource.md). Allocated memory sums all local NodeReservations, while pool is node allocatable capacity; neither is host memory.current, VMM charge or guest demand. Both Build usage levels exactly sum each nonterminal SQLite Build.Resources/execution claim, never active count × a default vector. Node resource drain or maintenance policy sets draining. Ordinary heartbeats update node_link liveness and local watermarks without changing node_list. Initial registration and draining changes drive low-frequency directory projection. At placement commit, only the current connection held by the Registry node owner establishes liveness.

### 10.3 Sandbox and Build projections

Node reports authoritative local execution state:

```text
sandbox{
  sid, profile, state(starting|running|paused|dead), artifact_location, template_id,
  stable_id, api_secret, api_secret_fingerprint,
  manifest_key_fingerprint, service_secret,
  envd_access_token, traffic_access_token, forward_access_token,
  mmds_secret
}
delete{sid}
build_sync_begin{}
build_upsert{build_event:{build_id, state, template_id, persist_id?, reason?}}
build_delete{build_event:{build_id, template_id}}
build_sync_end{}
bookmark{full_sync}
```

Deleting is local cleanup-pending state and is never projected as a Sandbox upsert. Durable Delete acceptance immediately excludes the SID from local cache and future full route snapshots and sends delete{sid} on existing live links. This withdraws routing, without proving unit/network/path cleanup or hard-delete. If the process exits between durable transition and incremental publication, its old stream expires; the next full snapshot excludes the SID and removes the old projection. Restart retries owners still present in the durable deleting row and never reacquires an exact-cleared network tuple. Local finalizer correctness does not depend on Registry projection; completion sends no second route Delete.

Node route events retain mmds_secret for local proxy/MMDS use; Registry excludes it when materializing protected cluster routes. Starting means local launch is in progress. Registry includes it in the full-sync seen set without overwriting reserved/paused routes; running/paused/delete drive cluster convergence. Events explicitly carry the remaining credentials required for those purposes.

Sandbox events do not self-report Registry-owned cluster context. Before dispatch, the node-link owner maintains the node's complete Sandbox/Build ownership table. It looks up `(node_id,sid)` or `(node_id,build_id)` to recover group/route_key. Event sid is the local NodeSandboxID, allocated by Registry as `<stable-sandbox-id>-g<N>`. Node neither receives SandboxGeneration nor parses this ID. Registry's ownership table recovers stable SandboxID and generation; there is no cross-group SandboxID index.

The bookmark ending a full Sandbox Range carries full_sync=true. Node-link owner compares seen SIDs only against the ownership baseline captured before subscription, then rechecks that each current entry still matches before deletion. This protects newly dispatched/rebound tasks during synchronization. Incremental-replay bookmarks only advance the resume token.

Build events preserve registration template_id independently of terminal persist_id. Hard deletion carries the original template_id, so late events cannot affect a new same-BuildID registration. Only committed node hard deletion emits BuildDelete; an error row leaving an observer collection is not hard deletion.

Existing post-registration Build projection has an independent bracket. Node subscribes to live Build changes, sends build_sync_begin, every retained local cluster Build row, then build_sync_end. The set includes registered/waiting/building and ready/error within retention. Registry fences these frames by NodeID session: once a new link takes effect, an overlapping old connection cannot alter projection. An unconditional per-Build fence orders each durable transition and live publication, independently of conductor Extension. Changes during snapshot follow end in order; slow subscribers disconnect and rebuild a complete snapshot. At build_sync_end, Registry deletes only unseen post-registration projection/refs from the preconnection immutable `(NodeID, BuildID, TemplateID)` baseline that still belong exactly to that node. Registry-owned BuildStarting ambiguous-dispatch intent is not node projection; an empty snapshot cannot establish definitive rejection. This protects new registrations and converges lost live build_delete on reconnection. Registry runs no separate terminal TTL. Long snapshots do not block control replies: the shared writer drains bounded command ACKs and coalesced heartbeats between items; live Build changes still wait for build_sync_end.

### 10.4 Command acceptance

Registry sends commands to serve, which reuses e2b lifecycle primitives (§8/§8.1). Every Sandbox sid is an exact NodeSandboxID. Normal commands return cmd_ack on acceptance and report terminal state through route or BuildUpsert events. CmdCreate ACK requires the unique launch owner, inserted starting row, published cache/route, successful ordered route-applied barrier and lease recheck. It proves durable acceptance, not READY. Before ACK, CmdConnect cleans old runner/network/RunDir ownership, atomically restores canonical RunDir/UDS on paused→starting acceptance, commits the final deadline and publishes cache/route. CmdExecSession first completes optional import, authorization/signing and the same resume acceptance. All three then launch asynchronously under the common lifecycle root:

| Command | Node action |
|---|---|
| `create{cmd_id, sid, template_ref, profile, api_secret_fingerprint, config, cluster}` | Cold-launch template_ref and retain/merge config (§8; snp means snapshot restoration for fresh Create). Full APISecret fingerprint selects an installed credential pair. Persist cluster={group,route_key,stable_id?} and profile as separate system fields; resolve and strip credentials namespace before the business insert. Before ACK, state is starting with empty run_id, an active attempt and the successful route/lease barrier. |
| `connect{cmd_id, sid, profile, api_secret_fingerprint, cluster, migration_token?, timeout_seconds?, memory?}` | SID is NodeSandboxID. Existing targets ignore migration tokens and validate full fingerprint, profile and cluster context. Missing targets with KMT1 synchronously verify the matching local pair and insert paused with command SID/context. Before ACK, clean old owners and atomically restore canonical RunDir/UDS, resolved launch mode and deadline. Optional memory retains absent/null/false/true semantics through ResumeMode (§8.1.2). ACK carries ConnectResult{NodeSandboxID,TemplateID,Profile,EnvdAccessToken,TrafficAccessToken,ForwardAccessToken}, without root/fingerprint. Restore is asynchronous; missing targets without tokens are rejected. |
| `exec_session{cmd_id, sid, profile, api_secret_fingerprint, cluster, ttl_seconds, exec_conditions, migration_token?}` | Reuse Connect's exact target, optional synchronous import and profile/context/full-fingerprint checks; raw API keys never enter node-link. Node authoritatively compiles conditions, generates UUIDv7 session ID and optional exp from actual issuance time. ACK contains only ExecSessionResult{ExecAccessToken}. Resume remains asynchronous without waiting for READY. Conditions never enter Sandbox rows/routes/events/metadata. |
| `delete{cmd_id, sid, api_secret_fingerprint}` | Full fingerprint must match the existing row. ACK means the exact owner durably entered deleting, without awaiting finalizer completion. Publish route Delete before ACK; withdrawal is not cleanup proof. Pending replay is idempotent (§5 kill). |
| `key_put{api_secret_fingerprint, api_secret_type, api_secret?, api_secret_ref?, manifest_key_fingerprint, manifest_key_type, manifest_key?, manifest_key_ref?, expires_unix}` / `key_drop{api_secret_fingerprint}` | Atomically validate/install or renew the complete pair; both fingerprints are 64-hex SHA-256. Key_drop best-effort removes by full APISecret fingerprint; TTL eviction supplies correctness (§7). Cluster.md specifies distribution. |
| `build_register{build_id, template_id, profile, resources, image_repo, registry_auth, api_secret_fingerprint, config}` | Config.builder.target is the canonical register-only requested target and participates in replay digest/node validation. Node transactionally performs final registration admission against the same canonical Build.Resources. Persist final Sandbox Create config/resources, build-only options and cluster group separately; never derive A/B resources from target Sandbox resources. Only definitive rejection without side effects permits reselection; ambiguous outcomes retry the same node/BuildID. |

There is no drain command. Resource drain (node-resource.md §2) or local maintenance originates at the node; the cluster merely stops assigning it work.

Post-ACK CmdCreate launch failure rolls fresh starting back to dead and sends Delete. CmdConnect/ExecSession failure rolls back to paused and sends paused Upsert, never taking Create's replacement-Delete path. Registry retains the replacement-reservation rollback fence: only Delete matching this Create restores the pre-Reserve route. Premature reservation deletion would lose rollback evidence. Concurrent cluster create/connect/delete cannot acquire a second local launch owner.

Standalone and cluster Delete share the same local finalizer. Node-link has no alternative cleanup or path derivation. Finalizer failure neither reports deleting as ready/paused/starting nor delays/repeats route withdrawal. Terminal object observation waits for hard-delete.

Each exec-session API call is a separate authorization with a new CmdID and KAT; Resume can still join the SID's current attempt. CmdID correlates only the current command and ACK waiter. Node persists neither command digest nor typed result; Registry does not automatically resend that CmdID after disconnect, timeout or node restart. An API retry is a new operation: Connect revalidates/retries recovery and Exec Session may issue a new KAT.

### 10.5 Disconnection and security

Node reconnects with exponential backoff and registers again. Registry supplies its retained Sandbox route revision in Hello.resume_from; node replays from that subscriber cursor when its retention permits, otherwise sends individual full routes plus bookmark. This is not a node registration field. Build projection performs a complete bracket in every new session, independently of the route token, including after Registry restart.

Production node-link uses cluster.node_link.tls mTLS. Distributed APISecret+ManifestKey pairs and creation credentials enter only encrypted storage and runtime memory. Sandbox tokens are sensitive and travel only over trusted links.

Cluster integration and the local plugin plane share the routesync engine/wire format, with different subscriber kinds. Router never subscribes to node plugins; Registry aggregates fleet routes.

## 11. Guest profiles: envd and the toolchain

- Guest-runtime builds one sandbox-runtime.bundle: sandboxer's sandbox-init in a virtio-pmem/DAX runtime, with pinned envd, flatten-ctl and mkfs.erofs under /opt/sandbox-runtime/bin/. Raw EROFS starts at offset zero, followed by zero padding and a ZIP containing only an empty .kuasar.digest.<hex> marker. Runtime identity covers EROFS+padding and is computed once during construction; launch/restore reads it directly from EOF. Total length is aligned to 2 MiB for virtio-pmem, otherwise Cloud Hypervisor reports PmemSizeNotAligned. EROFS describes its own extent, so guest mounts ignore trailing padding/ZIP.
- **Boot chain:** sandbox-init automatically bind-mounts /opt/sandbox-runtime into the same guest path. Envd is launch.exec: `/opt/sandbox-runtime/bin/envd -isnotfc -port 49983`, omitting -isnotfc when MMDS is enabled (node-proxy.md §7), with restart=always and root user `0:0`. Envd needs CAP_SETUID/SETGID to execute workload commands as the image's Config.User; a non-root envd can fail with EPERM. Workloads themselves retain their target user.
- **Cgroup delegation:** ordinary e2b Create, builder steps and final snapshot templates set launch.cgroup_control=true without adding an envd cgroup-root argument. Real /sys/fs/cgroup/app is the namespace/freezer root and has no direct processes. Long-lived envd, plugins and native exec managed by sandbox-init reside in real /app/init and see /init through their scoped cgroup namespace. Envd uses default /sys/fs/cgroup to create user, ptys and socats at namespace root, corresponding to real /app/user, /app/ptys and /app/socats. Bare defaults to false and can enable kuasar-sandbox.launch.cgroup_control explicitly.
- Envd is a native-deps build product alongside vmlinux/mkfs.erofs. `make -C guest-runtime/native-deps envd` downloads the e2b-dev/infra release tarball (default tag 2026.22, override ENVD_TARBALL), then builds packages/envd. `make -C guest-runtime sandbox-runtime` injects it with the build toolchain.
- **Userland requirements for base/customer images:** bash, coreutils/util-linux, a precreated default user (normally user with /home/user), cgroup v2 and writable /run. Envd runs each guest command as the default user wrapped by `ionice -c 2 -n 4 nice -n N "$@"`. Missing users/utilities cause invalid default user or ionice: not found; plain Alpine lacks both prerequisites.
- **Guest /etc/hosts is required:** flattened Docker images omit Docker's runtime-injected hosts file. Calls such as socket.getfqdn(hostname), including Python http.server.server_bind between bind and listen, can fall through to DNS and stall hostname resolution; approximately 20 seconds has been observed, rather than a fixed timeout guarantee. This can look like broken host→floating-IP application forwarding. Cold-launch SANDBOX_CONFIG injects /etc/hosts with `127.0.1.1 <hostname>` and /etc/resolv.conf from sandbox.network.dns through ephemeral_files, and sets the hostname through network.hostname. These node-derived files never enter portable C0 and are not reinjected on memory restore.
- After runtime readiness, host directly calls mandatory POST /init over UDS to set envVars, default user/workdir user:/home/user and timestamp; accessToken appears only with MMDS (node-proxy.md §7). The short transport backoff described above covers an envd socket not yet dialable. Startup does not probe /health.

## 12. DNS / TLS

Production uses operator-provided wildcard DNS/TLS for *.<domain> and api.<domain>, including on-premises/offline deployments. Conductor api.listen serves control; independent Proxy data_listen serves data. They must be different listeners. Standalone can terminate TLS on both; using port 443 for both requires distinct addresses or an external hostname-routing load balancer. Development E2B_API_URL/E2B_SANDBOX_URL point separately at plaintext HTTP/h2c. In clusters, Router owns the public wildcard certificate and currently forwards plaintext to node APIEndpoint/DataEndpoint; node-link separately uses cluster.node_link.tls mTLS (§10).

## 13. Contract boundaries

| Object | Mechanism | Contract |
|---|---|---|
| Sandbox-ctl runtime | Run-sandbox ultimately execves `run --ready-fd=<fd> --cgroup-path=fd=<vmm-fd> --config <sid>.yaml --manifest-config … --run-root … --base-root … [--from E\|--restore S] [--connect]`. Run-builder directly spawns A/B/C; E-based C uses --from E --replace-boot. Platform relay uses exec --env/--stdin-from/--stdout-to; memory finalization uses snapshot/publish. Top-level E calls sandboxer assembly/publication packages without a sandbox-ctl process. Conductor Capture calls snapshot or export. | Managed launch/Build preparation never calls sandbox-ctl info. Readiness is control_ready→ready→EOF. Node-ctl injects the local VMM cgroup FD. Non-root-credential config files plus key environment; runtime owns resource admission. E2b semantic commands use envd ([Build §5](node-build.md#5-target-aware-execution-and-publication)). |
| Resource controller (node-resource.md) | Embedded in serve with resource_listen and inline tuning; dynamic Sandboxes dial the same canonical SocketIdentity through pkg/resource protocol. | Resource_listen is the sole endpoint source. Per-runner vmm/ is the Sandbox resource cgroup, with FD injected by run-sandbox. Disabled controller means static cgroup. |
| Registry (cluster-ctl) | Node-link: serve dials Registry, registers as route authority, sends registration/heartbeat/Sandbox routes/Build full-sync-upsert-delete and accepts create/connect/delete/key_put/key_drop/build_register (§10, cluster.md). | MTLS; cluster kill uses node-link delete. Registry has no Build projection TTL. Empty node_link.endpoint means standalone. |
| Connector-ctl vswitch | Without tapfd_socket, CLI attach <switch> --inner-ip [--transit-*] / detach --port; with it, resident TAPFD/1 PREPARE/OPEN/RELEASE. Render network.tapfd.socket/request in Sandbox config. | Prestart the kernel-data-plane switch with start/serve. Public port differs from internal slot. One Build reuses one slot. |
| Flatten-ctl builder | Guest runtime supplies export --output - for import/export and mountpoint, driven by sandbox-ctl exec. Host uses info --json --manifest-config only to verify registry referrer hits. | Sandboxer typed openers read all three image-carrier configs and publish outputs. Tenant FLATTEN_* enters guest only through exec env; stdio relays tarstream artifacts. |
| Sandboxer pkg/artifact + pkg/sandbox | Direct Manifest ingest/single-root Bundle publication of image/E logical sources; local IMG→top-level E has no intermediate file. | Sandboxer owns typed roles, customer keys, write admission, root-last, sparse rules and named-location atomic/reuse validation. Finalization never invokes manifest-ctl store. |
| Manifest-ctl (accelerator) | Independent Manifest Store CLI, outside Builder final publication. | Calling-process environment supplies MANIFEST_KEY. |
| Mkfs.erofs (deps) | Guest-runtime make sandbox-runtime and guest flatten backend. | Deterministic runtime packaging; guest exports EROFS images (§11/[Build §5](node-build.md#5-target-aware-execution-and-publication)). |
| Guest envd | UDS mapped by sandbox-ctl --connect. Build uses a minimal Connect+JSON process.Start client for steps/startCmd/readyCmd ([Build §5](node-build.md#5-target-aware-execution-and-publication)). | Unmodified upstream; protocol pins in §4.2/§4.3. |
| Systemd | D-Bus StartUnit/StopUnit/ResetFailed/ListUnitsByPatterns/Reload. | Process management and unit installation (§5). |
| Node-ctl proxy serve | Bidirectional framed-JSON h2c routesync UDS plus separate data listener. | Independently operated on the same node. Master registers once; workers inherit data-listener FDs and shared routes. Frozen EffectiveConfig includes paths.run_root; workers never reread proxy.yaml (node-proxy.md §2/§3/§4). |

Public Config/App/extension contracts live in config and app/conductor, app/proxy and app/telemetry; advanced Collector bindings are isolated in app/telemetry/otel. CGO_ENABLED=0 remains supported; internal core retains internal/* dependency boundaries.

## 14. Reliability

### 14.1 SQLite state

One SQLite file at paths.db_path uses WAL, mode 0600 and a pure-Go driver. Core tables:

```
sandboxes      id(node-local SandboxID,1..57 bytes DNS-label subset) PK,
               profile, cluster_group, cluster_route_key, stable_id,
               template_id, state(starting|running|paused|deleting|dead), deadline_unix, dead_unix,
               run_dir, base_dir, envd_uds, ci_uds, floatingip, vswitch_port,
               inner_ip, port_mac, api_secret_hash, api_secret_enc,
               manifest_key_hash, manifest_key_enc,
               resume_source_kind, resume_source_ref, auto_pause_memory, launch_mode,
               service_secret_enc, envd_access_token_enc, traffic_access_token_enc,
               forward_access_token_enc, metadata_json, env_json,
               created_unix
sandbox_mmds_route_secret_values
               sandbox_id PK/FK sandboxes(id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
manifest_keys  api_secret_hash PK, api_secret_enc, manifest_key_hash,
               manifest_key_enc, label, created_unix, expires_unix, registry_auth_enc
```

Build schema, admission and record authority are in [Build §6](node-build.md#6-persistence-recovery-and-retention). Resume_source_kind/ref is paused E/S's typed root, auto_pause_memory selects only TTL CaptureKind, and launch_mode records accepted image/cold/memory in starting. This lifecycle schema replaces the old single-string model without dual reading/writing or a migration shim; incompatible development databases must be rebuilt.

Root/service credentials in *_enc use AES-256-GCM; both *_hash values are full SHA-256. The first 24 hash characters only index candidate preselection (§7).

Each MMDS owner has at most one secretbox ciphertext row; §7 owns AAD/transaction/CAS/cleanup. Build schema markers, additive migration and terminal timestamp initialization are in [Build §6](node-build.md#6-persistence-recovery-and-retention).

### 14.2 Restart reconciliation

Before any cleanup (including early Build cancellation/deletion), conductor can
locate units by enumerating distinct configured templates with ListUnitsByPatterns.
Each actual unit is reconciled once. Restart restores RunID → actual unit, not the
historical pool position under a shared template. A missing index entry is not
proof of process absence; enumeration errors preserve durable ownership and never
cause a guess using the default template. Keep every template with execution or
pending-cleanup ownership in configuration; removed-template discovery, migration,
hot deletion and drain are outside this contract.

Before opening APIs, config-socket routesync or node-link, conductor reconciles the
configured runner unit set:

- **Deleting:** already accepted explicit deletion with unfinished cleanup. Cancel the SID's launch owner and fence its exact unit. Under allocation fence, detach a nonempty exact network tuple and exact-clear all four fields; an empty tuple skips Detach. Remove canonical RunDir, BaseDir, then exact hard-delete. Any failure blocks startup and retains unfinished ownership. Directory retries never reacquire a durably cleared network owner. Deleting is excluded from full routes, Wake/Resume/Exec and all activation.
- **Starting:** never adopt directly as running. Fresh Create first stops/resets exact runner, detaches and removes both directories, then exact-run CASes to dead. A valid ResumeSource identifies accepted Resume: release old ownership while retaining starting, source and durable launch_mode; remove only RunDir, retain BaseDir/checkpoint, clear runtime owner and enqueue recovery using the original cold/memory choice. Any stop/reset/detach/directory failure blocks startup without clearing unfinished fields or exposing APIs.
- **Running with active/activating unit:** adopt, restore in-memory route, preserve TTL, resend the snapshot to Proxy and report through node-link in clusters.
- **Running without live unit:** fence unit, detach, remove both directories, then atomically exact-owner-CAS clear paths/runtime/network/artifact and mark dead. Dead is resource-free history.
- **Runner units without running rows:** old-pool idle/orphan run IDs are stopped/reset; the new pool replenishes configured idle capacity.
- **Paused:** retry complete stop/reset/inactive fencing, detach, exact CAS and RunDir removal even if RunID/port already cleared. Preserve source and BaseDir/checkpoint. Resume/Wake/Exec use the same admission gate and cannot enter starting before old cleanup completes. Run_root is tmpfs: host reboot makes lost running instances dead, while paused E/S and sbx/snp templates survive and can Connect/Wake from persistent BaseDir/checkpoint.

The same startup gate reconstructs Builder ownership under [Build recovery](node-build.md#6-persistence-recovery-and-retention) before allowing bootstrap/results. Builds remain subject to the shared reaper and failure-domain rules below.

The same conductor reaper runs terminal retention every five seconds, without another timer/unit. Dead and ready/error transitions atomically write dead_unix/finished_unix. Each pass processes at most 128 rows of each type. Exact-delete only after dead_ttl/terminal_ttl and complete owner release. Sandbox must have no unit, network, RunDir/BaseDir, UDS, ResumeSource or launch owner. Build must have no execution claim, unit/cgroup, phase, network, prepare/result owner. Concurrent changes after candidate scanning fail the delete CAS and preserve the row. Restart resumes using database timestamps. No automatic VACUUM or remote-artifact mutation occurs.

These local finalizers implement #132/#133's cleanup contract. Export #196 retains its publish/finalize race and source cleanup order; #205's Build resources, two admission levels and cgroup authority remain. Current source still has post-registration Build projection, so routesync v8 retains v6 Build full-sync/live-delete convergence without changing #46's immutable registered-node binding. If #46 later removes that projection, node TTL itself needs no recreated lifecycle event. This adds neither remote-artifact GC, per-step cleanup stages nor another path authority.

RouteSource.Range and later full snapshots therefore never mispublish abandoned starting as running. A crash after initial network ownership but before runner assignment deterministically releases the port and converges to dead/paused as appropriate.

### 14.3 Failure domains

| Failure | Impact | Recovery |
|---|---|---|
| Conductor crash/restart | Control interrupted; microVM units and established running data streams survive. | Systemd restarts and reconciliation adopts units. Proxy retains fixed routes for running traffic; paused Wake has no responder and can time out. Cluster node-link reconnects/reports. MMDS confidential authority still clears on routesync loss as below. |
| Proxy worker crash | Its connections close; absolute shared admission counts remain conservatively high. Other workers accept or conservatively reject. | Stats-stream fault first terminates worker. Only after cmd.Wait proves process exit does master clear its index and launch a new epoch there. Route/Sandbox state and Create are unchanged. |
| Proxy master crash | Data unavailable, plugin lease lost and new Create fails closed. | Systemd restarts master, registers, rebuilds shared tables and starts workers. |
| Runner unit/Cloud Hypervisor crash | That Sandbox dies; Restart=no avoids stateful retries. | Reconciliation marks dead; client creates again or resumes a separately preserved paused artifact. |
| Routesync disconnect | Fixed route view stops updating; MMDS routes/values/services immediately unavailable. | Master clears confidential heap, reconnects/registers with exponential backoff and reopens MMDS only after full-sync bookmark (node-proxy.md §4). Fixed routes retain existing retention/convergence behavior. |
| Cluster node-link disconnect | Registry temporarily loses fresh node view. | Node reconnects/registers/reports with exponential backoff (§10/cluster.md); local Sandboxes continue. |
| SQLite corruption | Control unavailable. | File-level backup/rebuild; operators can still discover Sandbox units through ListUnits. |

## 15. Tests

`make test` covers strict MMDS parsing/top-level merge/minimal persistence; encrypted owner values, AAD/CAS/cleanup; admin UDS/service relay/confidential projection; master/worker resync/rotation; HTTP routing; apikey/secretbox/regcreds; routesync registration/bookmarks; proxyshm route sharing/park/wake/generation sweep; proxyadmission multiworker bounded error, generation reuse and crash cleanup after Wait; same-ID plugin replacement; CONNECT tunnels; Exec KAT/64 KiB API/CmdExecSession/H1/H2 request gates and buffered half-close; deterministic MMDS keys; launch ownership; namespace parsing/capacity folding/network merge; migration; and node-link registration/event/command round trips.

Real-microVM native-exec cases cover token issuance, service=exec CONNECT and guest execution separately for standalone and cluster. That evidence covers those native-exec paths; it does not automatically accept later pause/resume or other stages of the aggregate script.

Feature E2E lives with implementation in orchestrator/test/e2e/. Lightweight cluster stubs and real-microVM cases requiring vmlinux, Cloud Hypervisor, mkfs.erofs and sandbox-runtime.bundle share run_all.sh. Direct script invocation sets BIN to the assembled project binary directory. Make test-e2e passes E2E_BIN, defaulting to the sibling project's bin/architecture directory; it does not build those prerequisites. Local stub execution uses make build followed by make test-e2e-cluster-stub. The component integration job assembles candidate sources with the other repositories and runs this entry. Individual scripts can skip missing heavy prerequisites, while full gates use REQUIRE_*=1 to fail instead.

| Script | Coverage |
|---|---|
| e2e_orchestrator.sh | Unit auto-install; /health and 401 control paths; Build register/trigger/status and cross-key ownership 404; bare create/list/kill with KVM. |
| e2e_runtask.sh | Pure-userspace launchers without root/systemd/KVM: pidfile locking/duplicate rejection, exact-run bootstrap, single-stage cold/two-stage restore, execve, TASK_* and duplicate MANIFEST_KEY stripping; config CLI round trips. |
| e2e_builder_unit_upgrade.sh | Isolated real systemd: generated slice update, preserved live runtime properties, and exact-source one-time removal; checks actual cgroup values after reload. |
| e2e_run_builder.sh | Real target-aware pipeline with KVM/vswitch/store-ctl/zot and guest pulls through management VIP: IMG/SBX/SNP; all source kinds; image/checkpoint publication matrix; Manifest/named Bundle; top-level E without full staging/C; portable C image refs; local/Bundle checkpoints; S→E cold selection; fixed memory-C wait; closure and canonical Create after row TTL. Also COPY/bare, parent/ctl isolation with finite VMM limits, live conductor recovery preserving the exact claim/run-id/processes, real total-timeout fencing and claim/reservation release, terminal cleanup and log/DB/artifact secrecy. |
| e2e_execute.sh | Real template cold launch/guest exec; persistent RunDir/BaseDir with diff/checkpoint only in BaseDir; no large RunRoot artifacts; PathID native exec; local Pause/Resume and restore policy; failed Create's owner-free dead; explicit finalizer removes row/directories while preserving node files. |
| e2e_mmds_routes.sh | Execute's Proxy topology for static/secret lifecycle and local UDS service. |
| e2e_mmds_routes_proxy_restart.sh | Proxy topology for MMDS full resync/fail-closed recovery. |
| e2e_orchestrator_proxy.sh | Master/workers, sole data ingress, route sync, authentication, auto-resume and CONNECT relay. |
| e2e_cluster_stub.sh | Real Registry/Router/Placer plus node-stub-ctl with distinct API/Data listeners: node-link, Reserve, control/data/exec/build routing, stable IDs and membership changes. |
| go test ./test/e2e/cluster_stub | Executable h2c integration: Build live projection and empty full snapshot removal of projection/exact-owner ref after a lost Delete. |
| e2e_cluster_real.sh | Real cluster control/node-ctl/microVM: single Registry, node-link redirect and joint Registry-route/node-row/directory convergence after Delete. |
| e2e_density.sh | Node admission, reclamation and density behavior. |
| e2e_sandbox_cold_target.sh | Production-shaped cold Sandbox target driven by node-ctl resource controller. |

Make test-e2e executes test/e2e/run_all.sh. The project repository supplies the common environment, aggregate entry and genuinely cross-component combinations without copying these scripts.

## 16. See also

- [Node Proxy](node-proxy.md): independent forwarding, routing/routesync, authentication, MMDS and CONNECT; §9's sole data ingress, reached through Router in clusters.
- [Node resources](node-resource.md): resource protocol, Sandbox policy and controller organization, embedded in serve at the sole resource_listen endpoint.
- [Cluster](cluster.md): node-link wire (§6, counterpart to this document's §10), Registry and Reserve; [Router](cluster-router.md) data ingress and [Placer](cluster-placer.md) placement/key distribution.
- [Sandbox lifecycle](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/sandbox.md): SANDBOX_CONFIG modes, run/snapshot/restore/connect and cgroups.
- [vSwitch operations](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch-operations.md): attach/detach/open-port; [vSwitch design](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch.md) owns floating IP and mgmt-service/MMDS VIP translation.
- [Flatten](https://github.com/kuasar-sandbox/guest-runtime/blob/main/docs/flatten.md): export, idempotent OCI Referrers and FLATTEN_REGISTRY_*.
- [Manifest](https://github.com/kuasar-sandbox/accelerator/blob/main/docs/manifest.md): content keys, convergent encryption and deduplication domains (§7's storage side).
- [Deployment](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md): topology and unit installation.
- [Demo](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/test/demo/DEMO.md): complete e2b CLI/SDK workflow.
