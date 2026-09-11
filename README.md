[English](README.md) | [简体中文](README_zh.md)

# orchestrator

`orchestrator` is the **E2B-compatible node service and multi-node control plane** for [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox).

It provides the northbound API, node-local lifecycle orchestration, data-plane proxy integration, tenant credentials, resource admission, and the Registry/Router/Placer control plane used to operate a cluster of Kuasar Sandbox nodes. The repository evolves and releases independently while participating in project-level cross-component validation and aggregate releases.

## Responsibilities

### Standalone node

`node-ctl` runs the compute-node services:

- **Conductor** — authoritative control-plane API, sandbox and build lifecycle, node-local routing, credential management, and optional node resource admission;
- **Proxy** — the single sandbox data ingress for Envd traffic, floating-IP services, native exec, MMDS, and traffic observation;
- **Node link** — an optional client that attaches the node to a cluster control plane;
- **Launch helpers** — systemd-managed sandbox and build runners;
- **Administration** — resource status/drain, template-build status, configuration inspection, manifest-key management, and sandbox export/import.

A standalone deployment exposes an E2B-compatible API that can be used by the unmodified E2B SDK and CLI.

### Multi-node cluster

`cluster-ctl` runs three independent roles:

- **Registry** — replicated cluster state and node-channel hub;
- **Router** — the cluster-wide E2B-compatible unified control- and data-plane ingress and stable-sandbox routing layer;
- **Placer** — sandbox-group providers/importers and node-placement recommendations for creation, restore, and migration. Registry and nodes own lifecycle commits and execution.

The control plane preserves a stable external sandbox identity while the actual node-local sandbox instance may change during restore or migration.

## Security and multi-tenancy

The node and cluster paths keep platform credentials, content-protection keys, and scoped data-plane capabilities separate. API access, regular data forwarding, and native exec use purpose-specific credentials. Content-protection keys are not exposed to the guest.

Node resource admission is also part of the multi-tenant boundary: the reservation controller owns node-wide admission, resource-pool accounting, grants, watermarks, and recovery. Per-sandbox cgroup, balloon, and VMM execution remains owned by `sandboxer`; the node controller does not take over the sandbox-internal resource loop.

For security architecture and deployment trust boundaries, see the [project system overview](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/kuasar-sandbox.md) and [deployment guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md). Report vulnerabilities privately through the [Kuasar Sandbox Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy).

## Main commands

| Command | Purpose |
| --- | --- |
| `node-ctl conductor serve` | Start the authoritative node control-plane API |
| `node-ctl proxy serve` | Start the node data-plane proxy master/worker |
| `node-ctl run-sandbox` | Launch one sandbox inside its managed unit |
| `node-ctl run-builder` | Launch one template-build execution |
| `node-ctl resource ...` | Inspect or drain node resource reservations |
| `node-ctl builder status` | Inspect build execution state |
| `node-ctl export-sandbox` / `import-sandbox` | Move portable sandbox state across nodes |
| `cluster-ctl registry` | Start a Registry process |
| `cluster-ctl router` | Start the unified cluster control- and data-plane ingress |
| `cluster-ctl placer` | Start the placement service |
| `e2b-key-ctl ...` | Generate and derive tenant credentials |

The repository also builds `node-stub-ctl`, an E2E helper that simulates node-link participants without starting MicroVMs.

## Profiles

- **`e2b`** — the guest runs Envd and exposes the full E2B-compatible data plane;
- **`bare`** — the guest does not require Envd and is reached through its network services and native platform integration.

## Source ownership

| Path | Role |
|---|---|
| `cmd/node-ctl` | Conductor/Proxy serve, managed run-sandbox/run-builder, resource status/list/drain, builder status, config, manifest-key, export/import and version |
| `cmd/cluster-ctl` | Independent Registry/Router/Placer processes, config and version |
| `cmd/node-stub-ctl` | E2E node-link participants with separate admin/API/data listeners, restart/reset, Sandbox/Build state and fault injection; no microVM |
| `cmd/e2b-key-ctl` | DB/config-free gen-key, derive-api-secret, gen-apikey, fingerprint and seal-pull-token |
| `config`, `app/conductor`, `app/proxy` | Public declarative configuration and static custom Apps; sealed-memfd/in-place-exec bootstrap and startup Config/Runtime Hooks |
| `internal/orch` | Node lifecycle, Build pools, local route authority, unit generation and restart reconciliation |
| `internal/nodectl` | Reservation admission, pools/watermarks/grants, inventory/StateSync recovery and audit; not the Sandbox balloon/cgroup loop |
| `internal/nodelink` | Conductor–Registry registration, heartbeat, events and commands over framed JSON/h2c |
| `internal/{registry,router,placer}` | Replicated shardkv namespaces, Reserve/Build state and node-link key cache; unified E2B ingress/active cache; provider/importer, WATCH_LIST and P2C placement |
| `internal/api` | E2B control REST, X-API-KEY authentication, export/import and ExecAccessToken issuance |
| `internal/proxy`, `internal/proxyshm`, `internal/proxyadmission`, `internal/routesync` | L7 forwarding, service-addressed CONNECT/native-exec gate, fixed shared route view, independent inflight arena, confidential MMDS heap, park/wake and generation cleanup; route/node-link register/bookmark/typed results |
| `internal/configsock` | Run/task/admin/plugin/API planes, exact-run bootstrap/prepare, manifest-key/MMDS/Builder administration and controlled routes; SO_PEERCRED/pidfile authentication |
| `internal/{apikey,secretbox,keys,regcreds}` | APISecret/API-key MAC, AES-GCM root storage, Forward/Exec kat1 and data tokens, ManifestKey-wrapped image-pull credentials |
| `internal/{config,clustercfg,sandboxcfg,store}` | Node/cluster configuration, SANDBOX_CONFIG rendering and node SQLite Sandbox/Build/credential-pair state |
| `internal/{mmds,mmdsrpc,mmdssvc,metrics,launcher,vswitch,util}` | MMDSv2/exact routes, worker-master queries, HTTP-over-UDS services, Prometheus, systemd D-Bus, connector-ctl adaptation and utilities |
| `deploy/` | Per-role example YAML and node/Proxy/Registry/Router/Placer systemd units |
| `examples/custom-conductor` | Buildable xconductor entered through node-ctl conductor serve |
| `examples/custom-proxy` | Buildable xproxy entered through node-ctl proxy serve; master reexecutes workers |

## Build and test

The Go binaries are built with `CGO_ENABLED=0`.

```bash
make build                      # node-ctl, cluster-ctl, node-stub-ctl, e2b-key-ctl
make build TARGET_ARCH=aarch64  # cross-compile; amd64/arm64 aliases are accepted
make test                       # unit tests
make test-e2e                   # component owner suite; requires the assembled project BIN
```

Running real sandboxes requires Linux with systemd, root privileges, KVM, `sandboxer`, `connector`, and the Runtime/VMLinux artifacts produced by `guest-runtime`.

Source builds require Go 1.24 or newer and sibling checkouts of `accelerator`,
`connector` and `sandboxer`, even for a change confined to this repository. The
internal `require` versions identify each dependency's target formal release;
Daily Preview development uses the same target without its preview suffix. A
target tag need not exist yet because the tracked local `replace` directives
select the actual sibling sources. `GOWORK=off` does not disable those directives.
Do not infer which source revision was tested from the version label: record the
exact sibling SHAs. Runtime/Kernel artifacts are additional prerequisites for
real sandbox tests, not Go-only compilation.

The local `make test-e2e-cluster-stub` flow starts real control-plane processes
but no MicroVMs. Its run directory is private. Failure output reports diagnostic
filenames and sizes, not raw responses, logs or capability-bearing objects.
Daemon output is written directly to private files, not streamed to CI; the
private diagnostic umask is applied only after any local binary build.
For local diagnosis, set `CLUSTER_STUB_KEEP_WORK=1` to retain the run directory
and inspect it privately; do not upload its unredacted files. This does not
replace the real MicroVM integration suite.

The real execute/MMDS cases allocate switch, namespace, veth and runner/builder
unit names from their existing private run directory. Cleanup stops only those
unit instances and removes only resources created by that run. Explicit names
that already exist or have inconsistent switch state are refused, not adopted
or force-deleted. The cases hold a host-local lock on the existing systemd runtime
directory because their routes, forwarding and slice names are shared; a second
concurrent execute/MMDS invocation is refused before resource creation. The lock
descriptor is not inherited by daemons. Async launch assertions wait for the
actual runner observation without relaxing sandbox identity or launch-mode
checks. `make test` includes isolated cleanup/concurrency regressions; those
checks do not substitute for running both real execute and MMDS cases.

Cross-repository contract changes require linked companion PRs and exact-source
integration validation. See the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md).

## Deployment overview

A standalone node normally starts Conductor first and Proxy second. A cluster adds independent Registry, Router, and Placer processes. Configuration examples and systemd units are under `deploy/`.

The user-facing, release-based installation path is documented in the project [Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md). Production deployments should use durable storage, production TLS, protected credentials, an explicit network policy, and capacity settings validated against their workloads.

## Development example

Prepare binaries, kernel/Runtime artifacts, networking and protected configuration using the project Quick Start. Run each long-lived role below in its own service or terminal; these are separate commands, not one foreground shell script. Start Conductor before using its manifest-key administration socket. Examples use the default config-socket path; custom paths require the corresponding CLI option. Treat generated credentials and the shell environment as sensitive.

```bash
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

The examples use plaintext API `:3000` and data `:3443`. Configuration may enable resource_listen and Proxy traffic.max_inflight defaults. Cluster registration requires distinct api_endpoint/data_endpoint values; production ingress TLS and trusted node-link/network boundaries follow the deployment owner.

```bash
# Tenant roots: the standalone default derives APISecret from ManifestKey.
MK=$(e2b-key-ctl gen-key)
API_SECRET=$(e2b-key-ctl derive-api-secret "$MK")
node-ctl manifest-key add --api-secret "$API_SECRET" --label tenant-a "$MK"
export E2B_API_KEY=$(e2b-key-ctl gen-apikey "$API_SECRET")

# Optional cluster roles, each in its own service/terminal.
cluster-ctl registry --config /etc/cluster-ctl/registry.yaml
cluster-ctl router   --config /etc/cluster-ctl/router.yaml
cluster-ctl placer   --config /etc/cluster-ctl/placer.yaml

# Standalone development; replace host and the canonical template placeholder.
export E2B_API_URL=http://host:3000
export E2B_SANDBOX_URL=http://host:3443
python -c 'from e2b import Sandbox; s = Sandbox.create("<canonical-template-id>"); print(s.commands.run("uname -a").stdout)'
```

Complete commands/configuration/API and cluster entry rules remain in [Node](docs/node.md) and [Cluster](docs/cluster.md).

## Contract ownership

Node roots, object RunDir/BaseDir, RunID/PathID and cleanup ordering are maintained in [Node paths and lifecycle](docs/node.md). Create input identity, stable versus node-local identity, credential binding, conflicts and retry behavior live in [Node §4.1.2](docs/node.md#412-create-identity). The complete Build directory/task/publication/recovery and retention contracts live in [Node Build](docs/node-build.md); retained Registry Build projection and reconnect convergence live in [Cluster](docs/cluster.md). Canonical artifacts remain launch authority independently of retained Build rows. Named publication is a logical commit, without a final-file/parent fsync power-loss guarantee; Build owns the full matrix and failure behavior.

## Release model

Build from the selected source with the component Makefile. `release.sh package`
uses the matching binaries in `bin/<arch>`, or an explicit `RELEASE_BIN_DIR`;
it collects materials and creates the bundle without rebuilding those binaries
or resetting source/build caches. Keep the selected source checkouts, dependency
versions and native build records together with the outputs.

Packaging records the actual Go versions and effective module replacements.
Go/module LICENSE and NOTICE files come from the selected compiler installation
and matching module sources, preserving nested paths. Module resolution uses the
normal Go cache and routing; downloaded module checksums must match the binaries.
Only explicitly collected internal sibling dependencies use their own source
materials; an organization namespace alone does not exempt other modules.
Unsupported third-party local replacements need versioned module inputs for
the official package. Existing Kuasar local `replace` directives remain in use.

Materials live under `share/licenses/<component>` and
`share/sources/<component>`. The latter contains `SOURCES.tsv`,
`GO-BUILD-INFO.tsv`, `GO-MODULES.tsv` and `MATERIALS.sha256`.
Collection fails on missing notices, unreadable subtrees or partial traversals.
Independent validation checks the shipped inventory, checksums, required files,
source-record consistency, payload identities and archive paths/types/modes.
It does not fetch source checkouts or Go modules, compare notices with remote
source trees, or download/authenticate compiler distributions. Checksums and
VCS records are consistency checks, not proof of an arbitrary producer's identity.

The archive name identifies the requested release target. Project and internal
dependency records use a release version when its local tag matches the selected
commit, otherwise `git:<commit>`; packaging does not require creating future
target tags. The publisher passes the selected project SHA to validation before
Tag/Release writes, uses the bundle's `release-notes.md` body, and appends the
existing source/Preview markers. Trusted source selection, build/publish permission
separation and the refusal to replace published assets remain required.

The four official Go executables must identify their own Orchestrator command
main packages and target Linux/amd64 with `CGO_ENABLED=0`. Packaging copies
the ten deployment files from the selected source tree and collects the actual
Accelerator, Connector and Sandboxer dependency materials using the existing
`RELEASE_*_SOURCE_DIR`, optional `RELEASE_*_SOURCE_SHA` and version selections.
Standalone validation checks the bundle's dependency URL/commit fields agree;
it does not require sibling repositories or dependency tags on the validation host.
Before extraction the archive gate rejects extra payloads, aliases, duplicate
entries, links, incorrect numeric root ownership and incorrect modes.
Material directories remain component-scoped.

This repository publishes independent component versions named `vX.Y.Z`. The x86_64 component archive contains the node and cluster binaries plus deployment files. Documentation and E2E sources are collected from the selected component tag into the project platform archive rather than duplicated in the component archive.

The project repository publishes aggregate versions named `release-vX.Y.Z`, selecting exact component tags and validating the combined system on real KVM infrastructure. An orchestrator component version and a project aggregate version are related by the aggregate selection; they are not required to have the same number.

Complete component-branch, Preview/Stable/Latest, mutation-group and cancelled-operation retry rules are maintained only in the project [release documentation](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md) and [latest Stable aggregate release](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest).

## Documentation

Detailed design and reference documents provide complete English and Chinese editions:

- [`docs/node.md`](docs/node.md) — node architecture, commands, configuration, E2B API, lifecycle, credentials, and reliability;
- [Node template builds](docs/node-build.md): full Build API, configuration, execution, publication and recovery.
- [Runtime extensions](docs/extensions.md): complete Conductor/Proxy SDK lifecycle, object sources, Hooks and wrappers.
- [`docs/node-journald.md`](docs/node-journald.md) ([Chinese edition](docs/node-journald_zh.md)) — explicit sandbox/Build output identities, StableID, queries, and validation;
- [`docs/node-proxy.md`](docs/node-proxy.md) — the independent node data-plane proxy, routing, authentication, MMDS, and native exec;
- [`docs/node-resource.md`](docs/node-resource.md) — node admission, reservations, watermarks, inventory recovery, and statistics;
- [`docs/cluster.md`](docs/cluster.md) — Registry membership, replicated state, node links, reservation state, and cluster E2E;
- [`docs/cluster-router.md`](docs/cluster-router.md) — unified cluster control- and data-plane ingress, routing, and stable/node-local identity translation;
- [`docs/cluster-placer.md`](docs/cluster-placer.md) — providers/importers, WATCH_LIST, source leases, selector patches, and placement recommendations.

Use the language selector at the beginning of each paired specification. Shared contracts have one complete owner; the guides link to that owner instead of maintaining parallel schemas.

## Project boundaries

- system-level design, shared integration tests infrastructure, demos, and aggregate releases belong to [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox);
- MicroVM lifecycle and guest control belong to [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- image and snapshot data infrastructure belongs to [`accelerator`](https://github.com/kuasar-sandbox/accelerator);
- high-density MicroVM networking belongs to [`connector`](https://github.com/kuasar-sandbox/connector);
- the guest kernel and runtime image belong to [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime).

## License

Original project content in this repository is licensed under the [Apache License 2.0](LICENSE). Preserve the applicable attribution and license declarations for generated or third-party material. See [CONTRIBUTING.md](CONTRIBUTING.md) and the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md).
