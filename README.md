[English](README.md) | [简体中文](README_zh.md)

<a id="overview"></a>

# orchestrator

For the first cross-component sandbox or Demo, follow the project
[Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md)
with the matching aggregate release and
[workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README.md).
This component can still be built/deployed independently; workbench is not a
production runtime dependency. Published architectures are specific to the
selected release; source support does not imply all historical assets exist.

`orchestrator` is the **E2B-compatible node service and multi-node control plane** for [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox).

It provides the northbound API, node-local lifecycle orchestration, data-plane proxy integration, tenant credentials, resource admission, and the Registry/Router/Placer control plane used to operate a cluster of Kuasar Sandbox nodes. The repository evolves and releases independently while participating in project-level cross-component validation and aggregate releases.

<a id="documentation"></a>

## Choose a capability

The eight maintained topics below contain both operational usage and the complete capability contract. Public use appears before internal ownership and protocols. Each topic has matching English and Chinese editions. Cross-component installation and upgrade belong to the [main project](https://github.com/kuasar-sandbox/kuasar-sandbox); source contributions and component packaging belong to [CONTRIBUTING.md](CONTRIBUTING.md).

| Topic | Contents |
|---|---|
| [Node operation](docs/node.md) | Services, shared configuration, systemd pools, local channels and recovery |
| [Sandbox lifecycle](docs/sandbox-lifecycle.md) | Create and identity, Pause/Resume, Reset/dead retention, Delete and migration |
| [Template Build](docs/node-build.md) | Build API, request inputs, immutable artifacts, execution, publication and cancellation |
| [Access and proxy](docs/node-proxy.md) | SDK access, credentials, exec, forwarding, MMDS, traffic admission and routesync |
| [Resources and admission](docs/node-resource.md) | Capacity/startup policy, reservation, node pressure, recovery and resource protocols |
| [Observability](docs/observability.md) | Journal output, native stats, lossless usage, OTLP metrics and history queries |
| [Cluster control](docs/cluster.md) | Public ingress, Groups, role configuration, replicated ownership, routing and recovery |
| [Application extensions](docs/extensions.md) | Static App bootstrap, public Go contracts, object sources, hooks and wrappers |

<a id="responsibilities"></a>

## Responsibilities

<a id="standalone-node"></a>

### Standalone node

`node-ctl` runs the compute-node services:

- **Conductor** — authoritative control-plane API, sandbox and build lifecycle, node-local routing, credential management, and optional node resource admission;
- **Proxy** — the single sandbox data ingress for Envd traffic, floating-IP services, native exec, MMDS, and traffic observation;
- **Telemetry** — independent envd/native stats/OTLP collection, trusted sandbox identity, native Collector pipelines, optional local TSDB, separate remote writes and queries, and E2B/custom HTTP handlers;
- **Node link** — an optional client that attaches the node to a cluster control plane;
- **Launch helpers** — systemd-managed sandbox and build runners;
- **Administration** — resource status/drain, template-build status, configuration inspection, manifest-key management, and sandbox export/import.

A standalone deployment exposes an E2B-compatible API that can be used by the unmodified E2B SDK and CLI.

<a id="multi-node-cluster"></a>

### Multi-node cluster

`cluster-ctl` runs three independent roles:

- **Registry** — replicated cluster state and node-channel hub;
- **Router** — the cluster-wide E2B-compatible unified control- and data-plane ingress and stable-sandbox routing layer;
- **Placer** — sandbox-group providers/importers and node-placement recommendations for creation, restore, and migration. Registry and nodes own lifecycle commits and execution.

The control plane preserves a stable external sandbox identity while the actual node-local sandbox instance may change during restore or migration.

<a id="security-and-multi-tenancy"></a>

## Security and multi-tenancy

The node and cluster paths keep platform credentials, content-protection keys, and scoped data-plane capabilities separate. API access, regular data forwarding, and native exec use purpose-specific credentials. Content-protection keys are not exposed to the guest.

Node resource admission is also part of the multi-tenant boundary: the reservation controller owns node-wide admission, resource-pool accounting, grants, watermarks, and recovery. Per-sandbox cgroup, balloon, and VMM execution remains owned by `sandboxer`; the node controller does not take over the sandbox-internal resource loop.

For security architecture and deployment trust boundaries, see the [project system overview](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/architecture.md) and [deployment guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md). Report vulnerabilities privately through the [Kuasar Sandbox Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy).

<a id="main-commands"></a>

## Main commands

| Command | Purpose |
| --- | --- |
| `node-ctl conductor serve` | Start the authoritative node control-plane API |
| `node-ctl proxy serve` | Start the node data-plane proxy master/worker |
| `node-ctl telemetry serve` | Start sandbox metrics collection and history queries |
| `node-ctl run-sandbox` | Launch one sandbox inside its managed unit |
| `node-ctl run-builder` | Launch one template-build execution |
| `node-ctl resource ...` | Inspect or drain node resource reservations |
| `node-ctl builder status` | Inspect build execution state |
| `node-ctl export-sandbox` / `import-sandbox` | Move portable sandbox state across nodes |
| `cluster-ctl registry` | Start a Registry process |
| `cluster-ctl router` | Start the unified cluster control- and data-plane ingress |
| `cluster-ctl placer` | Start the placement service |
| `e2b-key-ctl ...` | Generate and derive tenant credentials |

A successful move export accepts source deletion durably; RunDir, BaseDir and
checkpoint cleanup may finish later. Same-ID Create/import conflicts until cleanup
completes. `--keep-source` preserves the source. See the [node export/import contract](docs/sandbox-lifecycle.md#export-import-command).

The repository also builds `node-stub-ctl`, an E2E helper that simulates node-link participants without starting MicroVMs.

<a id="profiles"></a>

## Profiles

- **`e2b`** — the guest runs Envd and exposes the supported E2B-compatible data services;
- **`bare`** — the guest does not require Envd and is reached through its network services and native platform integration.

<a id="deployment-overview"></a>

## Deployment overview

A standalone node runs Conductor and Proxy; start the independent Telemetry service when needed. Telemetry uses the Plugin Plane without joining Create/Resume barriers, and its failure does not change Sandbox lifecycle. The example configuration explicitly enables embedded Prometheus TSDB. Local storage requires `local.enabled`; an unconfigured query backend defaults to `none`, and writes and queries are assembled independently. See [observability](docs/observability.md#telemetry). A cluster adds Registry, Router and Placer. Configuration examples and systemd units are under `deploy/`.

The release-based installation path is the project [Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md). Production deployments should use durable storage, production TLS, protected credentials, an explicit network policy and capacity settings validated against their workloads.

<a id="development-example"></a>

## Development example

Prepare binaries, kernel/Runtime artifacts, networking and protected configuration using the project Quick Start. Run each long-lived role below in its own service or terminal; these are separate commands, not one foreground shell script. Start Conductor before using its manifest-key administration socket. Examples use the default config-socket path; custom paths require the corresponding CLI option. Treat generated credentials and the shell environment as sensitive.

```bash
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
# Optional: independent metrics collection and history query service
node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
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
python - <<'PY'
from e2b import Sandbox
sandbox = Sandbox.create("<canonical-template-id>")
try:
    print(sandbox.commands.run("uname -a").stdout)
finally:
    sandbox.kill()
PY
```

Continue with [node configuration](docs/node.md), [Sandbox API](docs/sandbox-lifecycle.md), [access and credentials](docs/node-proxy.md), or [cluster ingress](docs/cluster.md#client-access).

<a id="contract-ownership"></a>

## Contract ownership

Node roots, object RunDir/BaseDir and RunID/PathID belong to [node directories and identities](docs/node.md#node-directories-and-identities). Public Create identity, conflicts/retries, state changes, Reset/dead retention and cleanup belong to [Sandbox lifecycle](docs/sandbox-lifecycle.md); credential binding belongs to [access and proxy](docs/node-proxy.md#credential-model). [Build](docs/node-build.md) owns Build directory/task/publication/recovery and retention; [cluster Build](docs/cluster.md#cluster-build) owns the retained Registry projection and reconnect convergence. Canonical artifacts remain long-term launch authority independently of retained Build rows. Named publication is a logical commit without a final-file/parent fsync power-loss guarantee; Build owns the full matrix and failure behavior.

<a id="release-model"></a>

## Release model

This repository publishes independent component versions named `vX.Y.Z`. Each published native-architecture component archive contains the node and cluster binaries plus deployment files. The aggregate platform archive carries selected user guides and canonical E2E runtime inputs from its explicit documentation/test selections. Source-only tests and unselected maintenance material remain in the source repositories.

The project repository publishes aggregate versions named `release-vX.Y.Z`, selecting exact component tags and validating the combined system on real KVM infrastructure. An orchestrator component version and a project aggregate version are related by the aggregate selection; they are not required to have the same number.

Complete component-branch, Preview/Stable/Latest, mutation-group and cancelled-operation retry rules are maintained only in the project [release documentation](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md) and [latest Stable aggregate release](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest).

<a id="project-boundaries"></a>

## Project boundaries

- system-level design, shared integration tests infrastructure, demos, and aggregate releases belong to [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox);
- MicroVM lifecycle and guest control belong to [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- image and snapshot data infrastructure belongs to [`accelerator`](https://github.com/kuasar-sandbox/accelerator);
- high-density MicroVM networking belongs to [`connector`](https://github.com/kuasar-sandbox/connector);
- the guest kernel and runtime image belong to [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime).

<a id="license"></a>

## License

Original project content in this repository is licensed under the [Apache License 2.0](LICENSE). Preserve the applicable attribution and license declarations for generated or third-party material. See [CONTRIBUTING.md](CONTRIBUTING.md) and the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md).

The pinned `rootless-containers/proto/go-proto` module omits its repository-root license. Packaging supplements that exact module version with the unchanged upstream Apache-2.0 notice in [scripts/release-notices/rootless-containers-proto](scripts/release-notices/rootless-containers-proto/NOTICE), checks its digest and retains the normal source/checksum inventory. Other versions still require their own license material.
