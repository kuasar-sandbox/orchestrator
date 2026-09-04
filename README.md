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
- **Router** — the cluster-wide E2B-compatible data-plane entry point and stable-sandbox routing layer;
- **Placer** — node selection, sandbox-group integration, on-demand creation, restore, and migration.

The control plane preserves a stable external sandbox identity while the actual node-local sandbox instance may change during restore or migration.

## Security and multi-tenancy

The node and cluster paths keep platform credentials, content-protection keys, and scoped data-plane capabilities separate. API access, regular data forwarding, and native exec use purpose-specific credentials. Content-protection keys are not exposed to the guest.

Node resource admission is also part of the multi-tenant boundary: the reservation controller owns node-wide admission, resource-pool accounting, grants, watermarks, and recovery. Per-sandbox cgroup, balloon, and VMM execution remains owned by `sandboxer`; the node controller does not take over the sandbox-internal resource loop.

For the complete project security model and private vulnerability reporting, see the [Kuasar Sandbox Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy).

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
| `cluster-ctl router` | Start the cluster data-plane router |
| `cluster-ctl placer` | Start the placement service |
| `e2b-key-ctl ...` | Generate and derive tenant credentials |

The repository also builds `node-stub-ctl`, an E2E helper that simulates node-link participants without starting MicroVMs.

## Profiles

- **`e2b`** — the guest runs Envd and exposes the full E2B-compatible data plane;
- **`bare`** — the guest does not require Envd and is reached through its network services and native platform integration.

## Build and test

The Go binaries are built with `CGO_ENABLED=0`.

```bash
make build                      # node-ctl, cluster-ctl, node-stub-ctl, e2b-key-ctl
make build TARGET_ARCH=aarch64  # cross-compile; amd64/arm64 aliases are accepted
make test                       # unit tests
make test-e2e                   # component owner suite; requires the assembled project BIN
```

Running real sandboxes requires Linux with systemd, root privileges, KVM, `sandboxer`, `connector`, and the Runtime/VMLinux artifacts produced by `guest-runtime`.

A component-local change can use this repository directly. A change that modifies cross-repository contracts must use linked companion pull requests and the project repository's exact-source BMS validation. See the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md).

## Deployment overview

A standalone node normally starts Conductor first and Proxy second. A cluster adds independent Registry, Router, and Placer processes. Configuration examples and systemd units are under `deploy/`.

The user-facing, release-based installation path is documented in the project [Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md). Production deployments should use durable storage, production TLS, protected credentials, an explicit network policy, and capacity settings validated against their workloads.

## Release model

This repository publishes independent component versions named `vX.Y.Z`. The x86_64 component archive contains the node and cluster binaries plus deployment files. Documentation and E2E sources are collected from the selected component tag into the project platform archive rather than duplicated in the component archive.

The project repository publishes aggregate versions named `release-vX.Y.Z`, selecting exact component tags and validating the combined system on real KVM infrastructure. An orchestrator component version and a project aggregate version are related by the aggregate selection; they are not required to have the same number.

See the project [release documentation](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md) and [latest Stable aggregate release](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest).

## Documentation

Detailed design and reference documents are currently maintained primarily in Chinese:

- [`docs/node.md`](docs/node.md) — node architecture, commands, configuration, E2B API, lifecycle, credentials, builds, and reliability;
- [`docs/node-proxy.md`](docs/node-proxy.md) — the independent node data-plane proxy, routing, authentication, MMDS, and native exec;
- [`docs/node-resource.md`](docs/node-resource.md) — node admission, reservations, watermarks, active reclaim, and recovery;
- [`docs/cluster.md`](docs/cluster.md) — Registry membership, replicated state, node links, reservation state, and cluster E2E;
- [`docs/cluster-router.md`](docs/cluster-router.md) — cluster data-plane routing and stable/node-local identity translation;
- [`docs/cluster-placer.md`](docs/cluster-placer.md) — providers, importers, node selection, placement, restore, and migration.

The English README contains the complete public component entry path; translating every detailed design document is not required to build or contribute to the component.

## Project boundaries

- system-level design, shared BMS infrastructure, demos, and aggregate releases belong to [`kuasar-sandbox/kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox);
- MicroVM lifecycle and guest control belong to [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer);
- image and snapshot data infrastructure belongs to [`accelerator`](https://github.com/kuasar-sandbox/accelerator);
- high-density MicroVM networking belongs to [`connector`](https://github.com/kuasar-sandbox/connector);
- the guest kernel and runtime image belong to [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime).

## License

Original project content in this repository is licensed under the [Apache License 2.0](LICENSE). Preserve the applicable attribution and license declarations for generated or third-party material. See [CONTRIBUTING.md](CONTRIBUTING.md) and the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md).
