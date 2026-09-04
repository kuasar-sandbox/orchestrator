# Kuasar Sandbox Orchestrator

[简体中文](README.md) · [Kuasar Sandbox project](https://github.com/kuasar-sandbox/kuasar-sandbox)

`orchestrator` provides the E2B-compatible node service and the multi-node control plane for Kuasar Sandbox. It turns the lower-level MicroVM, data, guest-runtime, and networking components into node and cluster services with stable sandbox identities, lifecycle coordination, resource admission, data-plane routing, template builds, and operational recovery.

The repository is one of five loosely coupled Kuasar Sandbox components. It can be developed and released independently; the project repository owns system-level documentation, cross-component BMS/E2E, demos, and aggregate release selection.

## What it provides

### Single-node service

`node-ctl` is the node-facing control and data-plane entry point. Depending on configuration and profile, it provides:

- E2B-compatible sandbox and template APIs;
- sandbox create, connect, execute, pause, resume, migrate, and terminate lifecycle coordination;
- template/build registration and execution;
- tenant identity and scoped access-token handling;
- node resource admission and reservation accounting;
- proxying and route activation for sandbox services;
- systemd-managed sandbox and build processes;
- integration with `sandboxer`, `connector`, `accelerator`, and `guest-runtime` artifacts.

The node service supports an E2B-oriented profile and lower-level native/Bare integration paths. The exact API and configuration surface is documented in this repository's `docs/` tree and in the project-level Quick Start.

### Multi-node control plane

The cluster deployment separates three responsibilities:

- **Registry** — maintains cluster state, node connections, sandbox groups, and projections of node-reported execution facts.
- **Router** — resolves stable sandbox identities to the node-local execution identity and forwards data-plane traffic to the current location.
- **Placer** — selects nodes and coordinates create, restore, import, and migration decisions.

External platform management can integrate through the published provider/importer boundaries without depending on the internal details of memory fault handling, block devices, manifests, caches, or the VMM.

## Resource model

The node-level Reservation Controller owns admission, the shared resource pool, watermarks, grants, inventory reconciliation, and restart recovery. It does not replace the per-sandbox resource loop implemented by `sandboxer`.

The intended high-density model is:

1. idle sandboxes release CPU and inactive memory where possible;
2. long-idle sandboxes may be paused and represented by snapshots;
3. reclaimed capacity returns to the node's shared pool;
4. admission, dynamic budgets, reclamation, and safety reserves protect concurrently active work;
5. higher sandbox density is the result of better node utilization, not an objective that permits OOM-driven work loss.

## Component boundaries

| Component | Orchestrator interaction |
| --- | --- |
| [`sandboxer`](https://github.com/kuasar-sandbox/sandboxer) | Executes the MicroVM lifecycle, snapshots/restores, guest control, memory/block-device paths, and native execution |
| [`accelerator`](https://github.com/kuasar-sandbox/accelerator) | Supplies local/shared/remote data references, image and snapshot data paths, stores, caches, and image construction services |
| [`connector`](https://github.com/kuasar-sandbox/connector) | Attaches sandbox networking and provides the high-density forwarding and trusted network-identity foundation |
| [`guest-runtime`](https://github.com/kuasar-sandbox/guest-runtime) | Supplies the guest Runtime and VMLinux release units consumed by sandbox execution and template construction |
| [`kuasar-sandbox`](https://github.com/kuasar-sandbox/kuasar-sandbox) | Selects exact component releases and validates the complete system |

This repository does **not** own the project-level aggregate release builder; that responsibility belongs to the project repository.

## Build and test

This is primarily a Go repository. For a standalone development checkout, keep workspace overrides disabled unless you intentionally use the six-repository sibling workspace:

```bash
GOWORK=off go test ./...
GOWORK=off go build ./...
```

Use the repository `Makefile` and existing Chinese README for the authoritative component targets, generated files, native prerequisites, and binary names for the selected commit.

Tests have different trust and infrastructure requirements:

- unit and static checks should run without production credentials;
- node integration tests may require systemd, cgroup v2, root, KVM, local registry/storage services, and component binaries;
- cluster tests may require several processes or nodes and stable test ports;
- cross-component changes run through the project's exact-source-set BMS.

A skipped privileged test is not evidence that the privileged path passed. Record executed, skipped, and BMS-delegated validation separately in pull requests.

## Cross-repository changes

Most changes should remain in this repository. A change to an exported API, wire protocol, release contract, or integration boundary may require companion pull requests.

For companion work:

- link every companion PR;
- identify each exact head commit and target branch;
- describe merge and rollout compatibility;
- let BMS validate the exact source set;
- do not merge one side while required companions remain incomplete.

See the project [English contribution guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/CONTRIBUTING_EN.md).

## Documentation

- [Project English overview](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/README_EN.md)
- [English Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart_en.md)
- [Project architecture](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/kuasar-sandbox.md)
- [Deployment guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md)
- [Release overview](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/releases_en.md)
- [Repository documentation](docs/)

The detailed component documents may remain Chinese during the initial public-source launch. Public behavior, configuration, build prerequisites, security boundaries, and release contracts must always have an accurate English entry point.

## Releases

The component publishes its own versioned artifacts. A Kuasar Sandbox aggregate release selects an exact orchestrator version together with exact `sandboxer`, `accelerator`, `connector`, Runtime, and VMLinux versions. Use the aggregate release selection rather than combining similarly numbered component archives by assumption.

- [Component releases](https://github.com/kuasar-sandbox/orchestrator/releases)
- [Aggregate releases](https://github.com/kuasar-sandbox/kuasar-sandbox/releases)

## Security

Do not report vulnerabilities in public issues. Use the project repository's [English Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/SECURITY_EN.md) and GitHub private vulnerability reporting. Cross-component reports are routed privately to the affected maintainers.

Never include real API secrets, tenant keys, internal endpoints, customer data, production logs, or unredacted authorization headers in issues, pull requests, tests, or workflow output.

## License

Project-owned code is licensed under the [Apache License 2.0](LICENSE). Preserve the copyright, source, and license obligations of generated APIs, compatibility code, vendored material, third-party protocol definitions, and redistributed artifacts.