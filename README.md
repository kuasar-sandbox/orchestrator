[English](README.md) | [简体中文](README_zh.md)

<a id="overview"></a>

# orchestrator

`orchestrator` provides the **E2B-compatible API, node services and multi-node control plane** for [Kuasar Sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox). Use it to build templates, create and manage sandboxes, connect SDK clients, and operate a fleet of compute nodes.

Start with the project [Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md): pull the **node-infra** image with Docker, run it with a dedicated data volume, and execute your first Guest command. The image supplies compatible runtime, network, storage and orchestration components. Then configure a persistent site Node and build your own application template. Use this repository's guides for the operations and configuration below.

<a id="documentation"></a>

## Recommended path

| Your next task | Guide | Completion check |
|---|---|---|
| Run your first Guest command | [Project Quick Start](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/quickstart.md) | The Guest command returns its output and exit status |
| Configure a persistent site Node | [Project deployment](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md), then [Node operation](docs/node.md#operate-a-node) | Conductor and Proxy are running; both client endpoints are reachable |
| Build your application template | [Template Build](docs/node-build.md#client-workflow) | This Build is `ready`; retain its canonical TemplateID |
| Run, pause and reconnect a sandbox | [Sandbox lifecycle](docs/sandbox-lifecycle.md#recommended-workflow) and [SDK access](docs/node-proxy.md#connect-a-client) | Commands and files work through the configured data endpoint |
| Add outbound access or change network policy | [Connector networking guide](https://github.com/kuasar-sandbox/connector/blob/main/docs/egress.md#deployment-and-ownership) | The guest reaches only the intended destinations through the selected egress path |
| Add a second compute node | [First cluster](docs/cluster.md#minimum-cluster) | Both Nodes publish the intended Group revision; the default template is eligible |
| Diagnose capacity, traffic or failures | [Resources](docs/node-resource.md#operate-resources), [Observability](docs/observability.md#diagnose-a-request) | Inspect the original SandboxID/BuildID and the responsible service before retrying |

A template's canonical ID is an output of a successful Build, not an arbitrary image name. An accepted Create starts a lifecycle operation; it does not certify guest readiness. The guides explain what to wait for and which identifiers to retain.

### Capability references

Each topic owns its fields and behavior. User workflows appear first, followed by configuration, recovery and implementation contracts. English and Chinese editions cover the same capabilities.

| Topic | Scope |
|---|---|
| [Node operation](docs/node.md) | Services, configuration, systemd workers, local channels and recovery |
| [Sandbox lifecycle](docs/sandbox-lifecycle.md) | Create, Pause/Resume, workspace, Reset, Delete and migration |
| [Template Build](docs/node-build.md) | Build inputs, target choice, canonical artifacts, cancellation and retention |
| [Access and proxy](docs/node-proxy.md) | SDKs, credentials, native exec, forwarding, MMDS and traffic admission |
| [Resources and admission](docs/node-resource.md) | Capacity, startup policy, resource pools, drain and pressure recovery |
| [Observability](docs/observability.md) | Logs, current stats, exact usage, OTLP metrics and history queries |
| [Cluster control](docs/cluster.md) | Registry/Router, Group publication, Node selection, routing and recovery |
| [Application extensions](docs/extensions.md) | Custom Conductor/Proxy/Telemetry applications and public Go APIs |

<a id="responsibilities"></a>

## Services and deployment choices

<a id="standalone-node"></a>

### Standalone node

A compute node runs **Conductor** for the API and lifecycle, and **Proxy** for sandbox data traffic. Add **Telemetry** when you need metric collection or historical queries. Conductor manages sandbox and Build workers through systemd; users operate the API or SDK, without launching `run-sandbox` or `run-builder` manually.

A standalone node supports the documented E2B SDK/CLI workflows. Its API and data listeners are distinct. Configure both for development HTTP access, or use the deployment's TLS domain. [Access and proxy](docs/node-proxy.md#connect-a-client) explains these choices and the supported SDK versions.

<a id="multi-node-cluster"></a>

### Multi-node cluster

Start with [one SQLite Registry, one Router and two Nodes](docs/cluster.md#minimum-cluster). Each Node retains its local lifecycle and complete Group context. Registry stores cluster metadata and chooses Nodes from their published Group support; Router provides the shared API and data ingress.

**Placer is optional.** Add it for source-driven Group import and credential renewal. Direct Group maintenance through `node-ctl group apply` is sufficient for the first cluster. SQLite supports one Registry; use a shared PostgreSQL or FoundationDB backend when deploying multiple Registry members. Backend selection and recovery are covered by [cluster metadata storage](docs/cluster-storage.md).

<a id="profiles"></a>

## Choose a sandbox profile

- **`e2b`** runs Envd for the supported SDK command, file and code-interpreter services.
- **`bare`** runs a customer image without requiring Envd; use its network services or native exec.

Build registration defaults to `e2b`. The output profile is part of artifact identity and cannot be changed by renaming the template. See [profiles](docs/node.md#two-sandbox-profiles) and [Build targets](docs/node-build.md#artifact-identity).

<a id="main-commands"></a>

## Operator commands

| Command | Purpose |
|---|---|
| `node-ctl --version` / `e2b-key-ctl --version` | Inspect the installed node tools |
| `node-ctl config conductor --config FILE --resolve` | Inspect the effective configuration before deployment |
| `node-ctl conductor serve` / `proxy serve` / `telemetry serve` | Service entry points; normally started by the deployment's units |
| `node-ctl resource status` / `list` / `pressure` | Inspect admission, reservations and resource pressure |
| `node-ctl resource drain` / `drain --disable` | Stop or resume new admission |
| `node-ctl builder status` | Inspect a Build execution |
| `node-ctl group apply -f FILE` | Maintain a Node Group with the tenant API key |
| `run-cluster-ctl status ...` | Inspect authenticated Group publication and template eligibility |
| `run-cluster-ctl registry` / `router` / `placer` | Cluster service entry points from the complete native release bundle |
| `node-ctl export-sandbox` / `import-sandbox` | Transfer portable sandbox state under the [export/import contract](docs/sandbox-lifecycle.md#export-import-command) |
| `e2b-key-ctl ...` | Generate and derive tenant credentials |

Resource `status`, `list` and `drain` use the resource-controller socket. `pressure`, Group and manifest-key administration use the Conductor control socket. If your deployment changes these paths, select the appropriate `--socket`; do not point every command at one socket. Full options are in the capability guides.

<a id="deployment-overview"></a>

## Deploy and operate

Use [node-infra](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/node-infra.md) for the maintained release installation and persistent service layout. Before admitting a workload, configure durable node state and its encryption key, tenant credentials, compatible kernel/runtime artifacts, writable storage, network egress, and measured resource budgets. The [project deployment guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md) owns cross-component installation, trust boundaries and upgrades.

For an existing installation, retain its service names, overlay order, sockets and state paths when applying this repository's examples. `deploy/` contains component examples, not a second configuration to copy over a working node-infra instance. Validate the complete effective configuration, then use the installed service units. A config inspection does not start services or move existing workers.

<a id="security-and-multi-tenancy"></a>

## Credentials and tenant isolation

Install each tenant's **APISecret/ManifestKey pair** through the authorized local administration path. Give application clients the generated API key. Data-plane capabilities have separate purposes and scopes; neither a ManifestKey nor the node database encryption key belongs in an SDK or guest image. Clearing an allowlist affects new resource admission and does not automatically revoke credentials already bound to existing resources. See [credential lifecycle](docs/node-proxy.md#credential-model).

Keep the node's internal listeners and administration sockets within the deployment's trust boundary. Registry/NodeLink TLS and public Router TLS are separate from Router-to-Node transport; follow the [cluster trust boundary](docs/cluster.md#minimum-cluster). Report vulnerabilities privately through the [project Security Policy](https://github.com/kuasar-sandbox/kuasar-sandbox/security/policy).

<a id="development-example"></a>

## Development and customization

Use the project [Workbench](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/workbench/README.md) for source development, demos and cross-component validation. For a custom service integration, start with [Application extensions](docs/extensions.md) and its runnable examples. Component builds, contribution requirements and tests belong to [CONTRIBUTING.md](CONTRIBUTING.md).

<a id="contract-ownership"></a>

## Reference ownership

Each capability guide owns its configuration and failure semantics. Node directories and identities belong to [Node operation](docs/node.md#node-directories-and-identities); public Sandbox identities and cleanup belong to [lifecycle](docs/sandbox-lifecycle.md); credential binding belongs to [access](docs/node-proxy.md#credential-model); artifact publication and retained Build records belong to [Build](docs/node-build.md). Cluster projection and recovery extend those local operations without replacing their owners.

<a id="release-model"></a>

## Release model

This repository publishes component tags `vX.Y.Z`. The main repository publishes aggregate tags `release-vX.Y.Z` that select exact component versions and validate the combined system. The numbers need not match. Install a complete native-architecture release archive, including the cluster launcher's private runtime libraries. Source support does not imply that every historical release has an asset for every architecture.

Version selection, upgrade and rollback rules are maintained in the project [release guide](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/release.md). Use [aggregate releases](https://github.com/kuasar-sandbox/kuasar-sandbox/releases) to select a compatible platform; the [latest Stable aggregate](https://github.com/kuasar-sandbox/kuasar-sandbox/releases/latest) is the normal starting point.

<a id="project-boundaries"></a>

## Project boundaries

| Repository | Responsibility |
|---|---|
| [kuasar-sandbox](https://github.com/kuasar-sandbox/kuasar-sandbox) | Platform installation, user journeys, integration validation and aggregate releases |
| [sandboxer](https://github.com/kuasar-sandbox/sandboxer) | MicroVM lifecycle, snapshots and guest control |
| [accelerator](https://github.com/kuasar-sandbox/accelerator) | Image, artifact and snapshot data infrastructure |
| [connector](https://github.com/kuasar-sandbox/connector) | MicroVM networking and egress |
| [guest-runtime](https://github.com/kuasar-sandbox/guest-runtime) | Guest kernel and runtime image |

<a id="license"></a>

## License

Original project content in this repository is licensed under the [Apache License 2.0](LICENSE). Preserve the applicable attribution and license declarations for generated or third-party material. See [CONTRIBUTING.md](CONTRIBUTING.md) and the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md).

The pinned `rootless-containers/proto/go-proto` module omits its repository-root license. Packaging supplements that exact module version with the unchanged upstream Apache-2.0 notice in [scripts/release-notices/rootless-containers-proto](scripts/release-notices/rootless-containers-proto/NOTICE), checks its digest and retains the normal source/checksum inventory. Other versions still require their own license material.
