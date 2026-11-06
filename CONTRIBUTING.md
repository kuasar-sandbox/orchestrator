[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING_zh.md)

<a id="overview"></a>

# Contributing

Thank you for contributing to `kuasar-sandbox/orchestrator`.

Documentation changes follow the shared [documentation contribution policy](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/CONTRIBUTING.md#documentation-contributions).

<a id="contribution-workflow"></a>

## Contribution workflow

Use a public Fork and a topic branch. Contributors can open issues, review changes
and propose PRs without upstream write access. Maintainers review changes, arrange
trusted validation and squash merge; direct pushes to upstream `main` are not the
contribution path.

<a id="prepare-a-fork"></a>

## Prepare a Fork

Fork the repository to your GitHub account, then configure HTTPS remotes:

```bash
git clone https://github.com/<your-login>/orchestrator.git
cd orchestrator
git remote add upstream https://github.com/kuasar-sandbox/orchestrator.git
git fetch upstream
git switch -c feat/short-description upstream/main
# edit, test, and commit
git push -u origin feat/short-description
```

For a maintenance fix, start from the supported `upstream/release/vMAJOR.MINOR.x`
branch instead and target that branch in the PR. Enable **Allow edits from
maintainers** when appropriate.

To advance your Fork's `main` without overwriting local commits or uncommitted work:

```bash
git fetch upstream
git switch main
git merge --ff-only upstream/main
git push origin main
```

Commit or safely preserve uncommitted work before switching. If `--ff-only`
refuses because histories diverged, inspect the local commits and resolve the
history deliberately; do not reset or force-push the Fork's main branch as a
routine synchronization step. Create each new topic branch from the freshly
fetched upstream target, not from an unrelated feature branch.

<a id="pull-request-requirements"></a>

## Pull request requirements

A pull request should:

- have a focused scope and a clear description of the problem and solution;
- link the relevant issue when one exists;
- include or update tests and documentation when behavior changes;
- contain no credentials, private keys, tokens, customer data, or unrelated generated files;
- describe validation already performed and any operational or compatibility risk.

Draft pull requests are welcome for early feedback, but they are not merge candidates.

<a id="source-ownership"></a>

## Source ownership

| Path | Role |
|---|---|
| `cmd/node-ctl` | Conductor/Proxy/Telemetry serve, managed run-sandbox/run-builder, resource status/list/drain, builder status, config, manifest-key, export/import and version |
| `cmd/cluster-ctl` | Independent Registry/Router/Placer processes, config and version |
| `cmd/node-stub-ctl` | E2E node-link participants with separate admin/API/data listeners, restart/reset, Sandbox/Build state and fault injection; no microVM |
| `cmd/e2b-key-ctl` | DB/config-free gen-key, derive-api-secret, gen-apikey, fingerprint and seal-pull-token |
| `config`, `app/conductor`, `app/proxy`, `app/telemetry` | Public declarative configuration and static custom Apps; sealed-memfd/in-place-exec bootstrap and startup Config/Runtime Hooks |
| `internal/telemetry`, `internal/telemetryapp` | Trusted route/peer identity, envd and OTLP Collector receivers, storage/exporters, E2B compatibility and component lifecycle |
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
| `deploy/` | Per-role example YAML and node/Proxy/Telemetry/Registry/Router/Placer systemd units |
| `examples/custom-conductor` | Buildable xconductor entered through node-ctl conductor serve |
| `examples/custom-proxy` | Buildable xproxy entered through node-ctl proxy serve; master reexecutes workers |
| `examples/custom-telemetry` | Buildable static telemetry App with native Collector factories, pipelines and query extension |

<a id="build-and-test"></a>

## Build and test

Local builds use environment-provided Go and inherit its `GOROOT` and
`GOTOOLCHAIN` selection. CI builds and packages use native x86_64/aarch64 runners
with the admitted Workbench image pinned once per architecture and run. Release
builds, ordinary tests and packaging use build mode with an ordinary UID and
private writable paths. The complete `bash scripts/ci-source-checks.sh` gate runs
in Workbench system mode, retaining source/race/vet/helper tests together with
systemd, real Docker telemetry backend and network-namespace checks. Their
required capabilities and assertions remain mandatory. Release orchestration
uses environment-provided `gh` with `api --slurp`; its credentials do not enter
the source environment.
The publisher's archive validator is compiled from trusted publisher source in a
separate Workbench invocation with cache restore/save disabled. Assembly and
publication execute that exact validator without another compilation.

The Go binaries are built with `CGO_ENABLED=0`.

```bash
make build                      # node-ctl, cluster-ctl, node-stub-ctl, e2b-key-ctl
make build TARGET_ARCH=aarch64  # cross-compile; amd64/arm64 aliases are accepted
make test                       # unit and helper tests
ORG=/path/to/exact/sources make test-source  # source/race/vet/integration gates
```

Capture CLI/DB fixture contracts are source integration, not product E2E. The
required source gate builds the exact sibling sandboxer's `sandbox-ctl` and runs
`TestCapturePairCLIToPausedDatabase`, `TestUploadCaptureCLIToPausedDatabase`
and `TestExportPublicationCLIAPI` from this exact orchestrator source. Every
selected group must run at least one subcase and every started subcase must pass;
Skip, failure, parent-only output, or missing exact sibling source fails the gate.

Product E2E follows **prebuilt products → prepare → `<suite>.<case>.sh` → the
shared public runner**. The full filename is the case ID; its first segment is
the suite. The project owns the nine suites `basic`, `storage`, `image`,
`network`, `sandbox`, `snapshot`, `orchestrator`, `builder`, and `telemetry`.
Cases consume `BIN`, `WORK`, `OUT`, `E2E_LIB`, prepared helper paths and prepared
images. They do not compile, search sibling source trees, pull fallback images,
or select a host product in place of a missing prepared input. Real guest builds,
flattening, snapshots and publication are operations under test and stay real.

Metadata, Proxy and guest-telemetry cases can reuse immutable execute-image
preparation when the public runner supplies `E2E_TEMPLATE_CACHE_DIR` and
`E2E_TEMPLATE_CACHE_PROVENANCE`. The first `build_image_template` call still
uses real `fromImage` construction. It exports verified, unencrypted tarstream
content; later calls verify and import it under their own keys into independent
Stores, then register new template/Build identities through `fromTemplate`.
The requested e2b/bare profile remains case-specific. Guest boot, workload,
network, snapshot and cleanup assertions remain real; dedicated Builder/Demo
and `build_ready_template` preparation do not use this cache.

Entries bind the complete prepared digest, native architecture, full execute
image config ID, recipe version and reader/tool hashes. Publication is atomic
under a lock. Matching corruption fails without rebuilding; interrupted staging
is never a hit. No key, encrypted Store, live VM, snapshot or writable disk is
shared. The runner owns retention and never reuses an earlier run's directory.
`run --no-template-cache` (or an older runner without these inputs) preserves
ordinary preparation. Per-case `template-preparation.jsonl` records hit/miss/
disabled and total duration; `template-cache-phases.jsonl` separates production,
export/verification, import and API registration costs. Compare real paired runs
at identical inputs/resources before claiming a speedup.

From the assembled project platform directory, use its sole public entrypoint:

```sh
python3 test/e2e/e2e prepare --release-dir /path/to/platform --workdir /path/to/prepared
python3 /path/to/prepared/test/e2e/e2e list --suite orchestrator
python3 /path/to/prepared/test/e2e/e2e run --workdir /path/to/prepared --include orchestrator.lifecycle.sh
```

Preparation must supply the selected products, helper executables and local
images required by the selection. KVM cases require Linux, root, systemd and
the prepared Kernel/Runtime. Missing inputs fail the case. Helpers own fixture,
process, network, HTTP and observation primitives; each case owns its scenario.
See the project [E2E contract](https://github.com/kuasar-sandbox/kuasar-sandbox/issues/172)
for aggregation and release acceptance.

The [owner coverage table](#node-validation) maps each substantive contract
to its maintained focused case.

Running real sandboxes requires Linux with systemd, root privileges, KVM, `sandboxer`, `connector`, and the Runtime/VMLinux artifacts produced by `guest-runtime`.

Source builds require Go 1.26.1 or newer. Standalone builds resolve the obtainable
versions committed in `go.mod`. Coordinated Preview builds use the selected local
`orchestrator`, `sandboxer`, `accelerator` and `connector` modules:

```bash
GOWORK="$(python3 ../kuasar-sandbox/ci/integration/go_workspace.py --root .. --owners orchestrator)" || exit 1
export GOWORK
make build
```

The workspace verifies module paths and uses exactly this closure without redundant
`replace` directives or changes to `go.mod`/`go.sum`. Product, helper and source tests
share the selected modules. Runtime/Kernel artifacts remain additional prerequisites
for real sandbox tests.

The source `REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh` flow starts real control-plane processes
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
checks do not substitute for running the corresponding real lifecycle, exec, pause/wake, snapshot and MMDS cases.

Cross-repository contract changes require linked companion PRs and exact-source
integration validation. See the [organization contribution guide](https://github.com/kuasar-sandbox/.github/blob/main/CONTRIBUTING.md).

<a id="node-validation"></a>

## Tests

`make test` covers strict MMDS parsing/top-level merge/minimal persistence; encrypted owner values, AAD/CAS/cleanup; admin UDS/service relay/confidential projection; master/worker resync/rotation; HTTP routing; apikey/secretbox/regcreds; routesync registration/bookmarks; proxyshm route sharing/park/wake/generation sweep; proxyadmission multiworker bounded error, generation reuse and crash cleanup after Wait; same-ID plugin replacement; CONNECT tunnels; Exec KAT/64 KiB API/CmdExecSession/H1/H2 request gates and buffered half-close; deterministic MMDS keys; launch ownership; namespace parsing/capacity folding/network merge; migration; and node-link registration/event/command round trips.

Paired-source validation covers real capture producers and CLI responses, atomic
SQL rollback/reopen and lifecycle CAS, preparation identity/digest replay, Builder
recovery of the accepted pair, and strict authenticated tokens. The real CLI/API
publication matrix covers all 16 combinations of S/E × token/template × keep/move
× local/portable. Portable cases import the token, make source artifacts
unavailable, then re-export. Capture cases delete the carrier before returning
the structured result and require torn responses to leave the running row and
old pair unchanged.

Required source integration runs through `make test-source` with Go 1.26.1+
and `ORG` pointing to the selected integration source workspace. It builds the
exact sibling sandboxer's CLI for the three capture/publication groups above;
missing groups, unfinished subcases or any Skip fail. `test/source/builder_state.sh`
executes the required state, concurrency, policy and publication-planning Go
contracts and rejects missing or skipped tests. Unit/race/vet, helper regressions,
privileged UFFD/Collector checks, unit upgrade, journal identity, cluster stub
integration and performance gates remain source checks, independent of product E2E.
Helper regressions exercise the resource cases' actual readiness, grow-event and
live-reservation predicates with delayed, invalid and superseding observations.
They also verify capture result completeness and recovery-before-setup ordering.

Product E2E follows **prebuilt products → prepare → `<suite>.<case>.sh` → the
shared public runner**. The full filename is the case ID; its first segment is
the suite. The project owns the nine suites `basic`, `storage`, `image`,
`network`, `sandbox`, `snapshot`, `orchestrator`, `builder`, and `telemetry`.
Cases consume `BIN`, `WORK`, `OUT`, `E2E_LIB`, prepared helper paths and prepared
images. They do not compile, search sibling source trees, pull fallback images,
or select a host product in place of a missing prepared input. Real guest builds,
flattening, snapshots and publication are operations under test and stay real.

From the assembled project platform directory, use its sole public entrypoint:

```sh
python3 test/e2e/e2e prepare --release-dir /path/to/platform --workdir /path/to/prepared
python3 /path/to/prepared/test/e2e/e2e list --suite orchestrator
python3 /path/to/prepared/test/e2e/e2e run --workdir /path/to/prepared --include orchestrator.lifecycle.sh
```

Preparation must supply the selected products, helper executables and local
images required by the selection. KVM cases require Linux, root, systemd and
the prepared Kernel/Runtime. Missing inputs fail the case. Helpers own fixture,
process, network, HTTP and observation primitives; each case owns its scenario.
See the project [E2E contract](https://github.com/kuasar-sandbox/kuasar-sandbox/issues/172)
for aggregation and release acceptance.

| Maintained case | Required assertions |
|---|---|
| [`basic.orchestrator-cli.sh`](test/e2e/cases/basic.orchestrator-cli.sh); [`orchestrator.api.sh`](test/e2e/cases/orchestrator.api.sh); [`builder.api.sh`](test/e2e/cases/builder.api.sh) | CLI help/config, unit installation, health/authentication, registration/trigger/status, cross-key ownership and invalid requests. |
| [`orchestrator.runtask.sh`](test/e2e/cases/orchestrator.runtask.sh) | Resident unit parent/direct child identity; Run session before assignment; same-run reconnect without duplicate assignment/launch; parent-only descriptor isolation; readiness EOF before child exit; one reap/result report; duplicate-parent rejection; delegated ctl/vmm cleanup; exact-run bootstrap and TASK_*/duplicate MANIFEST_KEY stripping. |
| [`orchestrator.lifecycle.sh`](test/e2e/cases/orchestrator.lifecycle.sh) | WaitAssignment/readiness/init failures, starting SetTimeout/Pause/Kill, Create parking, RunDir/BaseDir ownership, resource/lease observations, list/detail and finalization preserving node files; real runner/runtime/VMM exit convergence and live controller restart preserve exact run and StateSync identity. |
| [`orchestrator.exec.sh`](test/e2e/cases/orchestrator.exec.sh) | Explicit KAT issuance and conditions (empty, exact, OR, cwd/user/TTY), PTY resize/stdin/stdout/stderr/exit, envd initialization/hostname, marker and real FloatingIP HTTP. |
| [`orchestrator.pause-wake.sh`](test/e2e/cases/orchestrator.pause-wake.sh) | Guest /app delegation and frozen process/listener/counter state; accepted Pause cancellation with SIGTERM/143 barrier; same-token waking request; memory-policy node/body/header false/null precedence, cold E, repeated cleanup, native usage and TTL/reaper behavior. |
| [`orchestrator.reset.sh`](test/e2e/cases/orchestrator.reset.sh) | Real current-disk manual/automatic Reset, live-run adoption, conductor interruption recovery, historical checkpoint data, first-launch failures and final deletion. |
| [`orchestrator.cluster-reset.sh`](test/e2e/cases/orchestrator.cluster-reset.sh) | Dead management after node reconnect, route-key Create conflicts, checkpoint ID operations, Reset followed by immediate Wake, autoReset propagation and exact parent deletion through the cluster entry point. |
| [`orchestrator.snapshot.sh`](test/e2e/cases/orchestrator.snapshot.sh) | Local baseline/delta memory restore, disk-layer separation, portable keep/move export, noWake denial, self-only prefetch and cleanup. Real Bundle A/B/C closed graphs preserve historical memory; Store publication, retained local resume and E-only export/import preserve canonical references. |
| [`orchestrator.resource-startup.sh`](test/e2e/cases/orchestrator.resource-startup.sh); [`orchestrator.resource-admission.sh`](test/e2e/cases/orchestrator.resource-admission.sh) | Repeated 8GiB/low-allocation startup and finite VMM versus unbounded parent/ctl limits; default and small-capacity headroom across exec/pause/resume; bounded shutdown. Hardware capacity, explicit 512MiB cold reservation/balloon, Python execution and connected/settled admission precede red-zone rejection. |
| [`orchestrator.resource-control.sh`](test/e2e/cases/orchestrator.resource-control.sh); [`orchestrator.resource-recovery.sh`](test/e2e/cases/orchestrator.resource-recovery.sh); [`orchestrator.resource-reservation.sh`](test/e2e/cases/orchestrator.resource-reservation.sh) | Real sandbox-local pressure/grow and CH balloon delivery without OOM, controller restart/inventory convergence and reservation lifecycle. |
| [`orchestrator.proxy.sh`](test/e2e/cases/orchestrator.proxy.sh); [`orchestrator.proxy-auth.sh`](test/e2e/cases/orchestrator.proxy-auth.sh) | Sole data ingress; explicit local/stable identity, duplicate-ID rejection, parking, static stats/native traffic, correct/missing/wrong token behavior without idle refresh, canonical WebSocket, signed files, passive noWake, raw CONNECT and native exec. |
| [`orchestrator.proxy-wake.sh`](test/e2e/cases/orchestrator.proxy-wake.sh) | Condition-denied exec leaves a paused guest untouched; the same authorized KAT cold-resumes it and health/traffic converge. |
| [`orchestrator.proxy-restart.sh`](test/e2e/cases/orchestrator.proxy-restart.sh) | Prepared custom extension ingress/private WebSocket; missing-master admission 503 without resource ownership; master route resync, worker failure/replacement readiness, metrics and worker reaping. |
| [`orchestrator.mmds.sh`](test/e2e/cases/orchestrator.mmds.sh); [`orchestrator.mmds-recovery.sh`](test/e2e/cases/orchestrator.mmds-recovery.sh) | Independent guests run static, initial/unresolved secret, update/rotation/delete, undeclared-route rejection and UDS service assertions. Recovery restarts Proxy between update and full resync. Both inspect DB/log/route-table plaintext secrecy. |
| [`orchestrator.cluster-lifecycle.sh`](test/e2e/cases/orchestrator.cluster-lifecycle.sh) | One Create after node-link/placer readiness; group/stable-ID exec, E pause/wake, WebSocket, data/stats/list and route/node/directory finalization. Routed Build cancellation frees capacity; retained status is the first request after Router restart; canonical peer/reuse survives transient deletion with no retained directory FDs. |
| [`orchestrator.cluster-recovery.sh`](test/e2e/cases/orchestrator.cluster-recovery.sh) | Three Registries observe a real node-link redirect, restart its actual owner, await node-link and placer convergence, then one Create, data readiness and DELETE finalization on Registry and node. |
| [`builder.image.sh`](test/e2e/cases/builder.image.sh) | Independent duplicate-pool prewarm, real image pull/flatten with request-selected DNS and Phase-A reservation; terminal conflict/list state; e2b/bare image results and canonical cold Create after Build-row TTL. |
| [`builder.steps.sh`](test/e2e/cases/builder.steps.sh) | Real RUN/ENV/WORKDIR, delegated guest cgroups, start/ready inheritance, exact live worker/claim/process adoption after conductor SIGKILL, scoped phase cleanup and A/B/C resource ownership. Snapshot/image metadata, one task-local source preparation, SNP-to-image materialization, and Build MMDS immutability/terminal secrecy remain asserted. |
| [`builder.context.sh`](test/e2e/cases/builder.context.sh) | Unconfigured COPY/files 501, malformed or missing context 400, presigned direct upload and present false→true idempotency, real COPY default/chown ownership and A/B request DNS. |
| [`builder.sandbox.sh`](test/e2e/cases/builder.sandbox.sh) | Direct top-level E without A/B/C, target capacity/env and source closure; E-to-image auto resolution; memory-C fixed no-ready wait, fresh S→E identity and no inherited commands; canonical E/snapshot Create after row TTL. |
| [`builder.failure.sh`](test/e2e/cases/builder.failure.sh) | Repeated unassigned resident runner/Builder units fail run-session admission, are collected with journals retained; real hanging RUN cancellation versus automatic total timeout, queued capacity recovery, claim/process/cgroup/port/directory reclamation, canonical peer and live-guest survival, FD closure and deterministic business-error diagnostics. |
| [`builder.publish.sh`](test/e2e/cases/builder.publish.sh) | Five real local/Bundle and Manifest/named-location publication operations with stable DB/key: no intermediate image for top E, portable image refs under S→E delta, exact Store object changes, closed Bundle directories and canonical image/snapshot Create after row TTL. |
| [`telemetry.proxy.sh`](test/e2e/cases/telemetry.proxy.sh); [`telemetry.backends.sh`](test/e2e/cases/telemetry.backends.sh); [`telemetry.guest.sh`](test/e2e/cases/telemetry.guest.sh) | Prometheus counter baseline/increase after a real 404 and independent backend contracts; real guest OTLP HTTP/gRPC/native traffic, local/stable identity, pause/restart history/noWake, Collector absence/disconnection and namespace ownership. |

<a id="native-usage-read-validation"></a>

### Native usage read validation

The density benchmark measures
bounded reads and FD/goroutine behavior without a machine-specific gate;
native sampling and history append algorithms remain unchanged.

```sh
go test ./internal/orch -run '^$' -bench '^BenchmarkNativeUsageBatch$' -benchmem -count=3
```

This benchmark uses real SQLite object reads and native saved files at
1/16/64 objects per batch. It reports allocations and retained FD/goroutine
deltas; it does not represent guest sampling or remote export throughput.

<a id="e2e-execution-requirements"></a>

## E2E execution requirements

Every `test/e2e/cases/<suite>.<case>.sh` starts with one static
`# e2e-requires: [...]` JSON array below its shebang. The owner format is
`test/e2e/lib/requirements.json` with `{"schema":1}`. Declare the minimum kernel
and service capabilities used by the case and its helpers; use `[]` explicitly
when none are needed. Missing, duplicate or unknown capabilities fail validation.
Normal pinned source selection discovers additions, removals and requirement
changes without a platform case allowlist. Runner labels and placement belong
to the trusted platform configuration, not cases. See the
[capability contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/test/README.md#static-execution-requirements)
for supported names, checks and historical-input compatibility.

<a id="continuous-idle-regression-coverage"></a>

### Continuous idle regression coverage

The maintained `orchestrator.idle-timeout.sh` and `orchestrator.idle-condition.sh` cases run in the public prepared
E2E runner on each supported VM architecture. They exercise never-used Sandboxes,
`idleTimeout=0` and a positive timeout, HTTP and Native Exec memory preservation,
concurrent/streaming traffic, a post-capture barrier that requires the old runner
result to be durable before the paused commit, park/Wake, the unchanged park
deadline, and public UTC conditional Pause. Select both with
`--include orchestrator.idle-timeout.sh --include orchestrator.idle-condition.sh`
during `prepare` and `run`. The UTC case establishes a new Proxy observation
after its setup traffic ends; a clock step during its condition still fails closed.

Each policy emits `idle-timeout-<seconds>.json` under the case output directory:
raw end-to-paused and wake-response samples, state transitions, per-process
CPU/RSS observations, handshake/capture timing and attempt/retry counts.
`pause-exit-facts.jsonl` records each forced exit-before-commit observation. The wake-response
measurement includes client overhead and business execution; starting-to-running
is a 100 ms polling estimate of the Resume interval. These are measurements of
that run, not performance promises. Capture timings at the two deliberately held
barriers include artificial waiting and must be excluded from ordinary capture
latency distributions. Source tests separately
use controlled clocks and barriers for stale async statistics, old RunID/lease
notifications, clock steps and failure recovery. A simulated or skipped case and
an ARM cross-build do not establish real VM acceptance.

<a id="component-packaging"></a>

## Component packaging

The selected owner source declares its complete compiled E2E helper inventory in `release/helper-inputs.json`. Its recipe, `scripts/build-e2e-helpers.sh`, accepts the target architecture and a fresh output directory. It runs only during the source-build stage with the admitted dependency workspace; package preparation and case execution consume its verified binaries. Internal build targets and package paths remain owned here.

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
the official package. Selected Kuasar sources are provided through the explicit Go workspace.

Licenses and notices live under `share/licenses/<component>`. Collection fails on missing notices, unreadable subtrees or partial traversals. Package validation checks required files, payload identities and archive paths, types and modes. Downloads use native GitHub asset sizes and digests. Packages and Releases do not carry source or historical test records, or independent `SHA256SUMS`.

`make release` verifies `ACCELERATOR_VERSION`, `CONNECTOR_VERSION` and `SANDBOXER_VERSION` against clean sibling checkouts, generates the four-module Go workspace, then builds and packages the same outputs. Set `KUASAR_PLATFORM_DIR` when the project checkout is not at `../kuasar-sandbox`. Missing tags, modules or conflicting source directories stop before compilation. The publisher uses human release notes and refuses to replace an existing asset with different bytes.

The four official Go executables must identify their own Orchestrator command
main packages and target the selected Linux architecture (amd64 for x86_64,
arm64 for aarch64) with `CGO_ENABLED=0`. Packaging copies
the twelve deployment files from the selected source tree and collects the actual
Accelerator, Connector and Sandboxer dependency materials using the existing
`RELEASE_*_SOURCE_DIR`, optional `RELEASE_*_SOURCE_SHA` and version selections.
Standalone validation checks the bundle's dependency URL/commit fields agree;
it does not require sibling repositories or dependency tags on the validation host.
Before extraction the archive gate rejects extra payloads, aliases, duplicate
entries, links, incorrect numeric root ownership and incorrect modes.
Material directories remain component-scoped.

<a id="ci-and-merge-policy"></a>

## CI and merge policy

The trusted default-branch wrapper handles PR events; candidate workflows do not decide admission or obtain control-plane secrets. Every contributor may submit public Fork PRs. Privileged execution trust is separate from the right to contribute; maintainers arrange reviewed trusted candidates when required, preserving authorship and discussion.

The [central CI contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md) is the sole authority for admission, exact integration evidence, companion declarations, reruns, credential boundaries and squash merge. Validate the current base/head and companion set; skipped or cancelled execution is not success, and admission or branch protection must not be bypassed. The public Fork/PR contribution workflow remains unchanged.

<a id="licensing-of-contributions"></a>

## Licensing of contributions

By submitting a contribution, you agree that it is licensed under the license
that applies to the files being changed. New project files without a different
explicit license declaration are contributed under the
[Apache License 2.0](LICENSE).

Only submit work that you have the right to contribute. Preserve applicable
copyright, attribution, NOTICE, and SPDX declarations when modifying
third-party or differently licensed material.

<a id="review-expectations"></a>

## Review expectations

Be constructive and specific. Authors should resolve review conversations or explain why a requested change is not applicable. Maintainers may close stale, superseded, unsafe, or out-of-scope pull requests with an explanation.

<a id="documentation-validation"></a>

## Documentation ownership and validation

Keep definitions with their capability owner; README is the entry map and examples keep their runnable project files. A new file needs an independent stable subject, not a flag or PR. Retain complete contracts, code examples and failure semantics. English is the default; an added Chinese edition pairs with `_zh.md`. Validate every changed and newly added Markdown path with the main project `ci/check_docs.py --root . --paths ... --json`, and run `git diff --check`. The checker uses tracked files by default, so new files require explicit `--paths` until staged.
