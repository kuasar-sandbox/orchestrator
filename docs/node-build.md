[English](node-build.md) | [简体中文](node-build_zh.md)

# Node template builds

This is the complete node Build contract: registration and target selection, immutable execution resources, task preparation, sequential phase execution, publication and recovery. Shared process/socket/credential ownership remains in [node.md](node.md); Registry-owned cluster scheduling and intent remain in [cluster.md](cluster.md).

<a id="42-控制面模板构建-api"></a>
<a id="42-control-plane-template-build-api"></a>
## 1. Build API

These implement the e2b v2 Build-system endpoint family used by SDK Template.build and CLI; pipeline/resource semantics are in §5.

| Operation | Method and path | Contract |
|---|---|---|
| Register | POST /v3/templates → 202 | name/tags/profile?/cpuCount/memoryMB/metadata?/envVars?/secure? define immutable Build.Resources, never Sandbox capacity. X-Kuasar-Sandbox-Builder.resources may assert identical CPU/memory and add storage; disagreement returns 400. Builder target is register-only immutable input. X-Kuasar-Sandbox-Resource configures final template resources, not A/B. Profile defaults e2b and accepts e2b/bare. Response exposes requested target, auto when omitted |
| Trigger | POST /v2/templates/{tid}/builds/{bid} → 202 | fromImage/fromTemplate/fromImageRegistry username/password/steps/startCmd/readyCmd. Only one registered→waiting transition. Compatibility cpuCount/memoryMB may only assert registration values; increase/decrease returns 400 before credential/COPY/queue side effects. Trigger metadata, Builder/Resource and other general configuration headers are rejected. Insufficient execution capacity leaves FIFO waiting on the fixed node |
| Status | GET /templates/{tid}/builds/{bid}/status | SDK fields, requested target, terminal derived kind, canonical resources cpuMilli/memoryBytes/storageBytes, executionClaimed/runID/systemdEnforcement/storageEnforcement and phase name/sandboxID. In-progress SDK status remains building while retaining internal phase/claim details |
| Files | GET /templates/{tid}/files/{hash} → 201 | Resolve tid→Build→owner, then return present/url. present skips duplicate upload; URL is a direct-bucket presigned PUT, never bytes through control plane. No files_storage returns 501; missing/non-owned tid returns 404 ([§5](#5-target-aware-execution-and-publication)) |
| List | GET /templates | Tenant's ready templates with persistent templateID, immutable profile, requested target and resolved artifact kind |

<a id="44-templateid-与模板形态transient--persist无-templates-表"></a>
<a id="44-template-ids-and-transientpersistent-forms"></a>
## 2. Template IDs and artifact authority

```
persist  templateID = <profile>-<kind>-<base64url(canonical-portable-ref)>
                                            profile∈{e2b,bare}; kind∈{img,sbx,snp}
transient templateID = transient-<uuidv7>       Temporary registration handle; use the persistent ID after Build
```

- **Persistent IDs are self-describing.** Their payload is canonical manifest:// or a content-identified located file ref, e.g. `file://<digest>.image@digest:<digest>@location:<name>`; encrypted tarstreams use @hmac, Bundles use @manifest. Runtime parses profile, kind (img cold image, sbx cold Sandbox E, snp memory Snapshot S) and portable ref. Connect may explicitly cold-start an snp source, while that template's default is memory. Unlocated local refs, host absolute paths, noncanonical refs or mismatched artifact kinds fail.
- **Transient IDs** originate at registration. Build completion returns a persistent ID and stores names/aliases against that Build. Later launches use the persistent ID. Status, transient ID, name/alias and node-local list exist only during terminal-row retention.
- **There is no templates table.** TemplateID encodes profile/kind/ref; its artifact remains long-term launch authority. Builds hold execution and short-lived status/index/aliases, not a template catalog. After terminal-row deletion, canonical IDs still create img/sbx/snp sandboxes and serve directly as later fromTemplate. Snapshot promotion ([§8.1](node.md#81-artifact-capture-templates-and-migration)) likewise requires no Build row.

## 3. Build configuration

These Conductor configuration fields govern the Build service. Shared process, path and credential configuration remains in [node.md](node.md#3-configuration).

| Field | Default | Meaning |
|---|---|---|
| `units.builder_pool_size` | `0` | Prestarted idle Builder count; must be zero with aggregate execution CPU/memory limits. Shared units.dir/install/pool_wait_timeout remain in Node §3; complete unit and claim/readback ordering are in §4.1 |
| `builder.admission.execution.max_builds` | `2` | Maximum simultaneous durable execution claims |
| `builder.admission.execution.resources.{cpu,memory,storage}` | Unlimited | Aggregate execution vector; CPU/memory also constrain sandbox-builder.slice, while V1 storage is admission accounting only |
| `builder.admission.registration` | Entire resolved execution block | Registration limits for nonterminal Builds. An explicit block does not inherit missing fields; each finite dimension must be at least execution's |
| `builder.registration_ttl` / `.queue_ttl` | `1h` / `30m` | Durable expiry for untriggered registered and queued waiting Builds; terminal rows remain queryable but release registration usage |
| `builder.terminal_ttl` | `24h` | Positive Go duration retaining ready/error history after cleanup and release of execution/runtime/result ownership |
| `builder.insecure_registry` | `false` | Allow plaintext HTTP for base-image pulls, e.g. a local development registry |
| `builder.platform` | Empty | Pull platform, e.g. linux/amd64 |
| `builder.image_uri_mask` | Empty | Client-pushed image naming convention with templateID/buildID placeholders; match CLI E2B_IMAGE_URI_MASK. Used when trigger omits fromImage; must be reachable inside Build guests ([§5](#5-target-aware-execution-and-publication)) |
| `builder.referer` | Disabled | fromImage OCI Referrers cache: enabled defaults false; fallback/writeback true. Public owner desc is required when enabled; empty key equals desc; validity is optional Go duration. Request Builder options may further disable lookup/writeback, never enable disallowed behavior ([§4.6](node.md#46-sandbox-configuration-propagation), [§5](#5-target-aware-execution-and-publication)) |
| `builder.diff_template` | — | Preformatted sparse ext4 for pull cache, step changes and export scratch; suggested size at least three times the largest expected image |
| `builder.{pull,step,ready,total}_timeout_sec` | `600`/`600`/`120`/`1800` | Guest pull/flatten, each RUN step, readyCmd polling and whole Build. Step budget also reaches the guest as Connect-Timeout-Ms. Ready polling interval is 2s; absent readyCmd waits 20s. The unit sets no TimeoutStartSec; fencing/host cleanup has a separate 60-second window (§6) |
| `builder.files_storage` | Empty | COPY context S3/OBS storage: endpoint/region/bucket (required)/prefix/access_key/secret_key/force_path_style/presign_expiry. Empty returns 501 for COPY. Conductor only presigns/HEADs. Custom Runtime credentials override YAML/AWS defaults, support session token/expiry/refresh and never fall back after error. force_path_style defaults false; versitygw/minio use true. PUT expiry defaults 1h; GET uses total+5m. Local deployments may use versitygw ([§5](#5-target-aware-execution-and-publication)) |

The Builder schema replaces these legacy names without aliases:

```text
builder.max_concurrent -> builder.admission.execution.max_builds
builder.cpu_quota      -> builder.admission.execution.resources.cpu
builder.memory_max     -> builder.admission.execution.resources.memory
builder.vcpu/memory    -> Removed; A/B phases derive from immutable Build.Resources
```

An explicit registration block does not inherit omitted execution dimensions; an absent block inherits the entire resolved execution block. Only incompatible development databases missing the current schema marker require rebuild. Additive migrations for supported schemas and the no-dual-reader boundary are defined in §6.

### 3.1 Request-scoped Builder input

Build endpoints additionally accept build-only kuasar-sandbox.builder / X-Kuasar-Sandbox-Builder:

```json
{
  "target":{"kind":"sandbox","memory":false},
  "resources":{"cpu":4,"memory":"8GiB","storage":"64GiB"},
  "referer":{"enabled":false,"writeback":false}
}
```

Target allows image, sandbox+memory:false or sandbox+memory:true. Omission means auto; explicit null, unknown/duplicate fields or image+memory:true fails. Omitted sandbox.memory means false. Auto resolves only after source E defaults: either nonempty effective start/ready command selects memory Sandbox, otherwise Image. It never automatically selects top-level E. Explicit Image conflicts with explicit trigger start/ready and ignores inherited source commands.

Build resources govern execution/admission only; referer/registry govern this Build. Parsed target/build-only fields leave template metadata and persist in builds.builder_json, never in later create/resume runtime config.

Create and Register share the typed parser for resource/network/traffic/launch/init/mounts/files/metadata, `envVars` and instance options; namespace fields and Header/metadata merge rules remain in [Node §4.6](node.md#46-sandbox-configuration-propagation). Register always rejects `kuasar-sandbox.identity`, tenant-supplied node-managed cluster metadata, `restore` and `autoPauseMemory`. Build runtime inputs use `builds.metadata_json`; build-only input uses `builds.builder_json`. They serve this execution and artifact generation, not long-term canonical TemplateID lookup. Trigger cannot replace registered metadata, Builder/Resource or other general configuration headers; nonempty general metadata/headers are rejected.

- At synchronous admission, a known Image target rejects any remaining normalized Sandbox resource/traffic/launch/init/mounts/files/metadata/checkpoint/MMDS namespace, nonempty `envVars`, `secure=true`, nonzero credential overrides or explicit MMDS routes/secrets. Ordinary metadata labels and Build execution resources remain allowed; empty `envVars` or `secure=false` alone is not a rejection condition.
- Every target accepts `X-Kuasar-Sandbox-Network` / `kuasar-sandbox.network` for the Build execution network, including explicit or automatically resolved Image targets and an empty network object. Network input does not require a Sandbox output; A/B use it for image import and build steps, and Sandbox targets also project it into E.
- At synchronous admission, explicit top-level Sandbox E accepts portable Create configuration, including env, but rejects instance/action-only traffic, nonzero credentials, explicit MMDS routes/secrets, `secure=true` and nonempty checkpoint input.
- Only resolved `sandbox,memory:true` applies that latter category. `instance_config_enc` encrypts env/secure/credentials; traffic/checkpoint/MMDS routes remain metadata, and MMDS values use a separate encrypted table. Instance/action fields do not enter portable E/S; portable env retains its own projection rules. Empty objects are not a blanket exemption: `resource:{}` and `traffic:{}` still reject for Image, while empty checkpoint and zero credentials follow their namespace normalization.
- Validate target compatibility as early as it is knowable: explicit targets and bare auto at Register; e2b auto at Trigger after hooks and source alias resolution, if trigger commands or an Image source determine the target. Return HTTP 400 with the conflicting option; a rejected Trigger stays registered and can be corrected without entering the queue.
- If e2b auto still depends on task-local source E command defaults, accept the request. After acceptance, ignore configuration unsupported by the resolved target in preparation, worker publication, result validation and direct IMG reuse. Keep the registered definition unchanged; do not upgrade the target or start Phase C to accommodate options. Target, effective-command and exactly-one-artifact integrity checks still apply. To preserve env, select `sandbox,memory:false`; instance-only inputs require `sandbox,memory:true`.


Build execution resources and final Sandbox resources are independent, without mutual defaults, comparisons or derivation; Trigger `cpuCount`/`memoryMB` only assert immutable Build resources.

Build Register routes serve only its synthetic builder sandbox that might run memory Phase C. Explicit Image/top-level E rejects them during registration; auto is rejected synchronously when known incompatible, otherwise the accepted task omits them if source resolution selects Image. Initial values are encrypted under Build ownership. Terminal transaction removes both routes namespace from builds.metadata_json and the value blob; neither enters final template/snapshot/image. Trigger cannot override MMDS.

Build applies `kuasar-sandbox.checkpoint` policy only for resolved sandbox,memory:true and uses it for Phase C capture, never portable config. Known incompatible requests fail synchronously; an accepted source-dependent Image never resolves checkpoint policy or target Sandbox resources.

Build temporary VMs and final template derive from one NetworkSpec: absent hostname uses build-short-ID temporarily and node sandbox hostname in the template, excluding the temporary name. Phase C stores final effective network metadata in its E so Create/restore remains self-describing without Build history. Precedence is current request, then artifact, then node/profile defaults. Image outputs have no Sandbox-metadata channel and cannot recover it from old retained Build rows; SBX/SNP use their E.

## 4. Task handoff

Use the authenticated [node config socket and run plane](node.md#6-local-control-socket-run-task-admin-plugin-and-api-planes). Peer PID, exact RunID and locked task pidfile are verified before secret-bearing providers. The shared plane owns assignment and phase/result report envelopes; the Build-specific payload follows.

- **run-builder:** lock `<BuildRunDir>/builder.pid` after assignment and request exact-BuildID/RunID bootstrap. fromImage and IMG fromTemplate return final BuildSpec in that RPC. SBX/SNP fromTemplate installs authoritative `MANIFEST_KEY` and performs cold/E-selection preparation in the task. SNP uses S only to locate E. Complete E/source-image config and filtered sorted ref-locations remain in the task; only a bounded non-secret summary goes to conductor before final BuildSpec. Readers/fetchers are explicitly closed before submission; conductor reads no tenant artifacts. run-builder merges retained local results into final spec and **remains resident** to drive the target-aware pipeline of at most three phases (§5). Each `sandbox-ctl run` is its direct child; the entire Build is charged to the unit cgroup. It returns `{target, exactly-one-of image_ref|sandbox_ref|snapshot_ref, start_cmd, ready_cmd, error, failure_stage}` over config-socket. Root-config reads, preparation RPCs, phase reports and pipeline share one absolute deadline, reserving at most the final five seconds for durable result reporting. With less than ten seconds left, half the remaining budget is reserved, preventing a short timeout from expiring before work starts without resetting/extending the total budget. Only the final result POST may use that tail window. BuildID remains durable business identity and the verbatim directory leaf; the 48-byte cap keeps the deepest phase UDS within supported RunRoot/Linux `sun_path` limits.

- POST /internal/task/build/bootstrap takes build_id/run_id/version. **BuildTask schema is v6**: v6 requires source command presence for target-specific preparation; v5 separates checkpoint-location policy from image-class Bundle publication and removes broad publish_location_parent; v4 added immutable requested target and cold-E source normalization; v3 separated run_dir/base_dir; checkpoint_mode is required since v2. The BuildTask v6 gate also covers the optional Build-only command-presence summary/digest field; ordinary Sandbox summaries and digests are unchanged. **ArtifactPrepare remains independently v4**: v4 adds Build-only image Bundle publication preflight; v3 added Build-only source-image config reads, same-Bundle E/image scope and portable allocatable/deflate defaults; v2 replaced Snapshot-only v1 with typed E/S, durable LaunchMode and bounded network/disk summaries. Version mismatches fail before secret-bearing providers rather than silently dropping required publication semantics. See [build_task.go](../internal/configsock/build_task.go) and [sandbox_task.go](../internal/configsock/sandbox_task.go).

  Authenticated Build bootstrap returns task environment and exactly one Final/Prepare. Final BuildSpec includes build_id/profile/run_dir/base_dir, from_image or from_template plus kind, requested_target, checkpoint_mode, steps/start_cmd/ready_cmd, paths/net, Build resources, final Sandbox resources/spec/namespaces/environment, mmds_enabled/envd_token, insecure/platform and timeouts. Task environment contains MANIFEST_KEY plus tenant FLATTEN_* pull credentials. Artifact Prepare includes source kind/ref, LaunchMode, manifest config, ref-location parent, ref limits and absolute deadline from execution claim. Task reads root config and POSTs build_id/run_id/version/summary to /internal/task/build/prepare. Equal digest returns the same final result; conflict returns 409; waiter cancellation does not revoke accepted summary.

  Paths contain host kernel/runtime, two diff templates, sandbox-ctl/flatten-ctl/manifest-ctl/manifest config. Net contains the conductor-attached slot: TAPFD transport, MAC, inner IP, nexthop, hostname and DNS, reused throughout sequential phases. SBX reads E directly. SNP reads S's sandbox_ref to locate E and excludes S memory/from-refs. Only a bounded summary reaches conductor; full E and selected ref stay in run-builder, which must materialize Phase B. Final work is available only while that assigned Build unit is alive; its exit invalidates pending ownership.

### 4.1 Builder unit and process enforcement

Unit installation, shared pool assignment and cgroup infrastructure remain in [Node §5](node.md#5-process-management-through-systemd-template-units); §5.2 in the unit logging comment refers to [Node journald](node.md#52-journald-and-log-labels).

**Builder unit** (`%i` is RunID, §5):

```ini
# sandbox-builder@.service (Generated)
[Unit]
Description=kuasar image build runner %i
CollectMode=inactive-or-failed

[Service]
Type=exec
WorkingDirectory=/run/sandbox
StandardError=journal
# Disable unit rate limiting for Build detail; journal failures can still lose logs (§5.2)
LogRateLimitIntervalSec=0
ExecStart=<node-ctl> run-builder --pidfile=/run/sandbox/runners/%i.pid \
          --config-socket=/run/sandbox/node-ctl.socket --run-id=%i
ExecStopPost=/bin/rm -f /run/sandbox/runners/%i.pid
KillMode=control-group
# Defensive aggregate execution CPU/memory limits
Slice=sandbox-builder.slice
# Phase ctl/vmm subgroups and trusted VMM cgroup FD
Delegate=yes
```

Builder locks `<BuildRunDir>/builder.pid`, obtains exact-run bootstrap and, for artifacts, prepares the root before final BuildSpec; images receive final spec directly. It remains resident for the target-aware pipeline of up to three phases. Phase sandbox-ctl/CH processes are its descendants, charged to the entire builder unit. Results return through config-socket. KillMode=control-group makes StopUnit/timeouts cover phase VMs as well; normal systemd termination starts with SIGTERM and uses SIGKILL after the stop timeout if processes remain.

Aggregate Builder CPU/memory limits require builder_pool_size=0. On-demand units still enter WaitAssignment but already have a durable execution claim. Before publishing assignment, orchestrator sets and reads back unit properties. This prevents unclaimed idle CPU/RSS from occupying the slice's active-Build ceiling without hidden idle budgets.

<a id="12-模板构建target-aware最多三阶段的流水线构建在沙箱内进行"></a>
<a id="12-template-builds-target-aware-up-to-three-phases-inside-sandboxes"></a>
## 5. Target-aware execution and publication

Builds arrive through e2b APIs ([§1](#1-build-api); there is no separate build-submission CLI), enter the builds table and are scheduled by resource pools. Each execution binds one sandbox-builder@<run-id> unit. Image pulls and build steps execute inside microVMs. Host run-builder relays artifact streams, fetches/decompresses COPY contexts and publishes outputs; therefore the boundary is guest execution of tenant build commands, not absence of tenant bytes from host userspace.

Register-time builder.target names three outputs: `{kind:"image"}`, `{kind:"sandbox",memory:false}` and `{kind:"sandbox",memory:true}`. The first two skip C; top-level Sandbox E is assembled from final image plus ordinary Create config without a VM. Only memory Sandbox cold-starts C and captures a Snapshot. With target omitted, run-builder reads source E's default commands task-locally, then selects memory Sandbox if either effective start/ready command is nonempty, otherwise Image. Source kind, steps, profile and other Sandbox config do not infer target.

Every source first becomes an image or cold Sandbox E, then A/B yields this Build's top-level image. Builds never restore source S memory/VMM state: S only locates E. Final Image/E/S must not reference source S/E, writable/data disks or memory parents. A memory result permits only the S→E relation created by this Build itself.

**Conductor, once per Build:** registered/waiting creates no object directories. A durable execution claim precedes BuildRunDir=`<RunRoot>/builds/<BuildID>` and BuildBaseDir=`<BaseRoot>/builds/<BuildID>`, including checkpoint. Parse request-only spec/resource policy, allocate from builder pool, set/read back systemd limits, then durably bind exact run ID, starting a unit on demand if no idle one exists. Run-builder retrieves bootstrap. For SBX/SNP fromTemplate, task reads root config under cold/E-selection semantics and submits a summary; image fast path needs no second RPC. Strictly parse inherited network and merge current request; allocate one slot through TAPFD/1 PREPARE when tapfd_socket is configured, otherwise connector-ctl vswitch attach. All phases sequentially reuse that slot/tapfd. Mint envd token, then atomically write port/token and nonsecret runtime_prepare_json in one exact-run SQLite CAS. Runtime preparation schema v4 also freezes source command presence for equivalent recovery; only an actual Sandbox target resolves Sandbox resources, and only memory=true resolves checkpoint/instance policy. Preparation records digest, resolved build/template network and independent A/B execution versus target Sandbox resources. For e2b+MMDS where the resolved target is memory Sandbox, publish a synthetic Sandbox route before handing off final BuildSpec: C may start immediately and needs floating-IP self-resolution. This route projects registration MMDS routes and initial build-owner values to the builder guest. Run-builder executes the pipeline and reports `{target, exactly-one-of image_ref|sandbox_ref|snapshot_ref, start_cmd,ready_cmd,error,failure_stage}` through config-socket. While retaining execution claim, conductor first durably stores and fail-closed validates the result against requested/auto target. Terminal derived kind is img/sbx/snp; names/aliases store `<profile>-<kind>-<base64url(portable-ref)>`. Nonterminal Build.Kind stays empty and is never a second target authority. Profile is explicit throughout registration→BuildSpec; checkpoint.mode travels as checkpoint_mode to C. Register/trigger validate request network metadata before enqueueing; strict inherited artifact metadata validation follows task summary and can fail asynchronously during preparation. Resolve network once, supplementing profile/node defaults: the same NetworkSpec derives host AttachReq and guest BuildNet. Transit fields affect host Attach only and remain absent from BuildNet; without transit they stay zero.

Node config first validates publication matrix, especially that checkpoint.remote.manifest=true requires an absolute ref_location_parent. Before scanning any source image, writing large directory files or starting a VM, run-builder loads manifest_config, pins the task customer key and obtains/verifies image Bundle write admission. SBX/SNP two-stage bootstrap performs the same preflight before task-local E/S readers open the root carrier. Failure yields Build error without starting a phase or returning an artifact ref.

Synthetic MMDS routes publish only after durable real builder RunID, which also fences MMDSv2 token incarnation. Pipeline completion first withdraws the route view. Every ready/error/cleanup terminal update atomically removes MMDS route namespace and encrypted values with the Build row update. Registration MMDS input is request-scoped: it enters neither final template metadata, snapshot.cfg, images nor later Sandboxes; Trigger cannot rewrite it.

**Inside run-builder ([§2.4](node.md#24-node-ctl-run-sandbox--run-builder)):** BuildSpec ([§4](#4-task-handoff)) drives at most three phases, each one microVM launched as a direct sandbox-ctl child. Per-phase anonymous pipes pass `--ready-fd=<actual child fd>` through ExtraFiles. Parent strictly waits for `control_ready\nready\nEOF` and closes its writer immediately after successful Start; early child exit becomes EOF. A waits only for this runtime wire with a 60-second deadline, without guest-exec polling. B/C additionally wait for envd /health under the same per-boot 90-second deadline.

Each phase keeps its globally unique logical SandboxID but has PathID a, b or c. Run-builder invokes `sandbox-ctl run --run-root <BuildRunDir> --base-root <BuildBaseDir> --path-id <a|b|c> --sandbox-id <logical-phase-sid>`. Phase YAML, envd/ctl sockets and small JSON live in BuildRunDir/<phase>; writable diffs live in BuildBaseDir/<phase> with logical SandboxID retained in filenames. Phase exec/snapshot locates ctl.sock only by PathID. A sandboxer exit removes only its own RunDir, preserving BuildRunDir and siblings. Final node-local Build cleanup removes both complete Build directories.

- **A import (fromImage):** an empty single-disk Sandbox uses writable ext4 copied from builder.diff_template as root, with no base image and launch.placeholder as anchor. The shared guest runtime supplies flatten-ctl/mkfs.erofs under /opt/sandbox-runtime. With builder.referer.enabled, guest first runs `flatten-ctl referer lookup --json --owner <owner> <fromImage>` using tenant registry credentials. Lookup accepts only valid, unexpired referrers. On a hit, host validates the returned manifest ID and directly selects manifest://<id>, skipping pull/flatten. Unsupported/error follows fallback policy. On miss, guest `flatten-ctl export --output - <subject digest>` pulls/flattens the exact immutable identity shared by lookup, export and writeback; exec stdio relays the tarstream artifact to BuildBaseDir/checkpoint/image.img. If lookup confirmed Referrers support and writeback=true, host publishes via sandboxer and guest runs `flatten-ctl referer put --owner <owner> --manifest-id <id> <subject>` only when final image policy already requires that IMG in Manifest Store. Skip writeback when top-level E is the sole image-class root or remote.manifest=true; never create an intermediate IMG Manifest solely for a referrer. MANIFEST_KEY remains host-side for owner tokens/publication and never enters guest referer commands.
- **B build/materialize (steps or source E):** image sources use their local/Manifest base image, builder runtime and a large writable upper from the same diff_template. SBX/SNP always cold-run `sandbox-ctl run --from <E>` with E's complete root graph and a fresh builder upper. Clear source mounts/files/init/workload during B, preventing materialization from executing actions that would run again during final Create; separately inherit non-boot config through shared cold projection. Envd is the app in tool posture: always -isnotfc, no /init/token, with its only /run/e2b disk footprint on excluded tmpfs. Execute RUN steps sequentially through envd process.Start using `/bin/bash -l -c <cmd>`, operation-user Basic authorization and Connect-Timeout-Ms for each step budget; guest also kills on expiry. Host accumulates ENV/WORKDIR/USER starting from base image config, so RUN sees Docker-style context. ARG only substitutes `${k}` and never enters the image. Bash is thus an image contract for steps/startCmd, as in e2b. After steps, host overlays accumulated context on base config and writes it into guest. `flatten-ctl mountpoint /.kuasar-build` creates a self-bind mount, then `export --skip-mounts --runtime-config … --tmpdir /.kuasar-build --output - /` streams a new image back. The export mount and /opt/sandbox-runtime are excluded mounts, avoiding self-inclusion/toolchain leakage. E always runs B even without steps, producing a complete top-level image independent of source E/root layers.
- **C memory snapshot (only resolved sandbox,memory=true):** use the same cold-config projection as ordinary Create/top-level E and fully replace boot with the final image. Image sources cold-run normally. With E defaults, use `sandbox-ctl run --from <E> --replace-boot --config <C0>`, never --restore S. Production e2b envd posture follows deployment ([Proxy §7](node-proxy.md#7-mmds)); /init supplies registration envVars and optional instance credentials, with subsequent RPCs carrying X-Access-Token. Optional startCmd runs through envd as default user in /home/user. Retain its stream until ready, then disconnect; envd does not kill on stream loss, so the process freezes into the snapshot as envd-managed. Poll readyCmd every two seconds within ready_timeout_sec. Without readyCmd, wait a fixed 20 seconds regardless of startCmd, subject to absolute Build deadline/cancellation. Omitting startCmd is also valid in C. Close its stream first, failing if the command already failed, then run `sandbox-ctl snapshot --path-id c --output <BuildBaseDir>/checkpoint` for the local snapshot carrier.

**Two guest channels:** e2b semantic commands—steps/startCmd/readyCmd—use envd, matching its build operations. Platform mechanisms—flatten pull/export, runtime-config injection and artifact relay—use sandbox-ctl exec with raw stdio, available on arbitrary rootfs without image userland dependencies.

**FromTemplate:** sources are canonical artifacts. IMG is already an image carrier and can be reused without steps; steps require B. SBX opens E task-locally. SNP opens only S root config to find sandbox_ref, then follows the identical E path. No phase receives S, reads/prefetches memory payload, includes memory from-refs in closure or runs --restore. Complete PortableSandboxConfig remains inside run-builder for B materialization and Sandbox target's non-boot defaults. Any source boot.disks entry is explicitly unsupported in this version.

FromTemplate and fromImage are mutually exclusive. All E/S sources run B even without steps, so only an IMG without steps permits zero-VM reuse. E metadata's e2b.start_cmd/e2b.ready_cmd supplies e2b-profile defaults, overridden by nonempty Trigger commands. Explicit Image clears inherited commands without execution, and explicit Image plus Trigger commands is synchronously rejected. Strict network inheritance before host Attach is current explicit Build NetworkSpec > source E NetworkSpec > current profile/node defaults. IMG has no artifact-metadata channel and never recovers defaults from retention-bounded Build rows.

Task-local ref-location mapping retains only selected E and root refs required for B cold launch, filtering S memory-only Bundle locations and source S. B exports a complete top-level image; E assembly or C --replace-boot then installs only that image, removing source E/root/writable refs from the final graph. If C's final image publishes as a located Bundle, pass its actual publication name→directory mapping in C argv and retain all mappings still referenced by checkpoint graph during publication.

**COPY/ADD (context uploaded directly to object storage):** COPY extracts a tar into rootfs, a filesystem mechanism rather than an e2b process operation. It therefore uses sandbox-ctl exec + flatten-ctl, without envd or image-provided tar/gzip. Flatten's pure-Go extraction supports scratch/distroless. Three stages:

1. **Upload negotiation ([§1](#1-build-api) files endpoint):** client GETs /templates/{tid}/files/{hash}. After ownership validation, server returns {present,url}; if absent, client directly PUTs gzip(tar) to the presigned bucket URL, with no upload bytes through control. Object key is `{prefix}/files/{aaaa}/{bb}/{uuid}/{hash}`, where uuid is tid's UUIDv7 part, aaaa=uuid[0:4] spans about 50 days and bb=uuid[4:6] about five hours. Time-bucketed fan-out supports date-based GC. Endpoint ownership limits signed URLs to the caller's tid path; bucket stays private and clients receive no storage credentials.
2. **Trigger validation:** HeadObject checks each COPY (tid,hash). Missing files_storage returns 501; missing upload returns 400 before execution.
3. **Build extraction:** BuildSpecFor presigns each GET for total+5m. Host run-builder fetches and gunzips the context, then calls `sandbox-ctl exec --stdin-from <tar> -- flatten-ctl tar extract --dense [--chown O][--chmod M] <rule>`. Derive rules from src/dst and context entries: whole-root `:dst/`, directory-prefix `src/:dst/`, single-file `src:out`. Relative dst resolves under accumulated WORKDIR, default /. Default owner is Docker-style 0:0, overridden by --chown; names resolve using extraction-root /etc/passwd. Dense preserves the sparse rule: without authoritative hole metadata, zeros are data. COPY accepts one source, matching e2b executor's limit.

**Object storage (builder.files_storage, [§3](#3-build-configuration)):** S3/OBS. Conductor only presigns and HEADs, the AWS SDK integration point. Local deployments can use versitygw built with `make -C guest-runtime/native-deps versitygw`. Force_path_style defaults false for virtual-host addressing; use true for versitygw/minio.

**Publication and target matrix:**

Create the publication plan once after resolving target; never infer it from a worker result field. Image class comprises final IMG, top-level disk-only E and C's immutable IMG. Checkpoint class comprises C's incremental E, S and memory/disk incremental layers. Both E forms are standard Sandbox E, but only top-level E contains complete EROFS payload and uses image policy.

| Configuration | Image target | Sandbox, memory=false | Sandbox, memory=true |
|---|---|---|---|
| Empty parent, manifest=false | IMG → Manifest Store | Top-level E → Manifest Store | IMG, EΔ, S → Manifest Store |
| Nonempty parent, manifest=false | IMG → Manifest Store | Top-level E → Manifest Store | IMG → Manifest Store; EΔ/S → named location |
| Nonempty parent, manifest=true | IMG Bundle → named location | Top-level E Bundle → named location | IMG Bundle → named location; EΔ/S → named location |
| Empty parent, manifest=true | Invalid configuration | Invalid configuration | Invalid configuration |

Checkpoint.remote.manifest=true means directly materializing manifest-backed image-class logical artifacts as single-root Manifest Bundles in the named location, rather than uploading them to Manifest Store. Checkpoint.mode=local|bundle chooses role-specific tarstream or Snapshot Bundle for checkpoint class only; image class always uses Bundle in this named mode. With manifest=false, ref_location_parent does not change Image/top-level-E's Manifest Store destination.

- **Image:** send current logical source directly to sandboxer's typed publisher. Manifest=false returns manifest://IMG and may preserve the identity fast path for an existing IMG without steps. Manifest=true always uses Bundle publisher, including existing manifest/located Bundle sources, with common validation/content-address reuse, returning only located image_ref.
- **Sandbox, memory=false:** directly open final image—digest-qualified `file://<BuildBaseDir>/checkpoint/image.img@digest:...`, manifest:// or located image Bundle—materialize image defaults/canonical portable runtime config and assemble a standard direct-EROFS self-layout Sandbox E logical source. Publish it directly through Manifest/Bundle API, without separate IMG publication, manifest-ctl store, fetching a local IMG back through Manifest or staging a complete .sandbox under RunDir/BaseDir. Skip C and start/ready; return only sandbox_ref.
- **Sandbox, memory=true:** publish final IMG under image policy and use its portable ref as C replacement boot. Pass actual located IMG mapping into --from E --replace-boot. After C cold run, capture EΔ/S and publish under checkpoint policy, returning only snapshot_ref. EΔ retains that IMG ref. Neither local tarstream nor Snapshot Bundle rematerializes/copies portable image Bundle. There is no source-memory restoration or disk-only C branch.

Each named publication derives its name from the bare BuildID, so every publication of one build converges on one directory; each ref carries its own name. Single-root image Bundles create no .image/.sandbox tarstream or semantic BuildID/SandboxID alias; they commit only `<manifest-root>.bundle`. Publication uses target-directory temporary files, root-last finalization, complete validation, exclusive final creation, checked full writes/Close and reopening/validating the final path. The publisher intentionally does not fsync the fresh final or parent directory: success is logical publication, without a power-loss durability guarantee ([implementation](https://github.com/kuasar-sandbox/sandboxer/blob/main/pkg/artifact/location_target.go)). Concurrent/retried publication reuses only strictly verified same-key regular finals; corrupt, symlinked or mismatched finals fail closed. Failure returns no terminal ref; existing Build finalizer still removes complete RunDir/BaseDir.

Top-level E and C's initial C0 use one BuildColdConfig projection: source E non-boot defaults < registration Create options < builder-managed start/ready. Resources/network/boot/launch/env/mounts/files/init/metadata have identical semantics. Effective start/ready and NetworkSpec enter E metadata, preserving a self-describing canonical TemplateID after Build-row TTL. Omitted hostname records the ordinary Sandbox default, never build-<id>.

**Build logs (journald → status API → SDK on_build_logs):** SDK reads the `build` stream on demand, excluding `console` and `sandbox-ctl` component diagnostics. Complete phase targets carry `KUASAR_BUILD_ID`/`KUASAR_RUN_ID`; target construction and independent output identities are owned by [Build journal identities](node-journald.md#build-output). Run-builder writes milestones—import: pulling…, step N: RUN…, template: ready, uploading…—through pure-Go `go-systemd/journal` and replays retained RUN/startCmd envd streams into that sink, since envd itself has no journal. Phase app stdout/stderr and the four journal-routed flatten/export/referrer exec call sites use complete `build` targets. Flatten omits `--no-progress`, retaining layer counts and flatten progress. File/artifact destinations and stderr error-tail capture remain independent. Pipeline failure appends `build failed: <err>`. There are no temporary log files; sending/storage remain best-effort and disabling unit rate limiting does not guarantee delivery. Status paginates by ?logsOffset count ([§1](#1-build-api)): conductor runs `journalctl KUASAR_BUILD_ID=<bid> SYSLOG_IDENTIFIER=build --output=json`, reads MESSAGE/PRIORITY/timestamp, maps ≤3 to error, 4 warn, ≥7 debug and others info, and returns the [offset:] slice as logs[] and logEntries[{timestamp,level,message}]. Reading is best-effort: non-systemd/missing logs yields an empty list without blocking status. Journalctl avoids CGO-dependent sdjournal; writes use pure Go. Pipeline failure returns generic reason.message `build failed; see build logs`, with detail in the stream. Infrastructure failure before pipeline/result has no pipeline logs and reports the host error directly. Disabling unit rate limiting does not guarantee log delivery across every journal failure.

**FromImage selection:** Trigger can explicitly supply it. In the e2b CLI workflow that locally docker-builds/pushes and omits a trigger image, builder.image_uri_mask replaces `{templateID}` and `{buildID}` in the complete configured string, for example `registry/repo/{templateID}:{buildID}`, without appending a path and must match CLI E2B_IMAGE_URI_MASK. Guest must reach this registry: local registries bind a non-loopback address reachable through vswitch management VIP; external registries use NAT. Missing both sources rejects Trigger. For local/private registries, configure builder.insecure_registry and builder.platform.

**Registry TLS (HTTPS with internal/self-signed CA):** --insecure selects an HTTP-capable URL scheme; it does not disable TLS certificate validation. HTTPS internal CAs use flatten TLS configuration. This is per-Build trust policy supplied at registration through X-Kuasar-Sandbox-Builder, builder.registry.tls, outside Node configuration:

- Either ca_bundle_pem, at most 16 KiB of inline PEM containing parseable X.509 CERTIFICATE blocks, or insecure_skip_verify=true; mutually exclusive, with empty tls rejected.
- Register-only; Trigger with builder.registry returns 400.
- Only fromImage; fromTemplate plus registry.tls is rejected.
- Mutually exclusive with node insecure_registry's plain HTTP for the same Build.
- Builder projects PEM and generated flatten YAML into Phase A read-only files /run/kuasar-build/flatten/registry-ca.pem and config.yaml, mode 0444+read_only on /run tmpfs, outside the Build root disk. Import export/referer lookup/referer put all receive --config. Flatten appends the CA to system roots rather than replacing them. This policy never enters final metadata or other Builds.

**Two-level admission/enforcement:** in one SQLite transaction, Register checks count/CPU/memory/storage against builder.admission.registration and inserts/charges the immutable definition. Trigger only moves registered→waiting. Scheduler uses stable `waiting_sequence` FIFO (durable successful Trigger commit order) and transactionally claims builder.admission.execution. Insufficient capacity stays waiting until queue_ttl makes it durably terminal. After claiming, set/read back CPUQuota/MemoryMax on the exact builder unit, bind run ID, then publish assignment. Execution CPU/memory also limits sandbox-builder.slice; storage is admission-only in V1. Builder status/metrics expose configured/durable usage, headroom, queues and rejection/expiry counters. Legacy max_concurrent/cpu_quota/memory_max/vcpu/memory settings are rejected.

Each actual A/B/C phase has its own SID and uses ordinary sandbox-ctl controller Admit/heartbeat/Release, fully tearing down/releasing before the next phase. A/B resources derive only from immutable Build.Resources. Registration Create resources instead use E portable capacity defaults where present for final E/C0; Image has exact-zero target resources and need not resolve them. Neither derives from the other. Build.Resources itself never enters nodectl, so each active phase has one ordinary Sandbox reservation without double accounting. Disk-only E has no C reservation.

Ready/error are retained Build history. Terminal transition atomically records finished_unix and releases registration usage. An executed Build first completes unit/cgroup/network/runtime/result/directory cleanup, then terminal commit releases its execution claim. Only fully cleaned, unclaimed rows can be reaped after terminal_ttl. Status, transient registration TemplateID, names/aliases and local listing disappear with the row; previously returned canonical TemplateIDs still work for Create/fromTemplate.

**Image-pull credentials, in priority order, otherwise anonymous:**

1. Task pull token: SDK api_headers X-Kuasar-Pull-Token contains kpt_, sealed by e2b-key-ctl seal-pull-token using a tenant-manifest-key derivative; conductor unseals with the installed tenant key.
2. SDK plaintext: Trigger fromImageRegistry{username,password}.
3. Tenant defaults: manifest_keys.registry_auth_enc stores docker config.json bound to the complete pair. Manifest-key add --registry-auth or username/password/token flags can assemble a catch-all `*`; resolve by image host, then `*`.

Encrypt selected credentials into builds.registry_auth_enc. During Build, decrypt into FLATTEN_REGISTRY_{USERNAME,PASSWORD|TOKEN}; flatten pkg/remote prefers token. Exec environment carries only FLATTEN_* into import guest, never MANIFEST_KEY. Tenant roots/pull credentials remain encrypted at rest and in runtime environments, absent from generated phase YAML; caller-supplied workload env/files can themselves contain sensitive values and are a separate input category.

**Unsupported:** multi-source COPY (e2b executor likewise uses one src/dst), step caching (force is accepted but ignored; steps execute fully) and server-side Dockerfile parsing (CLI expands it into steps).

## 6. Persistence, recovery and retention

Build uses the same node SQLite database; its complete dedicated tables are:

```text
builds         build_id PK, template_id(transient-…), persist_id(<profile>-<kind>-<base64url-ref>),
               api_secret_hash, api_secret_enc, manifest_key_hash, manifest_key_enc,
               profile, kind, from_image, from_template, start_cmd, ready_cmd, steps_json,
               status(registered|waiting|building|ready|error), reason, run_id,
               names_json, aliases_json, registry_auth_enc,
               registration_image_repo, registration_registry_auth_enc,
               registration_mmds_routes_digest, registration_mmds_values_digest,
               registration_request_digest, cluster_group,
               resources_cpu(milli-CPU), resources_memory(bytes), resources_storage(bytes),
               metadata_json, builder_json, instance_config_enc,
               waiting_unix, waiting_sequence, execution_claimed, execution_claimed_unix,
               enforcement_status, phase, phase_sandbox_id,
               runtime_vswitch_port, runtime_floating_ip, runtime_port_mac,
               runtime_envd_access_token_enc, runtime_prepare_json,
               execution_result_json, created_unix, finished_unix
build_mmds_route_secret_values
               build_id PK/FK builds(build_id) ON DELETE CASCADE,
               routes_digest, revision, ciphertext, updated_unix
```

Builds is durable authority for registration/execution admission and execution state, plus status/index/alias within retention; it is not a permanent template catalog (§2).

Immutable resources_* belongs to Build execution/admission; final Sandbox resources belong to Create config in metadata_json and neither derives from the other. Instance_config_enc encrypts registration envVars (portable launch env for Sandbox targets) together with secure/credentials input used only by memory C. Readers still separate these categories before target validation/portable projection. There is no legacy Build-row dual reader. Startup uses instance_config_enc as the #300 schema-break marker, explicitly rejecting incompatible development databases before queries and requiring rebuild; it never infers/backfills plaintext. Execution_claimed and runtime/phase fields reconstruct usage and fence live units before releasing claims. Additive migration adds runtime_prepare_json; exact-run CAS commits it with the runtime port and terminal/cleanup clears both.

Startup first idempotently initializes the schema, including MMDS value tables, then checks the #300 `instance_config_enc` clean-break marker. An incompatible development database missing it is rejected before Build-row queries and subsequent additive column migrations, with no plaintext backfill or dual reader. Supported databases independently gain Build `runtime_prepare_json`, `registration_request_digest`, `finished_unix` and Sandbox `dead_unix`. Existing terminal records whose timestamp is zero receive a complete retention window starting at migration. This does not require rebuilding every database on startup. Each MMDS owner has at most one secretbox ciphertext row and shares the [Node credential model](node.md)'s AAD/transaction/CAS/cleanup contract.

Builder reconciles in the same startup gate. All live ownership is reconstructed before task bootstrap/results pass buildRecoveryReady:

- Bound run_id with no port is valid preparing. Adopt the same live run-builder and restore completion ownership. Artifact tasks may retrieve bootstrap or resubmit the same summary; conductor never reads artifacts.
- Port plus valid runtime_prepare_json is prepared/pipeline-running. Restore final BuildSpec from frozen network/resources. Same-digest retries neither reattach nor inherit changed node defaults.
- Port with absent, corrupt or unknown-schema preparation requires exact-unit fencing, detach and terminal failure; never invent potentially mismatched configuration.
- A durably accepted execution_result_json takes precedence over preparation recovery: fence worker and finalize that result without rereading snapshots. Task/host preparation, pipeline and result reporting share the original execution_claimed_unix+total_timeout deadline. The following 60 seconds is only for fencing/host cleanup; restart extends neither budget.

Build rows store no directory paths. Derive both Build directories from BuildID and current RunRoot/BaseRoot. Every terminal path first fences exact unit/cgroup, detaches, clears runtime ownership and removes both directories; terminal commit atomically clears RunID/result and releases execution claim. Phase child cleanup is not final correctness authority. Node database, config socket and runner pidfile are outside object directories and unaffected by object RemoveAll.

Shared SQLite infrastructure and the terminal reaper remain in [node reliability](node.md#15-reliability); this section owns the Build schema and recovery. Build rows are retention-bounded execution/status/alias records, not a permanent template catalog. Published canonical template references remain usable independently of their Build row.
