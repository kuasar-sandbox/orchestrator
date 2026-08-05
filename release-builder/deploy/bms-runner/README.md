# BMS runner slots

This directory provisions six privileged `systemd-nspawn` system containers on
the openEuler 24.03 BMS host. Each container owns its systemd, journald, PID,
mount, network, cgroup, Docker daemon, runner credentials, and Actions work
directory. The containers share only:

- `/var/cache/kuasar`, for exact-SHA source archives and immutable native
  artifacts protected by repository-owned locks;
- `/var/lib/kuasar-ci/tools`, read-only test tool binaries;
- `/usr/local/go` and the host kernel module tree, read-only.

Each slot receives a different bpffs subtree at `/sys/fs/bpf`; pinned BPF paths
cannot collide across concurrent jobs.

The host must preload the pinned x86_64 E2E tools instead of downloading their
large release artifacts during a job:

| Path | SHA-256 |
| --- | --- |
| `/var/lib/kuasar-ci/tools/zot` | `523e5bf29a013db09115f780c3152af98fc5b65fc408a0d3e6c293643dc9bde7` |
| `/var/lib/kuasar-ci/tools/versitygw` | `e839f0ce24a51dbf0a7a925e08a28a0bfa190d05290c13f2c4536852bc5f3a7d` |

Transfer these verified files from the operator host before running `check`.
The provisioner rejects missing or mismatched tools and mounts the directory
read-only into all slots.

The shared Go toolchain is pinned to `go1.26.5` for `linux/amd64`. If the
validated `/usr/local/go` toolchain is absent, installation fetches
`go1.26.5.linux-amd64.tar.gz` only from the direct Aliyun China mirror
`https://mirrors.aliyun.com/golang/` and verifies the Go release SHA-256
`5c2c3b16caefa1d968a94c1daca04a7ca301a496d9b086e17ad77bb81393f053`
before replacing the toolchain. Jobs never download a Go distribution.

The containers are privileged resource-name isolation, not a security boundary
for untrusted jobs. They deliberately receive KVM, TUN, vhost devices, all
capabilities, Docker keyring syscalls, and the `bpf` syscall required by the
connector datapath.

## Host layout

| Slot | Runner name | CPU/NUMA | Memory | Address |
| --- | --- | --- | --- | --- |
| 1 | `bms-tmp-kuasar-e2e-1` | NUMA0: `0-6,44-50` | high 52 GiB, max 56 GiB | `10.203.0.11/24` |
| 2 | `bms-tmp-kuasar-e2e-2` | NUMA0: `7-13,51-57` | high 52 GiB, max 56 GiB | `10.203.0.12/24` |
| 3 | `bms-tmp-kuasar-e2e-3` | NUMA0: `14-20,58-64` | high 52 GiB, max 56 GiB | `10.203.0.13/24` |
| 4 | `bms-tmp-kuasar-e2e-4` | NUMA1: `22-28,66-72` | high 52 GiB, max 56 GiB | `10.203.0.14/24` |
| 5 | `bms-tmp-kuasar-e2e-5` | NUMA1: `29-35,73-79` | high 52 GiB, max 56 GiB | `10.203.0.15/24` |
| 6 | `bms-tmp-kuasar-e2e-6` | NUMA1: `36-42,80-86` | high 52 GiB, max 56 GiB | `10.203.0.16/24` |

The layout reserves one physical core per NUMA node (`21,65` and `43,87`) and
about 39 GiB of host memory when every slot reaches `MemoryMax`. It targets
functional E2E concurrency; performance or density baselines that require the
former 176 GiB-per-slot layout must use a separate profile.

The host bridge is `kuasar-ci0` at `10.203.0.1/24`. Exact iptables rules NAT
that subnet through the current default uplink. The network service records its
ownership in the bridge interface alias and refuses to modify or delete an
unowned interface with the configured name. The provisioner refuses to run
while the existing host runner has an active `Runner.Worker`, requires cgroup
v2, and rejects enabled DNF repositories outside configured Chinese mirrors.
`check` can run before `kmod` is installed; `install` obtains the host package
set first, then loads the required modules and validates all device nodes.

## Install

Run from this directory on the BMS host as root:

```bash
./provision.sh check
./provision.sh install
```

The install root is built once from the already configured Huawei Cloud
openEuler mirror and copied into `/var/lib/machines/kuasar-ci-{1..6}`. Because
openEuler does not package the static `libuuid.a` required by the guest
`mkfs.erofs`, the provisioner builds it inside the install root from the pinned
openEuler `util-linux` source RPM. Both the source RPM and its upstream tarball
are SHA-256 verified; the 8 MiB RPM is cached under `/var/cache/kuasar/sources`
and downloaded from Huawei Cloud. The packaged `libstdc++-static` dependency
used by the RocksDB-linked `cache-ctl` and the Redis server used by Accelerator
E2E are installed from the same mirror. GNU `time` provides per-stage CPU,
memory, and I/O metrics. Every install reconciles the package manifest so
existing slots receive newly added build dependencies.

Changing the pinned util-linux source requires overriding the complete source
descriptor together: `KUASAR_UTIL_LINUX_SRPM_URL`,
`KUASAR_UTIL_LINUX_SRPM_SHA256`, `KUASAR_UTIL_LINUX_SOURCE_ARCHIVE`, and
`KUASAR_UTIL_LINUX_TARBALL_SHA256`. The archive name is validated as a plain
file name and is included in the static-library build identity.

The host install also writes `/etc/modules-load.d/kuasar-ci.conf` for bridge,
overlay, TUN, and vhost devices. Enabled slots therefore retain their required
bind devices after a host reboot. Space checks follow the filesystems that hold
the template and `/var/lib/machines`; a shared filesystem requires 35 GiB free,
while separate filesystems require 5 GiB and 30 GiB respectively.
The provisioner makes a slot root visible only after writing a separate owner
marker into a staging root. An incomplete owned root is rebuilt automatically
on retry; a markerless or mismatched root is never modified or deleted.
Owned staging roots left by interruption are removed before the free-space
check; an unowned staging path is refused. Completed owned roots are always
reconciled in place. Mutating provision commands are serialized by a host lock.

The existing runner distribution is copied without credentials, logs, or its
large work directory. Runner self-update is disabled so containers do not
download a large international release unexpectedly; update the host
distribution and rerun installation deliberately when GitHub's runner support
window requires it. Synchronization deletes files removed from the new runner
distribution while preserving only the explicitly excluded credentials,
diagnostics, work directory, environment, path, and registration marker. The
legacy host runner service must be stopped before an
install. Existing container slots must also be stopped while their package
sets and runner files are reconciled.

The runner service refuses a direct manual stop and has no start-rate ceiling.
This prevents an accidental `systemctl stop actions-runner` or a transient
failure burst from leaving an otherwise healthy slot offline. Deliberate
maintenance stops the owning `systemd-nspawn@kuasar-ci-N.service` through the
provisioner's `stop` command instead.

Each installroot transaction replaces the openEuler release package's default
metalink configuration with the host repository files both before and after the
package operation. This prevents an `openEuler-release` update from restoring
international metalinks. Keep `/etc/yum.repos.d/*.repo` on the host pointed at a
direct China mirror; the same configuration is propagated to all slots.

Generate short-lived organization registration tokens with an authenticated
`gh` client and stream them over SSH. The provisioner forwards the token over
the container's stdin as the runner's `ACTIONS_RUNNER_INPUT_TOKEN`; it never
places the token in command arguments or files:

```bash
gh api --method POST /orgs/kuasar-sandbox/actions/runners/registration-token \
  --jq .token | ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision register 1'

gh api --method POST /orgs/kuasar-sandbox/actions/runners/registration-token \
  --jq .token | ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision register 2'

# Repeat with a fresh token for slots 3 through 6.
```

A registration completion marker is written only after runner configuration,
the fixed PATH, and service enablement all succeed. Supplying a fresh token
retries any markerless partial registration through the runner's `--replace`
flow; a completed registration is left unchanged.

## GitHub outbound proxy

When direct GitHub connectivity from mainland China is unreliable, configure
the organization-controlled egress proxy before starting the slots. The proxy
must be credentialless from the workload's perspective and authorize egress by
network-side policy. Keep its URL outside the repository in a root-readable file
using the lowercase variable names consumed by the runner:

```text
https_proxy=<organization-proxy-url>
http_proxy=<organization-proxy-url>
no_proxy=<internal-hosts-and-test-networks>
```

Copy that file to each registered runner with mode `0600` while all slots are
stopped:

```bash
for slot in 1 2 3 4 5 6; do
  install -m 0600 /etc/kuasar-ci/runner-proxy.env \
    "/var/lib/machines/kuasar-ci-$slot/opt/actions-runner/.env"
done
```

The runner reads `.env` only at startup, so restart the slots after every proxy
change. The provisioner deliberately preserves each runner's `.env` while it
reconciles the runner distribution. These runners execute fork code; the
runner-level proxy must therefore use network-side access control and must not
put reusable credentials where a workload can read them.

The `.env` `no_proxy` list must include local control endpoints, sandbox test
networks, and internal registries so E2E traffic remains local. Workflows do
not override these variables, which also preserves a runner's direct network
environment when no proxy is configured.

All runners join the existing `kuasar-e2e` organization group with labels
`kuasar-e2e,kvm,cgroup-v2` plus a slot label. The group must remain
organization-wide (`visibility=all`) with no selected-repository or workflow
restriction.

Start and verify infrastructure isolation:

```bash
ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision start'
ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision verify'
```

`start` rechecks that the retained legacy host runner is stopped. If any
container fails to start or become ready, it stops every slot started by that
command and restores each unit's previous enabled state; slots that were already
active are left active.

For rollback, stop all container slots without deleting their state. A failed
container stop makes the command fail instead of leaving a slot running:

```bash
ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision stop'
```

`start` and `verify` require the current runner service invocation to report
`Listening for Jobs`, rather than matching an older process from the same boot
or treating an active retrying service as online. `verify` also checks
PID1/systemd, cgroup v2, KVM/TUN/vhost access,
private mount and network namespaces, distinct host cgroups, nested Docker,
private bpffs/netns, the static `libuuid` build dependency, outbound access
through the China-side proxy path, and active runner services. It is not an E2E
wrapper. Repository workflows still execute `make test-e2e` directly.

Do not stop or unregister the existing host runner until all slots have passed
the full five-repository E2E concurrently three times and cancellation cleanup
has been verified. During migration it may be stopped but retained as a quick
rollback path.
