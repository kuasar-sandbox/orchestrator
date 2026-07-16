# BMS runner slots

This directory provisions two privileged `systemd-nspawn` system containers on
the openEuler 24.03 BMS host. Each container owns its systemd, journald, PID,
mount, network, cgroup, Docker daemon, runner credentials, and Actions work
directory. The containers share only:

- `/var/cache/kuasar`, for exact-SHA source archives and immutable native
  artifacts protected by repository-owned locks;
- `/var/lib/kuasar-ci/tools`, read-only test tool binaries;
- `/usr/local/go` and the host kernel module tree, read-only.

Each slot receives a different bpffs subtree at `/sys/fs/bpf`; pinned BPF paths
cannot collide across concurrent jobs.

The containers are privileged resource-name isolation, not a security boundary
for untrusted jobs. They deliberately receive KVM, TUN, vhost devices, all
capabilities, Docker keyring syscalls, and the `bpf` syscall required by the
connector datapath.

## Host layout

| Slot | Runner name | CPU/NUMA | Memory | Address |
| --- | --- | --- | --- | --- |
| 1 | `bms-tmp-kuasar-e2e-1` | NUMA0: `0-21,44-65` | high 168 GiB, max 176 GiB | `10.203.0.11/24` |
| 2 | `bms-tmp-kuasar-e2e-2` | NUMA1: `22-43,66-87` | high 168 GiB, max 176 GiB | `10.203.0.12/24` |

The host bridge is `kuasar-ci0` at `10.203.0.1/24`. Exact iptables rules NAT
that subnet through the current default uplink. The provisioner refuses to run
while the existing host runner has an active `Runner.Worker`, requires cgroup
v2, and rejects enabled DNF repositories outside configured Chinese mirrors.

## Install

Run from this directory on the BMS host as root:

```bash
./provision.sh check
./provision.sh install
```

The install root is built once from the already configured Huawei Cloud
openEuler mirror and copied into `/var/lib/machines/kuasar-ci-{1,2}`. Because
openEuler does not package the static `libuuid.a` required by the guest
`mkfs.erofs`, the provisioner builds it inside the install root from the pinned
openEuler `util-linux` source RPM. Both the source RPM and its upstream tarball
are SHA-256 verified; the 8 MiB RPM is cached under `/var/cache/kuasar/sources`
and downloaded from Huawei Cloud.

The existing runner distribution is copied without credentials, logs, or its
large work directory. Runner self-update is disabled so containers do not
download a large international release unexpectedly; update the host
distribution and rerun installation deliberately when GitHub's runner support
window requires it. The legacy host runner service must be stopped before an
install, while container runner processes do not block an idempotent update.

Generate short-lived organization registration tokens with an authenticated
`gh` client and stream them over SSH; do not put them in command arguments or
files:

```bash
gh api --method POST /orgs/kuasar-sandbox/actions/runners/registration-token \
  --jq .token | ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision register 1'

gh api --method POST /orgs/kuasar-sandbox/actions/runners/registration-token \
  --jq .token | ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision register 2'
```

Both runners join the existing `kuasar-e2e` organization group with labels
`kuasar-e2e,kvm,cgroup-v2` plus a slot label. The group must remain
organization-wide (`visibility=all`) with no selected-repository or workflow
restriction.

Start and verify infrastructure isolation:

```bash
ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision start'
ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision verify'
```

For rollback, stop both container slots without deleting their state:

```bash
ssh bms.tmp '/usr/local/sbin/kuasar-ci-runner-provision stop'
```

`verify` checks PID1/systemd, cgroup v2, KVM/TUN/vhost access, private mount and
network namespaces, distinct host cgroups, nested Docker, private bpffs/netns,
the static `libuuid` build dependency, outbound access through the China-side
proxy path, and active runner services. It is not an E2E wrapper. Repository
workflows still execute `make test-e2e` directly.

Do not stop or unregister the existing host runner until both slots have passed
the full five-repository E2E concurrently three times and cancellation cleanup
has been verified. During migration it may be stopped but retained as a quick
rollback path.
