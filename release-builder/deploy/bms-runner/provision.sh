#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORG=${KUASAR_ORG:-kuasar-sandbox}
RUNNER_GROUP=${KUASAR_RUNNER_GROUP:-kuasar-e2e}
RUNNER_SOURCE=${KUASAR_RUNNER_SOURCE:-/opt/actions-runner}
TEMPLATE_ROOT=${KUASAR_TEMPLATE_ROOT:-/var/lib/kuasar-ci/rootfs-template}
MACHINE_ROOT=${KUASAR_MACHINE_ROOT:-/var/lib/machines}
BRIDGE=${KUASAR_CI_BRIDGE:-kuasar-ci0}
SUBNET=${KUASAR_CI_SUBNET:-10.203.0.0/24}
BRIDGE_ADDRESS=${KUASAR_CI_BRIDGE_ADDRESS:-10.203.0.1/24}
MEMORY_MAX=${KUASAR_SLOT_MEMORY_MAX:-176G}
MEMORY_HIGH=${KUASAR_SLOT_MEMORY_HIGH:-168G}

SLOT1_CPUS=${KUASAR_SLOT1_CPUS:-0-21,44-65}
SLOT2_CPUS=${KUASAR_SLOT2_CPUS:-22-43,66-87}
SLOT1_NUMA=${KUASAR_SLOT1_NUMA:-0}
SLOT2_NUMA=${KUASAR_SLOT2_NUMA:-1}
SLOT1_IP=${KUASAR_SLOT1_IP:-10.203.0.11/24}
SLOT2_IP=${KUASAR_SLOT2_IP:-10.203.0.12/24}

PACKAGES=(
    openEuler-release systemd systemd-networkd dbus dnf passwd shadow sudo
    bash coreutils findutils grep gawk sed diffutils patch tar gzip xz cpio
    ca-certificates curl libcurl libicu krb5-libs openssh-clients tzdata zlib
    git git-lfs rsync util-linux util-linux-devel iproute iptables nftables
    procps-ng which file hostname kmod iputils jq socat openssl sqlite
    gcc gcc-c++ make cmake autoconf automake libtool pkgconf
    glibc-devel openssl-devel elfutils-libelf-devel ncurses-devel flex bison dwarves perl bc
    rust cargo rust-std-static clang llvm bpftool
    python3 python3-pip python3-devel python3-pyyaml
    moby-engine moby-client e2fsprogs unzip zip zstd lz4
)

die() {
    echo "provision: $*" >&2
    exit 1
}

log() {
    echo "provision: $*"
}

require_root() {
    [ "$(id -u)" -eq 0 ] || die "must run as root"
}

machine_name() {
    printf 'kuasar-ci-%s' "$1"
}

slot_root() {
    printf '%s/%s' "$MACHINE_ROOT" "$(machine_name "$1")"
}

slot_value() {
    local slot=$1 field=$2 variable="SLOT${slot}_${field}"
    printf '%s' "${!variable}"
}

assert_supported_host() {
    # shellcheck disable=SC1091
    source /etc/os-release
    [ "${ID:-}" = openEuler ] || die "this provisioner targets openEuler, found ${ID:-unknown}"
    [ "${VERSION_ID:-}" = 24.03 ] || die "expected openEuler 24.03, found ${VERSION_ID:-unknown}"
    [ "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs ] || die "host must use cgroup v2"
    [ -c /dev/kvm ] && [ -c /dev/net/tun ] && [ -c /dev/vhost-vsock ] \
        || die "KVM/TUN/vhost-vsock devices are required"
}

assert_china_repositories() {
    local urls
    urls="$(dnf repolist -v 2>/dev/null | awk -F ': ' '/Repo-baseurl/ { print $2 }')"
    [ -n "$urls" ] || die "dnf has no enabled repository base URLs"
    while IFS= read -r url; do
        case "$url" in
            https://mirrors.huaweicloud.com/*|https://mirrors.tuna.tsinghua.edu.cn/*|https://mirrors.aliyun.com/*) ;;
            *) die "non-China dnf mirror is enabled: $url" ;;
        esac
    done <<<"$urls"
}

assert_host_runner_idle() {
    if pgrep -af '/Runner.Worker' >/dev/null 2>&1; then
        pgrep -af '/Runner.Worker' >&2 || true
        die "the existing host runner is executing a job"
    fi
}

check_host() {
    require_root
    assert_supported_host
    assert_china_repositories
    local package missing=()
    for package in "${PACKAGES[@]}"; do
        if ! dnf -q repoquery --available --qf '%{name}' "$package" | grep -qx "$package"; then
            missing+=("$package")
        fi
    done
    [ "${#missing[@]}" -eq 0 ] || die "packages unavailable from enabled mirrors: ${missing[*]}"
    log "host, devices, cgroup v2, China mirrors, and package set are ready"
}

install_host_support() {
    dnf -y --setopt=install_weak_deps=False install systemd-container
    install -d -m 0755 /usr/local/libexec /etc/systemd/system /etc/systemd/nspawn /etc/kuasar-ci
    install -m 0755 "$SCRIPT_DIR/kuasar-ci-network" /usr/local/libexec/kuasar-ci-network
    install -m 0644 "$SCRIPT_DIR/kuasar-ci-network.service" /etc/systemd/system/kuasar-ci-network.service
    install -m 0755 "$SCRIPT_DIR/provision.sh" /usr/local/sbin/kuasar-ci-runner-provision

    local uplink
    uplink="$(ip route show default | awk 'NR == 1 { print $5 }')"
    [ -n "$uplink" ] || die "cannot determine the host uplink"
    cat >/etc/kuasar-ci-network.conf <<EOF
BRIDGE=$BRIDGE
BRIDGE_ADDRESS=$BRIDGE_ADDRESS
SUBNET=$SUBNET
UPLINK_IFACE=$uplink
EOF
    systemctl daemon-reload
    systemctl enable --now kuasar-ci-network.service
    modprobe bridge
    modprobe overlay
    modprobe tun
    modprobe vhost_net
    modprobe vhost_vsock
}

copy_runner_distribution() {
    local destination=$1
    [ -x "$RUNNER_SOURCE/config.sh" ] || die "runner distribution not found at $RUNNER_SOURCE"
    install -d -m 0755 "$destination"
    rsync -a \
        --exclude '/_diag/' --exclude '/_work/' \
        --exclude '/.credentials' --exclude '/.credentials_rsaparams' \
        --exclude '/.runner' --exclude '/.runner_migrated' --exclude '/.service' \
        --exclude '/.env' --exclude '/.path' \
        "$RUNNER_SOURCE/" "$destination/"
}

build_template_root() {
    if [ ! -f "$TEMPLATE_ROOT/.kuasar-ci-template" ]; then
        [ ! -e "$TEMPLATE_ROOT" ] || die "partial template root exists: $TEMPLATE_ROOT"
        install -d -m 0755 "$TEMPLATE_ROOT"
        log "installing container packages from configured China mirrors"
        dnf -y --installroot="$TEMPLATE_ROOT" --releasever=24.03 \
            --setopt=install_weak_deps=False --setopt=keepcache=False \
            install "${PACKAGES[@]}"
        touch "$TEMPLATE_ROOT/.kuasar-ci-template"
    fi

    copy_runner_distribution "$TEMPLATE_ROOT/opt/actions-runner"
    install -d -m 0755 \
        "$TEMPLATE_ROOT/etc/systemd/system" \
        "$TEMPLATE_ROOT/etc/systemd/network" \
        "$TEMPLATE_ROOT/etc/docker" \
        "$TEMPLATE_ROOT/etc/pip" \
        "$TEMPLATE_ROOT/root/.cargo" \
        "$TEMPLATE_ROOT/var/log/journal" \
        "$TEMPLATE_ROOT/var/cache/kuasar" \
        "$TEMPLATE_ROOT/var/lib/kuasar-ci/tools" \
        "$TEMPLATE_ROOT/usr/local/go" \
        "$TEMPLATE_ROOT/usr/lib/modules"

    cat >"$TEMPLATE_ROOT/root/.cargo/config.toml" <<'EOF'
[source.crates-io]
replace-with = "rsproxy-sparse"

[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"

[registries.rsproxy]
index = "sparse+https://rsproxy.cn/index/"

[net]
git-fetch-with-cli = true
retry = 3
EOF
    cat >"$TEMPLATE_ROOT/etc/pip.conf" <<'EOF'
[global]
index-url = https://pypi.tuna.tsinghua.edu.cn/simple
trusted-host = pypi.tuna.tsinghua.edu.cn
timeout = 60
retries = 5
EOF
    cat >"$TEMPLATE_ROOT/etc/docker/daemon.json" <<'EOF'
{
  "log-driver": "local",
  "storage-driver": "overlay2"
}
EOF
    cat >"$TEMPLATE_ROOT/etc/resolv.conf" <<'EOF'
nameserver 223.5.5.5
nameserver 119.29.29.29
options timeout:2 attempts:3
EOF
    install -m 0644 "$SCRIPT_DIR/actions-runner.service" \
        "$TEMPLATE_ROOT/etc/systemd/system/actions-runner.service"
    systemctl --root="$TEMPLATE_ROOT" enable systemd-networkd.service docker.service >/dev/null
}

write_nspawn_config() {
    local slot=$1 machine root cpus numa
    machine="$(machine_name "$slot")"
    root="$(slot_root "$slot")"
    cpus="$(slot_value "$slot" CPUS)"
    numa="$(slot_value "$slot" NUMA)"

    cat >"/etc/systemd/nspawn/$machine.nspawn" <<EOF
[Exec]
Boot=yes
PrivateUsers=no
Capability=all
SystemCallFilter=@keyring bpf
SuppressSync=false
ResolvConf=off
Timezone=off
LinkJournal=no

[Files]
Bind=/var/cache/kuasar
BindReadOnly=/var/lib/kuasar-ci/tools
BindReadOnly=/usr/local/go
BindReadOnly=/usr/lib/modules
Bind=/dev/kvm
Bind=/dev/net/tun
Bind=/dev/vhost-net
Bind=/dev/vhost-vsock

[Network]
Bridge=$BRIDGE
EOF

    install -d -m 0755 "/etc/systemd/system/systemd-nspawn@$machine.service.d"
    cat >"/etc/systemd/system/systemd-nspawn@$machine.service.d/override.conf" <<EOF
[Unit]
Requires=kuasar-ci-network.service
After=kuasar-ci-network.service

[Service]
DeviceAllow=/dev/kvm rw
DeviceAllow=/dev/net/tun rw
DeviceAllow=/dev/vhost-net rw
DeviceAllow=/dev/vhost-vsock rw
AllowedCPUs=$cpus
AllowedMemoryNodes=$numa
NUMAPolicy=bind
NUMAMask=$numa
MemoryHigh=$MEMORY_HIGH
MemoryMax=$MEMORY_MAX
TasksMax=infinity
LimitNOFILE=1048576
LimitMEMLOCK=infinity
IOWeight=100
EOF
}

prepare_slot() {
    local slot=$1 machine root ip new_slot=0
    machine="$(machine_name "$slot")"
    root="$(slot_root "$slot")"
    ip="$(slot_value "$slot" IP)"

    if [ ! -f "$root/.kuasar-ci-slot" ]; then
        [ ! -e "$root" ] || die "partial slot root exists: $root"
        log "copying rootfs for $machine"
        cp -a "$TEMPLATE_ROOT" "$root"
        touch "$root/.kuasar-ci-slot"
        new_slot=1
    fi
    copy_runner_distribution "$root/opt/actions-runner"

    if [ "$new_slot" -eq 1 ]; then
        rm -f "$root/etc/machine-id" "$root/var/lib/dbus/machine-id"
        : >"$root/etc/machine-id"
        systemd-machine-id-setup --root="$root" >/dev/null
        ln -sfn /etc/machine-id "$root/var/lib/dbus/machine-id"
    fi
    printf '%s\n' "$machine" >"$root/etc/hostname"
    cat >"$root/etc/hosts" <<EOF
127.0.0.1 localhost
127.0.1.1 $machine
::1 localhost ip6-localhost ip6-loopback
EOF
    cat >"$root/etc/systemd/network/80-host0.network" <<EOF
[Match]
Name=host0

[Network]
Address=$ip
Gateway=${BRIDGE_ADDRESS%/*}
DNS=223.5.5.5
DNS=119.29.29.29
IPv6AcceptRA=no
EOF
    install -m 0644 "$SCRIPT_DIR/actions-runner.service" \
        "$root/etc/systemd/system/actions-runner.service"
    systemctl --root="$root" enable systemd-networkd.service docker.service >/dev/null
    write_nspawn_config "$slot"
}

install_slots() {
    require_root
    assert_supported_host
    assert_china_repositories
    assert_host_runner_idle
    local free_kib
    free_kib="$(df --output=avail -k / | tail -1)"
    [ "$free_kib" -ge $((15 * 1024 * 1024)) ] || die "at least 15 GiB free space is required"
    install_host_support
    build_template_root
    prepare_slot 1
    prepare_slot 2
    systemctl daemon-reload
    log "slots installed but not started; register each slot next"
}

register_slot() {
    require_root
    local slot=${1:-} token machine root
    case "$slot" in 1|2) ;; *) die "register requires slot 1 or 2" ;; esac
    IFS= read -r token
    [ -n "$token" ] || die "registration token must be provided on stdin"
    machine="$(machine_name "$slot")"
    root="$(slot_root "$slot")"
    [ -f "$root/.kuasar-ci-slot" ] || die "slot $slot is not installed"
    systemctl is-active --quiet "systemd-nspawn@$machine.service" \
        && die "$machine must be stopped before registration"
    [ ! -f "$root/opt/actions-runner/.runner" ] || die "$machine is already registered"

    rm -f "$root/opt/actions-runner/.credentials" \
        "$root/opt/actions-runner/.credentials_rsaparams" \
        "$root/opt/actions-runner/.runner" \
        "$root/opt/actions-runner/.runner_migrated" \
        "$root/opt/actions-runner/.service"
    RUNNER_ALLOW_RUNASROOT=1 systemd-nspawn --quiet --pipe --settings=no --register=no \
        --directory="$root" --setenv=RUNNER_ALLOW_RUNASROOT=1 \
        /opt/actions-runner/config.sh --unattended --replace --disableupdate \
        --url "https://github.com/$ORG" --token "$token" \
        --name "bms-tmp-kuasar-e2e-$slot" --runnergroup "$RUNNER_GROUP" \
        --labels "kuasar-e2e,kvm,cgroup-v2,bms-slot-$slot" --work _work
    printf '/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin\n' \
        >"$root/opt/actions-runner/.path"
    systemctl --root="$root" enable actions-runner.service >/dev/null
    log "$machine registered"
}

start_slots() {
    require_root
    systemctl start kuasar-ci-network.service
    local slot machine root
    for slot in 1 2; do
        machine="$(machine_name "$slot")"
        root="$(slot_root "$slot")"
        [ -f "$root/opt/actions-runner/.runner" ] || die "$machine is not registered"
        systemctl enable --now "systemd-nspawn@$machine.service"
    done
}

stop_slots() {
    require_root
    local slot machine
    for slot in 1 2; do
        machine="$(machine_name "$slot")"
        systemctl disable --now "systemd-nspawn@$machine.service" 2>/dev/null || true
    done
}

run_in_slot() {
    local slot=$1
    shift
    systemd-run --quiet --wait --pipe --collect --machine="$(machine_name "$slot")" "$@"
}

verify_slots() {
    require_root
    local slot machine
    for slot in 1 2; do
        machine="$(machine_name "$slot")"
        [ "$(machinectl show "$machine" -p State --value)" = running ] || die "$machine is not running"
        run_in_slot "$slot" /bin/bash -ceu '
            [ "$(cat /proc/1/comm)" = systemd ]
            [ "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs ]
            test -r /dev/kvm -a -w /dev/kvm
            test -r /dev/net/tun -a -w /dev/net/tun
            test -r /dev/vhost-vsock -a -w /dev/vhost-vsock
            systemctl is-active --quiet docker.service
            docker info >/dev/null
            ip route get 223.5.5.5 >/dev/null
            curl --fail --silent --show-error --connect-timeout 5 --max-time 20 https://goproxy.cn >/dev/null
            mkdir -p /sys/fs/bpf
            mountpoint -q /sys/fs/bpf || mount -t bpf bpf /sys/fs/bpf
            ip netns add kuasar-isolation-probe
            ip netns del kuasar-isolation-probe
            systemctl is-active --quiet actions-runner.service
        '
    done

    run_in_slot 1 /usr/bin/touch /run/kuasar-slot-isolation-probe
    if run_in_slot 2 /usr/bin/test -e /run/kuasar-slot-isolation-probe; then
        die "slot 2 can see slot 1 private /run"
    fi
    run_in_slot 1 /usr/bin/rm -f /run/kuasar-slot-isolation-probe

    local leader1 leader2 mnt1 mnt2 net1 net2 cgroup1 cgroup2
    leader1="$(machinectl show kuasar-ci-1 -p Leader --value)"
    leader2="$(machinectl show kuasar-ci-2 -p Leader --value)"
    mnt1="$(readlink "/proc/$leader1/ns/mnt")"
    mnt2="$(readlink "/proc/$leader2/ns/mnt")"
    net1="$(readlink "/proc/$leader1/ns/net")"
    net2="$(readlink "/proc/$leader2/ns/net")"
    cgroup1="$(cat "/proc/$leader1/cgroup")"
    cgroup2="$(cat "/proc/$leader2/cgroup")"
    [ "$mnt1" != "$mnt2" ] || die "slots share a mount namespace"
    [ "$net1" != "$net2" ] || die "slots share a network namespace"
    [ "$cgroup1" != "$cgroup2" ] || die "slots share a host cgroup"
    log "slot isolation checks passed"
}

status_slots() {
    local slot machine
    systemctl --no-pager --full status kuasar-ci-network.service || true
    for slot in 1 2; do
        machine="$(machine_name "$slot")"
        systemctl --no-pager --full status "systemd-nspawn@$machine.service" || true
        machinectl status "$machine" || true
    done
}

usage() {
    cat <<'EOF'
usage: provision.sh install
       provision.sh check
       provision.sh register 1|2   # registration token on stdin
       provision.sh start
       provision.sh stop
       provision.sh verify
       provision.sh status
EOF
}

case "${1:-}" in
    check) check_host ;;
    install) install_slots ;;
    register) shift; register_slot "${1:-}" ;;
    start) start_slots ;;
    stop) stop_slots ;;
    verify) verify_slots ;;
    status) status_slots ;;
    *) usage >&2; exit 2 ;;
esac
