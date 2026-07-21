#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORG=${KUASAR_ORG:-kuasar-sandbox}
RUNNER_GROUP=${KUASAR_RUNNER_GROUP:-kuasar-e2e}
RUNNER_SOURCE=${KUASAR_RUNNER_SOURCE:-/opt/actions-runner}
TEMPLATE_ROOT=/var/lib/kuasar-ci/rootfs-template
MACHINE_ROOT=/var/lib/machines
BRIDGE=${KUASAR_CI_BRIDGE:-kuasar-ci0}
SUBNET=${KUASAR_CI_SUBNET:-10.203.0.0/24}
BRIDGE_ADDRESS=${KUASAR_CI_BRIDGE_ADDRESS:-10.203.0.1/24}
MEMORY_MAX=${KUASAR_SLOT_MEMORY_MAX:-56G}
MEMORY_HIGH=${KUASAR_SLOT_MEMORY_HIGH:-52G}
SOURCE_CACHE=${KUASAR_SOURCE_CACHE:-/var/cache/kuasar/sources}
TOOL_ROOT=/var/lib/kuasar-ci/tools
ZOT_TOOL_SHA256=523e5bf29a013db09115f780c3152af98fc5b65fc408a0d3e6c293643dc9bde7
VERSITYGW_TOOL_SHA256=e839f0ce24a51dbf0a7a925e08a28a0bfa190d05290c13f2c4536852bc5f3a7d
GO_ROOT=/usr/local/go
GO_VERSION=go1.26.5
GO_TARBALL_URL=https://mirrors.aliyun.com/golang/go1.26.5.linux-amd64.tar.gz
GO_TARBALL_SHA256=5c2c3b16caefa1d968a94c1daca04a7ca301a496d9b086e17ad77bb81393f053
GO_BINARY_SHA256=8da5fd321795754b994c64e3eb8a5a14ff47bd285559a7e876f3c79abafc67f9
UTIL_LINUX_SRPM_URL=${KUASAR_UTIL_LINUX_SRPM_URL:-https://mirrors.huaweicloud.com/openeuler/openEuler-24.03-LTS-SP4/source/Packages/util-linux-2.39.1-38.oe2403sp4.src.rpm}
UTIL_LINUX_SRPM_SHA256=${KUASAR_UTIL_LINUX_SRPM_SHA256:-40324d3ab54be52ef67544732a71ec14f6aecb2e92f5d8fa0aaaac532c55c0bf}
UTIL_LINUX_SOURCE_ARCHIVE=${KUASAR_UTIL_LINUX_SOURCE_ARCHIVE:-util-linux-2.39.1.tar.xz}
UTIL_LINUX_TARBALL_SHA256=${KUASAR_UTIL_LINUX_TARBALL_SHA256:-890ae8ff810247bd19e274df76e8371d202cda01ad277681b0ea88eeaa00286b}
LIBUUID_BUILD_SCHEMA=util-linux-static-v1
SLOT_OWNER_MARKER=.kuasar-ci-slot-owner
SLOT_OWNER_ID=kuasar-ci-bms-runner-v1
RUNNER_REGISTRATION_MARKER=.kuasar-ci-registration-complete
PROVISION_LOCK=/run/lock/kuasar-ci-runner-provision.lock

SLOTS=(1 2 3 4 5 6)
SLOT1_CPUS=${KUASAR_SLOT1_CPUS:-0-6,44-50}
SLOT2_CPUS=${KUASAR_SLOT2_CPUS:-7-13,51-57}
SLOT3_CPUS=${KUASAR_SLOT3_CPUS:-14-20,58-64}
SLOT4_CPUS=${KUASAR_SLOT4_CPUS:-22-28,66-72}
SLOT5_CPUS=${KUASAR_SLOT5_CPUS:-29-35,73-79}
SLOT6_CPUS=${KUASAR_SLOT6_CPUS:-36-42,80-86}
SLOT1_NUMA=${KUASAR_SLOT1_NUMA:-0}
SLOT2_NUMA=${KUASAR_SLOT2_NUMA:-0}
SLOT3_NUMA=${KUASAR_SLOT3_NUMA:-0}
SLOT4_NUMA=${KUASAR_SLOT4_NUMA:-1}
SLOT5_NUMA=${KUASAR_SLOT5_NUMA:-1}
SLOT6_NUMA=${KUASAR_SLOT6_NUMA:-1}
SLOT1_IP=${KUASAR_SLOT1_IP:-10.203.0.11/24}
SLOT2_IP=${KUASAR_SLOT2_IP:-10.203.0.12/24}
SLOT3_IP=${KUASAR_SLOT3_IP:-10.203.0.13/24}
SLOT4_IP=${KUASAR_SLOT4_IP:-10.203.0.14/24}
SLOT5_IP=${KUASAR_SLOT5_IP:-10.203.0.15/24}
SLOT6_IP=${KUASAR_SLOT6_IP:-10.203.0.16/24}

PACKAGES=(
    openEuler-release systemd systemd-networkd dbus dnf passwd shadow sudo
    bash coreutils findutils grep gawk sed diffutils patch tar gzip xz cpio
    ca-certificates curl libcurl libicu krb5-libs openssh-clients tzdata zlib
    git git-lfs rsync util-linux util-linux-devel iproute iptables nftables
    procps-ng which time file hostname kmod iputils jq socat openssl sqlite
    gcc gcc-c++ libstdc++-static make cmake autoconf automake libtool pkgconf
    glibc-devel openssl-devel elfutils-libelf-devel ncurses-devel flex bison dwarves perl bc
    lz4-devel zstd-devel zlib-devel snappy-devel
    rust cargo rust-std-static clang llvm bpftool
    python3 python3-pip python3-devel python3-pyyaml
    moby-engine moby-client redis e2fsprogs unzip zip zstd lz4
)
BOOTSTRAP_PACKAGES=(filesystem glibc bash coreutils)
HOST_PACKAGES=(
    bash coreutils findutils grep gawk tar xz curl rsync util-linux procps-ng
    systemd systemd-container systemd-nspawn dnf rpm cpio
    iproute iptables kmod
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

acquire_provision_lock() {
    install -d -m 0755 "${PROVISION_LOCK%/*}"
    exec 9>"$PROVISION_LOCK"
    flock -n 9 || die "another runner provisioning command is active"
}

machine_name() {
    printf 'kuasar-ci-%s' "$1"
}

slot_is_valid() {
    local expected
    for expected in "${SLOTS[@]}"; do
        [ "$1" = "$expected" ] && return 0
    done
    return 1
}

slot_root() {
    printf '%s/%s' "$MACHINE_ROOT" "$(machine_name "$1")"
}

slot_root_owned() {
    local root=$1 owner
    [ -d "$root" ] && [ ! -L "$root" ] || return 1
    IFS= read -r owner <"$root/$SLOT_OWNER_MARKER" || return 1
    [ "$owner" = "$SLOT_OWNER_ID" ]
}

assert_slot_root_owned() {
    local slot=$1 root
    root="$(slot_root "$slot")"
    slot_root_owned "$root" \
        || die "refusing to manage unowned slot root $root"
}

runner_registration_complete() {
    local root=$1 runner="$1/opt/actions-runner"
    [ -f "$runner/$RUNNER_REGISTRATION_MARKER" ] \
        && [ -s "$runner/.runner" ] \
        && [ -s "$runner/.path" ] \
        && systemctl --root="$root" is-enabled --quiet actions-runner.service
}

cleanup_stale_slot_staging() {
    local slot machine staging stale=()
    [ -d "$MACHINE_ROOT" ] || return 0
    for slot in "${SLOTS[@]}"; do
        machine="$(machine_name "$slot")"
        shopt -s nullglob
        stale=("$MACHINE_ROOT/.${machine}.install."*)
        shopt -u nullglob
        for staging in "${stale[@]}"; do
            slot_root_owned "$staging" \
                || die "refusing to remove unowned staging root $staging"
            log "removing interrupted staging root $staging"
            rm -rf "$staging"
        done
    done
}

assert_distinct_slot_machine_ids() {
    local slot id
    local -A owners=()
    for slot in "${SLOTS[@]}"; do
        id="$(cat "$(slot_root "$slot")/etc/machine-id" 2>/dev/null || true)"
        [[ "$id" =~ ^[0-9a-f]{32}$ ]] || die "slot $slot has an invalid machine ID"
        [ -z "${owners[$id]:-}" ] \
            || die "slots ${owners[$id]} and $slot share a machine ID"
        owners[$id]=$slot
    done
}

slot_value() {
    local slot=$1 field=$2 variable
    variable="SLOT${slot}_${field}"
    printf '%s' "${!variable}"
}

load_required_modules() {
    local module
    command -v modprobe >/dev/null || die "modprobe is required"
    for module in bridge overlay tun vhost_net vhost_vsock; do
        modprobe "$module" || die "failed to load required module $module"
    done
}

assert_required_devices() {
    [ -c /dev/kvm ] && [ -c /dev/net/tun ] \
        && [ -c /dev/vhost-net ] && [ -c /dev/vhost-vsock ] \
        || die "KVM/TUN/vhost-net/vhost-vsock devices are required"
}

assert_supported_host() {
    # shellcheck disable=SC1091
    source /etc/os-release
    [ "${ID:-}" = openEuler ] || die "this provisioner targets openEuler, found ${ID:-unknown}"
    [ "${VERSION_ID:-}" = 24.03 ] || die "expected openEuler 24.03, found ${VERSION_ID:-unknown}"
    [ "$(uname -m)" = x86_64 ] || die "this runner layout and pinned tools require x86_64"
    [ "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs ] || die "host must use cgroup v2"
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

runner_worker_in_managed_slot() {
    local proc=$1 slot
    [ -r "$proc/cgroup" ] || return 1
    for slot in "${SLOTS[@]}"; do
        grep -Fq "/systemd-nspawn@$(machine_name "$slot").service" "$proc/cgroup" \
            && return 0
    done
    return 1
}

assert_host_runner_idle() {
    local proc comm service_file="$RUNNER_SOURCE/.service" service
    for proc in /proc/[0-9]*; do
        [ -r "$proc/comm" ] || continue
        IFS= read -r comm <"$proc/comm" || continue
        if [ "$comm" = Runner.Worker ] && ! runner_worker_in_managed_slot "$proc"; then
            die "host Runner.Worker process ${proc##*/} must exit before managing container slots"
        fi
    done
    [ -f "$service_file" ] || return 0
    service="$(<"$service_file")"
    if systemctl is-active --quiet "$service"; then
        die "the existing host runner service must be stopped before managing container slots: $service"
    fi
}

assert_slots_stopped() {
    local slot machine
    for slot in "${SLOTS[@]}"; do
        machine="$(machine_name "$slot")"
        if systemctl is-active --quiet "systemd-nspawn@$machine.service"; then
            die "$machine must be stopped before installation"
        fi
    done
}

verify_sha256() {
    local path=$1 expected=$2
    printf '%s  %s\n' "$expected" "$path" | sha256sum --check --status
}

assert_e2e_tools() {
    [ -x "$TOOL_ROOT/zot" ] \
        || die "preload the pinned zot binary at $TOOL_ROOT/zot before installation"
    [ -x "$TOOL_ROOT/versitygw" ] \
        || die "preload the pinned versitygw binary at $TOOL_ROOT/versitygw before installation"
    verify_sha256 "$TOOL_ROOT/zot" "$ZOT_TOOL_SHA256" \
        || die "zot checksum mismatch at $TOOL_ROOT/zot"
    verify_sha256 "$TOOL_ROOT/versitygw" "$VERSITYGW_TOOL_SHA256" \
        || die "versitygw checksum mismatch at $TOOL_ROOT/versitygw"
}

go_toolchain_valid() {
    local root=$1
    [ -x "$root/bin/go" ] \
        && verify_sha256 "$root/bin/go" "$GO_BINARY_SHA256" \
        && [ "$(env GOROOT="$root" "$root/bin/go" version 2>/dev/null)" = "go version $GO_VERSION linux/amd64" ] \
        && [ "$(env GOROOT="$root" "$root/bin/go" env GOROOT 2>/dev/null)" = "$root" ] \
        && [ "$(env GOROOT="$root" "$root/bin/go" tool compile -V=full 2>/dev/null)" = "compile version $GO_VERSION" ]
}

ensure_go_toolchain() {
    go_toolchain_valid "$GO_ROOT" && return
    case "$GO_TARBALL_URL" in
        https://mirrors.aliyun.com/*) ;;
        *) die "Go toolchain must use the configured China mirror" ;;
    esac

    local archive="$SOURCE_CACHE/${GO_TARBALL_URL##*/}" work
    install -d -m 0755 "$SOURCE_CACHE"
    if [ ! -f "$archive" ] || ! verify_sha256 "$archive" "$GO_TARBALL_SHA256"; then
        rm -f "$archive"
        log "downloading pinned $GO_VERSION toolchain from the Aliyun mirror"
        curl --fail --location --retry 3 --output "$archive" "$GO_TARBALL_URL"
    fi
    verify_sha256 "$archive" "$GO_TARBALL_SHA256" \
        || die "Go toolchain checksum mismatch: $archive"

    work="$(mktemp -d /usr/local/kuasar-go.XXXXXX)"
    if ! tar -xzf "$archive" -C "$work"; then
        rm -rf "$work"
        die "failed to extract the Go toolchain"
    fi
    if ! go_toolchain_valid "$work/go"; then
        rm -rf "$work"
        die "extracted Go toolchain failed validation"
    fi
    rm -rf "$GO_ROOT"
    mv "$work/go" "$GO_ROOT"
    rmdir "$work"
}

existing_ancestor() {
    local path=$1 parent
    [[ "$path" = /* ]] || die "installation paths must be absolute: $path"
    while [ ! -e "$path" ]; do
        parent=${path%/*}
        path=${parent:-/}
    done
    printf '%s' "$path"
}

require_free_space() {
    local path=$1 gib=$2 label=$3 free_kib
    free_kib="$(df --output=avail -k "$path" | tail -1)"
    [ "$free_kib" -ge $((gib * 1024 * 1024)) ] \
        || die "$label filesystem requires at least $gib GiB free space"
}

assert_install_space() {
    local template_path machine_path template_device machine_device
    template_path="$(existing_ancestor "$TEMPLATE_ROOT")"
    machine_path="$(existing_ancestor "$MACHINE_ROOT")"
    template_device="$(stat -c %d "$template_path")"
    machine_device="$(stat -c %d "$machine_path")"
    if [ "$template_device" = "$machine_device" ]; then
        require_free_space "$template_path" 35 "template and machine root"
    else
        require_free_space "$template_path" 5 "template root"
        require_free_space "$machine_path" 30 "machine root"
    fi
}

install_static_libuuid() {
    local marker="$TEMPLATE_ROOT/usr/lib64/.kuasar-libuuid-build-id" build_id
    case "$UTIL_LINUX_SOURCE_ARCHIVE" in
        ""|.|..|*/*) die "util-linux source archive must be a file name" ;;
    esac
    build_id="$({
        printf 'schema=%s\n' "$LIBUUID_BUILD_SCHEMA"
        printf 'srpm_url=%s\n' "$UTIL_LINUX_SRPM_URL"
        printf 'srpm_sha256=%s\n' "$UTIL_LINUX_SRPM_SHA256"
        printf 'source_archive=%s\n' "$UTIL_LINUX_SOURCE_ARCHIVE"
        printf 'tarball_sha256=%s\n' "$UTIL_LINUX_TARBALL_SHA256"
    } | sha256sum | awk '{print $1}')"
    if [ -s "$TEMPLATE_ROOT/usr/lib64/libuuid.a" ] \
        && [ "$(cat "$marker" 2>/dev/null || true)" = "$build_id" ]; then
        return
    fi

    case "$UTIL_LINUX_SRPM_URL" in
        https://mirrors.huaweicloud.com/*) ;;
        *) die "util-linux source RPM must use the configured Huawei Cloud mirror" ;;
    esac

    local srpm_name srpm work source
    srpm_name=${UTIL_LINUX_SRPM_URL##*/}
    srpm="$SOURCE_CACHE/$srpm_name"
    install -d -m 0755 "$SOURCE_CACHE"

    if [ ! -f "$srpm" ] || ! verify_sha256 "$srpm" "$UTIL_LINUX_SRPM_SHA256"; then
        rm -f "$srpm"
        log "downloading pinned util-linux source RPM from the China mirror"
        curl --fail --location --retry 3 --output "$srpm" "$UTIL_LINUX_SRPM_URL"
    fi
    verify_sha256 "$srpm" "$UTIL_LINUX_SRPM_SHA256" \
        || die "source RPM checksum mismatch: $srpm"

    work="$TEMPLATE_ROOT/tmp/kuasar-libuuid-build"
    rm -rf "$work"
    install -d -m 0755 "$work/srpm" "$work/src"
    (cd "$work/srpm" && rpm2cpio "$srpm" | cpio -idm --quiet)
    source="$work/srpm/$UTIL_LINUX_SOURCE_ARCHIVE"
    [ -f "$source" ] \
        || die "$UTIL_LINUX_SOURCE_ARCHIVE is missing from $srpm"
    verify_sha256 "$source" "$UTIL_LINUX_TARBALL_SHA256" \
        || die "util-linux source tarball checksum mismatch"
    tar -xf "$source" --strip-components=1 -C "$work/src"

    log "building static libuuid inside the runner root"
    chroot "$TEMPLATE_ROOT" /bin/bash -ceu '
        cd /tmp/kuasar-libuuid-build/src
        ./configure --prefix=/usr --libdir=/usr/lib64 \
            --disable-all-programs --enable-libuuid --enable-static \
            --disable-shared --disable-nls >/dev/null
        make -j"$(nproc)" libuuid.la >/dev/null
        install -m 0644 .libs/libuuid.a /usr/lib64/libuuid.a
    '
    printf '%s\n' "$build_id" >"$marker"
    rm -rf "$work"
    [ -s "$TEMPLATE_ROOT/usr/lib64/libuuid.a" ] \
        || die "static libuuid build did not produce /usr/lib64/libuuid.a"
}

check_host() {
    require_root
    assert_supported_host
    assert_china_repositories
    assert_e2e_tools
    local package missing=()
    for package in "${HOST_PACKAGES[@]}" "${PACKAGES[@]}"; do
        if ! dnf -q repoquery --available --qf '%{name}' "$package" | grep -qx "$package"; then
            missing+=("$package")
        fi
    done
    [ "${#missing[@]}" -eq 0 ] || die "packages unavailable from enabled mirrors: ${missing[*]}"
    if command -v modprobe >/dev/null; then
        load_required_modules
        assert_required_devices
        log "host devices are ready"
    else
        log "kmod is not installed; install will bootstrap it before validating devices"
    fi
    log "host platform, cgroup v2, China mirrors, pinned E2E tools, and package set are ready"
}

install_host_support() {
    dnf -y --setopt=install_weak_deps=False install "${HOST_PACKAGES[@]}"
    command -v systemd-nspawn >/dev/null || die "systemd-nspawn was not installed"
    ensure_go_toolchain
    if systemctl is-active --quiet kuasar-ci-network.service; then
        log "stopping the active CI network before replacing its configuration"
        systemctl stop kuasar-ci-network.service
    fi
    install -d -m 0755 \
        /usr/local/libexec /etc/systemd/system /etc/systemd/nspawn /etc/kuasar-ci \
        /etc/modules-load.d /var/cache/kuasar "$TOOL_ROOT" "$MACHINE_ROOT"
    install -m 0755 "$SCRIPT_DIR/kuasar-ci-network" /usr/local/libexec/kuasar-ci-network
    install -m 0644 "$SCRIPT_DIR/kuasar-ci-network.service" /etc/systemd/system/kuasar-ci-network.service
    install -m 0755 "$SCRIPT_DIR/kuasar-ci-bpf" /usr/local/libexec/kuasar-ci-bpf
    install -m 0644 "$SCRIPT_DIR/kuasar-ci-bpf.service" /etc/systemd/system/kuasar-ci-bpf.service
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
    cat >/etc/modules-load.d/kuasar-ci.conf <<'EOF'
bridge
overlay
tun
vhost_net
vhost_vsock
EOF
    systemctl daemon-reload
    load_required_modules
    assert_required_devices
    systemctl enable --now kuasar-ci-network.service kuasar-ci-bpf.service
    # RemainAfterExit keeps an already active oneshot from rerunning after its
    # executable is updated. Restart it so newly added slot bpffs roots exist.
    systemctl restart kuasar-ci-bpf.service
}

copy_runner_distribution() {
    local destination=$1
    [ -x "$RUNNER_SOURCE/config.sh" ] || die "runner distribution not found at $RUNNER_SOURCE"
    install -d -m 0755 "$destination"
    rsync -a --delete --delete-delay \
        --exclude '/_diag/' --exclude '/_work/' \
        --exclude '/.credentials' --exclude '/.credentials_rsaparams' \
        --exclude '/.runner' --exclude '/.runner_migrated' --exclude '/.service' \
        --exclude '/.env' --exclude '/.path' \
        --exclude "/$RUNNER_REGISTRATION_MARKER" \
        "$RUNNER_SOURCE/" "$destination/"
}

sync_root_repositories() {
    local root=$1
    local host_repos=(/etc/yum.repos.d/*.repo)
    [ -e "${host_repos[0]}" ] || die "host has no yum repository files"

    # openEuler-release installs its upstream metalink configuration into a new
    # root. Replace it with the host configuration that passed the China-mirror
    # check before every installroot transaction.
    install -d -m 0755 "$root/etc/yum.repos.d"
    rm -f "$root/etc/yum.repos.d/"*.repo
    cp -a "${host_repos[@]}" "$root/etc/yum.repos.d/"
}

reconcile_root_packages() {
    local root=$1
    sync_root_repositories "$root"
    dnf -y --installroot="$root" --releasever=24.03 \
        --setopt=install_weak_deps=False --setopt=keepcache=False \
        install "${PACKAGES[@]}"
    sync_root_repositories "$root"
}

build_template_root() {
    if [ ! -f "$TEMPLATE_ROOT/.kuasar-ci-template" ]; then
        if [ -e "$TEMPLATE_ROOT" ]; then
            log "removing interrupted template root $TEMPLATE_ROOT"
            rm -rf "$TEMPLATE_ROOT"
        fi
        install -d -m 0755 "$TEMPLATE_ROOT"
        install -d -m 0755 "$TEMPLATE_ROOT/dev" "$TEMPLATE_ROOT/proc" "$TEMPLATE_ROOT/sys"
        mknod -m 0666 "$TEMPLATE_ROOT/dev/null" c 1 3
        log "installing container packages from configured China mirrors"
        dnf -y --installroot="$TEMPLATE_ROOT" --releasever=24.03 \
            --setopt=install_weak_deps=False --setopt=keepcache=False \
            install "${BOOTSTRAP_PACKAGES[@]}"
    fi

    # Reconcile the complete package set on every install so additions to this
    # manifest also reach existing templates without rebuilding their rootfs.
    reconcile_root_packages "$TEMPLATE_ROOT"
    touch "$TEMPLATE_ROOT/.kuasar-ci-template"

    install_static_libuuid

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
Bind=/sys/fs/bpf/$machine:/sys/fs/bpf
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
Requires=systemd-modules-load.service kuasar-ci-network.service kuasar-ci-bpf.service
After=systemd-modules-load.service kuasar-ci-network.service kuasar-ci-bpf.service

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
    local slot=$1 machine root ip new_slot=0 staging
    machine="$(machine_name "$slot")"
    root="$(slot_root "$slot")"
    ip="$(slot_value "$slot" IP)"

    if [ -e "$root" ] || [ -L "$root" ]; then
        slot_root_owned "$root" \
            || die "refusing to replace or modify unowned slot root $root"
    fi
    if [ ! -f "$root/.kuasar-ci-slot" ]; then
        if [ -e "$root" ]; then
            log "removing interrupted slot root $root"
            rm -rf "$root"
        fi
        log "copying rootfs for $machine"
        staging="$(mktemp -d "$MACHINE_ROOT/.${machine}.install.XXXXXX")"
        chmod 0755 "$staging"
        printf '%s\n' "$SLOT_OWNER_ID" >"$staging/$SLOT_OWNER_MARKER"
        if ! cp -a "$TEMPLATE_ROOT/." "$staging/"; then
            rm -rf "$staging"
            die "failed to copy rootfs for $machine"
        fi
        printf '%s\n' "$SLOT_OWNER_ID" >"$staging/$SLOT_OWNER_MARKER"
        if ! mv -T -- "$staging" "$root"; then
            rm -rf "$staging"
            die "failed to install owned slot root $root"
        fi
        new_slot=1
    fi
    if [ "$new_slot" -eq 0 ]; then
        reconcile_root_packages "$root"
    fi
    copy_runner_distribution "$root/opt/actions-runner"
    install -d -m 0755 "$root/etc/docker" "$root/etc/pip" "$root/root/.cargo"
    install -m 0644 "$TEMPLATE_ROOT/etc/docker/daemon.json" "$root/etc/docker/daemon.json"
    install -m 0644 "$TEMPLATE_ROOT/etc/pip.conf" "$root/etc/pip.conf"
    install -m 0644 "$TEMPLATE_ROOT/root/.cargo/config.toml" "$root/root/.cargo/config.toml"
    install -m 0644 "$TEMPLATE_ROOT/etc/resolv.conf" "$root/etc/resolv.conf"
    install -m 0644 "$TEMPLATE_ROOT/usr/lib64/libuuid.a" "$root/usr/lib64/libuuid.a"
    install -m 0644 "$TEMPLATE_ROOT/usr/lib64/.kuasar-libuuid-build-id" \
        "$root/usr/lib64/.kuasar-libuuid-build-id"

    local machine_id template_machine_id
    machine_id="$(cat "$root/etc/machine-id" 2>/dev/null || true)"
    template_machine_id="$(cat "$TEMPLATE_ROOT/etc/machine-id" 2>/dev/null || true)"
    if [ "$new_slot" -eq 1 ] \
        || ! [[ "$machine_id" =~ ^[0-9a-f]{32}$ ]] \
        || { [ -n "$template_machine_id" ] && [ "$machine_id" = "$template_machine_id" ]; }; then
        rm -f "$root/etc/machine-id" "$root/var/lib/dbus/machine-id"
        : >"$root/etc/machine-id"
        systemd-machine-id-setup --root="$root" >/dev/null
    fi
    ln -sfn /etc/machine-id "$root/var/lib/dbus/machine-id"
    machine_id="$(cat "$root/etc/machine-id")"
    [[ "$machine_id" =~ ^[0-9a-f]{32}$ ]] \
        || die "$machine has an invalid machine ID"
    printf '%s\n' "$machine" >"$root/etc/hostname"
    cat >"$root/etc/hosts" <<EOF
127.0.0.1 localhost
127.0.1.1 $machine
::1 localhost ip6-localhost ip6-loopback
EOF
    rm -f "$root/etc/systemd/network/80-host0.network"
    cat >"$root/etc/systemd/network/10-kuasar-host0.network" <<EOF
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
    touch "$root/.kuasar-ci-slot"
}

install_slots() {
    require_root
    acquire_provision_lock
    assert_supported_host
    assert_china_repositories
    assert_host_runner_idle
    assert_slots_stopped
    assert_e2e_tools
    cleanup_stale_slot_staging
    assert_install_space
    install_host_support
    build_template_root
    local slot
    for slot in "${SLOTS[@]}"; do
        prepare_slot "$slot"
    done
    assert_distinct_slot_machine_ids
    systemctl daemon-reload
    log "slots installed but not started; register each slot next"
}

register_slot() {
    require_root
    acquire_provision_lock
    local slot=${1:-} token machine root runner
    slot_is_valid "$slot" || die "register requires slot 1 through 6"
    IFS= read -r token
    [ -n "$token" ] || die "registration token must be provided on stdin"
    machine="$(machine_name "$slot")"
    root="$(slot_root "$slot")"
    runner="$root/opt/actions-runner"
    assert_slot_root_owned "$slot"
    [ -f "$root/.kuasar-ci-slot" ] || die "slot $slot is not installed"
    systemctl is-active --quiet "systemd-nspawn@$machine.service" \
        && die "$machine must be stopped before registration"
    runner_registration_complete "$root" && die "$machine is already registered"

    rm -f "$runner/.credentials" \
        "$runner/.credentials_rsaparams" \
        "$runner/.runner" \
        "$runner/.runner_migrated" \
        "$runner/.service" \
        "$runner/.path" \
        "$runner/$RUNNER_REGISTRATION_MARKER"
    printf '%s\n' "$token" \
        | RUNNER_ALLOW_RUNASROOT=1 systemd-nspawn --quiet --pipe --settings=no --register=no \
            --directory="$root" --setenv=RUNNER_ALLOW_RUNASROOT=1 \
            /bin/bash -ceu '
                cd /opt/actions-runner
                IFS= read -r token
                [ -n "$token" ]
                export ACTIONS_RUNNER_INPUT_TOKEN="$token"
                unset token
                exec ./config.sh --unattended --replace --disableupdate \
                    --url "$1" --name "$2" --runnergroup "$3" \
                    --labels "$4" --work _work
            ' register-runner "https://github.com/$ORG" "bms-tmp-kuasar-e2e-$slot" \
            "$RUNNER_GROUP" "kuasar-e2e,kvm,cgroup-v2,bms-slot-$slot"
    [ -s "$runner/.runner" ] || die "$machine registration did not produce runner metadata"
    printf '/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin\n' \
        >"$runner/.path"
    systemctl --root="$root" enable actions-runner.service >/dev/null
    touch "$runner/$RUNNER_REGISTRATION_MARKER"
    runner_registration_complete "$root" \
        || die "$machine registration did not complete"
    log "$machine registered"
}

start_slots() {
    require_root
    acquire_provision_lock
    assert_host_runner_idle
    local slot machine root unit was_active was_enabled
    local -a startup_records=()
    for slot in "${SLOTS[@]}"; do
        machine="$(machine_name "$slot")"
        root="$(slot_root "$slot")"
        assert_slot_root_owned "$slot"
        runner_registration_complete "$root" || die "$machine registration is incomplete"
    done
    assert_distinct_slot_machine_ids

    systemctl start kuasar-ci-network.service
    for slot in "${SLOTS[@]}"; do
        machine="$(machine_name "$slot")"
        unit="systemd-nspawn@$machine.service"
        was_active=no
        was_enabled=no
        if systemctl is-active --quiet "$unit"; then
            was_active=yes
        fi
        if systemctl is-enabled --quiet "$unit"; then
            was_enabled=yes
        fi
        startup_records+=("$unit|$was_active|$was_enabled")
        if ! systemctl enable --now "$unit"; then
            rollback_slot_startup "${startup_records[@]}"
            die "failed to start $machine"
        fi
    done
    for slot in "${SLOTS[@]}"; do
        if ! wait_slot_ready "$slot"; then
            rollback_slot_startup "${startup_records[@]}"
            die "$(machine_name "$slot") did not become network, Docker, and runner ready within 60s"
        fi
    done
}

rollback_slot_startup() {
    local record unit was_active was_enabled
    for record in "$@"; do
        IFS='|' read -r unit was_active was_enabled <<<"$record"
        if [ "$was_active" = no ] && ! systemctl stop "$unit"; then
            log "rollback could not stop $unit"
        fi
        if [ "$was_enabled" = no ] && ! systemctl disable "$unit"; then
            log "rollback could not disable $unit"
        fi
    done
}

stop_slots() {
    require_root
    acquire_provision_lock
    local slot machine unit
    for slot in "${SLOTS[@]}"; do
        assert_slot_root_owned "$slot"
    done
    for slot in "${SLOTS[@]}"; do
        machine="$(machine_name "$slot")"
        unit="systemd-nspawn@$machine.service"
        if systemctl is-active --quiet "$unit" || systemctl is-enabled --quiet "$unit"; then
            systemctl disable --now "$unit" \
                || die "failed to stop $machine"
        fi
        if systemctl is-active --quiet "$unit"; then
            die "$machine is still active after stop"
        fi
    done
}

run_in_slot() {
    local slot=$1
    shift
    systemd-run --quiet --wait --pipe --collect --machine="$(machine_name "$slot")" "$@"
}

wait_slot_ready() {
    local slot=$1 machine attempt
    machine="$(machine_name "$slot")"
    for attempt in $(seq 1 60); do
        if [ "$(machinectl show "$machine" -p State --value 2>/dev/null || true)" = running ] \
            && run_in_slot "$slot" /bin/bash -ceu '
                systemctl is-active --quiet systemd-networkd.service
                systemctl is-active --quiet docker.service
                systemctl is-active --quiet actions-runner.service
                invocation_id="$(systemctl show actions-runner.service \
                    --property=InvocationID --value)"
                [[ "$invocation_id" =~ ^[0-9a-f]{32}$ ]]
                journalctl "_SYSTEMD_INVOCATION_ID=$invocation_id" --no-pager -o cat \
                    | grep -Fq "Listening for Jobs"
                ip route get 223.5.5.5 >/dev/null
            ' >/dev/null 2>&1; then
            return
        fi
        sleep 1
    done
    return 1
}

verify_slots() {
    require_root
    local slot machine
    for slot in "${SLOTS[@]}"; do
        machine="$(machine_name "$slot")"
        assert_slot_root_owned "$slot"
        runner_registration_complete "$(slot_root "$slot")" \
            || die "$machine registration is incomplete"
        wait_slot_ready "$slot" \
            || die "$machine did not become network, Docker, and runner ready within 60s"
        [ "$(machinectl show "$machine" -p State --value)" = running ] || die "$machine is not running"
        run_in_slot "$slot" /bin/bash -ceu '
            [ "$(cat /proc/1/comm)" = systemd ]
            [ "$(stat -fc %T /sys/fs/cgroup)" = cgroup2fs ]
            test -r /dev/kvm -a -w /dev/kvm
            test -r /dev/net/tun -a -w /dev/net/tun
            test -r /dev/vhost-net -a -w /dev/vhost-net
            test -r /dev/vhost-vsock -a -w /dev/vhost-vsock
            systemctl is-active --quiet docker.service
            docker info >/dev/null
            test -s /usr/lib64/libuuid.a
            libstdcpp=$(gcc -print-file-name=libstdc++.a)
            test "$libstdcpp" != libstdc++.a
            test -s "$libstdcpp"
            test -e /usr/lib64/liblz4.so
            test -e /usr/lib64/libsnappy.so
            test -x /usr/bin/time
            [ "$(systemctl show actions-runner.service --property=RefuseManualStop --value)" = yes ]
            [ "$(systemctl show actions-runner.service --property=StartLimitIntervalUSec --value)" = 0 ]
            [ "$(/usr/local/go/bin/go version)" = "go version go1.26.5 linux/amd64" ]
            [ "$(/usr/local/go/bin/go tool compile -V=full)" = "compile version go1.26.5" ]
            redis-server --version >/dev/null
            ip route get 223.5.5.5 >/dev/null
            curl --fail --silent --show-error --connect-timeout 5 --max-time 20 https://goproxy.cn >/dev/null
            mountpoint -q /sys/fs/bpf
            [ "$(stat -fc %T /sys/fs/bpf)" = bpf_fs ]
            ip netns add kuasar-isolation-probe
            ip netns del kuasar-isolation-probe
            systemctl is-active --quiet actions-runner.service
        '
    done

    assert_distinct_slot_machine_ids

    local left right leader i j probe
    local -a mount_namespaces=() network_namespaces=() cgroups=() bpf_roots=()
    for slot in "${SLOTS[@]}"; do
        probe="/run/kuasar-slot-$slot-isolation-probe"
        run_in_slot "$slot" /usr/bin/touch "$probe"
        for right in "${SLOTS[@]}"; do
            [ "$right" = "$slot" ] && continue
            if run_in_slot "$right" /usr/bin/test -e "$probe"; then
                die "slot $right can see slot $slot private /run"
            fi
        done
        run_in_slot "$slot" /usr/bin/rm -f "$probe"

        leader="$(machinectl show "$(machine_name "$slot")" -p Leader --value)"
        mount_namespaces[$slot]="$(readlink "/proc/$leader/ns/mnt")"
        network_namespaces[$slot]="$(readlink "/proc/$leader/ns/net")"
        cgroups[$slot]="$(cat "/proc/$leader/cgroup")"
        bpf_roots[$slot]="$(run_in_slot "$slot" /usr/bin/findmnt -n -o FSROOT --target /sys/fs/bpf)"
    done
    for ((i = 0; i < ${#SLOTS[@]}; i++)); do
        left=${SLOTS[$i]}
        for ((j = i + 1; j < ${#SLOTS[@]}; j++)); do
            right=${SLOTS[$j]}
            [ "${mount_namespaces[$left]}" != "${mount_namespaces[$right]}" ] \
                || die "slots $left and $right share a mount namespace"
            [ "${network_namespaces[$left]}" != "${network_namespaces[$right]}" ] \
                || die "slots $left and $right share a network namespace"
            [ "${cgroups[$left]}" != "${cgroups[$right]}" ] \
                || die "slots $left and $right share a host cgroup"
            [ "${bpf_roots[$left]}" != "${bpf_roots[$right]}" ] \
                || die "slots $left and $right share a bpffs root"
        done
    done
    log "slot isolation checks passed"
}

status_slots() {
    local slot machine
    systemctl --no-pager --full status kuasar-ci-network.service || true
    for slot in "${SLOTS[@]}"; do
        machine="$(machine_name "$slot")"
        systemctl --no-pager --full status "systemd-nspawn@$machine.service" || true
        machinectl status "$machine" || true
    done
}

usage() {
    cat <<'EOF'
usage: provision.sh install
       provision.sh check
       provision.sh register 1..6  # registration token on stdin
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
