#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d)"
SYSTEM_PATH="$PATH"
cleanup() {
    chmod -R u+w "$TMP" 2>/dev/null || true
    rm -rf "$TMP"
}
trap cleanup EXIT

fail() {
    echo "test-ci-tools: $*" >&2
    exit 1
}

setup_workspace() {
    local root=$1
    mkdir -p "$root/guest-runtime/native-deps/deps"
    printf 'envd target fixture\n' >"$root/guest-runtime/native-deps/Makefile"
    printf 'common fixture\n' >"$root/guest-runtime/native-deps/deps/common.sh"
    printf 'build envd fixture\n' >"$root/guest-runtime/native-deps/deps/build-envd.sh"
}

setup_erofs_workspace() {
    local root=$1
    mkdir -p "$root/guest-runtime/native-deps/deps"
    printf 'erofs target fixture\n' >"$root/guest-runtime/native-deps/Makefile"
    printf 'common fixture\n' >"$root/guest-runtime/native-deps/deps/common.sh"
    printf 'build erofs fixture\n' >"$root/guest-runtime/native-deps/deps/build-erofs.sh"
}

setup_vmlinux_workspace() {
    local root=$1
    mkdir -p "$root/guest-runtime/native-deps/deps/vmlinux" \
        "$root/guest-runtime/native-deps/deps/linux-patches"
    cat >"$root/guest-runtime/native-deps/Makefile" <<'EOF'
TARGET_ARCH ?= x86_64
VMLINUX_BIN := $(abspath bin/$(TARGET_ARCH)/vmlinux)
VMLINUX_INPUTS := deps/build-vmlinux.sh deps/common.sh \
    deps/vmlinux/test.config \
    deps/linux-patches \
    $(wildcard deps/linux-patches/*.patch)

.PHONY: vmlinux
vmlinux: $(VMLINUX_BIN)
$(VMLINUX_BIN): $(VMLINUX_INPUTS)
	@mkdir -p "$(dir $@)"
	@printf 'build\n' >>"$(FAKE_BUILD_COUNTER)"
	@printf 'fake-vmlinux\n' >"$@"
	@chmod +x "$@"
EOF
    printf 'common fixture\n' >"$root/guest-runtime/native-deps/deps/common.sh"
    printf 'build vmlinux fixture\n' >"$root/guest-runtime/native-deps/deps/build-vmlinux.sh"
    printf 'config fixture\n' >"$root/guest-runtime/native-deps/deps/vmlinux/test.config"
    printf 'patch fixture\n' >"$root/guest-runtime/native-deps/deps/linux-patches/test.patch"
}

setup_cloud_hypervisor_workspace() {
    local root=$1
    mkdir -p "$root/sandboxer/native-deps/deps/ch-patches"
    printf 'cloud-hypervisor target fixture\n' >"$root/sandboxer/native-deps/Makefile"
    printf 'common fixture\n' >"$root/sandboxer/native-deps/deps/common.sh"
    printf 'build cloud-hypervisor fixture\n' \
        >"$root/sandboxer/native-deps/deps/build-cloud-hypervisor.sh"
    printf 'patch fixture\n' >"$root/sandboxer/native-deps/deps/ch-patches/test.patch"
}

setup_rocksdb_workspace() {
    local root=$1
    mkdir -p "$root/accelerator/deps"
    printf 'rocksdb target fixture\n' >"$root/accelerator/Makefile"
    printf 'common fixture\n' >"$root/accelerator/deps/common.sh"
    printf 'build rocksdb fixture\n' >"$root/accelerator/deps/build-rocksdb.sh"
}

mkdir -p "$TMP/bin"
cat >"$TMP/bin/make" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
workdir=""
arch=x86_64
while [ "$#" -gt 0 ]; do
    case "$1" in
        -C) workdir=$2; shift 2 ;;
        TARGET_ARCH=*) arch=${1#*=}; shift ;;
        *) shift ;;
    esac
done
[ -n "$workdir" ]
exec 9>>"$FAKE_BUILD_COUNTER"
flock 9
printf 'build\n' >&9
flock -u 9
sleep "${FAKE_BUILD_SLEEP:-0}"
mkdir -p "$workdir/bin/$arch"
printf 'fake-envd\n' >"$workdir/bin/$arch/envd"
chmod +x "$workdir/bin/$arch/envd"
EOF
chmod +x "$TMP/bin/make"

cat >"$TMP/bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
    version) printf 'go version go1.test linux/amd64\n' ;;
    env)
        shift
        for name in "$@"; do
            case "$name" in
                GOFLAGS) printf '%s\n' "${GOFLAGS:-}" ;;
                GOAMD64) printf '%s\n' "${GOAMD64:-v1}" ;;
                GOARM64) printf '%s\n' "${GOARM64:-v8.0}" ;;
                *) printf '%s=test\n' "$name" ;;
            esac
        done
        ;;
    *) exit 2 ;;
esac
EOF
chmod +x "$TMP/bin/go"

timings="$TMP/timings.tsv"
KUASAR_CI_TIMINGS="$timings" "$SCRIPT_DIR/ci-timed.sh" fixture/timing bash -c 'sleep 0.01'
[ "$(wc -l <"$timings")" -eq 2 ] || fail "timing file must contain one header and one row"
awk -F '\t' 'NR == 1 && NF == 10 && $1 == "stage" { ok=1 } END { exit !ok }' "$timings" \
    || fail "timing header is malformed"
awk -F '\t' 'NR == 2 && NF == 10 && $1 == "fixture/timing" && $10 == 0 { ok=1 } END { exit !ok }' "$timings" \
    || fail "timing row is malformed"

parallel_timings="$TMP/timings-parallel.tsv"
timing_pids=()
for i in $(seq 1 8); do
    KUASAR_CI_TIMINGS="$parallel_timings" "$SCRIPT_DIR/ci-timed.sh" \
        "fixture/parallel-$i" bash -c 'sleep 0.02' &
    timing_pids+=("$!")
done
for pid in "${timing_pids[@]}"; do
    wait "$pid"
done
awk -F '\t' '
    NR == 1 {
        if (NF != 10 || $1 != "stage") {
            exit 1
        }
        next
    }
    NF != 10 || $1 !~ /^fixture\/parallel-[1-8]$/ || $10 != 0 {
        exit 1
    }
    !seen[$1]++ {
        stages++
    }
    END {
        if (NR != 9 || stages != 8) {
            exit 1
        }
    }
' "$parallel_timings" || fail "parallel timing rows are malformed or incomplete"

workspace="$TMP/workspace"
cache="$TMP/cache"
counter="$TMP/build-counter"
metrics="$TMP/cache-metrics.tsv"
setup_workspace "$workspace"

plain_key="$(env PATH="$TMP/bin:$PATH" KUASAR_WORKSPACE_ROOT="$workspace" \
    "$SCRIPT_DIR/native-cache.sh" key envd | cut -f2)"
tagged_key="$(env PATH="$TMP/bin:$PATH" GOFLAGS=-tags=ci KUASAR_WORKSPACE_ROOT="$workspace" \
    "$SCRIPT_DIR/native-cache.sh" key envd | cut -f2)"
[ "$plain_key" != "$tagged_key" ] || fail "effective GOFLAGS did not invalidate the envd key"
microarch_key="$(env PATH="$TMP/bin:$PATH" GOAMD64=v3 KUASAR_WORKSPACE_ROOT="$workspace" \
    "$SCRIPT_DIR/native-cache.sh" key envd | cut -f2)"
[ "$plain_key" != "$microarch_key" ] || fail "GOAMD64 did not invalidate the envd key"

cross_workspace="$TMP/cross-workspace"
setup_erofs_workspace "$cross_workspace"
for tool in gcc g++ ar ld; do
    cat >"$TMP/bin/custom-$tool" <<EOF
#!/usr/bin/env bash
printf 'custom-$tool v1\\n'
EOF
    chmod +x "$TMP/bin/custom-$tool"
done
cross_key_v1="$(env PATH="$TMP/bin:$PATH" CROSS_PREFIX="$TMP/bin/custom-" \
    KUASAR_WORKSPACE_ROOT="$cross_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key erofs | cut -f2)"
printf '# toolchain update\n' >>"$TMP/bin/custom-gcc"
cross_key_v2="$(env PATH="$TMP/bin:$PATH" CROSS_PREFIX="$TMP/bin/custom-" \
    KUASAR_WORKSPACE_ROOT="$cross_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key erofs | cut -f2)"
[ "$cross_key_v1" != "$cross_key_v2" ] || fail "custom cross compiler did not invalidate the key"

pkg_workspace="$TMP/pkg-workspace"
pkg_fixture="$TMP/pkg-fixture"
pkg_bin="$TMP/pkg-bin"
setup_erofs_workspace "$pkg_workspace"
mkdir -p "$pkg_fixture/lib" "$pkg_bin"
printf 'Name: uuid fixture\nVersion: 1\n' >"$pkg_fixture/uuid.pc"
printf 'static uuid v1\n' >"$pkg_fixture/lib/libuuid.a"
cat >"$pkg_bin/pkg-config" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
: "${PKG_CONFIG_FIXTURE:?}"
case "${1:-}" in
    --version) printf 'pkg-config fixture 1\n' ;;
    --exists) exit 0 ;;
    --path) printf '%s/uuid.pc\n' "$PKG_CONFIG_FIXTURE" ;;
    --modversion) printf '1\n' ;;
    --cflags) printf '%s\n' "-I$PKG_CONFIG_FIXTURE/include" ;;
    --libs) printf '%s\n' "-L$PKG_CONFIG_FIXTURE/lib -luuid" ;;
    --variable=libdir) printf '%s/lib\n' "$PKG_CONFIG_FIXTURE" ;;
    *) exit 2 ;;
esac
EOF
chmod +x "$pkg_bin/pkg-config"
pkg_key_v1="$(env PATH="$TMP/bin:$PATH" PKG_CONFIG="$pkg_bin/pkg-config" \
    PKG_CONFIG_FIXTURE="$pkg_fixture" KUASAR_WORKSPACE_ROOT="$pkg_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key erofs | cut -f2)"
printf 'static uuid v2\n' >"$pkg_fixture/lib/libuuid.a"
pkg_key_v2="$(env PATH="$TMP/bin:$PATH" PKG_CONFIG="$pkg_bin/pkg-config" \
    PKG_CONFIG_FIXTURE="$pkg_fixture" KUASAR_WORKSPACE_ROOT="$pkg_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key erofs | cut -f2)"
[ "$pkg_key_v1" != "$pkg_key_v2" ] \
    || fail "pkg-config selected library did not invalidate the erofs key"

vmlinux_workspace="$TMP/vmlinux-workspace"
setup_vmlinux_workspace "$vmlinux_workspace"
vmlinux_key_plain="$(env PATH="$TMP/bin:$PATH" KUASAR_WORKSPACE_ROOT="$vmlinux_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key vmlinux | cut -f2)"
vmlinux_key_versioned="$(env PATH="$TMP/bin:$PATH" LOCALVERSION=-ci KBUILD_BUILD_USER=builder \
    KUASAR_WORKSPACE_ROOT="$vmlinux_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key vmlinux | cut -f2)"
[ "$vmlinux_key_plain" != "$vmlinux_key_versioned" ] \
    || fail "Kbuild overrides did not invalidate the vmlinux key"

vmlinux_cache="$TMP/vmlinux-cache"
vmlinux_counter="$TMP/vmlinux-build-counter"
vmlinux_metrics="$TMP/vmlinux-cache-metrics.tsv"
env PATH="$SYSTEM_PATH" FAKE_BUILD_COUNTER="$vmlinux_counter" \
    KUASAR_WORKSPACE_ROOT="$vmlinux_workspace" KUASAR_NATIVE_CACHE_ROOT="$vmlinux_cache" \
    KUASAR_NATIVE_CACHE_METRICS="$vmlinux_metrics" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build vmlinux
[ "$(wc -l <"$vmlinux_counter")" -eq 1 ] || fail "cold vmlinux cache must build once"
env PATH="$SYSTEM_PATH" FAKE_BUILD_COUNTER="$vmlinux_counter" \
    make -C "$vmlinux_workspace/guest-runtime/native-deps" \
    TARGET_ARCH=x86_64 vmlinux >/dev/null
[ "$(wc -l <"$vmlinux_counter")" -eq 1 ] \
    || fail "cold vmlinux cache restore left the Make target stale"

rm -f "$vmlinux_workspace/guest-runtime/native-deps/bin/x86_64/vmlinux"
env PATH="$SYSTEM_PATH" FAKE_BUILD_COUNTER="$vmlinux_counter" \
    KUASAR_WORKSPACE_ROOT="$vmlinux_workspace" KUASAR_NATIVE_CACHE_ROOT="$vmlinux_cache" \
    KUASAR_NATIVE_CACHE_METRICS="$vmlinux_metrics" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build vmlinux
env PATH="$SYSTEM_PATH" FAKE_BUILD_COUNTER="$vmlinux_counter" \
    make -C "$vmlinux_workspace/guest-runtime/native-deps" \
    TARGET_ARCH=x86_64 vmlinux >/dev/null
[ "$(wc -l <"$vmlinux_counter")" -eq 1 ] \
    || fail "hot vmlinux cache restore left the Make target stale"
grep -q $'vmlinux\thit\t' "$vmlinux_metrics" || fail "hot vmlinux cache metric is missing"

cloud_workspace="$TMP/cloud-workspace"
cargo_home="$TMP/cargo-home"
setup_cloud_hypervisor_workspace "$cloud_workspace"
mkdir -p "$cargo_home"
printf '[build]\nrustflags = ["-Cdebuginfo=0"]\n' >"$cargo_home/config.toml"
cloud_key_plain="$(env PATH="$TMP/bin:$PATH" CARGO_HOME="$cargo_home" \
    KUASAR_WORKSPACE_ROOT="$cloud_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key cloud-hypervisor | cut -f2)"
cloud_key_encoded="$(env PATH="$TMP/bin:$PATH" CARGO_HOME="$cargo_home" \
    CARGO_ENCODED_RUSTFLAGS=-Ctarget-cpu=x86-64-v3 \
    KUASAR_WORKSPACE_ROOT="$cloud_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key cloud-hypervisor | cut -f2)"
[ "$cloud_key_plain" != "$cloud_key_encoded" ] \
    || fail "CARGO_ENCODED_RUSTFLAGS did not invalidate the Cloud Hypervisor key"
cloud_key_target_flags="$(env PATH="$TMP/bin:$PATH" CARGO_HOME="$cargo_home" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS=-Ctarget-cpu=x86-64-v3 \
    KUASAR_WORKSPACE_ROOT="$cloud_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key cloud-hypervisor | cut -f2)"
[ "$cloud_key_plain" != "$cloud_key_target_flags" ] \
    || fail "target-specific Cargo rustflags did not invalidate the Cloud Hypervisor key"
printf '[build]\nrustflags = ["-Cdebuginfo=1"]\n' >"$cargo_home/config.toml"
cloud_key_config="$(env PATH="$TMP/bin:$PATH" CARGO_HOME="$cargo_home" \
    KUASAR_WORKSPACE_ROOT="$cloud_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key cloud-hypervisor | cut -f2)"
[ "$cloud_key_plain" != "$cloud_key_config" ] \
    || fail "Cargo config did not invalidate the Cloud Hypervisor key"

rocks_workspace="$TMP/rocks-workspace"
setup_rocksdb_workspace "$rocks_workspace"
for tool in clang clang++; do
    cat >"$TMP/bin/$tool" <<EOF
#!/usr/bin/env bash
printf '$tool v1\\n'
EOF
    chmod +x "$TMP/bin/$tool"
done
rocks_key_v1="$(env PATH="$TMP/bin:$PATH" CC=clang CXX=clang++ \
    KUASAR_WORKSPACE_ROOT="$rocks_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key rocksdb | cut -f2)"
printf '# compiler update\n' >>"$TMP/bin/clang"
rocks_key_v2="$(env PATH="$TMP/bin:$PATH" CC=clang CXX=clang++ \
    KUASAR_WORKSPACE_ROOT="$rocks_workspace" \
    "$SCRIPT_DIR/native-cache.sh" key rocksdb | cut -f2)"
[ "$rocks_key_v1" != "$rocks_key_v2" ] \
    || fail "selected RocksDB compiler did not invalidate the key"

env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$counter" \
    KUASAR_WORKSPACE_ROOT="$workspace" KUASAR_NATIVE_CACHE_ROOT="$cache" \
    KUASAR_NATIVE_CACHE_METRICS="$metrics" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd
[ "$(wc -l <"$counter")" -eq 1 ] || fail "cold cache must build once"

rm -f "$workspace/guest-runtime/native-deps/bin/x86_64/envd"
env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$counter" \
    KUASAR_WORKSPACE_ROOT="$workspace" KUASAR_NATIVE_CACHE_ROOT="$cache" \
    KUASAR_NATIVE_CACHE_METRICS="$metrics" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd
[ "$(wc -l <"$counter")" -eq 1 ] || fail "hot cache rebuilt the component"
grep -q $'envd\thit\t' "$metrics" || fail "hot cache metric is missing"

printf 'changed input\n' >>"$workspace/guest-runtime/native-deps/deps/build-envd.sh"
env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$counter" \
    KUASAR_WORKSPACE_ROOT="$workspace" KUASAR_NATIVE_CACHE_ROOT="$cache" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd
[ "$(wc -l <"$counter")" -eq 2 ] || fail "input change did not invalidate the cache"

current_key="$(env PATH="$TMP/bin:$PATH" KUASAR_WORKSPACE_ROOT="$workspace" \
    "$SCRIPT_DIR/native-cache.sh" key envd | cut -f2)"
entry="$cache/v1/x86_64/envd/$current_key"
chmod u+w "$entry/payload.tar"
printf 'tampered\n' >>"$entry/payload.tar"
if env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$counter" \
    KUASAR_WORKSPACE_ROOT="$workspace" KUASAR_NATIVE_CACHE_ROOT="$cache" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd >/dev/null 2>&1; then
    fail "tampered payload was accepted"
fi

concurrent_cache="$TMP/concurrent-cache"
concurrent_counter="$TMP/concurrent-counter"
setup_workspace "$TMP/workspace-a"
setup_workspace "$TMP/workspace-b"
env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$concurrent_counter" FAKE_BUILD_SLEEP=0.5 \
    KUASAR_WORKSPACE_ROOT="$TMP/workspace-a" KUASAR_NATIVE_CACHE_ROOT="$concurrent_cache" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd >"$TMP/a.log" 2>&1 &
pid_a=$!
env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$concurrent_counter" FAKE_BUILD_SLEEP=0.5 \
    KUASAR_WORKSPACE_ROOT="$TMP/workspace-b" KUASAR_NATIVE_CACHE_ROOT="$concurrent_cache" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd >"$TMP/b.log" 2>&1 &
pid_b=$!
wait "$pid_a"
wait "$pid_b"
[ "$(wc -l <"$concurrent_counter")" -eq 1 ] || fail "same-key concurrent misses built more than once"
grep -q 'miss-built' "$TMP/a.log" "$TMP/b.log" || fail "concurrent miss was not recorded"
grep -q 'hit-after-wait' "$TMP/a.log" "$TMP/b.log" || fail "concurrent waiter did not restore the published entry"

staging_cache="$TMP/staging-cache"
staging_workspace="$TMP/staging-workspace"
staging_counter="$TMP/staging-counter"
setup_workspace "$staging_workspace"
orphan_stage="$staging_cache/v1/.tmp/envd.orphan.test"
orphan_lock="$staging_cache/v1/.locks/staging/${orphan_stage##*/}.lock"
mkdir -p "$orphan_stage/payload" "$(dirname "$orphan_lock")"
exec 8>"$orphan_lock"
flock 8
env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$staging_counter" \
    KUASAR_WORKSPACE_ROOT="$staging_workspace" KUASAR_NATIVE_CACHE_ROOT="$staging_cache" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd >/dev/null
[ -d "$orphan_stage" ] || fail "active native staging directory was pruned"
flock -u 8
exec 8>&-
env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$staging_counter" \
    KUASAR_WORKSPACE_ROOT="$staging_workspace" KUASAR_NATIVE_CACHE_ROOT="$staging_cache" \
    "$SCRIPT_DIR/native-cache.sh" restore-or-build envd >/dev/null
[ ! -e "$orphan_stage" ] || fail "orphaned native staging directory was not pruned"
[ ! -e "$orphan_lock" ] || fail "orphaned native staging lock was not pruned"

retained_cache="$TMP/retained-cache"
retained_workspace="$TMP/retained-workspace"
retained_counter="$TMP/retained-counter"
setup_workspace "$retained_workspace"
for n in 1 2 3 4; do
    printf 'input-%s\n' "$n" >"$retained_workspace/guest-runtime/native-deps/deps/build-envd.sh"
    env PATH="$TMP/bin:$PATH" FAKE_BUILD_COUNTER="$retained_counter" \
        KUASAR_WORKSPACE_ROOT="$retained_workspace" KUASAR_NATIVE_CACHE_ROOT="$retained_cache" \
        KUASAR_NATIVE_CACHE_MAX_ENTRIES=2 KUASAR_NATIVE_CACHE_MIN_AGE_SECONDS=0 \
        "$SCRIPT_DIR/native-cache.sh" restore-or-build envd >/dev/null
done
[ "$(find "$retained_cache/v1/x86_64/envd" -mindepth 1 -maxdepth 1 -type d | wc -l)" -eq 2 ] \
    || fail "native cache retention limit was not enforced"

source_cache="$TMP/source-cache"
mkdir -p "$source_cache"
for n in 1 2 3 4; do
    printf 'archive-%s\n' "$n" >"$source_cache/$n.tar.gz"
    printf 'checksum-%s\n' "$n" >"$source_cache/$n.tar.gz.sha256"
    touch -d "2026-01-0$n 00:00:00 UTC" "$source_cache/$n.tar.gz" "$source_cache/$n.tar.gz.sha256"
done
touch "$source_cache/stale.lock"
printf 'orphan\n' >"$source_cache/orphan.tar.gz.sha256"
KUASAR_SOURCE_CACHE_MAX_ENTRIES=2 \
    "$SCRIPT_DIR/source-cache-prune.sh" "$source_cache" "$source_cache/1.tar.gz"
[ -f "$source_cache/1.tar.gz" ] || fail "source cache pruned the protected archive"
[ -f "$source_cache/4.tar.gz" ] || fail "source cache pruned the newest archive"
[ "$(find "$source_cache" -maxdepth 1 -type f -name '*.tar.gz' | wc -l)" -eq 2 ] \
    || fail "source cache retention limit was not enforced"
[ ! -e "$source_cache/stale.lock" ] || fail "legacy source cache lock was not pruned"
[ ! -e "$source_cache/orphan.tar.gz.sha256" ] || fail "orphan source checksum was not pruned"

echo "test-ci-tools: PASS"
