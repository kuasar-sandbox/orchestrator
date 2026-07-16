#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${KUASAR_WORKSPACE_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
CACHE_ROOT="${KUASAR_NATIVE_CACHE_ROOT:-/var/cache/kuasar/native}"
CACHE_SCHEMA="v1"
METRICS_FILE="${KUASAR_NATIVE_CACHE_METRICS:-}"

TARGET_ARCH="${TARGET_ARCH:-$(uname -m)}"
case "$TARGET_ARCH" in
    amd64) TARGET_ARCH=x86_64 ;;
    arm64) TARGET_ARCH=aarch64 ;;
    x86_64|aarch64) ;;
    *) echo "native-cache: unsupported TARGET_ARCH=$TARGET_ARCH" >&2; exit 2 ;;
esac

ACTIVE_STAGE=""
cleanup() {
    if [ -n "$ACTIVE_STAGE" ] && [ -d "$ACTIVE_STAGE" ]; then
        rm -rf "$ACTIVE_STAGE"
    fi
}
trap cleanup EXIT

die() {
    echo "native-cache: $*" >&2
    exit 1
}

log() {
    echo "native-cache: $*" >&2
}

now_ns() {
    date +%s%N
}

elapsed_seconds() {
    awk -v start="$1" -v end="$2" 'BEGIN { printf "%.3f", (end - start) / 1000000000 }'
}

record_metric() {
    local component=$1 status=$2 key=$3 elapsed=$4
    [ -n "$METRICS_FILE" ] || return 0
    mkdir -p "$(dirname "$METRICS_FILE")"
    exec {metrics_fd}>>"$METRICS_FILE"
    flock "$metrics_fd"
    if [ ! -s "$METRICS_FILE" ]; then
        printf 'component\tstatus\tinput_hash\telapsed_seconds\n' >&"$metrics_fd"
    fi
    printf '%s\t%s\t%s\t%s\n' "$component" "$status" "$key" "$elapsed" >&"$metrics_fd"
    flock -u "$metrics_fd"
    exec {metrics_fd}>&-
}

required_file() {
    local path=$1
    [ -f "$WORKSPACE_ROOT/$path" ] || die "missing cache input: $path"
    printf '%s\0' "$WORKSPACE_ROOT/$path"
}

component_input_paths() {
    local component=$1
    case "$component" in
        vmlinux)
            required_file guest-runtime/native-deps/Makefile
            required_file guest-runtime/native-deps/deps/common.sh
            required_file guest-runtime/native-deps/deps/build-vmlinux.sh
            find "$WORKSPACE_ROOT/guest-runtime/native-deps/deps/vmlinux" -type f -name '*.config' -print0
            find "$WORKSPACE_ROOT/guest-runtime/native-deps/deps/linux-patches" -type f -name '*.patch' -print0
            ;;
        erofs)
            required_file guest-runtime/native-deps/Makefile
            required_file guest-runtime/native-deps/deps/common.sh
            required_file guest-runtime/native-deps/deps/build-erofs.sh
            ;;
        envd)
            required_file guest-runtime/native-deps/Makefile
            required_file guest-runtime/native-deps/deps/common.sh
            required_file guest-runtime/native-deps/deps/build-envd.sh
            ;;
        rocksdb)
            required_file accelerator/Makefile
            required_file accelerator/deps/common.sh
            required_file accelerator/deps/build-rocksdb.sh
            ;;
        cloud-hypervisor)
            required_file sandboxer/native-deps/Makefile
            required_file sandboxer/native-deps/deps/common.sh
            required_file sandboxer/native-deps/deps/build-cloud-hypervisor.sh
            find "$WORKSPACE_ROOT/sandboxer/native-deps/deps/ch-patches" -type f -name '*.patch' -print0
            ;;
        *) die "unknown component: $component" ;;
    esac
}

print_env_input() {
    local name=$1 value=${!1-}
    printf 'env\t%s\t%q\n' "$name" "$value"
}

tool_identity() {
    local label=$1
    shift
    local executable=$1 path output output_hash binary_hash=missing
    path="$(command -v "$executable" 2>/dev/null || true)"
    if [ -z "$path" ]; then
        printf 'tool\t%s\tmissing\tmissing\n' "$label"
        return
    fi
    output="$({ "$@"; } 2>&1 || true)"
    output_hash="$(printf '%s' "$output" | sha256sum | awk '{print $1}')"
    path="$(readlink -f "$path")"
    if [ -f "$path" ]; then
        binary_hash="$(sha256sum "$path" | awk '{print $1}')"
    fi
    printf 'tool\t%s\t%s\t%s\n' "$label" "$output_hash" "$binary_hash"
}

package_identities() {
    local package result
    if command -v rpm >/dev/null 2>&1; then
        for package in "$@"; do
            result="$(rpm -q --qf '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}' "$package" 2>/dev/null || printf '%s-missing' "$package")"
            printf 'package\t%s\n' "$result"
        done
    elif command -v dpkg-query >/dev/null 2>&1; then
        for package in "$@"; do
            result="$(dpkg-query -W -f='${Package}=${Version}:${Architecture}' "$package" 2>/dev/null || printf '%s-missing' "$package")"
            printf 'package\t%s\n' "$result"
        done
    fi
}

component_environment() {
    local component=$1 name
    local names=(SOURCE_DATE_EPOCH)
    case "$component" in
        vmlinux)
            names+=(LINUX_TARBALL LINUX_TARBALL_SHA256 LINUX_BASE_TAG CROSS_PREFIX KERNEL_ARCH KCFLAGS)
            ;;
        erofs)
            names+=(EROFS_TARBALL EROFS_TARBALL_SHA256 CROSS_PREFIX CFLAGS CXXFLAGS LDFLAGS)
            ;;
        envd)
            names+=(ENVD_TARBALL ENVD_TARBALL_SHA256 ENVD_GOFLAGS GOTOOLCHAIN GOEXPERIMENT)
            ;;
        rocksdb)
            names+=(ROCKSDB_TARBALL ROCKSDB_TARBALL_SHA256 CROSS_PREFIX CFLAGS CXXFLAGS LDFLAGS)
            ;;
        cloud-hypervisor)
            names+=(CLOUD_HYPERVISOR_TARBALL CLOUD_HYPERVISOR_TARBALL_SHA256 CH_BASE_TAG CROSS_PREFIX RUST_TARGET RUSTFLAGS RUSTDOCFLAGS CARGO_INCREMENTAL CARGO_BUILD_RUSTC_WRAPPER)
            ;;
    esac
    for name in "${names[@]}"; do
        print_env_input "$name"
    done
}

component_toolchain() {
    local component=$1 cc=gcc cxx=g++ ar=ar
    if [ "$TARGET_ARCH" != "$(uname -m)" ]; then
        case "$TARGET_ARCH" in
            x86_64) cc=x86_64-linux-gnu-gcc; cxx=x86_64-linux-gnu-g++; ar=x86_64-linux-gnu-ar ;;
            aarch64) cc=aarch64-linux-gnu-gcc; cxx=aarch64-linux-gnu-g++; ar=aarch64-linux-gnu-ar ;;
        esac
    fi
    case "$component" in
        vmlinux)
            tool_identity cc "$cc" --version
            tool_identity ld ld --version
            tool_identity make make --version
            tool_identity pahole pahole --version
            tool_identity pkg-config pkg-config --version
            package_identities gcc make binutils openssl-devel elfutils-libelf-devel dwarves ncurses-devel flex bison perl
            ;;
        erofs)
            tool_identity cc "$cc" --version
            tool_identity cxx "$cxx" --version
            tool_identity ar "$ar" --version
            tool_identity make make --version
            tool_identity autoconf autoconf --version
            tool_identity automake automake --version
            tool_identity libtoolize libtoolize --version
            tool_identity pkg-config pkg-config --version
            package_identities gcc gcc-c++ make autoconf automake libtool libuuid-devel glibc-static
            ;;
        envd)
            tool_identity go go version
            tool_identity go-env go env GOOS GOARCH GOVERSION GOEXPERIMENT CGO_ENABLED
            package_identities golang
            ;;
        rocksdb)
            tool_identity cc "$cc" --version
            tool_identity cxx "$cxx" --version
            tool_identity ar "$ar" --version
            tool_identity cmake cmake --version
            tool_identity make make --version
            package_identities gcc gcc-c++ cmake make glibc-devel
            ;;
        cloud-hypervisor)
            tool_identity rustc rustc -vV
            tool_identity cargo cargo -Vv
            tool_identity cc "$cc" --version
            tool_identity ld ld --version
            tool_identity glibc ldd --version
            package_identities rust cargo gcc binutils glibc-devel
            ;;
    esac
}

compute_key() {
    local component=$1 descriptor=$2 path relative paths_file
    paths_file="$(mktemp)"
    component_input_paths "$component" >"$paths_file"
    sort -zu "$paths_file" -o "$paths_file"
    {
        printf 'schema\t%s\n' "$CACHE_SCHEMA"
        printf 'component\t%s\n' "$component"
        printf 'target_arch\t%s\n' "$TARGET_ARCH"
        component_environment "$component"
        component_toolchain "$component"
        while IFS= read -r -d '' path; do
            relative="${path#"$WORKSPACE_ROOT/"}"
            printf 'file\t%s\t%s\n' "$relative" "$(sha256sum "$path" | awk '{print $1}')"
        done <"$paths_file"
    } >"$descriptor"
    rm -f "$paths_file"
    sha256sum "$descriptor" | awk '{print $1}'
}

component_outputs() {
    case "$1" in
        vmlinux)
            printf 'guest-runtime/native-deps/bin/%s/vmlinux\n' "$TARGET_ARCH"
            ;;
        erofs)
            printf 'guest-runtime/native-deps/bin/%s/mkfs.erofs\n' "$TARGET_ARCH"
            printf 'guest-runtime/native-deps/bin/%s/fsck.erofs\n' "$TARGET_ARCH"
            ;;
        envd)
            printf 'guest-runtime/native-deps/bin/%s/envd\n' "$TARGET_ARCH"
            ;;
        rocksdb)
            printf 'accelerator/build/%s/rocksdb/include\n' "$TARGET_ARCH"
            printf 'accelerator/build/%s/rocksdb/lib/librocksdb.a\n' "$TARGET_ARCH"
            ;;
        cloud-hypervisor)
            printf 'sandboxer/native-deps/bin/%s/cloud-hypervisor\n' "$TARGET_ARCH"
            ;;
    esac
}

remove_outputs() {
    local relative
    while IFS= read -r relative; do
        [ -n "$relative" ] || continue
        rm -rf "$WORKSPACE_ROOT/$relative"
    done < <(component_outputs "$1")
}

validate_outputs() {
    local component=$1 relative
    while IFS= read -r relative; do
        [ -e "$WORKSPACE_ROOT/$relative" ] || die "$component did not produce $relative"
    done < <(component_outputs "$component")
}

assert_clean_source_tree() {
    local component=$1 source_dir=""
    case "$component" in
        vmlinux) source_dir="$WORKSPACE_ROOT/guest-runtime/native-deps/build/src/linux" ;;
        erofs) source_dir="$WORKSPACE_ROOT/guest-runtime/native-deps/build/$TARGET_ARCH/src/erofs-utils" ;;
        envd) source_dir="$WORKSPACE_ROOT/guest-runtime/native-deps/build/src/e2b-infra" ;;
        rocksdb) source_dir="$WORKSPACE_ROOT/accelerator/build/src/rocksdb" ;;
        cloud-hypervisor) source_dir="$WORKSPACE_ROOT/sandboxer/native-deps/build/src/cloud-hypervisor" ;;
    esac
    if [ -e "$source_dir" ]; then
        die "$component cache miss requires a clean assembled workspace; refusing to remove existing source tree $source_dir"
    fi
}

build_component() {
    local component=$1
    assert_clean_source_tree "$component"
    remove_outputs "$component"
    case "$component" in
        vmlinux) make -C "$WORKSPACE_ROOT/guest-runtime/native-deps" TARGET_ARCH="$TARGET_ARCH" vmlinux ;;
        erofs) make -C "$WORKSPACE_ROOT/guest-runtime/native-deps" TARGET_ARCH="$TARGET_ARCH" erofs ;;
        envd) make -C "$WORKSPACE_ROOT/guest-runtime/native-deps" TARGET_ARCH="$TARGET_ARCH" envd ;;
        rocksdb) make -C "$WORKSPACE_ROOT/accelerator" TARGET_ARCH="$TARGET_ARCH" deps-rocksdb ;;
        cloud-hypervisor) make -C "$WORKSPACE_ROOT/sandboxer/native-deps" TARGET_ARCH="$TARGET_ARCH" cloud-hypervisor ;;
    esac
    validate_outputs "$component"
}

validate_tar_paths() {
    local archive=$1 path
    while IFS= read -r path; do
        case "$path" in
            /*|../*|*/../*|..) die "unsafe path in cached payload: $path" ;;
        esac
    done < <(tar -tf "$archive")
}

verify_entry() {
    local entry=$1 key=$2 actual_key
    [ -d "$entry" ] || return 1
    [ -f "$entry/SHA256SUMS" ] || die "cache entry lacks SHA256SUMS: $entry"
    [ -f "$entry/inputs.tsv" ] || die "cache entry lacks inputs.tsv: $entry"
    [ -f "$entry/payload.tar" ] || die "cache entry lacks payload.tar: $entry"
    (
        cd "$entry"
        sha256sum --quiet -c SHA256SUMS
    ) || die "cache entry checksum verification failed: $entry"
    actual_key="$(sha256sum "$entry/inputs.tsv" | awk '{print $1}')"
    [ "$actual_key" = "$key" ] || die "cache input hash mismatch: expected $key, got $actual_key"
    validate_tar_paths "$entry/payload.tar"
}

restore_entry() {
    local component=$1 entry=$2 key=$3
    verify_entry "$entry" "$key"
    remove_outputs "$component"
    tar --extract --file "$entry/payload.tar" --directory "$WORKSPACE_ROOT" --no-same-owner
    validate_outputs "$component"
}

publish_entry() {
    local component=$1 key=$2 descriptor=$3 entry=$4 relative revision_hash=unavailable
    ACTIVE_STAGE="$CACHE_ROOT/$CACHE_SCHEMA/.tmp/$component.$key.$$"
    rm -rf "$ACTIVE_STAGE"
    mkdir -p "$ACTIVE_STAGE/payload"
    while IFS= read -r relative; do
        mkdir -p "$ACTIVE_STAGE/payload/$(dirname "$relative")"
        cp -a "$WORKSPACE_ROOT/$relative" "$ACTIVE_STAGE/payload/$relative"
    done < <(component_outputs "$component")
    (
        cd "$ACTIVE_STAGE/payload"
        tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner -cf ../payload.tar .
    )
    cp "$descriptor" "$ACTIVE_STAGE/inputs.tsv"
    if [ -n "${KUASAR_REVISION_MANIFEST:-}" ] && [ -f "$KUASAR_REVISION_MANIFEST" ]; then
        revision_hash="$(sha256sum "$KUASAR_REVISION_MANIFEST" | awk '{print $1}')"
    fi
    {
        printf 'schema=%s\n' "$CACHE_SCHEMA"
        printf 'component=%s\n' "$component"
        printf 'target_arch=%s\n' "$TARGET_ARCH"
        printf 'input_hash=%s\n' "$key"
        printf 'revision_manifest_sha256=%s\n' "$revision_hash"
        printf 'created_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } >"$ACTIVE_STAGE/provenance.txt"
    (
        cd "$ACTIVE_STAGE"
        sha256sum payload.tar inputs.tsv provenance.txt >SHA256SUMS
    )
    verify_entry "$ACTIVE_STAGE" "$key"
    mv "$ACTIVE_STAGE" "$entry"
    ACTIVE_STAGE=""
    chmod -R a-w "$entry"
}

restore_or_build() {
    local component=$1 descriptor key component_root entry lock start status
    start="$(now_ns)"
    descriptor="$(mktemp)"
    key="$(compute_key "$component" "$descriptor")"
    component_root="$CACHE_ROOT/$CACHE_SCHEMA/$TARGET_ARCH/$component"
    entry="$component_root/$key"
    lock="$CACHE_ROOT/$CACHE_SCHEMA/.locks/$component.$key.lock"
    mkdir -p "$component_root" "$(dirname "$lock")" "$CACHE_ROOT/$CACHE_SCHEMA/.tmp"

    if [ -d "$entry" ]; then
        restore_entry "$component" "$entry" "$key"
        status=hit
    else
        exec {lock_fd}>"$lock"
        flock "$lock_fd"
        if [ -d "$entry" ]; then
            restore_entry "$component" "$entry" "$key"
            status=hit-after-wait
        else
            log "$component cache miss (${key:0:12}); building"
            build_component "$component"
            publish_entry "$component" "$key" "$descriptor" "$entry"
            restore_entry "$component" "$entry" "$key"
            status=miss-built
        fi
        flock -u "$lock_fd"
        exec {lock_fd}>&-
    fi
    rm -f "$descriptor"
    local elapsed
    elapsed="$(elapsed_seconds "$start" "$(now_ns)")"
    record_metric "$component" "$status" "$key" "$elapsed"
    log "$component $status (${key:0:12}, ${elapsed}s)"
}

print_key() {
    local component=$1 descriptor key
    descriptor="$(mktemp)"
    key="$(compute_key "$component" "$descriptor")"
    rm -f "$descriptor"
    printf '%s\t%s\n' "$component" "$key"
}

usage() {
    cat <<'EOF'
usage: native-cache.sh restore-or-build [component ...]
       native-cache.sh key [component ...]

components: vmlinux erofs envd rocksdb cloud-hypervisor
EOF
}

main() {
    local command=${1:-}
    [ -n "$command" ] || { usage >&2; exit 2; }
    shift
    local components=("$@") component
    if [ "${#components[@]}" -eq 0 ]; then
        components=(vmlinux erofs envd rocksdb cloud-hypervisor)
    fi
    case "$command" in
        restore-or-build)
            for component in "${components[@]}"; do restore_or_build "$component"; done
            ;;
        key)
            for component in "${components[@]}"; do print_key "$component"; done
            ;;
        -h|--help|help) usage ;;
        *) usage >&2; exit 2 ;;
    esac
}

main "$@"
