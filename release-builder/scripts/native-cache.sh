#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${KUASAR_WORKSPACE_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
CACHE_ROOT="${KUASAR_NATIVE_CACHE_ROOT:-/var/cache/kuasar/native}"
CACHE_SCHEMA="v1"
METRICS_FILE="${KUASAR_NATIVE_CACHE_METRICS:-}"
MAX_ENTRIES="${KUASAR_NATIVE_CACHE_MAX_ENTRIES:-4}"
MIN_ENTRY_AGE_SECONDS="${KUASAR_NATIVE_CACHE_MIN_AGE_SECONDS:-3600}"

TARGET_ARCH="${TARGET_ARCH:-$(uname -m)}"
case "$TARGET_ARCH" in
    amd64) TARGET_ARCH=x86_64 ;;
    arm64) TARGET_ARCH=aarch64 ;;
    x86_64|aarch64) ;;
    *) echo "native-cache: unsupported TARGET_ARCH=$TARGET_ARCH" >&2; exit 2 ;;
esac

ACTIVE_STAGE=""
ACTIVE_STAGE_LOCK=""
ACTIVE_STAGE_FD=""

release_active_stage_lock() {
    if [ -n "${ACTIVE_STAGE_FD:-}" ]; then
        flock -u "$ACTIVE_STAGE_FD" 2>/dev/null || true
        exec {ACTIVE_STAGE_FD}>&-
        ACTIVE_STAGE_FD=""
    fi
    if [ -n "$ACTIVE_STAGE_LOCK" ]; then
        rm -f "$ACTIVE_STAGE_LOCK"
        ACTIVE_STAGE_LOCK=""
    fi
}

cleanup() {
    if [ -n "$ACTIVE_STAGE" ] && [ -d "$ACTIVE_STAGE" ]; then
        chmod -R u+w "$ACTIVE_STAGE" 2>/dev/null || true
        rm -rf "$ACTIVE_STAGE"
    fi
    ACTIVE_STAGE=""
    release_active_stage_lock
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
    local rpm_packages=$1 dpkg_packages=$2 package result
    local packages=()
    if command -v rpm >/dev/null 2>&1; then
        read -r -a packages <<<"$rpm_packages"
        for package in "${packages[@]}"; do
            result="$(rpm -q --qf '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}' "$package" 2>/dev/null || printf '%s-missing' "$package")"
            printf 'package\t%s\n' "$result"
        done
    elif command -v dpkg-query >/dev/null 2>&1; then
        read -r -a packages <<<"$dpkg_packages"
        for package in "${packages[@]}"; do
            result="$(dpkg-query -W -f='${Package}=${Version}:${Architecture}' "$package" 2>/dev/null || printf '%s-missing' "$package")"
            printf 'package\t%s\n' "$result"
        done
    fi
}

file_identity() {
    local label=$1 path=$2 resolved hash=missing
    if [ -f "$path" ]; then
        resolved="$(readlink -f "$path")"
        hash="$(sha256sum "$resolved" | awk '{print $1}')"
        printf 'file-identity\t%s\t%q\t%s\n' "$label" "$resolved" "$hash"
    else
        printf 'file-identity\t%s\t%q\tmissing\n' "$label" "$path"
    fi
}

pkg_config_module_identity() {
    local module=$1 pkg_config=${PKG_CONFIG:-pkg-config}
    local pc_path metadata metadata_hash pc_hash=missing libdir libs token name dir candidate
    if ! command -v "$pkg_config" >/dev/null 2>&1 \
        || ! "$pkg_config" --exists "$module" 2>/dev/null; then
        printf 'pkg-config-module\t%s\tmissing\n' "$module"
        return
    fi

    pc_path="$("$pkg_config" --path "$module" 2>/dev/null || true)"
    metadata="$({
        "$pkg_config" --modversion "$module"
        "$pkg_config" --cflags "$module"
        "$pkg_config" --libs "$module"
        "$pkg_config" --libs --static "$module"
    } 2>&1 || true)"
    metadata_hash="$(printf '%s' "$metadata" | sha256sum | awk '{print $1}')"
    if [ -f "$pc_path" ]; then
        pc_hash="$(sha256sum "$pc_path" | awk '{print $1}')"
    fi
    printf 'pkg-config-module\t%s\t%q\t%s\t%s\n' \
        "$module" "$pc_path" "$pc_hash" "$metadata_hash"

    local search_dirs=() library_names=() absolute_libraries=()
    libdir="$("$pkg_config" --variable=libdir "$module" 2>/dev/null || true)"
    [ -z "$libdir" ] || search_dirs+=("$libdir")
    libs="$("$pkg_config" --libs --static "$module" 2>/dev/null || true)"
    read -r -a tokens <<<"$libs"
    for token in "${tokens[@]}"; do
        case "$token" in
            -L*) search_dirs+=("${token#-L}") ;;
            -l:*) library_names+=("${token#-l:}") ;;
            -l*) library_names+=("lib${token#-l}") ;;
            /*.a|/*.so|/*.so.*) absolute_libraries+=("$token") ;;
        esac
    done

    declare -A seen=()
    for candidate in "${absolute_libraries[@]}"; do
        [ -z "${seen[$candidate]+x}" ] || continue
        seen[$candidate]=1
        file_identity "pkg-config:$module:library" "$candidate"
    done
    for name in "${library_names[@]}"; do
        for dir in "${search_dirs[@]}"; do
            for candidate in "$dir/$name" "$dir/$name.a" "$dir/$name.so"; do
                [ -f "$candidate" ] || continue
                [ -z "${seen[$candidate]+x}" ] || continue
                seen[$candidate]=1
                file_identity "pkg-config:$module:library" "$candidate"
            done
        done
    done
}

cargo_config_identities() {
    local cargo_home=${CARGO_HOME:-${HOME:-}/.cargo}
    printf 'cargo-home\t%q\n' "$cargo_home"
    file_identity cargo-config "$cargo_home/config"
    file_identity cargo-config-toml "$cargo_home/config.toml"
}

effective_rust_target() {
    # sandboxer/native-deps/Makefile derives and passes RUST_TARGET from
    # TARGET_ARCH; an inherited shell variable does not override that makefile
    # assignment. Derive the Cargo target the same way here.
    case "$TARGET_ARCH" in
        x86_64) printf 'x86_64-unknown-linux-gnu' ;;
        aarch64) printf 'aarch64-unknown-linux-gnu' ;;
    esac
}

component_environment() {
    local component=$1 name rust_target rust_target_prefix
    local names=(SOURCE_DATE_EPOCH)
    case "$component" in
        vmlinux)
            names+=(
                LINUX_TARBALL LINUX_TARBALL_SHA256 LINUX_BASE_TAG CROSS_PREFIX KERNEL_ARCH
                LOCALVERSION KBUILD_BUILD_TIMESTAMP KBUILD_BUILD_USER KBUILD_BUILD_HOST
                KBUILD_BUILD_VERSION KBUILD_BUILD_SALT KCFLAGS KAFLAGS KCPPFLAGS
                HOSTCFLAGS HOSTCXXFLAGS HOSTLDFLAGS LDFLAGS_vmlinux
                PKG_CONFIG PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR
            )
            ;;
        erofs)
            names+=(
                EROFS_TARBALL EROFS_TARBALL_SHA256 CROSS_PREFIX CFLAGS CXXFLAGS LDFLAGS
                PKG_CONFIG PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR
            )
            ;;
        envd)
            names+=(
                ENVD_TARBALL ENVD_TARBALL_SHA256 ENVD_GOFLAGS GOFLAGS GOTOOLCHAIN
                GOEXPERIMENT GOAMD64 GOARM64
            )
            ;;
        rocksdb)
            names+=(
                ROCKSDB_TARBALL ROCKSDB_TARBALL_SHA256 ROCKSDB_SOURCE_SHA256 CROSS_PREFIX
                CC CXX CFLAGS CXXFLAGS LDFLAGS
            )
            ;;
        cloud-hypervisor)
            names+=(
                CLOUD_HYPERVISOR_TARBALL CLOUD_HYPERVISOR_TARBALL_SHA256 CH_BASE_TAG
                CROSS_PREFIX RUST_TARGET RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS
                RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER CARGO_HOME CARGO_INCREMENTAL
                CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_TARGET
            )
            rust_target="$(effective_rust_target)"
            rust_target_prefix="CARGO_TARGET_$(printf '%s' "$rust_target" | tr 'a-z-' 'A-Z_')"
            names+=(
                "${rust_target_prefix}_RUSTFLAGS"
                "${rust_target_prefix}_LINKER"
            )
            ;;
    esac
    for name in "${names[@]}"; do
        print_env_input "$name"
    done
}

effective_cross_prefix() {
    if [ -n "${CROSS_PREFIX+x}" ]; then
        printf '%s' "$CROSS_PREFIX"
        return
    fi
    if [ "$TARGET_ARCH" != "$(uname -m)" ]; then
        case "$TARGET_ARCH" in
            x86_64) printf 'x86_64-linux-gnu-' ;;
            aarch64) printf 'aarch64-linux-gnu-' ;;
        esac
    fi
}

component_toolchain() {
    local component=$1 cross_prefix cc cxx ar ld pkg_config
    cross_prefix="$(effective_cross_prefix)"
    cc="${cross_prefix}gcc"
    cxx="${cross_prefix}g++"
    ar="${cross_prefix}ar"
    ld="${cross_prefix}ld"
    pkg_config=${PKG_CONFIG:-pkg-config}
    printf 'toolchain\tcross_prefix\t%q\n' "$cross_prefix"
    case "$component" in
        vmlinux)
            tool_identity cc "$cc" --version
            tool_identity ld "$ld" --version
            tool_identity make make --version
            tool_identity pahole pahole --version
            tool_identity pkg-config "$pkg_config" --version
            package_identities \
                'gcc make binutils openssl-devel elfutils-libelf-devel dwarves ncurses-devel flex bison perl' \
                'gcc make binutils libssl-dev libelf-dev dwarves libncurses-dev flex bison perl'
            pkg_config_module_identity libelf
            pkg_config_module_identity libssl
            pkg_config_module_identity openssl
            ;;
        erofs)
            tool_identity cc "$cc" --version
            tool_identity cxx "$cxx" --version
            tool_identity ar "$ar" --version
            tool_identity make make --version
            tool_identity autoconf autoconf --version
            tool_identity automake automake --version
            tool_identity libtoolize libtoolize --version
            tool_identity pkg-config "$pkg_config" --version
            package_identities \
                'gcc gcc-c++ make autoconf automake libtool libuuid-devel glibc-static' \
                'gcc g++ make autoconf automake libtool uuid-dev libc6-dev'
            pkg_config_module_identity uuid
            ;;
        envd)
            tool_identity go go version
            tool_identity go-env go env GOOS GOARCH GOVERSION GOEXPERIMENT GOFLAGS GOAMD64 GOARM64 CGO_ENABLED
            package_identities 'golang' 'golang-go'
            ;;
        rocksdb)
            if [ -z "$cross_prefix" ]; then
                cc=${CC:-gcc}
                cxx=${CXX:-g++}
            fi
            tool_identity cc "$cc" --version
            tool_identity cxx "$cxx" --version
            tool_identity ar "$ar" --version
            tool_identity cmake cmake --version
            tool_identity make make --version
            package_identities 'gcc gcc-c++ cmake make glibc-devel' 'gcc g++ cmake make libc6-dev'
            ;;
        cloud-hypervisor)
            tool_identity rustc rustc -vV
            tool_identity cargo cargo -Vv
            tool_identity cc "$cc" --version
            tool_identity ld "$ld" --version
            tool_identity glibc ldd --version
            package_identities 'rust cargo gcc binutils glibc-devel' 'rustc cargo gcc binutils libc6-dev'
            cargo_config_identities
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
    local component=$1 cross_prefix
    cross_prefix="$(effective_cross_prefix)"
    assert_clean_source_tree "$component"
    remove_outputs "$component"
    case "$component" in
        vmlinux) make -C "$WORKSPACE_ROOT/guest-runtime/native-deps" TARGET_ARCH="$TARGET_ARCH" CROSS_PREFIX="$cross_prefix" vmlinux ;;
        erofs) make -C "$WORKSPACE_ROOT/guest-runtime/native-deps" TARGET_ARCH="$TARGET_ARCH" CROSS_PREFIX="$cross_prefix" erofs ;;
        envd) make -C "$WORKSPACE_ROOT/guest-runtime/native-deps" TARGET_ARCH="$TARGET_ARCH" CROSS_PREFIX="$cross_prefix" envd ;;
        rocksdb) make -C "$WORKSPACE_ROOT/accelerator" TARGET_ARCH="$TARGET_ARCH" CROSS_PREFIX="$cross_prefix" deps-rocksdb ;;
        cloud-hypervisor) make -C "$WORKSPACE_ROOT/sandboxer/native-deps" TARGET_ARCH="$TARGET_ARCH" CROSS_PREFIX="$cross_prefix" cloud-hypervisor ;;
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
    # Cache payloads use a deterministic 1970 mtime.  A verified restore is
    # authoritative for the current input key, so stamp extracted outputs at
    # restore time instead of making downstream file targets appear stale.
    tar --extract --touch --file "$entry/payload.tar" \
        --directory "$WORKSPACE_ROOT" --no-same-owner
    validate_outputs "$component"
}

create_active_stage() {
    local component=$1 key=$2 stage_root lock_root
    local prune_lock prune_fd
    stage_root="$CACHE_ROOT/$CACHE_SCHEMA/.tmp"
    lock_root="$CACHE_ROOT/$CACHE_SCHEMA/.locks/staging"
    prune_lock="$CACHE_ROOT/$CACHE_SCHEMA/.locks/prune-staging.lock"
    mkdir -p "$stage_root" "$lock_root"

    # Close the creation race with the orphan cleaner: the stage is visible
    # only after its unique lock is held.
    exec {prune_fd}>"$prune_lock"
    flock "$prune_fd"
    ACTIVE_STAGE="$(mktemp -d "$stage_root/$component.$key.XXXXXX")"
    ACTIVE_STAGE_LOCK="$lock_root/${ACTIVE_STAGE##*/}.lock"
    exec {ACTIVE_STAGE_FD}>"$ACTIVE_STAGE_LOCK"
    flock "$ACTIVE_STAGE_FD"
    flock -u "$prune_fd"
    exec {prune_fd}>&-
}

prune_staging_directories() {
    local stage_root lock_root prune_lock stage name lock
    local prune_fd stage_fd
    stage_root="$CACHE_ROOT/$CACHE_SCHEMA/.tmp"
    lock_root="$CACHE_ROOT/$CACHE_SCHEMA/.locks/staging"
    prune_lock="$CACHE_ROOT/$CACHE_SCHEMA/.locks/prune-staging.lock"
    mkdir -p "$stage_root" "$lock_root"

    exec {prune_fd}>"$prune_lock"
    flock "$prune_fd"
    while IFS= read -r -d '' stage; do
        name=${stage##*/}
        lock="$lock_root/$name.lock"
        exec {stage_fd}>"$lock"
        if flock --nonblock "$stage_fd"; then
            chmod -R u+w "$stage" 2>/dev/null || true
            rm -rf "$stage"
            flock -u "$stage_fd"
        fi
        exec {stage_fd}>&-
        [ -e "$stage" ] || rm -f "$lock"
    done < <(find "$stage_root" -mindepth 1 -maxdepth 1 -type d -print0)

    # A crash immediately after atomic publication can leave only the tiny
    # external lock file. Stage names are mktemp-unique, so it is never reused.
    while IFS= read -r -d '' lock; do
        name=${lock##*/}
        name=${name%.lock}
        [ -e "$stage_root/$name" ] || rm -f "$lock"
    done < <(find "$lock_root" -maxdepth 1 -type f -name '*.lock' -print0)
    flock -u "$prune_fd"
    exec {prune_fd}>&-
}

publish_entry() {
    local component=$1 key=$2 descriptor=$3 entry=$4 relative revision_hash=unavailable
    create_active_stage "$component" "$key"
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
    release_active_stage_lock
    chmod -R a-w "$entry"
}

prune_component_entries() {
    local component=$1 protected_entry=$2 component_root prune_lock now
    [[ "$MAX_ENTRIES" =~ ^[1-9][0-9]*$ ]] \
        || die "KUASAR_NATIVE_CACHE_MAX_ENTRIES must be a positive integer"
    [[ "$MIN_ENTRY_AGE_SECONDS" =~ ^[0-9]+$ ]] \
        || die "KUASAR_NATIVE_CACHE_MIN_AGE_SECONDS must be a non-negative integer"

    component_root="$CACHE_ROOT/$CACHE_SCHEMA/$TARGET_ARCH/$component"
    prune_lock="$CACHE_ROOT/$CACHE_SCHEMA/.locks/prune.$TARGET_ARCH.$component.lock"
    exec {prune_fd}>"$prune_lock"
    flock "$prune_fd"

    local entries=() entry key lock mtime age index=0
    mapfile -t entries < <(
        find "$component_root" -mindepth 1 -maxdepth 1 -type d -printf '%T@\t%p\n' \
            | sort -t $'\t' -k1,1nr | cut -f2-
    )
    now=$(date +%s)
    for entry in "${entries[@]}"; do
        key=${entry##*/}
        [[ "$key" =~ ^[0-9a-f]{64}$ ]] || continue
        index=$((index + 1))
        [ "$index" -gt "$MAX_ENTRIES" ] || continue
        [ "$entry" != "$protected_entry" ] || continue
        mtime=$(stat -c %Y "$entry" 2>/dev/null || printf '%s' "$now")
        age=$((now - mtime))
        [ "$age" -ge "$MIN_ENTRY_AGE_SECONDS" ] || continue

        lock="$CACHE_ROOT/$CACHE_SCHEMA/.locks/$TARGET_ARCH.$component.$key.lock"
        exec {entry_fd}>"$lock"
        if flock --nonblock "$entry_fd"; then
            chmod -R u+w "$entry"
            rm -rf "$entry"
            flock -u "$entry_fd"
        fi
        exec {entry_fd}>&-
    done
    flock -u "$prune_fd"
    exec {prune_fd}>&-
}

restore_or_build() {
    local component=$1 descriptor key component_root entry lock start status existed_before_lock=0
    start="$(now_ns)"
    descriptor="$(mktemp)"
    key="$(compute_key "$component" "$descriptor")"
    component_root="$CACHE_ROOT/$CACHE_SCHEMA/$TARGET_ARCH/$component"
    entry="$component_root/$key"
    lock="$CACHE_ROOT/$CACHE_SCHEMA/.locks/$TARGET_ARCH.$component.$key.lock"
    mkdir -p "$component_root" "$(dirname "$lock")" "$CACHE_ROOT/$CACHE_SCHEMA/.tmp"
    prune_staging_directories

    [ -d "$entry" ] && existed_before_lock=1
    exec {lock_fd}>"$lock"
    flock "$lock_fd"
    if [ -d "$entry" ]; then
        restore_entry "$component" "$entry" "$key"
        if [ "$existed_before_lock" -eq 1 ]; then
            status=hit
        else
            status=hit-after-wait
        fi
    else
        log "$component cache miss (${key:0:12}); building"
        build_component "$component"
        publish_entry "$component" "$key" "$descriptor" "$entry"
        restore_entry "$component" "$entry" "$key"
        status=miss-built
    fi
    touch "$entry"
    flock -u "$lock_fd"
    exec {lock_fd}>&-
    prune_component_entries "$component" "$entry"
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
