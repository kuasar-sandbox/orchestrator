#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp -d)"
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
