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

echo "test-ci-tools: PASS"
