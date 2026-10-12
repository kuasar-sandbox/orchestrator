#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
dump_template_build_failure() {
    # Early CLI/runtime failures can precede the SDK-visible Build journal.
    # This task's unique unit prefix also captures that original stderr.
    local journal="$WORK/template-build-failure.journal"
    journalctl --no-pager -o cat -u "${BUILDER_PREFIX}*.service" >"$journal" 2>&1 || true
    echo "==> template Build unit journal (last 300 lines):" >&2
    tail -n 300 "$journal" >&2
    if [ -n "${ORCH_LOG:-}" ] && [ -f "$ORCH_LOG" ]; then
        echo "==> template Build conductor log (last 80 lines):" >&2
        tail -n 80 "$ORCH_LOG" >&2
    fi
}

build_ready_template() {
# The template Build uses the same 6GiB input as the focused Builder cases so
# its initial reservation fits the public runner's default startup pool.
# The resulting Sandbox still has the separately asserted 2560MiB capacity.
REQ_RESOURCE_HEADER='{"capacity":{"cpu":2,"memory":"2560MiB"}}'
code=$(req POST /v3/templates "$AK" "{\"name\":\"exec-tmpl\",\"cpuCount\":$BUILDER_CPU,\"memoryMB\":6144}")
unset REQ_RESOURCE_HEADER
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" \
    "{\"fromImage\":\"$GUEST_REF\",\"startCmd\":\"exec sleep 86400\",\"readyCmd\":\"true\"}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; dump_template_build_failure; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || { dump_template_build_failure; fail "build did not become ready"; }
case "$TEMPLATE" in e2b-snp-*) : ;; *) fail "build produced $TEMPLATE (want e2b-snp-...)";; esac
echo "==> built template: $TEMPLATE"

}

build_image_template_source() { # profile, optional case-owned imported Image
local profile="$1" source_body
if [ -n "${2:-}" ]; then
    source_body="{\"fromTemplate\":\"$2\"}"
else
    source_body="{\"fromImage\":\"$GUEST_REF\"}"
fi
code=$(req POST /v3/templates "$AK" "{\"name\":\"proxy-tmpl\",\"profile\":\"$profile\",\"cpuCount\":2,\"memoryMB\":6144}")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "register=$code"; }
TID=$(json_field "$WORK/resp.body" templateID)
BID=$(json_field "$WORK/resp.body" buildID)
code=$(req POST "/v2/templates/$TID/builds/$BID" "$AK" "$source_body")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "trigger=$code"; }
TEMPLATE=""
for _ in $(seq 1 120); do
    req GET "/templates/$TID/builds/$BID/status" "$AK" >/dev/null
    st=$(json_field "$WORK/resp.body" status)
    case "$st" in
        ready) TEMPLATE=$(json_field "$WORK/resp.body" templateID); break;;
        error) cat "$WORK/resp.body"; dump_template_build_failure; fail "build error";;
    esac; sleep 1
done
[ -n "$TEMPLATE" ] || { dump_template_build_failure; fail "build did not become ready"; }
echo "==> built template: $TEMPLATE"

}

build_image_template() { # optional profile, default e2b
    local profile="${1:-e2b}" entry cache_fd imported mode started phase_started
    local helper="$SCRIPT_DIR/template_cache.py"
    local phases="$CASE_OUT/template-cache-phases.jsonl"
    started=$(python3 -c 'import time; print(time.monotonic())')
    if [ -z "${E2E_TEMPLATE_CACHE_DIR:-}" ]; then
        build_image_template_source "$profile"
        mode=disabled
        python3 "$helper" record --mode "$mode" --phase production --profile "$profile" --started "$started" --output "$phases"
    else
        entry=$(python3 "$helper" entry) || fail "template cache identity"
        [ ! -L "$entry.lock" ] || fail "unsafe template cache lock"
        exec {cache_fd}>"$entry.lock"
        flock -w 600 "$cache_fd" || fail "template cache entry lock timed out"
        if [ -e "$entry" ] || [ -L "$entry" ]; then
            # Each consumer re-encrypts immutable bytes into its own Store.
            # Never pass a previous case's template ID, key or Manifest root.
            imported=$(MANIFEST_KEY="$MK" python3 "$helper" restore --work "$WORK" --profile "$profile" --output "$phases") \
                || fail "template cache restore"
            phase_started=$(python3 -c 'import time; print(time.monotonic())')
            build_image_template_source "$profile" "$imported"
            mode=hit
            python3 "$helper" record --mode "$mode" --phase registration --profile "$profile" --started "$phase_started" --output "$phases"
        else
            phase_started=$(python3 -c 'import time; print(time.monotonic())')
            build_image_template_source "$profile"
            python3 "$helper" record --mode miss --phase production --profile "$profile" --started "$phase_started" --output "$phases"
            MANIFEST_KEY="$MK" python3 "$helper" publish --work "$WORK" --profile "$profile" --template "$TEMPLATE" --output "$phases" \
                || fail "template cache publication"
            mode=miss
        fi
        flock -u "$cache_fd"
        exec {cache_fd}>&-
    fi
    python3 "$helper" record --mode "$mode" --profile "$profile" --started "$started" \
        --output "$CASE_OUT/template-preparation.jsonl"
}
