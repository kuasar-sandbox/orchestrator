#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${BIN:?BIN must point to the assembled platform binary directory}"
export BIN
export REQUIRE_KVM=1
export REQUIRE_EXEC=1
export REQUIRE_CLUSTER_STUB=1
export REQUIRE_CLUSTER_REAL=1
export REQUIRE_ORCH=1
export REQUIRE_PROXY=1
export REQUIRE_BUILDER=1
export REQUIRE_RUNTASK=1

# Source integration runs source-wide checks exactly once, before the KVM
# lifecycle suite. Binary-only staged packages intentionally have no Go source.
if [ -n "${CANDIDATE_REPOSITORY:-}" ]; then
    source_root="$(go list -m -f '{{.Dir}}' github.com/kuasar-sandbox/orchestrator)"
    [ -f "$source_root/go.mod" ] || {
        echo "orchestrator source integration checkout is missing" >&2
        exit 1
    }
    (
        cd "$source_root"
        echo "==> orchestrator source unit, race and vet regressions"
        CGO_ENABLED=0 go test -count=1 -timeout=5m ./...
        CGO_ENABLED=1 go test -race -count=1 -timeout=5m ./...
        CGO_ENABLED=0 go vet ./...
    )
fi

# A cancelled execute case may be killed before its EXIT trap. Recover the
# exact root-owned reservation before an earlier fixed-name case tries to add
# the same floating-IP return route.
recovery_status=0
flock --nonblock --exclusive --close --conflict-exit-code 75 \
    /run/systemd/system env BIN="$BIN" bash "$SCRIPT_DIR/lib/execute_state.sh" recover \
    || recovery_status=$?
[ "$recovery_status" -ne 75 ] \
    || { echo "another execute/MMDS case holds the host runtime lock" >&2; exit 1; }
[ "$recovery_status" -eq 0 ] || exit "$recovery_status"

shopt -s nullglob
cases=("$SCRIPT_DIR"/e2e_*.sh)
[ "${#cases[@]}" -gt 0 ] || {
    echo "orchestrator e2e suite contains no cases" >&2
    exit 1
}

# Reuse the real sandbox/cluster/Build cases, rather than booting duplicate
# fixtures solely for logging. Cursor and exact executable scope exclude old
# records and other source workspaces on the same journal host.
journal_work="$(mktemp -d)"
trap 'rm -rf "$journal_work"' EXIT
journal=(journalctl)
if [ "$(id -u)" -ne 0 ]; then journal=(sudo -n journalctl); fi
for script in "${cases[@]}"; do
    case "${ORCHESTRATOR_E2E_GROUP:-all}:$(basename "$script")" in
        a:e2e_builder_unit_upgrade.sh|a:e2e_cluster_real.sh|a:e2e_cluster_stub.sh|a:e2e_run_builder.sh)
            ;;
        a:*)
            continue
            ;;
        b:e2e_builder_unit_upgrade.sh|b:e2e_cluster_real.sh|b:e2e_cluster_stub.sh|b:e2e_run_builder.sh)
            continue
            ;;
    esac
    echo
    echo "========================================="
    echo "  orchestrator/$(basename "$script")"
    echo "========================================="
    kind=""
    case "$(basename "$script")" in
        e2e_execute.sh) kind=sandbox ;;
        e2e_cluster_real.sh) kind=cluster ;;
        e2e_run_builder.sh) kind=build ;;
    esac
    if [ -n "$kind" ]; then
        "${journal[@]}" --sync
        "${journal[@]}" -n 1 --show-cursor --no-pager -o cat >"$journal_work/cursor"
        cursor="$(sed -n 's/^-- cursor: //p' "$journal_work/cursor" | tail -1)"
        [ -n "$cursor" ] || { echo "cannot capture native journal cursor" >&2; exit 1; }
    fi
    case "$(basename "$script")" in
        e2e_mmds_routes.sh|e2e_mmds_routes_proxy_restart.sh)
            echo "==> covered by the corresponding owner lifecycle with MMDS enabled"
            continue
            ;;
        e2e_orchestrator_proxy.sh)
            export MMDS_ROUTES_E2E=1
            export MMDS_SECRET_INITIAL_VALUE=MMDS_SECRET_INITIAL_GUEST_E2E
            export REQ_MMDS_HEADER='{"secrets":{"e2e_secret":"MMDS_SECRET_INITIAL_GUEST_E2E"},"routes":[{"path":"/e2e/static","data":"MMDS_STATIC_GUEST_E2E"},{"path":"/e2e/secret","type":"secret","secret":"e2e_secret","content_type":"application/x-kuasar-e2e-secret"},{"path":"/e2e/unresolved","type":"secret","secret":"e2e_unresolved"},{"path":"/e2e/service","type":"service","service":"e2e_service"}]}'
            ;;
        *)
            unset MMDS_ROUTES_E2E MMDS_SECRET_INITIAL_VALUE REQ_MMDS_HEADER || true
            ;;
    esac
    bash "$script"
    if [ -n "$kind" ]; then
        "${journal[@]}" --sync
        "${journal[@]}" --after-cursor="$cursor" --no-pager -o json \
            "_EXE=$(readlink -f "$BIN/sandbox-ctl")" >"$journal_work/entries.jsonl"
        python3 "$SCRIPT_DIR/lib/journal_identity.py" "$kind" "$journal_work/entries.jsonl"
    fi
done
