#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/builder_env.sh"
write_builder_config
start_conductor
. "$SCRIPT_DIR/builder_proxy.sh"
start_builder_proxy

# Exercise the installed templates without going through orchestrator cleanup:
# an unknown run id reaches the resident parent, fails run-session admission,
# and exits non-zero before assignment. CollectMode must unload each failed
# instance while journald retains its diagnostics. Repetition proves the failed-unit set does not accumulate.
RUNNER_FAILED_BASE=$(failed_unit_count 'sandbox-runner@*.service')
BUILDER_FAILED_BASE=$(failed_unit_count 'sandbox-builder@*.service')
for kind in runner builder; do
    case "$kind" in
        runner) failed_before="$RUNNER_FAILED_BASE" ;;
        builder) failed_before="$BUILDER_FAILED_BASE" ;;
    esac
    for attempt in 1 2 3; do
        run_id="issue178-$kind-$RANDOM-$attempt"
        unit="sandbox-$kind@$run_id.service"
        systemctl start "$unit" >"$WORK/$run_id.start" 2>&1 || true
        wait_unit_journal_contains "$unit" "open run session: unknown run" "$WORK/$run_id.journal" \
            || { cat "$WORK/$run_id.start" "$WORK/$run_id.journal" >&2; fail "$unit journal was not retained"; }
        wait_unit_collected "$unit" || fail "$unit remained loaded and failed"
    done
    failed_after=$(failed_unit_count "sandbox-$kind@*.service")
    [ "$failed_after" = "$failed_before" ] \
        || fail "failed $kind units grew from $failed_before to $failed_after"
done
echo "==> PASS: repeated runner/builder pre-assignment failures were collected; journals remained queryable"

# This prerequisite is built through the real API from the prepared image.
register source-image
SOURCE_TID="$TID"; SOURCE_BID="$BID"
code=$(req POST "/v2/templates/$SOURCE_TID/builds/$SOURCE_BID" "$AK" "{\"fromImage\":\"$PULL_REF\"}")
[ "$code" = 202 ] || { cat "$WORK/resp.body"; fail "source image trigger=$code"; }
wait_ready "$SOURCE_TID" "$SOURCE_BID" source-image null img
SOURCE_IMAGE="$PERSIST"
run_interruption_contract() {
    PYTHONPATH="$SCRIPT_DIR" BUILD_ACTION_API_KEY="$AK" python3 - \
        --url "http://127.0.0.1:$PORT" --host "api.$DOMAIN" \
        --db "$WORK/lib/node-ctl.db" --run-root "$WORK/run" --base-root "$WORK/lib" \
        --socket "$WORK/node-ctl.socket" --bin "$BIN" --switch "$SWITCH" --conductor-pid "$CONDUCTOR_PID" \
        --source "$SOURCE_IMAGE" --cpu "$BUILDER_CPU" --timeout-only "$1" --evidence "$WORK/$2.json" <<'PY_ACTION'
from build_client import *
args = configure()
mode = "timeout" if args.timeout_only else "cancel"
fds_before = conductor_fds()
tid, bid = register(mode + "-hang")
marker = "ISSUE372_HANG_" + bid
trigger(tid, bid, "echo " + marker + "; while :; do sleep 1; done")

def live_hang():
    current = row(bid)
    assert current and current["status"] != "error", current and current["reason"]
    if not current["run_id"] or not current["phase_sandbox_id"]:
        return None
    log = command("journalctl", "KUASAR_BUILD_ID=" + bid, "--no-pager", "--output=cat")
    # The marker occurs in the submitted spec too; require the actual
    # guest output line rather than merely finding a command argument.
    return current if any(line.strip() == marker for line in log.splitlines()) else None

current = wait_for("real guest hang " + mode, live_hang)
unit = "sandbox-builder@" + current["run_id"] + ".service"
cg, processes = snapshot_processes(unit)
assert current["execution_claimed"] == 1 and current["runtime_vswitch_port"], current
wt, wb = register(mode + "-waiter")
trigger(wt, wb, "echo ISSUE372_NEXT_" + wb + "; sleep 3")
waiting = row(wb)
assert waiting and waiting["status"] == "waiting" and waiting["execution_claimed"] == 0, waiting
before = row(bid)
admin = json.loads(command(str(Path(args.bin) / "node-ctl"), "builder", "status", "--socket", args.socket))
owned = [b for b in admin["builds"] if b["buildID"] == bid]
assert len(owned) == 1 and owned[0]["templateID"] == tid and owned[0]["executionClaimed"], admin
require("DELETE", "/templates/" + tid, 409)
require("DELETE", "/templates/" + tid + "?cancel=true", 409, header='{"cancel":false}')
assert row(bid)["cancel_requested_unix"] == 0 and not unit_empty(unit), "ordinary DELETE interrupted the hang"
started = time.monotonic()
if mode == "cancel":
    require("POST", f"/templates/{tid}/builds/{bid}/cancel", 202)

if mode == "timeout":
    cancelled = wait_for("normal total timeout cleans automatically",
                         lambda: check_terminal(tid, bid, "error"), args.timeout_only + 60)
    assert not cancelled["cancelRequested"] and not cancelled["deleteRequested"], cancelled
    assert row(bid)["execution_claimed"] == 0
    age = time.time() - before["execution_claimed_unix"]
    assert args.timeout_only - 10 <= age <= args.timeout_only + 60, age
elif mode == "cancel":
    cancelled = wait_for("cancel completes", lambda: check_terminal(tid, bid, "error"), 60)
    assert cancelled["cancelRequested"] and not cancelled["deleteRequested"], cancelled
    require("POST", f"/templates/{tid}/builds/{bid}/cancel", 204)
    assert row(bid)["execution_claimed"] == 0
assert_reclaimed(bid, unit, cg, processes)
elapsed = time.monotonic() - started
if mode != "timeout":
    assert elapsed < 60, "cancellation fell back to execution/step timeout"
ready = wait_for("next normal Build completes", lambda: check_terminal(wt, wb, "ready"))
assert not ready["cancelRequested"] and not ready["deleteRequested"], ready
next_log = command("journalctl", "KUASAR_BUILD_ID=" + wb, "--no-pager", "--output=cat")
assert any(line.strip() == "ISSUE372_NEXT_" + wb for line in next_log.splitlines()), "waiting Build never ran its guest step"
slots = json.loads(command(str(Path(args.bin) / "connector-ctl"), "vswitch", "show", "slots", args.switch))
slot = [s for s in slots if str(s["port"]) == before["runtime_vswitch_port"]]
assert len(slot) == 1 and not slot[0]["allocated"], (before["runtime_vswitch_port"], slot)
assert row(wb)["execution_claimed"] == 0
# A successful Build's original transient ID still routes after its
# result has changed to a canonical reference; deletion cannot touch it.
canonical = ready["templateID"]
expected_kind = "img"
assert canonical.startswith("e2b-" + expected_kind + "-"), (expected_kind, canonical)
# Image reuse has the exact same PersistID, so deleting the waiter
# must preserve a concurrently retained Build of the same artifact.
peer = None
pt, pb = register(mode + "-canonical-peer")
require("POST", f"/v2/templates/{pt}/builds/{pb}", 202, {"fromTemplate": canonical})
peer_ready = wait_for("same-artifact peer completes", lambda: check_terminal(pt, pb, "ready"))
assert peer_ready["templateID"] == canonical, (peer_ready, canonical)
peer_before = row(pb)
assert pt != wt and pb != wb and peer_before["persist_id"] == row(wb)["persist_id"] == canonical
assert peer_before["execution_claimed"] == 0, peer_before
peer = (pt, pb, peer_before)

# Standalone Create takes the canonical ID. Cluster Create follows its
# group template configuration; cluster reference coverage below uses
# Build fromTemplate and the simultaneously retained image peer.
existing_sid = create_sandbox(canonical)
require("DELETE", "/templates/" + canonical, 400)
require("DELETE", "/templates/" + wt, 204)
assert row(wb) is None
if existing_sid:
    existing, _ = require("GET", "/sandboxes/" + existing_sid, 200)
    assert existing["state"] == "running", existing
    exec_sandbox(existing_sid)
    delete_sandbox(existing_sid)
if peer:
    pt, pb, peer_before = peer
    peer_ready, _ = require("GET", status_path(pt, pb), 200)
    assert peer_ready["status"] == "ready" and peer_ready["templateID"] == canonical, peer_ready
    assert row(pb) == peer_before, (peer_before, row(pb))

sid = create_sandbox(canonical)
delete_sandbox(sid)
rt, rb = register(mode + "-canonical-reuse")
require("POST", f"/v2/templates/{rt}/builds/{rb}", 202, {"fromTemplate": canonical})
reused = wait_for("canonical fromTemplate survives Build deletion", lambda: check_terminal(rt, rb, "ready"))
assert reused["templateID"].split("-", 2)[:2] == canonical.split("-", 2)[:2], (reused, canonical)
assert reused["templateID"] == canonical, (reused, canonical)
require("DELETE", "/templates/" + rt, 204)
if peer:
    require("DELETE", "/templates/" + peer[0], 204)
    assert row(peer[1]) is None
if mode in ("cancel", "timeout"):
    assert row(bid) is not None, "diagnostic row disappeared"
    require("DELETE", "/templates/" + tid, 204)
fds_after = conductor_fds()
for build_id in (bid, wb, rb) + ((peer[1],) if peer else ()):
    for root in (args.run_root, args.base_root):
        directory = str(Path(root) / "builds" / build_id)
        assert not any(fd == directory or fd.startswith(directory + "/") for fd in fds_after), (build_id, fds_after)
item = {"conductor_pid": args.conductor_pid, "conductor_fds_before": len(fds_before),
        "conductor_fds_after": len(fds_after), "build_directory_fds_retained": 0, "mode": mode, "hang_build": bid, "transient_id": tid, "unit": unit,
        "phase_sandbox": before["phase_sandbox_id"], "port": before["runtime_vswitch_port"],
        "cgroup": str(cg), "stopped_pids": list(processes), "seconds": round(elapsed, 3),
        "next_build": wb, "next_status": "ready", "canonical_reused": canonical,
        "canonical_kind": expected_kind, "create_exec_verified": True,
        "existing_sandbox": existing_sid, "existing_sandbox_exec_after_delete": bool(existing_sid),
        "same_artifact_peer": peer[1] if peer else None, "same_artifact_peer_preserved": bool(peer)}
Path(args.evidence).write_text(json.dumps(item, indent=2) + "\n")
print("==> PASS: issue372 " + json.dumps(item), flush=True)
PY_ACTION
}
# ---- #372: actual hang cancellation and immediate capacity recovery --------
# Keep diagnostics past these assertions; restore the canonical TTL/capacity
# afterwards so the existing retention and resource-vector cases still run.
stop_conductor
python3 - "$WORK/config.yaml" <<'PY_CONFIG'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text()
assert "max_builds: 2" in s and "terminal_ttl: 5s" in s
p.write_text(s.replace("max_builds: 2", "max_builds: 1").replace("terminal_ttl: 5s", "terminal_ttl: 1h"))
PY_CONFIG
start_conductor
run_interruption_contract 0 build-actions
# Normal timeout is exercised separately from cancellation at the 1200-second deadline.
stop_conductor
python3 - "$WORK/config.yaml" <<'PY_CONFIG'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text()
assert "total_timeout_sec: 1200" in s
p.write_text(s.replace("total_timeout_sec: 1200", "total_timeout_sec: 30"))
PY_CONFIG
start_conductor
run_interruption_contract 30 build-timeout
stop_conductor
python3 - "$WORK/config.yaml" <<'PY_CONFIG'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text()
p.write_text(s.replace("max_builds: 1\n", "max_builds: 2\n").replace("terminal_ttl: 1h", "terminal_ttl: 5s").replace("total_timeout_sec: 30", "total_timeout_sec: 1200"))
PY_CONFIG
start_conductor

# ---- BF: deterministic business failure → API error + collected unit -------
echo "==> BF: deterministic RUN failure keeps API/journal evidence without a failed unit"
register issue178-failed-build
BF_TID="$TID"; BF_BID="$BID"
BF_BODY=$(python3 - "$SOURCE_IMAGE" <<'PY'
import json, sys
print(json.dumps({
    "fromTemplate": sys.argv[1],
    "steps": [{"type": "RUN", "args": ["echo ISSUE178_BUILDER_FAILURE >&2; exit 78"]}],
}))
PY
)
code=$(req POST "/v2/templates/$BF_TID/builds/$BF_BID" "$AK" "$BF_BODY")
[ "$code" = "202" ] || { cat "$WORK/resp.body"; fail "BF trigger = $code (want 202)"; }
wait_error "$BF_TID" "$BF_BID" BF
assert_terminal_build_unowned "$BF_BID" error || fail "BF terminal row retained execution ownership"
BF_UNIT=$(build_journal_unit "$BF_BID") || fail "BF journal lost its builder unit identity"
wait_unit_journal_contains "$BF_UNIT" ISSUE178_BUILDER_FAILURE "$WORK/bf.journal" \
    || { diag "$BF_BID"; fail "BF journal was not retained after the business failure"; }
wait_unit_collected "$BF_UNIT" || fail "$BF_UNIT remained loaded and failed"
code=$(req GET "/templates/$BF_TID/builds/$BF_BID/status" "$AK")
[ "$code" = "200" ] && [ "$(json_field "$WORK/resp.body" status)" = "error" ] \
    || fail "BF API did not retain terminal error status"
BF_BEFORE_RETRY=$(build_trigger_signature "$BF_BID")
code=$(req POST "/v2/templates/$BF_TID/builds/$BF_BID" "$AK" \
    "{\"fromImage\":\"$PULL_REF\",\"force\":true}")
[ "$code" = "409" ] || { cat "$WORK/resp.body"; fail "BF second trigger = $code (want 409)"; }
assert_trigger_conflict error || fail "BF second trigger did not report exact error state"
[ "$(build_trigger_signature "$BF_BID")" = "$BF_BEFORE_RETRY" ] \
    || fail "BF second trigger changed the terminal build row"
[ "$(failed_unit_count 'sandbox-builder@*.service')" = "$BUILDER_FAILED_BASE" ] \
    || fail "BF increased the failed builder unit count"
echo "==> PASS: ready/error build ids reject force retries without mutation; ready remains listed"
echo "==> PASS: BF status=error, journal retained, sandbox-builder instance collected"

echo "PASS builder.failure.sh"
