#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/cluster_env.sh"

. "$SCRIPT_DIR/cluster_lifecycle_flow.sh"

GROUP=/e2e/cluster/lifecycle
ROUTE_KEY=user1/lifecycle
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MANIFEST_KEY")"
API_SECRET_FP="$("$BIN/e2b-key-ctl" fingerprint "$API_SECRET")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
start_store_zot_vswitch
make_ext4_templates
build_template_with_standalone_node
write_group_record
write_registry_configs 1
write_placer_router_configs
start_cluster_control_plane
NODE_ID=real-node-1
start_cluster_node "$NODE_ID"
wait_cluster_node_key_pair
GROUP_API_KEY="$CLUSTER_API_KEY" python3 "$SCRIPT_DIR/group_readiness.py" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-host "api.$DOMAIN" --node-id "$NODE_ID" --group "$GROUP" \
    --registry-url "http://127.0.0.1:$CONTROL_PORT"
run_cluster_flow
# Prove Build ownership routing through the group, then recover retained status
# through an empty Router cache. New writes wait for Group import/authentication.
PYTHONPATH="$SCRIPT_DIR" BUILD_ACTION_API_KEY="$CLUSTER_API_KEY" python3 - \
    --url "http://127.0.0.1:$ROUTER_PORT" --host "api.$DOMAIN" --group "$GROUP" \
    --db "$WORK/cl/node-ctl.db" --run-root "$WORK/cr" --base-root "$WORK/cl" \
    --socket "$WORK/cn.sock" --bin "$BIN" --switch "$SWITCH" --conductor-pid "$CLUSTER_CONDUCTOR_PID" \
    --source "$TEMPLATE_REF" --evidence "$WORK/build-actions.json" \
    --restart-request "$WORK/restart-request" --restart-ready "$WORK/restart-ready" \
    --node-url "http://127.0.0.1:$NODE_PORT" --node-id "$NODE_ID" \
    --registry-url "http://127.0.0.1:$CONTROL_PORT" >"$WORK/cluster-build.log" 2>&1 <<'PY_CLUSTER_BUILD' &
from build_client import *
args = configure()
fds_before = conductor_fds()
tid, bid = register("cluster-hang")
marker = "CLUSTER_BUILD_HANG_" + bid
trigger(tid, bid, "echo " + marker + "; while :; do sleep 1; done")

def live_hang():
    current = row(bid)
    assert current and current["status"] != "error", current
    log = command("journalctl", "KUASAR_BUILD_ID=" + bid, "--no-pager", "--output=cat")
    return current if current["run_id"] and current["phase_sandbox_id"] and marker in log.splitlines() else None

owned = wait_for("routed Build guest RUN", live_hang)
unit = "sandbox-builder@" + owned["run_id"] + ".service"
cg, processes = snapshot_processes(unit)
assert owned["execution_claimed"] == 1 and owned["runtime_vswitch_port"], owned
admin = json.loads(command(str(Path(args.bin)/"node-ctl"), "builder", "status", "--socket", args.socket))
matching = [b for b in admin["builds"] if b["buildID"] == bid]
assert len(matching) == 1 and matching[0]["templateID"] == tid and matching[0]["executionClaimed"], admin
wt, wb = register("cluster-waiter")
trigger(wt, wb, "echo CLUSTER_BUILD_NEXT_" + wb)
waiting = row(wb)
assert waiting["status"] == "waiting" and waiting["execution_claimed"] == 0, waiting
require("DELETE", "/templates/" + tid, 409)
require("DELETE", "/templates/" + tid + "?cancel=true", 409, header='{"cancel":false}')
assert row(bid)["cancel_requested_unix"] == 0 and not unit_empty(unit)
started = time.monotonic()
require("POST", f"/templates/{tid}/builds/{bid}/cancel", 202)
cancelled = wait_for("routed cancellation finishes", lambda: check_terminal(tid, bid, "error"), 60)
assert cancelled["cancelRequested"] and not cancelled["deleteRequested"], cancelled
require("POST", f"/templates/{tid}/builds/{bid}/cancel", 204)
assert row(bid)["execution_claimed"] == 0
assert_reclaimed(bid, unit, cg, processes)
elapsed = time.monotonic() - started
assert elapsed < 60, "cancellation waited for the normal deadline"
ready = wait_for("queued routed Build obtains capacity", lambda: check_terminal(wt, wb, "ready"))
assert not ready["cancelRequested"] and not ready["deleteRequested"], ready
assert row(wb)["execution_claimed"] == 0
log = command("journalctl", "KUASAR_BUILD_ID=" + wb, "--no-pager", "--output=cat")
assert "CLUSTER_BUILD_NEXT_" + wb in log.splitlines(), log
slots = json.loads(command(str(Path(args.bin)/"connector-ctl"), "vswitch", "show", "slots", args.switch))
released = [s for s in slots if str(s["port"]) == owned["runtime_vswitch_port"]]
assert len(released) == 1 and not released[0]["allocated"], released
canonical = ready["templateID"]
assert canonical.startswith("e2b-img-"), canonical
Path(args.restart_request).touch()
wait_for("fixture Router/Registry restart", lambda: Path(args.restart_ready).exists(), 60)
value, _ = require("GET", status_path(wt, wb), 200)
assert value["status"] == "ready" and value["templateID"] == canonical, value
args.post_restart = True

pt, pb = register("cluster-same-artifact-peer")
require("POST", f"/v2/templates/{pt}/builds/{pb}", 202, {"fromTemplate": canonical})
peer = wait_for("same-artifact peer ready", lambda: check_terminal(pt, pb, "ready"))
assert peer["templateID"] == canonical
peer_before = row(pb)
assert pt != wt and pb != wb and peer_before["persist_id"] == row(wb)["persist_id"] == canonical
assert peer_before["execution_claimed"] == 0
require("DELETE", "/templates/" + canonical, 400)
require("DELETE", "/templates/" + wt, 204)
assert row(wb) is None
peer, _ = require("GET", status_path(pt, pb), 200)
assert peer["status"] == "ready" and peer["templateID"] == canonical
assert row(pb) == peer_before
rt, rb = register("cluster-canonical-reuse")
require("POST", f"/v2/templates/{rt}/builds/{rb}", 202, {"fromTemplate": canonical})
reused = wait_for("canonical survives transient deletion", lambda: check_terminal(rt, rb, "ready"))
assert reused["templateID"] == canonical
for transient, build_id in ((rt, rb), (pt, pb), (tid, bid)):
    assert row(build_id) is not None
    require("DELETE", "/templates/" + transient, 204)
    assert row(build_id) is None
fds_after = conductor_fds()
for build_id in (bid, wb, pb, rb):
    for root in (args.run_root, args.base_root):
        directory = str(Path(root)/"builds"/build_id)
        assert not any(fd == directory or fd.startswith(directory + "/") for fd in fds_after)
Path(args.evidence).write_text(json.dumps({"cancel_seconds": elapsed, "canonical": canonical,
    "retained_status_after_restart": True, "peer_preserved": True,
    "conductor_fds_before": len(fds_before), "conductor_fds_after": len(fds_after),
    "build_directory_fds_retained": 0}, indent=2) + "\n")
PY_CLUSTER_BUILD
    ACTION_TEST_PID=$!
    PIDS+=("$ACTION_TEST_PID")
    while kill -0 "$ACTION_TEST_PID" 2>/dev/null; do
        if [ -e "$WORK/restart-request" ] && [ ! -e "$WORK/restart-ready" ]; then
            kill "$ROUTER_PID"
            wait "$ROUTER_PID" || true
            for i in "${!PIDS[@]}"; do
                [ "${PIDS[$i]}" != "$ROUTER_PID" ] || PIDS[$i]=""
            done
            "$BIN/run-cluster-ctl" router --config "$WORK/router.yaml" >>"$WORK/router.log" 2>&1 &
            ROUTER_PID=$!
            PIDS+=("$ROUTER_PID")
            wait_port "$ROUTER_PORT" router-restarted
            touch "$WORK/restart-ready"
            step "restarted router; testing retained transient routing from empty caches"
        fi
        sleep 0.1
    done
    action_status=0
    wait "$ACTION_TEST_PID" || action_status=$?
    for i in "${!PIDS[@]}"; do
        [ "${PIDS[$i]}" != "$ACTION_TEST_PID" ] || PIDS[$i]=""
    done
    [ "$action_status" = 0 ] || fail "Build action capacity/restart acceptance"
echo "PASS orchestrator.cluster-lifecycle.sh"
