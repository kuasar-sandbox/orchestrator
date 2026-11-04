#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
NATIVE_USAGE_ENABLED=false
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/proxy_guest.sh"
PROXY_WORKERS=2
PROXY_PARK_TIMEOUT=10s
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
wait_proxy_topology_ready "$PROXY_PID"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template
# Public UTC condition is usable under a normal clock, without the automatic
# BOOTTIME anchor; the default policy remains disabled.
create_guest "{\"templateID\":\"$TEMPLATE\",\"timeout\":600}"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")"
exec_through_connect "$SID" "$EXEC_TOKEN" "PUBLIC_IDLE_CONDITION"
# Establish the new UTC condition after complete Worker observation recovery.
# Earlier clock changes cannot certify history in the previous Master lifetime.
PROXY_MASTER_PID=$PROXY_PID
restart_proxy_master_fresh "$WORK/proxy-public.log"
sleep 0.2
X=$(python3 -c 'from datetime import datetime,timezone;print(datetime.now(timezone.utc).isoformat())')
sleep 0.2
code=$(req POST "/sandboxes/$SID/pause" "$AK" "{\"ifIdle\":true,\"ifIdleSince\":\"$X\"}")
[ "$code" = 204 ] || { cat "$WORK/resp.body"; fail "public continuous idle=$code"; }
code=$(req POST "/sandboxes/$SID/pause" "$AK" "{\"ifIdle\":true,\"ifIdleSince\":\"$X\"}")
[ "$code" = 409 ] || fail "paused continuous idle=$code"
code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "public idle cleanup=$code"
echo 'PASS orchestrator.idle-condition.sh'
