#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template
CFG_HOST=e2e-cfg-host
REQ_NET_HEADER="{\"hostname\":\"$CFG_HOST\"}"
create_guest
unset REQ_NET_HEADER
wait_sandbox_state "$SID" running 1200 || fail "sandbox did not run"
# ---- native exec capability -> CONNECT -> sandbox-ctl -> real guest -------
echo "==> issue an explicit native exec capability (create has no default token)"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" '{"conditions":[]}')" || fail "issue unrestricted [] native exec capability"
rm -f "$WORK/exec-session.secret"
NATIVE_MARK="NATIVE_EXEC_$RANDOM"
exec_through_connect "$SID" "$EXEC_TOKEN" "$NATIVE_MARK"
EXACT_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv == ['/bin/true']\"}]}")" \
    || fail "issue exact-argv exec capability"
exec_argv_allowed_through_connect "$SID" "$EXACT_EXEC_TOKEN" /bin/true
exec_argv_denied_through_connect "$SID" "$EXACT_EXEC_TOKEN"
OR_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.argv == ['/bin/true'] || request.argv == ['/usr/bin/true']\"}]}")" \
    || fail "issue OR exec capability"
exec_argv_allowed_through_connect "$SID" "$OR_EXEC_TOKEN" /usr/bin/true
PTY_MARK="NATIVE_EXEC_PTY_$RANDOM"
PTY_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" "{\"conditions\":[{\"expr\":\"request.cwd == '/tmp' && request.user == '0:0' && request.stdio.tty\"}]}")" \
    || fail "issue cwd/user/tty exec capability"
exec_pty_resize_through_connect "$SID" "$PTY_EXEC_TOKEN" "$PTY_MARK"
echo "==> PASS: real sandbox-ctl CONNECT enforced omitted/[]/exact/OR/cwd/user/tty conditions and preserved stdio, PTY resize, exit status"

MARK="HELLO_FROM_GUEST_$RANDOM"
echo "==> exec in guest: sh -c 'hostname; id; echo $MARK; uname -sm'"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "hostname; id; echo $MARK; uname -sm" > "$WORK/exec.out" 2>&1 || true
sed 's/^/  guest| /' "$WORK/exec.out"
grep -q "$MARK" "$WORK/exec.out" || fail "guest command output missing $MARK (envd exec failed; see above)"
grep -q 'EXIT_CODE 0' "$WORK/exec.out" || fail "guest command exit code != 0"
echo "==> PASS: command executed in guest (saw $MARK, exit 0)"
# Sandbox-config injection (docs/node.md §4.4): the X-Kuasar-Sandbox-Network header set the guest
# hostname, which is observed through the real guest command.
grep -q "$CFG_HOST" "$WORK/exec.out" || fail "guest hostname was not injected"

# ---- Proxy netns -> floatingip user port ----------------------------------
USER_MARK="proxy-netns-user-port-$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "mkdir -p /home/user/e2e-site; echo '$USER_MARK' > /home/user/e2e-site/index.html; cd /home/user/e2e-site; python3 -m http.server 8000 --bind 0.0.0.0 >/tmp/e2e-http-8000.log 2>&1 &" \
    >"$WORK/start-user-port.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/start-user-port.out" || { sed 's/^/  envd| /' "$WORK/start-user-port.out"; fail "start guest user-port server"; }
ok=""
for _ in $(seq 1 30); do
    code=$(DP_MAX_TIME=8 dp "8000-$SID" / "$FORWARD_TOKEN" || true)
    grep -q "$USER_MARK" "$WORK/dp.body" 2>/dev/null && { ok=1; break; }
    sleep 0.5
done
[ -n "$ok" ] || { echo "last code=$code"; cat "$WORK/dp.body"; sed 's/^/  orch| /' "$WORK/orch.log"; collect_proxy_log "$WORK/proxy.log"; fail "Proxy netns -> floatingip user port did not return marker"; }
echo "==> PASS: Proxy worker reached sandbox floatingip:8000 from proxy_netns (marker=$USER_MARK)"

code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "delete=$code"
wait_sandbox_state "$SID" missing 120 || fail "delete finalizer retained row"
[ ! -e "$WORK/run/sandboxes/$SID" ] && [ ! -e "$WORK/lib/sandboxes/$SID" ] || fail "delete retained owned directories"
[ -f "$WORK/lib/node-ctl.db" ] && [ -S "$WORK/node-ctl.socket" ] || fail "finalizer removed node files"
echo "PASS orchestrator.exec.sh"
