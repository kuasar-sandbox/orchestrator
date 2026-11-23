#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/execute_checkpoint.sh"
write_orchestrator_config unset controller
sed -i '/^resource_listen:$/a\  watermarks: { startup_factor: 0.8 }' "$WORK/config.yaml"
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_image_template

systemd_body() { # template, mode
    python3 - "$1" "$2" <<'PY'
import json, sys
print(json.dumps({'templateID': sys.argv[1], 'timeout': 600,
                  'metadata': {'kuasar-sandbox.launch': json.dumps({'systemd': sys.argv[2]})}}))
PY
}
systemd_health() {
    wait_sandbox_state "$SID" running 1200 || fail "systemd sandbox did not run"
    local status
    status=$(dp "49983-$SID" /health "$ENVD_TOKEN")
    [ "$status" = 200 ] || [ "$status" = 204 ] || fail "envd health=$status"
}
systemd_state() {
    checkpoint_guest /bin/sh -ceu '
        test "$(cat /proc/1/comm)" = systemd
        systemctl is-active --quiet sandbox-init.service
        test -z "$(cat /run/sandbox-init/cgroup/cgroup.procs)"
        for c in cpu io memory pids; do
            grep -qw "$c" /run/sandbox-init/cgroup/cgroup.subtree_control
        done
        systemctl show sandbox-init.service -p InvocationID --value
        for f in /proc/[0-9]*/comm; do
            if test "$(cat "$f" 2>/dev/null)" = envd; then
                pid=${f#/proc/}; pid=${pid%/comm}
                stat=$(cat "${f%/comm}/stat")
                stat=${stat##*) }
                set -- $stat
                printf "ENVD_IDENTITY=%s:%s\n" "$pid" "${20}"
                tr "\000" " " <"${f%/comm}/cmdline"
                echo
            fi
        done
    ' >"$1"
    sed -n '1p' "$1" >"${1%.out}.invocation.out"
    grep -Eq '^[0-9a-f]{32}$' "${1%.out}.invocation.out" || fail "invalid executor InvocationID"
    sed -n 's/^ENVD_IDENTITY=//p' "$1" >"${1%.out}.envd-identity.out"
    [ "$(wc -l <"${1%.out}.envd-identity.out")" = 1 ] || fail "expected one envd process"
    grep -Eq '^[1-9][0-9]*:[0-9]+$' "${1%.out}.envd-identity.out" || fail "invalid envd PID/starttime"
    grep -q -- '-cgroup-root[= ]/run/sandbox-init/cgroup' "$1" || fail "envd delegation root"
}
systemd_delete() {
    code=$(req DELETE "/sandboxes/$SID" "$AK")
    [ "$code" = 204 ] || fail "systemd delete=$code"
    wait_sandbox_state "$SID" missing 1200 || fail "systemd delete finalizer"
    [ ! -e "$CHECKPOINT_ROOT/$SID" ] && [ ! -e "$EXECUTE_RUN_ROOT/sandboxes/$SID" ] \
        || fail "systemd delete retained owned directories"
}

# E2B owns every other Launch key, including an explicit false value.
bad_body=$(python3 - "$TEMPLATE" <<'PY'
import json, sys
print(json.dumps({'templateID': sys.argv[1], 'metadata': {
    'kuasar-sandbox.launch': json.dumps({'systemd': 'runtime', 'placeholder': False})}}))
PY
)
code=$(req POST /sandboxes "$AK" "$bad_body")
[ "$code" = 400 ] || fail "E2B accepted a product-owned Launch field: $code"

create_guest "$(systemd_body "$TEMPLATE" runtime)"
systemd_health
systemd_state "$WORK/systemd-before.out"
checkpoint_guest /bin/sh -ceu 'cat /proc/sys/kernel/random/uuid >/run/systemd-api-token; cat /run/systemd-api-token' \
    >"$WORK/systemd-token.out"

# Frozen time must not consume the observer's five-second healthy-state budget.
pause_checkpoint_guest snapshot 0
sleep 6
resume_checkpoint_guest 0
systemd_health
systemd_state "$WORK/systemd-s-resume.out"
cmp "$WORK/systemd-before.envd-identity.out" "$WORK/systemd-s-resume.envd-identity.out"
cmp "$WORK/systemd-before.out" "$WORK/systemd-s-resume.out"
checkpoint_guest /bin/cat /run/systemd-api-token >"$WORK/systemd-s-token.out"
cmp "$WORK/systemd-token.out" "$WORK/systemd-s-token.out"

# Publish E once, then exercise both inherited mode and explicit off from it.
pause_checkpoint_guest sandbox 0
E2B_API_KEY="$AK" timeout -k 5s 300 "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --to-template --json --socket "$WORK/node-ctl.socket" \
    >"$CASE_OUT/systemd-export.json" 2>"$WORK/systemd-export.log"
SYSTEMD_TEMPLATE=$(json_field "$CASE_OUT/systemd-export.json" result)
resume_checkpoint_guest 0
systemd_health
systemd_state "$WORK/systemd-e-resume.out"
if cmp -s "$WORK/systemd-before.invocation.out" "$WORK/systemd-e-resume.invocation.out"; then
    fail "E cold resume retained the old executor invocation"
fi
checkpoint_guest /bin/test ! -e /run/systemd-api-token

# A real Guest stop hook distinguishes graceful Delete from VMM termination.
checkpoint_guest /bin/sh -ceu '
    mkdir -p /etc/systemd/system
    cat >/etc/systemd/system/systemd-api-stop.service <<EOF
[Unit]
Description=API graceful shutdown witness
[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/true
ExecStop=/bin/sh -c "echo SYSTEMD_API_STOP >/dev/console"
EOF
    systemctl daemon-reload
    systemctl start systemd-api-stop.service
'
DELETED_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$DELETED_RUN_ID" ] || fail "missing current run before Delete"
systemd_delete
wait_unit_journal_contains "${RUNNER_PREFIX}${DELETED_RUN_ID}.service" \
    SYSTEMD_API_STOP "$WORK/systemd-delete.journal" || fail "Delete omitted Guest stop hook"

create_guest "{\"templateID\":\"$SYSTEMD_TEMPLATE\",\"timeout\":600}"
systemd_health
systemd_state "$WORK/systemd-inherited.out"
systemd_delete

create_guest "$(systemd_body "$SYSTEMD_TEMPLATE" off)"
systemd_health
checkpoint_guest /bin/sh -ceu '
    test "$(cat /proc/1/comm)" != systemd
    for f in /proc/[0-9]*/comm; do
        if test "$(cat "$f" 2>/dev/null)" = envd; then
            tr "\000" " " <"${f%/comm}/cmdline"
            echo
        fi
    done
' >"$WORK/systemd-off.out"
grep -q -- '-cgroup-root[= ]/sys/fs/cgroup' "$WORK/systemd-off.out" || fail "explicit off kept systemd delegation"
systemd_delete
echo "PASS orchestrator.systemd.sh"
