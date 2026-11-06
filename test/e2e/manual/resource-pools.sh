#!/usr/bin/env bash
# Manual two-NUMA-node VM regression using prepared E2E products and helpers.
set -euo pipefail
CHECK="${E2E_LIB:?}/orchestrator/resource_pool_check.py"
: "${RESOURCE_POOL_NUMA_NODES:?supply two NUMA nodes}"
read -r NODE0 NODE1 <<<"$RESOURCE_POOL_NUMA_NODES"
[ -n "$NODE0" ] && [ -n "$NODE1" ] && [ "$NODE0" != "$NODE1" ]
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
BUILDER_CPU=2
EXTRA_PREFIX="sandbox-pool514-${RUN_KEY}@"
EXTRA_UNIT="$EXTRA_PREFIX.service"
for path in "$UNIT_DIR/$EXTRA_UNIT" "$UNIT_DIR/$EXTRA_UNIT.d"; do
    execute_state_path_absent "$path" || fail "foreign extra unit exists: $path"
done
LINKS=()
pool_cleanup() {
    set +e
    [ -e "$PAUSE_BARRIER_TARGET" ] && : > "$PAUSE_BARRIER_RELEASE"
    stop_orchestrator
    stop_owned_units "$EXTRA_PREFIX" "$RUNNER_PREFIX" "$BUILDER_PREFIX"
    for path in "${LINKS[@]}"; do rm -f -- "$path"; done
    cleanup
}
trap pool_cleanup EXIT
mkdir -p "$WORK/pool-units" "$CASE_OUT"
printf '%s\n' "$WORK" "$RUNNER_PREFIX" "$BUILDER_PREFIX" "$EXTRA_PREFIX" >"$CASE_OUT/owned-pool-units.txt"
cpu_for_node() { python3 - "$1" <<'PY'
import pathlib, sys
text = pathlib.Path('/sys/devices/system/node/node' + sys.argv[1] + '/cpulist').read_text().strip()
all_cpus = []
for part in text.split(','):
    a, _, b = part.partition('-')
    all_cpus.extend(range(int(a), int(b or a) + 1))
assert len(all_cpus) >= 2
print(','.join(map(str, all_cpus[:2])))
PY
}
CPU0="$(cpu_for_node "$NODE0")"; CPU1="$(cpu_for_node "$NODE1")"
for i in 0 1; do
    if [ "$i" = 0 ]; then node="$NODE0"; cpus="$CPU0"; else node="$NODE1"; cpus="$CPU1"; fi
    cat >"$WORK/numa$i.conf" <<EOF
[Service]
CPUAffinity=${cpus//,/ }
NUMAPolicy=bind
NUMAMask=$node
AllowedCPUs=${cpus//,/ }
AllowedMemoryNodes=$node
EOF
done
write_orchestrator_config unset static
python3 - "$WORK/config.yaml" "$WORK" "$RUNNER_PREFIX" "$BUILDER_PREFIX" "$EXTRA_PREFIX" <<'PY'
import pathlib, re, sys
path, work, runner, builder, extra = sys.argv[1:]
p = pathlib.Path(path)
s = p.read_text()
s = re.sub(r'(?ms)^units:.*?(?=^sandbox:)', f'''units:
  dir: {work}/pool-units
  runner_pools:
    - {{name: first, unit: '{runner}.service', resource_pool: numa0, size: 0, capacity: 2, priority: 100}}
    - {{name: second, unit: '{extra}.service', resource_pool: numa1, size: 0, capacity: 4}}
  builder_pools:
    - {{name: builder, unit: '{builder}.service', resource_pool: numa1, size: 0, capacity: 1}}
''', s)
s += f'''
resource_listen:
  enabled: true
  socket: {work}/sandbox-resource.sock
  state_path: {work}/resource-state.json
  pressure: {{interval: 1s, minimum_run_time: 90s, critical_exit_hold: 1s, red_to_yellow_hold: 1s, yellow_to_green_hold: 1s}}
  admission: {{rate: 50, burst: 50, startup_ttl: 180s, queue_ttl: 30s, queue_max_depth: 256}}
  pools:
    - name: numa0
      capacity: {{cpu: 8, memory: 16GiB}}
      allocatable: {{cpu: 2, memory: 12GiB}}
      unit_dropins: [{work}/numa0.conf]
    - name: numa1
      capacity: {{cpu: 16, memory: 32GiB}}
      allocatable: {{cpu: 16, memory: 12GiB}}
      unit_dropins: [{work}/numa1.conf]
'''
p.write_text(s)
PY
# Unit generation stays in the private work directory; only exact owned links
# enter systemd's search path. This also avoids any other conductor's fragments.
for unit in "$RUNNER_PREFIX.service" "$BUILDER_PREFIX.service" "$EXTRA_UNIT" sandbox-runner.slice sandbox-builder.slice; do
    link="$UNIT_DIR/$unit"
    ln -s "$WORK/pool-units/$unit" "$link"
    LINKS+=("$link")
    case "$unit" in *@.service)
        ln -s "$WORK/pool-units/$unit.d" "$link.d"
        LINKS+=("$link.d") ;;
    esac
done
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template
for n in $(seq 1 60); do
    if python3 "$CHECK" ledger "$WORK" 0 0 >"$CASE_OUT/after-build.json" 2>"$WORK/ledger.err"; then break; fi
    sleep 0.5
done
python3 "$CHECK" ledger "$WORK" 0 0 >"$CASE_OUT/after-build.json"
echo '==> PASS: real Builder produced snapshot; phase scheduling/runtime charges released'
declare -A TOKENS FORWARDS ENVDTOKENS RUNS
for i in 0 1; do
    create_guest "{\"templateID\":\"$TEMPLATE\",\"timeout\":600}"
    TOKENS[$SID]="$(issue_exec_session "$SID" "$AK")"
    FORWARDS[$SID]="$FORWARD_TOKEN"; ENVDTOKENS[$SID]="$ENVD_TOKEN"
    exec_through_connect "$SID" "${TOKENS[$SID]}" "POOL_$i"
    wait_sandbox_state "$SID" running 1200 || fail 'guest not running'
    read -r pool run unit < <(python3 "$CHECK" owner "$WORK" "$SID")
    RUNS[$SID]="$run"
    if [ "$pool" = numa0 ]; then SOURCE_SID="$SID"; else OTHER_SID="$SID"; fi
done
python3 "$CHECK" ledger "$WORK" 1 1 >"$CASE_OUT/before-restart.json"
python3 "$CHECK" placement "$WORK" "$SOURCE_SID" "$NODE0" "$CPU0" | tee "$CASE_OUT/source-placement.json"
python3 "$CHECK" placement "$WORK" "$OTHER_SID" "$NODE1" "$CPU1" | tee "$CASE_OUT/other-placement.json"
SID="$SOURCE_SID"; ENVD_SOCK="$EXECUTE_RUN_ROOT/sandboxes/$SID/envd.sock"
ENVD_TOKEN="${ENVDTOKENS[$SID]}"; FORWARD_TOKEN="${FORWARDS[$SID]}"
. "$SCRIPT_DIR/guest_service.sh"
start_freeze_service
COUNTER_BEFORE="$FREEZE_COUNTER"
"$BIN/sandbox-ctl" exec --path-id "$SID" --run-root "$EXECUTE_RUN_ROOT/sandboxes" -- /bin/sh -ceu 'echo pool514-persistent >/home/user/pool514-marker'
# Restart only the owned conductor; runners and Proxy keep their identities.
old_pid="$ORCH_PID"
kill -TERM "$old_pid"; wait "$old_pid" || true
for i in "${!PIDS[@]}"; do [ "${PIDS[$i]}" = "$old_pid" ] && unset 'PIDS[i]'; done
ORCH_PID=""
start_conductor "$WORK/orch-restarted.log"
for n in $(seq 1 120); do
    if python3 "$CHECK" ledger "$WORK" 1 1 >"$CASE_OUT/after-restart.json" 2>"$WORK/ledger.err"; then break; fi
    sleep 0.5
done
python3 "$CHECK" ledger "$WORK" 1 1 >"$CASE_OUT/after-restart.json"
for sid in "$SOURCE_SID" "$OTHER_SID"; do [ "$(sandbox_run_id "$sid")" = "${RUNS[$sid]}" ] || fail 'restart changed live run'; done
echo '==> PASS: conductor restart preserved both live pool identities and exact capacity/CPU ledgers'
printf '%s\n' "$SOURCE_SID" >"$PAUSE_BARRIER_TARGET"
for n in $(seq 1 240); do [ -e "$PAUSE_BARRIER_REACHED" ] && break; sleep 0.5; done
[ -e "$PAUSE_BARRIER_REACHED" ] || fail 'active balance did not capture critical source'
"$BIN/node-ctl" resource pressure --socket "$WORK/node-ctl.socket" >"$CASE_OUT/capture-intent.json"
python3 - "$CASE_OUT/capture-intent.json" "$SOURCE_SID" <<'PY'
import json, sys
s = json.load(open(sys.argv[1])); row = next(x for x in s['sandboxes'] if x['sandbox_id'] == sys.argv[2])
assert row['recovery_obligation'] and row['source_pool'] == 'numa0' and row['destination_pool'] == 'numa1', row
PY
: >"$PAUSE_BARRIER_RELEASE"
rm -f "$PAUSE_BARRIER_TARGET"
for n in $(seq 1 240); do
    if [ "$(sandbox_state "$SOURCE_SID")" = running ] && [ "$(sandbox_run_id "$SOURCE_SID")" != "${RUNS[$SOURCE_SID]}" ]; then break; fi
    sleep 0.5
done
python3 "$CHECK" ledger "$WORK" 0 2 >"$CASE_OUT/after-balance.json"
python3 "$CHECK" placement "$WORK" "$SOURCE_SID" "$NODE1" "$CPU1" | tee "$CASE_OUT/destination-placement.json"
SID="$SOURCE_SID"; freeze_service_probe || fail 'memory service lost across pools'
[ "$FREEZE_COUNTER" -ge "$COUNTER_BEFORE" ] || fail 'memory counter restarted'
"$BIN/sandbox-ctl" exec --path-id "$SID" --run-root "$EXECUTE_RUN_ROOT/sandboxes" -- /bin/sh -ceu 'test "$(cat /home/user/pool514-marker)" = pool514-persistent'
echo '==> PASS: idle-first active balance captured a durable recovery obligation, moved the VM across NUMA, preserved memory process and disk state'
sleep 10
python3 "$CHECK" ledger "$WORK" 0 2 >"$CASE_OUT/stable-after-balance.json"
code="$(req POST "/sandboxes/$SID/pause" "$AK" '{}')"
[ "$code" = 204 ] || fail "manual pause=$code"
wait_paused_cleanup "$SID" 1200 || fail 'manual pause cleanup'
exec_through_connect "$SID" "${TOKENS[$SID]}" 'MANUAL_CROSS_POOL'
wait_sandbox_state "$SID" running 1200 || fail 'manual resume'
python3 "$CHECK" ledger "$WORK" 1 1 >"$CASE_OUT/after-manual-resume.json"
python3 "$CHECK" placement "$WORK" "$SID" "$NODE0" "$CPU0" | tee "$CASE_OUT/manual-destination.json"
freeze_service_probe || fail 'memory service lost on manual cross-pool resume'
echo '==> PASS: ordinary Pause/Resume selected the empty pool and preserved the guest process'
