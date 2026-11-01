#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
# 8 MiB correctness/retention workload. Comparative measurements live in
# test/perf/checkpoint_platform.sh and require explicitly selected prepared inputs.
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/execute_checkpoint.sh"
unset CHECKPOINT_PERF_MODE
write_orchestrator_config unset controller
# This serial storage scenario builds a 6 GiB template. Admit that single
# startup on 12 GiB developer hosts without changing production defaults or
# the controller's measured physical-memory limit.
sed -i '/^resource_listen:$/a\  watermarks: { startup_factor: 0.8 }' "$WORK/config.yaml"
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template
python3 - "$RUN_ARGV_LOG" "$SNAPSHOT_ARGV_LOG" <<'PY'
import json, sys
runs, captures = ([json.loads(line) for line in open(path) if line.strip()] for path in sys.argv[1:])
assert runs and captures, (runs, captures)
phases = set()
for args in runs:
    assert '--enable-checkpoint' not in args, args
    phases.add(args[args.index('--path-id') + 1])
assert {'a', 'c'} <= phases <= {'a', 'b', 'c'}, phases
for args in captures:
    assert '--checkpoint' not in args, args
    assert args[args.index('--path-id') + 1] == 'c', args
PY

create_checkpoint_guest 8
write_checkpoint_payload 8
checkpoint_hash >"$WORK/checkpoint-original.hash"
checkpoint_disk_state "$SID" "$WORK/checkpoint-original.disks"
checkpoint_measure before_pause "$(checkpoint_clock)" 8
pause_checkpoint_guest snapshot 8
checkpoint_local_metadata "$SID" "$WORK/checkpoint-first.json"
checkpoint_disk_state "$SID" "$WORK/checkpoint-paused.disks"
checkpoint_same_disks "$WORK/checkpoint-original.disks" "$WORK/checkpoint-paused.disks"
resume_checkpoint_guest 8
checkpoint_wait_released "$SID" || fail "successful memory resume did not release versions"
checkpoint_hash >"$WORK/checkpoint-first-resume.hash"
cmp "$WORK/checkpoint-original.hash" "$WORK/checkpoint-first-resume.hash"
checkpoint_disk_state "$SID" "$WORK/checkpoint-resumed.disks"
checkpoint_same_disks "$WORK/checkpoint-original.disks" "$WORK/checkpoint-resumed.disks"

pause_checkpoint_guest sandbox 8
checkpoint_local_metadata "$SID" "$WORK/checkpoint-second.json"
python3 - "$WORK/checkpoint-first.json" "$WORK/checkpoint-second.json" <<'PY'
import json, sys
before, after = (json.load(open(path)) for path in sys.argv[1:])
assert before['checkpoint_id'] != after['checkpoint_id'], (before, after)
assert before['root'] == after['root'] and (before.get('disks') or []) == (after.get('disks') or []), (before, after)
assert not __import__('os').path.exists(before['artifact']), 'obsolete E was not selectively cleaned'
PY

# Hold an admitted real publisher before it reads the source. Resume and guest
# writes proceed, but the fixed version must survive until publication exits.
printf '%s\n' "$CHECKPOINT_ROOT/$SID/checkpoint" >"$PUBLISH_BARRIER_TARGET"
PUBLISH_STARTED=$(checkpoint_clock)
E2B_API_KEY="$AK" timeout -k 5s 300 "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --to-template --json --socket "$WORK/node-ctl.socket" \
    >"$WORK/checkpoint-publish.json" 2>"$WORK/checkpoint-publish.log" &
PUBLISH_PID=$!
PIDS+=("$PUBLISH_PID")
for _ in $(seq 1 300); do
    [ ! -e "$PUBLISH_BARRIER_REACHED" ] || break
    kill -0 "$PUBLISH_PID" 2>/dev/null || { cat "$WORK/checkpoint-publish.log"; fail "publisher exited before barrier"; }
    sleep 0.1
done
[ -e "$PUBLISH_BARRIER_REACHED" ] || fail "publisher never acquired source ownership"
resume_checkpoint_guest 8
checkpoint_disk_state "$SID" "$WORK/checkpoint-cold-resumed.disks"
checkpoint_same_disks "$WORK/checkpoint-original.disks" "$WORK/checkpoint-cold-resumed.disks"
checkpoint_hash >"$WORK/checkpoint-cold-resume.hash"
cmp "$WORK/checkpoint-original.hash" "$WORK/checkpoint-cold-resume.hash"
checkpoint_list "$SID" "$WORK/checkpoint-held.json"
python3 - "$WORK/checkpoint-second.json" "$WORK/checkpoint-held.json" <<'PY'
import json, sys
meta, refs = (json.load(open(path)) for path in sys.argv[1:])
want = {(disk['diff_id'], meta['checkpoint_id']) for disk in [meta['root'], *(meta.get('disks') or [])]}
assert {(ref['diff_id'], ref['id']) for ref in refs} == want, (refs, want)
PY
write_checkpoint_payload 8
checkpoint_hash >"$WORK/checkpoint-active.hash"
if cmp -s "$WORK/checkpoint-original.hash" "$WORK/checkpoint-active.hash"; then fail "overwrite did not change data"; fi
# Guest fsync/sync is not a host BlockCOW writeback boundary. A temporary named
# capture explicitly drains accepted writes while the original version remains
# held. Its content-addressed E stays case-owned until final BaseDir deletion.
# This fence is test preparation, not a native Pause/export performance sample.
FENCE_STARTED=$(checkpoint_clock)
FENCE_ID=$(python3 "$SCRIPT_DIR/disk_checkpoint.py" new-id)
python3 "$SCRIPT_DIR/disk_checkpoint.py" fence-state "$WORK/lib/node-ctl.db" "$SID" "$WORK/checkpoint-second.json" \
    >"$WORK/checkpoint-fence.before.json"
timeout -k 5s 120 "$ORCH_BIN_DIR/sandbox-ctl" export --json --resume --checkpoint "$FENCE_ID" \
    --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local \
    --run-root "$EXECUTE_RUN_ROOT/sandboxes" >"$WORK/checkpoint-fence.export.json"
checkpoint_guest /bin/true
checkpoint_list "$SID" "$WORK/checkpoint-fence.held.json"
python3 "$SCRIPT_DIR/disk_checkpoint.py" fence-refs "$WORK/checkpoint-second.json" \
    "$WORK/checkpoint-fence.export.json" "$WORK/checkpoint-fence.held.json" "$CHECKPOINT_ROOT/$SID/checkpoint" "$FENCE_ID" \
    >"$WORK/checkpoint-fence.refs.json"
mapfile -t FENCE_DIFF_IDS < <(python3 - "$WORK/checkpoint-fence.refs.json" <<'PY'
import json, sys
for ref in json.load(open(sys.argv[1])): print(ref['diff_id'])
PY
)
[ "${#FENCE_DIFF_IDS[@]}" -gt 0 ] || fail "temporary fence returned no exact DIFF identities"
FENCE_DELETE_ARGS=()
for diff_id in "${FENCE_DIFF_IDS[@]}"; do FENCE_DELETE_ARGS+=(--diff-id "$diff_id"); done
timeout -k 2s 30 "$BIN/sandbox-ctl" checkpoint delete --path-id "$SID" --checkpoint "$FENCE_ID" \
    "${FENCE_DELETE_ARGS[@]}" --run-root "$EXECUTE_RUN_ROOT/sandboxes" --json >"$WORK/checkpoint-fence.deleted.json"
checkpoint_list "$SID" "$WORK/checkpoint-fence.remaining.json"
python3 - "$WORK/checkpoint-held.json" "$WORK/checkpoint-fence.remaining.json" \
    "$WORK/checkpoint-fence.refs.json" "$WORK/checkpoint-fence.deleted.json" <<'PY'
import json, sys
def refs(path): return sorted((ref['diff_id'], ref['id']) for ref in json.load(open(path)))
assert refs(sys.argv[1]) == refs(sys.argv[2]), 'temporary fence changed the original held versions'
assert refs(sys.argv[3]) == refs(sys.argv[4]), 'temporary fence deletion did not match its exact refs'
PY
python3 "$SCRIPT_DIR/disk_checkpoint.py" fence-state "$WORK/lib/node-ctl.db" "$SID" "$WORK/checkpoint-second.json" \
    >"$WORK/checkpoint-fence.after.json"
cmp "$WORK/checkpoint-fence.before.json" "$WORK/checkpoint-fence.after.json"
kill -0 "$PUBLISH_PID" 2>/dev/null && [ -e "$PUBLISH_BARRIER_REACHED" ] && [ ! -e "$PUBLISH_BARRIER_RELEASE" ] \
    || fail "publisher did not remain held through the writeback fence"
checkpoint_measure test_writeback_fence "$FENCE_STARTED" 8
RETAINED_BYTES=$(checkpoint_allocated "$SID")
checkpoint_measure export_held_after_overwrite "$(checkpoint_clock)" 8
: >"$PUBLISH_BARRIER_RELEASE"
wait "$PUBLISH_PID" || { cat "$WORK/checkpoint-publish.log"; fail "detached template publication failed"; }
for index in "${!PIDS[@]}"; do
    [ "${PIDS[$index]}" != "$PUBLISH_PID" ] || unset 'PIDS[index]'
done
rm -f "$PUBLISH_BARRIER_TARGET" "$PUBLISH_BARRIER_REACHED" "$PUBLISH_BARRIER_RELEASE"
checkpoint_measure publish_including_test_barrier "$PUBLISH_STARTED" 8
checkpoint_wait_released "$SID" || fail "publisher completion did not release versions"

# Background GC is bounded. Require actual allocated-byte reduction, allowing
# up to three minutes for reclamation; List=[] alone cannot pass this check.
GC_STARTED=$(checkpoint_clock)
RECLAIMED=0
for _ in $(seq 1 180); do
    CURRENT_BYTES=$(checkpoint_allocated "$SID")
    if [ "$CURRENT_BYTES" -le "$((RETAINED_BYTES - 4 * 1024 * 1024))" ]; then RECLAIMED=1; break; fi
    sleep 1
done
[ "$RECLAIMED" = 1 ] || fail "GC did not reclaim 4 MiB of retained overwrite: before=$RETAINED_BYTES after=$CURRENT_BYTES"
checkpoint_measure after_version_gc "$GC_STARTED" 8
checkpoint_hash >"$WORK/checkpoint-after-gc.hash"
cmp "$WORK/checkpoint-active.hash" "$WORK/checkpoint-after-gc.hash"

PUBLISHED_TEMPLATE=$(json_field "$WORK/checkpoint-publish.json" result)
PUBLISHED_E=$(json_field "$WORK/checkpoint-publish.json" sandboxRef)
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$PUBLISHED_E" >"$WORK/checkpoint-portable.json"
python3 - "$WORK/checkpoint-portable.json" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
root = cfg['Boot']['Root']
nodes = [root, *(cfg['Boot'].get('Disks') or [])]
# The reader validates ordinary E's one-self binding; Checkpoint E has no self.
self_count = sum(node.get('Base') == 'self' for node in nodes)
self_count += sum((node.get('Overlay') or {}).get('Base') == 'self' for node in nodes)
assert self_count == 1, cfg['Boot']
PY
delete_checkpoint_guest
TEMPLATE="$PUBLISHED_TEMPLATE"
create_checkpoint_guest 8
checkpoint_hash >"$WORK/checkpoint-published.hash"
cmp "$WORK/checkpoint-original.hash" "$WORK/checkpoint-published.hash"
delete_checkpoint_guest
echo "==> PASS: named versions, same-directory reuse, export overlap, physical GC and source-independent cold restore"

# Hold capture after the Proxy gate has ACKed. An authenticated native exec
# arriving now must park, Wake, and run on the resumed incarnation.
create_checkpoint_guest 8
write_checkpoint_payload 8
checkpoint_hash >"$WORK/retained-original.hash"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")"
printf '%s\n' "$SID" >"$PAUSE_BARRIER_TARGET"
rm -f "$PAUSE_BARRIER_REACHED" "$PAUSE_BARRIER_RELEASE"
curl -sS --noproxy '*' --max-time 300 -o "$WORK/retained-pause.json" -w '%{http_code}' \
    -X POST -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" -H 'Content-Type: application/json' \
    --data '{"checkpoint":true,"ifIdle":true}' "http://127.0.0.1:$PORT/sandboxes/$SID/pause" \
    >"$WORK/retained-pause.code" 2>"$WORK/retained-pause.log" &
RETAIN_PAUSE_PID=$!
PIDS+=("$RETAIN_PAUSE_PID")
for _ in $(seq 1 1500); do
    [ ! -e "$PAUSE_BARRIER_REACHED" ] || break
    kill -0 "$RETAIN_PAUSE_PID" 2>/dev/null || fail "retained Pause failed before capture gate"
    sleep 0.02
done
[ -e "$PAUSE_BARRIER_REACHED" ] || fail "retained Pause did not reach capture gate"
[ "$(sandbox_state "$SID")" = pausing ] || fail "capture did not publish pausing"
exec_through_connect "$SID" "$EXEC_TOKEN" "WAKE_DURING_PAUSE_$RANDOM" &
PAUSING_EXEC_PID=$!
PIDS+=("$PAUSING_EXEC_PID")
PARKED=0
for _ in $(seq 1 100); do
    kill -0 "$PAUSING_EXEC_PID" 2>/dev/null || fail "pausing exec failed before capture completed"
    code=$(req GET "/sandboxes/$SID/stats/traffic" "$AK")
    if [ "$code" = 200 ] && python3 - "$WORK/resp.body" <<'PY_STATS'
import json, sys
stats = json.load(open(sys.argv[1]))
raise SystemExit(0 if stats['inflight']['parking'] > 0 else 1)
PY_STATS
    then PARKED=1; break; fi
    sleep 0.1
 done
[ "$PARKED" = 1 ] || fail "request did not park while capture was held"
: >"$PAUSE_BARRIER_RELEASE"
wait "$RETAIN_PAUSE_PID" || fail "retained Pause request failed"
[ "$(cat "$WORK/retained-pause.code")" = 200 ] || fail "retained Pause did not return checkpointID"
RETAINED_ID=$(json_field "$WORK/retained-pause.json" checkpointID)
wait "$PAUSING_EXEC_PID" || fail "pausing request did not survive Pause/Wake"
for index in "${!PIDS[@]}"; do
    if [ "${PIDS[$index]}" = "$RETAIN_PAUSE_PID" ] || [ "${PIDS[$index]}" = "$PAUSING_EXEC_PID" ]; then unset 'PIDS[index]'; fi
done
rm -f "$PAUSE_BARRIER_TARGET" "$PAUSE_BARRIER_REACHED" "$PAUSE_BARRIER_RELEASE"
wait_sandbox_state "$SID" running 1200 || fail "pausing Wake did not resume sandbox"
checkpoint_list "$SID" "$WORK/retained-after-resume.json"
python3 - "$WORK/retained-after-resume.json" "$RETAINED_ID" <<'PY'
import json, sys
refs = json.load(open(sys.argv[1]))
assert refs and all(ref['id'] == sys.argv[2] for ref in refs), refs
PY
code=$(req POST "/sandboxes/$SID/export" "$AK" '{"toTemplate":true}')
[ "$code" = 400 ] || fail "default export after Resume=$code (want 400)"
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"checkpoint":true}')
[ "$code" = 409 ] || fail "retained checkpoint quota=$code (want 409)"
[ "$(sandbox_state "$SID")" = running ] || fail "quota rejection paused the sandbox"
write_checkpoint_payload 8
checkpoint_hash >"$WORK/retained-current.hash"
if cmp -s "$WORK/retained-original.hash" "$WORK/retained-current.hash"; then fail "overwrite did not change guest data"; fi
# Ordinary Pause still succeeds, and its offline cleanup must keep the old S/E.
pause_checkpoint_guest sandbox 8
resume_checkpoint_guest 8
E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" --checkpoint "$RETAINED_ID" \
    --to-template --json --socket "$WORK/node-ctl.socket" >"$WORK/retained-export.json"
[ "$(sandbox_state "$SID")" = running ] || fail "checkpoint export changed sandbox lifecycle"
checkpoint_wait_released "$SID" || fail "consumed checkpoint versions were not released"
code=$(req DELETE "/sandboxes/$SID/checkpoints/$RETAINED_ID" "$AK")
[ "$code" = 404 ] || fail "exported checkpoint still exists"
RETAINED_TEMPLATE=$(json_field "$WORK/retained-export.json" result)
# Direct deletion releases quota, while parent deletion owns remaining data.
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":false,"checkpoint":true}')
[ "$code" = 200 ] || fail "second retained Pause=$code"
DELETE_CHECKPOINT_ID=$(json_field "$WORK/resp.body" checkpointID)
code=$(req DELETE "/sandboxes/$SID/checkpoints/$DELETE_CHECKPOINT_ID" "$AK")
[ "$code" = 204 ] || fail "direct checkpoint delete=$code"
resume_checkpoint_guest 8
checkpoint_wait_released "$SID" || fail "direct deletion did not release versions"
delete_checkpoint_guest
TEMPLATE="$RETAINED_TEMPLATE"
create_checkpoint_guest 8
checkpoint_hash >"$WORK/retained-exported.hash"
cmp "$WORK/retained-original.hash" "$WORK/retained-exported.hash"
delete_checkpoint_guest
echo "==> PASS: pausing Wake, retained capture across Resume/ordinary Pause, quota, export consumption and direct deletion"

# Verify the actual business launches as well as the Build launches above.
python3 - "$RUN_ARGV_LOG" <<'PY'
import json, sys
business = []
for line in open(sys.argv[1]):
    if not line.strip(): continue
    args = json.loads(line)
    builder = args[args.index('--path-id') + 1] in ('a', 'b', 'c')
    assert args.count('--enable-checkpoint') == int(not builder), args
    if not builder: business.append(args)
assert business, 'no managed business sandbox launches observed'
PY
echo "PASS orchestrator.checkpoint.sh"
