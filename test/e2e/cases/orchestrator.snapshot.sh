#!/usr/bin/env bash
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
write_orchestrator_config unset controller
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
. "$SCRIPT_DIR/execute_template.sh"
build_ready_template
create_guest "$(python3 - "$TEMPLATE" <<'PY_BODY'
import json,sys
print(json.dumps({"templateID":sys.argv[1],"timeout":120,"metadata":{"kuasar-sandbox.restore":json.dumps({"prefetch":"memory"})}}))
PY_BODY
)"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")"
exec_through_connect "$SID" "$EXEC_TOKEN" "READY_$RANDOM"
wait_sandbox_state "$SID" running 1200 || fail "sandbox did not reach running"
MARK="STATE_$RANDOM"
. "$SCRIPT_DIR/guest_service.sh"
start_freeze_service
PERSIST="PERSIST_$MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "echo $PERSIST > /home/user/persist.txt" >"$WORK/wr.out"
grep -q 'EXIT_CODE 0' "$WORK/wr.out" || fail "write snapshot marker"
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{}')
[ "$code" = 204 ] || fail "baseline snapshot=$code"
wait_paused_cleanup "$SID" || fail "baseline cleanup"
B_LOCAL="$CHECKPOINT_ROOT/$SID/checkpoint/$SID.snapshot"
B_ARTIFACT="$(readlink -f "$B_LOCAL")"
B_SNAPSHOT_BASENAME="$(basename "$B_ARTIFACT")"
"$BIN/sandbox-ctl" info --json "$B_LOCAL" >"$WORK/b-local.json"
exec_through_connect "$SID" "$EXEC_TOKEN" "BASELINE_RESTORE_$RANDOM"
EXACT_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK" '{"conditions":[{"expr":"request.argv == [\"/bin/true\"]"}]}')"
# ---- local B -> working-set W -> independent portable publication --------
# The second local Pause keeps memory self separate while disks still merge.
# Make the restored disk delta observably different from B: a content-addressed
# merge with no intervening writes can legitimately reproduce B's top digest.
# This W-only marker makes removal of B's old disk top a meaningful proof.
W_DISK_PERSIST="W_DISK_PERSIST_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "echo $W_DISK_PERSIST > /home/user/working-set-disk.txt" >"$WORK/w-disk-write.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/w-disk-write.out" \
    || { sed 's/^/  guest| /' "$WORK/w-disk-write.out"; fail "write W-only disk marker"; }
# Removing B's old root-disk top before promotion proves publish treats
# B.snapshot as an opaque memory lower instead of recursively publishing B's
# stale disk graph.
PORTABLE_W_CALL=$(snapshot_argv_count)
freeze_service_probe || fail "envd freeze service disappeared before portable W pause"
FREEZE_COUNTER_BEFORE_W=$FREEZE_COUNTER
PORTABLE_W_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$PORTABLE_W_RUN_ID" ] || fail "working-set source runner id is empty"
code=$(req POST "/sandboxes/$SID/pause" "$AK" \
    '{"memory":true,"checkpoint_merge_ref":false,"checkpoint_drop_caches":false}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "portable W pause=$code (want 204)"; }
assert_snapshot_argv "$PORTABLE_W_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode local --run-root "$WORK/run/sandboxes" \
    --merge-ref=false --drop-caches=false \
    || fail "portable W Pause policy did not reach sandbox-ctl exactly"
W_PORTABLE_LOCAL="$CHECKPOINT_ROOT/$SID/checkpoint/$SID.snapshot"
[ -f "$W_PORTABLE_LOCAL" ] || fail "working-set Pause did not create $W_PORTABLE_LOCAL"
W_PORTABLE_ARTIFACT="$(readlink -f "$W_PORTABLE_LOCAL")"
[ "$W_PORTABLE_ARTIFACT" != "$B_ARTIFACT" ] || fail "working-set W reused B memory self"
"$BIN/sandbox-ctl" info --json "$W_PORTABLE_LOCAL" >"$WORK/w-portable-local.json" \
    || fail "local working-set W is unreadable"
PORTABLE_W_UNIT="${RUNNER_PREFIX}$PORTABLE_W_RUN_ID.service"
wait_unit_journal_contains "$PORTABLE_W_UNIT" \
    'quiesce: guest acked (drop_caches=skipped' "$WORK/w-portable-local.journal" \
    || { tail -40 "$WORK/w-portable-local.journal" | sed 's/^/  unit| /'; fail "working-set Pause did not preserve guest page cache"; }
B_ROOT_TOP_BASENAME=$(python3 - "$WORK/b-local.json" "$WORK/w-portable-local.json" "$B_SNAPSHOT_BASENAME" <<'PY'
import json, os, sys

with open(sys.argv[1], encoding="utf-8") as source:
    parent = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    working = json.load(source)

parent_refs = parent.get("FromRefs") or []
memory_refs = working.get("FromRefs") or []
if len(memory_refs) != len(parent_refs) + 1 or not memory_refs[0].startswith("file://"):
    raise SystemExit(f"working-set from_refs={memory_refs!r}, want local B plus {parent_refs!r}")
memory_path = memory_refs[0][len("file://"):].split("@", 1)[0]
if os.path.basename(memory_path) != sys.argv[3]:
    raise SystemExit(f"working-set memory lower={memory_refs[0]!r}, want {sys.argv[3]!r}")
if memory_refs[1:] != parent_refs:
    raise SystemExit(f"working-set lower tail={memory_refs[1:]!r}, want inherited {parent_refs!r}")

def top(node):
    overlay = node.get("Overlay")
    return overlay["Base"] if overlay else node["Base"]

def chain(node):
    overlay = node.get("Overlay")
    return (overlay.get("BaseFromRefs") if overlay else node.get("BaseFromRefs")) or []

parent_top = top(parent["Boot"]["Root"])
working_root = working["Boot"]["Root"]
if parent_top == top(working_root) or parent_top in chain(working_root):
    raise SystemExit(f"W retained B disk top {parent_top!r}; local disks must merge")
if not parent_top.startswith("file://"):
    raise SystemExit(f"B root disk top is not local: {parent_top!r}")
relative = parent_top[len("file://"):].split("@", 1)[0]
if (
    relative != os.path.basename(relative)
    or not relative.endswith((".overlay", ".sandbox"))
):
    raise SystemExit(f"refusing to remove unexpected B disk ref {parent_top!r}")
print(relative)
PY
) || fail "local B/W graph validation failed"
B_ROOT_TOP_PATH="$CHECKPOINT_ROOT/$SID/checkpoint/$B_ROOT_TOP_BASENAME"
wait_paused_cleanup "$SID" || fail "W checkpoint cleanup did not finish"
[ ! -e "$B_ROOT_TOP_PATH" ] || fail "unused B root disk top remains after managed checkpoint cleanup: $B_ROOT_TOP_PATH"
echo "==> PASS: W -> local B is separate; managed cleanup retired B's merged disk top"

W_LOCAL_PAIR=$(checkpoint_pair "$SID" snapshot "$W_PORTABLE_LOCAL") || fail "invalid W capture pair"
W_RUNTIME_REF=$(checkpoint_runtime_ref "$W_LOCAL_PAIR" "$CHECKPOINT_ROOT/$SID/checkpoint")
PROMOTION_TOKEN=$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --socket "$WORK/node-ctl.socket") \
    || fail "independent keep-source export of local W failed"
case "$PROMOTION_TOKEN" in kmt1.*) ;; *) fail "export-sandbox returned a non-KMT result" ;; esac
# The sealed KMT token hides its artifact ref, so publish once more as a
# template id (plain <profile>-<kind>-<base64url(ref)>) to learn the portable
# ref the export produced.
W_TEMPLATE_ID=$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --to-template --socket "$WORK/node-ctl.socket") \
    || fail "template export of retained W failed"
PORTABLE_W_REF=$(python3 - "$W_TEMPLATE_ID" <<'PY'
import base64, sys
parts = sys.argv[1].split("-", 2)
if len(parts) != 3:
    raise SystemExit(f"template id {sys.argv[1]!r}: want <profile>-<kind>-<base64url-ref>")
print(base64.urlsafe_b64decode(parts[2] + "=" * (-len(parts[2]) % 4)).decode())
PY
) || fail "W template id did not decode to a portable ref"
PORTABLE_W_KEY="${PORTABLE_W_REF#manifest://}"
[[ "$PORTABLE_W_REF" == manifest://* && "$PORTABLE_W_KEY" =~ ^[0-9a-f]{64}$ ]] \
    || fail "published W ref is not manifest://<64hex>: $PORTABLE_W_REF"
# keep-source must leave the paused source row bit-for-bit unchanged after both
# exports, and must retain the local checkpoint it resumes from (#336).
[ "$(checkpoint_pair "$SID" snapshot "$W_PORTABLE_LOCAL")" = "$W_LOCAL_PAIR" ] \
    || fail "keep-source export rewrote the W S/E pair"
[ -e "$CHECKPOINT_ROOT/$SID/checkpoint" ] || fail "keep-source export removed the retained W checkpoint"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$PORTABLE_W_REF" >"$WORK/w-portable-manifest.json" \
    || fail "published W is not readable from the manifest store"
PORTABLE_LAYER_SUMMARY=$(python3 - "$WORK/w-portable-manifest.json" "$WORK/b-local.json" "$PORTABLE_W_REF" <<'PY'
import json, re, sys
with open(sys.argv[1], encoding="utf-8") as source:
    working = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    parent = json.load(source)
refs = working.get("FromRefs") or []
parent_refs = parent.get("FromRefs") or []
if len(refs) != len(parent_refs) + 1:
    raise SystemExit(
        f"portable W from_refs={refs!r}, want {len(parent_refs) + 1} published memory layers"
    )
invalid_refs = [ref for ref in refs if re.fullmatch(r"manifest://[0-9a-f]{64}", ref) is None]
if invalid_refs:
    raise SystemExit(f"portable W has non-manifest memory refs: {invalid_refs!r}")
if refs[0] == sys.argv[3]:
    raise SystemExit("portable W self and B memory lower collapsed to one ref")
print(refs[0][len("manifest://"):], len(refs))
PY
) || fail "portable W/B layer validation failed"
read -r PORTABLE_B_KEY PORTABLE_PARENT_LAYERS <<<"$PORTABLE_LAYER_SUMMARY"
echo "==> PASS: independent keep-source export published distinct W self and opaque B memory layer"

exec_argv_denied_through_connect "$SID" "$EXACT_EXEC_TOKEN"
wait_sandbox_state "$SID" paused 20 \
    || fail "condition-denied direct exec resumed the paused sandbox"
wait_proxy_traffic_stats "$SID" paused \
    || fail "condition-denied direct exec changed paused traffic accounting"
RESUME_MARK="PORTABLE_W_RESUME_$RANDOM"
W_RESUME_RUN_CALL=$(run_argv_count)
exec_through_connect "$SID" "$EXEC_TOKEN" "$RESUME_MARK"
resumed=""
for _ in $(seq 1 90); do
    code=$(curl -sS --max-time 1 --unix-socket "$ENVD_SOCK" \
        -o /dev/null -w '%{http_code}' http://envd/health 2>/dev/null || true)
    case "$code" in 200|204) resumed=1; break ;; esac
    sleep 0.5
done
[ -n "$resumed" ] || fail "retained W did not resume from its local checkpoint"
freeze_service_probe || fail "envd freeze service/listener missing after retained W resume"
FREEZE_COUNTER_AFTER_W=$FREEZE_COUNTER
FREEZE_DELTA_W=$((FREEZE_COUNTER_AFTER_W - FREEZE_COUNTER_BEFORE_W))
[ "$FREEZE_DELTA_W" -ge 0 ] && [ "$FREEZE_DELTA_W" -lt 100 ] \
    || fail "envd service counter advanced across frozen W resume window: before=$FREEZE_COUNTER_BEFORE_W after=$FREEZE_COUNTER_AFTER_W"
sleep 1
freeze_service_probe || fail "envd freeze service stopped after retained W resume"
[ "$FREEZE_COUNTER" -gt "$FREEZE_COUNTER_AFTER_W" ] \
    || fail "envd freeze service did not resume counter after retained W resume"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" \
    >"$WORK/portable-read.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/portable-read.out" || { sed 's/^/  guest| /' "$WORK/portable-read.out"; fail "retained W lost guest state"; }
# The working-set memory intentionally retained guest cache, so evict it before
# reading the W-only file. This makes the assertion prove the retained local
# checkpoint's disk artifact, independently of the restored memory self/lower chain.
"$BIN/sandbox-ctl" exec --path-id "$SID" --run-root "$WORK/run/sandboxes" -- /bin/sh -c \
    'sync && echo 3 > /proc/sys/vm/drop_caches && cat /home/user/working-set-disk.txt' \
    >"$WORK/portable-disk-read.out" 2>&1 || true
grep -q "$W_DISK_PERSIST" "$WORK/portable-disk-read.out" \
    || { sed 's/^/  guest| /' "$WORK/portable-disk-read.out"; fail "retained W lost merged W-only disk state"; }
W_RESUME_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$W_RESUME_RUN_ID" ] || fail "retained W resume runner id is empty"
# The runner's restore source must be the retained local checkpoint — the same
# ref the source row kept. Its memory prefetch must not be manifest-backed; a
# local artifact stream does not expose the prefetch capability, so accept
# either backend=file prefetch log variant (started or skipped).
assert_run_source_mode "$W_RESUME_RUN_CALL" "$SID" restore \
    || fail "retained W resume did not execute run --restore"
assert_run_option_value "$W_RESUME_RUN_CALL" "$SID" "--restore" "$W_RUNTIME_REF" \
    || fail "retained W resume did not select the retained local checkpoint"
W_RESUME_UNIT="${RUNNER_PREFIX}$W_RESUME_RUN_ID.service"
# The prefetch identity ("backend=file parent_layers=N") is emitted whether or
# not the local stream exposes the prefetch capability; vhost disk stats also
# log "backend=...", so anchor on parent_layers to match the memory line only.
wait_unit_journal_contains "$W_RESUME_UNIT" \
    "backend=file parent_layers=" "$WORK/keep-source-w.journal" \
    || { tail -40 "$WORK/keep-source-w.journal" | sed 's/^/  unit| /'; fail "retained W resume did not use the local checkpoint as its memory backend"; }
MANIFEST_PREFETCH_COUNT=$(grep -Fc 'memory prefetch started backend=manifest' "$WORK/keep-source-w.journal" || true)
[ "$MANIFEST_PREFETCH_COUNT" = "0" ] || fail "retained W resume prefetched $MANIFEST_PREFETCH_COUNT memory layer(s) from the manifest store, want no manifest memory prefetch"
echo "==> PASS: keep-source W resumed envd-managed PID/listener from its retained local checkpoint (frozen delta=$FREEZE_DELTA_W); no manifest-backed memory prefetch"

code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill first sandbox before KMT import=$code (want 204)"
wait_sandbox_state "$SID" missing 120 || fail "Sandbox delete finalizer retained durable row"
[ ! -e "$WORK/run/sandboxes/$SID" ] || fail "Sandbox delete retained RunDir"
[ ! -e "$WORK/lib/sandboxes/$SID" ] || fail "Sandbox delete retained BaseDir"
[ ! -e "$CHECKPOINT_ROOT/$SID/checkpoint" ] || fail "Sandbox delete retained the keep-source checkpoint"
[ -f "$WORK/lib/node-ctl.db" ] || fail "Sandbox cleanup removed node-level database"
[ -S "$WORK/node-ctl.socket" ] || fail "Sandbox cleanup removed node-level config socket"
unset EXEC_TOKEN

echo "==> KMT missing import -> paused -> durable starting -> asynchronous restore"
code=$(curl -sS --noproxy '*' --max-time 30 -o "$WORK/resp.body" -w '%{http_code}' \
    -X POST -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" \
    -H "X-Kuasar-Migration-Token: $PROMOTION_TOKEN" \
    -H 'Content-Type: application/json' --data '{"timeout":119}' \
    "http://127.0.0.1:$PORT/sandboxes/$SID/connect")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "KMT Connect=$code (want 200)"; }
[ "$(json_field "$WORK/resp.body" sandboxID)" = "$SID" ] || fail "KMT Connect changed sandbox identity"
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
KMT_RETURN_STATE="$(sandbox_state "$SID")"
case "$KMT_RETURN_STATE" in starting|running) ;; *) fail "KMT Connect returned with state=$KMT_RETURN_STATE";; esac
KMT_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue exec capability during KMT starting"
rm -f "$WORK/exec-session.secret"
KMT_MARK="KMT_RESTORE_$RANDOM"
exec_through_connect "$SID" "$KMT_EXEC_TOKEN" "$KMT_MARK"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" "cat /home/user/persist.txt" >"$WORK/kmt-read.out" 2>&1 || true
grep -q "$PERSIST" "$WORK/kmt-read.out" \
    || { sed 's/^/  guest| /' "$WORK/kmt-read.out"; fail "KMT restore lost portable guest state"; }
wait_sandbox_state "$SID" running 20 || fail "KMT restore did not commit running"
# The source resumed from its retained local checkpoint above; the KMT import
# is what proves the published portable artifact: it must restore from the
# manifest store, prefetching exactly the W self layer (#336).
KMT_RESTORE_RUN_ID=$(sandbox_run_id "$SID")
[ -n "$KMT_RESTORE_RUN_ID" ] || fail "KMT restore runner id is empty"
KMT_RESTORE_UNIT="${RUNNER_PREFIX}$KMT_RESTORE_RUN_ID.service"
wait_unit_journal_contains "$KMT_RESTORE_UNIT" \
    "memory prefetch started backend=manifest parent_layers=$PORTABLE_PARENT_LAYERS key=$PORTABLE_W_KEY" \
    "$WORK/kmt-w.journal" || { tail -40 "$WORK/kmt-w.journal" | sed 's/^/  unit| /'; fail "KMT restore did not prefetch W self from the manifest store"; }
KMT_MANIFEST_PREFETCH_COUNT=$(grep -Fc 'memory prefetch started backend=manifest' "$WORK/kmt-w.journal" || true)
[ "$KMT_MANIFEST_PREFETCH_COUNT" = "1" ] || fail "KMT restore started $KMT_MANIFEST_PREFETCH_COUNT manifest prefetches, want W self only"
grep -Fq "memory prefetch started backend=manifest parent_layers=$PORTABLE_PARENT_LAYERS key=$PORTABLE_B_KEY" \
    "$WORK/kmt-w.journal" && fail "KMT restore prefetched B memory lower"
echo "==> PASS: KMT Connect returned at $KMT_RETURN_STATE; immediate native exec parked, portable state restored, prefetch targeted W self only"
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill KMT-imported sandbox=$code (want 204)"
wait_sandbox_state "$SID" missing 120 || fail "KMT sandbox delete finalizer retained durable row"
unset KMT_EXEC_TOKEN
SID_UNSET="$SID"

# ---- bundle Pause -> local restore -> exact Store promotion ---------------
stop_orchestrator
write_orchestrator_config unset controller bundle
cat >> "$WORK/config.yaml" <<'EOF'
  merge_ref: false
EOF
start_orchestrator "$WORK/orch-bundle.log"
wait_mmds_listener

code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\",\"timeout\":120}")
[ "$code" = "201" ] || { cat "$WORK/resp.body"; fail "bundle create=$code"; }
SID=$(json_field "$WORK/resp.body" sandboxID)
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
ENVD_SOCK="$WORK/run/sandboxes/$SID/envd.sock"
EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue bundle sandbox exec capability"
exec_through_connect "$SID" "$EXEC_TOKEN" "BUNDLE_START_$RANDOM"
wait_sandbox_state "$SID" running 20 || fail "bundle sandbox did not reach running"
BUNDLE_PERSIST="BUNDLE_PERSIST_$RANDOM"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "echo $BUNDLE_PERSIST > /home/user/bundle-persist.txt" >"$WORK/bundle-write.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/bundle-write.out" \
    || { sed 's/^/  guest| /' "$WORK/bundle-write.out"; fail "write bundle marker"; }

BUNDLE_CALL=$(snapshot_argv_count)
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "bundle pause=$code"; }
assert_snapshot_argv "$BUNDLE_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode bundle --run-root "$WORK/run/sandboxes" \
    --merge-ref=false \
    || fail "bundle Pause did not pass --mode bundle exactly"
BUNDLE_LOCAL="$CHECKPOINT_ROOT/$SID/checkpoint/$SID.snapshot"
[ -L "$BUNDLE_LOCAL" ] || fail "bundle Pause did not retain $BUNDLE_LOCAL symlink"
BUNDLE_TARGET=$(readlink -f "$BUNDLE_LOCAL")
case "$BUNDLE_TARGET" in *.bundle) ;; *) fail "bundle symlink target is not .bundle: $BUNDLE_TARGET" ;; esac
BUNDLE_A_KEY=$(basename "$BUNDLE_TARGET" .bundle)
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$BUNDLE_LOCAL" >"$WORK/bundle-local.json" \
    || fail "local Manifest Bundle is unreadable"

exec_through_connect "$SID" "$EXEC_TOKEN" "BUNDLE_RESTORE_$RANDOM"
wait_sandbox_state "$SID" running 20 || fail "local Manifest Bundle did not restore"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "cat /home/user/bundle-persist.txt" >"$WORK/bundle-read.out" 2>&1 || true
grep -q "$BUNDLE_PERSIST" "$WORK/bundle-read.out" \
    || { sed 's/^/  guest| /' "$WORK/bundle-read.out"; fail "local Manifest Bundle restore lost guest state"; }

BUNDLE_B_CALL=$(snapshot_argv_count)
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "bundle B pause=$code"; }
assert_snapshot_argv "$BUNDLE_B_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode bundle --run-root "$WORK/run/sandboxes" \
    --merge-ref=false \
    || fail "bundle B Pause did not pass --mode bundle exactly"
BUNDLE_B_TARGET=$(readlink -f "$BUNDLE_LOCAL")
BUNDLE_B_KEY=$(basename "$BUNDLE_B_TARGET" .bundle)
[[ "$BUNDLE_B_TARGET" == *.bundle && "$BUNDLE_B_KEY" =~ ^[0-9a-f]{64}$ ]] \
    || fail "bundle B target does not encode its root ManifestKey: $BUNDLE_B_TARGET"
python3 - "$BUNDLE_B_TARGET" "$BUNDLE_A_KEY" <<'PY' \
    || fail "bundle B retained external refs or omitted its embedded A manifest"
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as archive:
    names = set(archive.namelist())
if "bundle/refs" in names:
    raise SystemExit("bundle B unexpectedly retained external refs")
want = "manifest/" + sys.argv[2]
if want not in names:
    raise SystemExit(f"bundle B entries omit embedded A manifest {want!r}")
PY
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$BUNDLE_B_TARGET" >"$WORK/bundle-b.json" || fail "bundle B snapshot.cfg is unreadable"
python3 - "$WORK/bundle-b.json" "$BUNDLE_A_KEY" <<'PY' \
    || fail "bundle B snapshot.cfg is not a manifest-only logical graph"
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    cfg = json.load(stream)
want = "manifest://" + sys.argv[2]
if not (cfg.get("FromRefs") or []) or cfg["FromRefs"][0] != want:
    raise SystemExit(f"bundle B FromRefs={cfg.get('FromRefs')!r}, want first {want!r}")
def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for nested in value.values():
            yield from strings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from strings(nested)
bad = [value for value in strings(cfg) if value.startswith("file://") and "@manifest:" in value]
if bad:
    raise SystemExit(f"physical Bundle selectors leaked into snapshot.cfg: {bad!r}")
PY

exec_through_connect "$SID" "$EXEC_TOKEN" "BUNDLE_CHAIN_RESTORE_$RANDOM"
wait_sandbox_state "$SID" running 20 || fail "bundle B with embedded A manifest did not restore"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "cat /home/user/bundle-persist.txt" >"$WORK/bundle-chain-read.out" 2>&1 || true
grep -q "$BUNDLE_PERSIST" "$WORK/bundle-chain-read.out" \
    || { sed 's/^/  guest| /' "$WORK/bundle-chain-read.out"; fail "bundle B restore lost A state"; }

BUNDLE_PROMOTE_CALL=$(snapshot_argv_count)
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "bundle C promote pause=$code"; }
assert_snapshot_argv "$BUNDLE_PROMOTE_CALL" \
    snapshot --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode bundle --run-root "$WORK/run/sandboxes" \
    --merge-ref=false \
    || fail "bundle C Pause did not pass --mode bundle exactly"
BUNDLE_PROMOTE_TARGET=$(readlink -f "$BUNDLE_LOCAL")
BUNDLE_ROOT_KEY=$(basename "$BUNDLE_PROMOTE_TARGET" .bundle)
[[ "$BUNDLE_PROMOTE_TARGET" == *.bundle && "$BUNDLE_ROOT_KEY" =~ ^[0-9a-f]{64}$ ]] \
    || fail "bundle C target does not encode its root ManifestKey: $BUNDLE_PROMOTE_TARGET"
wait_paused_cleanup "$SID" || fail "Bundle C checkpoint cleanup remained pending"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$BUNDLE_PROMOTE_TARGET" >"$WORK/bundle-c.json" || fail "bundle C snapshot.cfg is unreadable"
python3 - "$WORK/bundle-c.json" "$WORK/bundle-local.json" "$BUNDLE_PROMOTE_TARGET" "$BUNDLE_B_KEY" "$BUNDLE_A_KEY" <<'PY_BUNDLE' \
    || fail "Bundle C failed bounded historical memory composition"
import json, sys, zipfile
current, first = (json.load(open(path)) for path in sys.argv[1:3])
refs, suffix = current.get("FromRefs") or [], first.get("FromRefs") or []
if len(refs) != 1 + len(suffix) or refs[1:] != suffix:
    raise SystemExit(f"C memory refs={refs!r}, want one new history plus {suffix!r}")
history = refs[0].removeprefix("manifest://")
if len(history) != 64 or history in sys.argv[4:]:
    raise SystemExit(f"history did not receive a new immutable identity: {refs[0]}")
with zipfile.ZipFile(sys.argv[3]) as archive:
    names = set(archive.namelist())
if "bundle/refs" in names or "manifest/" + history not in names:
    raise SystemExit("C does not own its historical memory member")
for old in sys.argv[4:]:
    if "manifest/" + old in names:
        raise SystemExit(f"C copied retired full Snapshot {old} instead of composing memory")
PY_BUNDLE
[ ! -e "$BUNDLE_B_TARGET" ] && [ ! -e "$BUNDLE_TARGET" ] \
    || fail "Bundle C cleanup retained obsolete A/B carriers"
BUNDLE_LOCAL_PAIR=$(checkpoint_pair "$SID" snapshot "$BUNDLE_LOCAL") || fail "invalid Bundle capture pair"
BUNDLE_TOKEN=$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --socket "$WORK/node-ctl.socket") \
    || fail "bundle keep-source export failed"
case "$BUNDLE_TOKEN" in kmt1.*) ;; *) fail "bundle export returned a non-KMT result" ;; esac
# keep-source must leave the paused source row unchanged and retain its local
# checkpoint (#336). The published root stays content-addressed under the
# local bundle's ManifestKey, which the Store lookup below proves.
[ "$(checkpoint_pair "$SID" snapshot "$BUNDLE_LOCAL")" = "$BUNDLE_LOCAL_PAIR" ] \
    || fail "Bundle keep-source export rewrote the S/E pair"
[ -e "$CHECKPOINT_ROOT/$SID/checkpoint" ] || fail "bundle keep-source export removed the retained checkpoint"
BUNDLE_REMOTE_REF="manifest://$BUNDLE_ROOT_KEY"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$BUNDLE_REMOTE_REF" >"$WORK/bundle-remote.json" \
    || fail "published Bundle root $BUNDLE_REMOTE_REF is unreadable from Store"
exec_through_connect "$SID" "$EXEC_TOKEN" "BUNDLE_LOCAL_RESUME_$RANDOM"
wait_sandbox_state "$SID" running 20 || fail "retained Bundle C did not resume from its local checkpoint"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "cat /home/user/bundle-persist.txt" >"$WORK/bundle-store-read.out" 2>&1 || true
grep -q "$BUNDLE_PERSIST" "$WORK/bundle-store-read.out" \
    || { sed 's/^/  guest| /' "$WORK/bundle-store-read.out"; fail "local Bundle C resume lost A state"; }

E_BUNDLE_EXPORT_CALL=$(export_argv_count)
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":false}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "Bundle Sandbox E pause=$code"; }
assert_export_argv "$E_BUNDLE_EXPORT_CALL" \
    export --json --path-id "$SID" --output "$CHECKPOINT_ROOT/$SID/checkpoint" --mode bundle --run-root "$WORK/run/sandboxes" \
    || fail "Pause(memory=false) did not execute Bundle export"
E_BUNDLE_LOCAL="$CHECKPOINT_ROOT/$SID/checkpoint/$SID.sandbox"
[ -L "$E_BUNDLE_LOCAL" ] || fail "Bundle Sandbox E did not retain $E_BUNDLE_LOCAL symlink"
E_BUNDLE_TARGET=$(readlink -f "$E_BUNDLE_LOCAL")
case "$E_BUNDLE_TARGET" in *.bundle) ;; *) fail "Bundle Sandbox E target is not .bundle: $E_BUNDLE_TARGET" ;; esac
E_BUNDLE_PAIR=$(checkpoint_pair "$SID" sandbox "$E_BUNDLE_LOCAL") || fail "invalid E-only Bundle capture"
E_BUNDLE_RUNTIME_REF=$(checkpoint_runtime_ref "$E_BUNDLE_PAIR" "$CHECKPOINT_ROOT/$SID/checkpoint")
E_BUNDLE_TOKEN=$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" \
    --keep-source --socket "$WORK/node-ctl.socket") \
    || fail "Bundle Sandbox E publish failed"
case "$E_BUNDLE_TOKEN" in kmt1.*) ;; *) fail "Bundle Sandbox E export returned a non-KMT result" ;; esac
# keep-source leaves the paused Bundle E row pointing at its retained local
# checkpoint (#336); the published artifact lands in the Store under the
# bundle's content-addressed root key.
[ "$(checkpoint_pair "$SID" sandbox "$E_BUNDLE_LOCAL")" = "$E_BUNDLE_PAIR" ] \
    || fail "Bundle Sandbox E keep-source export rewrote the source"
[ -L "$E_BUNDLE_LOCAL" ] || fail "Bundle Sandbox E keep-source export removed the retained local symlink"
[ -e "$CHECKPOINT_ROOT/$SID/checkpoint" ] || fail "Bundle Sandbox E keep-source export removed the retained local directory"
E_BUNDLE_REMOTE_REF="manifest://$(basename "$E_BUNDLE_TARGET" .bundle)"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$E_BUNDLE_REMOTE_REF" >"$WORK/bundle-e-remote.json" \
    || fail "published Bundle E root is unreadable from Store"
E_BUNDLE_RUN_CALL=$(run_argv_count)
exec_through_connect "$SID" "$EXEC_TOKEN" "BUNDLE_E_WAKE_$RANDOM"
wait_sandbox_state "$SID" running 1200 || fail "retained Bundle Sandbox E did not cold Wake"
assert_run_source_mode "$E_BUNDLE_RUN_CALL" "$SID" from \
    || fail "Bundle Sandbox E Wake did not execute run --from"
assert_run_option_value "$E_BUNDLE_RUN_CALL" "$SID" "--from" "$E_BUNDLE_RUNTIME_REF" \
    || fail "Bundle Sandbox E Wake did not select the retained local checkpoint"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "cat /home/user/bundle-persist.txt" >"$WORK/bundle-e-read.out" 2>&1 || true
grep -q "$BUNDLE_PERSIST" "$WORK/bundle-e-read.out" \
    || { sed 's/^/  guest| /' "$WORK/bundle-e-read.out"; fail "retained Bundle Sandbox E lost disk state"; }
# Re-capture after the retained-local Wake, then move through the real CLI.
# Success accepts deletion; wait for the durable completion condition before
# reusing this node-local ID for KMT import.
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":false}')
[ "$code" = "204" ] || { cat "$WORK/resp.body"; fail "Bundle E pause before move=$code"; }
E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" --json \
    --socket "$WORK/node-ctl.socket" >"$WORK/bundle-e-move.json" \
    || fail "Bundle E move export failed"
E_BUNDLE_TOKEN=$(json_field "$WORK/bundle-e-move.json" result)
E_BUNDLE_REMOTE_REF=$(json_field "$WORK/bundle-e-move.json" sandboxRef)
case "$E_BUNDLE_TOKEN" in kmt1.*) ;; *) fail "Bundle E move returned a non-KMT result" ;; esac
wait_sandbox_state "$SID" missing 120 || fail "Bundle E move finalizer retained durable row"
[ ! -e "$WORK/run/sandboxes/$SID" ] || fail "Bundle E move retained RunDir"
[ ! -e "$WORK/lib/sandboxes/$SID" ] || fail "Bundle E move retained BaseDir/checkpoint"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$E_BUNDLE_REMOTE_REF" >"$WORK/bundle-e-moved-remote.json" \
    || fail "Bundle E move removed its published artifact"
unset EXEC_TOKEN
echo "==> KMT import of published Bundle E -> cold restore from the manifest Store"
E_KMT_RUN_CALL=$(run_argv_count)
code=$(curl -sS --noproxy '*' --max-time 30 -o "$WORK/resp.body" -w '%{http_code}' \
    -X POST -H "Host: api.$DOMAIN" -H "X-API-KEY: $AK" \
    -H "X-Kuasar-Migration-Token: $E_BUNDLE_TOKEN" \
    -H 'Content-Type: application/json' --data '{"timeout":119}' \
    "http://127.0.0.1:$PORT/sandboxes/$SID/connect")
[ "$code" = "200" ] || { cat "$WORK/resp.body"; fail "Bundle E KMT Connect=$code (want 200)"; }
ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
E_KMT_EXEC_TOKEN="$(issue_exec_session "$SID" "$AK")" || fail "issue exec capability for imported Bundle E"
rm -f "$WORK/exec-session.secret"
exec_through_connect "$SID" "$E_KMT_EXEC_TOKEN" "BUNDLE_E_KMT_$RANDOM"
wait_sandbox_state "$SID" running 1200 || fail "imported Bundle E did not cold restore from the Store"
assert_run_source_mode "$E_KMT_RUN_CALL" "$SID" from \
    || fail "imported Bundle E did not execute run --from"
assert_run_option_value "$E_KMT_RUN_CALL" "$SID" "--from" "$E_BUNDLE_REMOTE_REF" \
    || fail "imported Bundle E did not select the published Store root"
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "cat /home/user/bundle-persist.txt" >"$WORK/bundle-e-kmt-read.out" 2>&1 || true
grep -q "$BUNDLE_PERSIST" "$WORK/bundle-e-kmt-read.out" \
    || { sed 's/^/  guest| /' "$WORK/bundle-e-kmt-read.out"; fail "Store-only Bundle E restore lost disk state"; }
code=$(req DELETE "/sandboxes/$SID" "$AK"); [ "$code" = "204" ] || fail "kill imported Bundle E=$code"
wait_sandbox_state "$SID" missing 120 || fail "imported Bundle E delete finalizer retained durable row"
unset E_KMT_EXEC_TOKEN

# Import leaves a portable paused source. Even an older local checkpoint under
# its owned BaseDir must be removed by a later template move.
PORTABLE_MOVE_SID=$(E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" import-sandbox "$E_BUNDLE_TOKEN" \
    --socket "$WORK/node-ctl.socket") || fail "portable move fixture import failed"
[ "$PORTABLE_MOVE_SID" = "$SID" ] || fail "portable move import changed the node-local ID"
mkdir -p "$CHECKPOINT_ROOT/$SID/checkpoint"
printf '%s\n' 'isolated stale checkpoint fixture' >"$CHECKPOINT_ROOT/$SID/checkpoint/stale"
E2B_API_KEY="$AK" "$ORCH_BIN_DIR/node-ctl" export-sandbox "$SID" --to-template --json \
    --socket "$WORK/node-ctl.socket" >"$WORK/portable-e-move.json" \
    || fail "portable E template move failed"
[ "$(json_field "$WORK/portable-e-move.json" sandboxRef)" = "$E_BUNDLE_REMOTE_REF" ] \
    || fail "portable E move changed the published root"
wait_sandbox_state "$SID" missing 120 || fail "portable E move finalizer retained durable row"
[ ! -e "$WORK/run/sandboxes/$SID" ] || fail "portable E move retained RunDir"
[ ! -e "$WORK/lib/sandboxes/$SID" ] || fail "portable E move retained stale BaseDir/checkpoint"
MANIFEST_KEY="$MK" "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" \
    "$E_BUNDLE_REMOTE_REF" >"$WORK/portable-e-moved-remote.json" \
    || fail "portable E move removed shared published output"
echo "==> PASS: Bundle drove self-contained S chain plus E capture, exact publish, retained-local Wake, and cold Store restore via KMT"
echo "PASS orchestrator.snapshot.sh"
