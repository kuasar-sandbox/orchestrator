#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
# Actual HTTP, SQLite, VMM and resource ledger; private ENOSPC delays all holders.
set -euo pipefail
umask 077
if [ -z "${CHECKPOINT_HOLDER_FORMAT:-}" ]; then
    for format in local bundle; do
        CHECKPOINT_HOLDER_FORMAT=$format OUT="${OUT:?}/$format" bash "$0"
    done
    exit 0
fi
case "$CHECKPOINT_HOLDER_FORMAT" in local|bundle) ;; *) exit 2 ;; esac
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
. "$SCRIPT_DIR/execute_checkpoint.sh"
. "$SCRIPT_DIR/execute_template.sh"
mkdir -p "$CASE_OUT"
write_orchestrator_config unset controller "$CHECKPOINT_HOLDER_FORMAT"
sed -i '/^resource_listen:$/a\  watermarks: { startup_factor: 0.8 }' "$WORK/config.yaml"
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
build_image_template e2b

mkdir -p "$CHECKPOINT_ROOT"
python3 - "$WORK" "$CHECKPOINT_ROOT" <<'PY'
from pathlib import Path
import sys
work,root=map(Path,sys.argv[1:])
assert work.resolve()==work and root.resolve()==root and root==work/'lib/sandboxes'
assert not list(root.iterdir()),'refusing to hide another sandbox'
PY
MOUNT_ID=""
MOUNT_CREATED=0
FILLER="$CHECKPOINT_ROOT/.holder-test-filler"
holder_cleanup() {
    local status=$? current=""
    trap - EXIT
    set +e
    if [ "$MOUNT_CREATED" = 1 ]; then
        current=$(findmnt -rn -M "$CHECKPOINT_ROOT" -o ID)
        if [ -n "$MOUNT_ID" ] && [ "$current" = "$MOUNT_ID" ]; then
            rm -f -- "$FILLER"
            stop_orchestrator
            stop_owned_units "$RUNNER_PREFIX" "$BUILDER_PREFIX"
            if ! umount -- "$CHECKPOINT_ROOT"; then E2E_KEEP=1; status=1; fi
        else
            echo 'mount identity changed; preserving owned recovery state' >&2
            E2E_KEEP=1; status=1
        fi
    fi
    cleanup
    exit "$status"
}
trap holder_cleanup EXIT
mount -t tmpfs -o size=1g,mode=700 "checkpoint-holders-$RUN_KEY" "$CHECKPOINT_ROOT"
MOUNT_CREATED=1
MOUNT_ID=$(findmnt -rn -M "$CHECKPOINT_ROOT" -o ID)
[ -n "$MOUNT_ID" ] || fail 'mount identity missing'
printf '%s\t%s\n' "$MOUNT_ID" "$CHECKPOINT_ROOT" >"$CASE_OUT/mount-identity.txt"

# Quota is per-instance node policy carried by the checkpoint extension.
REQ_CHECKPOINT_HEADER='{"max_checkpoints":0}'
code=$(req POST /sandboxes "$AK" "{\"templateID\":\"$TEMPLATE\"}")
[ "$code" = 400 ] || fail "zero quota accepted: $code"
REQ_RESOURCE_HEADER='{"capacity":{"cpu":1,"memory":"512MiB"},"allocatable":{"cpu":1,"memory":"512MiB"}}'
REQ_CHECKPOINT_HEADER='{"max_checkpoints":2}'
create_guest "{\"templateID\":\"$TEMPLATE\",\"timeout\":1200}"
unset REQ_RESOURCE_HEADER REQ_CHECKPOINT_HEADER
wait_sandbox_state "$SID" running 1200 || fail 'cold create did not run'
checkpoint_guest /bin/true
checkpoint_guest /bin/sh -c 'cat > /home/user/holder-memory.py <<'"'"'PY'"'"'
import hashlib,json,os,time
memory=bytearray(os.urandom(128*1024*1024));expected=hashlib.sha256(memory).hexdigest();counter=0;generation=0
while True:
    try:
        with open("/home/user/holder-generation") as f:wanted=int(f.read())
    except FileNotFoundError:wanted=generation
    if wanted!=generation:
        assert wanted==generation+1
        memory[:]=os.urandom(len(memory))
        expected=hashlib.sha256(memory).hexdigest();generation=wanted
    for offset in range(0,len(memory),4096):
        memory[offset]^=1;memory[offset]^=1
    assert hashlib.sha256(memory).hexdigest()==expected
    counter+=1
    with open("/home/user/holder-status.tmp","w") as f:json.dump({"hash":expected,"counter":counter,"generation":generation},f)
    os.replace("/home/user/holder-status.tmp","/home/user/holder-status")
    time.sleep(.2)
PY
nohup python3 /home/user/holder-memory.py >/home/user/holder-memory.log 2>&1 </dev/null &'
for _ in $(seq 1 100); do
    if checkpoint_guest /bin/cat /home/user/holder-status >"$CASE_OUT/memory-before.json" 2>/dev/null; then break; fi
    sleep .2
done
python3 - "$CASE_OUT/memory-before.json" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))['counter']>0
PY
cp "$CASE_OUT/memory-before.json" "$CASE_OUT/memory-expected.json"
check_memory() {
    checkpoint_guest /bin/cat /home/user/holder-status >"$CASE_OUT/memory-$1.json"
    python3 - "$CASE_OUT/memory-expected.json" "$CASE_OUT/memory-$1.json" <<'PY'
import json,sys
a,b=(json.load(open(p)) for p in sys.argv[1:])
assert a['generation']==b['generation'] and a['hash']==b['hash'] and b['counter']>=a['counter'],(a,b)
PY
}
# Every successor must own fresh incompressible pages. Touching then restoring
# the same bytes can deduplicate the payload and accidentally settle a holder.
advance_memory() {
    checkpoint_guest python3 -c '
import json,pathlib,sys,time
wanted=int(sys.argv[1]);pathlib.Path("/home/user/holder-generation").write_text(str(wanted))
deadline=time.monotonic()+30
while True:
    status=json.loads(pathlib.Path("/home/user/holder-status").read_text())
    if status["generation"]==wanted:print(json.dumps(status));break
    assert time.monotonic()<deadline,"guest did not publish the requested generation"
    time.sleep(.05)
' "$1" >"$CASE_OUT/memory-generation-$1.json"
    python3 - "$CASE_OUT/memory-expected.json" "$CASE_OUT/memory-generation-$1.json" <<'PY'
import json,sys
a,b=(json.load(open(p)) for p in sys.argv[1:])
assert b['generation']==a['generation']+1 and b['hash']!=a['hash'] and b['counter']>a['counter'],(a,b)
PY
    cp "$CASE_OUT/memory-generation-$1.json" "$CASE_OUT/memory-expected.json"
}

hold_payload_failure() {
    python3 - "$CHECKPOINT_ROOT/$SID/checkpoint" "$CASE_OUT" "$1" <<'PY'
import json,os,pathlib,resource,sys,time
root,out,label=pathlib.Path(sys.argv[1]),pathlib.Path(sys.argv[2]),sys.argv[3]
known={json.loads(p.read_text())['capture_path'] for p in out.glob('*-file-limit.json')}
paths=[p for p in root.glob('memory/*/*.json') if str(p) not in known]
assert len(paths)==1,paths
path=paths[0];r=json.loads(path.read_text());holder=r['holder']
def identity():
    parts=pathlib.Path(f"/proc/{holder['pid']}/stat").read_text().rsplit(')',1)[1].split()
    assert parts[0]!='Z' and int(parts[19])==holder['start_time'],'retired holder identity changed'
deadline=time.monotonic()+30
while True:
    r=json.loads(path.read_text())
    assert r['committed'] and r['runtime_retired'] and not r['file_ready'] and not r['ram_released'],'capture did not retain the new RAM obligation'
    identity()
    if 'no space left on device' in r.get('save_error',''):break
    assert time.monotonic()<deadline,'holder did not encounter the injected ENOSPC'
    time.sleep(.02)
# Bound only this exact, retired writer after observing the intended fault.
# This leaves metadata room for successors while preserving the original hard
# limit; neither the live VM nor a later holder inherits this soft limit.
old=resource.prlimit(holder['pid'],resource.RLIMIT_FSIZE);limit=1024*1024
assert old[0]==resource.RLIM_INFINITY or old[0]>limit,old
resource.prlimit(holder['pid'],resource.RLIMIT_FSIZE,(limit,old[1]))
(out/(label+'-file-limit.json')).write_text(json.dumps({'holder':holder,'previous':old,'soft_limit':limit,
    'capture_path':str(path),'checkpoint_id':r['checkpoint_id'],'memory_id':r['memory_id'],'observed_error':r['save_error']}))
while os.statvfs(root).f_bavail*os.statvfs(root).f_frsize<32*1024*1024:
    identity()
    assert time.monotonic()<deadline,'holder did not free the failed partial payload'
    time.sleep(.02)
print('retired holder payload bounded after real ENOSPC:',label,flush=True)
PY
}
release_payload_failures() {
    python3 - "$CASE_OUT" <<'PY'
import json,pathlib,resource,sys
proofs=list(pathlib.Path(sys.argv[1]).glob('*-file-limit.json'));assert len(proofs)==4
for path in proofs:
    p=json.loads(path.read_text());h=p['holder']
    parts=pathlib.Path(f"/proc/{h['pid']}/stat").read_text().rsplit(')',1)[1].split()
    assert parts[0]!='Z' and int(parts[19])==h['start_time'],'retired holder identity changed'
    assert resource.prlimit(h['pid'],resource.RLIMIT_FSIZE)==(p['soft_limit'],p['previous'][1])
    resource.prlimit(h['pid'],resource.RLIMIT_FSIZE,tuple(p['previous']))
PY
}
python3 - "$CHECKPOINT_ROOT" "$FILLER" <<'PY'
import os,sys
v=os.statvfs(sys.argv[1]);amount=v.f_bavail*v.f_frsize-64*1024*1024
assert amount>0
with open(sys.argv[2],'wb') as f:os.posix_fallocate(f.fileno(),0,amount);os.fsync(f.fileno())
PY

ledger() { # expected pending count, active count, output label
    python3 - "$ORCH_BIN_DIR/node-ctl" "$WORK/sandbox-resource.sock" "$WORK/node-ctl.socket" \
        "$SID" "$1" "$2" "$CASE_OUT/$3.json" <<'PY'
import json,pathlib,subprocess,sys,time
node,socket,control,sid,pending,active,out=sys.argv[1:];deadline=time.monotonic()+20
pending,active=int(pending),int(active)
while True:
    accounts=[json.loads(p.read_text()) for p in pathlib.Path(socket+'.retained').glob('*.json')]
    own=[r for r in accounts if r['sandbox_id']==sid]
    assert len(own)==1 and own[0]['version']==2,own
    entries=own[0]['captures'];held=[r for r in entries if r['stage']=='held']
    rows=json.loads(subprocess.check_output([node,'resource','list','--socket',socket],text=True)) or []
    pressure=json.loads(subprocess.check_output([node,'resource','pressure','--socket',control],text=True))
    current=[r for r in rows if r['sandbox_id']==sid]
    all_r=sum(r['memory']['bytes'] for a in accounts for r in a['captures'] if r['stage']=='held')
    all_b=sum(r['allocatable_memory'] for r in rows)
    unique={(r['memory']['memory_id'],r['memory']['epoch']) for r in entries}
    assert len(unique)==len(entries),'duplicate frozen charge identity'
    for r in entries:
        record=json.loads(pathlib.Path(r['capture']['capture_path']).read_text())
        assert record['producer']==r['producer'] and record['memory_id']==r['memory']['memory_id']
        if r['stage']=='settled':assert record['ram_released'],'settled before exact RAM release'
    if len(held)==pending and len(current)==active and pressure['reserved_memory']==all_r+all_b:break
    assert time.monotonic()<deadline,{'expected_held':pending,'actual_held':len(held),'expected_active':active,'actual_active':len(current),'reserved_bytes':pressure['reserved_memory'],'calculated_bytes':all_r+all_b,'capture_stages':[r['stage'] for r in entries]}
    time.sleep(.1)
report={'sid':sid,'stage':own[0]['stage'],'account_id':own[0]['id'],'revision':own[0]['revision'],
 'active_bytes':sum(r['allocatable_memory'] for r in current),'retained_bytes':sum(r['memory']['bytes'] for r in held),
 'node_reserved_bytes':pressure['reserved_memory'],'captures':[{'id':r['checkpoint_id'],'memory':r['memory'],'stage':r['stage'],'producer':r['producer']} for r in entries]}
json.dump(report,open(out,'w'),indent=2)
PY
}
named_pause() { # label; leaves exact selected ID in NAMED_ID
    code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":true,"checkpoint":true}')
    [ "$code" = 200 ] || { cat "$WORK/resp.body"; fail "named pause $1=$code"; }
    cp "$WORK/resp.body" "$CASE_OUT/pause-$1.json"
    NAMED_ID=$(json_field "$WORK/resp.body" checkpointID)
    wait_paused_cleanup "$SID" 300 || fail 'ordinary RunDir cleanup waited for holder'
    [ ! -e "$EXECUTE_RUN_ROOT/sandboxes/$SID" ] || fail 'RunDir retained by holder'
}
named_pause a
A=$NAMED_ID
hold_payload_failure a
ledger 1 0 a-paused
resume_checkpoint_guest 0
check_memory b
ledger 1 1 b-running
advance_memory 1
named_pause b
B=$NAMED_ID
hold_payload_failure b
ledger 2 0 b-paused

# Restart with less headroom than R1+R2+B. Accepted Connect may be starting,
# but it must not enter business execution or create an uncharged candidate B.
stop_orchestrator
python3 - "$WORK/config.yaml" "$CASE_OUT/b-paused.json" <<'PY'
import json,sys,yaml
p=sys.argv[1];cfg=yaml.safe_load(open(p));r=json.load(open(sys.argv[2]))
saved=r['captures'][-1]['memory']['bytes'];reserve=1024*1024*1024
cfg['resource_listen']['resources']['physical_memory']=str(r['retained_bytes']+saved//2+reserve)
yaml.safe_dump(cfg,open(p,'w'),sort_keys=False)
PY
start_orchestrator "$WORK/orch-limited.log"
ledger 2 0 recovered-limited
code=$(req POST "/sandboxes/$SID/connect" "$AK" '{"timeout":1200}')
case "$code" in 200|503|429) ;; *) cat "$WORK/resp.body"; fail "limited Connect=$code" ;; esac
sleep 3
[ "$(sandbox_state "$SID")" != running ] || fail 'insufficient pool entered business running'
ledger 2 0 admission-blocked
if timeout -k 1s 3 "$BIN/sandbox-ctl" exec --path-id "$SID" --run-root "$EXECUTE_RUN_ROOT/sandboxes" -- /bin/true >"$CASE_OUT/blocked-exec.log" 2>&1; then
    fail 'business exec succeeded without B headroom'
fi
stop_orchestrator
python3 - "$WORK/config.yaml" <<'PY'
import sys,yaml
p=sys.argv[1];c=yaml.safe_load(open(p));c['resource_listen']['resources']['physical_memory']='auto';yaml.safe_dump(c,open(p,'w'),sort_keys=False)
PY
start_orchestrator "$WORK/orch-recovered.log"
# Pressure hysteresis is persisted across restart. Wait for the real policy to
# leave its restrictive zone after restoring capacity; do not change its timers.
for _ in $(seq 1 100); do
    "$ORCH_BIN_DIR/node-ctl" resource pressure --socket "$WORK/node-ctl.socket" >"$CASE_OUT/recovered-pressure.json"
    if python3 - "$CASE_OUT/recovered-pressure.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]));raise SystemExit(0 if p['zone']=='green' else 1)
PY
    then break; fi
    sleep 1
done
python3 - "$CASE_OUT/recovered-pressure.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1]));assert p['zone']=='green',p
PY
ledger 2 0 recovered-headroom
if [ "$(sandbox_state "$SID")" = paused ]; then resume_checkpoint_guest 0; fi
wait_sandbox_state "$SID" running 1200 || fail 'recovery did not admit one candidate'
check_memory c
ledger 2 1 c-running
code=$(req POST "/sandboxes/$SID/pause" "$AK" '{"memory":true,"checkpoint":true}')
[ "$code" = 409 ] && [ "$(sandbox_state "$SID")" = running ] || fail 'quota did not reject before freeze'
advance_memory 2
pause_checkpoint_guest snapshot 0
hold_payload_failure c
ledger 3 0 c-paused

# Name deletion frees one slot, while descendants keep A's backend available.
code=$(req DELETE "/sandboxes/$SID/checkpoints/$A" "$AK")
[ "$code" = 204 ] || fail "delete A name=$code"
python3 - "$WORK/lib/node-ctl.db" "$SID" "$A" "$B" "$CHECKPOINT_ROOT/$SID/checkpoint" <<'PY'
import json,pathlib,sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
 assert db.execute('select max_checkpoints from sandboxes where id=?',(sys.argv[2],)).fetchone()==(2,)
 assert db.execute('select checkpoint_id from sandbox_checkpoints where sandbox_id=?',(sys.argv[2],)).fetchall()==[(sys.argv[4],)]
records=[json.loads(p.read_text()) for p in pathlib.Path(sys.argv[5]).glob('memory/*/*.json')]
assert len(records)==3 and all(not r.get('ram_released') for r in records)
assert not next(r for r in records if r['checkpoint_id']==sys.argv[3]).get('deleted'),'descendant lost its parent'
PY
resume_checkpoint_guest 0
check_memory d
ledger 3 1 d-running
advance_memory 3
named_pause d
hold_payload_failure d
ledger 4 0 d-paused
stop_orchestrator
start_orchestrator "$WORK/orch-four-holders.log"
ledger 4 0 four-recovered
# Four independently changed 512 MiB captures can exceed the original 1 GiB
# fault volume. End the storage fault with room for every complete payload;
# only then restore each holder's original per-file limit.
[ "$(findmnt -rn -M "$CHECKPOINT_ROOT" -o ID)" = "$MOUNT_ID" ] || fail 'mount identity changed before fault release'
mount -o remount,size=3g "$CHECKPOINT_ROOT"
[ "$(findmnt -rn -M "$CHECKPOINT_ROOT" -o ID)" = "$MOUNT_ID" ] || fail 'fault release replaced the mount'
rm -f -- "$FILLER"
release_payload_failures
for _ in $(seq 1 300); do
    if python3 - "$CHECKPOINT_ROOT/$SID/checkpoint" <<'PY'
import json,pathlib,sys
records=[json.loads(p.read_text()) for p in pathlib.Path(sys.argv[1]).glob('memory/*/*.json')]
assert len(records)==4
raise SystemExit(0 if all(r.get('file_ready') and r.get('ram_released') and not r.get('discarded') for r in records) else 1)
PY
    then break; fi
    sleep 1
done
ledger 0 0 all-settled
resume_checkpoint_guest 0
check_memory final
ledger 0 1 final-running
# Reintroduce the same private storage fault after verifying ordinary recovery.
# Terminal deletion must cancel every pending save before waiting for its unit;
# serial SIGTERM waits would consume 20 seconds per detached holder.
python3 - "$CHECKPOINT_ROOT" "$FILLER" <<'PY'
import os,sys
v=os.statvfs(sys.argv[1]);amount=v.f_bavail*v.f_frsize-64*1024*1024
assert amount>0
with open(sys.argv[2],'wb') as f:os.posix_fallocate(f.fileno(),0,amount);os.fsync(f.fileno())
PY
advance_memory 4
pause_checkpoint_guest snapshot 0
hold_payload_failure terminal-a
ledger 1 0 terminal-a-paused
resume_checkpoint_guest 0
check_memory terminal-b
advance_memory 5
pause_checkpoint_guest snapshot 0
hold_payload_failure terminal-b
ledger 2 0 terminal-b-paused
code=$(req DELETE "/sandboxes/$SID" "$AK")
[ "$code" = 204 ] || fail "pending-holder delete=$code"
wait_sandbox_state "$SID" missing 120 || {
    # Preserve the failure boundary before cleanup releases the storage fault.
    # Emit lifecycle facts only; configuration, tokens and payload stay private.
    python3 - "$CHECKPOINT_ROOT/$SID/checkpoint" "$SID" "$RUNNER_PREFIX" <<'PY'
import json,pathlib,re,subprocess,sys
root,sid,prefix=pathlib.Path(sys.argv[1]),sys.argv[2],sys.argv[3]
assert re.fullmatch(r'sandbox-runner-[A-Za-z0-9_-]+@',prefix)
fields=('checkpoint_id','memory_id','epoch','committed','deleted','runtime_retired','file_ready','ram_released','discarded')
units=set()
for path in sorted(root.glob('memory/*/*.json')):
    try:r=json.loads(path.read_text())
    except (FileNotFoundError,json.JSONDecodeError):continue
    if r['producer']['sandbox_id']!=sid:continue
    run=r['producer']['run_id']
    print('terminal-capture',json.dumps(dict(run_id=run,**{k:r.get(k,False) for k in fields})),flush=True)
    if re.fullmatch(r'sr-[0-9a-f-]{36}',run):units.add(prefix+run+'.service')
for unit in sorted(units):
    print('terminal-unit',unit,flush=True)
    subprocess.run(['systemctl','show',unit,'--property=ActiveState,SubState,Result,ExecMainPID,ExecMainCode,ExecMainStatus,TimeoutStopUSec'],timeout=5,check=False)
    subprocess.run(['journalctl','-u',unit,'_COMM=systemd','--no-pager','-n','12','-o','short-precise'],timeout=5,check=False)
PY
    grep -F -e 'terminal holder cancellation unavailable' \
        -e 'sandbox delete cleanup incomplete' \
        -e 'sandbox delete worker retirement failed' "$WORK/orch-four-holders.log" || true
    fail 'terminal cleanup did not converge after holder cancellation'
}
python3 - "$WORK/sandbox-resource.sock.retained" "$SID" <<'PY'
import json,pathlib,sys
assert not [p for p in pathlib.Path(sys.argv[1]).glob('*.json') if json.loads(p.read_text())['sandbox_id']==sys.argv[2]],'terminal empty account remains'
PY
echo "PASS multiple holders, complete ledger, recovery/admission, quota and terminal cleanup ($CHECKPOINT_HOLDER_FORMAT)"
