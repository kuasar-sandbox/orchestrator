#!/usr/bin/env bash
# Compiler-free helper contracts shared by focused execute/MMDS product cases.
set -euo pipefail

run_host_serialized() {
    local lock_directory="$1" result=0
    shift
    flock --nonblock --exclusive --close --conflict-exit-code 75         "$lock_directory" env KUASAR_EXECUTE_LOCK_HELD=1 "$@" || result=$?
    [ "$result" -ne 75 ] || { echo "another execute/MMDS case holds the host runtime lock" >&2; return 1; }
    return "$result"
}

argv_log_count() {
    python3 - "$1" <<'PY'
import os,sys
p=sys.argv[1]
print(0 if not os.path.exists(p) else sum(1 for line in open(p,encoding="utf-8") if line.strip()))
PY
}

run_argv_count() { argv_log_count "$RUN_ARGV_LOG"; }

wait_run_argv() {
    local index="$1" attempt
    for ((attempt=0; attempt<120; attempt++)); do
        [ "$(run_argv_count)" -gt "$index" ] && return 0
        sleep 0.05
    done
    echo "timed out waiting for run call $index" >&2
    return 1
}

assert_run_source_mode() {
    python3 - "$RUN_ARGV_LOG" "$1" "$2" "$3" <<'PY'
import json,sys
path,index,sid,want=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4]
calls=[json.loads(x) for x in open(path,encoding="utf-8") if x.strip()]
assert index < len(calls), calls
call=calls[index]
def option(name):
    for i,arg in enumerate(call):
        if arg==name: return call[i+1] if i+1<len(call) else ""
        if arg.startswith(name+"="): return arg.split("=",1)[1]
    return ""
assert option("--sandbox-id")==sid, call
selected=option("--"+want); other=option("--restore" if want=="from" else "--from")
assert selected and not other, call
if want=="from": assert ".sandbox" in selected or selected.startswith("manifest://") or "@manifest:" in selected, selected
if want=="restore": assert ".snapshot" in selected or selected.startswith("manifest://") or "@manifest:" in selected, selected
PY
}
