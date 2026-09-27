#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
snapshot_argv_count() {
    python3 - "$SNAPSHOT_ARGV_LOG" <<'PY'
import json, os, sys
path = sys.argv[1]
if not os.path.exists(path):
    print(0)
else:
    with open(path, encoding="utf-8") as source:
        print(sum(1 for line in source if line.strip()))
PY
}

argv_log_count() { # $1=jsonl path
    python3 - "$1" <<'PY'
import os, sys
path = sys.argv[1]
if not os.path.exists(path):
    print(0)
else:
    with open(path, encoding="utf-8") as source:
        print(sum(1 for line in source if line.strip()))
PY
}

export_argv_count() { argv_log_count "$EXPORT_ARGV_LOG"; }
run_argv_count() { argv_log_count "$RUN_ARGV_LOG"; }

wait_run_argv() {
    local index="$1" attempt
    # Durable starting/Proxy parking precedes the asynchronous runner process.
    # Wait only for its observation; the separate source assertion still checks
    # the exact call index, sandbox identity and mutually exclusive launch mode.
    for ((attempt=0; attempt<120; attempt++)); do
        [ "$(run_argv_count)" -gt "$index" ] && return 0
        sleep 0.05
    done
    echo "timed out waiting for run call $index" >&2
    return 1
}

assert_export_argv() { # $1=index, remaining args=expected argv
    local index="$1"
    shift
    python3 - "$EXPORT_ARGV_LOG" "$index" "$@" <<'PY'
import json, sys
path, index, expected = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
with open(path, encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
if index >= len(calls):
    raise SystemExit(f"missing export call {index}; captured {len(calls)}")
if calls[index] != expected:
    raise SystemExit(f"export call {index}={calls[index]!r}, want {expected!r}")
PY
}

assert_run_source_mode() { # $1=index, $2=sid, $3=from|restore
    python3 - "$RUN_ARGV_LOG" "$1" "$2" "$3" <<'PY'
import json, sys
path, index, sid, want = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
with open(path, encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
if index >= len(calls):
    raise SystemExit(f"missing run call {index}; captured {len(calls)}")
call = calls[index]

def option(name):
    for i, arg in enumerate(call):
        if arg == name:
            if i + 1 >= len(call):
                raise SystemExit(f"run option {name} has no value: {call!r}")
            return call[i + 1]
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
    return ""

if option("--sandbox-id") != sid:
    raise SystemExit(f"run call {index} belongs to {option('--sandbox-id')!r}, want {sid!r}: {call!r}")
selected = option("--" + want)
other = option("--restore" if want == "from" else "--from")
if not selected or other:
    raise SystemExit(f"run source mode={want!r} selected={selected!r} other={other!r}: {call!r}")
if want == "from" and not (".sandbox" in selected or selected.startswith("manifest://") or "@manifest:" in selected):
    raise SystemExit(f"run --from did not select Sandbox E: {selected!r}")
if want == "restore" and not (".snapshot" in selected or selected.startswith("manifest://") or "@manifest:" in selected):
    raise SystemExit(f"run --restore did not select Snapshot S: {selected!r}")
PY
}

assert_run_option_value() { # $1=index, $2=sid, $3=option name, $4=expected value
    python3 - "$RUN_ARGV_LOG" "$1" "$2" "$3" "$4" <<'PY'
import json, sys
calls = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
index = int(sys.argv[2])
if index >= len(calls):
    raise SystemExit(f"missing run call {index}; captured {len(calls)}")
call = calls[index]

def option(name):
    for i, arg in enumerate(call):
        if arg == name:
            if i + 1 >= len(call):
                raise SystemExit(f"run option {name} has no value: {call!r}")
            return call[i + 1]
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
    return ""

if option("--sandbox-id") != sys.argv[3]:
    raise SystemExit(f"run call {index} belongs to {option('--sandbox-id')!r}, want {sys.argv[3]!r}: {call!r}")
selected = option(sys.argv[4])
if selected != sys.argv[5]:
    raise SystemExit(f"run {sys.argv[4]}={selected!r}, want {sys.argv[5]!r}")
PY
}

assert_snapshot_argv() { # $1=index, remaining args=expected argv
    local index="$1"
    shift
    python3 - "$SNAPSHOT_ARGV_LOG" "$index" "$@" <<'PY'
import json, sys
path, index, expected = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
with open(path, encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
if index >= len(calls):
    raise SystemExit(f"missing snapshot call {index}; captured {len(calls)}")
if calls[index] != expected:
    raise SystemExit(f"snapshot call {index}={calls[index]!r}, want {expected!r}")
PY
}

assert_snapshot_has_no_policy_flags() { # $1=index
    python3 - "$SNAPSHOT_ARGV_LOG" "$1" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
call = calls[int(sys.argv[2])]
bad = [arg for arg in call if arg.startswith("--merge-ref=") or arg.startswith("--drop-caches=")]
if bad:
    raise SystemExit(f"builder snapshot unexpectedly received checkpoint policy flags: {bad!r}")
PY
}

assert_snapshot_mode() { # $1=index, $2=mode
    python3 - "$SNAPSHOT_ARGV_LOG" "$1" "$2" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    calls = [json.loads(line) for line in source if line.strip()]
call = calls[int(sys.argv[2])]
try:
    mode = call[call.index("--mode") + 1]
except (ValueError, IndexError):
    raise SystemExit(f"snapshot call has no --mode value: {call!r}")
if mode != sys.argv[3]:
    raise SystemExit(f"snapshot mode={mode!r}, want {sys.argv[3]!r}: {call!r}")
PY
}
