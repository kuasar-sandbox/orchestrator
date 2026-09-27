#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
sandbox_run_id() { # $1=sandbox id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select run_id from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "")
PY
}
sandbox_state() { # $1=sandbox id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select state from sandboxes where id=?", (sys.argv[2],)).fetchone()
print(row[0] if row else "missing")
PY
}
assert_dead_no_ownership() { # $1=sandbox id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("""
        select state, run_id, vswitch_port, floatingip, inner_ip, port_mac,
               run_dir, base_dir, envd_uds, ci_uds,
               resume_source_kind, resume_source_ref, resume_sandbox_ref
          from sandboxes where id=?
    """, (sys.argv[2],)).fetchone()
if row is None or row[0] != "dead" or any(row[1:]):
    raise SystemExit(f"dead sandbox retained local ownership: {row!r}")
PY
}
# Capture identity is a complete content-identified pair; directory ownership is
# execution context rather than part of the public JSON/database file spelling.
checkpoint_pair() { # $1=sandbox id, $2=kind, $3=semantic alias
    python3 - "$WORK/lib/node-ctl.db" "$1" "$2" "$3" <<'PY'
import json, os, re, sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select state, resume_source_kind, resume_source_ref, resume_sandbox_ref, launch_mode from sandboxes where id=?", (sys.argv[2],)).fetchone()
if row is None or row[:2] != ("paused", sys.argv[3]) or row[4]:
    raise SystemExit(f"invalid paused checkpoint: {row!r}")
def physical(ref):
    match = re.fullmatch(r"file://([^/@]+)@(digest|hmac|manifest):([0-9a-f]{64})", ref)
    if not match or match[1] in (".", "..") or "\\" in match[1]:
        raise SystemExit(f"checkpoint must retain a content-identified basename: {ref!r}")
    path = os.path.join(os.path.dirname(sys.argv[4]), match[1])
    if not os.path.isfile(path):
        raise SystemExit(f"checkpoint carrier unavailable: {ref!r}")
    return path
if os.path.realpath(physical(row[2])) != os.path.realpath(sys.argv[4]):
    raise SystemExit(f"checkpoint root differs from committed alias: {row!r}")
if row[1] == "snapshot":
    physical(row[3])
elif row[3]:
    raise SystemExit(f"E-only checkpoint retained Snapshot association: {row!r}")
print(json.dumps(row, separators=(",", ":")))
PY
}
checkpoint_runtime_ref() { # $1=validated pair JSON, $2=checkpoint directory
    python3 - "$1" "$2" <<'PY'
import json, os, sys
root = json.loads(sys.argv[1])[2]
print("file://" + os.path.join(sys.argv[2], root.removeprefix("file://")))
PY
}
wait_sandbox_state() { # $1=sandbox id, $2=state, $3=attempts(optional)
    local sid="$1" want="$2" attempts="${3:-120}" state=""
    for _ in $(seq 1 "$attempts"); do
        state="$(sandbox_state "$sid")"
        [ "$state" = "$want" ] && return 0
        sleep 0.1
    done
    echo "sandbox $sid state=$state, want $want" >&2
    return 1
}
wait_paused_cleanup() { # $1=sandbox id, $2=attempts(optional)
    local sid="$1" attempts="${2:-120}"
    for _ in $(seq 1 "$attempts"); do
        if python3 - "$WORK/lib/node-ctl.db" "$sid" "$WORK/lib/sandboxes/$sid" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("""
        select state, run_id, vswitch_port, floatingip, inner_ip, port_mac,
               run_dir, envd_uds, ci_uds, base_dir
          from sandboxes where id=?
    """, (sys.argv[2],)).fetchone()
want = ("paused", "", "", "", "", "", "", "", "", sys.argv[3])
raise SystemExit(0 if row == want else 1)
PY
        then
            [ ! -e "$WORK/run/sandboxes/$sid" ] && [ -d "$WORK/lib/sandboxes/$sid" ] && return 0
        fi
        sleep 0.1
    done
    python3 - "$WORK/lib/node-ctl.db" "$sid" <<'PY' >&2 || true
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    print("paused cleanup row:", db.execute(
        "select state,run_id,vswitch_port,run_dir,base_dir,envd_uds,ci_uds from sandboxes where id=?",
        (sys.argv[2],),
    ).fetchone())
PY
    return 1
}
wait_unit_journal_contains() { # $1=unit, $2=fixed string, $3=output file
    local unit="$1" pattern="$2" output="$3"
    for _ in $(seq 1 50); do
        journalctl -u "$unit" --no-pager >"$output" 2>/dev/null || true
        grep -Fq "$pattern" "$output" && return 0
        sleep 0.1
    done
    return 1
}
assert_sandbox_detail() {
    python3 - "$@" <<'PY'
import datetime
import json
import re
import sys

path, sandbox_id, expected_cpu, expected_memory, expected_disk = sys.argv[1:]
expected = {
    "cpuCount": int(expected_cpu),
    "memoryMB": int(expected_memory),
    "diskSizeMB": int(expected_disk),
}
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
with open(path, encoding="utf-8") as f:
    detail = json.load(f)

if detail.get("sandboxID") != sandbox_id:
    raise SystemExit(f"detail sandboxID={detail.get('sandboxID')!r}, want {sandbox_id!r}")
for field in ("startedAt", "endAt"):
    value = detail.get(field)
    if not isinstance(value, str) or not RFC3339.fullmatch(value):
        raise SystemExit(f"detail {field}={value!r} is not RFC3339")
    try:
        datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"detail {field}={value!r} is not RFC3339: {exc}")

started = datetime.datetime.fromisoformat(detail["startedAt"].replace("Z", "+00:00"))
ended = datetime.datetime.fromisoformat(detail["endAt"].replace("Z", "+00:00"))
if ended < started:
    raise SystemExit(f"detail endAt={detail['endAt']!r} is before startedAt={detail['startedAt']!r}")
for field, want in expected.items():
    if detail.get(field) != want:
        raise SystemExit(f"detail {field}={detail.get(field)!r}, want {want}")
PY
}
