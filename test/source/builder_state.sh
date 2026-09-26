#!/usr/bin/env bash
# Source-level Build state/policy contracts extracted from the legacy mega-case.
set -euo pipefail

build_trigger_signature() { # $1=build id
    python3 - "$WORK/lib/node-ctl.db" "$1" <<'PY'
import json, sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute(
        """select kind, from_image, from_template, start_cmd, ready_cmd,
                  steps_json, registry_auth_enc, metadata_json, builder_json,
                  status, reason, run_id
             from builds where build_id=?""",
        (sys.argv[2],),
    ).fetchone()
assert row is not None, "build row missing"
print(json.dumps(row, separators=(",", ":")))
PY
}

assert_terminal_build_unowned() { # $1=build id, $2=terminal state
    python3 - "$WORK/lib/node-ctl.db" "$1" "$2" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("""
        select status, run_id, execution_claimed, execution_claimed_unix,
               phase, phase_sandbox_id,
               runtime_vswitch_port, runtime_floating_ip, runtime_port_mac,
               runtime_envd_access_token_enc, runtime_prepare_json,
               execution_result_json
          from builds where build_id=?
    """, (sys.argv[2],)).fetchone()
assert row is not None and row[0] == sys.argv[3], row
assert row[1] == "" and row[2] == 0 and row[3] == 0, row
assert all(value == "" for value in row[4:]), row
PY
}

assert_build_finalized() { # build id label
    local bid="$1" label="$2"
    [ ! -e "$WORK/run/builds/$bid" ] || fail "$label retained BuildRunDir"
    [ ! -e "$WORK/lib/builds/$bid" ] || fail "$label retained BuildBaseDir"
    if find "$WORK/run" -type f \( -name '*.sandbox' -o -name '*.snapshot' -o -name '*.bundle' \) \
        -print -quit | grep -q .; then
        find "$WORK/run" -type f -print >&2
        fail "$label left a complete E/S/Bundle under run_root"
    fi
}

