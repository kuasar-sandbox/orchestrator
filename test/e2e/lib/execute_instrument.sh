#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
ORCH_BIN_DIR="$WORK/orch-bin"
SNAPSHOT_ARGV_LOG="$WORK/snapshot-argv.jsonl"
EXPORT_ARGV_LOG="$WORK/export-argv.jsonl"
RUN_ARGV_LOG="$WORK/run-argv.jsonl"
PAUSE_BARRIER_TARGET="$WORK/pause-barrier-target"
PAUSE_BARRIER_REACHED="$WORK/pause-barrier-reached"
PAUSE_BARRIER_RELEASE="$WORK/pause-barrier-release"
mkdir -p "$ORCH_BIN_DIR"
# Prepared test-only node-ctl uses the real assignment/preparation/SDK boundary.
# Build with: go test -c -o node-ctl-runner-test ./cmd/node-ctl
# Production binaries contain no fault injection or source observation hooks.
runner_test_binary="${NODE_CTL_RUNNER_TEST_BINARY:-$BIN/node-ctl-runner-test}"
[ -x "$runner_test_binary" ] || fail "missing prepared runner test binary: $runner_test_binary"
cp "$runner_test_binary" "$ORCH_BIN_DIR/node-ctl"
printf '%s\n' "$WORK" > "$ORCH_BIN_DIR/runner-e2e-workdir"
# SDK execution resolves CH beside this sandbox-ctl wrapper; unlike the old
# CLI exec, it does not enter BIN. Keep the exact prepared VMM adjacent too.
for b in connector-ctl flatten-ctl manifest-ctl cloud-hypervisor; do
    ln -s "$BIN/$b" "$ORCH_BIN_DIR/$b"
done
cat > "$ORCH_BIN_DIR/sandbox-ctl" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [ "\${1:-}" = "snapshot" ]; then
    python3 - "$SNAPSHOT_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\\n")
PY
fi
path_id=""
if [ "\${1:-}" = "snapshot" ] && [ -f "$PAUSE_BARRIER_TARGET" ]; then
    previous=""
    for arg in "\$@"; do
        if [ "\$previous" = "--path-id" ]; then path_id="\$arg"; break; fi
        previous="\$arg"
    done
    if [ "\$path_id" = "\$(cat "$PAUSE_BARRIER_TARGET")" ]; then
        : > "$PAUSE_BARRIER_REACHED"
        while [ ! -e "$PAUSE_BARRIER_RELEASE" ]; do sleep 0.02; done
    fi
fi
if [ "\${1:-}" = "export" ]; then
    python3 - "$EXPORT_ARGV_LOG" "\$@" <<'PY'
import json, sys
with open(sys.argv[1], "a", encoding="utf-8") as output:
    output.write(json.dumps(sys.argv[2:]) + "\n")
PY
fi
exec "$BIN/sandbox-ctl" "\$@"
EOF
chmod +x "$ORCH_BIN_DIR/sandbox-ctl"
