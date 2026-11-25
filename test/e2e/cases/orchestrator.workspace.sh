#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
# This maintained case exercises generated disks without using the raw files
# prepared by the common fixture for other cases.
rm -f -- "$OVL" "$BLD"
OVL='mkfs://ext4?size=1GiB'
BLD='mkfs://ext4?size=2GiB'
write_workspace_config() {
write_orchestrator_config unset static
python3 - "$WORK/config.yaml" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
text = path.read_text()
assert text.count("overlay_diff_template: ") == 1
path.write_text(text.replace("overlay_diff_template: ",
    'workspace_diff_template: "mkfs://ext4?size=1GiB", overlay_diff_template: '))
PY
}
write_workspace_config
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/workspace.image" --no-progress
ROOT=$(MANIFEST_KEY="$MK" "$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$WORK/workspace.image")
[[ "$ROOT" =~ ^[[:xdigit:]]{64}$ ]] || fail "invalid image root"
export WORKSPACE_IMAGE_ROOT="$ROOT" E2B_API_KEY="$AK" NODE_CTL_SOCKET="$WORK/node-ctl.socket" BUILDER_CPU
python3 "$SCRIPT_DIR/workspace_handoff.py" "$PORT" "$DOMAIN" "$BIN" "$PROXY_PORT" "$CASE_OUT"
stop_orchestrator
write_orchestrator_config unset static
start_orchestrator "$WORK/orch-fixed-slot.log"
wait_mmds_listener
WORKSPACE_DEFAULT_TEST=fixed-slot python3 "$SCRIPT_DIR/workspace_handoff.py" "$PORT" "$DOMAIN" "$BIN" "$PROXY_PORT" "$CASE_OUT"
stop_orchestrator
write_workspace_config
cat >> "$WORK/config.yaml" <<EOF
  remote: { ref_location_parent: "file://$WORK/workspace-published" }
EOF
start_orchestrator "$WORK/orch-workspace-location.log"
wait_mmds_listener
# Repeat only handoff against named publication, including a second export that
# must retain the first sandbox's published parent instead of copying it again.
WORKSPACE_DEFAULT_TEST=named-handoff WORKSPACE_NAMED_PARENT="$WORK/workspace-published" \
    WORKSPACE_MANIFEST_CONFIG="$WORK/manifest.yaml" MANIFEST_KEY="$MK" \
    python3 "$SCRIPT_DIR/workspace_handoff.py" "$PORT" "$DOMAIN" "$BIN" "$PROXY_PORT" "$CASE_OUT"
echo "PASS orchestrator.workspace.sh"
