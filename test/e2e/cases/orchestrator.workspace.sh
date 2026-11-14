#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
. "${E2E_LIB:?}/orchestrator/execute_env.sh"
write_orchestrator_config unset static
start_orchestrator "$WORK/orch.log"
wait_mmds_listener
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "$MK" >/dev/null
docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/workspace.image" --no-progress
ROOT=$(MANIFEST_KEY="$MK" "$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$WORK/workspace.image")
[[ "$ROOT" =~ ^[[:xdigit:]]{64}$ ]] || fail "invalid image root"
export WORKSPACE_IMAGE_ROOT="$ROOT" E2B_API_KEY="$AK" NODE_CTL_SOCKET="$WORK/node-ctl.socket"
python3 "$SCRIPT_DIR/workspace_handoff.py" "$PORT" "$DOMAIN" "$BIN" "$PROXY_PORT" "$CASE_OUT"
echo "PASS orchestrator.workspace.sh"
