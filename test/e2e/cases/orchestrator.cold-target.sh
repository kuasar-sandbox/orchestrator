#!/usr/bin/env bash
set -euo pipefail
: "${BIN:?BIN must point to prepared products}"
: "${WORK:?WORK must be provided by the E2E runner}"
: "${E2E_LIB:?E2E_LIB must point to prepared helpers}"
: "${ORCHESTRATOR_BASE_IMAGE:?ORCHESTRATOR_BASE_IMAGE must name the prepared base image}"
. "$E2E_LIB/orchestrator/tarstream.sh"

for b in cloud-hypervisor sandbox-ctl node-ctl sandbox-init sandbox-runtime.bundle flatten-ctl vmlinux; do
    [ -e "$BIN/$b" ] || { echo "missing prepared product: $BIN/$b" >&2; exit 1; }
done
for tool in docker python3 ip mkfs.ext4; do command -v "$tool" >/dev/null; done
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || { echo "/dev/kvm is required" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || { echo "root is required" >&2; exit 1; }
docker image inspect "$ORCHESTRATOR_BASE_IMAGE" >/dev/null

TAP="cold-target-$$"; CGROOT=/sys/fs/cgroup/sandboxes; SID=cold-target
DAEMON=""; SBPID=""
cleanup(){ set +e; [ -n "$SBPID" ] && kill -TERM "$SBPID" 2>/dev/null; [ -n "$SBPID" ] && wait "$SBPID" 2>/dev/null; [ -n "$DAEMON" ] && kill -TERM "$DAEMON" 2>/dev/null; [ -n "$DAEMON" ] && wait "$DAEMON" 2>/dev/null; ip link del "$TAP" 2>/dev/null; rmdir "$CGROOT/$SID" 2>/dev/null; }
trap cleanup EXIT
ip tuntap add "$TAP" mode tap; ip link set "$TAP" up
mkdir -p "$CGROOT/$SID" "$WORK/run" "$WORK/lib" "$WORK/units"
echo "+memory +cpu" >"$CGROOT/cgroup.subtree_control" 2>/dev/null || true

docker save "$ORCHESTRATOR_BASE_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/base.img" --no-progress
BASE="$(plaintext_tarstream_ref "$WORK/base.img")"
truncate -s 1G "$WORK/diff.ext4"; mkfs.ext4 -q -F -O ^has_journal "$WORK/diff.ext4"
cat >"$WORK/node.yaml" <<EOF
api: { domain: cold-target.local, listen: "127.0.0.1:0" }
encryption_key: "0000000000000000000000000000000000000000000000000000000000000000"
proxy: { auth: enforce }
sandbox: { boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle } }
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node.sock, db_path: $WORK/node.db }
units: { dir: $WORK/units, install: false }
resource_listen:
  enabled: true
  socket: $WORK/resource.sock
  state_path: $WORK/state.json
  cgroup_scan_paths: [$CGROOT]
  resources: { physical_memory: 4GiB, physical_cpu: 4, host_reserved: { memory: 512MiB, cpu: 1 } }
  watermarks: { operational_margin_factor: 0.10, high_factor: 0.85, low_factor: 0.70, emergency_factor: 0.05, startup_factor: 0.50 }
  rate_limits: { memory_grant_per_sec_factor: 0.20 }
  admission: { rate: 50, burst: 50, startup_ttl: 120s, queue_ttl: 30s, queue_max_depth: 256 }
EOF
"$BIN/node-ctl" conductor serve --config "$WORK/node.yaml" >"$WORK/node.log" 2>&1 & DAEMON=$!
for _ in $(seq 1 80); do [ -S "$WORK/resource.sock" ] && "$BIN/node-ctl" resource status --socket "$WORK/resource.sock" >/dev/null 2>&1 && break; kill -0 "$DAEMON" || exit 1; sleep .25; done
[ -S "$WORK/resource.sock" ] || { cat "$WORK/node.log"; exit 1; }

cat >"$WORK/sandbox.yaml" <<EOF
resources:
  capacity: { cpu: 2, memory: 8GiB }
  allocatable: { cpu: 0.1, memory: 128MiB }
  control: { cgroup_path: $CGROOT/$SID, controller: $WORK/resource.sock }
  startup: { memory: 512MiB }
network: { tap: $TAP }
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0"
  root: { base: $BASE, overlay: { diff: file://$WORK/diff.ext4 } }
launch:
  exec: /usr/local/bin/python3
  args: ["-c", "print('PYBOOT-OK')"]
EOF
"$BIN/sandbox-ctl" run --config "$WORK/sandbox.yaml" --sandbox-id "$SID" --ch-binary "$BIN/cloud-hypervisor" --run-root "$WORK/run" >"$WORK/sandbox.log" 2>&1 & SBPID=$!
deadline=$((SECONDS+120)); settled=0
while [ "$SECONDS" -lt "$deadline" ]; do
  if grep -q PYBOOT-OK "$WORK/sandbox.log" 2>/dev/null; then
    rows="$("$BIN/node-ctl" resource list --socket "$WORK/resource.sock" 2>/dev/null || true)"
    if ROWS="$rows" python3 - <<'PY' 2>/dev/null
import json,os
r=json.loads(os.environ["ROWS"]); assert len(r)==1
x=r[0]; assert x.get("connected") is True and not x.get("provisional",False) and x.get("stage")=="settled"
PY
    then settled=1; break; fi
  fi
  kill -0 "$SBPID" || break
  sleep .1
done
[ "$settled" = 1 ] || { cat "$WORK/sandbox.log"; echo "cold target did not reach app+settled reservation" >&2; exit 1; }
cap=$((8*1024*1024*1024)); initial=$((512*1024*1024)); balloon=$((cap-initial))
grep -q -- '--cpus boot=2' "$WORK/sandbox.log" || { cat "$WORK/sandbox.log"; exit 1; }
grep -q "memory-zone.*size=$cap" "$WORK/sandbox.log" || { cat "$WORK/sandbox.log"; exit 1; }
grep -q -- "--balloon size=$balloon" "$WORK/sandbox.log" || { cat "$WORK/sandbox.log"; exit 1; }
grep -q "controller admit: .*initial_reservation=$initial" "$WORK/sandbox.log" || { cat "$WORK/sandbox.log"; exit 1; }
grep -q "initial cold Budget reserved=$initial" "$WORK/sandbox.log" || { cat "$WORK/sandbox.log"; exit 1; }
echo "PASS orchestrator.cold-target.sh"
