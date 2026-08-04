#!/usr/bin/env bash
#
# e2e_sandbox_encrypted_diff.sh — host-side encrypted active diffs:
#
#   1. Create encrypted immutable root/data bases under crypto.local=required.
#   2. Cold-boot root + two data disks from plaintext diff_template inputs.
#   3. Assert every active diff has the 4 KiB KDXTS1 header and a
#      length-preserving sparse body, while the guest sees normal ext4 disks.
#   4. Snapshot to local encrypted @hmac artifacts and restore them.
#   5. Snapshot --upload and restore the manifest graph, preserving root and
#      both data-disk writes.
#
# The customer key stays on the host. The guest runtime and block geometry are
# unchanged. Missing prerequisites skip unless REQUIRE_KVM=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
IMAGE="${IMAGE:-busybox:latest}"
TAP_NAME="${TAP_NAME:-sb-xdiff0}"

skip() {
    echo
    echo "==> e2e_sandbox_encrypted_diff: skipping ($*)"
    [ "${REQUIRE_KVM:-0}" = "1" ] && exit 1
    exit 0
}

[ -e /dev/kvm ] || skip "/dev/kvm not present"
if [ ! -r /dev/kvm ] || [ ! -w /dev/kvm ]; then skip "/dev/kvm not accessible"; fi
for binary in cloud-hypervisor sandbox-ctl sandbox-runtime.bundle flatten-ctl manifest-ctl store-ctl mkfs.erofs; do
    [ -e "$BIN/$binary" ] || skip "missing $BIN/$binary"
done
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
[ -f "$VMLINUX" ] || skip "no vmlinux at $VMLINUX"
for command in docker mkfs.ext4 openssl python3; do
    command -v "$command" >/dev/null 2>&1 || skip "$command not on PATH"
done
if [ "$(id -u)" -ne 0 ]; then exec sudo -nE "$0" "$@"; fi

WORK="$(mktemp -d /tmp/e2e-encrypted-diff-XXXXXX)"
RUN_ROOT="$WORK/run"
OUT="$WORK/out"
mkdir -p "$RUN_ROOT" "$OUT"
PIDS=()
TAP_CREATED=0
cleanup() {
    set +e
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    pkill -f "cloud-hypervisor.*xd-" 2>/dev/null || true
    if [ "$TAP_CREATED" = 1 ]; then ip link del "$TAP_NAME" 2>/dev/null || true; fi
    if [ -n "${E2E_KEEP:-}" ]; then echo "kept: $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT

if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED=1
fi

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

KEY="$(openssl rand -hex 32)"
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/store-data, verify_content_key: true }
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store.log" 2>&1 &
PIDS+=("$!")
for _ in $(seq 1 100); do
    (echo >/dev/tcp/127.0.0.1/"$STORE_PORT") 2>/dev/null && break
    sleep 0.05
done
(echo >/dev/tcp/127.0.0.1/"$STORE_PORT") 2>/dev/null || { echo "FAIL: store did not become ready"; cat "$WORK/store.log"; exit 1; }

write_manifest_config() { # $1=path $2=auto|required
    cat > "$1" <<EOF
manifest: { key: "$KEY" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc }
crypto: { chunk: aes, manifest: aes, local: $2 }
EOF
}
AUTO_CONFIG="$WORK/manifest-auto.yaml"
REQUIRED_CONFIG="$WORK/manifest-required.yaml"
write_manifest_config "$AUTO_CONFIG" auto
write_manifest_config "$REQUIRED_CONFIG" required

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then docker pull "$IMAGE" >/dev/null; fi
echo "==> building immutable disk bases"
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/root.plain" --no-progress
mkdir -p "$WORK/dataset-root"
printf 'DATASET-BASE-OK\n' > "$WORK/dataset-root/DATASET-BASE-OK"
MKFS_EROFS_PATH="$BIN/mkfs.erofs" "$BIN/flatten-ctl" export --no-progress \
    --tmpdir "$WORK/flatten-tmp" --output "$WORK/dataset.plain" "$WORK/dataset-root"

key_bound_ref() { # $1=plaintext artifact $2=encrypted output
    local plain="$1" encrypted="$2" manifest_key plain_digest digest
    manifest_key=$("$BIN/manifest-ctl" store --manifest-config "$AUTO_CONFIG" --no-progress "$plain")
    "$BIN/manifest-ctl" load --manifest-config "$REQUIRED_CONFIG" --no-progress \
        --output "$encrypted" "$manifest_key"
    plain_digest=$(tar -tf "$plain" | sed -n 's/^\.kuasar\.sha256\.\([0-9a-f]\{64\}\)$/\1/p')
    [ "${#plain_digest}" -eq 64 ] || { echo "FAIL: plaintext fixture has no canonical digest marker" >&2; return 1; }
    digest=$(python3 - "$KEY" "$plain_digest" <<'PY'
import hashlib, hmac, sys
print(hmac.new(bytes.fromhex(sys.argv[1]), bytes.fromhex(sys.argv[2]), hashlib.sha256).hexdigest())
PY
)
    printf 'file://%s@hmac:%s\n' "$encrypted" "$digest"
}

ROOT_REF="$(key_bound_ref "$WORK/root.plain" "$WORK/root.encrypted")"
DATASET_REF="$(key_bound_ref "$WORK/dataset.plain" "$WORK/dataset.encrypted")"

make_ext4_template() { # $1=path $2=size
    truncate -s "$2" "$1"
    mkfs.ext4 -q -F "$1"
}
make_ext4_template "$WORK/root-template.ext4" 512M
make_ext4_template "$WORK/scratch-template.ext4" 256M
make_ext4_template "$WORK/dataset-template.ext4" 256M
ROOT_TEMPLATE_SHA="$(sha256sum "$WORK/root-template.ext4" | awk '{print $1}')"
SCRATCH_TEMPLATE_SHA="$(sha256sum "$WORK/scratch-template.ext4" | awk '{print $1}')"
DATASET_TEMPLATE_SHA="$(sha256sum "$WORK/dataset-template.ext4" | awk '{print $1}')"

ROOT_DIFF="$WORK/root-active.diff"
SCRATCH_DIFF="$WORK/scratch-active.diff"
DATASET_DIFF="$WORK/dataset-active.diff"
cat > "$WORK/cold.yaml" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: encrypted-diff }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $ROOT_REF
    overlay: { diff: file://$ROOT_DIFF, diff_template: file://$WORK/root-template.ext4 }
  disks:
    - { name: scratch, diff: file://$SCRATCH_DIFF, diff_template: file://$WORK/scratch-template.ext4 }
    - { name: dataset, base: $DATASET_REF, overlay: { diff: file://$DATASET_DIFF, diff_template: file://$WORK/dataset-template.ext4 } }
mounts:
  - { target: /scratch, type: disk, source: scratch }
  - { target: /data, type: disk, source: dataset }
launch: { exec: /bin/sleep, args: ["3600"] }
EOF

ready() { # $1=sandbox id $2=process id $3=log
    local sid="$1" pid="$2" log="$3"
    for _ in $(seq 1 120); do
        if timeout -k 2s 6 "$BIN/sandbox-ctl" exec --sandbox-id "$sid" --run-root "$RUN_ROOT" -- /bin/sh -c 'echo READY' \
            >"$WORK/ready.out" 2>/dev/null && grep -q '^READY$' "$WORK/ready.out"; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || { echo "FAIL: sandbox $sid exited"; cat "$log"; return 1; }
        sleep 1
    done
    echo "FAIL: sandbox $sid did not become ready"
    cat "$log"
    return 1
}

assert_diff() { # $1=active path $2=logical size
    local path="$1" logical="$2" magic physical blocks block_size allocated
    [ -f "$path" ] || { echo "FAIL: no active diff $path"; return 1; }
    magic=$(od -An -tx1 -N8 "$path" | tr -d ' \n')
    [ "$magic" = 894b44585453310a ] || { echo "FAIL: $path magic=$magic"; return 1; }
    physical=$(stat -c %s "$path")
    [ "$physical" -eq $((logical + 4096)) ] || { echo "FAIL: $path physical=$physical logical=$logical"; return 1; }
    read -r blocks block_size < <(stat -c '%b %B' "$path")
    allocated=$((blocks * block_size))
    [ "$allocated" -lt "$physical" ] || {
        echo "FAIL: $path is not sparse: allocated=$allocated physical=$physical"
        return 1
    }
}

echo "==> cold boot with required policy and plaintext templates"
SID1="xd-cold-$$"
LOG1="$WORK/cold.log"
"$BIN/sandbox-ctl" run --config "$WORK/cold.yaml" --manifest-config "$REQUIRED_CONFIG" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RUN_ROOT" --sandbox-id "$SID1" >"$LOG1" 2>&1 &
PID1=$!
PIDS+=("$PID1")
ready "$SID1" "$PID1" "$LOG1"
"$BIN/sandbox-ctl" exec --sandbox-id "$SID1" --run-root "$RUN_ROOT" -- /bin/sh -c \
    'set -e; cat /data/DATASET-BASE-OK; echo ROOT-ACTIVE-OK > /root-active; echo SCRATCH-ACTIVE-OK > /scratch/persist; echo DATA-ACTIVE-OK > /data/persist; sync' \
    >"$WORK/cold-check.out" 2>&1
grep -qx DATASET-BASE-OK "$WORK/cold-check.out" || { echo "FAIL: encrypted immutable data base was unreadable"; cat "$WORK/cold-check.out"; exit 1; }

assert_diff "$ROOT_DIFF" $((512 * 1024 * 1024))
assert_diff "$SCRATCH_DIFF" $((256 * 1024 * 1024))
assert_diff "$DATASET_DIFF" $((256 * 1024 * 1024))
[ "$(sha256sum "$WORK/root-template.ext4" | awk '{print $1}')" = "$ROOT_TEMPLATE_SHA" ]
[ "$(sha256sum "$WORK/scratch-template.ext4" | awk '{print $1}')" = "$SCRATCH_TEMPLATE_SHA" ]
[ "$(sha256sum "$WORK/dataset-template.ext4" | awk '{print $1}')" = "$DATASET_TEMPLATE_SHA" ]
HEADERS=$(for path in "$ROOT_DIFF" "$SCRATCH_DIFF" "$DATASET_DIFF"; do dd if="$path" bs=4096 count=1 status=none | sha256sum | awk '{print $1}'; done | sort -u | wc -l)
[ "$HEADERS" -eq 3 ] || { echo "FAIL: active diffs reused a wrapped header/XTS key"; exit 1; }
for marker in ROOT-ACTIVE-OK SCRATCH-ACTIVE-OK DATA-ACTIVE-OK; do
    if grep -aFq "$marker" "$ROOT_DIFF" "$SCRATCH_DIFF" "$DATASET_DIFF"; then
        echo "FAIL: guest plaintext marker appears in an encrypted body"
        exit 1
    fi
done
echo "==> PASS: root and data active diffs are encrypted, length-preserving, and guest-transparent"

"$BIN/sandbox-ctl" snapshot --sandbox-id "$SID1" --output "$OUT" --run-root "$RUN_ROOT" >"$WORK/snapshot-local.out" 2>"$WORK/snapshot-local.log"
wait "$PID1" 2>/dev/null || true
SNAP="$OUT/$SID1.snapshot"
[ -e "$SNAP" ] || { echo "FAIL: local snapshot missing"; cat "$WORK/snapshot-local.log"; exit 1; }
mapfile -t OVERLAYS < <(find "$OUT" -maxdepth 1 -type f -name '*.overlay' -print | sort)
[ "${#OVERLAYS[@]}" -eq 3 ] || { echo "FAIL: expected 3 overlay artifacts, got ${#OVERLAYS[@]}"; exit 1; }
for artifact in "${OVERLAYS[@]}" "$(readlink -f "$SNAP")"; do
    magic=$(od -An -tx1 -N8 "$artifact" | tr -d ' \n')
    [ "$magic" = 894b5453454e430a ] || { echo "FAIL: local artifact is not encrypted v1: $artifact"; exit 1; }
done
for marker in ROOT-ACTIVE-OK SCRATCH-ACTIVE-OK DATA-ACTIVE-OK; do
    if grep -aFq "$marker" "${OVERLAYS[@]}" "$(readlink -f "$SNAP")"; then
        echo "FAIL: guest plaintext marker appears in an encrypted snapshot artifact"
        exit 1
    fi
done
"$BIN/sandbox-ctl" info --json --manifest-config "$REQUIRED_CONFIG" "$SNAP" > "$WORK/snapshot.json"
python3 - "$WORK/snapshot.json" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
refs = []
root = cfg["Boot"]["Root"]
refs.extend([root.get("BaseRef", ""), root.get("Base", "")])
if root.get("Overlay"):
    refs.append(root["Overlay"].get("Base", ""))
    refs.extend(root["Overlay"].get("BaseFromRefs") or [])
refs.extend(root.get("BaseFromRefs") or [])
for disk in cfg["Boot"].get("Disks") or []:
    refs.extend([disk.get("BaseRef", ""), disk.get("Base", "")])
    refs.extend(disk.get("BaseFromRefs") or [])
    if disk.get("Overlay"):
        refs.append(disk["Overlay"].get("Base", ""))
        refs.extend(disk["Overlay"].get("BaseFromRefs") or [])
refs = [ref for ref in refs if ref]
if not refs or any("@hmac:" not in ref for ref in refs):
    raise SystemExit("disk artifact refs are not uniformly @hmac: %r" % refs)
PY
echo "==> PASS: local snapshot contains encrypted @hmac disk artifacts"

write_restore_config() { # $1=path $2=root diff $3=scratch diff $4=dataset diff $5=hostname
    cat > "$1" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: $5 }
boot:
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $ROOT_REF
    overlay: { diff: file://$2 }
  disks:
    - { name: scratch, diff: file://$3 }
    - { name: dataset, base: $DATASET_REF, overlay: { diff: file://$4 } }
EOF
}

ROOT_RESTORE="$WORK/root-restore.diff"
SCRATCH_RESTORE="$WORK/scratch-restore.diff"
DATASET_RESTORE="$WORK/dataset-restore.diff"
write_restore_config "$WORK/restore-local.yaml" "$ROOT_RESTORE" "$SCRATCH_RESTORE" "$DATASET_RESTORE" encrypted-restore
SID2="xd-local-$$"
LOG2="$WORK/restore-local.log"
"$BIN/sandbox-ctl" run --restore "$SNAP" --config "$WORK/restore-local.yaml" --manifest-config "$REQUIRED_CONFIG" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RUN_ROOT" --sandbox-id "$SID2" >"$LOG2" 2>&1 &
PID2=$!
PIDS+=("$PID2")
ready "$SID2" "$PID2" "$LOG2"
"$BIN/sandbox-ctl" exec --sandbox-id "$SID2" --run-root "$RUN_ROOT" -- /bin/sh -c \
    'set -e; cat /root-active /scratch/persist /data/persist /data/DATASET-BASE-OK; echo ROOT-RESTORED-OK > /root-restored; echo SCRATCH-RESTORED-OK > /scratch/restored; echo DATA-RESTORED-OK > /data/restored; sync' \
    >"$WORK/restore-local-check.out" 2>&1
for marker in ROOT-ACTIVE-OK SCRATCH-ACTIVE-OK DATA-ACTIVE-OK DATASET-BASE-OK; do
    grep -qx "$marker" "$WORK/restore-local-check.out" || { echo "FAIL: local restore lost $marker"; cat "$WORK/restore-local-check.out"; exit 1; }
done
assert_diff "$ROOT_RESTORE" $((512 * 1024 * 1024))
assert_diff "$SCRATCH_RESTORE" $((256 * 1024 * 1024))
assert_diff "$DATASET_RESTORE" $((256 * 1024 * 1024))
for marker in ROOT-RESTORED-OK SCRATCH-RESTORED-OK DATA-RESTORED-OK; do
    if grep -aFq "$marker" "$ROOT_RESTORE" "$SCRATCH_RESTORE" "$DATASET_RESTORE"; then
        echo "FAIL: restored guest plaintext marker appears in an encrypted body"
        exit 1
    fi
done
echo "==> PASS: required local restore rebuilt encrypted root and data uppers"

SNAP_KEY=$("$BIN/sandbox-ctl" snapshot --sandbox-id "$SID2" --upload --run-root "$RUN_ROOT" 2>"$WORK/snapshot-upload.log")
wait "$PID2" 2>/dev/null || true
SNAP_KEY="${SNAP_KEY#manifest://}"
[ "${#SNAP_KEY}" -eq 64 ] || { echo "FAIL: snapshot upload key length ${#SNAP_KEY}"; cat "$WORK/snapshot-upload.log"; exit 1; }

ROOT_REMOTE="$WORK/root-remote.diff"
SCRATCH_REMOTE="$WORK/scratch-remote.diff"
DATASET_REMOTE="$WORK/dataset-remote.diff"
write_restore_config "$WORK/restore-remote.yaml" "$ROOT_REMOTE" "$SCRATCH_REMOTE" "$DATASET_REMOTE" encrypted-remote
SID3="xd-remote-$$"
LOG3="$WORK/restore-remote.log"
"$BIN/sandbox-ctl" run --restore "manifest://$SNAP_KEY" --config "$WORK/restore-remote.yaml" --manifest-config "$REQUIRED_CONFIG" \
    --ch-binary "$BIN/cloud-hypervisor" --run-root "$RUN_ROOT" --sandbox-id "$SID3" >"$LOG3" 2>&1 &
PID3=$!
PIDS+=("$PID3")
ready "$SID3" "$PID3" "$LOG3"
"$BIN/sandbox-ctl" exec --sandbox-id "$SID3" --run-root "$RUN_ROOT" -- /bin/sh -c \
    'set -e; cat /root-active /scratch/persist /data/persist /root-restored /scratch/restored /data/restored /data/DATASET-BASE-OK' \
    >"$WORK/restore-remote-check.out" 2>&1
for marker in ROOT-ACTIVE-OK SCRATCH-ACTIVE-OK DATA-ACTIVE-OK ROOT-RESTORED-OK SCRATCH-RESTORED-OK DATA-RESTORED-OK DATASET-BASE-OK; do
    grep -qx "$marker" "$WORK/restore-remote-check.out" || { echo "FAIL: manifest restore lost $marker"; cat "$WORK/restore-remote-check.out"; exit 1; }
done
assert_diff "$ROOT_REMOTE" $((512 * 1024 * 1024))
assert_diff "$SCRATCH_REMOTE" $((256 * 1024 * 1024))
assert_diff "$DATASET_REMOTE" $((256 * 1024 * 1024))
echo "==> PASS: manifest restore preserved root/data contents and encrypted fresh uppers"
kill -TERM "$PID3" 2>/dev/null || true
wait "$PID3" 2>/dev/null || true

echo "==> e2e_sandbox_encrypted_diff: OK"
