#!/usr/bin/env bash
#
# Reproducible A/B/C/D working-set snapshot matrix for sandboxer #54/#41.
#
# Every sample restores the same immutable local B, performs the same HTTP
# warm-up, captures a local W, publishes W independently, then restores the
# portable W from a cold manifest cache. The main matrix fixes prefetch=off so
# only drop-caches/merge-ref vary; D also gets a paired prefetch=memory restore.
#
# Output (PERF_OUT_DIR, ignored by git by default):
#   environment.json  exact revisions, host, image, binaries, workload
#   samples.jsonl     one machine-readable row per independent restore
#   report.md         nearest-rank p50/p95/p99 summary
#   raw/              per-sample snapshot/publisher/restore evidence
#
# Canonical report runs should use PERF_ITERS=30. The default 5 is a smoke run.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
VMLINUX="${VMLINUX:-$BIN/vmlinux}"
IMAGE="${IMAGE:-python:3.12-slim}"
TAP_NAME="${TAP_NAME:-sb-tap0}"
ITERS="${PERF_ITERS:-5}"
MATRIX_GROUPS_RAW="${PERF_GROUPS:-A B C D}"
read -r -a MATRIX_GROUPS <<<"$MATRIX_GROUPS_RAW"
WARM_BYTES="${PERF_WARM_BYTES:-16777216}"
OUT_DIR="${PERF_OUT_DIR:-$REPO_ROOT/test/results/sandbox-perf-working-set-$(date -u +%Y%m%dT%H%M%SZ)}"
if [ -w /proc/sys/vm/drop_caches ]; then
    HOST_CACHE_RESET_MODE=global-drop-caches
else
    HOST_CACHE_RESET_MODE=targeted-posix-fadvise-dontneed
fi

fatal() {
    echo "FATAL: $*" >&2
    exit 1
}

[[ "$ITERS" =~ ^[1-9][0-9]*$ ]] || fatal "PERF_ITERS must be a positive integer"
[[ "$WARM_BYTES" =~ ^[1-9][0-9]*$ ]] || fatal "PERF_WARM_BYTES must be a positive integer"
for group in "${MATRIX_GROUPS[@]}"; do
    case "$group" in A|B|C|D) ;; *) fatal "unknown PERF_GROUPS entry: $group" ;; esac
done

[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || fatal "/dev/kvm must be readable and writable"
for binary in cloud-hypervisor sandbox-ctl sandbox-init sandbox-runtime.bundle flatten-ctl manifest-ctl store-ctl cache-ctl mkfs.erofs; do
    [ -e "$BIN/$binary" ] || fatal "missing $BIN/$binary"
done
[ -f "$VMLINUX" ] || fatal "missing kernel $VMLINUX"
for command in curl docker ip mkfs.ext4 python3 sha256sum; do
    command -v "$command" >/dev/null 2>&1 || fatal "missing host command: $command"
done
docker info >/dev/null 2>&1 || fatal "docker is not usable"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi

WORK="$(mktemp -d /tmp/perf-working-set-XXXXXX)"
mkdir -p "$OUT_DIR/raw"
SAMPLES="$OUT_DIR/samples.jsonl"
ENVIRONMENT="$OUT_DIR/environment.json"
REPORT="$OUT_DIR/report.md"
: >"$SAMPLES"

declare -a PIDS=()
TAP_CREATED=0
STORE_PID=""
CACHE_PID=""
ACTIVE_SANDBOX_PID=""
STORE_ROOT="$WORK/store"
STORE_BASELINE="$WORK/store-baseline"

cleanup() {
    set +e
    for pid in "${PIDS[@]}"; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill -TERM "$pid" 2>/dev/null
    done
    for pid in "${PIDS[@]}"; do
        [ -n "$pid" ] && wait "$pid" 2>/dev/null
    done
    [ "$TAP_CREATED" = 1 ] && ip link del "$TAP_NAME" 2>/dev/null
    if [ -n "${PERF_KEEP_WORK:-}" ]; then
        echo "kept work directory: $WORK" >&2
    else
        rm -rf -- "$WORK"
    fi
}
trap cleanup EXIT

untrack_pid() { # $1=pid
    local target="$1" pid
    local -a kept=()
    for pid in "${PIDS[@]}"; do
        [ "$pid" = "$target" ] || kept+=("$pid")
    done
    PIDS=("${kept[@]}")
}

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'
}

milliseconds_between() { # $1=start ns, $2=end ns
    python3 -c 'import sys; print(f"{(int(sys.argv[2])-int(sys.argv[1]))/1e6:.3f}")' "$1" "$2"
}

reset_tap() {
    ip link set "$TAP_NAME" down 2>/dev/null || true
    ip link set "$TAP_NAME" up 2>/dev/null || true
    sleep 0.3
}

stop_sandbox() { # $1=pid
    local pid="$1" waited=0
    [ -n "$pid" ] || return 0
    if ! kill -0 "$pid" 2>/dev/null; then
        wait "$pid" 2>/dev/null || true
        untrack_pid "$pid"
        ACTIVE_SANDBOX_PID=""
        reset_tap
        return 0
    fi
    kill -TERM "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 600 ]; do
        sleep 0.1
        waited=$((waited + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        fatal "sandbox-ctl pid=$pid did not exit within 60s; left running for diagnosis"
    fi
    wait "$pid" 2>/dev/null || true
    untrack_pid "$pid"
    ACTIVE_SANDBOX_PID=""
    reset_tap
}

wait_http_ready() { # $1=pid, $2=log, $3=path
    local pid="$1" log="$2" path="$3"
    for _ in $(seq 1 1200); do
        if curl --noproxy '*' --fail --silent --show-error --connect-timeout 0.2 --max-time 0.5 \
            "http://169.254.1.1:8000$path" >/dev/null 2>&1; then
            return 0
        fi
        kill -0 "$pid" 2>/dev/null || { tail -80 "$log" >&2; return 1; }
        sleep 0.05
    done
    tail -80 "$log" >&2
    return 1
}

drop_host_caches() {
    sync
    if [ "$HOST_CACHE_RESET_MODE" = global-drop-caches ]; then
        echo 3 >/proc/sys/vm/drop_caches
        return
    fi

    python3 - "$STORE_ROOT" "$B_ARTIFACT" "${B_DISK_TOPS[@]}" \
        "$VMLINUX" "$BIN/sandbox-runtime.bundle" "$BIN/cloud-hypervisor" "$BIN/sandbox-ctl" <<'PY'
import os
import pathlib
import stat
import sys

seen = set()
paths = []
for raw in sys.argv[1:]:
    candidate = pathlib.Path(raw)
    if candidate.is_dir():
        for root, directories, names in os.walk(candidate, followlinks=False):
            directories.sort()
            for name in sorted(names):
                paths.append(pathlib.Path(root, name))
    else:
        paths.append(candidate)

for path in paths:
    try:
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size == 0:
            continue
        identity = (metadata.st_dev, metadata.st_ino)
        if identity in seen:
            continue
        seen.add(identity)
        fd = os.open(resolved, os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
    except OSError as error:
        raise SystemExit(f"cannot evict host page cache for {path}: {error}") from error
PY
}

if ! ip link show "$TAP_NAME" >/dev/null 2>&1; then
    ip tuntap add dev "$TAP_NAME" mode tap
    ip addr add 169.254.1.0/31 dev "$TAP_NAME"
    ip link set "$TAP_NAME" up
    TAP_CREATED=1
fi

# ---- manifest store and cold-resettable cache ---------------------------

STORE_PORT="$(free_port)"
CACHE_PORT="$(free_port)"
CACHE_HEALTH_PORT="$(free_port)"
CACHE_ROCKS="$WORK/cache-rocks"
MANIFEST_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"

cat >"$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $STORE_ROOT
  verify_content_key: true
EOF
cat >"$WORK/cache.yaml" <<EOF
mode: tiered
listen: 127.0.0.1:$CACHE_PORT
health_listen: 127.0.0.1:$CACHE_HEALTH_PORT
rpc_timeout: 5s
freq:
  counters: 1M
  reset_after: 100K
tiers:
  - type: embedded
    rocks:
      path: $CACHE_ROCKS
      disk_bytes: 2GiB
      mem_ratio: 0.1
      direct_reads: false
      bloom_bits: 10
origin:
  type: store
  store:
    endpoint: 127.0.0.1:$STORE_PORT
    pool: 2
    timeout: 5s
  max_inflight: 16
EOF
cat >"$WORK/manifest.yaml" <<EOF
manifest:
  key: "$MANIFEST_KEY"
store:
  endpoint: 127.0.0.1:$STORE_PORT
  pool: 4
  timeout: 30s
cache:
  endpoint: 127.0.0.1:$CACHE_PORT
  pool: 4
  timeout: 10s
chunk:
  mode: cdc
crypto:
  chunk: aes
  manifest: aes
EOF

start_store() {
    "$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store.log" 2>&1 &
    STORE_PID=$!
    PIDS+=("$STORE_PID")
    for _ in $(seq 1 100); do
        (echo >/dev/tcp/127.0.0.1/"$STORE_PORT") 2>/dev/null && return 0
        kill -0 "$STORE_PID" 2>/dev/null || break
        sleep 0.05
    done
    tail -80 "$WORK/store.log" >&2
    fatal "store-ctl did not become ready"
}

stop_store() {
    [ -n "$STORE_PID" ] || return 0
    kill -TERM "$STORE_PID" 2>/dev/null || true
    wait "$STORE_PID" 2>/dev/null || true
    untrack_pid "$STORE_PID"
    STORE_PID=""
}

reset_store() {
    stop_store
    rm -rf -- "$STORE_ROOT"
    cp -al -- "$STORE_BASELINE" "$STORE_ROOT"
    start_store
}

start_cache() {
    "$BIN/cache-ctl" serve --config "$WORK/cache.yaml" >"$WORK/cache.log" 2>&1 &
    CACHE_PID=$!
    PIDS+=("$CACHE_PID")
    for _ in $(seq 1 100); do
        "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$CACHE_HEALTH_PORT" 2>/dev/null | grep -q SERVING && return 0
        kill -0 "$CACHE_PID" 2>/dev/null || break
        sleep 0.05
    done
    tail -80 "$WORK/cache.log" >&2
    fatal "cache-ctl did not become ready"
}

stop_cache() {
    if [ -n "$CACHE_PID" ]; then
        kill -TERM "$CACHE_PID" 2>/dev/null || true
        wait "$CACHE_PID" 2>/dev/null || true
        untrack_pid "$CACHE_PID"
        CACHE_PID=""
    fi
}

reset_cache() {
    stop_cache
    rm -rf -- "$CACHE_ROCKS"
    start_cache
}

reset_sample_storage() {
    stop_cache
    reset_store
    rm -rf -- "$CACHE_ROCKS"
    start_cache
}

cache_info() { # $1=output
    "$BIN/cache-ctl" info --endpoint "127.0.0.1:$CACHE_HEALTH_PORT" --json >"$1"
}

"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1
start_store
start_cache

# ---- immutable base artifacts and B ------------------------------------

docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull "$IMAGE" >/dev/null
ROOT_IMAGE="$WORK/root.img"
docker save "$IMAGE" | "$BIN/flatten-ctl" export --output "$ROOT_IMAGE" --no-progress
[ -s "$ROOT_IMAGE" ] || fatal "empty root image"
ROOT_KEY=$("$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$ROOT_IMAGE")
[[ "$ROOT_KEY" =~ ^[0-9a-f]{64}$ ]] || fatal "invalid root manifest key: $ROOT_KEY"

mkdir -p "$WORK/dataset-source"
echo DATASET-OK >"$WORK/dataset-source/DATASET-OK"
DATASET_IMAGE="$WORK/dataset.img"
MKFS_EROFS_PATH="$BIN/mkfs.erofs" "$BIN/flatten-ctl" export --tmpdir "$WORK/flatten-tmp" \
    --output "$DATASET_IMAGE" --no-progress "$WORK/dataset-source"
DATASET_KEY=$("$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$DATASET_IMAGE")
[[ "$DATASET_KEY" =~ ^[0-9a-f]{64}$ ]] || fatal "invalid dataset manifest key: $DATASET_KEY"

format_diff() { # $1=path, $2=size
    truncate -s "$2" "$1"
    mkfs.ext4 -q -F "$1"
}

write_config() { # $1=path, $2=diff dir, $3=prefetch, $4=cold|restore
    local path="$1" dir="$2" prefetch="$3" mode="${4:-restore}"
    mkdir -p "$dir"
    format_diff "$dir/root.ext4" 1G
    format_diff "$dir/scratch.ext4" 512M
    format_diff "$dir/dataset.ext4" 512M
    cat >"$path" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: perf-working-set }
boot:
  kernel: file://$VMLINUX
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: manifest://$ROOT_KEY
    overlay: { diff: file://$dir/root.ext4, size: 1GiB }
  disks:
    - { name: scratch, diff: file://$dir/scratch.ext4, diff_size: 512MiB }
    - { name: dataset, base: manifest://$DATASET_KEY, overlay: { diff: file://$dir/dataset.ext4, size: 512MiB } }
EOF
    if [ "$mode" = cold ]; then
        cat >>"$path" <<'EOF'
mounts:
  - { target: /scratch, type: disk, source: scratch }
  - { target: /data, type: disk, source: dataset }
launch:
  exec: /usr/local/bin/python3
  args: ["-m", "http.server", "8000", "--bind", "0.0.0.0", "--directory", "/scratch"]
  restart: never
EOF
    fi
    cat >>"$path" <<EOF
restore:
  prefetch: $prefetch
EOF
}

start_sandbox() { # $1=sid, $2=config, $3=run root, $4=base root, $5=log, $6=stats, $7=optional restore ref
    local sid="$1" config="$2" run_root="$3" base_root="$4" log="$5" stats="$6" restore_ref="${7:-}"
    mkdir -p "$run_root" "$base_root"
    local args=(run --sandbox-id "$sid" --config "$config" --manifest-config "$WORK/manifest.yaml"
        --ch-binary "$BIN/cloud-hypervisor" --run-root "$run_root" --base-root "$base_root"
        --stats-json "$stats")
    if [ -n "$restore_ref" ]; then
        args+=(--restore "$restore_ref")
    fi
    "$BIN/sandbox-ctl" "${args[@]}" >"$log" 2>&1 &
    ACTIVE_SANDBOX_PID=$!
    PIDS+=("$ACTIVE_SANDBOX_PID")
}

B_DIR="$WORK/base"
B_OUT="$B_DIR/snapshot"
mkdir -p "$B_OUT"
write_config "$B_DIR/cold.yaml" "$B_DIR/cold-diffs" off cold
start_sandbox ws-base "$B_DIR/cold.yaml" "$B_DIR/run" "$B_DIR/base-root" "$B_DIR/run.log" "$B_DIR/stats.json"
B_PID="$ACTIVE_SANDBOX_PID"
wait_http_ready "$B_PID" "$B_DIR/run.log" / || fatal "base sandbox did not become ready"
"$BIN/sandbox-ctl" exec --sandbox-id ws-base --run-root "$B_DIR/run" -- /bin/sh -c \
    'yes KUASAR-WORKING-SET | head -c "$1" > /scratch/working-set-cache.bin; echo ROOT-B-OK > /root-b; echo S-OK > /scratch/persist; echo D-OK > /data/persist; sync; sha256sum /scratch/working-set-cache.bin' \
    sh "$WARM_BYTES" >"$B_DIR/seed.log" 2>&1 || { cat "$B_DIR/seed.log" >&2; fatal "seed B workload"; }
WARM_SHA=$(grep -Eo '^[0-9a-f]{64}' "$B_DIR/seed.log" | head -1)
[[ "$WARM_SHA" =~ ^[0-9a-f]{64}$ ]] || fatal "could not extract warm-up checksum"
"$BIN/sandbox-ctl" snapshot --sandbox-id ws-base --output "$B_OUT" --run-root "$B_DIR/run" \
    >"$B_DIR/snapshot.log" 2>&1
wait "$B_PID" 2>/dev/null || true
untrack_pid "$B_PID"
ACTIVE_SANDBOX_PID=""
grep -Fq 'quiesce: guest acked (drop_caches=succeeded' "$B_DIR/run.log" \
    || { tail -80 "$B_DIR/run.log" >&2; fatal "B capture did not establish a cold guest cache"; }
B="$B_OUT/ws-base.snapshot"
[ -f "$B" ] || fatal "missing B snapshot $B"
B_ARTIFACT="$(readlink -f "$B")"
B_BASENAME="$(basename "$B_ARTIFACT")"
"$BIN/sandbox-ctl" info --json "$B" >"$B_DIR/info.json"
mapfile -t B_DISK_TOPS < <(python3 - "$B_DIR/info.json" "$B_OUT" <<'PY'
import json
import os
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    document = json.load(source)
root = sys.argv[2]
boot = document["Boot"]
paths = []
for node in [boot["Root"], *(boot.get("Disks") or [])]:
    overlay = node.get("Overlay")
    ref = overlay["Base"] if overlay else node["Base"]
    if not ref.startswith("file://"):
        raise SystemExit(f"B disk top is not local: {ref!r}")
    path = ref[len("file://"):].split("@", 1)[0]
    if not os.path.isabs(path):
        path = os.path.join(root, path)
    paths.append(os.path.realpath(path))
if len(set(paths)) != len(paths):
    raise SystemExit(f"B disk tops are not distinct: {paths!r}")
print(*paths, sep="\n")
PY
)
[ "${#B_DISK_TOPS[@]}" -eq 3 ] || fatal "B must contain one root plus two data-disk tops"
for path in "${B_DISK_TOPS[@]}"; do
    [ -f "$path" ] || fatal "missing B disk top $path"
done
reset_tap
stop_cache
stop_store
cp -a --reflink=auto -- "$STORE_ROOT" "$STORE_BASELINE"
start_store
start_cache
mkdir -p "$OUT_DIR/raw/base"
cp "$B_DIR/info.json" "$B_DIR/run.log" "$B_DIR/seed.log" "$B_DIR/snapshot.log" \
    "$OUT_DIR/raw/base/"
echo "==> immutable B: $B_ARTIFACT" >&2

# Record the complete environment after every binary and image has been used.
python3 - "$ENVIRONMENT" "$REPO_ROOT" "$BIN" "$VMLINUX" "$IMAGE" "$ITERS" "${MATRIX_GROUPS[*]}" "$WARM_BYTES" "$HOST_CACHE_RESET_MODE" <<'PY'
import csv
import datetime
import hashlib
import json
import os
import pathlib
import platform
import subprocess
import sys

(output, repo_root, bindir, kernel, image, iterations, groups, warm_bytes,
 host_cache_reset_mode) = sys.argv[1:]
workspace = pathlib.Path(repo_root).parent.parent
repository_names = ("accelerator", "connector", "guest-runtime", "orchestrator", "sandboxer")
revision_manifest = os.environ.get("KUASAR_REVISION_MANIFEST")
repositories = {}
if revision_manifest:
    manifest_path = pathlib.Path(revision_manifest)
    if not manifest_path.is_file():
        raise SystemExit(f"revision manifest is missing: {manifest_path}")
    with manifest_path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source, delimiter="\t"):
            name = row["repository"].removeprefix("kuasar-sandbox/")
            if name in repository_names:
                repositories[name] = {
                    "sha": row["resolved_sha"],
                    "dirty": False,
                    "requested_ref": row["requested_ref"],
                    "role": row["role"],
                    "source": "revision-manifest",
                }
else:
    for name in repository_names:
        path = workspace / name
        try:
            top = pathlib.Path(
                subprocess.check_output(
                    ["git", "-C", path, "rev-parse", "--show-toplevel"], text=True
                ).strip()
            ).resolve()
            if top != path.resolve():
                raise RuntimeError(f"{path} resolved to unrelated git root {top}")
            sha = subprocess.check_output(["git", "-C", path, "rev-parse", "HEAD"], text=True).strip()
            dirty = bool(
                subprocess.check_output(
                    ["git", "-C", path, "status", "--porcelain"], text=True
                ).strip()
            )
        except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
            raise SystemExit(f"cannot resolve exact revision for {name}: {error}") from error
        repositories[name] = {"sha": sha, "dirty": dirty, "source": "git"}
missing_repositories = sorted(set(repository_names) - set(repositories))
if missing_repositories:
    raise SystemExit(f"revision set is incomplete: {missing_repositories}")
for name, revision in repositories.items():
    sha = revision["sha"]
    if len(sha) != 40 or any(character not in "0123456789abcdef" for character in sha):
        raise SystemExit(f"invalid revision for {name}: {sha!r}")

def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

cpu_model = None
try:
    for line in pathlib.Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            cpu_model = line.split(":", 1)[1].strip()
            break
except OSError:
    pass
mem_total_kib = None
try:
    for line in pathlib.Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            mem_total_kib = int(line.split()[1])
            break
except OSError:
    pass

binary_names = (
    "cache-ctl",
    "cloud-hypervisor",
    "flatten-ctl",
    "manifest-ctl",
    "mkfs.erofs",
    "sandbox-ctl",
    "sandbox-init",
    "sandbox-runtime.bundle",
    "store-ctl",
)
try:
    image_id = subprocess.check_output(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image], text=True
    ).strip()
except (OSError, subprocess.CalledProcessError):
    image_id = None
try:
    cloud_hypervisor_version = subprocess.check_output(
        [os.path.join(bindir, "cloud-hypervisor"), "--version"],
        text=True,
        stderr=subprocess.STDOUT,
    ).strip()
except (OSError, subprocess.CalledProcessError):
    cloud_hypervisor_version = None
environment = {
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "repositories": repositories,
    "host": {
        "uname": platform.uname()._asdict(),
        "cpu_model": cpu_model,
        "cpu_count": os.cpu_count(),
        "mem_total_kib": mem_total_kib,
        "kvm": str(pathlib.Path("/dev/kvm").resolve()),
    },
    "inputs": {
        "image": image,
        "image_id": image_id,
        "iterations": int(iterations),
        "groups": groups.split(),
        "warm_bytes": int(warm_bytes),
        "guest_memory": "512MiB",
        "guest_cpu": 1,
        "manifest_chunk_mode": "cdc",
        "manifest_encryption": {"chunk": "aes", "manifest": "aes"},
        "main_prefetch": "off",
        "paired_prefetch": "D/memory",
        "host_cache_reset": host_cache_reset_mode,
        "manifest_cache_reset": "restart cache-ctl with an empty RocksDB before each B and portable restore",
        "manifest_store_reset": "restart store-ctl from the immutable root/dataset baseline before each sample",
    },
    "artifacts": {
        "kernel_sha256": digest(kernel),
        "cloud_hypervisor_version": cloud_hypervisor_version,
        "binaries_sha256": {name: digest(os.path.join(bindir, name)) for name in binary_names},
    },
}
pathlib.Path(output).write_text(json.dumps(environment, indent=2, sort_keys=True) + "\n")
PY

group_flags() { # $1=group -> prints drop merge
    case "$1" in
        A) echo "true true" ;;
        B) echo "false true" ;;
        C) echo "true false" ;;
        D) echo "false false" ;;
    esac
}

artifact_metrics() { # $1=group $2=B-info $3=W-info $4=B-artifact $5=W-artifact $6=W-dir $7=snapshot-log $8=wall-ms $9=out
    python3 - "$@" <<'PY'
import json
import os
import re
import sys

group, b_info_path, w_info_path, b_artifact, w_artifact, w_dir, log_path, wall_ms, output = sys.argv[1:]
with open(b_info_path, encoding="utf-8") as source:
    parent = json.load(source)
with open(w_info_path, encoding="utf-8") as source:
    working = json.load(source)
with open(log_path, encoding="utf-8") as source:
    log = source.read()

def allocated(path):
    return os.stat(path).st_blocks * 512

def disk_nodes(document):
    boot = document["Boot"]
    return [boot["Root"], *(boot.get("Disks") or [])]

def top(node):
    overlay = node.get("Overlay")
    return overlay["Base"] if overlay else node["Base"]

def chain(node):
    overlay = node.get("Overlay")
    return (overlay.get("BaseFromRefs") if overlay else node.get("BaseFromRefs")) or []

def local_path(ref):
    if not ref.startswith("file://"):
        raise SystemExit(f"W disk top is not local: {ref!r}")
    path = ref[len("file://"):].split("@", 1)[0]
    if not os.path.isabs(path):
        path = os.path.join(w_dir, path)
    return path

expected_refs = 0 if group in {"A", "B"} else 1
refs = working.get("FromRefs") or []
if len(refs) != expected_refs:
    raise SystemExit(f"group {group} memory refs={refs!r}, want {expected_refs}")
if expected_refs and os.path.basename(refs[0].split("@", 1)[0]) != os.path.basename(b_artifact):
    raise SystemExit(f"group {group} memory parent={refs[0]!r}, want {os.path.basename(b_artifact)!r}")

parent_nodes = disk_nodes(parent)
working_nodes = disk_nodes(working)
if len(parent_nodes) != 3 or len(working_nodes) != 3:
    raise SystemExit("working-set matrix requires root plus two data disks")
disk_paths = []
for index, (parent_node, working_node) in enumerate(zip(parent_nodes, working_nodes)):
    parent_top = top(parent_node)
    if top(working_node) == parent_top or parent_top in chain(working_node):
        raise SystemExit(f"group {group} disk {index} retained merged parent {parent_top!r}")
    if chain(working_node) != chain(parent_node):
        raise SystemExit(
            f"group {group} disk {index} lower chain={chain(working_node)!r}, "
            f"want inherited {chain(parent_node)!r}"
        )
    path = local_path(top(working_node))
    if not os.path.isfile(path):
        raise SystemExit(f"group {group} disk {index} top is missing: {path}")
    disk_paths.append(path)

match = re.search(r"memory_size=(\d+) resident=(\d+) pause_ms=(\d+) dump_ms=(\d+)", log)
if not match:
    raise SystemExit(f"snapshot timing line missing from {log_path}")
memory_size, resident, pause_ms, dump_ms = map(int, match.groups())
row = {
    "b_memory_logical_bytes": os.stat(b_artifact).st_size,
    "b_memory_allocated_bytes": allocated(b_artifact),
    "w_memory_logical_bytes": os.stat(w_artifact).st_size,
    "w_memory_allocated_bytes": allocated(w_artifact),
    "w_disk_logical_bytes": sum(os.stat(path).st_size for path in disk_paths),
    "w_disk_allocated_bytes": sum(allocated(path) for path in disk_paths),
    "w_memory_resident_bytes": resident,
    "w_memory_resident_ratio": resident / memory_size if memory_size else None,
    "snapshot_pause_ms": pause_ms,
    "snapshot_dump_ms": dump_ms,
    "snapshot_wall_ms": float(wall_ms),
}
with open(output, "w", encoding="utf-8") as target:
    json.dump(row, target, sort_keys=True)
    target.write("\n")
PY
}

portable_restore() { # $1=group $2=iter $3=prefetch $4=portable-ref $5=artifact-json $6=publish-ms $7=portable-info $8=sample-root
    local group="$1" iteration="$2" prefetch="$3" portable_ref="$4" artifact_json="$5" publish_ms="$6" portable_info="$7" sample_root="$8"
    local restore_dir="$sample_root/restore-$prefetch"
    local raw_dir="$OUT_DIR/raw/$group/$iteration/$prefetch"
    mkdir -p "$restore_dir" "$raw_dir"

    write_config "$restore_dir/config.yaml" "$restore_dir/diffs" "$prefetch"
    reset_cache
    cache_info "$restore_dir/cache-before.json"
    drop_host_caches

    local sid="ws-${group,,}-${iteration}-${prefetch}"
    local start_ns ready_ns first_seconds first_ms
    start_ns=$(date +%s%N)
    start_sandbox "$sid" "$restore_dir/config.yaml" "$restore_dir/run" "$restore_dir/base-root" \
        "$restore_dir/run.log" "$restore_dir/stats.json" "$portable_ref"
    local pid="$ACTIVE_SANDBOX_PID"
    wait_http_ready "$pid" "$restore_dir/run.log" /persist || fatal "$group/$iteration/$prefetch restore did not become ready"
    ready_ns=$(date +%s%N)

    first_seconds=$(curl --noproxy '*' --fail --silent --show-error --connect-timeout 1 --max-time 120 \
        --output "$restore_dir/first-request.bin" --write-out '%{time_total}' \
        http://169.254.1.1:8000/working-set-cache.bin)
    local first_hash
    first_hash=$(sha256sum "$restore_dir/first-request.bin" | awk '{print $1}')
    [ "$first_hash" = "$WARM_SHA" ] || fatal "$group/$iteration/$prefetch first request returned the wrong content"
    rm -f -- "$restore_dir/first-request.bin"
    first_ms=$(python3 -c 'import sys; print(f"{float(sys.argv[1])*1000:.3f}")' "$first_seconds")

    "$BIN/sandbox-ctl" exec --sandbox-id "$sid" --run-root "$restore_dir/run" -- /bin/sh -c \
        'set -eu
         sync
         echo 3 > /proc/sys/vm/drop_caches
         [ "$(cat /root-w)" = ROOT-W-OK ]
         [ "$(cat /scratch/working-set)" = SCRATCH-W-OK ]
         [ "$(cat /data/working-set)" = DATA-W-OK ]
         echo W-DISK-STATE-OK' >"$restore_dir/disk-state.log" 2>&1 \
        || { cat "$restore_dir/disk-state.log" >&2; fatal "$group/$iteration/$prefetch lost W-only disk state"; }
    grep -Fxq W-DISK-STATE-OK "$restore_dir/disk-state.log" \
        || fatal "$group/$iteration/$prefetch did not confirm W-only disk state"

    if [ "$prefetch" = memory ]; then
        for _ in $(seq 1 600); do
            grep -Fq 'memory prefetch completed backend=manifest' "$restore_dir/run.log" && break
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.05
        done
        grep -Fq 'memory prefetch completed backend=manifest' "$restore_dir/run.log" \
            || { tail -80 "$restore_dir/run.log" >&2; fatal "$group/$iteration memory prefetch did not complete"; }
    fi
    cache_info "$restore_dir/cache-after.json"
    stop_sandbox "$pid"
    [ -s "$restore_dir/stats.json" ] || fatal "$group/$iteration/$prefetch did not write stats-json"

    local ready_ms
    ready_ms=$(milliseconds_between "$start_ns" "$ready_ns")
    python3 - "$artifact_json" "$restore_dir/stats.json" "$restore_dir/run.log" \
        "$restore_dir/cache-before.json" "$restore_dir/cache-after.json" "$portable_info" \
        "$group" "$iteration" "$prefetch" "$publish_ms" "$ready_ms" "$first_ms" "$start_ns" "$portable_ref" <<'PY' \
        >"$restore_dir/row.json"
import datetime
import json
import re
import sys

(artifact_path, stats_path, log_path, cache_before_path, cache_after_path,
 portable_info_path, group, iteration, prefetch, publish_ms, ready_ms,
 first_ms, start_ns, portable_ref) = sys.argv[1:]
with open(artifact_path, encoding="utf-8") as source:
    row = json.load(source)
with open(stats_path, encoding="utf-8") as source:
    stats = json.load(source)
with open(log_path, encoding="utf-8") as source:
    log = source.read()
with open(cache_before_path, encoding="utf-8") as source:
    cache_before = json.load(source)
with open(cache_after_path, encoding="utf-8") as source:
    cache_after = json.load(source)
with open(portable_info_path, encoding="utf-8") as source:
    portable_info = json.load(source)

drop_caches, merge_ref = {
    "A": (True, True), "B": (False, True), "C": (True, False), "D": (False, False),
}[group]
memory_refs = portable_info.get("FromRefs") or []
expected_memory_refs = 0 if merge_ref else 1
if len(memory_refs) != expected_memory_refs:
    raise SystemExit(
        f"group {group} portable memory refs={memory_refs!r}, want {expected_memory_refs}"
    )
if any(not ref.startswith("manifest://") for ref in memory_refs):
    raise SystemExit(f"group {group} retained a non-portable memory ref: {memory_refs!r}")
if portable_ref in memory_refs:
    raise SystemExit(f"group {group} collapsed W self and its memory lower: {memory_refs!r}")

boot = portable_info["Boot"]
for index, node in enumerate([boot["Root"], *(boot.get("Disks") or [])]):
    overlay = node.get("Overlay")
    disk_refs = [overlay["Base"], *((overlay.get("BaseFromRefs") or []))] if overlay else [
        node["Base"], *((node.get("BaseFromRefs") or []))
    ]
    if any(not ref.startswith("manifest://") for ref in disk_refs):
        raise SystemExit(f"group {group} portable disk {index} refs={disk_refs!r}")
ack = re.search(r"restore notify acked in (\d+)µs", log)
if not ack:
    raise SystemExit(f"restore-to-ack log missing from {log_path}")

def origin_hits(document):
    tiered = document.get("tiered") or {}
    origin = tiered.get("origin") or {}
    return origin.get("hits")

before_hits = origin_hits(cache_before)
after_hits = origin_hits(cache_after)
origin_requests = None
if before_hits is not None and after_hits is not None:
    origin_requests = after_hits - before_hits
if origin_requests is None or origin_requests < 0:
    raise SystemExit(
        f"cache origin counter delta is invalid: before={before_hits!r} after={after_hits!r}"
    )

uffd = stats.get("uffd") or {}
required_uffd = ("faults_absent", "pages_copied", "pages_zeroed", "lazy_load_ratio")
missing_uffd = [name for name in required_uffd if uffd.get(name) is None]
if missing_uffd:
    raise SystemExit(f"stats-json missing UFFD metrics: {missing_uffd}")
disk_reads = {}
for backend in stats.get("backends") or []:
    read = backend.get("read") or {}
    disk_reads[backend["name"]] = {
        "path": backend.get("path"),
        "bytes": read.get("bytes"),
        "p50_us": (read.get("p50_ns") or 0) / 1000,
        "p99_us": (read.get("p99_ns") or 0) / 1000,
    }
disk_roles = {
    "blk0": "root.base",
    "blk1": "root.top",
    "blk2": "scratch.top",
    "blk3": "dataset.base",
    "blk4": "dataset.top",
}
if set(disk_reads) != set(disk_roles):
    raise SystemExit(
        f"stats-json disk backends={sorted(disk_reads)}, want {sorted(disk_roles)}"
    )
for name, read in disk_reads.items():
    if read["bytes"] is None:
        raise SystemExit(f"stats-json {name} missing read bytes")
    read["role"] = disk_roles[name]

prefetch_started_ms = None
prefetch_duration_ms = None
prefetch_result = "disabled"
started = re.search(r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+) .*memory prefetch started backend=manifest", log, re.M)
finished = re.search(r"memory prefetch (completed|canceled|failed) backend=manifest.* duration=(\S+)", log)

def duration_ms(raw):
    total = 0.0
    for value, unit in re.findall(r"([0-9.]+)(ns|µs|ms|s|m|h)", raw):
        total += float(value) * {"ns": 1e-6, "µs": 1e-3, "ms": 1, "s": 1e3, "m": 6e4, "h": 3.6e6}[unit]
    return total

if started:
    timestamp = datetime.datetime.strptime(started.group(1), "%Y/%m/%d %H:%M:%S.%f").astimezone()
    prefetch_started_ms = timestamp.timestamp() * 1000 - int(start_ns) / 1e6
if finished:
    prefetch_result = finished.group(1)
    prefetch_duration_ms = duration_ms(finished.group(2))

started_lines = [line for line in log.splitlines() if "memory prefetch started" in line]
self_key = portable_ref.removeprefix("manifest://")
parent_keys = [ref.removeprefix("manifest://") for ref in (portable_info.get("FromRefs") or []) if ref.startswith("manifest://")]
if prefetch == "off":
    if started_lines:
        raise SystemExit(f"prefetch=off unexpectedly started: {started_lines!r}")
elif len(started_lines) != 1 or f"key={self_key}" not in started_lines[0]:
    raise SystemExit(f"prefetch=memory starts={started_lines!r}, want W self key {self_key}")
elif any(f"key={key}" in started_lines[0] for key in parent_keys):
    raise SystemExit(f"prefetch targeted a memory parent: {started_lines[0]!r}")
elif prefetch_result != "completed":
    raise SystemExit(f"manifest prefetch result={prefetch_result!r}, want completed")

row.update({
    "group": group,
    "iteration": int(iteration),
    "drop_caches": drop_caches,
    "merge_ref": merge_ref,
    "prefetch": prefetch,
    "portable_ref": portable_ref,
    "publish_wall_ms": float(publish_ms),
    "restore_ack_ms": int(ack.group(1)) / 1000,
    "app_ready_ms": float(ready_ms),
    "first_request_ms": float(first_ms),
    "uffd_faults": uffd.get("faults_absent"),
    "uffd_pages_copied": uffd.get("pages_copied"),
    "uffd_pages_zeroed": uffd.get("pages_zeroed"),
    "lazy_load_ratio": uffd.get("lazy_load_ratio"),
    "disk_reads": disk_reads,
    "cache_origin_requests": origin_requests,
    "cache_origin_bytes": None,
    "prefetch_started_ms": prefetch_started_ms,
    "prefetch_duration_ms": prefetch_duration_ms,
    "prefetch_result": prefetch_result,
})
print(json.dumps(row, sort_keys=True))
PY

    cat "$restore_dir/row.json" >>"$SAMPLES"
    cp "$restore_dir/run.log" "$restore_dir/disk-state.log" "$restore_dir/stats.json" "$restore_dir/cache-before.json" \
        "$restore_dir/cache-after.json" "$restore_dir/row.json" "$raw_dir/"
    python3 - "$restore_dir/row.json" <<'PY'
import json, sys
row = json.load(open(sys.argv[1], encoding="utf-8"))
print(
    f"    {row['group']}/{row['prefetch']} iter={row['iteration']} "
    f"W-resident={row['w_memory_resident_bytes']/1024/1024:.1f}MiB "
    f"pause={row['snapshot_pause_ms']}ms dump={row['snapshot_dump_ms']}ms "
    f"publish={row['publish_wall_ms']:.1f}ms ready={row['app_ready_ms']:.1f}ms "
    f"first={row['first_request_ms']:.1f}ms lazy={row['lazy_load_ratio']*100:.2f}%"
)
PY
}

for group in "${MATRIX_GROUPS[@]}"; do
    read -r drop_caches merge_ref <<<"$(group_flags "$group")"
    for iteration in $(seq 1 "$ITERS"); do
        echo "==> [$group] iteration $iteration/$ITERS (drop_caches=$drop_caches merge_ref=$merge_ref)" >&2
        SAMPLE_DIR="$WORK/sample-$group-$iteration"
        mkdir -p "$SAMPLE_DIR"
        write_config "$SAMPLE_DIR/restore-b.yaml" "$SAMPLE_DIR/b-diffs" off
        reset_sample_storage
        drop_host_caches
        SID="ws-${group,,}-${iteration}-source"
        start_sandbox "$SID" "$SAMPLE_DIR/restore-b.yaml" "$SAMPLE_DIR/b-run" "$SAMPLE_DIR/b-base" \
            "$SAMPLE_DIR/b-run.log" "$SAMPLE_DIR/b-stats.json" "$B"
        SOURCE_PID="$ACTIVE_SANDBOX_PID"
        wait_http_ready "$SOURCE_PID" "$SAMPLE_DIR/b-run.log" /persist || fatal "$group/$iteration B restore did not become ready"
        GOT_WARM_SHA=$(curl --noproxy '*' --fail --silent --show-error --connect-timeout 1 --max-time 120 \
            http://169.254.1.1:8000/working-set-cache.bin | sha256sum | awk '{print $1}')
        [ "$GOT_WARM_SHA" = "$WARM_SHA" ] || fatal "$group/$iteration deterministic warm-up mismatch"
        "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$SAMPLE_DIR/b-run" -- /bin/sh -c \
            'echo ROOT-W-OK > /root-w; echo SCRATCH-W-OK > /scratch/working-set; echo DATA-W-OK > /data/working-set; sync' \
            >"$SAMPLE_DIR/writes.log" 2>&1 || { cat "$SAMPLE_DIR/writes.log" >&2; fatal "$group/$iteration W disk writes"; }

        W_OUT="$SAMPLE_DIR/w"
        mkdir -p "$W_OUT"
        if [ "$merge_ref" = false ]; then
            ln "$B_ARTIFACT" "$W_OUT/$B_BASENAME"
        fi
        SNAPSHOT_START=$(date +%s%N)
        "$BIN/sandbox-ctl" snapshot --sandbox-id "$SID" --output "$W_OUT" --run-root "$SAMPLE_DIR/b-run" \
            --drop-caches="$drop_caches" --merge-ref="$merge_ref" >"$SAMPLE_DIR/snapshot.log" 2>&1
        SNAPSHOT_END=$(date +%s%N)
        SNAPSHOT_WALL_MS=$(milliseconds_between "$SNAPSHOT_START" "$SNAPSHOT_END")
        wait "$SOURCE_PID" 2>/dev/null || true
        untrack_pid "$SOURCE_PID"
        ACTIVE_SANDBOX_PID=""
        if [ "$drop_caches" = true ]; then
            EXPECTED_DROP=succeeded
        else
            EXPECTED_DROP=skipped
        fi
        grep -Fq "quiesce: guest acked (drop_caches=$EXPECTED_DROP" "$SAMPLE_DIR/b-run.log" \
            || { tail -80 "$SAMPLE_DIR/b-run.log" >&2; fatal "$group/$iteration drop_caches result"; }
        reset_tap

        W="$W_OUT/$SID.snapshot"
        [ -f "$W" ] || fatal "$group/$iteration missing W snapshot"
        W_ARTIFACT="$(readlink -f "$W")"
        "$BIN/sandbox-ctl" info --json "$W" >"$SAMPLE_DIR/w-info.json"
        artifact_metrics "$group" "$B_DIR/info.json" "$SAMPLE_DIR/w-info.json" "$B_ARTIFACT" \
            "$W_ARTIFACT" "$W_OUT" "$SAMPLE_DIR/snapshot.log" "$SNAPSHOT_WALL_MS" "$SAMPLE_DIR/artifact.json"

        # The W disk tops already absorbed all three local B disk tops. Hide
        # those obsolete files while publishing so an opaque B memory lower
        # cannot accidentally be traversed as a root snapshot graph.
        HIDDEN_B_DISK_TOPS=()
        for path in "${B_DISK_TOPS[@]}"; do
            hidden="$path.perf-hidden-$group-$iteration"
            mv -- "$path" "$hidden"
            HIDDEN_B_DISK_TOPS+=("$hidden")
        done
        PUBLISH_START=$(date +%s%N)
        PORTABLE_REF=$("$BIN/sandbox-ctl" upload-snapshot --manifest-config "$WORK/manifest.yaml" --quiet "$W" \
            2>"$SAMPLE_DIR/publish.log")
        PUBLISH_END=$(date +%s%N)
        PUBLISH_MS=$(milliseconds_between "$PUBLISH_START" "$PUBLISH_END")
        for index in "${!B_DISK_TOPS[@]}"; do
            mv -- "${HIDDEN_B_DISK_TOPS[$index]}" "${B_DISK_TOPS[$index]}"
        done
        [[ "$PORTABLE_REF" =~ ^manifest://[0-9a-f]{64}$ ]] || { cat "$SAMPLE_DIR/publish.log" >&2; fatal "$group/$iteration invalid portable ref: $PORTABLE_REF"; }
        "$BIN/sandbox-ctl" info --json --manifest-config "$WORK/manifest.yaml" "$PORTABLE_REF" \
            >"$SAMPLE_DIR/portable-info.json"

        mkdir -p "$OUT_DIR/raw/$group/$iteration/capture"
        cp "$SAMPLE_DIR/b-run.log" "$SAMPLE_DIR/writes.log" "$SAMPLE_DIR/snapshot.log" \
            "$SAMPLE_DIR/publish.log" "$SAMPLE_DIR/w-info.json" \
            "$SAMPLE_DIR/portable-info.json" "$SAMPLE_DIR/artifact.json" \
            "$OUT_DIR/raw/$group/$iteration/capture/"

        # Portable restore must not succeed by falling back to any local W
        # memory/disk artifact. Publication and graph inspection are complete,
        # so remove the local W set before opening the manifest root.
        rm -rf -- "$W_OUT"

        portable_restore "$group" "$iteration" off "$PORTABLE_REF" "$SAMPLE_DIR/artifact.json" \
            "$PUBLISH_MS" "$SAMPLE_DIR/portable-info.json" "$SAMPLE_DIR"
        if [ "$group" = D ]; then
            portable_restore "$group" "$iteration" memory "$PORTABLE_REF" "$SAMPLE_DIR/artifact.json" \
                "$PUBLISH_MS" "$SAMPLE_DIR/portable-info.json" "$SAMPLE_DIR"
        fi
        if [ -z "${PERF_KEEP_WORK:-}" ]; then
            rm -rf -- "$SAMPLE_DIR"
        fi
    done
done

REPORT_ARGS=(--samples "$SAMPLES" --environment "$ENVIRONMENT" --output "$REPORT")
if [ "$(printf '%s\n' "${MATRIX_GROUPS[@]}" | sort -u | tr '\n' ' ' | sed 's/ $//')" != "A B C D" ]; then
    REPORT_ARGS+=(--allow-partial)
fi
python3 "$REPO_ROOT/test/perf/working_set_report.py" "${REPORT_ARGS[@]}"

echo >&2
echo "==> working-set performance matrix complete" >&2
echo "    environment: $ENVIRONMENT" >&2
echo "    raw samples: $SAMPLES" >&2
echo "    report:      $REPORT" >&2
