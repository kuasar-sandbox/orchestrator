#!/usr/bin/env bash
start_builder_store_registry() {
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs:
  root: $WORK/store
  verify_content_key: true
EOF
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 \
    || { cat "$WORK/store-init.log"; fail "store-ctl init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store-serve.log" 2>&1 &
PIDS+=($!)
wait_port 127.0.0.1 "$STORE_PORT" store-ctl
echo "==> store-ctl up (127.0.0.1:$STORE_PORT, fs backend, G1)"

# ---- zot (anonymous, insecure) + seed --------------------------------------
# Bound to 0.0.0.0: the host pushes via 127.0.0.1, the BUILD SANDBOX pulls via
# the vswitch mgmt VIP ($MGMT_VIP) — guest loopback is not the host's.
ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{
  "storage": { "rootDirectory": "$WORK/zot", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" }
}
EOF
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
PIDS+=($!)
wait_port 127.0.0.1 "$ZOT_PORT" zot
PUSH_REF="127.0.0.1:$ZOT_PORT/e2e/base:v1"
PULL_REF="$MGMT_VIP:$ZOT_PORT/e2e/base:v1"
docker tag "$E2E_IMAGE" "$PUSH_REF"; TAGS+=("$PUSH_REF")
docker push "$PUSH_REF" >"$WORK/push.log" 2>&1 || { cat "$WORK/push.log"; fail "docker push $PUSH_REF"; }
echo "==> zot up (0.0.0.0:$ZOT_PORT); seeded $E2E_IMAGE → $PUSH_REF (guest pulls $PULL_REF)"

# ---- vswitch (guest network for the build sandboxes) -----------------------
}

start_builder_files_storage() {
    : "${VGW_BIN:?VGW_BIN must point to the prepared files gateway}"
VGW_AK="e2eaccess"; VGW_SK="e2esecretkey0123"; VGW_BUCKET="build-files"
FILES_STORAGE_YAML=""
if [ -n "$VGW_BIN" ] && [ -x "$VGW_BIN" ]; then
    VGW_PORT="$(free_port)"
    mkdir -p "$WORK/vgw/$VGW_BUCKET"   # posix backend: a bucket is a top-level dir
    ROOT_ACCESS_KEY="$VGW_AK" ROOT_SECRET_KEY="$VGW_SK" \
        "$VGW_BIN" --port "127.0.0.1:$VGW_PORT" posix "$WORK/vgw" >"$WORK/vgw.log" 2>&1 &
    PIDS+=($!)
    wait_port 127.0.0.1 "$VGW_PORT" versitygw
    FILES_STORAGE_YAML=$(cat <<EOF
  files_storage:
    endpoint: http://127.0.0.1:$VGW_PORT
    region: us-east-1
    bucket: $VGW_BUCKET
    access_key: $VGW_AK
    secret_key: $VGW_SK
    force_path_style: true
EOF
)
    echo "==> versitygw up (127.0.0.1:$VGW_PORT, posix, bucket=$VGW_BUCKET) — COPY chain enabled"
else
    fail "versitygw missing; COPY chain is required by test-e2e (set VGW_BIN to the prepared gateway)"
fi

}
