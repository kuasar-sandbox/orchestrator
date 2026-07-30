#!/usr/bin/env bash
#
# e2e_cluster_real.sh -- real cluster e2e:
#
#   Phase 1 / registry-n1:
#     registry + placer + router + one real node-ctl conductor serve.
#     An explicit create drives Reserve -> node-link create -> real microVM
#     boot, then envd /health is reached by sandbox ID through router -> delete.
#
#   Phase 2 / registry-redirect:
#     three registries, node_link owner_count=1. The node first connects to the
#     bootstrap registry, gets a node-link redirect to its owner, reconnects, and
#     then runs the same real sandbox flow.
#
# This is intentionally a release-builder test: it needs artifacts from all
# repos. Missing heavy prerequisites exits 0 ("skipped") unless
# REQUIRE_CLUSTER_REAL=1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
MMDS_STATIC_E2E="${MMDS_STATIC_E2E:-0}"
MMDS_SECRET_E2E="${MMDS_SECRET_E2E:-0}"
DOMAIN="${DOMAIN:-cluster.real.local}"
SWITCH="${SWITCH:-sw0}"
E2E_IMAGE="${E2E_IMAGE:-python:3.12-slim}"
if [ -z "${ZOT_BIN:-}" ]; then
    ZOT_BIN="$(command -v zot || true)"
fi
SW_NETNS="${SW_NETNS:-e2e_cluster_sw}"

step() { echo "==> $*" >&2; }

skip() {
    echo >&2
    echo "==> e2e_cluster_real: skipping ($*)" >&2
    if [ "${REQUIRE_CLUSTER_REAL:-0}" = "1" ]; then
        echo "REQUIRE_CLUSTER_REAL=1; failing" >&2
        exit 1
    fi
    exit 0
}

fail() {
    echo "==> FAIL: $*" >&2
    if [ -n "${WORK:-}" ] && [ -d "$WORK" ]; then
        for f in "$WORK"/*.body "$WORK"/*.json "$WORK"/*.out; do
            [ -f "$f" ] || continue
            echo "---- $f ----" >&2
            sed -n '1,220p' "$f" >&2 || true
        done
        for f in "$WORK"/*.log; do
            [ -f "$f" ] || continue
            echo "---- $f ----" >&2
            sed -n '1,260p' "$f" >&2 || true
        done
        echo "---- sandbox systemd units ----" >&2
        systemctl list-units 'sandbox-builder@*.service' 'sandbox-runner@*.service' --all --no-pager >&2 2>/dev/null || true
        while IFS= read -r unit; do
            [ -n "$unit" ] || continue
            echo "---- journal $unit ----" >&2
            journalctl -u "$unit" --no-pager -n 100 2>/dev/null | sed 's/^/  unit| /' >&2 || true
        done < <(systemctl list-units 'sandbox-builder@*.service' 'sandbox-runner@*.service' --all --no-legend --no-pager 2>/dev/null | awk '{print $1}')
        if [ -d "$WORK/cr" ]; then
            while IFS= read -r sid; do
                [ -n "$sid" ] || continue
                echo "---- journal sandbox $sid ----" >&2
                journalctl KUASAR_SANDBOX_ID="$sid" --no-pager -n 100 2>/dev/null | sed 's/^/  sandbox| /' >&2 || true
            done < <(find "$WORK/cr" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null)
        fi
    fi
    exit 1
}

if [ -z "${CLUSTER_REAL_CASE:-}" ]; then
    for case_name in registry-n1 registry-redirect; do
        step "running $case_name"
        CLUSTER_REAL_CASE="$case_name" "$0"
    done
    exit 0
fi

case "$CLUSTER_REAL_CASE" in
    registry-n1|registry-redirect) ;;
    *) fail "unknown CLUSTER_REAL_CASE=$CLUSTER_REAL_CASE" ;;
esac

for b in node-ctl sandbox-ctl flatten-ctl store-ctl e2b-key-ctl connector-ctl cluster-ctl cloud-hypervisor; do
    [ -x "$BIN/$b" ] || skip "missing $BIN/$b"
done
[ -f "$BIN/vmlinux" ] || skip "missing $BIN/vmlinux"
[ -f "$BIN/sandbox-runtime.bundle" ] || skip "missing $BIN/sandbox-runtime.bundle"
command -v python3 >/dev/null 2>&1 || skip "python3 not on PATH"
command -v curl >/dev/null 2>&1 || skip "curl not on PATH"
command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || skip "docker not usable"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || skip "zot not found (set ZOT_BIN or install zot on PATH)"
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || skip "mkfs.erofs not found"
command -v ip >/dev/null 2>&1 || skip "iproute2 (ip) not found"
[ -d /run/systemd/system ] || skip "systemd not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || skip "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || docker pull "$E2E_IMAGE" >/dev/null 2>&1 \
    || skip "base image $E2E_IMAGE unavailable (set E2E_IMAGE to a local or pullable image)"

if [ "$(id -u)" -ne 0 ]; then
    exec sudo -nE "$0" "$@"
fi
if ! command -v mkfs.erofs >/dev/null 2>&1; then
    export PATH="$BIN:$PATH"
fi

WORK="$(mktemp -d /tmp/e2e-cr-XXXXXX)"
UNIT_DIR="/run/systemd/system"
UNIT_NAMES=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=()
for u in "${UNIT_NAMES[@]}"; do
    [ -e "$UNIT_DIR/$u" ] && skip "$UNIT_DIR/$u exists; refusing to clobber"
    OURS+=("$UNIT_DIR/$u")
done
mkdir -p "$WORK/r" "$WORK/l" "$WORK/s" "$WORK/z/d" "$WORK/g" "$WORK/br" "$WORK/bl" "$WORK/cr" "$WORK/cl"
declare -a PIDS=()
declare -a TAGS=()
SW_STARTED=""

cleanup() {
    set +e
    rm -f "$WORK/create.credentials"
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        p="${PIDS[$i]}"
        [ -n "$p" ] && kill "$p" 2>/dev/null
        [ -n "$p" ] && wait "$p" 2>/dev/null
    done
    [ -n "$SW_STARTED" ] && "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
    ip netns del "$SWITCH" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done
    systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    [ -n "${E2E_KEEP:-}" ] && echo "kept work dir: $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

free_port() {
    python3 <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_port() {
    local port="$1" name="$2"
    for _ in $(seq 1 120); do
        if python3 - "$port" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.socket()
s.settimeout(0.2)
s.connect(("127.0.0.1", int(sys.argv[1])))
s.close()
PY
        then
            return 0
        fi
        sleep 0.25
    done
    fail "$name did not open port $port"
}

wait_api_health() {
    local port="$1" name="$2"
    for _ in $(seq 1 120); do
        if curl -fsS --noproxy '*' --max-time 1 -H "Host: api.$DOMAIN" "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    fail "$name did not become healthy"
}

http_code() {
    local out="$1"; shift
    curl -sS --noproxy '*' --max-time 260 -o "$out" -w '%{http_code}' "$@"
}

node_req() {
    local port="$1" method="$2" path="$3" key="$4" body="${5:-}"
    local args=(-sS --noproxy '*' --max-time 260 -o "$WORK/node-resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$port$path"
}
json_field() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

assert_no_default_exec_token() {
    python3 - "$1" <<'PY'
import json, sys
created = json.load(open(sys.argv[1]))
if "execAccessToken" in created:
    raise SystemExit("create response unexpectedly contains execAccessToken")
PY
}

issue_cluster_exec_session() {
    local sid="$1" code
    code="$(curl -sS --noproxy '*' --max-time 30 \
        -D "$WORK/exec-session.headers" \
        -o "$WORK/exec-session.secret" \
        -w '%{http_code}' \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-Kuasar-Route-Key: $ROUTE_KEY" \
        -H "X-API-KEY: $CLUSTER_API_KEY" \
        -H 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:$ROUTER_PORT/sandboxes/$sid/exec-sessions")"
    [ "$code" = "201" ] || fail "exec-session returned $code (want 201)"
    python3 - "$WORK/exec-session.headers" "$WORK/exec-session.secret" <<'PY'
import json, sys
headers = [line.strip().lower() for line in open(sys.argv[1], "rb").read().splitlines()]
if b"cache-control: no-store" not in headers:
    raise SystemExit("exec-session response omitted Cache-Control: no-store")
payload = json.load(open(sys.argv[2]))
if not isinstance(payload, dict) or set(payload) != {"execAccessToken"}:
    raise SystemExit("exec-session response must contain only execAccessToken")
token = payload["execAccessToken"]
if not isinstance(token, str) or not token.startswith("kat1.") or len(token.split(".")) != 3:
    raise SystemExit("exec-session response contains an invalid KAT token")
print(token)
PY
}

exec_through_cluster_connect() {
    local sid="$1" token="$2" marker="$3"
    local retries="${4:-1}"
    local input="$WORK/native-exec.stdin"
    local output="$WORK/native-exec.stdout"
    local error_output="$WORK/native-exec.stderr"
    local diagnostics="$WORK/native-exec.client.log"
    local attempt status

    printf 'stdin:%s\n' "$marker" >"$input"
    for attempt in $(seq 1 "$retries"); do
        : >"$output"; : >"$error_output"; : >"$diagnostics"
        if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
            --proxy "http://127.0.0.1:$ROUTER_PORT" \
            --proxy-header "E2b-Sandbox-Id: $sid" \
            --proxy-header "E2b-Sandbox-Service: exec" \
            --proxy-header "X-Access-Token: $token" \
            --proxy-header "X-Kuasar-Sandbox-Group: $GROUP" \
            --proxy-header "X-Kuasar-Route-Key: $ROUTE_KEY" \
            --proxy-header "X-Kuasar-E2E-Duplicate: first" \
            --proxy-header "X-Kuasar-E2E-Duplicate: second" \
            --stdin-from "$input" --stdout-to "$output" --stderr-to "$error_output" -- \
            /bin/sh -c "IFS= read -r value; printf 'stdout:%s:%s\\n' '$marker' \"\$value\"; printf 'stderr:%s\\n' '$marker' >&2; exit 47" \
            >"$diagnostics" 2>&1; then
            status=0
        else
            status=$?
        fi
        if [ "$status" = "47" ] && \
            grep -Fxq "stdout:$marker:stdin:$marker" "$output" 2>/dev/null && \
            grep -Fxq "stderr:$marker" "$error_output" 2>/dev/null; then
            return 0
        fi
        [ "$attempt" = "$retries" ] || sleep 0.5
    done
    sed 's/^/  client| /' "$diagnostics" >&2
    sed 's/^/  stdout| /' "$output" 2>/dev/null >&2
    sed 's/^/  stderr| /' "$error_output" 2>/dev/null >&2
    fail "native exec did not complete after $retries attempt(s), last exit=$status"
}

mmds_static_through_cluster_connect() {
    local sid="$1" token="$2"
    local output="$WORK/mmds-static-guest-cluster.out"
    local error_output="$WORK/mmds-static-guest-cluster.err"
    local diagnostics="$WORK/mmds-static-guest-cluster.client.log"
    local guest_command status=0

    guest_command="$(mmds_static_guest_command)"
    timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$ROUTER_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --proxy-header "X-Kuasar-Sandbox-Group: $GROUP" \
        --proxy-header "X-Kuasar-Route-Key: $ROUTE_KEY" \
        --stdout-to "$output" --stderr-to "$error_output" -- \
        /bin/sh -c "$guest_command" >"$diagnostics" 2>&1 || status=$?
    if [ "$status" != "0" ] || ! grep -q 'MMDS_STATIC_GUEST_GET_OK' "$output" 2>/dev/null; then
        sed 's/^/  client| /' "$diagnostics" >&2
        sed 's/^/  stdout| /' "$output" 2>/dev/null >&2
        sed 's/^/  stderr| /' "$error_output" 2>/dev/null >&2
        fail "cluster MMDS guest GET failed, exec status=$status"
    fi
}

mmds_secret_through_cluster_connect() {
    local sid="$1" token="$2" expected_status="$3" expected_body="$4"
    local expected_content_type="$5" marker="$6"
    local output="$WORK/mmds-secret-guest-cluster.out"
    local error_output="$WORK/mmds-secret-guest-cluster.err"
    local diagnostics="$WORK/mmds-secret-guest-cluster.client.log"
    local guest_command attempt status

    guest_command="$(mmds_secret_guest_command "$expected_status" "$expected_body" \
        "$expected_content_type" "$marker")"
    for attempt in $(seq 1 20); do
        status=0
        : >"$output"; : >"$error_output"; : >"$diagnostics"
        timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
            --proxy "http://127.0.0.1:$ROUTER_PORT" \
            --proxy-header "E2b-Sandbox-Id: $sid" \
            --proxy-header "E2b-Sandbox-Service: exec" \
            --proxy-header "X-Access-Token: $token" \
            --proxy-header "X-Kuasar-Sandbox-Group: $GROUP" \
            --proxy-header "X-Kuasar-Route-Key: $ROUTE_KEY" \
            --stdout-to "$output" --stderr-to "$error_output" -- \
            /bin/sh -c "$guest_command" >"$diagnostics" 2>&1 || status=$?
        if [ "$status" = "0" ] && grep -q "$marker" "$output" 2>/dev/null; then
            sed 's/^/  mmds-secret-guest| /' "$output" >&2
            return 0
        fi
        [ "$attempt" = "20" ] || sleep 0.25
    done
    sed 's/^/  client| /' "$diagnostics" >&2
    sed 's/^/  stdout| /' "$output" 2>/dev/null >&2
    sed 's/^/  stderr| /' "$error_output" 2>/dev/null >&2
    return 1
}

run_cluster_mmds_secret_e2e() {
    local sid="$1" token="$2"
    local response="$WORK/mmds-secret-admin-cluster.json" code node_sid
    local initial="MMDS_SECRET_CLUSTER_INITIAL_$RANDOM"
    local updated="MMDS_SECRET_CLUSTER_UPDATED_$RANDOM"

    # Router APIs use the stable sandbox ID; the node-local admin socket owns a
    # concrete generation (stable-gN), which is also the node store key.
    node_sid="$(find "$WORK/cr" -mindepth 1 -maxdepth 1 -type d -name "$sid-g*" \
        -printf '%f\n' | head -1)"
    [ -n "$node_sid" ] || fail "node-local generation for stable sandbox $sid not found"

    code="$(mmds_secret_admin_request "$WORK/cn.sock" PUT "$node_sid" "$MMDS_SECRET_NAME" \
        "$initial" "text/plain" "$response")"
    [ "$code" = "204" ] && mmds_secret_assert_revision "$response" 1 \
        || fail "cluster MMDS secret initial admin PUT returned $code"
    mmds_secret_assert_not_plaintext_at_rest "$WORK/cl" "$initial" \
        || fail "cluster MMDS secret initial value persisted in plaintext"
    mmds_secret_through_cluster_connect "$sid" "$token" 200 "$initial" "text/plain" \
        "MMDS_SECRET_CLUSTER_INITIAL_OK" \
        || fail "cluster MMDS secret initial guest GET"

    code="$(mmds_secret_admin_request "$WORK/cn.sock" PUT "$node_sid" "$MMDS_SECRET_NAME" \
        "$updated" "application/x-kuasar-e2e-secret" "$response")"
    [ "$code" = "204" ] && mmds_secret_assert_revision "$response" 2 \
        || fail "cluster MMDS secret update admin PUT returned $code"
    mmds_secret_assert_not_plaintext_at_rest "$WORK/cl" "$updated" \
        || fail "cluster MMDS secret updated value persisted in plaintext"
    mmds_secret_through_cluster_connect "$sid" "$token" 200 "$updated" \
        "application/x-kuasar-e2e-secret" "MMDS_SECRET_CLUSTER_UPDATED_OK" \
        || fail "cluster MMDS secret updated guest GET"

    code="$(mmds_secret_admin_request "$WORK/cn.sock" DELETE "$node_sid" "$MMDS_SECRET_NAME" \
        "" "" "$response")"
    [ "$code" = "204" ] && mmds_secret_assert_revision "$response" 3 \
        || fail "cluster MMDS secret DELETE returned $code"
    mmds_secret_through_cluster_connect "$sid" "$token" 404 "" "" \
        "MMDS_SECRET_CLUSTER_REVOKED_OK" \
        || fail "cluster MMDS secret revoked guest GET"

    code="$(mmds_secret_admin_request "$WORK/cn.sock" PUT "$node_sid" undeclared \
        "$initial" "text/plain" "$response")"
    [ "$code" = "400" ] || fail "cluster undeclared MMDS secret PUT returned $code"
}

router_req() {
    local method="$1" path="$2" key="$3" route_key="${4:-}" body="${5:-}"
    local args=(-sS --noproxy '*' --max-time 260 -o "$WORK/router-resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" -H "X-API-KEY: $key")
    [ -n "$route_key" ] && args+=(-H "X-Kuasar-Route-Key: $route_key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$ROUTER_PORT$path"
}

create_sandbox() {
    local out="$1"
    local mmds_args=()
    if [ -n "${REQ_MMDS_HEADER:-}" ]; then
        mmds_args=(-H "X-Kuasar-Sandbox-MMDS: ${REQ_MMDS_HEADER}")
    fi
    http_code "$out" \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-Kuasar-Route-Key: $ROUTE_KEY" \
        -H "X-API-KEY: $CLUSTER_API_KEY" \
        "${mmds_args[@]}" \
        -H 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:$ROUTER_PORT/sandboxes"
}

retry_create_sandbox() {
    local out="$1" code="000"
    for attempt in $(seq 1 12); do
        code="$(create_sandbox "$out" || true)"
        if [ "$code" = "201" ]; then
            echo "$code"
            return 0
        fi
        if [ "$code" != "503" ] || [ "$attempt" = "12" ]; then
            echo "$code"
            return 1
        fi
        step "sandbox create attempt $attempt returned 503; waiting for placement convergence"
        sleep 2
    done
}

sandbox_route() {
    python3 - "$1" "$ROUTE_KEY" <<'PY'
import json, sys
path, expected_route_key = sys.argv[1:]
created = json.load(open(path))
if created.get("routeKey") != expected_route_key:
    raise SystemExit("create response routeKey mismatch")
fields = [created.get(name) for name in (
    "sandboxID", "envdAccessToken", "trafficAccessToken", "forwardAccessToken"
)]
if not all(isinstance(value, str) and value for value in fields):
    raise SystemExit("create response omitted e2b sandbox credentials")
print("\t".join(fields[:2]))
PY
}

data_by_sid_code() {
    local out="$1" sid="$2" envd_token="$3"
    http_code "$out" \
        -H "Host: 49983-$sid.$DOMAIN" \
        -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-Kuasar-Route-Key: $ROUTE_KEY" \
        -H "X-Access-Token: $envd_token" \
        "http://127.0.0.1:$ROUTER_PORT/health"
}

retry_data_by_sid() {
    local sid="$1" envd_token="$2"
    local code="000"
    for i in $(seq 1 12); do
        step "data-plane attempt $i: router -> route resolve -> node -> real envd"
        code="$(data_by_sid_code "$WORK/data-health.body" "$sid" "$envd_token" 2>/dev/null || echo 000)"
        if [ "$code" = "204" ] || [ "$code" = "200" ]; then
            echo "$code"
            return 0
        fi
        if grep -qiE 'credential pair (is )?not (installed|distributed)' "$WORK/data-health.body" 2>/dev/null; then
			cat "$WORK/data-health.body" >&2
			fail "data-plane hit node before credential-pair cache was ready"
        fi
        step "data-plane attempt $i returned $code; retrying while sandbox boot converges"
        sleep 2
    done
    echo "$code"
    return 1
}

start_store_zot_vswitch() {
    STORE_PORT="$(free_port)"
    cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/s, verify_content_key: true }
EOF
    "$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; fail "store init"; }
    "$BIN/store-ctl" serve --config "$WORK/store.yaml" > >(tee "$WORK/store-serve.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$STORE_PORT" store-ctl

    ZOT_PORT="$(free_port)"
    cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/z/d", "dedupe": false, "gc": false },
  "http": { "address": "0.0.0.0", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
    "$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.stdout" 2>&1 &
    PIDS+=("$!")
    wait_port "$ZOT_PORT" zot

    REF="127.0.0.1:$ZOT_PORT/e2e/cluster-real:v1"
    cat > "$WORK/niceshim" <<'SH'
#!/bin/sh
while [ $# -gt 0 ]; do
  case "$1" in
    -c|-n) shift 2 ;;
    -c*|-n*) shift ;;
    --) shift; break ;;
    *) break ;;
  esac
done
exec "$@"
SH
    cat > "$WORK/Dockerfile.e2e" <<EOF
FROM $E2E_IMAGE
COPY niceshim /usr/bin/ionice
COPY niceshim /usr/bin/nice
RUN chmod +x /usr/bin/ionice /usr/bin/nice \
 && if ! id -u user >/dev/null 2>&1; then \
      if command -v useradd >/dev/null 2>&1; then useradd -m -d /home/user -s /bin/sh user; \
      elif command -v adduser >/dev/null 2>&1; then adduser -D -h /home/user -s /bin/sh user; \
      else echo "missing useradd/adduser" >&2; exit 1; fi; \
    fi \
 && mkdir -p /home/user \
 && chown user:user /home/user \
 && id user >/dev/null
EOF
    docker build --network=none -t "$REF" -f "$WORK/Dockerfile.e2e" "$WORK" > >(tee "$WORK/imgbuild.log" >&2) 2>&1 || fail "docker build e2e image"
    TAGS+=("$REF")
    docker push "$REF" > >(tee "$WORK/push.log" >&2) 2>&1 || fail "docker push e2e image"
    step "store-ctl + zot up; seeded $REF"

    MGMT_VIP="169.254.169.254"
    MMDS_PORT="$(free_port)"
    local mgmt_service_args=()
    if [ "$MMDS_STATIC_E2E" = "1" ]; then
        mgmt_service_args=(--mgmt-service="$MGMT_VIP:80:$MGMT_VIP:$MMDS_PORT")
    fi
    "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true
    ip netns del "$SW_NETNS" 2>/dev/null || true
    ip netns del "$SWITCH" 2>/dev/null || true
    ip netns add "$SW_NETNS" 2>/dev/null || true
    step "starting vswitch $SWITCH (netns=$SW_NETNS)"
    "$BIN/connector-ctl" vswitch start "$SWITCH" \
        --netns="$SW_NETNS" \
        --ports=64 \
        --mac-addr=02:00:00:00:00:01 \
        --floating-ip-base=100.100.96.0 \
        --mode=tap \
        --mgmt-extract=:${SWITCH}m0:$MGMT_VIP,0.0.0.0/0 \
        "${mgmt_service_args[@]}" > >(tee "$WORK/vswitch-start.log" >&2) 2>&1 || fail "vswitch start"
    SW_STARTED=1
    GUEST_REF="$MGMT_VIP:$ZOT_PORT/e2e/cluster-real:v1"
    step "vswitch up; build sandboxes pull $GUEST_REF"

    cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF
}

make_ext4_templates() {
    MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"
    [ -x "$MKFS_EXT4" ] || skip "mkfs.ext4 not found"
    OVL="$WORK/overlay-1G.ext4"
    truncate -s 1G "$OVL"
    "$MKFS_EXT4" -F -q -b 4096 "$OVL" >"$WORK/mkfs-overlay.log" 2>&1 || { cat "$WORK/mkfs-overlay.log"; fail "mkfs overlay"; }
    BLD="$WORK/builder-2G.ext4"
    truncate -s 2G "$BLD"
    "$MKFS_EXT4" -F -q -b 4096 "$BLD" >"$WORK/mkfs-builder.log" 2>&1 || { cat "$WORK/mkfs-builder.log"; fail "mkfs builder"; }
}

build_template_with_standalone_node() {
    BUILD_PORT="$(free_port)"
    cat > "$WORK/build-node.yaml" <<EOF
api: { domain: $DOMAIN, listen: "127.0.0.1:$BUILD_PORT" }
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/br, base_root: $WORK/bl, config_socket: $WORK/bn.sock }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
  vcpu: 1
  memory: 1GiB
checkpoint: { mode: remote }
EOF
    step "starting temporary standalone node-ctl for template build (:${BUILD_PORT})"
    "$BIN/node-ctl" conductor serve --config "$WORK/build-node.yaml" > >(tee "$WORK/build-node.log" >&2) 2>&1 &
    local build_pid="$!"
    PIDS+=("$build_pid")
    wait_api_health "$BUILD_PORT" "temporary node-ctl"
    "$BIN/node-ctl" manifest-key add --socket "$WORK/bn.sock" "$MANIFEST_KEY" >/dev/null || fail "temporary manifest-key add"

    local code tid bid status
    code="$(node_req "$BUILD_PORT" POST /v3/templates "$BUILD_API_KEY" '{"name":"cluster-real-tmpl"}')"
    [ "$code" = "202" ] || { cat "$WORK/node-resp.body"; fail "template register returned $code"; }
    tid="$(json_field "$WORK/node-resp.body" templateID)"
    bid="$(json_field "$WORK/node-resp.body" buildID)"
    step "building template through temporary node: templateID=$tid buildID=$bid"
    code="$(node_req "$BUILD_PORT" POST "/v2/templates/$tid/builds/$bid" "$BUILD_API_KEY" "{\"fromImage\":\"$GUEST_REF\"}")"
    [ "$code" = "202" ] || { cat "$WORK/node-resp.body"; fail "template build trigger returned $code"; }
    TEMPLATE_REF=""
    for _ in $(seq 1 180); do
        node_req "$BUILD_PORT" GET "/templates/$tid/builds/$bid/status" "$BUILD_API_KEY" >/dev/null
        status="$(json_field "$WORK/node-resp.body" status)"
        case "$status" in
            ready)
                TEMPLATE_REF="$(json_field "$WORK/node-resp.body" templateID)"
                break
                ;;
            error)
                cat "$WORK/node-resp.body"
                fail "template build error"
                ;;
        esac
        sleep 1
    done
    [ -n "$TEMPLATE_REF" ] || fail "template build did not become ready"
    step "built reusable template: $TEMPLATE_REF"

    kill "$build_pid" 2>/dev/null || true
    wait "$build_pid" 2>/dev/null || true
    systemctl stop 'sandbox-builder@*.service' 2>/dev/null || true
}

write_group_record() {
    cat > "$WORK/g/group.json" <<EOF
{
  "group": "$GROUP",
  "manifest_key": { "type": "inline", "value": "$MANIFEST_KEY" },
  "api_secret": { "type": "inline", "value": "$API_SECRET" },
  "template_ref": "$TEMPLATE_REF",
  "target_port": 49983,
  "node_selectors": [{ "pool": "real" }]
}
EOF
    step "wrote placer file group source for $GROUP"
}

write_registry_configs() {
    CONTROL_PORTS=()
    local registries="$1"
    for _ in $(seq 1 "$registries"); do
        CONTROL_PORTS+=("$(free_port)")
    done
    CONTROL_PORT="${CONTROL_PORTS[0]}"
    ACTIVE_MEMBERS_YAML=""
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        ACTIVE_MEMBERS_YAML="$ACTIVE_MEMBERS_YAML        - { id: registry-$i, advertise: \"http://127.0.0.1:$port\", node_advertise: \"127.0.0.1:$port\" }
"
    done
    ROUTE_OWNER_COUNT="$registries"
    NODE_OWNER_COUNT="$registries"
    PLACER_OWNER_COUNT="$registries"
    NODE_LIST_OWNER_COUNT="$registries"
    if [ "$CLUSTER_REAL_CASE" = "registry-redirect" ]; then
        NODE_OWNER_COUNT=1
    fi
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        cat >"$WORK/registry-$i.yaml" <<EOF
member:
  id: registry-$i
  listen: "127.0.0.1:$port"
membership:
  active: 1
  versions:
    - version: 1
      members:
$ACTIVE_MEMBERS_YAML
  owners:
    route_link: $ROUTE_OWNER_COUNT
    node_link: $NODE_OWNER_COUNT
    placer_link: $PLACER_OWNER_COUNT
    node_list: $NODE_LIST_OWNER_COUNT
node_link:
  heartbeat_interval: "500ms"
  node_dead_after: "5s"
route_link:
  park_timeout: "180s"
placer_link:
  min_ready_placers: 1
  place_timeout: "5s"
EOF
    done
}

write_placer_router_configs() {
    PLACER_PORT="$(free_port)"
    ROUTER_PORT="$(free_port)"
    cat > "$WORK/placer.yaml" <<EOF
placer:
  id: placer-1
  listen: "127.0.0.1:$PLACER_PORT"
  advertise: "http://127.0.0.1:$PLACER_PORT"
  memberlist_label: "placer.default"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
import_groups:
  - source_id: cluster-real-file-source
    source_type: file
    path: "$WORK/g"
placement:
  candidates: 2
  zone_admit_max: "yellow"
  node_dead_after: "5s"
  import_source_lease_ttl: "10s"
  selector_patch_refresh_interval: "1s"
EOF
    cat > "$WORK/router.yaml" <<EOF
domain: "$DOMAIN"
registry:
  bootstrap: "127.0.0.1:$CONTROL_PORT"
ingress:
  listen: "127.0.0.1:$ROUTER_PORT"
auth:
  data_plane: "enforce"
  cache_ttl: "500ms"
cache:
  route_ttl: "5m"
  idle_timeout: "2m"
EOF
}

start_cluster_control_plane() {
    local registries="${#CONTROL_PORTS[@]}"
    for i in $(seq 1 "$registries"); do
        local port="${CONTROL_PORTS[$((i-1))]}"
        step "starting registry-$i (:${port})"
        "$BIN/cluster-ctl" registry --config "$WORK/registry-$i.yaml" > >(tee "$WORK/registry-$i.log" >&2) 2>&1 &
        PIDS+=("$!")
        wait_port "$port" "registry-$i"
    done
    step "checking registry membership endpoint"
    python3 - "${CONTROL_PORTS[0]}" "$registries" <<'PY' || fail "membership endpoint failed"
import json, sys, urllib.request
port, want = sys.argv[1], int(sys.argv[2])
m = json.load(urllib.request.urlopen("http://127.0.0.1:%s/cluster/membership" % port, timeout=2))
active = m.get("active", m.get("Active"))
versions = m.get("versions", m.get("Versions", []))
assert active, m
active_versions = [v for v in versions if v.get("version", v.get("Version")) == active]
assert len(active_versions) == 1, m
members = active_versions[0].get("members", active_versions[0].get("Members", []))
assert len(members) == want, m
assert active_versions[0].get("label", active_versions[0].get("Label")), m
PY

    step "starting placer (:${PLACER_PORT})"
    "$BIN/cluster-ctl" placer --config "$WORK/placer.yaml" > >(tee "$WORK/placer.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$PLACER_PORT" placer

    step "starting router (:${ROUTER_PORT})"
    "$BIN/cluster-ctl" router --config "$WORK/router.yaml" > >(tee "$WORK/router.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_port "$ROUTER_PORT" router
}

choose_redirect_node_id() {
    python3 - "http://127.0.0.1:$CONTROL_PORT/node-link/session" <<'PY'
import json, struct, sys, urllib.request
url = sys.argv[1]
for idx in range(1, 80):
    node_id = "real-node-redirect-%02d" % idx
    msg = {
        "type": "node_register",
        "node_register": {
            "node_id": node_id,
            "labels": {"pool": "probe"},
            "data_endpoint": "127.0.0.1:1",
            "capacity": 1,
            "accept_redirect": True,
        },
    }
    payload = json.dumps(msg, separators=(",", ":")).encode()
    body = struct.pack("<I", len(payload)) + payload
    req = urllib.request.Request(url, data=body, method="PUT", headers={"Content-Type": "application/octet-stream"})
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            hdr = resp.read(4)
            if len(hdr) != 4:
                continue
            n = struct.unpack("<I", hdr)[0]
            raw = resp.read(n)
            hello = json.loads(raw.decode())
    except Exception:
        continue
    redir = (((hello.get("hello") or {}).get("redirect") or {}).get("targets") or [])
    if redir:
        print(node_id)
        raise SystemExit(0)
raise SystemExit(1)
PY
}

start_cluster_node() {
    NODE_PORT="$(free_port)"
    local node_id="$1"
    local mmds_config=""
    if [ "$MMDS_STATIC_E2E" = "1" ]; then
        mmds_config="proxy: { mode: internal, auth: enforce }
mmds:
  enabled: true
  listen: \"0.0.0.0:$MMDS_PORT\"
  routes: { enabled: true }"
    fi
    cat > "$WORK/cluster-node.yaml" <<EOF
api: { domain: $DOMAIN, listen: "127.0.0.1:$NODE_PORT" }
$mmds_config
encryption_key: "$ENC_KEY"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/cr, base_root: $WORK/cl, config_socket: $WORK/cn.sock }
units: { dir: $UNIT_DIR }
sandbox:
  timeout_sec: 120
  network: { switch: $SWITCH }
  boot: { kernel: $BIN/vmlinux, runtime: $BIN/sandbox-runtime.bundle, overlay_diff_template: $OVL }
builder:
  insecure_registry: true
  diff_template: $BLD
  vcpu: 1
  memory: 1GiB
checkpoint: { mode: remote }
cluster:
  node_link: { endpoint: "127.0.0.1:$CONTROL_PORT" }
  node_id: "$node_id"
  data_endpoint: "127.0.0.1:$NODE_PORT"
  heartbeat_interval: "500ms"
  labels: { pool: "real" }
EOF
    step "starting cluster node-ctl node_id=$node_id (:${NODE_PORT}, no manual manifest-key add)"
    "$BIN/node-ctl" conductor serve --config "$WORK/cluster-node.yaml" > >(tee "$WORK/cluster-node.log" >&2) 2>&1 &
    PIDS+=("$!")
    wait_api_health "$NODE_PORT" "cluster node-ctl"
    if [ "$CLUSTER_REAL_CASE" = "registry-redirect" ]; then
        for _ in $(seq 1 80); do
            if grep -q "redirecting to node owner" "$WORK/cluster-node.log"; then
                step "observed node-link redirect to node owner"
                return 0
            fi
            sleep 0.25
        done
        fail "node-link redirect was not observed in cluster-node.log"
    fi
}

wait_cluster_node_key_pair() {
    step "waiting for node_link credential-pair cache on $NODE_ID"
    for _ in $(seq 1 120); do
        if "$BIN/node-ctl" manifest-key list --socket "$WORK/cn.sock" >"$WORK/cluster-node-keys.out" 2>&1; then
            if grep -q "^api=$API_SECRET_FP[[:space:]]" "$WORK/cluster-node-keys.out"; then
                step "node credential-pair cache ready: $API_SECRET_FP"
                return 0
            fi
        fi
        sleep 0.5
    done
    cat "$WORK/cluster-node-keys.out" >&2 || true
    fail "node credential-pair cache did not receive $API_SECRET_FP"
}

run_cluster_flow() {
    local code sid envd_token create_response="$WORK/create.credentials"
    step "creating sandbox explicitly through router"
    code="$(retry_create_sandbox "$create_response" || true)"
    if [ "$code" != "201" ]; then
        [ -s "$create_response" ] && { step "sandbox create response:"; sed 's/^/  create| /' "$create_response" >&2; }
        fail "sandbox create returned $code"
    fi
    assert_no_default_exec_token "$create_response" || {
        rm -f "$create_response"
        fail "create response exposed a default exec token"
    }
    if ! IFS=$'\t' read -r sid envd_token < <(sandbox_route "$create_response"); then
        rm -f "$create_response"
        fail "sandbox create returned an invalid e2b response"
    fi
    rm -f "$create_response"
    step "created sandbox: $sid"

    step "issuing explicit exec capability through router (stable SID + group + route-key)"
    local exec_token native_mark
    exec_token="$(issue_cluster_exec_session "$sid")" || fail "issue cluster exec capability"
    rm -f "$WORK/exec-session.secret"
    native_mark="CLUSTER_NATIVE_EXEC_$RANDOM"
    exec_through_cluster_connect "$sid" "$exec_token" "$native_mark"
    if [ "$MMDS_STATIC_E2E" = "1" ]; then
        # Scenario: static route.
        source "$REPO_ROOT/test/e2e/lib/mmds_static_guest.sh"
        mmds_static_through_cluster_connect "$sid" "$exec_token"
        step "PASS: cluster real guest GET reached declared MMDS static route"
    fi
    if [ "$MMDS_SECRET_E2E" = "1" ]; then
        # Scenario: secret backend.
        source "$REPO_ROOT/test/e2e/lib/mmds_secret_guest.sh"
        run_cluster_mmds_secret_e2e "$sid" "$exec_token"
        step "PASS: cluster MMDS secret install/update/revoke lifecycle"
    fi
    step "pausing sandbox, then reusing the same KAT to wake the current node-local generation"
    code="$(router_req POST "/sandboxes/$sid/pause" "$CLUSTER_API_KEY" "$ROUTE_KEY")"
    [ "$code" = "204" ] || { cat "$WORK/router-resp.body"; fail "pause returned $code"; }
    local resume_mark="CLUSTER_NATIVE_EXEC_RESUME_$RANDOM"
    exec_through_cluster_connect "$sid" "$exec_token" "$resume_mark" 40
    unset exec_token
    step "PASS: real sandbox-ctl used stable SID through exec CONNECT, then the same KAT resumed the paused sandbox"

    step "checking SID-addressed envd data request with X-Access-Token"
    code="$(retry_data_by_sid "$sid" "$envd_token" || true)"
    unset envd_token
    [ "$code" = "204" ] || [ "$code" = "200" ] || fail "data-plane /health returned $code"
    step "PASS: explicit create -> SID route -> real envd /health ($code)"

    step "checking group-local sandbox list through router"
    code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
    [ "$code" = "200" ] || { cat "$WORK/router-resp.body"; fail "list returned $code"; }
    python3 - "$WORK/router-resp.body" "$sid" <<'PY' || fail "sandbox list did not contain created sandbox $sid"
import json, sys
rows = json.load(open(sys.argv[1]))
expected = sys.argv[2]
raise SystemExit(0 if any(row.get("sandboxID") == expected for row in rows) else 1)
PY
    step "listed sandbox: $sid"

    step "checking router control DELETE forwards to the real node"
    code="$(router_req DELETE "/sandboxes/$sid" "$CLUSTER_API_KEY" "$ROUTE_KEY")"
    [ "$code" = "204" ] || { cat "$WORK/router-resp.body"; fail "delete returned $code"; }
    for _ in $(seq 1 80); do
        code="$(router_req GET /v2/sandboxes "$CLUSTER_API_KEY")"
        [ "$code" = "200" ] || { sleep 0.25; continue; }
        if python3 - "$WORK/router-resp.body" "$sid" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))
sid = sys.argv[2]
raise SystemExit(0 if all(r.get("sandboxID") != sid for r in rows) else 1)
PY
        then
            step "PASS: sandbox delete converged through route_link"
            return 0
        fi
        sleep 0.25
    done
    fail "deleted sandbox still appears in list"
}

step "cluster real e2e case=$CLUSTER_REAL_CASE work=$WORK using BIN=$BIN"
MANIFEST_KEY="$("$BIN/e2b-key-ctl" gen-key)"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MANIFEST_KEY")"
API_SECRET_FP="$("$BIN/e2b-key-ctl" fingerprint "$API_SECRET")"
BUILD_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
CLUSTER_API_KEY="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ENC_KEY="$("$BIN/e2b-key-ctl" gen-key)"
GROUP="/e2e/cluster/real/$CLUSTER_REAL_CASE"
ROUTE_KEY="user1/$CLUSTER_REAL_CASE"

start_store_zot_vswitch
make_ext4_templates
build_template_with_standalone_node
write_group_record

if [ "$CLUSTER_REAL_CASE" = "registry-n1" ]; then
    write_registry_configs 1
else
    write_registry_configs 3
fi
write_placer_router_configs
start_cluster_control_plane

NODE_ID="real-node-1"
if [ "$CLUSTER_REAL_CASE" = "registry-redirect" ]; then
    step "probing node ids until bootstrap registry returns a node-link redirect"
    NODE_ID="$(choose_redirect_node_id)" || fail "could not find a node_id redirected away from bootstrap registry"
    step "selected redirected node_id=$NODE_ID"
fi
start_cluster_node "$NODE_ID"
wait_cluster_node_key_pair
run_cluster_flow

echo "==> PASS: e2e_cluster_real $CLUSTER_REAL_CASE"
