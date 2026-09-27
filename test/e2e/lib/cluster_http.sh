#!/usr/bin/env bash
# Process, configuration and request primitives for real cluster cases.
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
    local sid="$1" body="${2:-}" code
    [ -n "$body" ] || body='{}'
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
        --data "$body" \
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

exec_argv_denied_through_cluster() {
    local sid="$1" token="$2" diagnostics="$WORK/native-exec-condition-denied.log"
    if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$ROUTER_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --proxy-header "X-Kuasar-Sandbox-Group: $GROUP" \
        --proxy-header "X-Kuasar-Route-Key: $ROUTE_KEY" \
        -- /bin/true >"$diagnostics" 2>&1; then
        fail "cluster executed a condition-denied argv"
    fi
    grep -Fq "exec: remote exec rejected" "$diagnostics" \
        || { sed 's/^/  client| /' "$diagnostics" >&2; fail "cluster denial was not redacted"; }
}

exec_through_cluster_connect() {
    local sid="$1" token="$2" marker="$3"
    local retries="${4:-1}"
    local input="$WORK/native-exec.stdin"
    local output="$WORK/native-exec.stdout"
    local error_output="$WORK/native-exec.stderr"
    local diagnostics="$WORK/native-exec.client.log"
    local attempt status guest_command

    # The pinned envd is quiet on successful requests. Write test data into its
    # actual guest stdio pipes so the managed run, not this exec client's file
    # destinations, exercises the application journal targets. Shared guest PID
    # namespace and the root test capability allow this without changing envd
    # verbosity or production launch policy. The outer timeout bounds all I/O.
    guest_command="$(cat <<'SH'
primary=
for executable in /proc/[0-9]*/exe; do
    case "$(readlink "$executable" 2>/dev/null)" in
        */envd) primary=${executable%/exe}; break ;;
    esac
done
[ -n "$primary" ] || { echo 'journal probe: envd not found' >&2; exit 48; }
[ -p "$primary/fd/1" ] && [ -p "$primary/fd/2" ] || {
    echo 'journal probe: primary stdio is not pipe-backed' >&2; exit 48;
}
printf 'journal-primary-stdout:%s\n' "$1" >"$primary/fd/1" || exit 49
printf 'journal-primary-stderr:%s\n' "$1" >"$primary/fd/2" || exit 49
IFS= read -r value
printf 'stdout:%s:%s\n' "$1" "$value"
printf 'stderr:%s\n' "$1" >&2
exit 47
SH
)"
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
            /bin/sh -c "$guest_command" journal-probe "$marker" \
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

router_req() {
    local method="$1" path="$2" key="$3" route_key="${4:-}" body="${5:-}"
    local args=(-sS --noproxy '*' --max-time 260 -o "$WORK/router-resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-Kuasar-Sandbox-Group: $GROUP" -H "X-API-KEY: $key")
    [ -n "$route_key" ] && args+=(-H "X-Kuasar-Route-Key: $route_key")
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$ROUTER_PORT$path"
}

wait_cluster_traffic_stats() { # $1=sid, $2=parking|idle|paused
    local sid="$1" mode="$2" code=""
    for _ in $(seq 1 240); do
        code="$(router_req GET "/sandboxes/$sid/stats/traffic" "$CLUSTER_API_KEY" "$ROUTE_KEY" || true)"
        if [ "$code" = "200" ] && python3 - "$WORK/router-resp.body" "$mode" <<'PY'
import json, sys
stats = json.load(open(sys.argv[1]))
mode = sys.argv[2]
if set(stats) - {"state", "maxInflight", "inflight", "idleSince", "services", "platform", "transit", "egress"}:
    raise SystemExit(1)
if stats.get("egress") != {}:
    raise SystemExit(1)
for plane in ("platform", "transit"):
    counters = stats.get(plane)
    if not isinstance(counters, dict) or (counters and set(counters) != {"rxPackets", "rxBytes", "txPackets", "txBytes"}):
        raise SystemExit(1)
    if any(type(value) is not int or value < 0 for value in counters.values()):
        raise SystemExit(1)
max_inflight = stats.get("maxInflight")
if not isinstance(max_inflight, dict) or set(max_inflight) != {"total", "forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
if any(type(value) is not int or value < 0 for value in max_inflight.values()):
    raise SystemExit(1)
inflight = stats.get("inflight", {})
services = stats.get("services", {})
if set(inflight) != {"parking", "connected"}:
    raise SystemExit(1)
if set(services) != {"forward", "e2b:envd", "e2b:code-interpreter", "exec"}:
    raise SystemExit(1)
for item in services.values():
    if set(item) - {"parking", "connected", "idleSince"} or not {"parking", "connected"} <= set(item):
        raise SystemExit(1)
    if (item["parking"] or item["connected"]) and "idleSince" in item:
        raise SystemExit(1)
if mode == "parking":
    ok = inflight["parking"] >= 1 and "idleSince" not in stats
elif mode == "idle":
    ok = stats.get("state") == "running" and inflight == {"parking": 0, "connected": 0} and "idleSince" in stats
elif mode == "paused":
    ok = stats.get("state") == "paused" and inflight == {"parking": 0, "connected": 0} and "idleSince" not in stats
else:
    ok = False
raise SystemExit(0 if ok else 1)
PY
        then
            return 0
        fi
        sleep 0.05
    done
    step "traffic stats sid=$sid mode=$mode last_status=$code body=$(cat "$WORK/router-resp.body" 2>/dev/null)"
    return 1
}

wait_node_sandbox_finalized() { # $1=node-local sandbox id
    local sid="$1"
    for _ in $(seq 1 120); do
        if [ -f "$WORK/cl/node-ctl.db" ] && \
           python3 - "$WORK/cl/node-ctl.db" "$sid" <<'PY'
import sqlite3, sys
with sqlite3.connect(sys.argv[1], timeout=5) as db:
    row = db.execute("select 1 from sandboxes where id=?", (sys.argv[2],)).fetchone()
raise SystemExit(0 if row is None else 1)
PY
        then
            if [ ! -e "$WORK/cr/sandboxes/$sid" ] && [ ! -e "$WORK/cl/sandboxes/$sid" ]; then
                [ -S "$WORK/cn.sock" ] || return 1
                return 0
            fi
        fi
        sleep 0.25
    done
    return 1
}

create_sandbox() {
    local out="$1"
    http_code "$out" \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-Kuasar-Sandbox-Group: $GROUP" \
        -H "X-Kuasar-Route-Key: $ROUTE_KEY" \
        -H "X-API-KEY: $CLUSTER_API_KEY" \
        -H 'Content-Type: application/json' \
        --data '{}' \
        "http://127.0.0.1:$ROUTER_PORT/sandboxes"
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
print("\t".join([fields[0], fields[1], fields[3]]))
PY
}

assert_cluster_resource_yaml() { # $1=node-local generated sandbox YAML
    python3 - "$1" <<'PY'
import sys

path = sys.argv[1]
values, stack = {}, []
for raw in open(path, encoding="utf-8"):
    line = raw.split("#", 1)[0].rstrip()
    if not line.strip() or line.lstrip().startswith("-") or ":" not in line:
        continue
    indent = len(line) - len(line.lstrip(" "))
    key, value = line.strip().split(":", 1)
    while stack and indent <= stack[-1][0]:
        stack.pop()
    dotted = ".".join([item[1] for item in stack] + [key])
    value = value.strip().strip('"').strip("'")
    if value:
        values[dotted] = value
    else:
        stack.append((indent, key))
expected = {
    "resources.capacity.cpu": "2",
    "resources.capacity.memory": "2GiB",
    "resources.allocatable.cpu": "2",
    "resources.allocatable.memory": "256MiB",
    "resources.allocatable.deflate_on_oom": "true",
    "resources.startup.memory": "2GiB",
    "resources.overhead.memory": "32MiB",
    "resources.watermark_high.ratio": "0.875",
}
for key, want in expected.items():
    if values.get(key) != want:
        raise SystemExit(f"{path}: {key}={values.get(key)!r}, want {want!r}; values={values}")
for forbidden in ("resources.control.controller", "resources.control.cgroup_path",
                  "resources.watermark_high.memory",
                  "resources.control.sensor.mode"):
    if forbidden in values:
        raise SystemExit(f"{path}: static cluster YAML contains {forbidden}: {values}")
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
