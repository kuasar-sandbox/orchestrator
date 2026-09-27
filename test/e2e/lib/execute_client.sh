#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
req() {
    local method="$1" path="$2" key="$3" body="${4:-}"
    local args=(-sS --noproxy '*' -o "$WORK/resp.body" -w '%{http_code}' -X "$method" -H "Host: api.$DOMAIN" -H "X-API-KEY: $key")
    # Optional sandbox-config injection header (docs/node.md §4.4): set REQ_NET_HEADER to a JSON
    # network spec to exercise X-Kuasar-Sandbox-Network on a create.
    [ -n "${REQ_NET_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Network: ${REQ_NET_HEADER}")
    [ -n "${REQ_RESOURCE_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Resource: ${REQ_RESOURCE_HEADER}")
    [ -n "${REQ_CHECKPOINT_HEADER:-}" ] && args+=(-H "X-Kuasar-Sandbox-Checkpoint: ${REQ_CHECKPOINT_HEADER}")
    if [ "${REQ_ATTACH_MMDS:-0}" = 1 ] && [ -n "${REQ_MMDS_HEADER:-}" ]; then
        args+=(-H "X-Kuasar-Sandbox-MMDS: ${REQ_MMDS_HEADER}")
    fi
    [ -n "$body" ] && args+=(-H 'Content-Type: application/json' -d "$body")
    curl "${args[@]}" "http://127.0.0.1:$PORT$path"
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
issue_exec_session() {
    local sid="$1" key="$2" body="${3:-}" code
    [ -n "$body" ] || body='{}'
    code="$(curl -sS --noproxy '*' --max-time 30 \
        -D "$WORK/exec-session.headers" \
        -o "$WORK/exec-session.secret" \
        -w '%{http_code}' \
        -X POST \
        -H "Host: api.$DOMAIN" \
        -H "X-API-KEY: $key" \
        -H 'Content-Type: application/json' \
        --data "$body" \
        "http://127.0.0.1:$PORT/sandboxes/$sid/exec-sessions")"
    [ "$code" = "201" ] || fail "exec-session=$code (want 201)"
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
exec_argv_allowed_through_connect() {
    local sid="$1" token="$2" argv0="$3" diagnostics="$WORK/native-exec-condition-allowed.log"
    timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PROXY_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        -- "$argv0" >"$diagnostics" 2>&1 \
        || { sed 's/^/  client| /' "$diagnostics"; fail "conditioned argv $argv0 was rejected"; }
}
exec_argv_denied_through_connect() {
    local sid="$1" token="$2" diagnostics="$WORK/native-exec-condition-denied.log"
    if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PROXY_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        -- /bin/echo must-not-run >"$diagnostics" 2>&1; then
        fail "denied argv unexpectedly executed"
    fi
    grep -Fq "exec: remote exec rejected" "$diagnostics" \
        || { sed 's/^/  client| /' "$diagnostics"; fail "denied argv did not return the redacted remote error"; }
}
exec_through_connect() {
    local sid="$1" token="$2" marker="$3"
    local input="$WORK/native-exec.stdin"
    local output="$WORK/native-exec.stdout"
    local error_output="$WORK/native-exec.stderr"
    local diagnostics="$WORK/native-exec.client.log"
    local status

    printf 'stdin:%s\n' "$marker" >"$input"
    : >"$output"
    : >"$error_output"
    if timeout -k 5s 60 "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PROXY_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --proxy-header "X-Kuasar-E2E-Duplicate: first" \
        --proxy-header "X-Kuasar-E2E-Duplicate: second" \
        --stdin-from "$input" --stdout-to "$output" --stderr-to "$error_output" -- \
        /bin/sh -c "IFS= read -r value; printf 'stdout:%s:%s\\n' '$marker' \"\$value\"; printf 'stderr:%s\\n' '$marker' >&2; exit 47" \
        >"$diagnostics" 2>&1; then
        status=0
    else
        status=$?
    fi
    grep -Fxq "stdout:$marker:stdin:$marker" "$output" 2>/dev/null \
        || { sed 's/^/  client| /' "$diagnostics"; sed 's/^/  stdout| /' "$output" 2>/dev/null; dump_exec_failure_context "$sid"; fail "native exec stdout/stdin mismatch"; }
    grep -Fxq "stderr:$marker" "$error_output" 2>/dev/null \
        || { sed 's/^/  client| /' "$diagnostics"; sed 's/^/  stderr| /' "$error_output" 2>/dev/null; dump_exec_failure_context "$sid"; fail "native exec stderr mismatch"; }
    [ "$status" = "47" ] \
        || { sed 's/^/  client| /' "$diagnostics"; dump_exec_failure_context "$sid"; fail "native exec exit=$status (want guest status 47)"; }
}

dump_exec_failure_context() { # $1=sandbox id
    local sid="$1" state run_id failed_run_id=""
    state="$(sandbox_state "$sid")"
    run_id="$(sandbox_run_id "$sid")"
    echo "==> exec failure context: sid=$sid state=$state run_id=${run_id:-<empty>}" >&2
    if [ -n "${ORCH_LOG:-}" ] && [ -f "$ORCH_LOG" ]; then
        echo "==> matching node-ctl lifecycle log:" >&2
        grep -F "sid=$sid" "$ORCH_LOG" | sed 's/^/  orch| /' >&2 || true
        failed_run_id="$(python3 - "$ORCH_LOG" "$sid" <<'PY'
import re, sys
last = ""
for line in open(sys.argv[1], encoding="utf-8"):
    if f"sid={sys.argv[2]}" not in line:
        continue
    match = re.search(r'\brun_id=(?:"([^"]*)"|(\S+))', line)
    if match and (match.group(1) or match.group(2)):
        last = match.group(1) or match.group(2)
print(last)
PY
)"
    fi
    if [ -n "$failed_run_id" ]; then
        echo "==> failed runner unit journal: ${RUNNER_PREFIX}$failed_run_id.service" >&2
        journalctl -u "${RUNNER_PREFIX}$failed_run_id.service" --no-pager 2>/dev/null | sed 's/^/  unit| /' >&2 || true
    else
        echo "==> matching sandbox runner journal:" >&2
        journalctl KUASAR_SANDBOX_ID="$sid" --no-pager 2>/dev/null | sed 's/^/  unit| /' >&2 || true
    fi
}

exec_pty_resize_through_connect() {
    local sid="$1" token="$2" marker="$3"
    local output="$WORK/native-exec-pty.out"
    local status=0

    python3 - "$output" "$BIN/sandbox-ctl" exec \
        --proxy "http://127.0.0.1:$PROXY_PORT" \
        --proxy-header "E2b-Sandbox-Id: $sid" \
        --proxy-header "E2b-Sandbox-Service: exec" \
        --proxy-header "X-Access-Token: $token" \
        --cwd /tmp --user 0:0 --tty -- /bin/sh -c \
        "stty size; trap 'stty size; echo $marker; exit 23' WINCH; echo PTY_READY; while :; do sleep 1; done" <<'PY' || status=$?
import errno, fcntl, os, pty, select, signal, struct, subprocess, sys, termios, time

output_path, argv = sys.argv[1], sys.argv[2:]
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 37, 91, 0, 0))
proc = subprocess.Popen(argv, stdin=slave, stdout=slave, stderr=slave, close_fds=True)
os.close(slave)
captured = bytearray()
resized = False
deadline = time.monotonic() + 60
try:
    while True:
        if time.monotonic() >= deadline:
            proc.kill()
            proc.wait()
            raise SystemExit(124)
        readable, _, _ = select.select([master], [], [], 0.1)
        if not readable:
            continue
        try:
            chunk = os.read(master, 65536)
        except OSError as exc:
            if exc.errno == errno.EIO:
                break
            raise
        if not chunk:
            break
        captured.extend(chunk)
        if not resized and b"PTY_READY" in captured:
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 41, 101, 0, 0))
            os.kill(proc.pid, signal.SIGWINCH)
            resized = True
finally:
    os.close(master)
    with open(output_path, "wb") as output_file:
        output_file.write(captured)
raise SystemExit(proc.wait())
PY
    grep -q '37 91' "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec initial PTY size mismatch"; }
    grep -q '41 101' "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec resized PTY size mismatch"; }
    grep -q "$marker" "$output" 2>/dev/null || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec PTY marker missing"; }
    [ "$status" = "23" ] || { sed 's/^/  pty| /' "$output" 2>/dev/null; fail "native exec PTY exit=$status (want 23)"; }
}
dp() {
    local port_sid="$1" path="$2" token="${3:-}"
    local args=(-sS --max-time "${DP_MAX_TIME:-120}" --noproxy '*' -o "$WORK/dp.body" -w '%{http_code}' -H "Host: $port_sid.$DOMAIN")
    [ -n "$token" ] && args+=(-H "X-Access-Token: $token")
    curl "${args[@]}" "http://127.0.0.1:$PROXY_PORT$path"
}


create_guest() {
    local body="${1:-}"
    [ -n "$body" ] || body="{\"templateID\":\"$TEMPLATE\",\"timeout\":120}"
    code=$(req POST /sandboxes "$AK" "$body")
    [ "$code" = 201 ] || { cat "$WORK/resp.body"; fail "create=$code"; }
    SID=$(json_field "$WORK/resp.body" sandboxID)
    ENVD_TOKEN=$(json_field "$WORK/resp.body" envdAccessToken)
    FORWARD_TOKEN=$(json_field "$WORK/resp.body" forwardAccessToken)
    assert_no_default_exec_token "$WORK/resp.body" || fail "default exec token"
    ENVD_SOCK="$WORK/run/sandboxes/$SID/envd.sock"
}
