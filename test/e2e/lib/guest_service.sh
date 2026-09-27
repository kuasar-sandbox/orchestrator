#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
freeze_service_probe() {
    local service_code
    service_code=$(DP_MAX_TIME=8 dp "8001-$SID" / "$FORWARD_TOKEN" || true)
    [ "$service_code" = "200" ] || return 1
    read -r FREEZE_MARK FREEZE_COUNTER FREEZE_PROBE_PID FREEZE_CGROUP <"$WORK/dp.body"
    [ "$FREEZE_MARK" = "ENVD_FREEZE_SERVICE" ] &&
        [ "$FREEZE_PROBE_PID" = "$FREEZE_PID" ] &&
        [[ "$FREEZE_COUNTER" =~ ^[0-9]+$ ]] &&
        [[ "$FREEZE_CGROUP" == /user || "$FREEZE_CGROUP" == /user/* ]]
}

start_freeze_service() {
FREEZE_SERVICE_B64=$(python3 - <<'PY'
import base64
program = r'''
import os, pathlib, socket, time

listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("0.0.0.0", 8001))
listener.listen()
listener.settimeout(0.05)
counter = 0
cgroup = pathlib.Path("/proc/self/cgroup").read_text().strip().splitlines()[0].split(":", 2)[2]
while True:
    counter += 1
    try:
        conn, _ = listener.accept()
    except socket.timeout:
        pass
    else:
        with conn:
            conn.settimeout(0.2)
            try:
                conn.recv(4096)
            except OSError:
                pass
            body = f"ENVD_FREEZE_SERVICE {counter} {os.getpid()} {cgroup}\n".encode()
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " +
                         str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
    time.sleep(0.05)
'''
print(base64.b64encode(program.encode()).decode())
PY
)
python3 "$WORK/envd_exec.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "python3 -c \"import base64; open('/home/user/freeze_service.py','wb').write(base64.b64decode('$FREEZE_SERVICE_B64'))\"" \
    >"$WORK/install-freeze-service.out" 2>&1 || true
grep -q 'EXIT_CODE 0' "$WORK/install-freeze-service.out" \
    || { sed 's/^/  envd| /' "$WORK/install-freeze-service.out"; fail "install envd freeze service"; }
FREEZE_PID=$(python3 "$WORK/envd_start.py" "$ENVD_SOCK" "$ENVD_TOKEN" \
    "python3 /home/user/freeze_service.py" 2>"$WORK/start-freeze-service.err") \
    || { cat "$WORK/start-freeze-service.err"; fail "start envd freeze service"; }
[[ "$FREEZE_PID" =~ ^[0-9]+$ ]] || fail "envd freeze service returned invalid pid: $FREEZE_PID"

service_ready=""
for _ in $(seq 1 40); do
    freeze_service_probe && { service_ready=1; break; }
    sleep 0.25
done
[ -n "$service_ready" ] || { cat "$WORK/dp.body" 2>/dev/null || true; fail "envd freeze service did not listen"; }

}
