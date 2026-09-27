#!/usr/bin/env bash
start_builder_network() {
if "$BIN/connector-ctl" vswitch status "$SWITCH" >/dev/null 2>&1; then fail "fixture switch already exists: $SWITCH"; fi
ip netns add "$SW_NETNS"
NETNS_CREATED=1
SW_STARTED=1
# The extraction CIDRs classify guest traffic; this test owns the management
# VIP address used to reach Zot and MMDS on the host. No NAT is required.
"$BIN/connector-ctl" vswitch serve "$SWITCH" --netns="$SW_NETNS" --ports=16 --mac-addr=02:00:00:00:01:01 \
    --floating-ip-base=100.100.112.0 --mode=tap \
    --mgmt-extract=:$SW_MGMT:$MGMT_VIP,0.0.0.0/0 \
    --mgmt-service=$MGMT_VIP:80:$MGMT_VIP:$MMDS_PORT \
    --tapfd-listen="$TAPFD_SOCKET" --watch-interval=2s >"$WORK/vswitch.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
    if [ -S "$TAPFD_SOCKET" ] && "$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1; then
        break
    fi
    kill -0 "${PIDS[-1]}" 2>/dev/null || { cat "$WORK/vswitch.log"; fail "vswitch serve exited"; }
    sleep 0.2
done
"$BIN/connector-ctl" vswitch status "$SWITCH" --ready >/dev/null 2>&1 \
    || { cat "$WORK/vswitch.log"; fail "vswitch not ready"; }
ip addr replace "$MGMT_VIP/32" dev "$SW_MGMT" \
    || fail "configure management VIP on $SW_MGMT"

# This name is resolvable only by the request-selected DNS server. Default
# networking cannot make these image builds pass if Register drops the header
# or the worker ignores the resolved network. Use stdlib Python, already
# required by this suite, rather than depending on an external DNS service.
BUILD_REGISTRY_HOST="image-build.invalid"
NETWORK_PULL_REF="$BUILD_REGISTRY_HOST:$ZOT_PORT/e2e/base:v1"
BUILD_NETWORK_HEADER="{\"dns\":[\"$MGMT_VIP\"]}"
python3 -u - "$MGMT_VIP" "$BUILD_REGISTRY_HOST" >"$WORK/build-dns.log" 2>&1 <<'PY' &
import socket, struct, sys

address, hostname = sys.argv[1:]
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((address, 53))
print("ready", flush=True)
while True:
    packet, peer = sock.recvfrom(4096)
    if len(packet) < 12 or struct.unpack("!H", packet[4:6])[0] != 1:
        continue
    try:
        labels, offset = [], 12
        while packet[offset]:
            length = packet[offset]
            if length > 63:
                raise ValueError("compressed or invalid query label")
            offset += 1
            labels.append(packet[offset:offset + length].decode("ascii"))
            offset += length
        offset += 1
        qtype, qclass = struct.unpack("!HH", packet[offset:offset + 4])
        name = ".".join(labels).lower()
        known = name == hostname and qclass == 1
        answer = b""
        if known and qtype == 1:
            answer = b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 1, 4) + socket.inet_aton(address)
        header = packet[:2] + struct.pack("!HHHHH", 0x8180 if known else 0x8183, 1, bool(answer), 0, 0)
        sock.sendto(header + packet[12:offset + 4] + answer, peer)
        print(name, qtype, flush=True)
    except (IndexError, UnicodeError, ValueError, struct.error):
        continue
PY
BUILD_DNS_PID=$!
PIDS+=("$BUILD_DNS_PID")
for _ in $(seq 1 50); do
    grep -q '^ready$' "$WORK/build-dns.log" && break
    kill -0 "$BUILD_DNS_PID" 2>/dev/null || { cat "$WORK/build-dns.log"; fail "Build DNS server exited"; }
    sleep 0.1
done
grep -q '^ready$' "$WORK/build-dns.log" || fail "Build DNS server did not bind"
BUILD_NETWORK_STEP=$(python3 - "$BUILD_REGISTRY_HOST" "$ZOT_PORT" <<'PY'
import json, shlex, sys
url = f"http://{sys.argv[1]}:{sys.argv[2]}/v2/"
program = "import urllib.request; opener = urllib.request.build_opener(urllib.request.ProxyHandler({})); "
program += f"assert opener.open({url!r}, timeout=5).status == 200"
print(json.dumps({"type": "RUN", "args": ["python3 -c " + shlex.quote(program)]}))
PY
)
echo "==> vswitch up ($SWITCH; mgmt $SW_MGMT=$MGMT_VIP; tapfd_socket=$TAPFD_SOCKET)"
}
