#!/usr/bin/env bash
# Prepared process/configuration and assertion primitives; cases own test flows.
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; fail "$3 did not open $1:$2"; }
wait_mmds_listener() {
    local hex
    hex="$(printf '%04X' "$MMDS_PORT")"
    for _ in $(seq 1 60); do
        ip netns exec "$PROXY_NETNS" awk -v p=":$hex" '$2 ~ p && $4 == "0A" { found = 1 } END { exit(found ? 0 : 1) }' /proc/net/tcp 2>/dev/null && return 0
        sleep 0.5
    done
    fail "Proxy MMDS listener did not appear in proxy_netns=$PROXY_NETNS on $PROXY_NS_IP:$MMDS_PORT"
}
check_proxy_netns_address() {
    local phase="$1"
    # This is namespace/link metadata only, never conductor configuration or
    # sandbox credentials. Diagnose address loss at the exact startup boundary.
    if ! ip -n "$PROXY_NETNS" -o -4 addr show dev "$PROXY_VETH_NS" | \
        awk -v address="$PROXY_NS_IP/30" '$4 == address { found = 1 } END { exit !found }'; then
        echo "==> Proxy namespace address missing ($phase)" >&2
        ip -n "$PROXY_NETNS" -br address >&2 || true
        fail "expected $PROXY_NS_IP/30 on $PROXY_NETNS/$PROXY_VETH_NS"
    fi
    echo "==> Proxy namespace address verified ($phase; netns=$(stat -Lc '%i' "/var/run/netns/$PROXY_NETNS"))"
}
allow_proxy_forwarding() {
    if ! iptables -C FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT 2>/dev/null; then
        iptables -A FORWARD -i "$PROXY_VETH_HOST" -o "${SWITCH}m0" -j ACCEPT
        FORWARD_TO_SWITCH_OWNED=1
    fi
    if ! iptables -C FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT 2>/dev/null; then
        iptables -A FORWARD -i "${SWITCH}m0" -o "$PROXY_VETH_HOST" -j ACCEPT
        FORWARD_FROM_SWITCH_OWNED=1
    fi
}
