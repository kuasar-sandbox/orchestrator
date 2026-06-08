#!/usr/bin/env bash
#
# demo_e2b.sh — end-to-end demonstration of the e2b-compatible sandbox host,
# driven by the unmodified e2b CLI over the REAL e2b base image
# (e2bdev/code-interpreter): build a template, spawn a real microVM, run commands,
# expose a port + reach the network, pause/resume (state survives), then kill.
#
#   e2b template build         → flatten the real e2b image into a template
#   e2b sandbox create -d      → boot a real cloud-hypervisor microVM
#   e2b sandbox exec  … -- CMD  → run a command in the guest (through the proxy)
#   port forward + egress       → host→guest via floatingip (sw0m0) + guest→internet (NAT)
#   e2b sandbox pause / resume  → snapshot to the store / restore
#   e2b sandbox list / kill     → lifecycle
#
# The CLI talks to the orchestrator exactly as against e2b.dev: control plane at
# https://api.<domain>, data plane at https://<port>-<sid>.<domain>. We make those
# resolve locally with /etc/hosts + a self-signed *.<domain> TLS cert on :443, and
# point the CLI with E2B_DOMAIN / E2B_API_KEY. Nothing about the CLI is modified.
#
# Networking: vswitch is started with --mgmt-extract so the host gets a sw0m0 NIC
# that reaches sandbox floatingips, plus host NAT (MASQUERADE) so sandboxes egress.
#
# Requires: e2b CLI, systemd+root, /dev/kvm, docker (with e2bdev/code-interpreter
# cached), zot, store-ctl, mkfs.erofs, openssl, iptables, curl, sqlite3, the built
# kernel+runtime erofs in bin, TCP :443 free. Run as a normal user; it re-execs
# under sudo. Set DEMO_PAUSE=1 to step through (and drive the CLI from another term).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.demo.local}"
TLS_PORT="${TLS_PORT:-443}"               # the CLI uses the implicit :443 for api/sandbox hosts
E2E_IMAGE="${E2E_IMAGE:-e2bdev/code-interpreter:latest}"   # the real e2b base (cached locally)
SWITCH="${SWITCH:-sw0}"; SW_NETNS="${SW_NETNS:-demo_sw}"
SW_MGMT="${SW_MGMT:-sw0m0}"; FIP_CIDR="${FIP_CIDR:-100.100.96.0/20}"  # mgmt NIC + floating-IP range
GUEST_DNS="${GUEST_DNS:-169.254.169.253}"; HOST_DNS=""               # guest resolv.conf stub (DNAT'd to host DNS below)
ZOT_BIN="${ZOT_BIN:-$(command -v zot || true)}"
E2B_BIN="${E2B_BIN:-$(command -v e2b || true)}"

# ---- pretty narration -----------------------------------------------------
c_hd=$'\e[1;36m'; c_cmd=$'\e[1;33m'; c_ok=$'\e[1;32m'; c_dim=$'\e[2m'; c_off=$'\e[0m'
[ -t 1 ] || { c_hd=; c_cmd=; c_ok=; c_dim=; c_off=; }
step=0
banner() { step=$((step+1)); echo; echo "${c_hd}══════ [$step] $* ══════${c_off}"; }
say()    { echo "${c_dim}  · $*${c_off}"; }
ok()     { echo "${c_ok}  ✓ $*${c_off}"; }
pause()  { [ -n "${DEMO_PAUSE:-}" ] && { printf "${c_dim}  ⏎ to continue…${c_off}"; read -r _ </dev/tty 2>/dev/null || true; } || true; }
# show($desc) CMD…  — echo the command, then run it
show()   { local d="$1"; shift; say "$d"; echo "${c_cmd}  \$ $*${c_off}"; "$@"; }
die()    { echo $'\e[1;31m'"  ✗ $*"$'\e[0m' >&2; exit 1; }

# ---- prerequisites --------------------------------------------------------
for b in orchestrator-ctl store-ctl flatten-ctl e2b-key-ctl vswitch-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || die "missing $BIN/$b — run 'make build'"; done
[ -f "$BIN/vmlinux" ] && [ -f "$BIN/sandbox-runtime-e2b.erofs" ] || die "missing kernel/runtime erofs in $BIN"
[ -n "$E2B_BIN" ] && [ -x "$E2B_BIN" ] || die "e2b CLI not on PATH"
[ -n "$ZOT_BIN" ] && [ -x "$ZOT_BIN" ] || die "zot not on PATH"
for t in docker openssl python3 ip curl sqlite3 iptables; do command -v $t >/dev/null 2>&1 || die "$t not on PATH"; done
command -v mkfs.erofs >/dev/null 2>&1 || [ -x "$BIN/mkfs.erofs" ] || die "mkfs.erofs not found"
[ -d /run/systemd/system ] || die "systemd is not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || die "/dev/kvm not available (rw)"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || die "base image $E2E_IMAGE not cached (set E2E_IMAGE)"
if [ "$(id -u)" -ne 0 ]; then echo "(re-exec under sudo for systemd/KVM/:443)"; exec sudo -nE "$0" "$@"; fi
command -v mkfs.erofs >/dev/null 2>&1 || export PATH="$BIN:$PATH"

echo "${c_hd}"'
   kuasar-sandbox · e2b-compatible microVM host — end-to-end demo
   (everything below is driven by the UNMODIFIED e2b CLI '"$("$E2B_BIN" --version 2>/dev/null | head -1)"')
'"${c_off}"

# ---- workspace + teardown -------------------------------------------------
WORK="$(mktemp -d /tmp/demo-e2b-XXXXXX)"
# A world-readable env file another (non-root) terminal can `source` to drive the
# e2b CLI against this node ($WORK is root-only). Demo-only credentials.
CLI_ENV_FILE="/tmp/demo-e2b-cli-env.sh"
UNIT_DIR=/run/systemd/system
UNITS=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=(); for u in "${UNITS[@]}"; do [ -e "$UNIT_DIR/$u" ] && die "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/store" "$WORK/zot/data" "$WORK/home"
declare -a PIDS=() TAGS=() NAT_ADDED=()
cleanup() {
    set +e; echo; echo "${c_dim}── teardown ──${c_off}"
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    # remove ONLY the NAT rules we added (not pre-existing ones)
    for r in "${NAT_ADDED[@]:-}"; do case "$r" in
        r1) iptables -D FORWARD -o "$SW_MGMT" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null;;
        r2) iptables -D FORWARD -i "$SW_MGMT" -j ACCEPT 2>/dev/null;;
        r3) iptables -t nat -D POSTROUTING -s "$FIP_CIDR" -j MASQUERADE 2>/dev/null;;
        dns-*) iptables -t nat -D PREROUTING -d "$GUEST_DNS" -p "${r#dns-}" --dport 53 -j DNAT --to-destination "${HOST_DNS:-}:53" 2>/dev/null;;
    esac; done
    "$BIN/vswitch-ctl" stop "$SWITCH" --force >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done; systemctl daemon-reload 2>/dev/null
    for t in "${TAGS[@]:-}"; do [ -n "$t" ] && docker rmi -f "$t" >/dev/null 2>&1; done
    sed -i "/# demo-e2b-$$\$/d" /etc/hosts 2>/dev/null   # delete THIS run's entries (tagged by PID)
    rm -f "$CLI_ENV_FILE"
    [ -n "${DEMO_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT
hosts_add() { local fqdn="$1"; grep -q "[[:space:]]$fqdn\b" /etc/hosts 2>/dev/null || echo "127.0.0.1 $fqdn # demo-e2b-$$" >> /etc/hosts; }

# print_cli_access — write a sourceable env file + show how to drive the e2b CLI
# from ANOTHER terminal against this running node (run with DEMO_PAUSE=1 to hold).
print_cli_access() {
    cat > "$CLI_ENV_FILE" <<EOF
# e2b CLI → this demo node. Source me in another terminal. (demo-only credentials)
export E2B_DOMAIN='$DOMAIN'
export E2B_API_KEY='$AK'
export E2B_ACCESS_TOKEN='sk_demo_dummy'
export E2B_IMAGE_URI_MASK='$MASK'
export NODE_TLS_REJECT_UNAUTHORIZED=0
export NO_PROXY='*'
EOF
    chmod 644 "$CLI_ENV_FILE"
    echo
    echo "${c_hd}══════ 另开一个终端，用真实 e2b CLI 操作本节点 ══════${c_off}"
    echo "${c_dim}  在另一个窗口里 source 这个文件（或复制其内容）：${c_off}"
    echo "${c_cmd}    source $CLI_ENV_FILE${c_off}"
    sed 's/^/      /' "$CLI_ENV_FILE"
    echo "${c_dim}  然后例如（凭据已就绪，CLI 二进制零改造）：${c_off}"
    echo "${c_cmd}    e2b sandbox list${c_off}"
    echo "${c_cmd}    e2b sandbox exec <SID> -- 'id; uname -a'${c_off}   ${c_dim}# 本演示创建的沙箱(下方给出 SID)可直接用${c_off}"
    echo "${c_cmd}    e2b template build --name foo --dockerfile e2b.Dockerfile${c_off}"
    echo "${c_dim}  对你在该窗口自己 create 的沙箱，exec/connect 前先加它的 host（数据面经 <port>-<sid>.<domain>）：${c_off}"
    echo "${c_cmd}    SID=<id>; echo \"127.0.0.1 49983-\$SID.$DOMAIN 49999-\$SID.$DOMAIN\" | sudo tee -a /etc/hosts${c_off}"
    echo "${c_hd}═════════════════════════════════════════════════════${c_off}"
}
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
wait_port() { for _ in $(seq 1 80); do (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }; sleep 0.5; done; die "$3 not listening on $1:$2"; }

# ===========================================================================
banner "Bring up the node stack (content store, registry, orchestrator, switch)"
# ---------------------------------------------------------------------------
STORE_PORT="$(free_port)"
cat > "$WORK/store.yaml" <<EOF
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: { root: $WORK/store, verify_content_key: true }
EOF
say "store-ctl — content-addressed store (dedup, convergent encryption); generation G1"
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1 >"$WORK/store-init.log" 2>&1 || { cat "$WORK/store-init.log"; die "store init"; }
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store.log" 2>&1 & PIDS+=($!); wait_port 127.0.0.1 "$STORE_PORT" store-ctl
ok "store-ctl on 127.0.0.1:$STORE_PORT"

ZOT_PORT="$(free_port)"
cat > "$WORK/zot.json" <<EOF
{ "storage": { "rootDirectory": "$WORK/zot/data", "dedupe": false, "gc": false },
  "http": { "address": "127.0.0.1", "port": "$ZOT_PORT", "compat": ["docker2s2"] },
  "log": { "level": "warn", "output": "$WORK/zot.log" } }
EOF
say "zot — local OCI registry the e2b CLI pushes its client-built image into"
"$ZOT_BIN" serve "$WORK/zot.json" >"$WORK/zot.out" 2>&1 & PIDS+=($!); wait_port 127.0.0.1 "$ZOT_PORT" zot
ok "zot on 127.0.0.1:$ZOT_PORT"

cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 30s }
cache: { endpoint: "" }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF

say "overlay diff_template — pre-formatted empty ext4 seeding each cold boot's writable upper"
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"; [ -x "$MKFS_EXT4" ] || die "mkfs.ext4 not found"
OVL="$WORK/overlay-1G.ext4"; truncate -s 1G "$OVL"; "$MKFS_EXT4" -F -q -b 4096 "$OVL" >/dev/null 2>&1 || die "mkfs.ext4"

say "self-signed *.$DOMAIN TLS cert (so the CLI reaches api/sandbox hosts over https; node trusts it via NODE_TLS_REJECT_UNAUTHORIZED=0)"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/tls.key" -out "$WORK/tls.crt" -days 2 \
    -subj "/CN=*.$DOMAIN" -addext "subjectAltName=DNS:*.$DOMAIN,DNS:$DOMAIN" >/dev/null 2>&1 || die "openssl"

MASK="127.0.0.1:$ZOT_PORT/e2b/{templateID}:{buildID}"
MK="$("$BIN/e2b-key-ctl" gen-key)"; ENC="$("$BIN/e2b-key-ctl" gen-key)"
cat > "$WORK/config.yaml" <<EOF
api:
  domain: $DOMAIN
  listen: ":$TLS_PORT"
  tls: { cert: $WORK/tls.crt, key: $WORK/tls.key }
proxy:
  auth: enforce
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths:
  run_root: $WORK/run
  base_root: $WORK/lib
  config_socket: $WORK/orchestrator.socket
units:
  dir: $UNIT_DIR
sandbox:
  network: { switch: $SWITCH }                 # e2b profile defaults: ip 169.254.0.21/30 nexthop .22; hostname/dns default + injected via files:
  boot:
    kernel: $BIN/vmlinux
    runtime_e2b: $BIN/sandbox-runtime-e2b.erofs
    runtime_base: $BIN/sandbox-runtime.erofs
    overlay_diff_template: $OVL
builder:
  insecure_registry: true
  image_uri_mask: "$MASK"
checkpoint:
  mode: local                                  # node-local snapshot files (default)
  local_dir: $WORK/saved
EOF
# external binaries (sandbox-ctl/vswitch-ctl/…) auto-discovered next to orchestrator-ctl ($BIN)

# the switch (eBPF/TC) — created once, lives in the kernel
"$BIN/vswitch-ctl" stop "$SWITCH" --force >/dev/null 2>&1 || true; ip netns del "$SW_NETNS" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
say "vswitch-ctl start — eBPF/TC switch; --mgmt-extract adds host NIC $SW_MGMT (169.254.169.254) to reach sandbox floatingips"
"$BIN/vswitch-ctl" start "$SWITCH" --netns="$SW_NETNS" --ports=64 --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 --mode=tap \
    --mgmt-extract=:$SW_MGMT:169.254.169.254,0.0.0.0/0 >"$WORK/vswitch.log" 2>&1 || { cat "$WORK/vswitch.log"; die "vswitch start"; }
ok "switch $SWITCH up (host mgmt NIC $SW_MGMT)"

# Host NAT so sandboxes egress (internet) / inter-talk. Add idempotently and track
# what we added so teardown removes only those (not pre-existing rules).
say "host NAT (MASQUERADE $FIP_CIDR) so sandboxes reach the network via $SW_MGMT"
iptables -C FORWARD -o "$SW_MGMT" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null \
  || { iptables -A FORWARD -o "$SW_MGMT" -m state --state RELATED,ESTABLISHED -j ACCEPT && NAT_ADDED+=(r1); }
iptables -C FORWARD -i "$SW_MGMT" -j ACCEPT 2>/dev/null \
  || { iptables -A FORWARD -i "$SW_MGMT" -j ACCEPT && NAT_ADDED+=(r2); }
iptables -t nat -C POSTROUTING -s "$FIP_CIDR" -j MASQUERADE 2>/dev/null \
  || { iptables -t nat -A POSTROUTING -s "$FIP_CIDR" -j MASQUERADE && NAT_ADDED+=(r3); }
# Guest DNS: the orchestrator injects /etc/resolv.conf -> $GUEST_DNS (a link-local
# stub that needs routing to a real resolver). DNAT it to the host's first nameserver
# so in-guest name resolution works. (Port-forward needs no DNS — /etc/hosts is
# injected too — this just makes the guest resolver functional for the demo.)
HOST_DNS="$(awk '/^nameserver/{print $2; exit}' /etc/resolv.conf 2>/dev/null)"
if [ -n "$HOST_DNS" ]; then
  for pr in udp tcp; do
    iptables -t nat -C PREROUTING -d "$GUEST_DNS" -p $pr --dport 53 -j DNAT --to-destination "$HOST_DNS:53" 2>/dev/null \
      || { iptables -t nat -A PREROUTING -d "$GUEST_DNS" -p $pr --dport 53 -j DNAT --to-destination "$HOST_DNS:53" && NAT_ADDED+=(dns-$pr); }
  done
  ok "NAT ready (added: ${NAT_ADDED[*]:-none already present}); guest DNS $GUEST_DNS → host $HOST_DNS"
else
  say "no host nameserver in /etc/resolv.conf — guest DNS ($GUEST_DNS) left unrouted (port-forward still works via /etc/hosts)"
  ok "NAT ready (added: ${NAT_ADDED[*]:-none already present})"
fi

hosts_add "api.$DOMAIN"
say "orchestrator-ctl serve — the e2b control plane + data-plane proxy (TLS :$TLS_PORT, Host-routed)"
"$BIN/orchestrator-ctl" serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 & PIDS+=($!)
for _ in $(seq 1 40); do (exec 3<>"/dev/tcp/127.0.0.1/$TLS_PORT") 2>/dev/null && { exec 3>&- 3<&-; break; }; kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/    /' "$WORK/orch.log"; die "orchestrator exited"; }; sleep 0.5; done
ok "orchestrator-ctl serving https://api.$DOMAIN  (data plane: https://<port>-<sid>.$DOMAIN)"
pause

# ===========================================================================
banner "Onboard a tenant (root manifest key → allowlist → derived e2b API key)"
# ---------------------------------------------------------------------------
say "manifest_key is the tenant ROOT secret; the e2b api_key is DERIVED from it (never the reverse)."
show "allowlist this tenant's manifest key (who may create/build)" \
    "$BIN/orchestrator-ctl" manifest-key add --config "$WORK/config.yaml" "$MK"
AK="$("$BIN/e2b-key-ctl" gen-apikey "$MK")"
say "derive an e2b API key from the manifest key (this is what the CLI uses):"
echo "${c_cmd}  \$ e2b-key-ctl gen-apikey \$MANIFEST_KEY${c_off}"
echo "    → ${AK:0:16}…  (format e2b_<hex>, validated by the e2b SDK)"
ok "tenant ready"

# point the UNMODIFIED e2b CLI at this node (env only; the CLI binary is untouched)
export E2B_DOMAIN="$DOMAIN" E2B_API_KEY="$AK" E2B_ACCESS_TOKEN="sk_demo_dummy" \
       E2B_IMAGE_URI_MASK="$MASK" NODE_TLS_REJECT_UNAUTHORIZED=0 NO_PROXY='*' no_proxy='*' HOME="$WORK/home"
say "e2b CLI is now pointed at this node via: E2B_DOMAIN=$DOMAIN  E2B_API_KEY=…  (+ self-signed TLS trust)"
print_cli_access
pause

# ===========================================================================
banner "Build a template with the real 'e2b template build'"
# ---------------------------------------------------------------------------
# The e2b CLI runs `docker build --pull`, so the FROM base must be pullable from a
# registry. Seed the real e2b image into the local zot (just tag + push — it
# already ships the e2b userland: user account, bash, ionice/nice, python, curl;
# no shims). docker treats 127.0.0.1 as insecure, so the push needs no TLS.
BASE_REF="127.0.0.1:$ZOT_PORT/e2b/base:v1"
say "seed the real e2b image into zot (no build, no shims — it is already e2b-ready):"
echo "${c_cmd}  \$ docker tag $E2E_IMAGE $BASE_REF && docker push $BASE_REF${c_off}"
docker tag "$E2E_IMAGE" "$BASE_REF" && TAGS+=("$BASE_REF")
docker push "$BASE_REF" >"$WORK/base.log" 2>&1 || { cat "$WORK/base.log"; die "push base to zot"; }
ok "seeded $E2E_IMAGE → $BASE_REF"

mkdir -p "$WORK/tmpl"; cat > "$WORK/tmpl/e2b.Dockerfile" <<EOF
FROM $BASE_REF
EOF
say "template Dockerfile — 'e2b template build' docker-builds this, pushes it, and the node flattens the result into a microVM template:"
sed 's/^/      /' "$WORK/tmpl/e2b.Dockerfile"
echo "${c_cmd}  \$ e2b template build --name demo-app --dockerfile e2b.Dockerfile${c_off}"
( cd "$WORK/tmpl" && "$E2B_BIN" template build --name demo-app --dockerfile e2b.Dockerfile ) 2>&1 | sed 's/^/    /' | tee "$WORK/build.out"
grep -qiE 'finished|✅' "$WORK/build.out" || die "template build did not finish (see above)"
TEMPLATE="$(grep -oE 'e2b-img-[0-9a-f]{64}' "$WORK/build.out" | head -1)"
[ -n "$TEMPLATE" ] || TEMPLATE="demo-app"
ok "template built → $TEMPLATE"
pause

# ===========================================================================
banner "Spawn a real microVM sandbox (e2b sandbox create)"
# ---------------------------------------------------------------------------
say "create boots cloud-hypervisor from the flattened template + the e2b runtime; envd starts inside."
echo "${c_cmd}  \$ e2b sandbox create $TEMPLATE --detach${c_off}"
"$E2B_BIN" sandbox create "$TEMPLATE" --detach 2>&1 | sed 's/^/    /' | tee "$WORK/create.out"
SID="$(grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' "$WORK/create.out" | head -1)"
[ -n "$SID" ] || die "could not parse sandbox id from create output"
# the CLI reaches the guest at <port>-<sid>.<domain> — resolve those locally
hosts_add "49983-$SID.$DOMAIN"; hosts_add "49999-$SID.$DOMAIN"
ok "sandbox running: $SID  (data plane: https://49983-$SID.$DOMAIN → proxy → guest envd)"
echo "${c_dim}  · 另开窗口可直接操作此沙箱（hosts 已自动加好）：${c_off}${c_cmd}e2b sandbox exec $SID -- 'id; uname -a'${c_off}"
pause

# ===========================================================================
banner "Run commands in the guest (e2b sandbox exec)"
# ---------------------------------------------------------------------------
# The e2b CLI runs the command through the guest's /bin/bash; pass a single string.
# These run as the e2b default user 'user' (uid 1000) — the authentic default.
show "who am I + kernel + the real e2b image's python, in the guest (as default user 'user')" \
    "$E2B_BIN" sandbox exec "$SID" -- 'id; uname -sm; python3 --version; grep ^PRETTY_NAME= /etc/os-release'
echo
# flatten preserves the image's ownership, so the e2b default user owns /home/user
# and can write there (its default workdir) — no root needed.
show "write a file in the guest home (proves the 'user' owns /home/user; survives pause/resume)" \
    "$E2B_BIN" sandbox exec "$SID" -- 'echo "hello from before the snapshot" > /home/user/state.txt; cat /home/user/state.txt'
ok "command execution works end-to-end (CLI → proxy → envd → guest)"
pause

# ===========================================================================
banner "网络：端口转发 (host→沙箱 floatingip) + 沙箱出网 (NAT)"
# ---------------------------------------------------------------------------
PFMARK="pf-ok-$RANDOM"
say "在 guest 内起一个 HTTP 服务 (python3 -m http.server 8000, 后台), 服务 /home/user"
# /etc/hosts + /etc/resolv.conf are injected by the orchestrator via SANDBOX_CONFIG
# files: (so getfqdn(hostname) resolves locally; without /etc/hosts http.server's
# server_bind() stalls ~20s on DNS between bind() and listen()). Root cause + the
# files:-based fix: docs/orchestrator.md §10. No in-guest workaround needed here.
echo "${c_cmd}  \$ e2b sandbox exec $SID -b -- 'cd /home/user && echo $PFMARK > pf.txt && python3 -m http.server 8000'${c_off}"
"$E2B_BIN" sandbox exec "$SID" -b -- "cd /home/user && echo $PFMARK > pf.txt && python3 -m http.server 8000" >/dev/null 2>&1 || true
sleep 3

# (a) direct via the host mgmt NIC sw0m0 → the sandbox floatingip
FIP="$(sqlite3 "$WORK/lib/orchestrator.db" "select floatingip from sandboxes where id='$SID'" 2>/dev/null)"
INNER="$(sqlite3 "$WORK/lib/orchestrator.db" "select inner_ip from sandboxes where id='$SID'" 2>/dev/null)"
say "沙箱 floatingip = ${FIP:-?}  inner = ${INNER:-?}  (host 经 $SW_MGMT 直达)"
netdiag() {
  echo "  ${c_cmd}--- NETDIAG host ---${c_off}"
  ip -br addr show "$SW_MGMT"        2>&1 | sed 's/^/    sw0m0:    /'
  ip route get "$FIP"                2>&1 | sed 's/^/    rt-get:   /'
  ip route show 2>&1 | grep -E '100\.100|sw0m0' | sed 's/^/    rt:       /'
  ip neigh show dev "$SW_MGMT"       2>&1 | sed 's/^/    neigh:    /'
  echo "  ${c_cmd}--- NETDIAG guest (self-curl isolates server-up vs host-reach) ---${c_off}"
  "$E2B_BIN" sandbox exec "$SID" -- 'echo "self-curl: $(curl -s --max-time 3 http://127.0.0.1:8000/pf.txt 2>&1)"; echo "listen-8000:"; (ss -ltn 2>/dev/null||netstat -ltn 2>/dev/null)|grep ":8000"||echo "(none)"; echo "http-proc:"; ps -eo pid,args 2>/dev/null|grep "http\.server"|grep -v grep||echo "(none)"; echo "getfqdn(ms):"; python3 -c "import socket,time;t=time.time();socket.getfqdn(chr(48)+\".0.0.0\");print(int((time.time()-t)*1000))" 2>&1; echo "hosts:"; cat /etc/hosts 2>&1; echo "route(default=Dest 00000000):"; cat /proc/net/route 2>/dev/null' \
    2>&1 | grep -vE 'NODE_TLS|trace-warnings' | sed 's/^/    g: /' || true
  echo "  ${c_cmd}--- vswitch.log tail ---${c_off}"; tail -12 "$WORK/vswitch.log" 2>/dev/null | sed 's/^/    vsw: /'
}
echo "${c_cmd}  \$ curl http://$FIP:8000/pf.txt${c_off}"
out=""; for _ in $(seq 1 8); do out="$(curl -s --max-time 5 --noproxy '*' "http://$FIP:8000/pf.txt" 2>&1)" || true; [ "$out" = "$PFMARK" ] && break; sleep 1; done
echo "    ${out:-<no response>}"
if [ "$out" = "$PFMARK" ]; then ok "host→沙箱 floatingip:8000 直连成功 (经 $SW_MGMT)"
else netdiag; [ -n "${DEMO_NETDIAG:-}" ] && say "直连 floatingip 失败 (NETDIAG 模式, 继续)" || die "直连 floatingip 失败: ${out:-<empty>}"; fi

# (b) the e2b way: expose the port via the proxy <port>-<sid>.<domain>
TOK="$(curl -sk --max-time 8 --noproxy '*' -H "X-API-KEY: $AK" "https://api.$DOMAIN/sandboxes/$SID" 2>/dev/null | grep -o '"envdAccessToken":"[^"]*"' | cut -d'"' -f4)"
hosts_add "8000-$SID.$DOMAIN"
echo "${c_cmd}  \$ curl -H 'X-Access-Token: …' https://8000-$SID.$DOMAIN/pf.txt${c_off}"
out="$(curl -sk --max-time 8 --noproxy '*' -H "X-Access-Token: $TOK" "https://8000-$SID.$DOMAIN/pf.txt" 2>&1)" || true; echo "    ${out:-<no response>}"
if [ "$out" = "$PFMARK" ]; then ok "e2b 暴露端口 https://8000-<sid>.<domain> (proxy→floatingip) 成功"
else [ -n "${DEMO_NETDIAG:-}" ] && say "e2b 暴露端口失败 (NETDIAG 模式, 继续)" || die "e2b 暴露端口失败: ${out:-<empty>}"; fi

# (c) sandbox egress via NAT MASQUERADE
say "沙箱出网 (NAT MASQUERADE $FIP_CIDR): guest curl http://1.1.1.1"
"$E2B_BIN" sandbox exec "$SID" -- 'curl -s -m10 -o /dev/null -w "egress HTTP %{http_code}\n" http://1.1.1.1' 2>&1 | grep -iE 'egress|HTTP' | sed 's/^/    /' | tee "$WORK/egress.out"
grep -qE 'egress HTTP [23]' "$WORK/egress.out" && ok "沙箱出网成功 (NAT 生效)" || say "沙箱出网未成功 (host 无直连外网时正常; 非致命)"
pause

# ===========================================================================
banner "Pause → resume (snapshot to the store, then restore) — state survives"
# ---------------------------------------------------------------------------
show "pause: snapshot the live VM (memory + overlay) into the content store" \
    "$E2B_BIN" sandbox pause "$SID"
show "list (paused sandboxes are excluded by default; -s shows all)" \
    "$E2B_BIN" sandbox list -s 2>/dev/null || "$E2B_BIN" sandbox list
show "resume: restore the VM from the snapshot" \
    "$E2B_BIN" sandbox resume "$SID"
echo
show "read the file written BEFORE the snapshot — proving guest state was preserved" \
    "$E2B_BIN" sandbox exec "$SID" -- 'cat /home/user/state.txt'
ok "pause/resume round-trip preserved guest state"
pause

# ===========================================================================
banner "Lifecycle: list, then kill"
# ---------------------------------------------------------------------------
show "list running sandboxes" "$E2B_BIN" sandbox list
show "kill the sandbox" "$E2B_BIN" sandbox kill "$SID"
ok "sandbox killed"

echo; echo "${c_ok}══════ demo complete — built, ran, paused/resumed and killed a real microVM, all via the e2b CLI ══════${c_off}"
