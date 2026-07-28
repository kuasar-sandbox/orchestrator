#!/usr/bin/env bash
#
# demo_e2b.sh — end-to-end demo of the e2b-compatible sandbox host, driven by the
# UNMODIFIED e2b Python SDK (pip install e2b e2b-code-interpreter). The storage tier
# (content store + L1 cache + image registry) is persistent prep — run demo_prep.sh
# ONCE first; this script starts only the per-run pieces (orchestrator + eBPF switch)
# and runs the flow:
#
#   Template().from_image(ref).copy(…).run_cmd(…).set_start_cmd(…).build()
#                                        → the node runs the 3-phase in-sandbox build
#                                          (A pull+flatten · B COPY/RUN/ENV/WORKDIR via
#                                          envd · C startCmd→snapshot); NO client docker
#   Sandbox.create(template)             → restore the snapshot into a real microVM
#   sbx.commands.run(...)                → run commands in the guest (through the proxy)
#   port forward + egress                → reach the build's start_cmd server via floatingip
#                                          + guest→internet (NAT)
#   sbx.pause() / Sandbox.connect(id)    → snapshot / restore (resume = connect)
#   export-sandbox --to-template         → fork the paused state into a reusable template
#   connect(id, api_headers=migration)   → one-call migrate (auto import + resume)
#   sbx.kill()                           → lifecycle
#
# The SDK reaches this node exactly as it reaches e2b.dev: control plane at
# https://api.<domain>, data plane at https://<port>-<sid>.<domain>. We resolve those
# locally (/etc/hosts + a self-signed *.<domain> cert; SSL_CERT_FILE so httpx trusts
# it) and point the SDK with E2B_DOMAIN / E2B_API_KEY. Nothing about the SDK changes.
#
# Requires: demo_prep.sh already run; e2b Python SDK; systemd+root; /dev/kvm; openssl,
# python3, ip, curl, sqlite3, iptables; the built kernel+runtime erofs in bin; TCP :443.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BIN:-$REPO_ROOT/bin}"
DOMAIN="${DOMAIN:-sandboxes.demo.local}"
TLS_PORT="${TLS_PORT:-443}"
SWITCH="${SWITCH:-sw0}"; SW_NETNS="${SW_NETNS:-demo_sw}"
SW_MGMT="${SW_MGMT:-sw0m0}"; FIP_CIDR="${FIP_CIDR:-100.100.96.0/20}"
GUEST_DNS="${GUEST_DNS:-169.254.169.253}"; HOST_DNS=""
DEMO_DATA_DIR="${DEMO_DATA_DIR:-$HOME/.cache/kuasar-demo}"

# Pull in the persistent-prep handoff (REGISTRY / sockets / BASE_REF).
[ -f "$DEMO_DATA_DIR/prep.env" ] || { echo "✗ run demo_prep.sh first (no $DEMO_DATA_DIR/prep.env)"; exit 1; }
# shellcheck disable=SC1090
. "$DEMO_DATA_DIR/prep.env"

c_hd=$'\e[1;36m'; c_cmd=$'\e[1;33m'; c_ok=$'\e[1;32m'; c_dim=$'\e[2m'; c_off=$'\e[0m'
[ -t 1 ] || { c_hd=; c_cmd=; c_ok=; c_dim=; c_off=; }
step=0
banner() { step=$((step+1)); echo; echo "${c_hd}══════ [$step] $* ══════${c_off}"; }
say()  { echo "${c_dim}  · $*${c_off}"; }
ok()   { echo "${c_ok}  ✓ $*${c_off}"; }
pause(){ [ -n "${DEMO_PAUSE:-}" ] && { printf "${c_dim}  ⏎ to continue…${c_off}"; read -r _ </dev/tty 2>/dev/null || true; } || true; }
die()  { echo $'\e[1;31m'"  ✗ $*"$'\e[0m' >&2; exit 1; }

# ---- prerequisites --------------------------------------------------------
for b in node-ctl e2b-key-ctl connector-ctl cloud-hypervisor; do [ -x "$BIN/$b" ] || die "missing $BIN/$b — run 'make build'"; done
[ -f "$BIN/vmlinux" ] && [ -f "$BIN/sandbox-runtime.bundle" ] || die "missing kernel/runtime erofs in $BIN"
[ -S "${STORE_SOCK:-}" ] || die "store socket $STORE_SOCK absent — run demo_prep.sh"
[ -S "${CACHE_SOCK:-}" ] || die "cache socket $CACHE_SOCK absent — run demo_prep.sh"
[ -n "${REGISTRY:-}" ] && [ -n "${BASE_REF:-}" ] || die "REGISTRY/BASE_REF not set — run demo_prep.sh"
for t in openssl python3 ip curl sqlite3 iptables; do command -v $t >/dev/null 2>&1 || die "$t not on PATH"; done
python3 -c 'import e2b' 2>/dev/null || die "e2b Python SDK not installed (pip install e2b e2b-code-interpreter)"
[ -d /run/systemd/system ] || die "systemd is not PID1"
[ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ] || die "/dev/kvm not available (rw)"
if [ "$(id -u)" -ne 0 ]; then echo "(re-exec under sudo for systemd/KVM/:443)"; exec sudo -nE "$0" "$@"; fi

echo "${c_hd}"'
   kuasar-sandbox · e2b-compatible microVM host — Python SDK demo
   (everything below is driven by the UNMODIFIED e2b Python SDK; storage tier reused from demo_prep.sh)
'"${c_off}"

# ---- workspace + teardown -------------------------------------------------
WORK="$(mktemp -d /tmp/demo-e2b-XXXXXX)"
CLI_ENV_FILE="/tmp/demo-e2b-cli-env.sh"
UNIT_DIR=/run/systemd/system
UNITS=(sandbox-runner@.service sandbox-builder@.service sandbox-runner.slice sandbox-builder.slice)
declare -a OURS=(); for u in "${UNITS[@]}"; do [ -e "$UNIT_DIR/$u" ] && die "$UNIT_DIR/$u exists; refusing to clobber"; OURS+=("$UNIT_DIR/$u"); done
mkdir -p "$WORK/run" "$WORK/lib" "$WORK/home" "$WORK/saved"
declare -a PIDS=() NAT_ADDED=()
cleanup() {
    set +e; echo; echo "${c_dim}── teardown (storage tier from demo_prep.sh is left running) ──${c_off}"
    systemctl stop 'sandbox-runner@*.service' 'sandbox-builder@*.service' 2>/dev/null
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    for r in "${NAT_ADDED[@]:-}"; do case "$r" in
        r1) iptables -D FORWARD -o "$SW_MGMT" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null;;
        r2) iptables -D FORWARD -i "$SW_MGMT" -j ACCEPT 2>/dev/null;;
        r3) iptables -t nat -D POSTROUTING -s "$FIP_CIDR" -j MASQUERADE 2>/dev/null;;
        dns-*) iptables -t nat -D PREROUTING -d "$GUEST_DNS" -p "${r#dns-}" --dport 53 -j DNAT --to-destination "${HOST_DNS:-}:53" 2>/dev/null;;
    esac; done
    "$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1
    ip netns del "$SW_NETNS" 2>/dev/null
    for u in "${OURS[@]:-}"; do [ -n "$u" ] && rm -f "$u"; done; systemctl daemon-reload 2>/dev/null
    sed -i "/# demo-e2b-$$\$/d" /etc/hosts 2>/dev/null
    rm -f "$CLI_ENV_FILE"
    [ -n "${DEMO_KEEP:-}" ] && echo "kept: $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT
hosts_add() { local fqdn="$1"; grep -q "[[:space:]]$fqdn\b" /etc/hosts 2>/dev/null || echo "127.0.0.1 $fqdn # demo-e2b-$$" >> /etc/hosts; }
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

# py — run a Python SDK snippet with the node env (control plane + self-signed TLS trust).
py() { HOME="$WORK/home" E2B_DOMAIN="$DOMAIN" E2B_API_KEY="$AK" E2B_ACCESS_TOKEN="sk_demo" \
       SSL_CERT_FILE="$WORK/tls.crt" REQUESTS_CA_BUNDLE="$WORK/tls.crt" NO_PROXY='*' python3 - "$@"; }

# ===========================================================================
banner "Per-run node stack (orchestrator + eBPF switch; storage tier already up)"
# ---------------------------------------------------------------------------
say "overlay diff_template — pre-formatted empty ext4 seeding each cold boot's writable upper"
MKFS_EXT4="$(command -v mkfs.ext4 || echo /sbin/mkfs.ext4)"; [ -x "$MKFS_EXT4" ] || die "mkfs.ext4 not found"
OVL="$WORK/overlay-1G.ext4"; truncate -s 1G "$OVL"; "$MKFS_EXT4" -F -q -b 4096 "$OVL" >/dev/null 2>&1 || die "mkfs.ext4"
say "builder diff_template — build-sandbox writable disk (pull cache + steps delta + export scratch; sparse)"
# Sparse, so the cap is free until written. The FULL build re-flattens the base
# again in phase B (steps export) on top of phase A's pull+flatten, so headroom
# beyond the ~3 GB base matters: blobs + unpacked tree + two erofs outputs + mkfs
# chunk staging can coexist. 24 GiB sparse covers a code-interpreter (~3 GB) build.
BLD="$WORK/builder-24G.ext4"; truncate -s 24G "$BLD"; "$MKFS_EXT4" -F -q -b 4096 "$BLD" >/dev/null 2>&1 || die "mkfs.ext4 (builder)"

say "self-signed *.$DOMAIN cert (SDK trusts it via SSL_CERT_FILE; data plane is https)"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/tls.key" -out "$WORK/tls.crt" -days 2 \
    -subj "/CN=*.$DOMAIN" -addext "subjectAltName=DNS:*.$DOMAIN,DNS:$DOMAIN" >/dev/null 2>&1 || die "openssl"

# manifest store/cache point at the persistent UDS daemons (demo_prep.sh).
cat > "$WORK/manifest.yaml" <<EOF
manifest: { key: "" }
store: { endpoint: $STORE_SOCK, pool: 4, timeout: 30s }
cache: { endpoint: $CACHE_SOCK }
chunker: { mode: cdc, cdc: { min: 128KiB, avg: 512KiB, max: 1MiB } }
crypto: { chunk: aes, manifest: aes }
EOF
INSECURE=false; [ -n "${REGISTRY_INSECURE:-}" ] && INSECURE=true
MK="$("$BIN/e2b-key-ctl" gen-key)"; ENC="$("$BIN/e2b-key-ctl" gen-key)"
# DEMO_MMDS=1: envd runs in FC mode + the orchestrator re-keys it via the metadata
# service (envd-enforced, defense-in-depth). Unset: envd runs non-secure, the proxy is
# the sole data-plane gate. Both modes make snapshot forks SDK-usable.
MMDS_CFG=""; MMDS_SVC=""
if [ -n "${DEMO_MMDS:-}" ]; then
    MMDS_CFG='mmds: { enabled: true, listen: "127.0.0.1:19254" }'
    # vswitch translates the guest's 169.254.169.254:80 VIP to the orchestrator's
    # loopback MMDS — no iptables (the eBPF datapath does it + rewrites replies).
    MMDS_SVC="--mgmt-service=169.254.169.254:80:127.0.0.1:19254"
fi
# builder.files_storage backs COPY build contexts (the e2b SDK direct-uploads the
# context via a presigned PUT; the build fetches it via presigned GET). Wired only
# when demo_prep.sh brought up versitygw (VGW_ENDPOINT in prep.env); absent → COPY
# is unavailable and the build below omits its COPY step.
FILES_STORAGE_CFG=""
if [ -n "${VGW_ENDPOINT:-}" ]; then
    FILES_STORAGE_CFG="  files_storage: { endpoint: $VGW_ENDPOINT, region: ${VGW_REGION:-us-east-1}, bucket: $VGW_BUCKET, access_key: $VGW_ACCESS_KEY, secret_key: $VGW_SECRET_KEY, force_path_style: true }"
fi
cat > "$WORK/config.yaml" <<EOF
api: { domain: $DOMAIN, listen: ":$TLS_PORT", tls: { cert: $WORK/tls.crt, key: $WORK/tls.key } }
proxy: { auth: enforce }
$MMDS_CFG
encryption_key: "$ENC"
manifest_config: $WORK/manifest.yaml
paths: { run_root: $WORK/run, base_root: $WORK/lib, config_socket: $WORK/node-ctl.socket }
units: { dir: $UNIT_DIR }
sandbox:
  network: { switch: $SWITCH }            # e2b defaults: ip 169.254.0.21/30 nexthop .22; hostname/dns injected via files:
  boot:
    kernel: $BIN/vmlinux
    runtime: $BIN/sandbox-runtime.bundle
    overlay_diff_template: $OVL
builder:                                    # no image_uri_mask: from_image names the image directly
  insecure_registry: $INSECURE
  diff_template: $BLD
  vcpu: 2                                   # build-VM budget: flatten streams, page cache reclaims —
  memory: 2GiB                              # 2GiB suffices for a ~3GB base on a small demo host
  pull_timeout_sec: 600                     # full A+B+C build on a ~3GB base: generous per-phase budgets
  step_timeout_sec: 300
  ready_timeout_sec: 120
  total_timeout_sec: 1800
$FILES_STORAGE_CFG
checkpoint: { mode: local, local_dir: $WORK/saved }
EOF

# eBPF/TC switch + host NAT (per run; the storage tier is persistent, not the network).
"$BIN/connector-ctl" vswitch stop "$SWITCH" --force >/dev/null 2>&1 || true; ip netns del "$SW_NETNS" 2>/dev/null || true
ip netns add "$SW_NETNS" 2>/dev/null || true
say "connector-ctl vswitch start — eBPF/TC switch; --mgmt-extract adds host NIC $SW_MGMT (169.254.169.254)"
"$BIN/connector-ctl" vswitch start "$SWITCH" --netns="$SW_NETNS" --ports=64 --mac-addr=02:00:00:00:00:01 \
    --floating-ip-base=100.100.96.0 --mode=tap \
    --mgmt-extract=:$SW_MGMT:169.254.169.254,0.0.0.0/0 $MMDS_SVC >"$WORK/vswitch.log" 2>&1 || { cat "$WORK/vswitch.log"; die "vswitch start"; }
iptables -C FORWARD -o "$SW_MGMT" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null \
  || { iptables -A FORWARD -o "$SW_MGMT" -m state --state RELATED,ESTABLISHED -j ACCEPT && NAT_ADDED+=(r1); }
iptables -C FORWARD -i "$SW_MGMT" -j ACCEPT 2>/dev/null \
  || { iptables -A FORWARD -i "$SW_MGMT" -j ACCEPT && NAT_ADDED+=(r2); }
iptables -t nat -C POSTROUTING -s "$FIP_CIDR" -j MASQUERADE 2>/dev/null \
  || { iptables -t nat -A POSTROUTING -s "$FIP_CIDR" -j MASQUERADE && NAT_ADDED+=(r3); }
HOST_DNS="$(awk '/^nameserver/{print $2; exit}' /etc/resolv.conf 2>/dev/null)"
if [ -n "$HOST_DNS" ]; then for pr in udp tcp; do
    iptables -t nat -C PREROUTING -d "$GUEST_DNS" -p $pr --dport 53 -j DNAT --to-destination "$HOST_DNS:53" 2>/dev/null \
      || { iptables -t nat -A PREROUTING -d "$GUEST_DNS" -p $pr --dport 53 -j DNAT --to-destination "$HOST_DNS:53" && NAT_ADDED+=(dns-$pr); }
done; fi
ok "switch + NAT ready (guest DNS $GUEST_DNS → host ${HOST_DNS:-unrouted})"
if [ -n "${DEMO_MMDS:-}" ]; then
    # vswitch --mgmt-service already translates 169.254.169.254:80 -> the loopback MMDS in
    # the eBPF datapath (no iptables); a loopback target just needs route_localnet on the
    # mgmt dev so the kernel accepts the redirected packet.
    sysctl -q -w "net.ipv4.conf.$SW_MGMT.route_localnet=1" 2>/dev/null || true
    ok "MMDS mode: envd re-keyed via metadata service (vswitch mgmt-service 169.254.169.254:80 → 127.0.0.1:19254)"
fi

hosts_add "api.$DOMAIN"
say "node-ctl conductor serve — e2b control plane + data-plane proxy (TLS :$TLS_PORT)"
"$BIN/node-ctl" conductor serve --config "$WORK/config.yaml" >"$WORK/orch.log" 2>&1 & PIDS+=($!)
for _ in $(seq 1 40); do (exec 3<>"/dev/tcp/127.0.0.1/$TLS_PORT") 2>/dev/null && { exec 3>&- 3<&-; break; }; kill -0 "${PIDS[-1]}" 2>/dev/null || { sed 's/^/    /' "$WORK/orch.log"; die "orchestrator exited"; }; sleep 0.5; done
ok "orchestrator serving https://api.$DOMAIN"
pause

# ===========================================================================
banner "Onboard a tenant (ManifestKey + derived APISecret → allowlist + API key)"
# ---------------------------------------------------------------------------
REG_FLAGS=(); [ -n "${REGISTRY_USER:-}" ] && REG_FLAGS=(--registry-username "$REGISTRY_USER" --registry-password "${REGISTRY_PASS:-}")
say "allowlist the tenant manifest key + its default registry pull creds (so the node can pull $REGISTRY):"
echo "${c_cmd}  \$ node-ctl manifest-key add ${REG_FLAGS:+--registry-username … } \$MANIFEST_KEY${c_off}"
"$BIN/node-ctl" manifest-key add --socket "$WORK/node-ctl.socket" "${REG_FLAGS[@]}" "$MK" >/dev/null || die "manifest-key add"
API_SECRET="$("$BIN/e2b-key-ctl" derive-api-secret "$MK")"
AK="$("$BIN/e2b-key-ctl" gen-apikey "$API_SECRET")"
ok "tenant ready — e2b API key ${AK:0:16}…  (format e2b_<hex>)"
# world-readable env for another terminal to drive the Python SDK against this node
cat > "$CLI_ENV_FILE" <<EOF
export E2B_DOMAIN='$DOMAIN' E2B_API_KEY='$AK' E2B_ACCESS_TOKEN='sk_demo'
export SSL_CERT_FILE='$WORK/tls.crt' REQUESTS_CA_BUNDLE='$WORK/tls.crt' NO_PROXY='*'
EOF
chmod 644 "$CLI_ENV_FILE"
say "another terminal can drive the SDK: ${c_cmd}source $CLI_ENV_FILE${c_off}${c_dim} then python3 -c 'from e2b import Sandbox; …'${c_off}"
pause

# ===========================================================================
banner "Build a template — full pipeline (from_image + COPY + RUN + ENV + WORKDIR + startCmd→snapshot)"
# ---------------------------------------------------------------------------
# The pull runs INSIDE the build microVM (tenant network), so the image ref
# must be guest-reachable: a loopback-bound local zot is addressed via the
# vswitch mgmt VIP instead (same host); third-party registries pass through.
GUEST_BASE_REF="$BASE_REF"
case "$BASE_REF" in
  127.0.0.1:*|localhost:*)
    GUEST_BASE_REF="169.254.169.254:${BASE_REF#*:}"
    curl -fs --max-time 3 --noproxy '*' -o /dev/null "http://${GUEST_BASE_REF%%/*}/v2/" \
      || die "registry $REGISTRY unreachable on the mgmt VIP — zot must bind 0.0.0.0; restart prep: demo_prep.sh stop && bash demo_prep.sh"
    ;;
esac

# A tiny local build context for the COPY step: a static site the template's
# start command will serve. BUILT_MARKER threads through the whole demo — it is
# COPY'd into the image here, RUN appends to the page, and step 6 fetches it from
# the start_cmd server after a snapshot+restore (proving every phase end to end).
BUILT_MARKER="kuasar-built-$RANDOM"
mkdir -p "$WORK/ctx/site"
cat > "$WORK/ctx/site/index.html" <<HTML
<h1>kuasar build demo</h1>
<p>[COPY] this file came from the build context.</p>
HTML
echo "this file was COPY'd from the build context" > "$WORK/ctx/site/COPIED.txt"

# COPY needs builder.files_storage (versitygw, brought up by demo_prep.sh).
# Present → the build COPYs the context; absent → it omits COPY (still runs
# RUN/ENV/WORKDIR and the startCmd→snapshot, just without the object-store path).
HAS_COPY=False; [ -n "${VGW_ENDPOINT:-}" ] && HAS_COPY=True
COPY_DISP=""; [ "$HAS_COPY" = True ] && COPY_DISP=".copy('site','/home/user/site')"
[ "$HAS_COPY" = True ] \
  && say "files_storage (versitygw) up — the build includes COPY (context direct-uploaded via presigned PUT, fetched + extracted in-build)" \
  || say "no files_storage — the build omits COPY (re-run demo_prep.sh with versitygw to include it)"
say "ONE fluent build drives all three phases — A pull+flatten · B steps (COPY/RUN/ENV/WORKDIR via envd) · C startCmd→snapshot:"
echo "${c_cmd}  \$ Template().from_image('$GUEST_BASE_REF')${COPY_DISP}.run_cmd(…).set_envs(…).set_workdir(…).set_start_cmd('python3 -m http.server 8000', wait_for_url(…))${c_off}"
say "on_build_logs streams the node's build journal (every phase + step) live as it runs:"
# on_build_logs prints to STDERR so the live stream shows in the terminal without
# polluting the template id captured from stdout below. With a start command the
# result is a SNAPSHOT template (kind=snp): a microVM frozen with the start
# command left running under envd, so create is a restore, not a cold boot.
TEMPLATE="$(py <<PY
import os, sys
from e2b import Template, wait_for_url
os.chdir("$WORK/ctx")                                  # COPY src paths resolve from here
def show(e):
    msg = e.message.rstrip()
    if msg:
        print("    · " + msg, file=sys.stderr, flush=True)
tpl = Template().from_image("$GUEST_BASE_REF")
if $HAS_COPY:
    tpl = tpl.copy("site", "/home/user/site", user="1000:1000")   # B: COPY into the image, owned by the e2b user
tpl = (tpl
    .run_cmd("mkdir -p /home/user/site && echo '<p>[RUN] $BUILT_MARKER - appended in the build sandbox via envd.</p>' >> /home/user/site/index.html")
    .set_envs({"DEMO_BUILT": "kuasar"})                # B: ENV (into the image config)
    .set_workdir("/home/user/site")                    # B: WORKDIR
    .set_start_cmd("python3 -m http.server 8000 --directory /home/user/site",   # C: startCmd → snapshot
                   wait_for_url("http://localhost:8000/")))                     # C: readyCmd (curl; base lacks ss)
info = Template.build(tpl, name="demo-app", cpu_count=2, memory_mb=2048, on_build_logs=show)
print(info.template_id)
PY
)" || die "template build failed (see $WORK/orch.log)"
[ -n "$TEMPLATE" ] || die "no template id from build"
ok "snapshot template built → $TEMPLATE  (start command frozen under envd)"
pause

# ===========================================================================
banner "Spawn a real microVM sandbox (Sandbox.create)"
# ---------------------------------------------------------------------------
say "create boots cloud-hypervisor from the flattened template; envd starts inside (create is lazy — no eager data-plane connect)."
SID="$(py <<PY
from e2b import Sandbox
s = Sandbox.create("$TEMPLATE", timeout=300)
print(s.sandbox_id)
PY
)" || die "create failed"
[ -n "$SID" ] || die "no sandbox id from create"
hosts_add "49983-$SID.$DOMAIN"; hosts_add "49999-$SID.$DOMAIN"
ok "sandbox running: $SID  (data plane: https://49983-$SID.$DOMAIN → proxy → guest envd)"
pause

# ===========================================================================
banner "Run commands in the guest (sbx.commands.run)"
# ---------------------------------------------------------------------------
py <<PY || die "exec failed"
from e2b import Sandbox
s = Sandbox.connect("$SID")
r = s.commands.run("id; uname -sm; python3 --version; grep ^PRETTY_NAME= /etc/os-release")
print(r.stdout.rstrip())
# e2b sandboxes share sandbox-init's PID namespace (launch.pid_namespace=shared) so PID 1
# reaps orphaned descendants of guest commands instead of them piling up as zombies under envd.
init1 = s.commands.run("cat /proc/1/comm").stdout.strip()
print("guest PID 1 (reaps orphaned descendants):", init1)
assert init1 != "envd", f"e2b sandbox must share sandbox-init's PID ns (PID 1 = reaper, not envd); got {init1!r}"
print("--- the template's build outputs are present (COPY + RUN + ENV + startCmd) ---")
print(s.commands.run("echo '[/home/user/site/index.html]'; cat /home/user/site/index.html; "
                     "echo '[/home/user/site/COPIED.txt]'; cat /home/user/site/COPIED.txt 2>/dev/null || echo '(COPY skipped — no files_storage)'").stdout.rstrip())
print("ENV DEMO_BUILT =", s.commands.run("printenv DEMO_BUILT || echo '(image env not applied to exec)'").stdout.strip())
code = s.commands.run("curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/ || echo none").stdout.strip()
print("startCmd http.server in-guest (HTTP", code + ") — frozen in the snapshot, live after restore")
assert code == "200", f"the template start command (http.server) is not serving after restore (got {code!r})"
print("--- write a file that must survive pause/resume ---")
s.commands.run("echo 'hello from before the snapshot' > /home/user/state.txt")
print(s.commands.run("cat /home/user/state.txt").stdout.rstrip())
PY
ok "command execution works end-to-end (SDK → proxy → envd → guest)"
pause

# ===========================================================================
banner "网络：端口转发 (host→沙箱 floatingip) + 沙箱出网 (NAT)"
# ---------------------------------------------------------------------------
# No server to start: the template's start command (python3 -m http.server 8000,
# set at build time) is ALREADY running in the sandbox, serving the COPY'd+RUN-built
# page — reaching it proves the start_cmd survived create (snapshot→restore).
say "the template's start command already serves the built site on :8000 (frozen under envd; /etc/hosts+resolv.conf injected by orchestrator via files:)"
DB="$WORK/lib/node-ctl.db"
[ -s "$DB" ] || DB="$WORK/lib/orchestrator.db"
FIP="$(sqlite3 "$DB" "select floatingip from sandboxes where id='$SID'" 2>/dev/null || true)"
[ -n "$FIP" ] || die "could not resolve floatingip for sandbox $SID from $DB"
say "沙箱 floatingip = ${FIP:-?}  (host 经 $SW_MGMT 直达)"
echo "${c_cmd}  \$ curl http://$FIP:8000/    # served by the template's start command${c_off}"
out=""; for _ in $(seq 1 8); do out="$(curl -s --max-time 5 --noproxy '*' "http://$FIP:8000/" 2>&1)" || true; case "$out" in *"$BUILT_MARKER"*) break;; esac; sleep 1; done
case "$out" in
  *"$BUILT_MARKER"*) ok "host→沙箱 floatingip:8000 直连成功，且页面即构建产物 (start_cmd 经快照→恢复仍在服务)";;
  *) [ -n "${DEMO_NETDIAG:-}" ] && say "直连失败 (NETDIAG, 继续)" || die "直连 floatingip 失败或页面非构建产物: ${out:-<empty>}";;
esac
TOKEN_RESPONSE="$WORK/connect-token.json"
code="$(curl -sk --max-time 8 --noproxy '*' -o "$TOKEN_RESPONSE" -w '%{http_code}' \
    -X POST -H "X-API-KEY: $AK" -H 'Content-Type: application/json' -d '{}' \
    "https://api.$DOMAIN/sandboxes/$SID/connect" 2>/dev/null || true)"
[ "$code" = "200" ] || die "connect did not return sandbox credentials (HTTP ${code:-000})"
TOK="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["forwardAccessToken"])' "$TOKEN_RESPONSE" 2>/dev/null || true)"
[ -n "$TOK" ] || die "connect response did not contain forwardAccessToken"
hosts_add "8000-$SID.$DOMAIN"
out="$(curl -sk --max-time 8 --noproxy '*' -H "X-Access-Token: $TOK" "https://8000-$SID.$DOMAIN/" 2>&1)" || true
case "$out" in
  *"$BUILT_MARKER"*) ok "e2b 暴露端口 https://8000-<sid>.<domain> (proxy→floatingip) 服务构建产物成功";;
  *) [ -n "${DEMO_NETDIAG:-}" ] && say "暴露端口失败 (NETDIAG, 继续)" || die "e2b 暴露端口失败: ${out:-<empty>}";;
esac
say "沙箱出网 (NAT MASQUERADE): guest curl http://1.1.1.1"
py <<PY 2>&1 | grep -iE 'egress|HTTP' | sed 's/^/    /' || true
from e2b import Sandbox
print(Sandbox.connect("$SID").commands.run("curl -s -m10 -o /dev/null -w 'egress HTTP %{http_code}\\n' http://1.1.1.1").stdout)
PY
pause

# ===========================================================================
banner "Pause → resume (snapshot then restore; resume == Sandbox.connect)"
# ---------------------------------------------------------------------------
say "pause snapshots the live VM; connect resumes it (the SDK has no resume() — connect auto-resumes a paused sandbox)."
py <<PY || die "pause/resume failed"
from e2b import Sandbox
Sandbox.connect("$SID").pause()
print("paused; resuming via connect…")
s = Sandbox.connect("$SID")
print("state survived:", s.commands.run("cat /home/user/state.txt").stdout.rstrip())
PY
ok "pause/resume preserved guest state"
pause

# ===========================================================================
banner "暂停态转模板 (export-sandbox --to-template) → 从模板扇出新沙箱"
# ---------------------------------------------------------------------------
say "pause, then promote the paused snapshot to a reusable remote template (local→remote)."
py <<PY || die "pause-before-export failed"
from e2b import Sandbox
Sandbox.connect("$SID").pause()
PY
echo "${c_cmd}  \$ node-ctl export-sandbox $SID --to-template --keep-source${c_off}"
FORK_TMPL="$(E2B_API_KEY="$AK" "$BIN/node-ctl" export-sandbox "$SID" --to-template --keep-source --socket "$WORK/node-ctl.socket")" \
  || die "export-sandbox --to-template failed"
ok "paused state → template $FORK_TMPL"
say "create a NEW sandbox from that template — it carries the forked state:"
CHILD="$(py <<PY
from e2b import Sandbox
s = Sandbox.create("$FORK_TMPL", timeout=120)
print(s.sandbox_id)
PY
)" || die "create-from-fork failed"
hosts_add "49983-$CHILD.$DOMAIN"; hosts_add "49999-$CHILD.$DOMAIN"
py <<PY || die "forked child exec failed"
from e2b import Sandbox
ids = [s.sandbox_id for s in Sandbox.list().next_items()]
print("forked child", "$CHILD", "running:", "$CHILD" in ids)
print("  child sees forked state:", Sandbox.connect("$CHILD").commands.run("cat /home/user/state.txt").stdout.rstrip())
print("  child exec user:", Sandbox.connect("$CHILD").commands.run("id -un").stdout.rstrip())
Sandbox.connect("$CHILD").kill()
PY
ok "fork via template: a fresh sandbox booted from the paused state + in-guest exec works"
pause

# ===========================================================================
banner "一步迁移 (export move → connect with X-Kuasar-Migration-Token = import+resume)"
# ---------------------------------------------------------------------------
say "export (move) mints a one-line token and relinquishes the source row; connect with the token re-imports + resumes in ONE SDK call."
echo "${c_cmd}  \$ TOKEN=\$(node-ctl export-sandbox $SID)${c_off}"
MIG_TOKEN="$(E2B_API_KEY="$AK" "$BIN/node-ctl" export-sandbox "$SID" --socket "$WORK/node-ctl.socket")" || die "export-sandbox (move) failed"
say "source row now gone; resume on (logically) another node with the token in api_headers:"
py <<PY || die "connect-with-migration-token failed"
from e2b import Sandbox
s = Sandbox.connect("$SID", api_headers={"X-Kuasar-Migration-Token": "$MIG_TOKEN"})
print("migrated + resumed; state:", s.commands.run("cat /home/user/state.txt").stdout.rstrip())
PY
ok "one-call migration (auto import + resume) preserved guest state"
pause

# ===========================================================================
banner "Lifecycle: list, then kill"
# ---------------------------------------------------------------------------
py <<PY || true
from e2b import Sandbox
print("running sandboxes:", [s.sandbox_id for s in Sandbox.list().next_items()])
Sandbox.connect("$SID").kill()
print("killed $SID")
PY
ok "sandbox killed"
echo; echo "${c_ok}══════ demo complete — built (full 3-phase pipeline → snapshot), created, ran, served the built site, paused/resumed, forked-to-template, migrated, killed — all via the e2b Python SDK ══════${c_off}"
