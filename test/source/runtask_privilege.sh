#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d /tmp/runtask-privilege-test-XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/id" <<'EOF'
#!/usr/bin/env bash
[ "${1:-}" = -u ]
printf '%s\n' "${TEST_UID:?}"
EOF
chmod +x "$TMP/id"

# Fake sudo validates the production command shape and simulates a root child.
# It intentionally reconstructs the child's environment from the explicit
# env -i assignments, so an unrelated parent sentinel cannot leak through.
cat > "$TMP/sudo" <<EOF
#!/usr/bin/env bash
set -euo pipefail
[ "\${1:-}" = -n ] || exit 60
shift
[ "\${1:-}" = /usr/bin/env ] || exit 61
shift
[ "\${1:-}" = -i ] || exit 62
shift
root_path="" bin="" lib="" work="" out_set=0 out="" keep_set=0 keep=""
while [ \$# -gt 0 ] && [[ "\$1" == *=* ]]; do
    case "\$1" in
        PATH=*) root_path="\${1#PATH=}" ;;
        BIN=*) bin="\${1#BIN=}" ;;
        E2E_LIB=*) lib="\${1#E2E_LIB=}" ;;
        WORK=*) work="\${1#WORK=}" ;;
        OUT=*) out_set=1; out="\${1#OUT=}" ;;
        E2E_KEEP=*) keep_set=1; keep="\${1#E2E_KEEP=}" ;;
        SENTINEL=*) exit 63 ;;
        *) exit 64 ;;
    esac
    shift
done
cmd="\${1:?missing sudo command}"
shift
printf 'sudo:path=%s:bin=%s:lib=%s:work=%s:out_set=%s:out=%s:keep_set=%s:keep=%s:cmd=%s\n' \
    "\$root_path" "\$bin" "\$lib" "\$work" "\$out_set" "\$out" "\$keep_set" "\$keep" "\$cmd" >> "$TMP/sudo.log"
if [ "\$cmd" = /usr/bin/true ]; then
    exit "\${SUDO_PROBE_STATUS:-0}"
fi
[ "\$cmd" = /bin/bash ] || exit 66
name="\$(basename "\${1:?missing case script}")"
[ ! -e "$TMP/\$name.workspace" ] || { echo "pre-sudo workspace leaked" >&2; exit 65; }
env_args=(
    "PATH=$TMP:\$root_path"
    "BIN=\$bin"
    "E2E_LIB=\$lib"
    "WORK=\$work"
    "TEST_UID=0"
)
[ "\$out_set" -eq 0 ] || env_args+=("OUT=\$out")
[ "\$keep_set" -eq 0 ] || env_args+=("E2E_KEEP=\$keep")
exec /usr/bin/env -i "\${env_args[@]}" "\$cmd" "\$@"
EOF
chmod +x "$TMP/sudo"

make_harness() {
    local name="$1"
    local harness="$TMP/harness-$name"
    cat > "$harness" <<EOF
#!/usr/bin/env bash
set -euo pipefail
out="$TMP/harness-$name.out"
fail() {
    printf 'fail:%s\\n' "\$*" >> "\$out"
    exit 1
}
before_exec() {
    rm -rf "$TMP/harness-$name.workspace"
    printf 'before-exec\\n' >> "\$out"
}
. "$ROOT/test/e2e/lib/runtask_privilege.sh"
touch "$TMP/harness-$name.workspace"
runtask_enter_privileged before_exec "\$0" "\$@"
printf 'direct:uid=%s:bin=%s:lib=%s:work=%s:out=%s:keep=%s:sentinel=%s:arg=%s\\n' \
    "\$(id -u)" "\${BIN:-missing}" "\${E2E_LIB:-missing}" "\${WORK:-missing}" \
    "\${OUT-unset}" "\${E2E_KEEP-unset}" "\${SENTINEL-unset}" "\${1:-missing}" >> "\$out"
EOF
    chmod 0644 "$harness"
    printf '%s\n' "$harness"
}

run_case() {
    local name="$1" uid="$2" probe="$3" out_mode="$4" keep_mode="$5"
    local harness
    harness="$(make_harness "$name")"
    local -a env_args=(
        "TEST_UID=$uid"
        "SUDO_PROBE_STATUS=$probe"
        "BIN=/prepared products/bin"
        "E2E_LIB=/prepared helpers/lib"
        "WORK=/runner workspace/case"
        "SENTINEL=must-not-cross-sudo"
        "PATH=$TMP:$PATH"
    )
    [ "$out_mode" = unset ] || env_args+=("OUT=$out_mode")
    [ "$keep_mode" = unset ] || env_args+=("E2E_KEEP=$keep_mode")
    /usr/bin/env -u OUT -u E2E_KEEP "${env_args[@]}" /bin/bash "$harness" "argument with spaces"
}

: > "$TMP/sudo.log"
run_case nonroot_keep 1000 0 '/runner output/case' 1
cat "$TMP/harness-nonroot_keep.out"
grep -qx 'before-exec' "$TMP/harness-nonroot_keep.out"
grep -qx 'direct:uid=0:bin=/prepared products/bin:lib=/prepared helpers/lib:work=/runner workspace/case:out=/runner output/case:keep=1:sentinel=unset:arg=argument with spaces' "$TMP/harness-nonroot_keep.out"
[ -e "$TMP/harness-nonroot_keep.workspace" ]
grep -q 'sudo:path=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:bin=/prepared products/bin:lib=/prepared helpers/lib:work=/runner workspace/case:out_set=1:out=/runner output/case:keep_set=1:keep=1:cmd=/usr/bin/true' "$TMP/sudo.log"
grep -q 'cmd=/bin/bash$' "$TMP/sudo.log"

: > "$TMP/sudo.log"
run_case nonroot_unset 1000 0 unset unset
grep -qx 'direct:uid=0:bin=/prepared products/bin:lib=/prepared helpers/lib:work=/runner workspace/case:out=unset:keep=unset:sentinel=unset:arg=argument with spaces' "$TMP/harness-nonroot_unset.out"
grep -q 'out_set=0:out=:keep_set=0:keep=:cmd=/usr/bin/true' "$TMP/sudo.log"

: > "$TMP/sudo.log"
run_case root 0 64 '/runner output/case' 1
grep -qx 'direct:uid=0:bin=/prepared products/bin:lib=/prepared helpers/lib:work=/runner workspace/case:out=/runner output/case:keep=1:sentinel=must-not-cross-sudo:arg=argument with spaces' "$TMP/harness-root.out"
[ ! -s "$TMP/sudo.log" ]

# The removed optional legacy entrypoint cannot make a selected case pass by
# setting REQUIRE_RUNTASK=0. Both inherited values must fail without privilege.
for required in 0 1; do
    : > "$TMP/sudo.log"
    if REQUIRE_RUNTASK="$required" run_case "failure_$required" 1000 1 unset unset; then
        echo "sudo failure unexpectedly succeeded (legacy flag=$required)" >&2
        exit 1
    fi
    grep -qx 'fail:run-sandbox handoff requires root or passwordless sudo' "$TMP/harness-failure_$required.out"
    [ -e "$TMP/harness-failure_$required.workspace" ]
done

echo "runtask privilege helper tests: PASS"
