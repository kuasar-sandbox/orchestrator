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
root_path="" bin="" require="" keep_set=0 keep=""
while [ \$# -gt 0 ] && [[ "\$1" == *=* ]]; do
    case "\$1" in
        PATH=*) root_path="\${1#PATH=}" ;;
        BIN=*) bin="\${1#BIN=}" ;;
        REQUIRE_RUNTASK=*) require="\${1#REQUIRE_RUNTASK=}" ;;
        E2E_KEEP=*) keep_set=1; keep="\${1#E2E_KEEP=}" ;;
        SENTINEL=*) exit 63 ;;
        *) exit 64 ;;
    esac
    shift
done
cmd="\${1:?missing sudo command}"
shift
printf 'sudo:path=%s:bin=%s:require=%s:keep_set=%s:keep=%s:cmd=%s\n' \
    "\$root_path" "\$bin" "\$require" "\$keep_set" "\$keep" "\$cmd" >> "$TMP/sudo.log"
if [ "\$cmd" = /usr/bin/true ]; then
    exit "\${SUDO_PROBE_STATUS:-0}"
fi
name="\$(basename "\$cmd")"
[ ! -e "$TMP/\$name.workspace" ] || { echo "pre-sudo workspace leaked" >&2; exit 65; }
env_args=(
    "PATH=$TMP:\$root_path"
    "BIN=\$bin"
    "REQUIRE_RUNTASK=\$require"
    "TEST_UID=0"
)
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
skip() {
    printf 'skip:%s\\n' "\$*" >> "\$out"
    [ "\${REQUIRE_RUNTASK:-0}" = 1 ] && exit 1
    exit 0
}
before_exec() {
    rm -rf "$TMP/harness-$name.workspace"
    printf 'before-exec\\n' >> "\$out"
}
. "$ROOT/test/e2e/lib/runtask_privilege.sh"
touch "$TMP/harness-$name.workspace"
runtask_enter_privileged before_exec "\$0" "\$@"
printf 'direct:uid=%s:bin=%s:require=%s:keep=%s:sentinel=%s\\n' \
    "\$(id -u)" "\${BIN:-missing}" "\${REQUIRE_RUNTASK:-missing}" \
    "\${E2E_KEEP-unset}" "\${SENTINEL-unset}" >> "\$out"
EOF
    chmod +x "$harness"
    printf '%s\n' "$harness"
}

run_case() {
    local name="$1" uid="$2" probe="$3" required="$4" keep_mode="$5"
    local harness
    harness="$(make_harness "$name")"
    local -a env_args=(
        "TEST_UID=$uid"
        "SUDO_PROBE_STATUS=$probe"
        "REQUIRE_RUNTASK=$required"
        "BIN=/owner/selected/bin"
        "SENTINEL=must-not-cross-sudo"
        "PATH=$TMP:$PATH"
    )
    [ "$keep_mode" = unset ] || env_args+=("E2E_KEEP=$keep_mode")
    /usr/bin/env "${env_args[@]}" "$harness"
}

: > "$TMP/sudo.log"
run_case nonroot_keep 1000 0 1 1
cat "$TMP/harness-nonroot_keep.out"
grep -qx 'before-exec' "$TMP/harness-nonroot_keep.out"
grep -qx 'direct:uid=0:bin=/owner/selected/bin:require=1:keep=1:sentinel=unset' "$TMP/harness-nonroot_keep.out"
[ -e "$TMP/harness-nonroot_keep.workspace" ]
grep -q 'sudo:path=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:bin=/owner/selected/bin:require=1:keep_set=1:keep=1:cmd=/usr/bin/true' "$TMP/sudo.log"
grep -q 'cmd=.*/harness-nonroot_keep$' "$TMP/sudo.log"

: > "$TMP/sudo.log"
run_case nonroot_unset 1000 0 1 unset
grep -qx 'direct:uid=0:bin=/owner/selected/bin:require=1:keep=unset:sentinel=unset' "$TMP/harness-nonroot_unset.out"
grep -q 'keep_set=0:keep=:cmd=/usr/bin/true' "$TMP/sudo.log"

: > "$TMP/sudo.log"
run_case root 0 64 1 1
grep -qx 'direct:uid=0:bin=/owner/selected/bin:require=1:keep=1:sentinel=must-not-cross-sudo' "$TMP/harness-root.out"
[ ! -s "$TMP/sudo.log" ]

: > "$TMP/sudo.log"
run_case optional_failure 1000 1 0 unset
grep -qx 'skip:run-sandbox handoff requires root or passwordless sudo' "$TMP/harness-optional_failure.out"
[ -e "$TMP/harness-optional_failure.workspace" ]

: > "$TMP/sudo.log"
if run_case required_failure 1000 1 1 unset; then
    echo "required sudo failure unexpectedly succeeded" >&2
    exit 1
fi
grep -qx 'skip:run-sandbox handoff requires root or passwordless sudo' "$TMP/harness-required_failure.out"

# Lock the real owner case to the tested helper and unconditional pre-reexec
# cleanup. Then prove the wiring check catches a direct-root-skip regression.
assert_entry_wiring() {
    local entry="$1"
    grep -Fqx ". \"\$SCRIPT_DIR/lib/runtask_privilege.sh\"" "$entry"
    grep -Fqx "    rm -rf \"\$WORK\"" "$entry"
    grep -Fqx "runtask_enter_privileged cleanup_before_privilege_reexec \"\$SCRIPT_DIR/e2e_runtask.sh\" \"\$@\"" "$entry"
}
ENTRY="$ROOT/test/e2e/e2e_runtask.sh"
assert_entry_wiring "$ENTRY"
MUTANT="$TMP/e2e_runtask-mutant.sh"
# The replacement intentionally writes a literal shell expression.
# shellcheck disable=SC2016
sed 's/^runtask_enter_privileged cleanup_before_privilege_reexec .*$/[ "$(id -u)" -eq 0 ] || skip "run-sandbox handoff requires a delegated systemd unit"/' "$ENTRY" > "$MUTANT"
if assert_entry_wiring "$MUTANT" 2>/dev/null; then
    echo "wiring mutant unexpectedly passed" >&2
    exit 1
fi

echo "runtask privilege entry tests: PASS"
