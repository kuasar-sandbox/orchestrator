[Reading 74 lines from start (total: 74 lines, 0 remaining)]

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d /tmp/runtask-privilege-test-XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/id" <<'EOF'
#!/usr/bin/env bash
[ "$1" = -u ]
printf '%s\n' "${TEST_UID:?}"
EOF

cat > "$TMP/sudo" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  -n)
    shift
    [ "${1:-}" = true ] && exit "${SUDO_PROBE_STATUS:-0}"
    ;;
  -nE)
    shift
    export TEST_UID=0
    printf 'delegated:%s\n' "${PRESERVED_JOB_VALUE:-missing}" >> "${RESULTS:?}"
    exec "$@"
    ;;
esac
exit 64
EOF
chmod +x "$TMP/id" "$TMP/sudo"

cat > "$TMP/harness" <<EOF
#!/usr/bin/env bash
set -euo pipefail
skip() {
    printf 'skip:%s\n' "\$*" >> "\${RESULTS:?}"
    [ "\${REQUIRE_RUNTASK:-0}" = 1 ] && exit 1
    exit 0
}
. "$ROOT/test/e2e/lib/runtask_privilege.sh"
runtask_enter_privileged : "\$@"
printf 'direct:%s:%s\n' "\$(id -u)" "\${PRESERVED_JOB_VALUE:-missing}" >> "\${RESULTS:?}"
EOF
chmod +x "$TMP/harness"

run_case() {
    local name="$1" uid="$2" probe="$3" required="$4"
    RESULTS="$TMP/$name.out" TEST_UID="$uid" SUDO_PROBE_STATUS="$probe" \
        REQUIRE_RUNTASK="$required" PRESERVED_JOB_VALUE=owner-selection \
        PATH="$TMP:$PATH" "$TMP/harness"
}

run_case nonroot 1000 0 1
[ "$(cat "$TMP/nonroot.out")" = $'delegated:owner-selection\ndirect:0:owner-selection' ]

run_case root 0 64 1
[ "$(cat "$TMP/root.out")" = 'direct:0:owner-selection' ]

run_case optional_failure 1000 1 0
grep -qx 'skip:run-sandbox handoff requires root or passwordless sudo' "$TMP/optional_failure.out"

if run_case required_failure 1000 1 1; then
    echo "required sudo failure unexpectedly succeeded" >&2
    exit 1
fi
grep -qx 'skip:run-sandbox handoff requires root or passwordless sudo' "$TMP/required_failure.out"

# Lock the real owner case to the tested helper; a detached helper test must not
# stay green if e2e_runtask regresses to the old direct non-root skip.
ENTRY="$ROOT/test/e2e/e2e_runtask.sh"
grep -Fqx ". \"\$SCRIPT_DIR/lib/runtask_privilege.sh\"" "$ENTRY"
grep -Fqx "runtask_enter_privileged cleanup \"\$@\"" "$ENTRY"

echo "runtask privilege entry tests: PASS"