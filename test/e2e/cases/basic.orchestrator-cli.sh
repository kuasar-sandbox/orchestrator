#!/usr/bin/env bash
set -euo pipefail

: "${BIN:?BIN must point to the prepared platform binary directory}"
: "${WORK:?WORK must be provided by the platform E2E runner}"

NODE="$BIN/node-ctl"
SANDBOX="$BIN/sandbox-ctl"
FLATTEN="$BIN/flatten-ctl"
for tool in "$NODE" "$SANDBOX" "$FLATTEN"; do
    [ -x "$tool" ] || { echo "missing prepared product: $tool" >&2; exit 1; }
done

mkdir -p "$WORK/config"
"$NODE" config conductor --template >"$WORK/config/node.yaml"
grep -q 'domain:' "$WORK/config/node.yaml"
"$NODE" config conductor --config "$WORK/config/node.yaml" >/dev/null

"$FLATTEN" config --template >"$WORK/config/flatten.yaml"
grep -q 'referer:' "$WORK/config/flatten.yaml"
grep -q 'validity:' "$WORK/config/flatten.yaml"
"$FLATTEN" config --config "$WORK/config/flatten.yaml" >/dev/null

"$SANDBOX" config --template >"$WORK/config/sandbox.yaml"
grep -q 'resources:' "$WORK/config/sandbox.yaml"

if "$NODE" run-sandbox >/dev/null 2>&1; then
    echo "node-ctl run-sandbox accepted missing arguments" >&2
    exit 1
fi
if "$NODE" run-builder >/dev/null 2>&1; then
    echo "node-ctl run-builder accepted missing arguments" >&2
    exit 1
fi
if "$SANDBOX" info >/dev/null 2>&1; then
    echo "sandbox-ctl info accepted a missing snapshot" >&2
    exit 1
fi
if "$SANDBOX" info /dev/null >/dev/null 2>&1; then
    echo "sandbox-ctl info accepted a non-snapshot" >&2
    exit 1
fi

echo "PASS basic.orchestrator-cli.sh"
