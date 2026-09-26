#!/usr/bin/env bash
# Source integration for internal orchestrator DB/publication paths that require
# the real sandbox-ctl CLI. This is deliberately not prepared product E2E.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
: "${ORG:?ORG must point to the exact integration source workspace}"
SANDBOXER_SOURCE_ROOT="${SANDBOXER_SOURCE_ROOT:-$ORG/sandboxer}"
[ -f "$SANDBOXER_SOURCE_ROOT/go.mod" ] || {
    echo "missing exact sandboxer source at $SANDBOXER_SOURCE_ROOT" >&2
    exit 1
}
command -v go >/dev/null || { echo "go is required for source integration" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required for source integration" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

(
    cd "$SANDBOXER_SOURCE_ROOT"
    GOOS=linux CGO_ENABLED=0 go build -trimpath -o "$WORK/sandbox-ctl" ./cmd/sandbox-ctl
)
(
    cd "$ROOT"
    GOOS=linux CGO_ENABLED=0 go build -trimpath -o "$WORK/node-ctl" ./cmd/node-ctl
)

RESULTS="$WORK/results.jsonl"
KUASAR_TEST_SANDBOX_CTL="$WORK/sandbox-ctl"     go test -json -count=1 -timeout=5m ./internal/orch     -run '^(TestCapturePairCLIToPausedDatabase|TestUploadCaptureCLIToPausedDatabase|TestExportPublicationCLIAPI)$'     >"$RESULTS"

python3 - "$RESULTS" <<'PY'
import json
import sys

required = {
    "TestCapturePairCLIToPausedDatabase",
    "TestUploadCaptureCLIToPausedDatabase",
    "TestExportPublicationCLIAPI",
}
passed = set()
started = set()
for raw in open(sys.argv[1], encoding="utf-8"):
    event = json.loads(raw)
    name = event.get("Test")
    action = event.get("Action")
    if not name:
        continue
    root = name.split("/", 1)[0]
    if root not in required:
        continue
    if action == "run":
        started.add(root)
    elif action == "pass" and name == root:
        passed.add(root)
    elif action in {"fail", "skip"}:
        raise SystemExit(f"capture source integration {action}: {name}")

missing = required - started
if missing:
    raise SystemExit(f"capture source integration did not run: {sorted(missing)}")
missing = required - passed
if missing:
    raise SystemExit(f"capture source integration did not pass: {sorted(missing)}")
PY

echo "PASS source/capture-cli"
